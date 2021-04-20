//! `slb` main executable

use std::collections::hash_map::DefaultHasher;
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, Write};
use std::iter;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::thread;

use bstr::io::BufReadExt;
use memchr::memchr;
use structopt::StructOpt;

/// Performs streaming load balancing on stdin, handing off input
/// to child processes based on a hash of the first word on each line.
///
/// E.g., suppose we have a file with contents like
///
/// ```
/// key1 a b c d
/// key2 e f g h
/// key1 a b
/// ```
///
/// The key is all bytes leading up to the first space, or all bytes
/// on a line if there are no spaces. Suppose `hash(key1) == 1` and
/// `hash(key2) == 2`. For a machine with 2 cores, `slb` will have
/// two processes, and the zeroth one will receive as stdin
///
/// ```
/// key2 e f g h
/// ```
///
/// since `2 % 2 == 0` and all `key1` lines will be fed into the
/// process at index 1.
///
/// These processes are expected to perform some kind of aggregation
/// and print at the end of their execution. For instance, suppose we invoke
/// `slb 'awk -f catter.awk'` where `catter.awk` is just defined to be
/// `{key = $1; $1 = ""; a[key] += $0}END{for (k in a) print k,a[k]}`,
/// which just concatenates the keyed values. Then the output might be
///
/// ```
/// key1  a b c d a b
/// key2  e f g h
/// ```
#[derive(Debug, StructOpt)]
#[structopt(name = "slb", about = "Performs streaming load balancing.")]
struct Opt {
    /// The first positional argument determines the child processes
    /// that get launched. It is required.
    ///
    /// Multiple instances of this same process are created with the same
    /// command-line string. Text lines from the stdin of `slb` are fed
    /// into these processes, and stdout is shared between this parent
    /// process and its children.
    cmd: String,

    /// Queue size for mpsc queues used for load balancing to inputs.
    #[structopt(short, long)]
    queuesize: Option<usize>,

    /// Buffer size for reading input.
    #[structopt(short, long)]
    bufsize: Option<usize>,
    // arguments to consider:
    // #[structopt(short,long)]
    // sort-like KEYDEF -k --key
    // nproc -j --jobs
    // buffer input size (buffer full stdin reads, do line parsing
    // ourselves)
    // queue buffer size for mpsc queues
    /// Print debug information to stderr.
    #[structopt(short, long)]
    verbose: Option<bool>,
}

fn main() {
    let opt = Opt::from_args();
    let verbose = opt.verbose.unwrap_or(false);

    let children: Vec<_> = (0..rayon::current_num_threads())
        .map(|i| {
            Command::new("/bin/bash")
                .arg("-c")
                .arg(&opt.cmd)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap_or_else(|err| panic!("error spawn child {}: {}", i, err))
        })
        .collect();

    let (txs, rxs): (Vec<_>, Vec<_>) = (0..children.len())
        .map(|_| sync_channel(opt.queuesize.unwrap_or(16 * 1024)))
        .unzip();

    let reader_queue_max_size = 16;
    let (read_tx, read_rx) = sync_channel(reader_queue_max_size);

    let reader_queue_size = Arc::new(AtomicU32::new(0));
    let reader_rqs = Arc::clone(&reader_queue_size);
    let reader = thread::spawn(move || {
        let mut done = false;
        let mut total_queue_len = 0;
        let mut num_enqueues = 0;
        while !done {
            let bufsize = opt.bufsize.unwrap_or(16 * 1024);
            let mut buf = Vec::with_capacity(bufsize);
            let mut lines = Vec::with_capacity(bufsize / 8);
            // keep reading up until 5x the average line size so far
            // for the most recent block
            // to minimize reallocations
            // buf.len() < bufsize - 5 * avg
            // avg == buf.len() / lines.len()
            // <=> lines.len() * buf.len() <= bufsize * lines.len() - 5 * buf.len()
            while buf.len() * lines.len() <= bufsize * lines.len() - 5 * buf.len() {
                let bytes = io::stdin()
                    .lock()
                    .read_until(b'\n', &mut buf)
                    .expect("read");
                if bytes == 0 {
                    done = true;
                    break;
                }
                buf.pop();
                lines.push(buf.len());
            }
            read_tx.send((buf, lines)).expect("send");
            total_queue_len += reader_rqs.fetch_add(1, Ordering::Relaxed);
            num_enqueues += 1;
        }
        // read tx dropped here, hanging up send
        drop(read_tx);
        if verbose {
            eprintln!(
                "avg queue len, rounding up {} (max {})",
                (total_queue_len + num_enqueues - 1) / num_enqueues,
                reader_queue_max_size
            );
        }
    });

    let writer_rqs = Arc::clone(&reader_queue_size);
    let writer = thread::spawn(move || {
        while let Ok((buf, lines)) = read_rx.recv() {
            writer_rqs.fetch_sub(1, Ordering::Relaxed);
            let mut start = 0;
            for end in lines {
                let line = &buf[start..end];
                let key = hash_key(line);
                let send_ix = key % txs.len();
                start = end;
                txs[send_ix].send(line.to_vec()).expect("send");
            }
        }
        // txs dropped here, hanging up the send channel
        drop(txs)
    });
    // rxs valid until txs dropped, since they loop until err

    let handles: Vec<_> = children
        .into_iter()
        .zip(rxs.into_iter())
        .map(|(mut child, rx)| {
            thread::spawn(move || {
                let mut child_stdin = child.stdin.take().expect("child stdin");
                while let Ok(line) = rx.recv() {
                    child_stdin.write_all(&line).expect("write line");
                    child_stdin.write(b"\n").expect("write newline");
                }
                drop(child_stdin);
                let child_stdout = child.stdout.take().expect("child_stdout");
                let child_stdout = BufReader::new(child_stdout);
                let stdout = io::stdout();
                let mut handle = stdout.lock();
                child_stdout
                    .for_byte_line_with_terminator(move |line: &[u8]| {
                        handle.write_all(line).map(|_| true)
                    })
                    .expect("write");
            })
        })
        .collect();

    reader.join().expect("reader join");
    writer.join().expect("writer join");
    handles
        .into_iter()
        .for_each(|handle| handle.join().expect("join"));
}

fn hash_key(bytes: &[u8]) -> usize {
    let end = memchr(b' ', bytes).unwrap_or(bytes.len());
    // consider faster hasher?
    let mut hasher = DefaultHasher::default();
    bytes[..end].hash(&mut hasher);
    hasher.finish().try_into().expect("u64 to usize")
}
