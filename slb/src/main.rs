//! `slb` main executable

use std::collections::hash_map::DefaultHasher;
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, Write};
use std::iter;
use std::process::{Command, Stdio};
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
}

fn main() {
    let opt = Opt::from_args();

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

    rayon::spawn(move || {
        // txs captured by value here
        let mut done = false;
        let txs_ref = &txs;
        // use bstr here --> collect multiple slices into hashes
        // line by line
        // but also try chunking (for_byte_line, collect buffers
        iter::from_fn(move || {
            if done {
                return None;
            }
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
            Some((buf, lines))
        })
        .flat_map(|(buf, lines)| {
            let mut start = 0;
            lines.into_iter().map(move |end| {
                let line = &buf[start..end];
                let key = hash_key(line);
                let send_ix = key % txs_ref.len();
                start = end;
                (send_ix, line.to_vec())
            })
        })
        .for_each(|(send_ix, line)| {
            txs_ref[send_ix].send(line).expect("send");
        });
        // txs dropped here, hanging up the send channel
        drop(txs)
    });
    // rxs valid until txs dropped, since they loop until err

    // instead of a lock here we could just use a channel to pipe stdout lines
    let lock = Arc::new(Mutex::new(()));
    let handles: Vec<_> = children
        .into_iter()
        .zip(rxs.into_iter())
        .map(|(mut child, rx)| {
            let lock = Arc::clone(&lock);
            thread::spawn(move || {
                let mut child_stdin = child.stdin.take().expect("child stdin");
                while let Ok(line) = rx.recv() {
                    child_stdin.write_all(&line).expect("write line");
                    child_stdin.write(b"\n").expect("write newline");
                }
                let _ = lock.lock().expect("lock");
                drop(child_stdin);
                let stdout = child.stdout.take().expect("child_stdout");
                let stdout = BufReader::new(stdout);
                stdout
                    .for_byte_line_with_terminator(|line: &[u8]| {
                        let stdout = io::stdout();
                        let mut handle = stdout.lock();
                        handle.write_all(line).map(|_| true)
                    })
                    .expect("write");
            })
        })
        .collect();
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
