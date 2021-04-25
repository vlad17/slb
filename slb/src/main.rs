//! `slb` main executable

use std::path::PathBuf;
use std::ops::Deref;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::thread;

use bstr::io::BufReadExt;
use structopt::StructOpt;

use slb::{fileblocks,sharder};

/// Performs sharded load balancing on stdin, handing off input
/// to child processes based on a hash of the first word on each line.
///
/// E.g., suppose we have a file with contents like
///
/// ```
/// a b c d
/// e f g h
/// a b
/// ```
///
/// Every line is handed to stdin of the the [`mapper`] processes,
/// which are really "flat maps": they can generate multiple output
/// lines per input line, but should be pure functions.
///
/// The above might generate input that looks like
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
/// <file outprefix0.txt>
/// key2  e f g h
///
/// <file outprefix1.txt>
/// key1  a b c d a b
/// ```
#[derive(Debug, StructOpt)]
#[structopt(name = "slb", about = "Performs streaming load balancing.")]
struct Opt {

    /// A flat-map pure function, which is a bash command line string
    /// that performs line-by-line operations on the input to emit
    /// output lines for reduction.
    ///
    /// By default, this is just the identity, equivalent to `cat`.
    #[structopt(long)]
    mapper: Option<String>,
    
    /// The folder function.
    ///
    /// Multiple instances of this same process are created with the same
    /// command-line string. Text lines from mapper output are fed
    /// into these processes, and stdout is shared between this parent
    /// process and its children, but collated.
    #[structopt(long)]
    folder: String,

    /// The input file to read lines from.
    #[structopt(long)]
    infile: PathBuf,

    /// Output file prefixes.
    #[structopt(long)]
    outprefix: PathBuf,

    /// Buffer size in KB for reading chunks of input, parameter
    /// shared between both mapper and folder right now.
    ///
    /// Note memory usage is O(bufsize * nthreads)
    #[structopt(long)]
    bufsize: Option<usize>,
    
    // TODO consider sort-like KEYDEF -k --key which wouldn't hash if n (numeric) flag set

    /// Print debug information to stderr.
    #[structopt(long)]
    verbose: Option<bool>,

    /// Number of parallel threads per stage to launch: the total
    /// number of live threads could be a multiple of this becuase
    /// of concurrent mapper and folder stages.
    ///
    /// Defaults to num CPUs.
    #[structopt(long)]
    nthreads: Option<usize>,
    
}

fn main() {
    let opt = Opt::from_args();
    let verbose = opt.verbose.unwrap_or(false);
    let nthreads = opt.nthreads.unwrap_or(num_cpus::get_physical());
    let mapper_cmd = opt.mapper.as_deref().unwrap_or("cat");
    let folder_cmd = &opt.folder;
    let bufsize = opt.bufsize.unwrap_or(16) * 1024;
    let queuesize = 256;

    // TODO: could play with mapper/folder parallelism, bufsize,
    // and queuesize being tuned. Ideally could do tuning automatically.

    // TODO: very crude chunking below probably hurts in the presence of stragglers,
    // finer grained work-stealing would be ideal.

    // Allow enough chunks for parallelism but not so few the chunksize
    // is small.
    let chunks = fileblocks::chunkify(&opt.infile, nthreads, bufsize);
    let nthreads = chunks.len(); // smaller b/c of min bufsize
    assert!(nthreads >= 1);

    let mut mapper_processes: Vec<_> = (0..nthreads)
        .map(|i| {
            Command::new("/bin/bash")
                .arg("-c")
                .arg(mapper_cmd)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap_or_else(|err| panic!("error spawn map child {}: {}", i, err))
        })
        .collect();

    let (mapper_inputs, mapper_outputs): (Vec<_>, Vec<_>) = mapper_processes
        .iter_mut()
        .map(|child| {
            let stdin = child.stdin.take().unwrap();
            let stdout = child.stdout.take().unwrap();
            (stdin, stdout)
        })
        .unzip();

    let mapper_input_threads: Vec<_> = mapper_inputs
        .into_iter()
        .zip(chunks.into_iter())
        .map(|(input, chunk)| {
            thread::spawn(move || {
                chunk.dump(input)
            })
        }).collect();

    // TODO: wrap these with atomic ints to log capacity / P(capacity=max)
    let (txs, rxs): (Vec<_>, Vec<_>) = (0..nthreads)
        .map(|_| sync_channel(queuesize))
        .unzip();

    let txs_ref = Arc::new(txs);
    let mapper_output_threads: Vec<_> = mapper_outputs
        .into_iter()
        .map(|output| {
            let txs_ref_clone = Arc::clone(&txs_ref);
            thread::spawn(move || {
                let output = BufReader::new(output);
                let txs_ref_local = txs_ref_clone.deref();
                sharder::shard(output, nthreads, bufsize, |ix, buf| {
                    txs_ref_local[ix].send(buf).expect("send");
                })
            })
        })
        .collect();

    let folder_processes: Vec<_> = (0..nthreads)
        .map(|i| {
            Command::new("/bin/bash")
                .arg("-c")
                .arg(folder_cmd)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap_or_else(|err| panic!("error spawn fold child {}: {}", i, err))
        })
        .collect();

    // must be both I/O thread to manage livelock from stdin EOF
    // expectation of folder procs
    let folder_input_output_threads: Vec<_> = folder_processes.into_iter()
        .zip(rxs.into_iter())
        .enumerate()
        .map(|(i, (mut child, rx))| {
            let outprefix = opt.outprefix.clone();
            thread::spawn(move || {
                let mut child_stdin = child.stdin.take().expect("child stdin");
                while let Ok(lines) = rx.recv() {
                    child_stdin.write_all(&lines).expect("write lines");
                }
                drop(child_stdin);
                
                let child_stdout = child.stdout.take().expect("child_stdout");
                let child_stdout = BufReader::new(child_stdout);

                let width = format!("{}", nthreads-1).len();
                let suffix = format!("{:0>width$}", i, width=width);
                let mut fname = outprefix.file_name().expect("file name").to_owned();
                fname.push(&suffix);
                let path = outprefix.with_file_name(fname);
                let file = File::create(&path).expect("write file");
                let mut file = BufWriter::new(file);
                child_stdout
                    .for_byte_line_with_terminator(|line: &[u8]| {
                        file.write_all(line).map(|_| true)
                    })
                    .expect("write");
                assert!(child.wait().expect("wait").success());
                file.flush().expect("flush");
            })
        })
        .collect();

    mapper_input_threads
        .into_iter()
        .for_each(|handle| handle.join().expect("map input join"));
    mapper_processes
        .into_iter()
        .for_each(|mut child| assert!(child.wait().expect("wait").success()));
    mapper_output_threads
        .into_iter()
        .for_each(|handle| handle.join().expect("map output join"));

    let txs = Arc::try_unwrap(txs_ref)
        .expect("final reference");        
    drop(txs); // ensure hangup of transmission channel
    
    // Closures here own the fold processes
    folder_input_output_threads
        .into_iter()
        .for_each(|handle| handle.join().expect("fold join"));
}

