//! `slb` main executable

use std::fs::File;
use std::io::{BufReader, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use std::sync::mpsc::{sync_channel, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;

use structopt::StructOpt;

use slb::{fileblocks, sharder};

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

    /// The input files to read lines from.
    #[structopt(long)]
    infile: Vec<PathBuf>,

    /// Output file prefixes.
    #[structopt(long)]
    outprefix: PathBuf,

    /// Buffer size in KB for buffering output before it's sent to
    /// folders from a mapper.
    ///
    /// Note memory usage is O(bufsize * nthreads).
    #[structopt(long)]
    bufsize: Option<usize>,

    // TODO: consider sort-like KEYDEF -k --key which wouldn't hash if n (numeric) flag set
    /// Print debug information to stderr.
    #[structopt(long)]
    verbose: bool,

    // TODO: this isn't very useful as an option, consider removing entirely
    // or allowing a max_mappers and max_folders which controls maximum
    // concurrency
    /// Approximate target number of parallel threads per stage to launch:
    /// the total number of live threads could be a multiple of this becuase
    /// of concurrent mapper and folder stages.
    ///
    /// Defaults to num CPUs.
    #[structopt(long)]
    nthreads: Option<usize>,
}

fn main() {
    let opt = Opt::from_args();
    let verbose = opt.verbose;
    let nthreads = opt.nthreads.unwrap_or(num_cpus::get_physical());
    let mapper_cmd = opt.mapper.as_deref().unwrap_or("cat");
    let folder_cmd = &opt.folder;
    let bufsize = opt.bufsize.unwrap_or(64) * 1024;
    let queuesize = 256;

    assert!(!opt.infile.is_empty());
    // TODO: Assume bufsize is fixed due to memory constraints.
    //
    // We could play with queuesize and mapper:folder ratio tuning.
    // For map-constrained tasks, reducing folders past 1:1 ratio
    // probably doesn't help since folders sitting idle don't hurt anyone.
    // However, for fold-constrained tasks lower mapper ratios like 1:2, 1:4,
    // and etc. are interesting since memory usage and block time could be
    // reduced after dynamic tuning. Then for a given ideal mapper:folder
    // ratio, which could be derived with Little's law, and a given
    // variance in mapper speed (normalized by reducer speed), after
    // assuming the hash is uniform, one can compute variance in queue
    // lengths given a Poisson process setup. This means that computing
    // statistics about mapper/folder speeds is enough to back out
    // the ideal mapper:folder ratio and queue size (queue size chosen such
    // that blocking is avoided with 99% probability at any fixed steady-state
    // time).
    //
    // Above approach can work simply assuming mapper/folder speeds are constant
    // over time quanta holding past trends. Otherwise, a control theory
    // approach could be used.
    //
    // This would be fun to investigate more deeply, but I have yet to encounter
    // a folder-constrained task IRL to test this on.

    // Allow enough chunks for parallelism but not so few the chunksize
    // is small.
    let read_chunk_size = 16 * 1024;
    let chunks = fileblocks::chunkify_multiple(&opt.infile, nthreads, read_chunk_size);
    let nthreads = chunks.len(); // smaller b/c of min bufsize
    assert!(nthreads >= 1);

    let mut mapper_processes: Vec<_> = chunks
        .iter()
        .enumerate()
        .map(|(i, chunk)| {
            Command::new("/bin/bash")
                .arg("-c")
                .arg(format!(
                    "/bin/bash -c 'head -c {} | {}'",
                    chunk.nbytes(),
                    mapper_cmd
                ))
                .stdin(chunk.file())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap_or_else(|err| panic!("error spawn map child {}: {}", i, err))
        })
        .collect();

    let mapper_outputs: Vec<_> = mapper_processes
        .iter_mut()
        .map(|child| child.stdout.take().unwrap())
        .collect();

    let (txs, rxs): (Vec<_>, Vec<_>) = (0..nthreads).map(|_| sync_channel(queuesize)).unzip();
    let lines_sent = vec![0usize; nthreads];
    let lines_blocking = vec![0usize; nthreads];
    let stats = Arc::new(Mutex::new((lines_sent, lines_blocking)));

    let txs_ref = Arc::new(txs);
    let mapper_output_threads: Vec<_> = mapper_outputs
        .into_iter()
        .map(|output| {
            let txs_ref_clone = Arc::clone(&txs_ref);
            let stats = Arc::clone(&stats);
            thread::spawn(move || {
                let output = BufReader::new(output);
                let txs_ref_local = txs_ref_clone.deref();
                let mut lines_sent = vec![0usize; nthreads];
                let mut lines_blocking = vec![0usize; nthreads];
                sharder::shard(output, nthreads, bufsize, |ix, buf| {
                    lines_sent[ix] += 1;
                    if let Err(TrySendError::Full(buf)) = txs_ref_local[ix].try_send(buf) {
                        lines_blocking[ix] += 1;
                        txs_ref_local[ix].send(buf).expect("send");
                    }
                });
                let mut guard = stats.lock().unwrap();
                for i in 0..nthreads {
                    let ref mut sends = guard.0;
                    sends[i] += lines_sent[i];
                    let ref mut blocks = guard.1;
                    blocks[i] += lines_blocking[i];
                }
            })
        })
        .collect();

    let folder_processes: Vec<_> = (0..nthreads)
        .map(|i| {
            let outprefix = opt.outprefix.clone();
            let width = format!("{}", nthreads - 1).len();
            let suffix = format!("{:0>width$}", i, width = width);
            let mut fname = outprefix.file_name().expect("file name").to_owned();
            fname.push(&suffix);
            let path = outprefix.with_file_name(fname);
            let file = File::create(&path).expect("write file");

            Command::new("/bin/bash")
                .arg("-c")
                .arg(folder_cmd)
                .stdin(Stdio::piped())
                .stdout(file)
                .spawn()
                .unwrap_or_else(|err| panic!("error spawn fold child {}: {}", i, err))
        })
        .collect();

    // must be both I/O thread to manage livelock from stdin EOF
    // expectation of folder procs
    let folder_input_output_threads: Vec<_> = folder_processes
        .into_iter()
        .zip(rxs.into_iter())
        .map(|(mut child, rx)| {
            thread::spawn(move || {
                let mut child_stdin = child.stdin.take().expect("child stdin");
                while let Ok(lines) = rx.recv() {
                    child_stdin.write_all(&lines).expect("write lines");
                }
                drop(child_stdin);

                assert!(child.wait().expect("wait").success());
            })
        })
        .collect();

    mapper_processes
        .into_iter()
        .for_each(|mut child| assert!(child.wait().expect("wait").success()));
    mapper_output_threads
        .into_iter()
        .for_each(|handle| handle.join().expect("map output join"));

    let txs = Arc::try_unwrap(txs_ref).expect("final reference");
    drop(txs); // ensure hangup of transmission channel

    // Closures here own the fold processes
    folder_input_output_threads
        .into_iter()
        .for_each(|handle| handle.join().expect("fold join"));

    let stats = Arc::try_unwrap(stats).expect("final reference");
    let (lines_sent, lines_blocking) = stats.into_inner().unwrap();
    if verbose {
        println!("sent {:?}\nblock {:?}", lines_sent, lines_blocking);
    }
}
