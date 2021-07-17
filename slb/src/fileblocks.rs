//! Utilities for converting files into blocks.

use std::convert::TryInto;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::io::{BufRead, BufReader, ErrorKind};
use std::io::{Seek, SeekFrom};

use std::path::Path;
use std::path::PathBuf;

use bstr::io::BufReadExt;

use memchr;

const BUFFER_SIZE: usize = 16 * 1024;

#[derive(Debug)]
pub struct FileChunk {
    path: PathBuf,
    start: usize,
    stop: usize,
}

impl FileChunk {
    /// Prepare a pre-seeked file for this chunk.
    pub fn file(&self) -> File {
        let mut file = File::open(&self.path).expect("file available");
        file.seek(SeekFrom::Start(self.start.try_into().unwrap()))
            .expect("seek");
        file
    }

    /// Return the number of bytes to read for this chunk.
    pub fn nbytes(&self) -> usize {
        self.stop - self.start
    }

    /// Iterates over just those lines the file chunk refers to.
    pub fn dump<W: Write>(&self, mut w: W) {
        let mut file = File::open(&self.path).expect("file available");
        file.seek(SeekFrom::Start(self.start.try_into().unwrap()))
            .expect("seek");
        let reader = BufReader::with_capacity(BUFFER_SIZE.min(self.stop - self.start), file);
        let mut current_byte = self.start;
        let stop_byte = self.stop;

        reader
            .for_byte_line_with_terminator(|line| {
                if current_byte >= stop_byte {
                    return Ok(false);
                }
                assert!(
                    current_byte < stop_byte + 1,
                    "can only overshoot if non-newline split or eof with no newline"
                );
                current_byte += line.len();
                w.write(line).expect("write");
                Ok(true)
            })
            .expect("read");
    }
}

/// Uses up to `max_chunks + paths.len()` chunks to chunkify multiple files.
pub fn chunkify_multiple(paths: &[PathBuf], max_chunks: usize, min_size: usize) -> Vec<FileChunk> {
    assert!(max_chunks > 0);
    assert!(!paths.is_empty());
    let sizes: Vec<usize> = paths
        .iter()
        .map(|path| {
            fs::metadata(path)
                .expect("metadata")
                .len()
                .try_into()
                .unwrap()
        })
        .collect();
    let avg_size = (sizes.iter().copied().sum::<usize>() + paths.len() - 1) / paths.len();
    paths
        .iter()
        .zip(sizes.into_iter())
        .flat_map(|(path, sz)| {
            let desired_chunks: usize = (sz + avg_size - 1) / avg_size;
            chunkify(&path, desired_chunks, min_size).into_iter()
        })
        .collect()
}

/// Returns a list of up to `max_chunks` file chunks splitting up the given
/// file, roughly of the same size, which should be rougly at least
/// `min_size`, newline aligned.
///
/// Of course, the file is assumed to not be modified between the start
/// of this method and the usage of the corresponding file chunks,
/// else someone will panic.
pub fn chunkify(path: &Path, max_chunks: usize, min_size: usize) -> Vec<FileChunk> {
    assert!(max_chunks > 0);
    let metadata = fs::metadata(path).unwrap();
    let size: usize = metadata.len().try_into().unwrap();
    let max_chunks = max_chunks.min(size / min_size).max(1);

    let mut file = File::open(path).unwrap();
    let mut chunks = Vec::with_capacity(max_chunks);
    let mut current_byte = 0;
    for i in 0..max_chunks {
        let stop = size * (i + 1) / max_chunks;

        // in the rare case when a line takes up a whole block, skip it
        if current_byte >= stop {
            continue;
        }

        file.seek(SeekFrom::Start(stop.try_into().unwrap()))
            .expect("seek");
        let mut reader = BufReader::new(&mut file);
        let stop = stop + read_until(b'\n', &mut reader);

        chunks.push(FileChunk {
            path: path.to_owned(),
            start: current_byte,
            stop,
        });
        current_byte = stop;

        if stop == size {
            break;
        }
    }

    chunks
}

fn read_until<R: BufRead + ?Sized>(delim: u8, r: &mut R) -> usize {
    // from stdlib
    let mut read = 0;
    loop {
        let (done, used) = {
            let available = match r.fill_buf() {
                Ok(n) => n,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                x => x.unwrap(),
            };
            match memchr::memchr(delim, available) {
                Some(i) => (true, i + 1),
                None => (false, available.len()),
            }
        };
        r.consume(used);
        read += used;
        if done || used == 0 {
            return read;
        }
    }
}
