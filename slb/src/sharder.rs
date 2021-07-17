//! Shard by first key into buffers.

use std::collections::hash_map::DefaultHasher;
use std::convert::TryInto;
use std::hash::{Hash, Hasher};
use std::io::BufRead;
use std::mem;

use bstr::io::BufReadExt;
use memchr::memchr;

/// Reads from `r` until EOF, calling `f` occasionally with
/// the arguments `(index, buffer)` where `index` is the index
/// of the partition that the hash key (first word of each line)
/// falls into and `buffer` is a byte buffer of newline-terminated byte
/// lines (there could be multiple, but each line starts with a key in that
/// hash space partition).
///
/// `bufsize` is the size of each buffer per partition before flush.
pub fn shard<R, F>(r: R, npartitions: usize, bufsize: usize, mut f: F)
where
    R: BufRead,
    F: FnMut(usize, Vec<u8>),
{
    // TODO: currently memory usage here is O(bufsize * npartitions^2)
    // we should still buffer as much as we can but keep global usage
    // below some new `bufsize` given from command line.
    //
    // Can experiment with flushing only half the used bytes, prioritizing
    // minimizing sends (so sending the largest bufs first).
    let mut bufs = vec![Vec::with_capacity(bufsize * 2); npartitions];
    let npartitions: u64 = npartitions.try_into().unwrap();
    r.for_byte_line_with_terminator(|line| {
        let key = hash_key(line, npartitions);
        bufs[key].extend_from_slice(line);
        if bufs[key].len() >= bufsize {
            f(key, mem::take(&mut bufs[key]));
            bufs[key].reserve(bufsize * 2);
        }
        Ok(true)
    })
    .expect("successful byte line read");
    for (i, buf) in bufs.into_iter().enumerate() {
        f(i, buf)
    }
}

fn hash_key(bytes: &[u8], npartitions: u64) -> usize {
    let end = memchr(b' ', bytes).unwrap_or(bytes.len());
    // TODO: consider faster hasher?
    let mut hasher = DefaultHasher::default();
    bytes[..end].hash(&mut hasher);
    (hasher.finish() % npartitions) as usize
}
