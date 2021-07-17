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
    let mut used_space = 0;
    let mut bufs = vec![Vec::new(); npartitions];
    let npartitions: u64 = npartitions.try_into().unwrap();
    r.for_byte_line_with_terminator(|line| {
        let key = hash_key(line, npartitions);
        used_space += line.len();
        bufs[key].extend_from_slice(line);
        if used_space >= bufsize {
            // You might be tempted to ask, why not just send the largest
            // few buffers to avoid communication overhead? It turns out
            // this really does not help, at least if we can view
            // line sizes as constant (or with standard deviation much
            // smaller than `bufsize`).
            //
            // The size of the largest bucket of a hash table with n keys
            // is lg(n) on average (up to lg(lg(n)) factors). So flushing
            // the top-k largest buffers at most gets rid of about k*lg(n)
            // keys. With k set to asymptotically anything less than n
            // (up to lg(n) factors), we'd be increasing the net number
            // of flushes (calls to f) we perform.
            //
            // Thus, we may as well flush every buffer.
            for (i, buf) in bufs.iter_mut().enumerate() {
                if buf.len() > 0 {
                    f(i, mem::take(buf));
                }
            }
            used_space = 0;
        }
        Ok(true)
    })
    .expect("successful byte line read");
    for (i, buf) in bufs.into_iter().enumerate() {
        if buf.len() > 0 {
            f(i, buf)
        }
    }
}

fn hash_key(bytes: &[u8], npartitions: u64) -> usize {
    let end = memchr(b' ', bytes).unwrap_or(bytes.len());
    // TODO: consider faster hasher?
    let mut hasher = DefaultHasher::default();
    bytes[..end].hash(&mut hasher);
    (hasher.finish() % npartitions) as usize
}
