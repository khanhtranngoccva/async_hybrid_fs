#[cfg(target_os = "linux")]
use crate::client::pending_io::uring::UringPendingIoFiller;
use crate::{IoBuf, IoBufMut};
use std::cmp::min;

/// Result of a read operation: the buffer and actual bytes read.
pub struct ReadResult<B> {
    /// The buffer containing the data read.
    pub buf: B,
    /// Number of bytes actually read (may be less than buffer capacity at EOF). Limited to ~2GB per operation.
    pub bytes_read: usize,
}

impl<B> ReadResult<B>
where
    B: IoBufMut,
{
    /// Create a ReadResult object and forcibly resize the buffer if possible
    ///
    /// # Safety
    /// The caller must ensure that the buffer reports the correct capacity.
    pub(crate) unsafe fn new(mut buf: B, bytes_read: usize) -> Self {
        unsafe { buf.set_len(bytes_read) };
        Self { buf, bytes_read }
    }
}

/// Result of a readv operation: the buffers and actual bytes read.
pub struct ReadvResult<B> {
    /// The buffers containing the data read, in the same order as they were passed to the operation. They are filled from first to last.
    pub bufs: Vec<B>,
    /// Number of bytes actually read (may be less than buffer capacity at EOF). Limited to ~2GB per operation.
    pub bytes_read: usize,
}

impl<B> ReadvResult<B>
where
    B: IoBufMut,
{
    /// Create a ReadvResult object and forcibly resize the buffer if possible
    ///
    /// # Safety
    /// The caller must ensure that the buffers report the correct capacity.
    pub(crate) unsafe fn new(mut bufs: Vec<B>, bytes_read: usize) -> Self {
        let mut cur_bytes_read = bytes_read;
        for buf in bufs.iter_mut() {
            let bytes_read_into_target = min(buf.capacity(), cur_bytes_read);
            unsafe {
                buf.set_len(bytes_read_into_target);
            }
            cur_bytes_read -= bytes_read_into_target;
        }
        ReadvResult { bufs, bytes_read }
    }
}

/// Result of a write operation: the buffer and actual bytes written.
pub struct WriteResult<B> {
    /// The original buffer (returned for reuse).
    pub buf: B,
    /// Number of bytes actually written (may be less than buffer size for non-regular files). Limited to ~2GB per operation.
    pub bytes_written: usize,
}

impl<B> WriteResult<B>
where
    B: IoBuf,
{
    /// Create a WriteResult object
    pub(crate) fn new(buf: B, bytes_written: usize) -> Self {
        WriteResult { buf, bytes_written }
    }
}

/// Result of a writev operation: the buffers and actual bytes written.
pub struct WritevResult<B> {
    /// The original buffers (returned for reuse), in the same order as they were passed to the operation. They are written from first to last.
    pub bufs: Vec<B>,
    /// Number of bytes actually written (may be less than buffer size for non-regular files). Limited to ~2GB per operation.
    pub bytes_written: usize,
}

impl<B> WritevResult<B>
where
    B: IoBuf,
{
    /// Create a WritevResult object
    pub(crate) fn new(bufs: Vec<B>, bytes_written: usize) -> Self {
        WritevResult {
            bufs,
            bytes_written,
        }
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn handle_completion(filler: UringPendingIoFiller, result: i32) {
    use std::io;
    let result: io::Result<i32> = if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result)
    };
    filler.complete(result);
}
