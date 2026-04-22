use crate::client::pending_io::uring::UringPendingIoFiller;
use std::io;

/// Result of a read operation: the buffer and actual bytes read.
pub struct ReadResult<B> {
    /// The buffer containing the data read.
    pub buf: B,
    /// Number of bytes actually read (may be less than buffer capacity at EOF). Limited to ~2GB per operation.
    pub bytes_read: usize,
}

/// Result of a readv operation: the buffers and actual bytes read.
pub struct ReadvResult<B> {
    /// The buffers containing the data read, in the same order as they were passed to the operation. They are filled from first to last.
    pub bufs: Vec<B>,
    /// Number of bytes actually read (may be less than buffer capacity at EOF). Limited to ~2GB per operation.
    pub bytes_read: usize,
}

/// Result of a write operation: the buffer and actual bytes written.
pub struct WriteResult<B> {
    /// The original buffer (returned for reuse).
    pub buf: B,
    /// Number of bytes actually written (may be less than buffer size for non-regular files). Limited to ~2GB per operation.
    pub bytes_written: usize,
}

/// Result of a writev operation: the buffers and actual bytes written.
pub struct WritevResult<B> {
    /// The original buffers (returned for reuse), in the same order as they were passed to the operation. They are written from first to last.
    pub bufs: Vec<B>,
    /// Number of bytes actually written (may be less than buffer size for non-regular files). Limited to ~2GB per operation.
    pub bytes_written: usize,
}

pub(crate) fn handle_completion(filler: UringPendingIoFiller, result: i32) {
    let result: io::Result<i32> = if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result)
    };
    filler.complete(result);
}
