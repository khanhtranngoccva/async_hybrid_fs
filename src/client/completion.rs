use crate::{
    client::command::{Command, CommandWithTicket},
    metadata::Metadata,
};
use std::{
    io,
    os::fd::{FromRawFd, OwnedFd},
};

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

pub(crate) fn handle_completion(cmd: CommandWithTicket, result: i32) {
    let result: io::Result<i32> = if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result)
    };
    match cmd.command {
        Command::Read { res, .. } => {
            let _ = res.send(result.map(|n| n as u32));
        }
        Command::Readv { res, .. } => {
            let _ = res.send(result.map(|n| n as u32));
        }
        Command::Write { res, .. } => {
            let _ = res.send(result.map(|n| n as u32));
        }
        Command::Writev { res, .. } => {
            let _ = res.send(result.map(|n| n as u32));
        }
        Command::Sync { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
        Command::Statx { req, res, .. } => {
            let outcome = result.map(|_| {
                // SAFETY: The kernel has initialized the statx buffer
                let statx = unsafe { (*req.statx_buf).assume_init() };
                Metadata(statx)
            });
            let _ = res.send(outcome);
        }
        Command::Fallocate { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
        Command::Fadvise { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
        Command::Ftruncate { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
        Command::OpenAt { res, .. } => {
            let outcome = result.map(|fd| {
                // SAFETY: The kernel returns a valid fd on success
                unsafe { OwnedFd::from_raw_fd(fd) }
            });
            let _ = res.send(outcome);
        }
        Command::StatxPath { req, res, .. } => {
            let outcome = result.map(|_| {
                // SAFETY: The kernel has initialized the statx buffer
                let statx = unsafe { (*req.statx_buf).assume_init() };
                Metadata(statx)
            });
            let _ = res.send(outcome);
        }
        Command::Close { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
        Command::RenameAt { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
        Command::UnlinkAt { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
        Command::MkdirAt { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
        Command::SymlinkAt { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
        Command::LinkAt { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
        Command::Cancel { res, .. } => {
            let _ = res.send(result.map(|_| ()));
        }
    }
}
