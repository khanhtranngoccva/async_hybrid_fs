pub(crate) mod close;
pub(crate) mod fadvise;
pub(crate) mod fallocate;
pub(crate) mod ftruncate;
pub(crate) mod link_at;
mod macros;
pub(crate) mod mkdir_at;
pub(crate) mod open_at;
pub(crate) mod read_into;
pub(crate) mod read_into_vectored;
pub(crate) mod rename_at;
pub(crate) mod statx;
pub(crate) mod statx_path;
pub(crate) mod symlink_at;
pub(crate) mod sync;
pub(crate) mod unlink_at;
pub(crate) mod write_from;
pub(crate) mod write_from_vectored;

use super::PendingIoImpl;
use crate::client::command::Command;

/// Trait for cancellable pending io operations done via io_uring.
// "Send" is required for compatibility with [`async_trait`] and any application crates using [`async_trait`].
pub(crate) trait UringPendingIo<T>: PendingIoImpl<T> {
    /// Create a [`Command`] to send to the submission queue.
    ///
    /// SAFETY: Note that the created [`Command`] must always be sent to the queue to avoid resource leak
    /// and to make cancellation possible.
    ///
    /// The command must also not outlive the [`UringPendingIo`] structure.
    unsafe fn build_command(&mut self) -> Command;
}
