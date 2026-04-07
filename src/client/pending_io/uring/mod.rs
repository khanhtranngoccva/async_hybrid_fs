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

#[cfg(test)]
mod tests {
    use crate::{HybridFile, HybridRead, default_client};
    use std::{io::pipe, os::fd::AsFd, time::Duration};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    #[test_log::test]
    async fn uring_future_should_multiplex_with_cancel_token() {
        if !default_client().is_uring_available_and_active() {
            println!("uring is not available, skipping test");
        }
        let (tx, rx) = oneshot::channel::<()>();
        let cancellation_token = CancellationToken::new();
        let join_handle = tokio::task::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                let (pipe_read, _pipe_write) = pipe().expect("should be able to create a pipe");
                let mut buf = [0; 64];
                let mut pipe_read_fd = pipe_read.as_fd();
                let mut pending_io = pipe_read_fd.hybrid_read(&mut buf).map(|_| {
                    let _ = tx.send(());
                });
                let future = pending_io
                    .completion()
                    .expect("future should not be cancelled");
                tokio::select! {
                    _ = future => {
                        panic!("future should not be completable because the pipe writer is not used")
                    },
                    _ = cancellation_token.cancelled() => {
                        log::info!("cancellation token cancelled");
                        assert!(
                            pending_io.cancel().await.is_none(),
                            "pipe operation should be cancellable because the writer has not sent anything"
                        );
                    }
                };
            }
        });
        cancellation_token.cancel();
        join_handle.await.expect("task should not panic");
        rx.recv().expect_err(
            "should not be able to receive a message because the processor code should not run",
        );
    }

    #[tokio::test]
    #[test_log::test]
    async fn uring_future_should_multiplex_with_cancel_token_on_nonblocking_fd() {
        if !default_client().is_uring_available_and_active() {
            println!("uring is not available, skipping test");
        }
        let (tx, rx) = oneshot::channel::<()>();
        let cancellation_token = CancellationToken::new();
        let join_handle = tokio::task::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                let (mut pipe_read, _pipe_write) = pipe().expect("should be able to create a pipe");
                pipe_read
                    .hybrid_set_nonblocking(true)
                    .await
                    .expect("should be able to set nonblocking");
                let mut buf = [0; 64];
                let mut pipe_read_fd = pipe_read.as_fd();
                let mut pending_io = pipe_read_fd.hybrid_read(&mut buf).map(|_| {
                    let _ = tx.send(());
                });
                let future = pending_io
                    .completion()
                    .expect("future should not be cancelled");
                tokio::select! {
                    _ = future => {
                        panic!("future should not be completable because the pipe writer is not used")
                    },
                    _ = cancellation_token.cancelled() => {
                        log::info!("cancellation token cancelled");
                        assert!(
                            pending_io.cancel().await.is_none(),
                            "pipe operation should be cancellable because the writer has not sent anything"
                        );
                    }
                };
            }
        });
        cancellation_token.cancel();
        join_handle.await.expect("task should not panic");
        rx.recv().expect_err(
            "should not be able to receive a message because the processor code should not run",
        );
    }

    #[tokio::test]
    #[test_log::test]
    async fn uring_future_should_multiplex_with_timeout() {
        if !default_client().is_uring_available_and_active() {
            println!("uring is not available, skipping test");
        }
        let (tx, rx) = oneshot::channel::<()>();
        let join_handle = tokio::task::spawn({
            async move {
                let (pipe_read, _pipe_write) = pipe().expect("should be able to create a pipe");
                let mut buf = [0; 64];
                let mut pipe_read_fd = pipe_read.as_fd();
                let mut pending_io = pipe_read_fd.hybrid_read(&mut buf).map(|_| {
                    let _ = tx.send(());
                });
                let future = pending_io
                    .completion()
                    .expect("future should not be cancelled");
                let timeout = tokio::time::sleep(Duration::from_secs_f64(0.5));
                tokio::select! {
                    _ = future => {
                        panic!("future should not be completable because the pipe writer is not used")
                    },
                    _ = timeout => {
                        log::info!("successfully timed out");
                        assert!(
                            pending_io.cancel().await.is_none(),
                            "pipe operation should be cancellable because the writer has not sent anything"
                        );
                    }
                };
            }
        });
        join_handle.await.expect("task should not panic");
        rx.recv().expect_err(
            "should not be able to receive a message because the processor code should not run",
        );
    }
}
