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
    use crate::{
        Client, HybridFile, HybridRead, PendingIo, UringCfg,
        client::pending_io::{PendingIoDebuggingEvent, uring::read_into::UringReadIntoAt},
        default_client,
    };
    use std::{io::pipe, os::fd::AsFd, sync::Arc, time::Duration, u64};
    use tokio::runtime::{Handle, RuntimeFlavor};
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
                let mut pending_io = pipe_read_fd.hybrid_read(&mut buf).await.map(|_| {
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
                let mut pending_io = pipe_read_fd.hybrid_read(&mut buf).await.map(|_| {
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
                let mut pending_io = pipe_read_fd.hybrid_read(&mut buf).await.map(|_| {
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

    #[tokio::test]
    #[test_log::test]
    async fn uring_future_should_be_able_to_drop_on_single_thread_runtime() {
        let handle = Handle::current();
        assert!(handle.runtime_flavor() == RuntimeFlavor::CurrentThread);
        let client = default_client();
        if !client.is_uring_available_and_active() {
            println!("uring is not available, skipping test");
        }
        let (pipe_read, _pipe_write) = pipe().expect("should be able to create a pipe");
        let mut buf = [0u8; 64];
        let pending_io = PendingIo::new(
            UringReadIntoAt::new(
                client.uring.as_ref().unwrap(),
                &pipe_read,
                buf.as_mut_slice(),
                u64::MAX,
                None,
            )
            .await,
        );
        drop(pending_io);
    }

    #[tokio::test(
        // Multithreading is needed because the pending I/O operation will block until a task is freed (it has to be acknowledged first).
        flavor = "multi_thread", worker_threads = 2
    )]
    #[test_log::test]
    async fn tiny_uring_client_should_have_dedicated_cancel_queue() {
        // This test case prevents scenarios where the submission queue is filled with normal operations that cannot progress, but there are no dedicated slots for cancel operations, leading to a deadlock.
        let client = Arc::new(
            Client::build(UringCfg {
                operation_queue_size: 1,
                cancel_queue_size: 1,
                ..Default::default()
            })
            .expect("failed to build client"),
        );
        if !client.is_uring_available_and_active() {
            log::warn!("uring is not available, skipping test");
            return;
        }
        let (first_ack_tx, mut first_ack_rx) = tokio::sync::mpsc::unbounded_channel();
        let (second_wait_tx, mut second_wait_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_1 = tokio::task::spawn({
            let client = client.clone();
            async move {
                let (reader_1, _writer_1) = pipe().expect("failed to create pipe");
                let mut buf_1 = [0u8; 64];
                let pending_io = PendingIo::new(
                    UringReadIntoAt::new(
                        client.uring.as_ref().unwrap(),
                        &reader_1,
                        buf_1.as_mut_slice(),
                        u64::MAX,
                        Some(first_ack_tx),
                    )
                    .await,
                );
                let event = second_wait_rx
                    .recv()
                    .await
                    .expect("failed to receive from blocking channel");
                assert_eq!(event, PendingIoDebuggingEvent::NeedWaitForSubmissionTicket);
                let _ = pending_io.cancel().await;
            }
        });
        let task_2 = tokio::task::spawn({
            let client = client.clone();
            async move {
                let (reader_2, _writer_2) = pipe().expect("failed to create pipe");
                let mut buf_2 = [0u8; 64];
                // The first operation must be running first.
                let event = first_ack_rx
                    .recv()
                    .await
                    .expect("failed to receive from first ack channel - required to guarantee that the following operation will be stuck");
                assert_eq!(event, PendingIoDebuggingEvent::SubmissionTicketGranted);
                // The backpressure thread for this operation will block, cancelling the previous pending I/O
                // will unblock the backpressure thread.
                let pending_io = PendingIo::new(
                    UringReadIntoAt::new(
                        client.uring.as_ref().unwrap(),
                        &reader_2,
                        buf_2.as_mut_slice(),
                        u64::MAX,
                        Some(second_wait_tx),
                    )
                    .await,
                );
                let _ = pending_io.cancel().await;
            }
        });

        task_1.await.expect("task 1 should not panic");
        task_2.await.expect("task 2 should not panic");
    }
}
