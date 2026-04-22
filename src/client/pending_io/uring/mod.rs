pub(crate) mod close;
pub(crate) mod fadvise;
pub(crate) mod fallocate;
pub(crate) mod ftruncate;
pub(crate) mod link_at;
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

use std::{
    fmt::Debug,
    io,
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
};

use io_uring::squeue;
use parking_lot::{Condvar, Mutex};

use super::PendingIoImpl;
use crate::client::{
    ClientUring,
    ticketing::{SubmissionTicket, SubmissionTicketQueue},
};

// New implementation of pending I/O, universally usable for all operations

/// Status of the operation. If the operation is not submitted, it is trivially cancellable. If the operation is already submitted (which occurs after the point of retrieving the ticket), it must be waited until `Done`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UringPendingIoStatus {
    // Not yet submitted. At this point, the operation has not yet received a submission ticket or appeared in the io_uring submission queue.
    NotSubmitted,
    // Submitting. The operation has received a ticket, and is being staged in the submission thread without risking being indefinitely blocked. An operation will have to wait until it is in the Submitted state (registered by io_uring) to be cancellable.
    Submitting,
    // Submitted. At this point, the operation has received a ticket and is already acknowledged by the kernel.
    Submitted,
    // The operation's filler has received a value.
    Done,
}

/// Structure for describing the state of the operation.
#[derive(Debug)]
pub(crate) struct UringPendingIoState {
    /// The status of the operation.
    status: UringPendingIoStatus,
    /// Flag whether the cancellation has been triggered or not.
    cancel_triggered: bool,
    /// The waker, used as a condition variable for async code.
    waker: Waker,
    /// The result of the operation.
    result: Option<io::Result<i32>>,
}

impl UringPendingIoState {
    fn new() -> Self {
        Self {
            status: UringPendingIoStatus::NotSubmitted,
            cancel_triggered: false,
            waker: Waker::noop().clone(),
            result: None,
        }
    }

    /// Trigger the cancellation of the operation, and returns a cloned reference to the submission ticket to prevent the ticket from being available prematurely. The reaper still requires a reference to live here so that no operation can steal the ticket.
    fn trigger_cancel(&mut self) -> bool {
        if self.status != UringPendingIoStatus::Submitted || self.cancel_triggered {
            return false;
        }
        self.cancel_triggered = true;
        true
    }
}

/// Base structure for a uring pending I/O operation, which specific operations wrap around to interpret its results.
pub(crate) struct UringPendingIoObj<'lifetime> {
    /// The state of the operation. Note that the lock should be held for as briefly as possible.
    state: Arc<Mutex<UringPendingIoState>>,
    transition_cv: Arc<Condvar>,
    /// The anonymous I/O entry to send to the actual io_uring queue. When a ticket is retrieved, a cloned entry is assigned the ID corresponding to the ticket and submitted to the queue. Note that the ID should not be assigned yet.
    entry: squeue::Entry,
    /// The submission ticket. It can only be removed when the operation is done and the filler is filled. When the ticket is acquired, the operation is sent to the submission thread.
    submission_ticket: Option<Arc<SubmissionTicket>>,
    uring: &'lifetime ClientUring,
    ticket_queue: &'lifetime SubmissionTicketQueue,
}

impl<'lifetime> UringPendingIoObj<'lifetime> {
    pub(crate) fn new(uring: &'lifetime ClientUring, entry: squeue::Entry) -> Self {
        let opcode = entry.get_opcode();
        // Cancel opcodes are redirected to the cancel submission ticket queue.
        let ticket_queue = if opcode == (io_uring::opcode::AsyncCancel::CODE as u32)
            || opcode == (io_uring::opcode::AsyncCancel2::CODE as u32)
        {
            &uring.cancel_submission_ticket_queue
        } else {
            &uring.normal_submission_ticket_queue
        };
        Self {
            state: Arc::new(Mutex::new(UringPendingIoState::new())),
            transition_cv: Arc::new(Condvar::new()),
            entry,
            submission_ticket: None,
            uring,
            ticket_queue,
        }
    }

    pub(crate) fn poison(uring: &'lifetime ClientUring) -> Self {
        let entry = io_uring::opcode::Nop::new()
            .build()
            .flags(squeue::Flags::IO_DRAIN);
        Self {
            state: Arc::new(Mutex::new(UringPendingIoState::new())),
            transition_cv: Arc::new(Condvar::new()),
            entry: entry,
            submission_ticket: Some(Arc::new(
                uring.normal_submission_ticket_queue.poison_ticket(),
            )),
            uring,
            ticket_queue: &uring.normal_submission_ticket_queue,
        }
    }

    fn submitter(&self) -> UringPendingIoSubmitter {
        UringPendingIoSubmitter {
            state: self.state.clone(),
            transition_cv: self.transition_cv.clone(),
            entry: self.entry.clone(),
            submission_ticket: self
                .submission_ticket
                .clone()
                .expect("submission ticket should be assigned"),
        }
    }

    fn filler(&self) -> UringPendingIoFiller {
        UringPendingIoFiller {
            state: self.state.clone(),
            transition_cv: self.transition_cv.clone(),
        }
    }

    fn submit(&mut self, ticket: Arc<SubmissionTicket>) -> () {
        let ticket_id = ticket.id();
        self.submission_ticket = Some(ticket);
        let submitter = self.submitter();
        let filler = self.filler();
        // Insert the filler into the pending map.
        let preexisting_key = self.uring.pending.insert(ticket_id, filler);
        assert!(
            preexisting_key.is_none(),
            "ticket id {} already exists in pending map",
            ticket_id.0
        );
        // Send the submitter to the submission thread.
        self.uring
            .submission_sender
            .send(submitter)
            .expect("submission thread should be running");
    }
}

pub(crate) struct UringPendingIoSubmitter {
    state: Arc<Mutex<UringPendingIoState>>,
    submission_ticket: Arc<SubmissionTicket>,
    transition_cv: Arc<Condvar>,
    entry: squeue::Entry,
}

impl UringPendingIoSubmitter {
    // Mark the operation as submitted.
    pub(crate) fn mark_submitted(self) {
        let mut state = self.state.lock();
        // The completion thread may race and fill the result prematurely, then we should do nothing.
        if state.status == UringPendingIoStatus::Done {
            return;
        }
        state.status = UringPendingIoStatus::Submitted;
        // Notify the future or the blocking thread that the operation changed its state.
        let waker = core::mem::replace(&mut state.waker, Waker::noop().clone());
        waker.wake();
        self.transition_cv.notify_all();
    }

    // Create an identified entry for the operation.
    pub(crate) fn create_entry(&self) -> squeue::Entry {
        let mut entry = self.entry.clone();
        entry.set_user_data(self.submission_ticket.id().0);
        entry
    }
}

/// Filler to be sent to the reaper thread for updating the state of the operation.
pub(crate) struct UringPendingIoFiller {
    state: Arc<Mutex<UringPendingIoState>>,
    transition_cv: Arc<Condvar>,
}

impl UringPendingIoFiller {
    pub(crate) fn complete(self, result: io::Result<i32>) {
        let mut state = self.state.lock();
        state.status = UringPendingIoStatus::Done;
        state.result = Some(result);
        // We would like to remove the ticket, but it blocks the reaper thread.
        // Notify the future or the blocking thread that the operation changed its state.
        let waker = core::mem::replace(&mut state.waker, Waker::noop().clone());
        waker.wake();
        self.transition_cv.notify_one();
    }
}

impl<'lifetime> Debug for UringPendingIoObj<'lifetime> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lock = self.state.lock();
        f.debug_struct("UringPendingIoObj")
            .field("status", &lock.status)
            .field("submission_ticket", &self.submission_ticket)
            .finish()
    }
}

/// Async version of the operation.
impl<'lifetime> Future for UringPendingIoObj<'lifetime> {
    type Output = Result<i32, io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // IMPORTANT: lock hierarchy: filler state -> submission ticket queue + submission queue + pending map + completion ticket submitter
        let inner = self.get_mut();
        let mut state = inner.state.lock();
        match state.status {
            UringPendingIoStatus::NotSubmitted => {
                assert!(
                    inner.submission_ticket.is_none(),
                    "submission ticket should not be assigned yet"
                );
                let ticket = match inner.submission_ticket.clone() {
                    // This branch allows ticket overrides
                    Some(t) => t,
                    // Attempt to assign a ticket to the operation in a nonblocking manner. As long as it has not received a ticket and entered the Submitting state yet, it can be trivially cancelled.
                    None => match inner.ticket_queue.poll_submission_ticket(cx) {
                        Poll::Ready(ticket) => Arc::new(ticket),
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    },
                };
                state.status = UringPendingIoStatus::Submitting;
                state.waker.clone_from(&cx.waker());
                drop(state);
                inner.submit(ticket);
                Poll::Pending
            }
            UringPendingIoStatus::Submitting => {
                state.waker.clone_from(&cx.waker());
                Poll::Pending
            }
            UringPendingIoStatus::Submitted => {
                state.waker.clone_from(&cx.waker());
                Poll::Pending
            }
            UringPendingIoStatus::Done => {
                let res = state
                    .result
                    .take()
                    .expect("result should be Some - future should not be polled multiple times");
                // The operation is done, remove the ticket and return the result.
                inner.submission_ticket.take();
                Poll::Ready(res)
            }
        }
    }
}

/// Cancel a pending operation using the borrowed submission ticket.
fn cancel_operation(uring: &ClientUring, ticket: &SubmissionTicket) -> () {
    let entry = io_uring::opcode::AsyncCancel::new(ticket.id().0).build();
    // Create the cancellation operation.
    let mut cancel_obj = UringPendingIoObj::new(uring, entry);
    // Submit and wait for the cancellation operation to complete.
    match cancel_obj
        .wait()
        .expect("new operation should succeed in waiting")
    {
        // Operation was cancelled successfully, we still need to wait until it ends.
        Ok(_) => {}
        // Operation may have already been done.
        Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {}
        // Operation is no longer cancellable, need to wait.
        Err(e) if e.raw_os_error() == Some(libc::EALREADY) => {}
        Err(e) => panic!("failed to cancel operation: {}", e),
    };
}

#[derive(Debug)]
pub(crate) enum CancelResult {
    WaitNeeded,
    WaitDone(Option<Result<i32, io::Error>>),
}

impl<'lifetime> UringPendingIoObj<'lifetime> {
    pub(crate) fn wait(&mut self) -> Option<Result<i32, io::Error>> {
        let mut state = self.state.lock();
        loop {
            match state.status {
                UringPendingIoStatus::NotSubmitted => {
                    let ticket = match self.submission_ticket.clone() {
                        // The operation may use overridden tickets.
                        Some(t) => t,
                        // Blocking call here is OK - the reaper thread cannot see the operation yet.
                        None => Arc::new(self.ticket_queue.request_submission_ticket()),
                    };
                    state.status = UringPendingIoStatus::Submitting;
                    drop(state);
                    // Temporarily release the lock to submit the operation - we can do that because the status is marked.
                    self.submit(ticket);
                    state = self.state.lock();
                    continue;
                }
                UringPendingIoStatus::Submitted | UringPendingIoStatus::Submitting => {
                    self.transition_cv.wait(&mut state);
                }
                UringPendingIoStatus::Done => {
                    let res = state.result.take();
                    drop(state);
                    self.submission_ticket.take();
                    return res;
                }
            }
        }
    }

    pub(crate) fn cancel_inner(&mut self) -> CancelResult {
        let mut state = self.state.lock();
        match state.status {
            // The operation is not submitted yet, so we have to do nothing.
            UringPendingIoStatus::NotSubmitted => {
                return CancelResult::WaitDone(None);
            }
            // The operation is submitting, so we have to wait for it to be submitted, only after that the SQE is cancellable.
            UringPendingIoStatus::Submitting => {
                self.transition_cv.wait(&mut state);
            }
            UringPendingIoStatus::Submitted => {}
            UringPendingIoStatus::Done => {
                // Must remove the submission ticket here because the operation may be polled midway,
                // marked as done, and then reach this point with a ticket.
                let result = state.result.take();
                drop(state);
                self.submission_ticket.take();
                return CancelResult::WaitDone(result);
            }
        };
        // When submitted, the ticket must have been acquired and the operation must have already been acknowledged.
        let cancellable = state.trigger_cancel();
        // Reaper thread may not block.
        drop(state);
        if cancellable && let Some(ticket) = self.submission_ticket.clone() {
            cancel_operation(self.uring, &ticket);
        }
        CancelResult::WaitNeeded
    }

    pub(crate) fn cancel(&mut self) -> Option<Result<i32, io::Error>> {
        let raw = match self.cancel_inner() {
            CancelResult::WaitNeeded => self.wait(),
            CancelResult::WaitDone(result) => result,
        };
        match raw {
            Some(Err(e)) if e.raw_os_error() == Some(libc::ECANCELED) => None,
            res => res,
        }
    }

    pub(crate) async fn cancel_async(&mut self) -> Option<Result<i32, io::Error>> {
        let raw = match self.cancel_inner() {
            CancelResult::WaitNeeded => Some(self.await),
            CancelResult::WaitDone(result) => result,
        };
        match raw {
            Some(Err(e)) if e.raw_os_error() == Some(libc::ECANCELED) => None,
            res => res,
        }
    }
}

impl<'lifetime> Unpin for UringPendingIoObj<'lifetime> {}

impl<'lifetime> Drop for UringPendingIoObj<'lifetime> {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
}

/// Helper to build a submission entry for either Fd or Fixed target.
macro_rules! build_op {
    ($target:expr, | $fd:ident | $op:expr) => {{
        use crate::Target;
        use io_uring::types;

        match $target {
            Target::Fd(raw) => {
                let $fd = types::Fd(raw);
                $op
            }
            Target::Fixed { index, .. } => {
                let $fd = types::Fixed(index);
                $op
            }
        }
    }};
}

/// Helper to build a submission entry that only supports Fd (not Fixed).
macro_rules! build_op_fd_only {
    ($target:expr, | $fd:ident | $op:expr) => {{
        use crate::Target;
        use io_uring::types;

        match $target {
            Target::Fd(raw) => {
                let $fd = types::Fd(raw);
                $op
            }
            Target::Fixed { raw_fd, .. } => {
                let $fd = types::Fd(raw_fd);
                $op
            }
        }
    }};
}

pub(crate) use build_op;
pub(crate) use build_op_fd_only;

#[cfg(test)]
mod tests {
    use crate::{
        Client, HybridFile, HybridRead, PendingIo, UringCfg,
        client::pending_io::uring::read_into::UringReadIntoAt, default_client,
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
        let pending_io = PendingIo::new(UringReadIntoAt::new(
            client.uring.as_ref().unwrap(),
            &pipe_read,
            buf.as_mut_slice(),
            u64::MAX,
        ));
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
        let (first_pending_created_tx, first_pending_created_rx) = tokio::sync::oneshot::channel();
        let task_1 = tokio::task::spawn({
            let client = client.clone();
            async move {
                let (reader_1, _writer_1) = pipe().expect("failed to create pipe");
                let mut buf_1 = [0u8; 64];
                log::info!("first pending I/O object creating");
                let pending_io = PendingIo::new(UringReadIntoAt::new(
                    client.uring.as_ref().unwrap(),
                    &reader_1,
                    buf_1.as_mut_slice(),
                    u64::MAX,
                ));
                log::info!("first pending I/O object created");
                // Flaky wait here is the only option because we are batching for multiple tickets at once while submission tickets are granted for operations indiscriminately.
                // An I/O uring request is expected to be acknowledged within this time window.
                tokio::time::sleep(Duration::from_secs_f64(0.1)).await;
                first_pending_created_tx
                    .send(())
                    .expect("failed to send first pending created");
                // The second I/O uring request should be tried and blocked within this time window.
                tokio::time::sleep(Duration::from_secs_f64(0.1)).await;
                log::info!("cancelling first pending I/O object");
                let _ = pending_io.cancel().await;
                log::info!("first pending I/O object cancelled");
            }
        });
        let task_2 = tokio::task::spawn({
            let client = client.clone();
            async move {
                let (reader_2, _writer_2) = pipe().expect("failed to create pipe");
                let mut buf_2 = [0u8; 64];
                first_pending_created_rx
                    .await
                    .expect("failed to wait for the first pending I/O object to be created");
                log::info!("second pending I/O object creating");
                let pending_io = PendingIo::new(UringReadIntoAt::new(
                    client.uring.as_ref().unwrap(),
                    &reader_2,
                    buf_2.as_mut_slice(),
                    u64::MAX,
                ));
                log::info!("second pending I/O object created");
                // The submission thread for this operation will block, cancelling the previous pending I/O
                // will unblock the submission thread and allowing the operation to be cancelled.
                log::info!("cancelling second pending I/O object");
                // FIXME: this cancel() blocks the runtime.
                let _ = pending_io.cancel().await;
            }
        });

        task_1.await.expect("task 1 should not panic");
        task_2.await.expect("task 2 should not panic");
    }
}
