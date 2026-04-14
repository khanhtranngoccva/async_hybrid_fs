use super::{UringPendingIo, macros};
use crate::{
    ClientUring, UringTarget,
    client::ticketing::SubmissionTicketId,
    client::{
        command::Command,
        pending_io::{PendingIoDebuggingEvent, PendingIoImpl},
        requests::RenameAtRequest,
    },
    runtime,
};
use nix::fcntl::RenameFlags;
use std::{ffi::CString, os::fd::AsRawFd};
use std::{io, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::oneshot as oneshot_async;

struct CompletionState {
    /// Channel for receiving operation results. If the operation is done, the channel will no longer exist and the cancellation is ignored.
    result_rx: oneshot_async::Receiver<io::Result<()>>,
}

struct Completion<'req, 'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
    state: Option<CompletionState>,
    request: &'req mut UringRenameAt<'a, OldDir, NewDir>,
}

impl<'req, 'a, OldDir, NewDir> Completion<'req, 'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringRenameAt<'a, OldDir, NewDir>,
        state: CompletionState,
    ) -> Self {
        Self {
            state: Some(state),
            request,
        }
    }
}

impl<'req, 'a, OldDir, NewDir> Future for Completion<'req, 'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
    type Output = io::Result<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state must be Some");
        match Pin::new(&mut state.result_rx).poll(cx) {
            Poll::Ready(Ok(Ok(()))) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(e))),
            Poll::Pending => {
                inner.state = Some(state);
                Poll::Pending
            }
        }
    }
}

impl<'req, 'a, OldDir, NewDir> Unpin for Completion<'req, 'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
}

impl<'req, 'a, OldDir, NewDir> Drop for Completion<'req, 'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        self.request.completion_state = self.state.take();
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::rename_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringRenameAt<'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
    /// Directory target for relative paths, or AT_FDCWD for current directory.
    old_dir: &'a OldDir,
    /// Old path to rename. Owned to ensure validity until completion.
    old_path: CString,
    /// New directory target for relative paths, or AT_FDCWD for current directory.
    new_dir: &'a NewDir,
    /// New path to rename. Owned to ensure validity until completion.
    new_path: CString,
    /// Flags for the rename operation.
    flags: RenameFlags,
    /// Identity of the io_uring instance.
    _identity: &'a Arc<()>,
    /// Channel for sending operation IDs.
    ack_tx: Option<oneshot::Sender<SubmissionTicketId>>,
    /// Channel for receiving confirmation that the operation has been submitted. The ID must be received before the operation could be cancelled; otherwise, the future might drop before the operation even starts, leading to an operation with dangling pointers. We do not need the ID for any other purpose.
    ack_rx: Option<oneshot::Receiver<SubmissionTicketId>>,
    /// Channel for sending operation results.
    result_tx: Option<oneshot_async::Sender<io::Result<()>>>,
    /// Completion state, containing the result channel.
    completion_state: Option<CompletionState>,
    /// Client to use for submitting the operation and cancelling it.
    client: &'a ClientUring,
    /// Cancellation ID.
    cancellation: Option<SubmissionTicketId>,
    /// Whether cancellation is acknowledged.
    // The reason for an extra field is that the cancel_uring method may not be called twice.
    cancel_done: bool,
}

impl<'a, OldDir, NewDir> UringPendingIo<Result<(), io::Error>> for UringRenameAt<'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
    unsafe fn build_command(&mut self) -> Command {
        Command::RenameAt {
            req: RenameAtRequest {
                old_dir_fd: self.old_dir.as_file_descriptor().as_raw_fd(),
                old_path: self.old_path.clone(),
                new_dir_fd: self.new_dir.as_file_descriptor().as_raw_fd(),
                new_path: self.new_path.clone(),
                flags: self.flags.bits(),
            },
            ack: Some(
                self.ack_tx
                    .take()
                    .expect("build_command may only be run once per pending operation"),
            ),
            res: self
                .result_tx
                .take()
                .expect("build_command may only be run once per pending operation"),
        }
    }
}

impl<'a, OldDir, NewDir> UringRenameAt<'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
    pub(crate) async fn new(
        uring: &'a ClientUring,
        old_dir: &'a OldDir,
        old_path: CString,
        new_dir: &'a NewDir,
        new_path: CString,
        flags: RenameFlags,
        debug_event_tx: Option<tokio::sync::mpsc::UnboundedSender<PendingIoDebuggingEvent>>,
    ) -> Self {
        let (ack_tx, ack_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot_async::channel();
        let mut op = Self {
            old_dir,
            old_path,
            new_dir,
            new_path,
            flags,
            _identity: &uring.identity,
            ack_rx: Some(ack_rx),
            ack_tx: Some(ack_tx),
            result_tx: Some(result_tx),
            completion_state: Some(CompletionState { result_rx }),
            client: uring,
            cancellation: None,
            cancel_done: false,
        };
        uring.send(&mut op, debug_event_tx).await;
        op
    }
}

#[async_trait::async_trait]
impl<'a, OldDir, NewDir> PendingIoImpl<Result<(), io::Error>> for UringRenameAt<'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
    fn _completion<'req>(
        &'req mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send + 'req>>> {
        match self.completion_state.take() {
            Some(state) => Some(Box::pin(Completion::new(self, state))),
            None => None,
        }
    }

    async fn _cancel(&mut self) -> Option<Result<(), io::Error>> {
        macros::uring_cancel_impl!(self)
    }
}

impl<'a, OldDir, NewDir> Unpin for UringRenameAt<'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
}

impl<'a, OldDir, NewDir> Drop for UringRenameAt<'a, OldDir, NewDir>
where
    OldDir: UringTarget + Sync + ?Sized,
    NewDir: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        runtime::execute_future_from_sync(self._cancel());
    }
}
