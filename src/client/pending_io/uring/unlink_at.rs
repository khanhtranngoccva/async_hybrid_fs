use super::{PendingIoImpl, UringPendingIo, macros};
use crate::{
    ClientUring, UringTarget,
    client::{
        command::Command, requests::UnlinkAtRequest,
        ticketing::SubmissionTicketId,
    },
    runtime,
};
use nix::unistd::UnlinkatFlags;
use std::{ffi::CString, os::fd::AsRawFd};
use std::{io, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::oneshot as oneshot_async;

struct CompletionState {
    /// Channel for receiving operation results. If the operation is done, the channel will no longer exist and the cancellation is ignored.
    result_rx: oneshot_async::Receiver<io::Result<()>>,
}

struct Completion<'req, 'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    state: Option<CompletionState>,
    request: &'req mut UringUnlinkAt<'a, OldDir>,
}

impl<'req, 'a, OldDir> Completion<'req, 'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringUnlinkAt<'a, OldDir>,
        state: CompletionState,
    ) -> Self {
        Self {
            state: Some(state),
            request,
        }
    }
}

impl<'req, 'a, OldDir> Future for Completion<'req, 'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
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

impl<'req, 'a, OldDir> Unpin for Completion<'req, 'a, OldDir> where
    OldDir: UringTarget + Sync + ?Sized
{
}

impl<'req, 'a, OldDir> Drop for Completion<'req, 'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        self.request.completion_state = self.state.take();
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::unlink_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringUnlinkAt<'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    /// Directory target for relative paths, or AT_FDCWD for current directory.
    old_dir: &'a OldDir,
    /// Old path to rename. Owned to ensure validity until completion.
    old_path: CString,
    /// Flags for the unlink operation.
    flags: UnlinkatFlags,
    /// Identity of the io_uring instance.
    _identity: &'a Arc<()>,
    /// Channel for sending operation IDs.
    ack_tx: Option<oneshot::Sender<SubmissionTicketId>>,
    /// Channel for receiving confirmation that the operation has been submitted. The ID must be received before the operation could be cancelled; otherwise, the future might drop before the operation even starts, leading to an operation with dangling pointers. We do not need the ID for any other purpose.
    ack_rx: Option<oneshot::AsyncReceiver<SubmissionTicketId>>,
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

impl<'a, OldDir> UringPendingIo<Result<(), io::Error>> for UringUnlinkAt<'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    unsafe fn build_command(&mut self) -> Command {
        Command::UnlinkAt {
            req: UnlinkAtRequest {
                dir_fd: self.old_dir.as_file_descriptor().as_raw_fd(),
                path: self.old_path.clone(),
                flags: match self.flags {
                    UnlinkatFlags::RemoveDir => libc::AT_REMOVEDIR,
                    UnlinkatFlags::NoRemoveDir => 0,
                },
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

impl<'a, OldDir> Future for UringUnlinkAt<'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    type Output = io::Result<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner
            .completion_state
            .take()
            .expect("result_rx must be Some");
        match Pin::new(&mut state.result_rx).poll(cx) {
            Poll::Ready(Ok(Ok(()))) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(e))),
            Poll::Pending => {
                inner.completion_state = Some(state);
                Poll::Pending
            }
        }
    }
}

impl<'a, OldDir> UringUnlinkAt<'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        old_dir: &'a OldDir,
        old_path: CString,
        flags: UnlinkatFlags,
    ) -> Self {
        let (ack_tx, ack_rx) = oneshot::async_channel();
        let (result_tx, result_rx) = oneshot_async::channel();
        let mut op = Self {
            old_dir,
            old_path,
            flags,
            _identity: &uring.identity,
            ack_rx: Some(ack_rx),
            ack_tx: Some(ack_tx),
            result_tx: Some(result_tx),
            completion_state: Some(CompletionState {
                result_rx: result_rx,
            }),
            client: uring,
            cancellation: None,
            cancel_done: false,
        };
        let command = unsafe { op.build_command() };
        uring.send(command);
        op
    }
}

#[async_trait::async_trait]
impl<'a, OldDir> PendingIoImpl<Result<(), io::Error>> for UringUnlinkAt<'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
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

impl<'a, OldDir> Unpin for UringUnlinkAt<'a, OldDir> where OldDir: UringTarget + Sync + ?Sized {}

impl<'a, OldDir> Drop for UringUnlinkAt<'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        runtime::execute_future_from_sync(self._cancel());
    }
}
