use super::{UringPendingIo, macros};
use crate::{
    ClientUring, UringTarget,
    client::{
        command::Command,
        pending_io::PendingIoImpl,
        requests::SymlinkAtRequest,
        ticketing::SubmissionTicketId,
    },
    runtime,
};
use std::{ffi::CString, os::fd::AsRawFd};
use std::{io, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::oneshot as oneshot_async;

struct CompletionState {
    /// Channel for receiving operation results. If the operation is done, the channel will no longer exist and the cancellation is ignored.
    result_rx: oneshot_async::Receiver<io::Result<()>>,
}

struct Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    state: Option<CompletionState>,
    request: &'req mut UringSymlinkAt<'a, DirTarget>,
}

impl<'req, 'a, DirTarget> Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringSymlinkAt<'a, DirTarget>,
        state: CompletionState,
    ) -> Self {
        Self {
            state: Some(state),
            request,
        }
    }
}

impl<'req, 'a, DirTarget> Future for Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
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

impl<'req, 'a, DirTarget> Unpin for Completion<'req, 'a, DirTarget> where
    DirTarget: UringTarget + Sync + ?Sized
{
}

impl<'req, 'a, DirTarget> Drop for Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        self.request.completion_state = self.state.take();
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::symlink_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringSymlinkAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    /// Directory target for relative paths, or AT_FDCWD for current directory.
    new_dir: &'a DirTarget,
    /// Target path to symlink to. Owned to ensure validity until completion.
    target: CString,
    /// Link path to create. Owned to ensure validity until completion.
    link_path: CString,
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

impl<'a, DirTarget> UringPendingIo<Result<(), io::Error>> for UringSymlinkAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    unsafe fn build_command(&mut self) -> Command {
        Command::SymlinkAt {
            req: SymlinkAtRequest {
                new_dir_fd: self.new_dir.as_file_descriptor().as_raw_fd(),
                target: self.target.clone(),
                link_path: self.link_path.clone(),
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

impl<'a, DirTarget> Future for UringSymlinkAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
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
            .expect("completion_state must be Some");
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

impl<'a, DirTarget> UringSymlinkAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        new_dir: &'a DirTarget,
        target: CString,
        link_path: CString,
    ) -> Self {
        let (ack_tx, ack_rx) = oneshot::async_channel();
        let (result_tx, result_rx) = oneshot_async::channel();
        let mut op = Self {
            new_dir,
            target,
            link_path,
            _identity: &uring.identity,
            ack_rx: Some(ack_rx),
            ack_tx: Some(ack_tx),
            result_tx: Some(result_tx),
            completion_state: Some(CompletionState { result_rx }),
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
impl<'a, DirTarget> PendingIoImpl<Result<(), io::Error>> for UringSymlinkAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
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

impl<'a, DirTarget> Unpin for UringSymlinkAt<'a, DirTarget> where
    DirTarget: UringTarget + Sync + ?Sized
{
}

impl<'a, DirTarget> Drop for UringSymlinkAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        runtime::execute_future_from_sync(self._cancel());
    }
}
