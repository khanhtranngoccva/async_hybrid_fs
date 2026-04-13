use super::{UringPendingIo, macros};
use crate::runtime;
use crate::{
    ClientUring, Metadata, UringTarget,
    client::ticketing::SubmissionTicketId,
    client::{command::Command, pending_io::{PendingIoDebuggingEvent, PendingIoImpl}, requests::StatxPathRequest},
};
use nix::fcntl::AtFlags;
use std::{ffi::CString, mem::MaybeUninit, os::fd::AsRawFd};
use std::{io, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::oneshot as oneshot_async;

struct CompletionState {
    result_rx: oneshot_async::Receiver<io::Result<Metadata>>,
}

struct Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    state: Option<CompletionState>,
    request: &'req mut UringStatxPath<'a, Target>,
}

impl<'req, 'a, Target> Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringStatxPath<'a, Target>,
        state: CompletionState,
    ) -> Self {
        Self {
            state: Some(state),
            request,
        }
    }
}

impl<'req, 'a, Target> Future for Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    type Output = io::Result<Metadata>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state must be Some");

        match Pin::new(&mut state.result_rx).poll(cx) {
            Poll::Ready(Ok(Ok(metadata))) => Poll::Ready(Ok(metadata)),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(e))),
            Poll::Pending => {
                inner.state = Some(state);
                Poll::Pending
            }
        }
    }
}

impl<'req, 'a, Target> Unpin for Completion<'req, 'a, Target> where
    Target: UringTarget + Sync + ?Sized
{
}
impl<'req, 'a, Target> Drop for Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        self.request.completion_state = self.state.take();
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::metadata_path`] and [`Client::statx_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringStatxPath<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    /// Directory target for relative paths, or AT_FDCWD for current directory.
    dir_target: &'a Target,
    /// Path to stat. Owned to ensure validity until completion.
    path: CString,
    /// Flags for the statx operation.
    flags: AtFlags,
    /// Identity of the io_uring instance.
    _identity: &'a Arc<()>,
    /// Channel for sending operation IDs.
    ack_tx: Option<oneshot::Sender<SubmissionTicketId>>,
    /// Channel for receiving confirmation that the operation has been submitted. The ID must be received before the operation could be cancelled; otherwise, the future might drop before the operation even starts, leading to an operation with dangling pointers. We do not need the ID for any other purpose.
    ack_rx: Option<oneshot::Receiver<SubmissionTicketId>>,
    /// Channel for sending operation results.
    result_tx: Option<oneshot_async::Sender<io::Result<Metadata>>>,
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

impl<'a, Target> UringPendingIo<Result<Metadata, io::Error>> for UringStatxPath<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    unsafe fn build_command(&mut self) -> Command {
        Command::StatxPath {
            req: StatxPathRequest {
                dir_fd: self.dir_target.as_file_descriptor().as_raw_fd(),
                path: self.path.clone(),
                flags: self.flags.bits(),
                statx_buf: Box::new(MaybeUninit::uninit()),
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

impl<'a, Target> UringStatxPath<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        dir_target: &'a Target,
        path: CString,
        flags: AtFlags,
        debug_event_tx: Option<tokio::sync::mpsc::UnboundedSender<PendingIoDebuggingEvent>>,
    ) -> Self {
        let (ack_tx, ack_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot_async::channel();
        let mut op = Self {
            dir_target,
            path,
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
        let command = unsafe { op.build_command() };
        uring.send(command, debug_event_tx);
        op
    }
}

#[async_trait::async_trait]
impl<'a, Target> PendingIoImpl<Result<Metadata, io::Error>> for UringStatxPath<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn _completion<'req>(
        &'req mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Result<Metadata, io::Error>> + Send + 'req>>> {
        match self.completion_state.take() {
            Some(state) => Some(Box::pin(Completion::new(self, state))),
            None => None,
        }
    }

    async fn _cancel(&mut self) -> Option<Result<Metadata, io::Error>> {
        macros::uring_cancel_impl!(self)
    }
}

impl<'a, Target> Drop for UringStatxPath<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        runtime::execute_future_from_sync(self._cancel());
    }
}
