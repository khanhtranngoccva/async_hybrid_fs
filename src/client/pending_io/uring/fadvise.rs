use super::{UringPendingIo, macros};
use crate::client::ticketing::SubmissionTicketId;
use crate::{ClientUring, runtime};
use crate::{
    UringTarget,
    client::{command::Command, pending_io::PendingIoImpl, requests::FadviseRequest},
};
use nix::fcntl::PosixFadviseAdvice;
use std::{io, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::oneshot as oneshot_async;

struct CompletionState {
    /// Channel for receiving operation results. If the operation is done, the channel will no longer exist and the cancellation is ignored.
    result_rx: oneshot_async::Receiver<io::Result<()>>,
}

struct Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    request: &'req mut UringFadvise<'a, Target>,
    state: Option<CompletionState>,
}

impl<'req, 'a, Target> Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(request: &'req mut UringFadvise<'a, Target>, state: CompletionState) -> Self {
        Self {
            request,
            state: Some(state),
        }
    }
}

impl<'req, 'a, Target> Future for Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
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

/// Cancellation-correct future for io_uring mode of [`Client::fadvise`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringFadvise<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    /// Target to fadvise.
    target: &'a Target,
    /// Identity of the io_uring instance.
    identity: &'a Arc<()>,
    /// Offset to fadvise.
    offset: u64,
    /// Length of the operation.
    len: i64,
    /// Advice of the operation.
    advice: PosixFadviseAdvice,
    /// Channel for sending operation IDs.
    ack_tx: Option<oneshot::Sender<SubmissionTicketId>>,
    /// Channel for receiving confirmation that the operation has been submitted. The ID must be received before the operation could be cancelled; otherwise, the future might drop before the operation even starts, leading to an operation with dangling pointers. We do not need the ID for any other purpose.
    ack_rx: Option<oneshot::AsyncReceiver<SubmissionTicketId>>,
    /// Channel for sending operation results.
    result_tx: Option<oneshot_async::Sender<io::Result<()>>>,
    /// Completion state.
    completion_state: Option<CompletionState>,
    /// Client to use for submitting the operation and cancelling it.
    client: &'a ClientUring,
    /// Cancellation ID.
    cancellation: Option<SubmissionTicketId>,
    /// Whether cancellation is acknowledged.
    // The reason for an extra field is that the cancel_uring method may not be called twice.
    cancel_done: bool,
}

impl<'a, Target> UringPendingIo<Result<(), io::Error>> for UringFadvise<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    unsafe fn build_command(&mut self) -> Command {
        Command::Fadvise {
            req: FadviseRequest {
                target: unsafe { self.target.as_target(self.identity) },
                offset: self.offset,
                len: self.len,
                advice: self.advice as i32,
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

impl<'a, Target> UringFadvise<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        client: &'a ClientUring,
        target: &'a Target,
        advice: PosixFadviseAdvice,
        offset: u64,
        len: i64,
    ) -> Self {
        let (ack_tx, ack_rx) = oneshot::async_channel();
        let (result_tx, result_rx) = oneshot_async::channel();
        let mut op = Self {
            target,
            identity: &client.identity,
            offset,
            len,
            advice,
            ack_rx: Some(ack_rx),
            ack_tx: Some(ack_tx),
            result_tx: Some(result_tx),
            completion_state: Some(CompletionState { result_rx }),
            client,
            cancellation: None,
            cancel_done: false,
        };
        let command = unsafe { op.build_command() };
        client.send(command);
        op
    }
}

#[async_trait::async_trait]
impl<'a, Target> PendingIoImpl<Result<(), io::Error>> for UringFadvise<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
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

impl<'a, Target> Unpin for UringFadvise<'a, Target> where Target: UringTarget + Sync + ?Sized {}

impl<'a, Target> Drop for UringFadvise<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        // Hot path: There is generally no point in cancelling the operation if it is
        // already completed or has not been submitted yet, executing the cancellation
        // function costs more time.
        if self.completion_state.is_none() {
            return;
        }
        runtime::execute_future_from_sync(self._cancel());
    }
}
