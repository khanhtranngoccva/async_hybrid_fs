use super::UringPendingIo;
use super::macros;
use crate::runtime;
use crate::{
    ClientUring, UringTarget,
    client::{
        command::Command, completion::WriteResult, pending_io::PendingIoImpl,
        requests::WriteRequest, ticketing::SubmissionTicketId,
    },
    iobuf::IoBuf,
};
use std::{io, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::oneshot as oneshot_async;

struct CompletionState<Buf>
where
    Buf: IoBuf,
{
    /// Holder for the buffer, ensuring that the raw pointer is valid.
    buf: Buf,
    /// Channel for receiving operation results. If the operation is done, the channel will no longer exist and the cancellation is ignored.
    result_rx: oneshot_async::Receiver<io::Result<u32>>,
}

struct Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    state: Option<CompletionState<Buf>>,
    request: &'req mut UringWriteFromAt<'a, Target, Buf>,
}

impl<'req, 'a, Target, Buf> Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    pub(crate) fn new(
        request: &'req mut UringWriteFromAt<'a, Target, Buf>,
        state: CompletionState<Buf>,
    ) -> Self {
        Self {
            state: Some(state),
            request,
        }
    }
}

impl<'req, 'a, Target, Buf> Future for Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    type Output = io::Result<WriteResult<Buf>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state object must be Some");
        match Pin::new(&mut state.result_rx).poll(cx) {
            Poll::Ready(Ok(Ok(bytes_written))) => Poll::Ready({
                Ok(WriteResult {
                    buf: state.buf,
                    bytes_written: bytes_written as usize,
                })
            }),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(e))),
            Poll::Pending => {
                inner.state = Some(state);
                Poll::Pending
            }
        }
    }
}

impl<'req, 'a, Target, Buf> Unpin for Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
}

impl<'req, 'a, Target, Buf> Drop for Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    fn drop(&mut self) {
        self.request.completion_state = self.state.take()
    }
}

/// Cancellation-correct pending I/O object for io_uring mode of [`Client::write_from`] and [`Client::write_from_at`].
/// When the object is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringWriteFromAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    /// Target to write to.
    target: &'a Target,
    /// Identity of the io_uring instance.
    identity: &'a Arc<()>,
    /// Completion state, containing the buffer and the result channel.
    completion_state: Option<CompletionState<Buf>>,
    /// Offset to write to.
    offset: u64,
    /// Channel for sending operation IDs.
    ack_tx: Option<oneshot::Sender<SubmissionTicketId>>,
    /// Channel for receiving confirmation that the operation has been submitted. The ID must be received before the operation could be cancelled; otherwise, the future might drop before the operation even starts, leading to an operation with dangling pointers. We do not need the ID for any other purpose.
    ack_rx: Option<oneshot::AsyncReceiver<SubmissionTicketId>>,
    /// Channel for sending operation results.
    result_tx: Option<oneshot_async::Sender<io::Result<u32>>>,
    /// Client to use for submitting the operation and cancelling it.
    client: &'a ClientUring,
    /// Cancellation ID.
    cancellation: Option<SubmissionTicketId>,
    /// Whether cancellation is acknowledged.
    // The reason for an extra field is that the cancel_uring method may not be called twice.
    cancel_done: bool,
}

impl<'a, Target, Buf> UringPendingIo<Result<WriteResult<Buf>, io::Error>>
    for UringWriteFromAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    unsafe fn build_command(&mut self) -> Command {
        let state = self.completion_state.as_ref().expect("state must be Some");
        Command::Write {
            req: WriteRequest {
                target: unsafe { self.target.as_target(self.identity) },
                buf_ptr: state.buf.as_ptr(),
                buf_len: state.buf.len().try_into().unwrap_or(u32::MAX),
                offset: self.offset,
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

impl<'a, Target, Buf> UringWriteFromAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    pub(crate) fn new(uring: &'a ClientUring, target: &'a Target, buf: Buf, offset: u64) -> Self {
        let (ack_tx, ack_rx) = oneshot::async_channel();
        let (result_tx, result_rx) = oneshot_async::channel();
        let mut op = Self {
            target,
            identity: &uring.identity,
            completion_state: Some(CompletionState {
                buf,
                result_rx: result_rx,
            }),
            offset,
            ack_rx: Some(ack_rx),
            ack_tx: Some(ack_tx),
            result_tx: Some(result_tx),
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
impl<'a, Target, Buf> PendingIoImpl<Result<WriteResult<Buf>, io::Error>>
    for UringWriteFromAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    fn _completion<'req>(
        &'req mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Result<WriteResult<Buf>, io::Error>> + Send + 'req>>>
    {
        match self.completion_state.take() {
            Some(state) => Some(Box::pin(Completion::new(self, state))),
            None => None,
        }
    }

    async fn _cancel(&mut self) -> Option<Result<WriteResult<Buf>, io::Error>> {
        macros::uring_cancel_impl!(self)
    }
}

impl<'a, Target, Buf> Unpin for UringWriteFromAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
}

impl<'a, Target, Buf> Drop for UringWriteFromAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
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
