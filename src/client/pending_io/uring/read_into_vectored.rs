use super::{UringPendingIo, macros};
use crate::client::pending_io::PendingIoImpl;
use crate::client::requests::IovecArray;
use crate::client::ticketing::SubmissionTicketId;
use crate::runtime;
use crate::{
    ClientUring, UringTarget,
    client::{command::Command, completion::ReadvResult, requests::ReadvRequest},
    iobuf::IoBufMut,
};
use std::cmp::min;
use std::{io, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::oneshot as oneshot_async;

struct CompletionState<Buf> {
    /// Buffers to read into.
    bufs: Vec<Buf>,
    /// Channel for receiving operation results.
    result_rx: oneshot_async::Receiver<io::Result<u32>>,
}

struct Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    request: &'req mut UringReadIntoVectoredAt<'a, Target, Buf>,
    state: Option<CompletionState<Buf>>,
}

impl<'req, 'a, Target, Buf> Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    pub(crate) fn new(
        request: &'req mut UringReadIntoVectoredAt<'a, Target, Buf>,
        state: CompletionState<Buf>,
    ) -> Self {
        Self {
            request,
            state: Some(state),
        }
    }
}

impl<'req, 'a, Target, Buf> Future for Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    type Output = io::Result<ReadvResult<Buf>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state must be Some");
        match Pin::new(&mut state.result_rx).poll(cx) {
            Poll::Ready(Ok(Ok(bytes_read))) => Poll::Ready({
                let mut cur_bytes_read = bytes_read as usize;
                for buf in state.bufs.iter_mut() {
                    let bytes_read_into_target = min(buf.capacity(), cur_bytes_read);
                    unsafe {
                        buf.set_len(bytes_read_into_target);
                    }
                    cur_bytes_read -= bytes_read_into_target;
                }
                Ok(ReadvResult {
                    bufs: state.bufs,
                    bytes_read: bytes_read as usize,
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
    Buf: IoBufMut,
{
}

impl<'req, 'a, Target, Buf> Drop for Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    fn drop(&mut self) {
        self.request.completion_state = self.state.take();
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::read_into_vectored`] and [`Client::read_into_vectored_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringReadIntoVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    /// Target to read from.
    target: &'a Target,
    /// Identity of the io_uring instance.
    identity: &'a Arc<()>,
    /// Holder for the iovec array.
    // Array is pinned because the command object requires a pointer to iovec.
    iovec_array: Pin<IovecArray>,
    /// Offset to read from.
    offset: u64,
    /// Channel for sending operation IDs.
    ack_tx: Option<oneshot::Sender<SubmissionTicketId>>,
    /// Channel for receiving confirmation that the operation has been submitted. The ID must be received before the operation could be cancelled; otherwise, the future might drop before the operation even starts, leading to an operation with dangling pointers. We do not need the ID for any other purpose.
    ack_rx: Option<oneshot::AsyncReceiver<SubmissionTicketId>>,
    /// Channel for sending operation results.
    result_tx: Option<oneshot_async::Sender<io::Result<u32>>>,
    /// Completion state for the operation.
    completion_state: Option<CompletionState<Buf>>,
    /// Client to use for submitting the operation and cancelling it.
    client: &'a ClientUring,
    /// Cancellation ID.
    cancellation: Option<SubmissionTicketId>,
    /// Whether cancellation is acknowledged.
    // The reason for an extra field is that the cancel_uring method may not be called twice.
    cancel_done: bool,
}

impl<'a, Target, Buf> UringPendingIo<Result<ReadvResult<Buf>, io::Error>>
    for UringReadIntoVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    unsafe fn build_command(&mut self) -> Command {
        Command::Readv {
            req: ReadvRequest {
                target: unsafe { self.target.as_target(self.identity) },
                io_slices: self.iovec_array.as_ptr(),
                io_slices_len: self
                    .iovec_array
                    .len()
                    .try_into()
                    .expect("len exceeds u32::MAX"),
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

impl<'a, Target, Buf> UringReadIntoVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        target: &'a Target,
        mut bufs: Vec<Buf>,
        offset: u64,
    ) -> Self {
        let (ack_tx, ack_rx) = oneshot::async_channel();
        let (result_tx, result_rx) = oneshot_async::channel();
        let mut op = Self {
            target,
            identity: &uring.identity,
            // Need to pin the iovec array because the command object requires a pointer.
            iovec_array: Pin::new(IovecArray(
                bufs.iter_mut()
                    .map(|buf| libc::iovec {
                        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                        iov_len: buf.capacity().into(),
                    })
                    .collect::<Vec<_>>(),
            )),
            completion_state: Some(CompletionState { bufs, result_rx }),
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
impl<'a, Target, Buf> PendingIoImpl<Result<ReadvResult<Buf>, io::Error>>
    for UringReadIntoVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    fn _completion<'req>(
        &'req mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Result<ReadvResult<Buf>, io::Error>> + Send + 'req>>>
    {
        match self.completion_state.take() {
            Some(state) => Some(Box::pin(Completion::new(self, state))),
            None => None,
        }
    }

    async fn _cancel(&mut self) -> Option<Result<ReadvResult<Buf>, io::Error>> {
        macros::uring_cancel_impl!(self)
    }
}

impl<'a, Target, Buf> Unpin for UringReadIntoVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
}

impl<'a, Target, Buf> Drop for UringReadIntoVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    fn drop(&mut self) {
        runtime::execute_future_from_sync(self._cancel());
    }
}
