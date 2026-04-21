use super::{PendingIoImpl, UringPendingIoObj};
use crate::client::requests::IovecArray;
use crate::{ClientUring, UringTarget, client::completion::WritevResult, iobuf::IoBuf};
use std::{io, pin::Pin, sync::Arc, task::Poll};

struct CompletionState<'a, Buf>
where
    Buf: IoBuf,
{
    raw: UringPendingIoObj<'a>,
    bufs: Vec<Buf>,
}

struct Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    state: Option<CompletionState<'a, Buf>>,
    request: &'req mut UringWriteFromVectoredAt<'a, Target, Buf>,
}

impl<'req, 'a, Target, Buf> Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    pub(crate) fn new(
        request: &'req mut UringWriteFromVectoredAt<'a, Target, Buf>,
        state: CompletionState<'a, Buf>,
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
    type Output = io::Result<WritevResult<Buf>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state must be Some");
        match Pin::new(&mut state.raw).poll(cx) {
            Poll::Ready(res) => Poll::Ready(res.map(|bytes_written| WritevResult {
                bufs: state.bufs,
                bytes_written: bytes_written as usize,
            })),
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
        self.request.completion_state = self.state.take();
    }
}

/// Cancellation-correct pending I/O object for io_uring mode of [`Client::write_from_vectored`] and [`Client::write_from_vectored_at`].
/// When the object is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringWriteFromVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    /// Completion state for the operation.
    completion_state: Option<CompletionState<'a, Buf>>,
    /// Target to write to.
    target: &'a Target,
    /// Identity of the io_uring instance.
    identity: &'a Arc<()>,
    /// Holder for the iovec array.
    // Array is pinned because the command object requires a pointer to iovec.
    iovec_array: Pin<IovecArray>,
    /// Offset to write to.
    offset: u64,
}

impl<'a, Target, Buf> UringWriteFromVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        target: &'a Target,
        mut bufs: Vec<Buf>,
        offset: u64,
    ) -> Self {
        let iov = Pin::new(IovecArray(
            // Need to pin the iovec array because the command object requires a pointer.
            bufs.iter_mut()
                .map(|buf| libc::iovec {
                    iov_base: buf.as_ptr() as *mut libc::c_void,
                    iov_len: buf.len().into(),
                })
                .collect::<Vec<_>>(),
        ));
        let iov_ptr = iov.as_ptr();
        let iov_len = iov.len().try_into().unwrap_or(u32::MAX);
        Self {
            target,
            identity: &uring.identity,
            iovec_array: iov,
            completion_state: Some(CompletionState {
                bufs,
                raw: UringPendingIoObj::new(
                    uring,
                    super::build_op!(unsafe { target.as_target(&uring.identity) }, |fd| {
                        io_uring::opcode::Writev::new(fd, iov_ptr, iov_len)
                            .offset(offset)
                            .build()
                    }),
                ),
            }),
            offset,
        }
    }
}

#[async_trait::async_trait]
impl<'a, Target, Buf> PendingIoImpl<Result<WritevResult<Buf>, io::Error>>
    for UringWriteFromVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    fn _completion<'req>(
        &'req mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Result<WritevResult<Buf>, io::Error>> + Send + 'req>>>
    {
        match self.completion_state.take() {
            Some(state) => Some(Box::pin(Completion::new(self, state))),
            None => None,
        }
    }

    async fn _cancel_async(&mut self) -> Option<Result<WritevResult<Buf>, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state.raw.cancel_async().await.map(|r| {
                    r.map(|bytes_written| {
                        let state = self.completion_state.take().expect("state must be Some");
                        WritevResult {
                            bufs: state.bufs,
                            bytes_written: bytes_written as usize,
                        }
                    })
                });
                self.completion_state = None;
                res
            }
            None => None,
        }
    }

    fn _cancel(&mut self) -> Option<Result<WritevResult<Buf>, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state.raw.cancel().map(|r| {
                    r.map(|bytes_written| {
                        let state = self.completion_state.take().expect("state must be Some");
                        WritevResult {
                            bufs: state.bufs,
                            bytes_written: bytes_written as usize,
                        }
                    })
                });
                self.completion_state = None;
                res
            }
            None => None,
        }
    }
}

impl<'a, Target, Buf> Unpin for UringWriteFromVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
}

impl<'a, Target, Buf> Drop for UringWriteFromVectoredAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}
