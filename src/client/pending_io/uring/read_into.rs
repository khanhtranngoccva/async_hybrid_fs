use crate::client::pending_io::PendingIoImpl;
use crate::client::pending_io::uring::UringPendingIoObj;
use crate::{ClientUring, UringTarget, client::completion::ReadResult, iobuf::IoBufMut};
use std::{io, pin::Pin, sync::Arc, task::Poll};

struct CompletionState<'a, Buf>
where
    Buf: IoBufMut,
{
    raw: UringPendingIoObj<'a>,
    buf: Buf,
}

struct Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    request: &'req mut UringReadIntoAt<'a, Target, Buf>,
    state: Option<CompletionState<'a, Buf>>,
}

impl<'req, 'a, Target, Buf> Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    pub(crate) fn new(
        request: &'req mut UringReadIntoAt<'a, Target, Buf>,
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
    Buf: IoBufMut,
{
    type Output = io::Result<ReadResult<Buf>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state must be Some");
        match Pin::new(&mut state.raw).poll(cx) {
            Poll::Ready(Ok(bytes_read)) => {
                unsafe {
                    state.buf.set_len(bytes_read as usize);
                }
                Poll::Ready({
                    Ok(ReadResult {
                        buf: state.buf,
                        bytes_read: bytes_read as usize,
                    })
                })
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
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

/// Cancellation-correct future for io_uring mode of [`Client::read_into`] and [`Client::read_into_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
#[allow(unused)]
pub struct UringReadIntoAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    /// Completion state for the operation.
    completion_state: Option<CompletionState<'a, Buf>>,
    /// Target to read from.
    target: &'a Target,
    /// Identity of the io_uring instance.
    identity: &'a Arc<()>,
    /// Offset to read from.
    offset: u64,
}

impl<'a, Target, Buf> UringReadIntoAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        target: &'a Target,
        mut buf: Buf,
        offset: u64,
    ) -> Self {
        let buf_ptr = buf.as_mut_ptr();
        let buf_len = buf.capacity().try_into().unwrap_or(u32::MAX);
        Self {
            target,
            identity: &uring.identity,
            offset,
            completion_state: Some(CompletionState {
                buf,
                raw: UringPendingIoObj::new(
                    uring,
                    super::build_op!(unsafe { target.as_target(&uring.identity) }, |fd| {
                        io_uring::opcode::Read::new(fd, buf_ptr, buf_len)
                            .offset(offset)
                            .build()
                    }),
                ),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<'a, Target, Buf> PendingIoImpl<Result<ReadResult<Buf>, io::Error>>
    for UringReadIntoAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    fn _completion<'req>(
        &'req mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Result<ReadResult<Buf>, io::Error>> + Send + 'req>>>
    {
        match self.completion_state.take() {
            Some(state) => Some(Box::pin(Completion::new(self, state))),
            None => None,
        }
    }

    async fn _cancel_async(&mut self) -> Option<Result<ReadResult<Buf>, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state.raw.cancel_async().await.map(|r| {
                    r.map(|bytes_read| {
                        let mut state = self.completion_state.take().expect("state must be Some");
                        unsafe {
                            state.buf.set_len(bytes_read as usize);
                        }
                        ReadResult {
                            buf: state.buf,
                            bytes_read: bytes_read as usize,
                        }
                    })
                });
                self.completion_state = None;
                res
            }
            None => None,
        }
    }

    fn _cancel(&mut self) -> Option<Result<ReadResult<Buf>, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state.raw.cancel().map(|r| {
                    r.map(|bytes_read| {
                        let mut state = self.completion_state.take().expect("state must be Some");
                        unsafe {
                            state.buf.set_len(bytes_read as usize);
                        }
                        ReadResult {
                            buf: state.buf,
                            bytes_read: bytes_read as usize,
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

impl<'a, Target, Buf> Unpin for UringReadIntoAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
}

impl<'a, Target, Buf> Drop for UringReadIntoAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}
