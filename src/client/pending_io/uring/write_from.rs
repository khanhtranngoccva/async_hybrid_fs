use super::UringPendingIoObj;
use crate::{
    ClientUring, UringTarget,
    client::{completion::WriteResult, pending_io::PendingIoImpl},
    iobuf::IoBuf,
};
use std::{io, pin::Pin, task::Poll};

struct CompletionState<'a, Buf>
where
    Buf: IoBuf,
{
    raw: UringPendingIoObj<'a>,
    /// Holder for the buffer, ensuring that the raw pointer is valid.
    buf: Buf,
}

struct Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    state: Option<CompletionState<'a, Buf>>,
    request: &'req mut UringWriteFromAt<'a, Target, Buf>,
}

impl<'req, 'a, Target, Buf> Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    pub(crate) fn new(
        request: &'req mut UringWriteFromAt<'a, Target, Buf>,
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
    type Output = io::Result<WriteResult<Buf>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state object must be Some");
        match Pin::new(&mut state.raw).poll(cx) {
            Poll::Ready(Ok(bytes_written)) => Poll::Ready(Ok(WriteResult {
                buf: state.buf,
                bytes_written: bytes_written as usize,
            })),
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
    /// Completion state for the operation.
    completion_state: Option<CompletionState<'a, Buf>>,
    /// Target to write to.
    target: &'a Target,
    /// Offset to write to.
    offset: u64,
}

impl<'a, Target, Buf> UringWriteFromAt<'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBuf,
{
    pub(crate) fn new(uring: &'a ClientUring, target: &'a Target, buf: Buf, offset: u64) -> Self {
        let raw = UringPendingIoObj::new(
            uring,
            super::build_op!(unsafe { target.as_target(&uring.identity) }, |fd| {
                io_uring::opcode::Write::new(
                    fd,
                    buf.as_ptr(),
                    buf.len().try_into().unwrap_or(u32::MAX),
                )
                .offset(offset)
                .build()
            }),
        );
        Self {
            completion_state: Some(CompletionState { raw, buf }),
            target,
            offset,
        }
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

    async fn _cancel_async(&mut self) -> Option<Result<WriteResult<Buf>, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state.raw.cancel_async().await.map(|r| {
                    r.map(|bytes_written| {
                        let state = self.completion_state.take().expect("state must be Some");
                        WriteResult {
                            buf: state.buf,
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

    fn _cancel(&mut self) -> Option<Result<WriteResult<Buf>, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state.raw.cancel().map(|r| {
                    r.map(|bytes_written| {
                        let state = self.completion_state.take().expect("state must be Some");
                        WriteResult {
                            buf: state.buf,
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
        let _ = self._cancel();
    }
}
