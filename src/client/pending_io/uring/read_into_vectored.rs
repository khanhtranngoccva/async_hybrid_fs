use crate::client::pending_io::PendingIoImpl;
use crate::client::pending_io::uring::UringPendingIoObj;
use crate::client::requests::IovecArray;
use crate::{ClientUring, UringTarget, client::completion::ReadvResult, iobuf::IoBufMut};
use std::cmp::min;
use std::{io, pin::Pin, sync::Arc, task::Poll};

struct CompletionState<'a, Buf> {
    raw: UringPendingIoObj<'a>,
    /// Buffers to read into.
    // Note: the Buf object is supposed to be dropped after the operation is completed.
    bufs: Vec<Buf>,
}

struct Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    request: &'req mut UringReadIntoVectoredAt<'a, Target, Buf>,
    state: Option<CompletionState<'a, Buf>>,
}

impl<'req, 'a, Target, Buf> Completion<'req, 'a, Target, Buf>
where
    Target: UringTarget + Sync + ?Sized,
    Buf: IoBufMut,
{
    pub(crate) fn new(
        request: &'req mut UringReadIntoVectoredAt<'a, Target, Buf>,
        state: CompletionState<'a, Buf>,
    ) -> Self {
        Self {
            request,
            state: Some(state),
        }
    }
}

fn convert_result<Buf>(mut bufs: Vec<Buf>, bytes_read: i32) -> ReadvResult<Buf>
where
    Buf: IoBufMut,
{
    let mut cur_bytes_read = bytes_read as usize;
    for buf in bufs.iter_mut() {
        let bytes_read_into_target = min(buf.capacity(), cur_bytes_read);
        unsafe {
            buf.set_len(bytes_read_into_target);
        }
        cur_bytes_read -= bytes_read_into_target;
    }
    ReadvResult {
        bufs,
        bytes_read: bytes_read as usize,
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
        match Pin::new(&mut state.raw).poll(cx) {
            Poll::Ready(Ok(bytes_read)) => Poll::Ready(Ok(convert_result(state.bufs, bytes_read))),
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

/// Cancellation-correct future for io_uring mode of [`Client::read_into_vectored`] and [`Client::read_into_vectored_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringReadIntoVectoredAt<'a, Target, Buf>
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
    /// Holder for the iovec array.
    // Array is pinned because the command object requires a pointer to iovec.
    iovec_array: Pin<IovecArray>,
    /// Offset to read from.
    offset: u64,
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
        let iovec_array = Pin::new(IovecArray(
            bufs.iter_mut()
                .map(|buf| libc::iovec {
                    iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                    iov_len: buf.capacity().into(),
                })
                .collect::<Vec<_>>(),
        ));
        let iov_len = iovec_array.len().try_into().unwrap_or(u32::MAX);
        let iov_ptr = iovec_array.as_ptr();
        Self {
            target,
            identity: &uring.identity,
            iovec_array,
            completion_state: Some(CompletionState {
                bufs,
                raw: UringPendingIoObj::new(
                    uring,
                    super::build_op!(unsafe { target.as_target(&uring.identity) }, |fd| {
                        io_uring::opcode::Readv::new(fd, iov_ptr, iov_len)
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

    async fn _cancel_async(&mut self) -> Option<Result<ReadvResult<Buf>, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state.raw.cancel_async().await.map(|r| {
                    r.map(|bytes_read| {
                        let state = self.completion_state.take().expect("state must be Some");
                        convert_result(state.bufs, bytes_read)
                    })
                });
                self.completion_state = None;
                res
            }
            None => None,
        }
    }

    fn _cancel(&mut self) -> Option<Result<ReadvResult<Buf>, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state.raw.cancel().map(|r| {
                    r.map(|bytes_read| {
                        let state = self.completion_state.take().expect("state must be Some");
                        convert_result(state.bufs, bytes_read)
                    })
                });
                self.completion_state = None;
                res
            }
            None => None,
        }
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
        let _ = self._cancel();
    }
}
