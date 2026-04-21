use super::UringPendingIoObj;
use crate::{ClientUring, Metadata, UringTarget, client::pending_io::PendingIoImpl};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::{io, pin::Pin, task::Poll};

struct CompletionState<'a> {
    statx_buf: Box<MaybeUninit<libc::statx>>,
    raw: UringPendingIoObj<'a>,
}

struct Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    state: Option<CompletionState<'a>>,
    request: &'req mut UringStatx<'a, Target>,
}

impl<'req, 'a, Target> Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringStatx<'a, Target>,
        state: CompletionState<'a>,
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
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state must be Some");
        match Pin::new(&mut state.raw).poll(cx) {
            Poll::Ready(Ok(_)) => {
                Poll::Ready(Ok(Metadata(unsafe { (*state.statx_buf).assume_init() })))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
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

/// Cancellation-correct future for io_uring mode of [`Client::metadata`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
#[allow(unused)]
pub struct UringStatx<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    /// Completion state, containing the statx buffer.
    completion_state: Option<CompletionState<'a>>,
    /// Target to query metadata.
    target: &'a Target,
}

impl<'a, Target> UringStatx<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(uring: &'a ClientUring, target: &'a Target) -> Self {
        let mut statx_buf = Box::new(MaybeUninit::uninit());
        let statx_ptr = statx_buf.as_mut_ptr();
        Self {
            target,
            completion_state: Some(CompletionState {
                statx_buf,
                raw: UringPendingIoObj::new(
                    uring,
                    io_uring::opcode::Statx::new(
                        io_uring::types::Fd(target.as_file_descriptor().as_raw_fd()),
                        c"".as_ptr(),
                        statx_ptr as *mut io_uring::types::statx,
                    )
                    .flags(libc::AT_EMPTY_PATH)
                    .build(),
                ),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<'a, Target> PendingIoImpl<Result<Metadata, io::Error>> for UringStatx<'a, Target>
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

    async fn _cancel_async(&mut self) -> Option<Result<Metadata, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state
                    .raw
                    .cancel_async()
                    .await
                    .map(|r| r.map(|_s| Metadata(unsafe { (*state.statx_buf).assume_init() })));
                self.completion_state = None;
                res
            }
            None => None,
        }
    }

    fn _cancel(&mut self) -> Option<Result<Metadata, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state
                    .raw
                    .cancel()
                    .map(|r| r.map(|_s| Metadata(unsafe { (*state.statx_buf).assume_init() })));
                self.completion_state = None;
                res
            }
            None => None,
        }
    }
}

impl<'a, Target> Unpin for UringStatx<'a, Target> where Target: UringTarget + Sync + ?Sized {}

impl<'a, Target> Drop for UringStatx<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}
