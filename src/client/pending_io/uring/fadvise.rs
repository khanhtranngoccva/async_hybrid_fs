use crate::ClientUring;
use crate::client::pending_io::uring::UringPendingIoObj;
use crate::{UringTarget, client::pending_io::PendingIoImpl};
use nix::fcntl::PosixFadviseAdvice;
use std::{io, pin::Pin, sync::Arc, task::Poll};

struct CompletionState<'a> {
    raw: UringPendingIoObj<'a>,
}

struct Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    request: &'req mut UringFadvise<'a, Target>,
    inner: Option<CompletionState<'a>>,
}

impl<'req, 'a, Target> Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringFadvise<'a, Target>,
        state: CompletionState<'a>,
    ) -> Self {
        Self {
            request,
            inner: Some(state),
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
        let mut state = inner.inner.take().expect("state must be Some");
        match Pin::new(&mut state.raw).poll(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                inner.inner = Some(state);
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
        self.request.completion_state = self.inner.take();
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::fadvise`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringFadvise<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    /// Completion state.
    completion_state: Option<CompletionState<'a>>,
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
        Self {
            target,
            identity: &client.identity,
            offset,
            len,
            advice,
            completion_state: Some(CompletionState {
                raw: UringPendingIoObj::new(
                    client,
                    super::build_op!(unsafe { target.as_target(&client.identity) }, |fd| {
                        io_uring::opcode::Fadvise::new(fd, len, advice as i32)
                            .offset(offset)
                            .build()
                    }),
                ),
            }),
        }
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

    async fn _cancel_async(&mut self) -> Option<Result<(), io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state.raw.cancel_async().await.map(|r| r.map(|_s| ()));
                self.completion_state = None;
                res
            }
            None => None,
        }
    }

    fn _cancel(&mut self) -> Option<Result<(), io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state.raw.cancel().map(|r| r.map(|_s| ()));
                self.completion_state = None;
                res
            }
            None => None,
        }
    }
}

impl<'a, Target> Unpin for UringFadvise<'a, Target> where Target: UringTarget + Sync + ?Sized {}

impl<'a, Target> Drop for UringFadvise<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        let _ = self.completion_state.take().map(|mut c| c.raw.cancel());
    }
}
