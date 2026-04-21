use crate::client::pending_io::uring::UringPendingIoObj;
use crate::{ClientUring, UringTarget, client::pending_io::PendingIoImpl};
use std::{io, pin::Pin, sync::Arc, task::Poll};

struct CompletionState<'a> {
    raw: UringPendingIoObj<'a>,
}

struct Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    request: &'req mut UringFtruncate<'a, Target>,
    inner: Option<CompletionState<'a>>,
}

impl<'req, 'a, Target> Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringFtruncate<'a, Target>,
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
            Poll::Ready(res) => Poll::Ready(res.map(|_| ())),
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

/// Cancellation-correct future for io_uring mode of [`Client::ftruncate`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
#[allow(unused)]
pub struct UringFtruncate<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    /// Completion state.
    completion_state: Option<CompletionState<'a>>,
    /// Target to ftruncate.
    target: &'a Target,
    /// Identity of the io_uring instance.
    identity: &'a Arc<()>,
    /// Desired length of the file.
    len: u64,
}

impl<'a, Target> UringFtruncate<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(client: &'a ClientUring, target: &'a Target, len: u64) -> Self {
        Self {
            target,
            identity: &client.identity,
            len,
            completion_state: Some(CompletionState {
                raw: UringPendingIoObj::new(
                    client,
                    super::build_op!(unsafe { target.as_target(&client.identity) }, |fd| {
                        io_uring::opcode::Ftruncate::new(fd, len).build()
                    }),
                ),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<'a, Target> PendingIoImpl<Result<(), io::Error>> for UringFtruncate<'a, Target>
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

impl<'a, Target> Unpin for UringFtruncate<'a, Target> where Target: UringTarget + Sync + ?Sized {}

impl<'a, Target> Drop for UringFtruncate<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}
