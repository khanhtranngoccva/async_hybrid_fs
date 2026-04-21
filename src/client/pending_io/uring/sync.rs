use super::UringPendingIoObj;
use crate::{ClientUring, UringTarget, client::pending_io::PendingIoImpl};
use std::{io, pin::Pin, sync::Arc, task::Poll};

struct CompletionState<'a> {
    raw: UringPendingIoObj<'a>,
}

struct Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    state: Option<CompletionState<'a>>,
    request: &'req mut UringSync<'a, Target>,
}

impl<'req, 'a, Target> Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringSync<'a, Target>,
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
    type Output = io::Result<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state must be Some");
        match Pin::new(&mut state.raw).poll(cx) {
            Poll::Ready(res) => Poll::Ready(res.map(|_| ())),
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

/// Cancellation-correct future for io_uring mode of [`Client::sync_all`] and [`Client::sync_data`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringSync<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    /// Completion state for the operation.
    completion_state: Option<CompletionState<'a>>,
    /// Target to sync.
    target: &'a Target,
    /// Identity of the io_uring instance.
    identity: &'a Arc<()>,
    /// Datasync flag.
    datasync: bool,
}

impl<'a, Target> UringSync<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(uring: &'a ClientUring, target: &'a Target, datasync: bool) -> Self {
        Self {
            target,
            identity: &uring.identity,
            datasync,
            completion_state: Some(CompletionState {
                raw: UringPendingIoObj::new(
                    uring,
                    super::build_op!(unsafe { target.as_target(&uring.identity) }, |fd| {
                        let mut fsync = io_uring::opcode::Fsync::new(fd);
                        if datasync {
                            fsync = fsync.flags(types::FsyncFlags::DATASYNC);
                        }
                        fsync.build()
                    }),
                ),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<'a, Target> PendingIoImpl<Result<(), io::Error>> for UringSync<'a, Target>
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

impl<'a, Target> Unpin for UringSync<'a, Target> where Target: UringTarget + Sync + ?Sized {}

impl<'a, Target> Drop for UringSync<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        self._cancel();
    }
}
