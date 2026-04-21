use crate::{
    ClientUring,
    client::pending_io::{PendingIoImpl, uring::UringPendingIoObj},
};
use std::{io, marker::PhantomData, os::fd::IntoRawFd, pin::Pin, task::Poll};

struct CompletionState<'a> {
    raw: UringPendingIoObj<'a>,
}

struct Completion<'req, 'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    state: Option<CompletionState<'a>>,
    request: &'req mut UringClose<'a, Target>,
}

impl<'req, 'a, Target> Completion<'req, 'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    pub(crate) fn new(
        request: &'req mut UringClose<'a, Target>,
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
    Target: IntoRawFd + Sized + Send,
{
    type Output = io::Result<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state object must be Some");
        match Pin::new(&mut state.raw).poll(cx) {
            Poll::Ready(res) => Poll::Ready(res.map(|_| ())),
            Poll::Pending => {
                inner.state = Some(state);
                Poll::Pending
            }
        }
    }
}

impl<'req, 'a, Target> Unpin for Completion<'req, 'a, Target> where Target: IntoRawFd + Sized + Send {}

impl<'req, 'a, Target> Drop for Completion<'req, 'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    fn drop(&mut self) {
        self.request.state = self.state.take()
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::close`].
/// When the future is dropped, the operation is waited for blocking mode (because it is not possible to return the fd back to the user in the original form).
pub struct UringClose<'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    state: Option<CompletionState<'a>>,
    _client: &'a ClientUring,
    _target: PhantomData<Target>,
}

impl<'a, Target> UringClose<'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    pub(crate) fn new(client: &'a ClientUring, target: Target) -> Self {
        Self {
            state: Some(CompletionState {
                raw: UringPendingIoObj::new(
                    client,
                    super::build_op_fd_only!(Target::Fd(target.into_raw_fd()), |fd| {
                        io_uring::opcode::Close::new(fd).build()
                    }),
                ),
            }),
            _client: client,
            _target: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<'a, Target> PendingIoImpl<Result<(), io::Error>> for UringClose<'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    fn _completion<'req>(
        &'req mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send + 'req>>> {
        match self.state.take() {
            Some(state) => Some(Box::pin(Completion::new(self, state))),
            None => None,
        }
    }

    async fn _cancel_async(&mut self) -> Option<Result<(), io::Error>> {
        // The operation is not cancellable, so we return the result of the operation.
        Some(self._completion()?.await)
    }

    fn _cancel(&mut self) -> Option<Result<(), io::Error>> {
        // The operation is not cancellable, so we return the result of the operation.
        match self.state.as_mut() {
            Some(state) => {
                let res = state.raw.wait().map(|r| r.map(|_s| ()));
                self.state = None;
                res
            }
            None => None,
        }
    }
}

impl<'a, Target> Unpin for UringClose<'a, Target> where Target: IntoRawFd + Sized + Send {}

impl<'a, Target> Drop for UringClose<'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}
