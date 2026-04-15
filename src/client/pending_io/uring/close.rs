use super::UringPendingIo;
use crate::{
    ClientUring,
    client::{command::Command, pending_io::PendingIoImpl, requests::CloseRequest},
};
use std::{io, os::fd::IntoRawFd, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::oneshot::{self, Receiver, Sender};

struct CompletionState {
    /// Channel for receiving operation results. If the operation is done, the channel will no longer exist and the cancellation is ignored.
    result_rx: Receiver<io::Result<()>>,
}

struct Completion<'req, 'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    state: Option<CompletionState>,
    request: &'req mut UringClose<'a, Target>,
}

impl<'req, 'a, Target> Completion<'req, 'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    pub(crate) fn new(request: &'req mut UringClose<'a, Target>, state: CompletionState) -> Self {
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
        match Pin::new(&mut state.result_rx).poll(cx) {
            Poll::Ready(Ok(Ok(()))) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(e))),
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
    /// Target to close.
    target: Option<Target>,
    /// Channel for sending operation results.
    result_tx: Option<Sender<io::Result<()>>>,
    /// Minimal completion state.
    state: Option<CompletionState>,
    _identity: &'a Arc<()>,
    _client: &'a ClientUring,
}

impl<'a, Target> UringPendingIo<Result<(), io::Error>> for UringClose<'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    unsafe fn build_command(&mut self) -> Command {
        Command::Close {
            req: CloseRequest {
                fd: self
                    .target
                    .take()
                    .expect("target must be Some")
                    .into_raw_fd(),
            },
            res: self
                .result_tx
                .take()
                .expect("build_command may only be run once per pending operation"),
        }
    }
}

impl<'a, Target> UringClose<'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    pub(crate) fn new(client: &'a ClientUring, target: Target) -> Self {
        let (result_tx, result_rx) = oneshot::channel();
        let mut op = Self {
            target: Some(target),
            _identity: &client.identity,
            result_tx: Some(result_tx),
            state: Some(CompletionState { result_rx }),
            _client: client,
        };
        let command = unsafe { op.build_command() };
        client.send(command);
        op
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

    async fn _cancel(&mut self) -> Option<Result<(), io::Error>> {
        // The operation is not cancellable, so we return the result of the operation.
        Some(self._completion()?.await)
    }
}

impl<'a, Target> Unpin for UringClose<'a, Target> where Target: IntoRawFd + Sized + Send {}

impl<'a, Target> Drop for UringClose<'a, Target>
where
    Target: IntoRawFd + Sized + Send,
{
    fn drop(&mut self) {
        // Must attempt to cancel if operation is not completed
        if let Some(state) = self.state.take() {
            let _ = state
                .result_rx
                .blocking_recv()
                .expect("result_rx must be received to avoid a dangling pointer issue");
        }
    }
}
