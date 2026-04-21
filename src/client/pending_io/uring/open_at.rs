use crate::client::pending_io::uring::UringPendingIoObj;
use crate::{ClientUring, UringTarget, client::pending_io::PendingIoImpl};
use nix::{fcntl::OFlag, sys::stat::Mode};
use std::os::fd::FromRawFd;
use std::{
    ffi::CString,
    os::fd::{AsRawFd, OwnedFd},
};
use std::{io, pin::Pin, task::Poll};

struct CompletionState<'a> {
    raw: UringPendingIoObj<'a>,
}

struct Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    request: &'req mut UringOpenAt<'a, Target>,
    inner: Option<CompletionState<'a>>,
}

impl<'req, 'a, Target> Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringOpenAt<'a, Target>,
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
    type Output = io::Result<OwnedFd>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.inner.take().expect("state must be Some");
        match Pin::new(&mut state.raw).poll(cx) {
            Poll::Ready(res) => Poll::Ready(res.map(|fd| unsafe { OwnedFd::from_raw_fd(fd) })),
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

/// Cancellation-correct future for io_uring mode of [`Client::open_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringOpenAt<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    /// Completion state.
    completion_state: Option<CompletionState<'a>>,
    /// Directory target for relative paths, or AT_FDCWD for current directory.
    dir_target: &'a Target,
    /// Path to open. Owned to ensure validity until completion.
    path: CString,
    /// Flags for the open operation.
    flags: OFlag,
    /// Creation permissions for the open operation.
    mode: Mode,
}

impl<'a, Target> UringOpenAt<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        dir_target: &'a Target,
        path: CString,
        flags: OFlag,
        mode: Mode,
    ) -> Self {
        let path_ptr = path.as_ptr();
        Self {
            dir_target,
            path,
            flags,
            mode,
            completion_state: Some(CompletionState {
                raw: UringPendingIoObj::new(
                    uring,
                    io_uring::opcode::OpenAt::new(
                        io_uring::types::Fd(dir_target.as_file_descriptor().as_raw_fd()),
                        path_ptr,
                    )
                    .flags(flags.bits())
                    .mode(mode.bits())
                    .build(),
                ),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<'a, Target> PendingIoImpl<Result<OwnedFd, io::Error>> for UringOpenAt<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn _completion<'req>(
        &'req mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Result<OwnedFd, io::Error>> + Send + 'req>>> {
        match self.completion_state.take() {
            Some(state) => Some(Box::pin(Completion::new(self, state))),
            None => None,
        }
    }

    async fn _cancel_async(&mut self) -> Option<Result<OwnedFd, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state
                    .raw
                    .cancel_async()
                    .await
                    .map(|r| r.map(|fd| unsafe { OwnedFd::from_raw_fd(fd) }));
                self.completion_state = None;
                res
            }
            None => None,
        }
    }

    fn _cancel(&mut self) -> Option<Result<OwnedFd, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state
                    .raw
                    .cancel()
                    .map(|r| r.map(|fd| unsafe { OwnedFd::from_raw_fd(fd) }));
                self.completion_state = None;
                res
            }
            None => None,
        }
    }
}

impl<'a, Target> Unpin for UringOpenAt<'a, Target> where Target: UringTarget + Sync + ?Sized {}

impl<'a, Target> Drop for UringOpenAt<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}
