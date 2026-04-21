use crate::client::pending_io::uring::UringPendingIoObj;
use crate::{ClientUring, UringTarget, client::pending_io::PendingIoImpl};
use nix::sys::stat::Mode;
use std::{ffi::CString, os::fd::AsRawFd};
use std::{io, pin::Pin, task::Poll};

struct CompletionState<'a> {
    raw: UringPendingIoObj<'a>,
}

struct Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    request: &'req mut UringMkdirAt<'a, DirTarget>,
    inner: Option<CompletionState<'a>>,
}

impl<'req, 'a, DirTarget> Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringMkdirAt<'a, DirTarget>,
        state: CompletionState<'a>,
    ) -> Self {
        Self {
            request,
            inner: Some(state),
        }
    }
}

impl<'req, 'a, DirTarget> Future for Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
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

impl<'req, 'a, DirTarget> Unpin for Completion<'req, 'a, DirTarget> where
    DirTarget: UringTarget + Sync + ?Sized
{
}

impl<'req, 'a, DirTarget> Drop for Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        self.request.completion_state = self.inner.take();
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::mkdir_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringMkdirAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    /// Completion state.
    completion_state: Option<CompletionState<'a>>,
    /// Directory target for relative paths, or AT_FDCWD for current directory.
    dir: &'a DirTarget,
    /// Old path to rename. Owned to ensure validity until completion.
    path: CString,
    /// Creation permissions for the mkdir operation.
    mode: Mode,
}

impl<'a, DirTarget> UringMkdirAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        client: &'a ClientUring,
        dir: &'a DirTarget,
        path: CString,
        mode: Mode,
    ) -> Self {
        let path_ptr = path.as_ptr();
        Self {
            dir,
            path,
            mode,
            completion_state: Some(CompletionState {
                raw: UringPendingIoObj::new(
                    client,
                    io_uring::opcode::MkDirAt::new(
                        io_uring::types::Fd(dir.as_file_descriptor().as_raw_fd()),
                        path_ptr,
                    )
                    .mode(mode.bits())
                    .build(),
                ),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<'a, DirTarget> PendingIoImpl<Result<(), io::Error>> for UringMkdirAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
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

impl<'a, DirTarget> Unpin for UringMkdirAt<'a, DirTarget> where
    DirTarget: UringTarget + Sync + ?Sized
{
}

impl<'a, DirTarget> Drop for UringMkdirAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}
