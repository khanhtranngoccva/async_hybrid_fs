use crate::{
    ClientUring, UringTarget,
    client::pending_io::{PendingIoImpl, uring::UringPendingIoObj},
};
use std::{ffi::CString, os::fd::AsRawFd};
use std::{io, pin::Pin, task::Poll};

struct CompletionState<'a> {
    raw: UringPendingIoObj<'a>,
}

struct Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    state: Option<CompletionState<'a>>,
    request: &'req mut UringSymlinkAt<'a, DirTarget>,
}

impl<'req, 'a, DirTarget> Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringSymlinkAt<'a, DirTarget>,
        state: CompletionState<'a>,
    ) -> Self {
        Self {
            state: Some(state),
            request,
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

impl<'req, 'a, DirTarget> Unpin for Completion<'req, 'a, DirTarget> where
    DirTarget: UringTarget + Sync + ?Sized
{
}

impl<'req, 'a, DirTarget> Drop for Completion<'req, 'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        self.request.completion_state = self.state.take();
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::symlink_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringSymlinkAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    /// Completion state.
    completion_state: Option<CompletionState<'a>>,
    /// Directory target for relative paths, or AT_FDCWD for current directory.
    new_dir: &'a DirTarget,
    /// Target path to symlink to. Owned to ensure validity until completion.
    target: CString,
    /// Link path to create. Owned to ensure validity until completion.
    link_path: CString,
}

impl<'a, DirTarget> UringSymlinkAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        new_dir: &'a DirTarget,
        target: CString,
        link_path: CString,
    ) -> Self {
        let target_ptr = target.as_ptr();
        let link_path_ptr = link_path.as_ptr();
        Self {
            new_dir,
            target,
            link_path,
            completion_state: Some(CompletionState {
                raw: UringPendingIoObj::new(
                    uring,
                    io_uring::opcode::SymlinkAt::new(
                        io_uring::types::Fd(new_dir.as_file_descriptor().as_raw_fd()),
                        target_ptr,
                        link_path_ptr,
                    )
                    .build(),
                ),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<'a, DirTarget> PendingIoImpl<Result<(), io::Error>> for UringSymlinkAt<'a, DirTarget>
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

impl<'a, DirTarget> Unpin for UringSymlinkAt<'a, DirTarget> where
    DirTarget: UringTarget + Sync + ?Sized
{
}

impl<'a, DirTarget> Drop for UringSymlinkAt<'a, DirTarget>
where
    DirTarget: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}
