use crate::client::pending_io::uring::UringPendingIoObj;
use crate::{ClientUring, UringTarget, client::pending_io::PendingIoImpl};
use nix::unistd::LinkatFlags;
use std::{ffi::CString, os::fd::AsRawFd};
use std::{io, pin::Pin, task::Poll};

struct CompletionState<'a> {
    raw: UringPendingIoObj<'a>,
}

struct Completion<'req, 'a, Target, NewDirTarget>
where
    Target: UringTarget + Sync + ?Sized,
    NewDirTarget: UringTarget + Sync + ?Sized,
{
    request: &'req mut UringLinkAt<'a, Target, NewDirTarget>,
    inner: Option<CompletionState<'a>>,
}

impl<'req, 'a, Target, NewDirTarget> Completion<'req, 'a, Target, NewDirTarget>
where
    Target: UringTarget + Sync + ?Sized,
    NewDirTarget: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringLinkAt<'a, Target, NewDirTarget>,
        state: CompletionState<'a>,
    ) -> Self {
        Self {
            request,
            inner: Some(state),
        }
    }
}

impl<'req, 'a, Target, NewDirTarget> Future for Completion<'req, 'a, Target, NewDirTarget>
where
    Target: UringTarget + Sync + ?Sized,
    NewDirTarget: UringTarget + Sync + ?Sized,
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

impl<'req, 'a, Target, NewDirTarget> Unpin for Completion<'req, 'a, Target, NewDirTarget>
where
    Target: UringTarget + Sync + ?Sized,
    NewDirTarget: UringTarget + Sync + ?Sized,
{
}

impl<'req, 'a, Target, NewDirTarget> Drop for Completion<'req, 'a, Target, NewDirTarget>
where
    Target: UringTarget + Sync + ?Sized,
    NewDirTarget: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        self.request.completion_state = self.inner.take();
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::hard_link_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
#[allow(unused)]
pub struct UringLinkAt<'a, Target, NewDirTarget>
where
    Target: UringTarget + Sync + ?Sized,
    NewDirTarget: UringTarget + Sync + ?Sized,
{
    /// Completion state.
    completion_state: Option<CompletionState<'a>>,
    /// Target file to hard link.
    target: &'a Target,
    /// Old path relative to `target`. Owned to ensure validity until completion.
    old_path: CString,
    /// Directory target for relative paths, or AT_FDCWD for current directory.
    new_dir: &'a NewDirTarget,
    /// New path to hard link. Owned to ensure validity until completion.
    new_path: CString,
    /// Flags for the hard link operation.
    flags: LinkatFlags,
}

impl<'a, Target, NewDirTarget> UringLinkAt<'a, Target, NewDirTarget>
where
    Target: UringTarget + Sync + ?Sized,
    NewDirTarget: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        target: &'a Target,
        old_path: CString,
        new_dir: &'a NewDirTarget,
        new_path: CString,
        flags: LinkatFlags,
    ) -> Self {
        let old_path_ptr = old_path.as_ptr();
        let new_path_ptr = new_path.as_ptr();
        Self {
            new_dir,
            target,
            old_path,
            new_path,
            flags,
            completion_state: Some(CompletionState {
                raw: UringPendingIoObj::new(
                    uring,
                    io_uring::opcode::LinkAt::new(
                        io_uring::types::Fd(target.as_file_descriptor().as_raw_fd()),
                        old_path_ptr,
                        io_uring::types::Fd(new_dir.as_file_descriptor().as_raw_fd()),
                        new_path_ptr,
                    )
                    .flags(flags.bits())
                    .build(),
                ),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<'a, Target, NewDirTarget> PendingIoImpl<Result<(), io::Error>>
    for UringLinkAt<'a, Target, NewDirTarget>
where
    Target: UringTarget + Sync + ?Sized,
    NewDirTarget: UringTarget + Sync + ?Sized,
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

impl<'a, Target, NewDirTarget> Unpin for UringLinkAt<'a, Target, NewDirTarget>
where
    Target: UringTarget + Sync + ?Sized,
    NewDirTarget: UringTarget + Sync + ?Sized,
{
}

impl<'a, Target, NewDirTarget> Drop for UringLinkAt<'a, Target, NewDirTarget>
where
    Target: UringTarget + Sync + ?Sized,
    NewDirTarget: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}
