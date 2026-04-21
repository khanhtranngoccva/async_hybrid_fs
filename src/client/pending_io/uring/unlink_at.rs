use super::{PendingIoImpl, UringPendingIoObj};
use crate::{ClientUring, UringTarget};
use nix::unistd::UnlinkatFlags;
use std::{ffi::CString, os::fd::AsRawFd};
use std::{io, pin::Pin, task::Poll};

struct CompletionState<'a> {
    raw: UringPendingIoObj<'a>,
}

struct Completion<'req, 'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    state: Option<CompletionState<'a>>,
    request: &'req mut UringUnlinkAt<'a, OldDir>,
}

impl<'req, 'a, OldDir> Completion<'req, 'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringUnlinkAt<'a, OldDir>,
        state: CompletionState<'a>,
    ) -> Self {
        Self {
            state: Some(state),
            request,
        }
    }
}

impl<'req, 'a, OldDir> Future for Completion<'req, 'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
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

impl<'req, 'a, OldDir> Unpin for Completion<'req, 'a, OldDir> where
    OldDir: UringTarget + Sync + ?Sized
{
}

impl<'req, 'a, OldDir> Drop for Completion<'req, 'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        self.request.completion_state = self.state.take();
    }
}

/// Cancellation-correct future for io_uring mode of [`Client::unlink_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
pub struct UringUnlinkAt<'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    /// Completion state.
    completion_state: Option<CompletionState<'a>>,
    /// Directory target for relative paths, or AT_FDCWD for current directory.
    old_dir: &'a OldDir,
    /// Old path to rename. Owned to ensure validity until completion.
    old_path: CString,
    /// Flags for the unlink operation.
    flags: UnlinkatFlags,
}

impl<'a, OldDir> UringUnlinkAt<'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        old_dir: &'a OldDir,
        old_path: CString,
        flags: UnlinkatFlags,
    ) -> Self {
        let old_path_ptr = old_path.as_ptr();
        Self {
            old_dir,
            old_path,
            flags,
            completion_state: Some(CompletionState {
                raw: UringPendingIoObj::new(
                    uring,
                    io_uring::opcode::UnlinkAt::new(
                        io_uring::types::Fd(old_dir.as_file_descriptor().as_raw_fd()),
                        old_path_ptr,
                    )
                    .flags(match flags {
                        UnlinkatFlags::NoRemoveDir => 0,
                        UnlinkatFlags::RemoveDir => libc::AT_REMOVEDIR,
                    })
                    .build(),
                ),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<'a, OldDir> PendingIoImpl<Result<(), io::Error>> for UringUnlinkAt<'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
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

impl<'a, OldDir> Unpin for UringUnlinkAt<'a, OldDir> where OldDir: UringTarget + Sync + ?Sized {}

impl<'a, OldDir> Drop for UringUnlinkAt<'a, OldDir>
where
    OldDir: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        let _ = self._cancel();
    }
}
