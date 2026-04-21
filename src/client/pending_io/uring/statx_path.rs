use super::UringPendingIoObj;
use crate::{ClientUring, Metadata, UringTarget, client::pending_io::PendingIoImpl};
use nix::fcntl::AtFlags;
use std::{ffi::CString, mem::MaybeUninit, os::fd::AsRawFd};
use std::{io, pin::Pin, task::Poll};

struct CompletionState<'a> {
    raw: UringPendingIoObj<'a>,
    statx_buf: Box<MaybeUninit<libc::statx>>,
}

struct Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    state: Option<CompletionState<'a>>,
    request: &'req mut UringStatxPath<'a, Target>,
}

impl<'req, 'a, Target> Completion<'req, 'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        request: &'req mut UringStatxPath<'a, Target>,
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
    type Output = io::Result<Metadata>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = self.get_mut();
        let mut state = inner.state.take().expect("state must be Some");

        match Pin::new(&mut state.raw).poll(cx) {
            Poll::Ready(Ok(_)) => {
                Poll::Ready(Ok(Metadata(unsafe { (*state.statx_buf).assume_init() })))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
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

/// Cancellation-correct future for io_uring mode of [`Client::metadata_path`] and [`Client::statx_at`].
/// When the future is dropped, the operation is cancelled synchronously via an internal method.
#[allow(unused)]
pub struct UringStatxPath<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    /// Completion state, containing the statx buffer.
    completion_state: Option<CompletionState<'a>>,
    /// Directory target for relative paths, or AT_FDCWD for current directory.
    dir_target: &'a Target,
    /// Path to stat. Owned to ensure validity until completion.
    path: CString,
    /// Flags for the statx operation.
    flags: AtFlags,
}

impl<'a, Target> UringStatxPath<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    pub(crate) fn new(
        uring: &'a ClientUring,
        dir_target: &'a Target,
        path: CString,
        flags: AtFlags,
    ) -> Self {
        let path_ptr = path.as_ptr();
        let mut statx_buf = Box::new(MaybeUninit::uninit());
        let statx_ptr = statx_buf.as_mut_ptr();
        Self {
            dir_target,
            path,
            flags,
            completion_state: Some(CompletionState {
                statx_buf: Box::new(MaybeUninit::uninit()),
                raw: UringPendingIoObj::new(
                    uring,
                    io_uring::opcode::Statx::new(
                        io_uring::types::Fd(dir_target.as_file_descriptor().as_raw_fd()),
                        path_ptr,
                        statx_ptr,
                    )
                    .flags(flags.bits())
                    .mask(libc::STATX_BASIC_STATS)
                    .build(),
                ),
            }),
        }
    }
}

#[async_trait::async_trait]
impl<'a, Target> PendingIoImpl<Result<Metadata, io::Error>> for UringStatxPath<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn _completion<'req>(
        &'req mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Result<Metadata, io::Error>> + Send + 'req>>> {
        match self.completion_state.take() {
            Some(state) => Some(Box::pin(Completion::new(self, state))),
            None => None,
        }
    }

    async fn _cancel_async(&mut self) -> Option<Result<Metadata, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state
                    .raw
                    .cancel_async()
                    .await
                    .map(|r| r.map(|_s| unsafe { Metadata((*state.statx_buf).assume_init()) }));
                self.completion_state = None;
                res
            }
            None => None,
        }
    }

    fn _cancel(&mut self) -> Option<Result<Metadata, io::Error>> {
        match self.completion_state.as_mut() {
            Some(state) => {
                let res = state
                    .raw
                    .cancel()
                    .map(|r| r.map(|_s| unsafe { Metadata((*state.statx_buf).assume_init()) }));
                self.completion_state = None;
                res
            }
            None => None,
        }
    }
}

impl<'a, Target> Drop for UringStatxPath<'a, Target>
where
    Target: UringTarget + Sync + ?Sized,
{
    fn drop(&mut self) {
        self._cancel();
    }
}
