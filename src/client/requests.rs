use crate::UringTarget;
use libc::iovec;
use std::{
    ops::Deref,
    os::fd::{BorrowedFd, RawFd},
    sync::Arc,
};

/// A raw target object that can be used by the io_uring client, which represents either a borrowed file descriptor or a borrowed fixed entry.
///
/// A user may obtain a mutable Target object by calling the unsafe [`Client::as_target`](crate::Client::as_target) method on an immutable UringTarget object, which allows bypassing mutable checks to avoid certain overhead.
#[derive(Debug, Clone)]
pub enum Target {
    Fd(RawFd),
    Fixed {
        index: u32,
        raw_fd: RawFd,
        uring_identity: Arc<()>,
    },
}

impl UringTarget for Target {
    fn as_file_descriptor(&self) -> BorrowedFd<'_> {
        match self {
            Target::Fd(fd) => unsafe { BorrowedFd::borrow_raw(*fd) },
            Target::Fixed { raw_fd, .. } => unsafe { BorrowedFd::borrow_raw(*raw_fd) },
        }
    }

    unsafe fn as_target(&self, _uring_identity: &Arc<()>) -> Target {
        match self {
            Target::Fd(fd) => Target::Fd(*fd),
            Target::Fixed {
                index,
                raw_fd,
                uring_identity,
            } => {
                if Arc::ptr_eq(uring_identity, _uring_identity) {
                    Target::Fixed {
                        index: *index,
                        raw_fd: *raw_fd,
                        uring_identity: uring_identity.clone(),
                    }
                } else {
                    Target::Fd(*raw_fd)
                }
            }
        }
    }
}

pub(crate) struct IovecArray(pub(crate) Vec<iovec>);

// SAFETY: The iovec slice is owned by the caller's future, which awaits completion, the pointer is only accessed by the kernel threads
unsafe impl Send for IovecArray {}
unsafe impl Sync for IovecArray {}

impl Deref for IovecArray {
    type Target = [iovec];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
