use std::{ffi::CString, mem::MaybeUninit, ops::Deref, os::fd::RawFd};

use libc::iovec;

#[derive(Clone, Copy)]
pub enum Target {
    Fd(RawFd),
    Fixed { index: u32, raw_fd: RawFd },
}

impl Target {
    #[allow(unused)]
    pub(crate) fn fd(&self) -> RawFd {
        match self {
            Target::Fd(fd) => *fd,
            #[allow(unused)]
            Target::Fixed { index, raw_fd } => *raw_fd,
        }
    }
}

pub(crate) struct ReadRequest {
    pub(crate) target: Target,
    pub(crate) buf_ptr: *mut u8,
    pub(crate) buf_len: u32,
    pub(crate) offset: u64,
}

// SAFETY: The pointer is to heap-allocated memory owned by the caller's future, which awaits completion. The pointer is only dereferenced by the kernel, not by our threads.
unsafe impl Send for ReadRequest {}
unsafe impl Sync for ReadRequest {}

pub(crate) struct ReadvRequest {
    pub(crate) target: Target,
    pub(crate) io_slices: *const iovec,
    pub(crate) io_slices_len: u32,
    pub(crate) offset: u64,
}

// SAFETY: The pointer is to heap-allocated memory owned by the caller's future, which awaits completion. The pointer is only dereferenced by the kernel, not by our threads.
unsafe impl Send for ReadvRequest {}
unsafe impl Sync for ReadvRequest {}

pub(crate) struct IovecArray(pub(crate) Vec<iovec>);

// SAFETY: The iovec slice is owned by the caller's future, which awaits completion, the pointer is only accessed by the kernel threads
unsafe impl Send for IovecArray {}
unsafe impl Sync for IovecArray {}

impl IovecArray {
    pub(crate) fn as_ptr(&self) -> *const iovec {
        self.0.as_ptr()
    }
}

impl Deref for IovecArray {
    type Target = [iovec];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct WriteRequest {
    pub(crate) target: Target,
    pub(crate) buf_ptr: *const u8,
    pub(crate) buf_len: u32,
    pub(crate) offset: u64,
}

// SAFETY: The pointer is to heap-allocated memory owned by the caller's future, which awaits completion. The pointer is only dereferenced by the kernel, not by our threads.
unsafe impl Send for WriteRequest {}
unsafe impl Sync for WriteRequest {}

pub(crate) struct WritevRequest {
    pub(crate) target: Target,
    pub(crate) io_slices: *const iovec,
    pub(crate) io_slices_len: u32,
    pub(crate) offset: u64,
}

// SAFETY: The pointer is to heap-allocated memory owned by the caller's future, which awaits completion. The pointer is only dereferenced by the kernel, not by our threads.
unsafe impl Send for WritevRequest {}
unsafe impl Sync for WritevRequest {}

pub(crate) struct SyncRequest {
    pub(crate) target: Target,
    pub(crate) datasync: bool,
}

pub(crate) struct StatxRequest {
    pub(crate) target: Target,
    /// Caller-allocated buffer for statx result. We use libc::statx for the actual storage, cast to types::statx* for the opcode.
    pub(crate) statx_buf: Box<MaybeUninit<libc::statx>>,
}

pub(crate) struct FallocateRequest {
    pub(crate) target: Target,
    pub(crate) offset: u64,
    pub(crate) len: u64,
    pub(crate) mode: i32,
}

pub(crate) struct FadviseRequest {
    pub(crate) target: Target,
    pub(crate) offset: u64,
    pub(crate) len: i64,
    pub(crate) advice: i32,
}

pub(crate) struct FtruncateRequest {
    pub(crate) target: Target,
    pub(crate) len: u64,
}

pub(crate) struct OpenAtRequest {
    /// Directory fd for relative paths, or AT_FDCWD for current directory.
    pub(crate) dir_fd: RawFd,
    /// Path to open. Owned to ensure validity until completion.
    pub(crate) path: CString,
    pub(crate) flags: i32,
    pub(crate) mode: u32,
}

pub(crate) struct StatxPathRequest {
    /// Directory fd for relative paths, or AT_FDCWD for current directory.
    pub(crate) dir_fd: RawFd,
    /// Path to stat. Owned to ensure validity until completion.
    pub(crate) path: CString,
    pub(crate) flags: i32,
    pub(crate) statx_buf: Box<MaybeUninit<libc::statx>>,
}

pub(crate) struct CloseRequest {
    pub(crate) fd: RawFd,
}

pub(crate) struct RenameAtRequest {
    pub(crate) old_dir_fd: RawFd,
    pub(crate) old_path: CString,
    pub(crate) new_dir_fd: RawFd,
    pub(crate) new_path: CString,
    pub(crate) flags: u32,
}

pub(crate) struct UnlinkAtRequest {
    pub(crate) dir_fd: RawFd,
    pub(crate) path: CString,
    pub(crate) flags: i32,
}

pub(crate) struct MkdirAtRequest {
    pub(crate) dir_fd: RawFd,
    pub(crate) path: CString,
    pub(crate) mode: u32,
}

pub(crate) struct SymlinkAtRequest {
    pub(crate) new_dir_fd: RawFd,
    pub(crate) target: CString,
    pub(crate) link_path: CString,
}

pub(crate) struct LinkAtRequest {
    pub(crate) old_dir_fd: RawFd,
    pub(crate) old_path: CString,
    pub(crate) new_dir_fd: RawFd,
    pub(crate) new_path: CString,
    pub(crate) flags: i32,
}
