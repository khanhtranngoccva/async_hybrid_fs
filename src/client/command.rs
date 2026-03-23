use super::requests::{
    CloseRequest, FadviseRequest, FallocateRequest, FtruncateRequest, LinkAtRequest,
    MkdirAtRequest, OpenAtRequest, ReadRequest, ReadvRequest, RenameAtRequest, StatxPathRequest,
    StatxRequest, SymlinkAtRequest, SyncRequest, Target, UnlinkAtRequest, WriteRequest,
    WritevRequest,
};
use crate::metadata::Metadata;
use io_uring::{opcode, squeue::Entry, types};
use std::{io, os::fd::OwnedFd};
use tokio::sync::oneshot;

pub(crate) enum Command {
    Read {
        req: ReadRequest,
        res: oneshot::Sender<io::Result<u32>>,
    },
    Readv {
        req: ReadvRequest,
        res: oneshot::Sender<io::Result<u32>>,
    },
    Write {
        req: WriteRequest,
        res: oneshot::Sender<io::Result<u32>>,
    },
    Writev {
        req: WritevRequest,
        res: oneshot::Sender<io::Result<u32>>,
    },
    Sync {
        req: SyncRequest,
        res: oneshot::Sender<io::Result<()>>,
    },
    Statx {
        req: StatxRequest,
        res: oneshot::Sender<io::Result<Metadata>>,
    },
    Fallocate {
        req: FallocateRequest,
        res: oneshot::Sender<io::Result<()>>,
    },
    Fadvise {
        req: FadviseRequest,
        res: oneshot::Sender<io::Result<()>>,
    },
    Ftruncate {
        req: FtruncateRequest,
        res: oneshot::Sender<io::Result<()>>,
    },
    OpenAt {
        req: OpenAtRequest,
        res: oneshot::Sender<io::Result<OwnedFd>>,
    },
    StatxPath {
        req: StatxPathRequest,
        res: oneshot::Sender<io::Result<Metadata>>,
    },
    Close {
        req: CloseRequest,
        res: oneshot::Sender<io::Result<()>>,
    },
    RenameAt {
        req: RenameAtRequest,
        res: oneshot::Sender<io::Result<()>>,
    },
    UnlinkAt {
        req: UnlinkAtRequest,
        res: oneshot::Sender<io::Result<()>>,
    },
    MkdirAt {
        req: MkdirAtRequest,
        res: oneshot::Sender<io::Result<()>>,
    },
    SymlinkAt {
        req: SymlinkAtRequest,
        res: oneshot::Sender<io::Result<()>>,
    },
    LinkAt {
        req: LinkAtRequest,
        res: oneshot::Sender<io::Result<()>>,
    },
}

/// Helper to build a submission entry for either Fd or Fixed target.
macro_rules! build_op {
    ($target:expr, | $fd:ident | $op:expr) => {
        match $target {
            Target::Fd(raw) => {
                let $fd = types::Fd(raw);
                $op
            }
            Target::Fixed { index, .. } => {
                let $fd = types::Fixed(index);
                $op
            }
        }
    };
}

/// Helper to build a submission entry that only supports Fd (not Fixed).
macro_rules! build_op_fd_only {
    ($target:expr, | $fd:ident | $op:expr) => {
        match $target {
            Target::Fd(raw) => {
                let $fd = types::Fd(raw);
                $op
            }
            Target::Fixed { raw_fd, .. } => {
                let $fd = types::Fd(raw_fd);
                $op
            }
        }
    };
}

#[inline]
pub fn build_io_uring_entry(command: &Command, id: u64) -> Entry {
    match &command {
        Command::Read { req, .. } => {
            build_op!(req.target, |fd| opcode::Read::new(
                fd,
                req.buf_ptr,
                req.buf_len
            )
            .offset(req.offset)
            .build()
            .user_data(id))
        }
        Command::Readv { req, .. } => {
            build_op!(req.target, |fd| opcode::Readv::new(
                fd,
                req.io_slices,
                req.io_slices_len
            )
            .offset(req.offset)
            .build()
            .user_data(id))
        }
        Command::Write { req, .. } => {
            build_op!(req.target, |fd| opcode::Write::new(
                fd,
                req.buf_ptr,
                req.buf_len
            )
            .offset(req.offset)
            .build()
            .user_data(id))
        }
        Command::Writev { req, .. } => {
            build_op!(req.target, |fd| opcode::Writev::new(
                fd,
                req.io_slices,
                req.io_slices_len
            )
            .offset(req.offset)
            .build()
            .user_data(id))
        }
        Command::Sync { req, .. } => {
            build_op!(req.target, |fd| {
                let mut fsync = opcode::Fsync::new(fd);
                if req.datasync {
                    fsync = fsync.flags(types::FsyncFlags::DATASYNC);
                }
                fsync.build().user_data(id)
            })
        }
        Command::Statx { req, .. } => {
            const STATX_BASIC_STATS: u32 = 0x000007ff; // Request all basic stat fields
            const AT_EMPTY_PATH: i32 = 0x1000; // Interpret fd as the file itself, not a directory
            static EMPTY_PATH: &std::ffi::CStr = c""; // Empty path since we use AT_EMPTY_PATH

            // Cast libc::statx* to types::statx* - the opcode uses an opaque type but the kernel writes the actual statx struct
            let statx_ptr = req.statx_buf.as_ptr() as *mut types::statx;

            // Note: Statx doesn't support Fixed in the io-uring crate, so we fall back to raw fd
            build_op_fd_only!(req.target, |fd| opcode::Statx::new(
                fd,
                EMPTY_PATH.as_ptr(),
                statx_ptr
            )
            .flags(AT_EMPTY_PATH)
            .mask(STATX_BASIC_STATS)
            .build()
            .user_data(id))
        }
        Command::Fallocate { req, .. } => {
            build_op!(req.target, |fd| opcode::Fallocate::new(fd, req.len)
                .offset(req.offset)
                .mode(req.mode)
                .build()
                .user_data(id))
        }
        Command::Fadvise { req, .. } => {
            build_op!(req.target, |fd| opcode::Fadvise::new(
                fd, req.len, req.advice
            )
            .offset(req.offset)
            .build()
            .user_data(id))
        }
        Command::Ftruncate { req, .. } => {
            build_op!(req.target, |fd| opcode::Ftruncate::new(fd, req.len)
                .build()
                .user_data(id))
        }
        Command::OpenAt { req, .. } => {
            opcode::OpenAt::new(types::Fd(req.dir_fd), req.path.as_ptr())
                .flags(req.flags)
                .mode(req.mode)
                .build()
                .user_data(id)
        }
        Command::StatxPath { req, .. } => {
            const STATX_BASIC_STATS: u32 = 0x000007ff;
            let statx_ptr = req.statx_buf.as_ptr() as *mut types::statx;

            opcode::Statx::new(types::Fd(req.dir_fd), req.path.as_ptr(), statx_ptr)
                .flags(req.flags)
                .mask(STATX_BASIC_STATS)
                .build()
                .user_data(id)
        }
        Command::Close { req, .. } => opcode::Close::new(types::Fd(req.fd)).build().user_data(id),
        Command::RenameAt { req, .. } => opcode::RenameAt::new(
            types::Fd(req.old_dir_fd),
            req.old_path.as_ptr(),
            types::Fd(req.new_dir_fd),
            req.new_path.as_ptr(),
        )
        .flags(req.flags)
        .build()
        .user_data(id),
        Command::UnlinkAt { req, .. } => {
            opcode::UnlinkAt::new(types::Fd(req.dir_fd), req.path.as_ptr())
                .flags(req.flags)
                .build()
                .user_data(id)
        }
        Command::MkdirAt { req, .. } => {
            opcode::MkDirAt::new(types::Fd(req.dir_fd), req.path.as_ptr())
                .mode(req.mode)
                .build()
                .user_data(id)
        }
        Command::SymlinkAt { req, .. } => opcode::SymlinkAt::new(
            types::Fd(req.new_dir_fd),
            req.target.as_ptr(),
            req.link_path.as_ptr(),
        )
        .build()
        .user_data(id),
        Command::LinkAt { req, .. } => opcode::LinkAt::new(
            types::Fd(req.old_dir_fd),
            req.old_path.as_ptr(),
            types::Fd(req.new_dir_fd),
            req.new_path.as_ptr(),
        )
        .flags(req.flags)
        .build()
        .user_data(id),
    }
}
