use super::command::Command;
use crate::{
    PendingIo, Target,
    borrowed_buf::BorrowedBuf,
    client::{
        Client, ClientUring, URING_LEN_MAX, UringTarget,
        completion::{ReadResult, ReadvResult, WriteResult, WritevResult},
        pending_io::{
            fallback::TokioScopedPendingIo,
            fixed_value::FixedValuePendingIo,
            uring::{
                close::UringClose, fadvise::UringFadvise, fallocate::UringFallocate,
                ftruncate::UringFtruncate, link_at::UringLinkAt, mkdir_at::UringMkdirAt,
                open_at::UringOpenAt, read_into::UringReadIntoAt,
                read_into_vectored::UringReadIntoVectoredAt, rename_at::UringRenameAt,
                statx::UringStatx, statx_path::UringStatxPath, symlink_at::UringSymlinkAt,
                sync::UringSync, unlink_at::UringUnlinkAt, write_from::UringWriteFromAt,
                write_from_vectored::UringWriteFromVectoredAt,
            },
        },
        requests::CancelRequest,
        ticketing::SubmissionTicketId,
    },
    helpers,
    iobuf::{IoBuf, IoBufMut},
    metadata::{Metadata, MknodType, Permissions},
};
use core::fmt;
use io_uring::opcode;
use nix::{
    fcntl::{AtFlags, FallocateFlags, FcntlArg, FdFlag, OFlag, PosixFadviseAdvice, RenameFlags},
    sys::{
        stat::{FchmodatFlags, Mode, UtimensatFlags},
        time::TimeSpec,
    },
    unistd::{FchownatFlags, Gid, LinkatFlags, Uid, UnlinkatFlags},
};
use std::{
    cmp::{self, min},
    io::{self, IoSlice, IoSliceMut, SeekFrom},
    mem::MaybeUninit,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd},
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    u32, u64,
};

impl ClientUring {
    pub(crate) fn send(&self, command: Command) {
        self.normal_sender
            .send(command)
            .expect("normal submission thread dead");
    }

    fn send_cancel(&self, ticket_id: SubmissionTicketId) -> oneshot::Receiver<io::Result<()>> {
        let (tx, rx) = oneshot::channel();
        self.cancel_sender
            .send(Command::Cancel {
                req: CancelRequest { id: ticket_id },
                res: tx,
            })
            .expect("cancel submission thread dead");
        rx
    }

    /// Cancel a pending io_uring operation.
    pub(crate) fn cancel_uring(&self, id: SubmissionTicketId) -> io::Result<()> {
        assert!(
            self.probe.is_supported(opcode::AsyncCancel::CODE),
            "async cancel is not supported"
        );
        let rx = self.send_cancel(id);
        rx.recv().expect("uring completion channel dropped")?;
        Ok(())
    }
}

impl Client {
    const AT_FDCWD: BorrowedFd<'static> = unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) };

    fn is_uring_operation_supported(&self, code: u8) -> bool {
        let uring = match self.uring.as_ref() {
            Some(uring) => uring,
            None => return false,
        };
        uring.probe.is_supported(code)
    }

    pub fn is_uring_available_and_active(&self) -> bool {
        self.uring.is_some() && self.uring_enabled.load(Ordering::Relaxed)
    }

    /// Enable or disable io_uring dynamically, which is useful if the target system does not allow io_uring for security reasons. If disabled, the client will fall back to using [`tokio::task::spawn_blocking`] for all operations.
    pub fn enable_uring_operation(&self, enabled: bool) {
        self.uring_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Obtain a raw target object for use in mutable operations, which bypasses mutable checks.
    pub unsafe fn to_target(&self, target: &(impl UringTarget + ?Sized)) -> Target {
        match &self.uring {
            None => Target::Fd(target.as_file_descriptor().as_raw_fd()),
            Some(uring) => unsafe { target.as_target(&uring.identity) },
        }
    }

    /// Indicates how much extra capacity is needed to read the rest of the file.
    async fn _buffer_capacity_required(
        &self,
        file: &mut (impl UringTarget + ?Sized),
    ) -> Option<usize> {
        let size = self
            .metadata(&file.as_file_descriptor())
            .completion()
            .expect("no completion future returned")
            .await
            .ok()?
            .size();
        let pos = self
            .seek(&mut file.as_file_descriptor(), SeekFrom::Current(0))
            .completion()
            .expect("no completion future returned")
            .await
            .ok()?;
        // Don't worry about `usize` overflow because reading will fail regardless
        // in that case.
        Some(size.saturating_sub(pos) as usize)
    }

    /// Read into a user-provided buffer. This is the primitive read operation that accepts any buffer type implementing [`IoBufMut`].
    ///
    /// The buffer is returned along with the number of bytes read. This allows buffer reuse and supports custom allocators (e.g., aligned buffers for O_DIRECT).
    ///
    /// Note that the read data may reside in the spare capacity of the buffer if the specified buffer's capacity is greater than its reported length, so it might be necessary to use a [`Vec::set_len()`] call to force the buffer to resize. It is usually safer to use the [`Self::read_at`] method insteaed.
    pub fn read_into_at<'a, B: IoBufMut + 'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        mut buf: B,
        offset: impl TryInto<u64>,
    ) -> PendingIo<'a, io::Result<ReadResult<B>>> {
        let offset: u64 = match offset.try_into() {
            Ok(offset) => offset,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "offset exceeds u64::MAX",
                ))));
            }
        };
        // Need to cast to off_t
        if offset > i64::MAX as u64 {
            return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ))));
        }
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Read::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringReadIntoAt::new(uring, file, buf, offset))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(
                move || -> io::Result<ReadResult<B>> {
                    let bytes_read = nix::sys::uio::pread(
                        file.as_file_descriptor(),
                        unsafe { buf.as_mut_slice_with_uninit().assume_init_mut() },
                        offset as i64,
                    )?;
                    unsafe {
                        buf.set_len(bytes_read);
                    }
                    Ok(ReadResult { buf, bytes_read })
                },
            ))
        }
    }

    /// Standard-library compatible method for reading from a file at the specified offset using a zero-copy buffer.
    pub fn read_at<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        buf: &'a mut [u8],
        offset: impl TryInto<u64>,
    ) -> PendingIo<'a, io::Result<usize>> {
        match <u32>::try_from(buf.len()) {
            Ok(_) => self
                .read_into_at(file, buf, offset)
                .map(|result| result.map(|result| result.bytes_read)),
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "len exceeds u32::MAX",
                ))));
            }
        }
    }

    /// Read into multiple user-provided buffers at the specified offset. This is the primitive read operation that accepts any buffer type implementing [`IoBufMut`].
    ///
    /// The buffer is returned along with the number of bytes read. This allows buffer reuse and supports custom allocators (e.g., aligned buffers for O_DIRECT).
    pub fn read_into_vectored_at<'a, B: IoBufMut + 'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        mut bufs: Vec<B>,
        offset: impl TryInto<u64>,
    ) -> PendingIo<'a, io::Result<ReadvResult<B>>> {
        let offset: u64 = match offset.try_into() {
            Ok(offset) => offset,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "offset exceeds u64::MAX",
                ))));
            }
        };
        // Need to cast to off_t
        if offset > i64::MAX as u64 {
            return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ))));
        }
        let _io_slices_len: u32 = match bufs.len().try_into() {
            Ok(len) => len,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "count of buffers exceeds u32::MAX",
                ))));
            }
        };
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Readv::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringReadIntoVectoredAt::new(uring, file, bufs, offset))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(
                move || -> io::Result<ReadvResult<B>> {
                    let bytes_read = nix::sys::uio::preadv(
                        file.as_file_descriptor(),
                        &mut unsafe {
                            bufs.iter_mut()
                                .map(|buf| {
                                    IoSliceMut::new(
                                        buf.as_mut_slice_with_uninit().assume_init_mut(),
                                    )
                                })
                                .collect::<Vec<_>>()
                        },
                        offset as i64,
                    )?;
                    let mut cur_bytes_read = bytes_read as usize;
                    for buf in bufs.iter_mut() {
                        let bytes_read_into_target = min(buf.capacity(), cur_bytes_read);
                        unsafe {
                            buf.set_len(bytes_read_into_target);
                        }
                        cur_bytes_read -= bytes_read_into_target;
                    }
                    Ok(ReadvResult {
                        bufs,
                        bytes_read: bytes_read as usize,
                    })
                },
            ))
        }
    }

    /// Read into multiple user-provided buffers at the internal seek cursor and advance the seek position. This is the primitive read operation that accepts any buffer type implementing [`IoBufMut`].
    ///
    /// The buffer is returned along with the number of bytes read. This allows buffer reuse and supports custom allocators (e.g., aligned buffers for O_DIRECT).
    pub fn read_into_vectored<'a, B: IoBufMut + 'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Sync + ?Sized),
        mut bufs: Vec<B>,
    ) -> PendingIo<'a, io::Result<ReadvResult<B>>> {
        let _io_slices_len: u32 = match bufs.len().try_into() {
            Ok(len) => len,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "count of buffers exceeds u32::MAX",
                ))));
            }
        };
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Readv::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringReadIntoVectoredAt::new(
                uring,
                file,
                bufs,
                (-1i64) as u64,
            ))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(
                || -> io::Result<ReadvResult<B>> {
                    let bytes_read = nix::sys::uio::readv(
                        file.as_file_descriptor(),
                        &mut bufs
                            .iter_mut()
                            .map(|buf| {
                                IoSliceMut::new(unsafe {
                                    buf.as_mut_slice_with_uninit().assume_init_mut()
                                })
                            })
                            .collect::<Vec<_>>(),
                    )?;
                    let mut cur_bytes_read = bytes_read as usize;
                    for buf in bufs.iter_mut() {
                        let bytes_read_into_target = min(buf.capacity(), cur_bytes_read);
                        unsafe {
                            buf.set_len(bytes_read_into_target);
                        }
                        cur_bytes_read -= bytes_read_into_target;
                    }
                    Ok(ReadvResult {
                        bufs,
                        bytes_read: bytes_read as usize,
                    })
                },
            ))
        }
    }

    /// Standard-library compatible method for reading from a file at the specified offset using a zero-copy buffer, ensuring that the entire buffer is filled.
    pub async fn read_exact_at<'buf>(
        &self,
        file: &(impl UringTarget + Sync + ?Sized),
        mut buf: &'buf mut [u8],
        offset: impl TryInto<u64>,
    ) -> io::Result<()> {
        let mut offset: u64 = offset
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset exceeds u64::MAX"))?;
        while !buf.is_empty() {
            let res = self
                .read_at(file, buf, offset)
                .completion()
                .expect("no completion future returned")
                .await;
            match res {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        } else {
            Ok(())
        }
    }

    /// Read into a user-provided buffer using the file's internal seek cursor. This is the primitive read operation that accepts any buffertype implementing [`IoBufMut`].
    ///
    /// The buffer is returned along with the number of bytes read. This allows buffer reuse and supports custom allocators (e.g., aligned buffers for O_DIRECT).
    pub fn read_into<'a, B: IoBufMut + 'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        mut buf: B,
    ) -> PendingIo<'a, io::Result<ReadResult<B>>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Read::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringReadIntoAt::new(uring, file, buf, (-1i64) as u64))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(
                move || -> io::Result<ReadResult<B>> {
                    let bytes_read = nix::unistd::read(file.as_file_descriptor(), unsafe {
                        buf.as_mut_slice_with_uninit().assume_init_mut()
                    })?;
                    unsafe {
                        buf.set_len(bytes_read);
                    }
                    Ok(ReadResult { buf, bytes_read })
                },
            ))
        }
    }

    /// Standard library compatible method for reading from a file using a zero-copy buffer and the file's internal seek cursor.
    pub fn read<'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        buf: &'a mut [u8],
    ) -> PendingIo<'a, io::Result<usize>> {
        match <u32>::try_from(buf.len()) {
            Ok(_) => self
                .read_into(file, buf)
                .map(|result| result.map(|result| result.bytes_read)),
            Err(_) => PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "len exceeds u32::MAX",
            )))),
        }
    }

    /// Standard library compatible method for reading from a file into a vector of buffers.
    pub fn read_vectored<'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        bufs: &'a mut [IoSliceMut<'a>],
    ) -> PendingIo<'a, io::Result<usize>> {
        let transformed: Vec<_> = bufs
            .iter_mut()
            .map(|buf| IoSliceMut::new(unsafe { buf.as_mut_slice_with_uninit().assume_init_mut() }))
            .collect();
        self.read_into_vectored(file, transformed)
            .map(|result| result.map(|result| result.bytes_read))
    }

    /// Standard library compatible method for reading from a file into a vector of buffers at a specified offset.
    pub fn read_vectored_at<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        bufs: &'a mut [IoSliceMut<'a>],
        offset: impl TryInto<u64>,
    ) -> PendingIo<'a, io::Result<usize>> {
        let transformed: Vec<_> = bufs
            .iter_mut()
            .map(|buf| IoSliceMut::new(unsafe { buf.as_mut_slice_with_uninit().assume_init_mut() }))
            .collect();
        self.read_into_vectored_at(file, transformed, offset)
            .map(|result| result.map(|result| result.bytes_read))
    }

    /// Standard library compatible method for reading from a file into a vector.
    pub async fn read_to_end<'buf>(
        &self,
        file: &mut (impl UringTarget + Sync + Send + ?Sized),
        buf: &'buf mut Vec<u8>,
    ) -> io::Result<usize> {
        const DEFAULT_BUF_SIZE: usize = 8 * 1024;

        let size_hint = self._buffer_capacity_required(file).await;
        buf.try_reserve(size_hint.unwrap_or(0))?;
        let start_len = buf.len();
        let start_cap = buf.capacity();

        // Optionally limit the maximum bytes read on each iteration.
        // This adds an arbitrary fiddle factor to allow for more data than we expect.
        let mut max_read_size = size_hint
            .and_then(|s| {
                s.checked_add(1024)?
                    .checked_next_multiple_of(DEFAULT_BUF_SIZE)
            })
            .unwrap_or(DEFAULT_BUF_SIZE);

        let mut initialized = 0; // Extra initialized bytes from previous loop iteration

        const PROBE_SIZE: usize = 32;

        async fn small_probe_read(
            instance: &Client,
            file: &mut (impl UringTarget + Sync + Send + ?Sized),
            buf: &mut Vec<u8>,
        ) -> io::Result<usize> {
            let mut probe = [0u8; PROBE_SIZE];

            loop {
                let res = instance
                    .read(file, &mut probe)
                    .completion()
                    .expect("no completion future returned")
                    .await;
                match res {
                    Ok(n) => {
                        // there is no way to recover from allocation failure here
                        // because the data has already been read.
                        buf.extend_from_slice(&probe[..n]);
                        return Ok(n);
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        // avoid inflating empty/small vecs before we have determined that there's anything to read
        if (size_hint.is_none() || size_hint == Some(0)) && buf.capacity() - buf.len() < PROBE_SIZE
        {
            let read = small_probe_read(self, file, buf).await?;

            if read == 0 {
                return Ok(0);
            }
        }

        let mut consecutive_short_reads = 0;

        loop {
            if buf.len() == buf.capacity() && buf.capacity() == start_cap {
                // The buffer might be an exact fit. Let's read into a probe buffer
                // and see if it returns `Ok(0)`. If so, we've avoided an
                // unnecessary doubling of the capacity. But if not, append the
                // probe buffer to the primary buffer and let its capacity grow.
                let read = small_probe_read(self, file, buf).await?;

                if read == 0 {
                    return Ok(buf.len() - start_len);
                }
            }

            if buf.len() == buf.capacity() {
                // buf is full, need more space
                buf.try_reserve(PROBE_SIZE)?;
            }

            let mut spare = buf.spare_capacity_mut();
            // URING_LEN_MAX has to be enforced to prevent issues
            let buf_len = cmp::min(spare.len(), max_read_size).min(URING_LEN_MAX as usize);
            spare = &mut spare[..buf_len];
            let mut read_buf: BorrowedBuf<'_> = spare.into();

            // SAFETY: These bytes were initialized but not filled in the previous loop
            unsafe {
                read_buf.set_init(initialized);
            }

            let mut cursor = read_buf.unfilled();

            let result = loop {
                let res = self
                    .read(file, cursor.reborrow().ensure_init().init_mut())
                    .completion()
                    .expect("no completion future returned")
                    .await;
                match res {
                    Ok(n) => {
                        cursor.advance(n);
                        break Ok(n);
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    // Do not stop now in case of error: we might have received both data
                    // and an error
                    Err(e) => break Err(e),
                }
            };

            let unfilled_but_initialized = cursor.init_mut().len();
            let bytes_read = cursor.written();
            let was_fully_initialized = read_buf.init_len() == buf_len;

            // SAFETY: BorrowedBuf's invariants mean this much memory is initialized.
            unsafe {
                let new_len = bytes_read + buf.len();
                buf.set_len(new_len);
            }

            // Now that all data is pushed to the vector, we can fail without data loss
            result?;

            if bytes_read == 0 {
                return Ok(buf.len() - start_len);
            }

            if bytes_read < buf_len {
                consecutive_short_reads += 1;
            } else {
                consecutive_short_reads = 0;
            }

            // store how much was initialized but not filled
            initialized = unfilled_but_initialized;

            // Use heuristics to determine the max read size if no initial size hint was provided
            if size_hint.is_none() {
                // The reader is returning short reads but it doesn't call ensure_init().
                // In that case we no longer need to restrict read sizes to avoid
                // initialization costs.
                // When reading from disk we usually don't get any short reads except at EOF.
                // So we wait for at least 2 short reads before uncapping the read buffer;
                // this helps with the Windows issue.
                if !was_fully_initialized && consecutive_short_reads > 1 {
                    max_read_size = usize::MAX;
                }

                // we have passed a larger buffer than previously and the
                // reader still hasn't returned a short read
                if buf_len >= max_read_size && bytes_read == buf_len {
                    max_read_size = max_read_size.saturating_mul(2);
                }
            }
        }
    }

    /// Standard library compatible method for reading from a file into a string.
    pub async fn read_to_string<'buf>(
        &self,
        file: &mut (impl UringTarget + Sync + Send + ?Sized),
        str: &'buf mut String,
    ) -> io::Result<usize> {
        unsafe { helpers::append_to_string(str, async |b| self.read_to_end(file, b).await) }.await
    }

    /// Standard library compatible method for reading exactly the number of bytes required to fill the buffer.
    pub async fn read_exact<'buf>(
        &self,
        file: &mut (impl UringTarget + Sync + Send + ?Sized),
        mut buf: &'buf mut [u8],
    ) -> io::Result<()> {
        while !buf.is_empty() {
            let res = self
                .read(file, buf)
                .completion()
                .expect("no completion future returned")
                .await;
            match res {
                Ok(0) => break,
                Ok(n) => {
                    buf = &mut buf[n..];
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        } else {
            Ok(())
        }
    }

    /// Write a buffer to a file at the specified offset. Accepts any buffer type implementing [`IoBuf`].
    ///
    /// The buffer is returned along with the number of bytes written. This allows buffer reuse and supports custom allocators.
    pub fn write_from_at<'a, B: IoBuf + 'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        buf: B,
        offset: impl TryInto<u64>,
    ) -> PendingIo<'a, io::Result<WriteResult<B>>> {
        let offset: u64 = match offset.try_into() {
            Ok(offset) => offset,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "offset exceeds u64::MAX",
                ))));
            }
        };
        // Need to cast to off_t. This also prevents setting offset at -1,
        // which is reserved for seek-based write calls.
        if offset > i64::MAX as u64 {
            return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ))));
        }
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Write::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringWriteFromAt::new(uring, file, buf, offset))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(
                move || -> io::Result<WriteResult<B>> {
                    let res = nix::sys::uio::pwrite(
                        file.as_file_descriptor(),
                        &buf.as_slice(),
                        offset as i64,
                    )?;
                    Ok(WriteResult {
                        bytes_written: res,
                        buf,
                    })
                },
            ))
        }
    }

    /// Write multiple buffers to a file at the specified offset. Accepts any buffer type implementing [`IoBuf`].
    ///
    /// The buffer is returned along with the number of bytes written. This allows buffer reuse and supports custom allocators.
    pub fn write_from_vectored_at<'a, B: IoBuf + 'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        mut bufs: Vec<B>,
        offset: impl TryInto<u64>,
    ) -> PendingIo<'a, io::Result<WritevResult<B>>> {
        let offset: u64 = match offset.try_into() {
            Ok(offset) => offset,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "offset exceeds u64::MAX",
                ))));
            }
        };
        // Need to cast to off_t
        if offset > i64::MAX as u64 {
            return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ))));
        }
        let _io_slices_len: u32 = match bufs.len().try_into() {
            Ok(len) => len,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "count of I/O slices exceeds u32::MAX",
                ))));
            }
        };
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Writev::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringWriteFromVectoredAt::new(uring, file, bufs, offset))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(
                move || -> io::Result<WritevResult<B>> {
                    let slice = bufs
                        .iter_mut()
                        .map(|buf| IoSlice::new(buf.as_slice()))
                        .collect::<Vec<_>>();
                    let bytes_written =
                        nix::sys::uio::pwritev(file.as_file_descriptor(), &slice, offset as i64)?;
                    Ok(WritevResult {
                        bufs,
                        bytes_written,
                    })
                },
            ))
        }
    }

    /// Standard library compatible method for writing to a file using a zero-copy buffer and an offset.
    pub fn write_at<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        buf: &'a [u8],
        offset: impl TryInto<u64>,
    ) -> PendingIo<'a, io::Result<usize>> {
        match <u32>::try_from(buf.len()) {
            Ok(_) => {}
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "len exceeds u32::MAX",
                ))));
            }
        }
        self.write_from_at(file, buf, offset)
            .map(|result| match result {
                Ok(result) => Ok(result.bytes_written),
                Err(e) => return Err(e),
            })
    }

    /// Standard library compatible method for writing multiple buffers to a file at the specified offset.
    pub fn write_vectored_at<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        bufs: &'a [IoSlice<'a>],
        offset: impl TryInto<u64>,
    ) -> PendingIo<'a, io::Result<usize>> {
        let transformed = bufs
            .iter()
            .map(|buf| IoSlice::new(buf.as_slice()))
            .collect::<Vec<_>>();
        self.write_from_vectored_at(file, transformed, offset)
            .map(|result| result.map(|result| result.bytes_written))
    }

    /// Standard library compatible method for writing an entire buffer to a file using a zero-copy buffer and an offset.
    pub async fn write_all_at<'buf>(
        &self,
        file: &(impl UringTarget + Sync + ?Sized),
        mut buf: &'buf [u8],
        offset: impl TryInto<u64>,
    ) -> io::Result<()> {
        let mut offset: u64 = offset
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset exceeds u64::MAX"))?;
        while !buf.is_empty() {
            let res = self
                .write_at(file, buf, offset)
                .completion()
                .expect("no completion future returned")
                .await;
            match res {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Write a buffer to a file using the file's internal seek cursor. Accepts any buffer type implementing [`IoBuf`].
    ///
    /// The buffer is returned along with the number of bytes written. This allows buffer reuse and supports custom allocators.
    pub fn write_from<'a, B: IoBuf + 'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        buf: B,
    ) -> PendingIo<'a, io::Result<WriteResult<B>>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Write::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringWriteFromAt::new(uring, file, buf, (-1i64) as u64))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(
                move || -> io::Result<WriteResult<B>> {
                    let bytes_written =
                        nix::unistd::write(file.as_file_descriptor(), &buf.as_slice())?;
                    Ok(WriteResult { buf, bytes_written })
                },
            ))
        }
    }

    /// Write multiple buffers to a file using the file's internal seek cursor. Accepts any buffer type implementing [`IoBuf`].
    ///
    /// The buffers are returned along with the number of bytes written. This allows buffer reuse and supports custom allocators.
    pub fn write_from_vectored<'a, B: IoBuf + 'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        bufs: Vec<B>,
    ) -> PendingIo<'a, io::Result<WritevResult<B>>> {
        let _io_slices_len: u32 = match bufs.len().try_into() {
            Ok(len) => len,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "count of buffers exceeds u32::MAX",
                ))));
            }
        };
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Writev::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringWriteFromVectoredAt::new(
                uring,
                file,
                bufs,
                (-1i64) as u64,
            ))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(
                move || -> io::Result<WritevResult<B>> {
                    let slices = bufs
                        .iter()
                        .map(|buf| IoSlice::new(buf.as_slice()))
                        .collect::<Vec<_>>();
                    let bytes_written = nix::sys::uio::writev(file.as_file_descriptor(), &slices)?;
                    Ok(WritevResult {
                        bufs,
                        bytes_written,
                    })
                },
            ))
        }
    }

    /// Standard library compatible method for writing a single buffer to a file.
    pub fn write<'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        buf: &'a [u8],
    ) -> PendingIo<'a, io::Result<usize>> {
        self.write_from(file, buf)
            .map(|result| result.map(|result| result.bytes_written))
    }

    /// Standard library compatible method for writing multiple buffers to a file.
    pub fn write_vectored<'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        bufs: &'a [IoSlice<'a>],
    ) -> PendingIo<'a, io::Result<usize>> {
        let transformed = bufs
            .iter()
            .map(|buf| IoSlice::new(buf.as_slice()))
            .collect::<Vec<_>>();
        self.write_from_vectored(file, transformed)
            .map(|result| result.map(|result| result.bytes_written))
    }

    /// Standard-library compatible method for writing everything in a slice to a file.
    pub async fn write_all<'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        mut buf: &'a [u8],
    ) -> io::Result<()> {
        while !buf.is_empty() {
            let res = self
                .write(file, buf)
                .completion()
                .expect("no completion future returned")
                .await;
            match res {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => buf = &buf[n..],
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Standard library compatible method for writing a formatted string to a file.
    pub async fn write_fmt(
        &self,
        file: &mut (impl UringTarget + Sync + Send + ?Sized),
        args: fmt::Arguments<'_>,
    ) -> io::Result<()> {
        self.write_all(file, args.to_string().as_bytes()).await
    }

    /// Standard library compatible method for flushing the file.
    pub fn flush<'a>(
        &'a self,
        _file: &'a mut (impl UringTarget + Send + ?Sized),
    ) -> PendingIo<'a, io::Result<()>> {
        PendingIo::new(FixedValuePendingIo::new(Ok(())))
    }

    /// A more low-level flexible method for seeking to a specific offset in the file, which uses the [`nix`] crate.
    /// This is useful if SEEK_HOLE or SEEK_END is needed.
    pub fn seek_ll<'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Send + ?Sized),
        whence: nix::unistd::Whence,
        offset: impl TryInto<i64>,
    ) -> PendingIo<'a, io::Result<u64>> {
        let offset: i64 = match offset.try_into() {
            Ok(offset) => offset,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "offset exceeds i64::MAX",
                ))));
            }
        };
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<u64> {
            Ok(nix::unistd::lseek(file.as_file_descriptor(), offset, whence)? as u64)
        }))
    }

    /// Standard library compatible method for seeking to a specific offset in the file.
    pub fn seek<'a>(
        &'a self,
        file: &'a mut (impl UringTarget + Send + ?Sized),
        seek: SeekFrom,
    ) -> PendingIo<'a, io::Result<u64>> {
        let (whence, offset) = match seek {
            SeekFrom::Start(offset) => (
                nix::unistd::Whence::SeekSet,
                match offset.try_into() {
                    Ok(offset) => offset,
                    Err(_) => {
                        return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "offset exceeds i64::MAX",
                        ))));
                    }
                },
            ),
            SeekFrom::End(offset) => (nix::unistd::Whence::SeekEnd, offset),
            SeekFrom::Current(offset) => (nix::unistd::Whence::SeekCur, offset),
        };
        self.seek_ll(file, whence, offset)
    }

    /// Synchronize file data and metadata to disk (fsync). This ensures that all data and metadata modifications are flushed to the underlying storage device. Even when using direct I/O, this is necessary to ensure the device itself has flushed any internal caches.
    ///
    /// **Note on ordering**: io_uring does not guarantee ordering between operations. If you need to ensure writes complete before fsync, you should await the write first, then call fsync.
    pub fn sync_all<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
    ) -> PendingIo<'a, io::Result<()>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Fsync::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringSync::new(uring, file, false))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::unistd::fsync(file.as_file_descriptor())?;
                Ok(())
            }))
        }
    }

    /// Synchronize file data to disk (fdatasync). This ensures that only data modifications are flushed to the underlying storage device. This is useful for ensuring that data is written but not metadata.
    pub fn sync_data<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
    ) -> PendingIo<'a, io::Result<()>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Fsync::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringSync::new(uring, file, true))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::unistd::fsync(file.as_file_descriptor())?;
                Ok(())
            }))
        }
    }

    /// Standard library compatible method for getting metadata of an open file handle (statx). This is the io_uring equivalent of the low-level [`libc::statx()`] or [`std::fs::File::metadata`] functions.
    pub fn metadata<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
    ) -> PendingIo<'a, io::Result<Metadata>> {
        let mut statx_buf = Box::new(MaybeUninit::<libc::statx>::uninit());
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Statx::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringStatx::new(uring, file))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(
                move || -> io::Result<Metadata> {
                    helpers::syscall_cvt(unsafe {
                        libc::statx(
                            file.as_file_descriptor().as_raw_fd(),
                            c"".as_ptr(),
                            libc::AT_EMPTY_PATH,
                            libc::STATX_BASIC_STATS,
                            statx_buf.as_mut_ptr(),
                        )
                    })?;
                    Ok(Metadata(unsafe { (*statx_buf).assume_init() }))
                },
            ))
        }
    }

    /// Retrieves metadata from a file with the path relative to the specified directory file descriptor.
    pub fn statx_at<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
        path: impl AsRef<Path>,
        flags: AtFlags,
    ) -> PendingIo<'a, io::Result<Metadata>> {
        let mut statx_buf = Box::new(MaybeUninit::<libc::statx>::uninit());
        let path_cstr = match helpers::path_to_cstring(path.as_ref()) {
            Ok(path_cstr) => path_cstr,
            Err(e) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(e)));
            }
        };
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Statx::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringStatxPath::new(uring, fd, path_cstr, flags))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(
                move || -> io::Result<Metadata> {
                    helpers::syscall_cvt(unsafe {
                        libc::statx(
                            fd.as_file_descriptor().as_raw_fd(),
                            path_cstr.as_ptr(),
                            flags.bits(),
                            libc::STATX_BASIC_STATS,
                            statx_buf.as_mut_ptr(),
                        )
                    })?;
                    Ok(Metadata(unsafe { (*statx_buf).assume_init() }))
                },
            ))
        }
    }

    /// Standard library compatible method for getting metadata of a file path after following all symlinks, equivalent to [`std::fs::metadata`].
    pub fn metadata_path<'a>(
        &'a self,
        path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<Metadata>> {
        self.statx_at(&Self::AT_FDCWD, path, AtFlags::AT_EMPTY_PATH)
    }

    /// Standard library compatible method for getting metadata of a file path without following any symlinks, equivalent to [`std::fs::symlink_metadata`].
    pub fn symlink_metadata_path<'a>(
        &'a self,
        path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<Metadata>> {
        self.statx_at(
            &Self::AT_FDCWD,
            path,
            AtFlags::AT_SYMLINK_NOFOLLOW | AtFlags::AT_EMPTY_PATH,
        )
    }

    /// Pre-allocate or deallocate space for a file (fallocate). This can be used to pre-allocate space to avoid fragmentation, punch holes in sparse files, or zero-fill regions. Use `libc::FALLOC_FL_*` constants for mode flags.
    pub fn fallocate<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        mode: FallocateFlags,
        offset: impl TryInto<u64>,
        len: impl TryInto<u64>,
    ) -> PendingIo<'a, io::Result<()>> {
        let offset: u64 = match offset.try_into() {
            Ok(offset) => offset,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "offset exceeds u64::MAX",
                ))));
            }
        };
        let len: u64 = match len.try_into() {
            Ok(len) => len,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "len exceeds u64::MAX",
                ))));
            }
        };
        if offset > i64::MAX as u64 {
            return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ))));
        }
        if len > i64::MAX as u64 {
            return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "len exceeds i64::MAX",
            ))));
        }
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Fallocate::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringFallocate::new(uring, file, mode, offset, len))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::fcntl::fallocate(
                    file.as_file_descriptor(),
                    nix::fcntl::FallocateFlags::from_bits_retain(mode.bits()),
                    offset as i64,
                    len as i64,
                )?;
                Ok(())
            }))
        }
    }

    /// Advise the kernel about expected file access patterns (fadvise). This is a hint to the kernel about how you intend to access a file region. The kernel may use this to optimize readahead, caching, etc. Use `libc::POSIX_FADV_*` constants for advice values.
    pub fn fadvise<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        offset: impl TryInto<u64>,
        len: impl TryInto<u64>,
        advice: PosixFadviseAdvice,
    ) -> PendingIo<'a, io::Result<()>> {
        let offset: u64 = match offset.try_into() {
            Ok(offset) => offset,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "offset exceeds u64::MAX",
                ))));
            }
        };
        let len: u64 = match len.try_into() {
            Ok(len) => len,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "len exceeds u64::MAX",
                ))));
            }
        };
        if offset > i64::MAX as u64 {
            return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ))));
        }
        if len > i64::MAX as u64 {
            return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "len exceeds i64::MAX",
            ))));
        }
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Fadvise::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringFadvise::new(uring, file, advice, offset, len as i64))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::fcntl::posix_fadvise(
                    file.as_file_descriptor(),
                    offset as i64,
                    len as i64,
                    advice.into(),
                )?;
                Ok(())
            }))
        }
    }

    /// Truncate a file to a specified length (ftruncate). If the file is larger than the specified length, the extra data is lost. If the file is smaller, it is extended and the extended part reads as zeros.
    pub fn ftruncate<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        len: impl TryInto<u64>,
    ) -> PendingIo<'a, io::Result<()>> {
        let len: u64 = match len.try_into() {
            Ok(len) => len,
            Err(_) => {
                return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "len exceeds u64::MAX",
                ))));
            }
        };
        if len > i64::MAX as u64 {
            return PendingIo::new(FixedValuePendingIo::new(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "len exceeds i64::MAX",
            ))));
        }
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Ftruncate::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringFtruncate::new(uring, file, len))
        } else {
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::unistd::ftruncate(file.as_file_descriptor(), len as i64)?;
                Ok(())
            }))
        }
    }

    /// Open a file relative to a directory fd. This is useful for safe path traversal and avoiding TOCTOU races.
    ///
    /// # Arguments
    ///
    /// * `dir_fd` - Directory fd for relative paths, or `libc::AT_FDCWD` for current directory.
    /// * `path` - Path to open relative to `dir_fd`.
    /// * `flags` - Open flags.
    /// * `mode` - File mode for creation.
    ///
    /// # Notes
    /// - This low-level function does not provide the `O_CLOEXEC` flag by default. This may cause file descriptor leaks, so it is recommended to use [`Self::open_at`].
    pub fn open_at_ll<'a>(
        &'a self,
        dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        path: impl AsRef<Path>,
        flags: OFlag,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<OwnedFd>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::OpenAt::CODE)
        {
            let path = match helpers::path_to_cstring(path.as_ref()) {
                Ok(path) => path,
                Err(e) => {
                    return PendingIo::new(FixedValuePendingIo::new(Err(e)));
                }
            };
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringOpenAt::new(
                uring,
                dir_fd,
                path,
                flags,
                Mode::from_bits_retain(permissions.mode()),
            ))
        } else {
            let path_owned = path.as_ref().to_owned();
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<OwnedFd> {
                Ok(nix::fcntl::openat(
                    dir_fd.as_file_descriptor(),
                    &path_owned,
                    OFlag::from_bits_retain(flags.bits()),
                    Mode::from_bits_retain(permissions.mode()),
                )?)
            }))
        }
    }

    /// Open a file asynchronously using the path syntax. This is the io_uring equivalent of `open(2)`.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to open (any `CStr`-like type: `&CStr`, `CString`, `c"literal"`).
    /// * `flags` - Open flags from `libc` (e.g., `libc::O_RDONLY`, `libc::O_RDWR | libc::O_CREAT`).
    /// * `mode` - File mode for creation (only used with `O_CREAT`).
    ///
    /// # Returns
    ///
    /// Returns an `OwnedFd` on success. The fd is automatically closed when dropped.
    ///
    /// # Notes
    /// - This low-level function does not provide the `O_CLOEXEC` flag by default. This may cause file descriptor leaks, so it is recommended to use [`Self::open_path`].
    pub fn open_path_ll<'a>(
        &'a self,
        path: impl AsRef<Path>,
        flags: OFlag,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<OwnedFd>> {
        self.open_at_ll(&Self::AT_FDCWD, path, flags, permissions)
    }

    /// Open a file relative to a directory fd similar to [`Self::open_at_ll`], but with [`O_CLOEXEC`](libc::O_CLOEXEC) flag.
    pub fn open_at<'a>(
        &'a self,
        dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        path: impl AsRef<Path>,
        flags: OFlag,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<OwnedFd>> {
        self.open_at_ll(dir_fd, path, flags | OFlag::O_CLOEXEC, permissions)
    }

    /// Open a file using path syntax similar to [`Self::open_at_ll`]. This provides a convenient interface close to the standard library's [`std::fs::OpenOptions`] object, which includes the [`O_CLOEXEC`](libc::O_CLOEXEC) flag to prevent fd leaks.
    pub fn open_path<'a>(
        &'a self,
        path: impl AsRef<Path>,
        flags: OFlag,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<OwnedFd>> {
        self.open_at_ll(&Self::AT_FDCWD, path, flags | OFlag::O_CLOEXEC, permissions)
    }

    /// Standard library compatible method for opening a file, equivalent to [`std::fs::File::open`] (read-only open API),
    /// which also includes the `O_CLOEXEC` flag to provide parity with the library.
    pub fn open<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<OwnedFd>> {
        self.open_at_ll(
            &Self::AT_FDCWD,
            path,
            OFlag::O_RDONLY | OFlag::O_CLOEXEC,
            Permissions::from_mode(0o666),
        )
    }

    /// Standard library compatible method for creating a file or truncate an already existing file, equivalent to [`std::fs::File::create`], which also includes the [`O_CLOEXEC`](libc::O_CLOEXEC) flag to provide parity with the library.
    pub fn create<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<OwnedFd>> {
        self.open_at_ll(
            &Self::AT_FDCWD,
            path,
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_CLOEXEC,
            Permissions::from_mode(0o666),
        )
    }

    /// Standard library compatible method for creating a file and failing if the file already exists, equivalent to [`std::fs::File::create_new`], which also includes the [`O_CLOEXEC`](libc::O_CLOEXEC) flag to provide parity with the library.
    pub fn create_new<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<OwnedFd>> {
        self.open_at_ll(
            &Self::AT_FDCWD,
            path,
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_CLOEXEC,
            Permissions::from_mode(0o666),
        )
    }

    /// Close a file descriptor asynchronously. This is the io_uring equivalent of `close(2)`
    ///
    /// Takes ownership of the fd to prevent the automatic synchronous close on drop. This is useful when you want to:
    /// - Handle close errors (which are silently ignored by `OwnedFd::drop`)
    /// - Batch close operations with other io_uring operations
    /// - Avoid blocking the async runtime on close
    pub fn close<'a>(
        &'a self,
        fd: impl IntoRawFd + Sized + Send + 'a,
    ) -> PendingIo<'a, io::Result<()>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Close::CODE)
        {
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringClose::new(uring, fd))
        } else {
            let raw_fd = fd.into_raw_fd();
            let owned_fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::unistd::close(owned_fd)?;
                Ok(())
            }))
        }
    }

    /// Rename a file relative to directory fds. This is the io_uring equivalent of `renameat2(2)`.
    ///
    /// Flags can include [`RenameFlags`](nix::fcntl::RenameFlags) constants.
    pub fn rename_at<'a>(
        &'a self,
        old_dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        old_path: impl AsRef<Path>,
        new_dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path>,
        flags: RenameFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::RenameAt::CODE)
        {
            let old_path = match helpers::path_to_cstring(old_path.as_ref()) {
                Ok(old_path) => old_path,
                Err(e) => {
                    return PendingIo::new(FixedValuePendingIo::new(Err(e)));
                }
            };
            let new_path = match helpers::path_to_cstring(new_path.as_ref()) {
                Ok(new_path) => new_path,
                Err(e) => {
                    return PendingIo::new(FixedValuePendingIo::new(Err(e)));
                }
            };
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringRenameAt::new(
                uring, old_dir_fd, old_path, new_dir_fd, new_path, flags,
            ))
        } else {
            let old_path_owned = old_path.as_ref().to_owned();
            let new_path_owned = new_path.as_ref().to_owned();
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::fcntl::renameat2(
                    &old_dir_fd.as_file_descriptor(),
                    &old_path_owned,
                    &new_dir_fd.as_file_descriptor(),
                    &new_path_owned,
                    flags,
                )?;
                Ok(())
            }))
        }
    }

    /// Rename a file asynchronously. This is the io_uring equivalent of `rename(2)`.
    pub fn rename<'a>(
        &'a self,
        old_path: impl AsRef<Path>,
        new_path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<()>> {
        self.rename_at(
            &Self::AT_FDCWD,
            old_path,
            &Self::AT_FDCWD,
            new_path,
            RenameFlags::empty(),
        )
    }

    /// Delete a file or directory relative to a directory fd. This is the io_uring equivalent of `unlinkat(2)`.
    ///
    /// Use [`UnlinkatFlags::RemoveDir`](nix::unistd::UnlinkatFlags::RemoveDir) flag to remove directories.
    pub fn unlink_at<'a>(
        &'a self,
        dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        path: impl AsRef<Path>,
        flags: UnlinkatFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::UnlinkAt::CODE)
        {
            let path = match helpers::path_to_cstring(path.as_ref()) {
                Ok(path) => path,
                Err(e) => {
                    return PendingIo::new(FixedValuePendingIo::new(Err(e)));
                }
            };
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringUnlinkAt::new(uring, dir_fd, path, flags))
        } else {
            let path_owned = path.as_ref().to_owned();
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::unistd::unlinkat(&dir_fd.as_file_descriptor(), &path_owned, flags)?;
                Ok(())
            }))
        }
    }

    /// Delete a file or empty directory. This is the io_uring equivalent of `unlink(2)`.
    pub fn unlink<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<()>> {
        self.unlink_at(&Self::AT_FDCWD, path, UnlinkatFlags::NoRemoveDir)
    }

    /// Delete a directory. This is the io_uring equivalent of `rmdir(2)`.
    pub fn rmdir<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<()>> {
        self.unlink_at(&Self::AT_FDCWD, path, UnlinkatFlags::RemoveDir)
    }

    /// Delete a directory. Alias for [`Self::rmdir`].
    #[inline]
    pub fn remove_dir<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<()>> {
        self.rmdir(path)
    }

    /// Create a directory relative to a directory fd. This is the io_uring equivalent of `mkdirat(2)`.
    pub fn mkdir_at<'a>(
        &'a self,
        dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        path: impl AsRef<Path>,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::MkDirAt::CODE)
        {
            let path = match helpers::path_to_cstring(path.as_ref()) {
                Ok(path) => path,
                Err(e) => {
                    return PendingIo::new(FixedValuePendingIo::new(Err(e)));
                }
            };
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringMkdirAt::new(
                uring,
                dir_fd,
                path,
                Mode::from_bits_retain(permissions.mode()),
            ))
        } else {
            let path_owned = path.as_ref().to_owned();
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::sys::stat::mkdirat(
                    &dir_fd.as_file_descriptor(),
                    &path_owned,
                    Mode::from_bits_retain(permissions.mode()),
                )?;
                Ok(())
            }))
        }
    }

    /// Create a directory using path syntax. This is the io_uring equivalent of `mkdir(2)`.
    pub fn mkdir<'a>(
        &'a self,
        path: impl AsRef<Path>,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        self.mkdir_at(&Self::AT_FDCWD, path, permissions)
    }

    /// Standard library compatible method for creating a directory, equivalent to [`std::fs::create_dir`].
    pub fn create_dir<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<()>> {
        self.mkdir_at(&Self::AT_FDCWD, path, Permissions::from_mode(0o777))
    }

    /// Standard library compatible method for ensuring that a directory exists by creating it and all missing parent directories,
    /// equivalent to [`std::fs::create_dir_all`].
    pub async fn create_dir_all(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();
        if path == Path::new("") {
            return Ok(());
        }

        match self
            .create_dir(path)
            .completion()
            .expect("no completion")
            .await
        {
            Ok(()) => return Ok(()),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) if path.is_dir() => return Ok(()),
            Err(e) => return Err(e),
        }
        match path.parent() {
            Some(p) => Box::pin(self.create_dir_all(p)).await?,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "failed to create whole tree",
                ));
            }
        }
        match self
            .create_dir(path)
            .completion()
            .expect("no completion")
            .await
        {
            Ok(()) => Ok(()),
            Err(_) if path.is_dir() => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Create a directory and all missing parent directories with the specified permissions.
    pub async fn create_dir_all_with_permissions(
        &self,
        path: impl AsRef<Path>,
        permissions: Permissions,
    ) -> io::Result<()> {
        let path = path.as_ref();
        if path == Path::new("") {
            return Ok(());
        }

        match self
            .mkdir(path, permissions)
            .completion()
            .expect("no completion")
            .await
        {
            Ok(()) => return Ok(()),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) if path.is_dir() => return Ok(()),
            Err(e) => return Err(e),
        }
        match path.parent() {
            Some(p) => Box::pin(self.create_dir_all_with_permissions(p, permissions)).await?,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "failed to create whole tree",
                ));
            }
        }
        match self
            .mkdir(path, permissions)
            .completion()
            .expect("no completion")
            .await
        {
            Ok(()) => Ok(()),
            Err(_) if path.is_dir() => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Create a symbolic link relative to a directory fd. This is the io_uring equivalent of `symlinkat(2)`.
    pub fn symlink_at<'a>(
        &'a self,
        target: impl AsRef<Path>,
        new_dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        link_path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<()>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::SymlinkAt::CODE)
        {
            let target = match helpers::path_to_cstring(target.as_ref()) {
                Ok(target) => target,
                Err(e) => {
                    return PendingIo::new(FixedValuePendingIo::new(Err(e)));
                }
            };
            let link_path = match helpers::path_to_cstring(link_path.as_ref()) {
                Ok(link_path) => link_path,
                Err(e) => {
                    return PendingIo::new(FixedValuePendingIo::new(Err(e)));
                }
            };
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringSymlinkAt::new(uring, new_dir_fd, target, link_path))
        } else {
            let target_owned = target.as_ref().to_owned();
            let link_path_owned = link_path.as_ref().to_owned();
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::unistd::symlinkat(
                    &target_owned,
                    new_dir_fd.as_file_descriptor(),
                    &link_path_owned,
                )?;
                Ok(())
            }))
        }
    }

    /// Standard library compatible method for creating a symbolic link, equivalent to [`std::os::unix::fs::symlink`] or `symlink(2)`.
    pub fn symlink<'a>(
        &'a self,
        target: impl AsRef<Path>,
        link_path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<()>> {
        self.symlink_at(target, &Self::AT_FDCWD, link_path)
    }

    /// Create a hard link to the location relative to the target directory at a location relative to the specified directory fd. This is the io_uring equivalent of `linkat(2)`.
    pub fn hard_link_at<'a>(
        &'a self,
        old_dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        old_path: impl AsRef<Path>,
        new_dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path>,
        flags: LinkatFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::LinkAt::CODE)
        {
            let old_path = match helpers::path_to_cstring(old_path.as_ref()) {
                Ok(old_path) => old_path,
                Err(e) => {
                    return PendingIo::new(FixedValuePendingIo::new(Err(e)));
                }
            };
            let new_path = match helpers::path_to_cstring(new_path.as_ref()) {
                Ok(new_path) => new_path,
                Err(e) => {
                    return PendingIo::new(FixedValuePendingIo::new(Err(e)));
                }
            };
            let uring = self.uring.as_ref().expect("uring must be Some");
            PendingIo::new(UringLinkAt::new(
                uring, old_dir_fd, old_path, new_dir_fd, new_path, flags,
            ))
        } else {
            let old_path_owned = old_path.as_ref().to_owned();
            let new_path_owned = new_path.as_ref().to_owned();
            PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
                nix::unistd::linkat(
                    old_dir_fd.as_file_descriptor(),
                    &old_path_owned,
                    new_dir_fd.as_file_descriptor(),
                    &new_path_owned,
                    flags,
                )?;
                Ok(())
            }))
        }
    }

    /// Create a hard link to the open file handle at a location relative to the specified directory fd.
    pub fn hard_link_file_at<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        new_dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<()>> {
        self.hard_link_at(
            file,
            Path::new(""),
            new_dir_fd,
            new_path,
            // Points to the target file.
            LinkatFlags::AT_EMPTY_PATH,
        )
    }

    /// Create a hard link to the open file handle at the specified path.
    pub fn hard_link_file<'a>(
        &'a self,
        file: &'a (impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<()>> {
        self.hard_link_at(
            file,
            Path::new(""),
            &Self::AT_FDCWD,
            new_path,
            LinkatFlags::empty(),
        )
    }

    /// Standard library compatible method for creating a hard link using path syntax. This is the io_uring equivalent of `link(2)`.
    pub fn hard_link<'a>(
        &'a self,
        old_path: impl AsRef<Path>,
        new_path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<()>> {
        self.hard_link_at(
            &Self::AT_FDCWD,
            old_path,
            &Self::AT_FDCWD,
            new_path,
            LinkatFlags::empty(),
        )
    }

    /// Alias for [`Self::fchmod`].
    #[inline]
    pub fn set_permissions<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        self.fchmod(fd, permissions)
    }

    /// Set the permissions of an open handle to a file or directory, which is equivalent to [`std::fs::File::set_permissions`] or `fchmod(2)`.
    pub fn fchmod<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
            nix::sys::stat::fchmod(
                fd.as_file_descriptor(),
                Mode::from_bits_retain(permissions.mode()),
            )?;
            Ok(())
        }))
    }

    /// Set the permissions of a file or directory relative to a directory fd, which is equivalent to `fchmodat(2)`.
    pub fn fchmod_at<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
        path: impl AsRef<Path>,
        permissions: Permissions,
        flags: FchmodatFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        let path = path.as_ref().to_owned();
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
            nix::sys::stat::fchmodat(
                fd.as_file_descriptor(),
                &path,
                Mode::from_bits_retain(permissions.mode()),
                flags,
            )?;
            Ok(())
        }))
    }

    /// Set the permissions of a file or directory using path syntax, equivalent to `chmod(2)`.
    pub fn chmod<'a>(
        &'a self,
        path: impl AsRef<Path>,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        self.fchmod_at(
            &Self::AT_FDCWD,
            path,
            permissions,
            FchmodatFlags::FollowSymlink,
        )
    }

    /// Set the permissions of a file or directory using path syntax, but can target a symlink.
    pub fn lchmod<'a>(
        &'a self,
        path: impl AsRef<Path>,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        self.fchmod_at(
            &Self::AT_FDCWD,
            path,
            permissions,
            FchmodatFlags::NoFollowSymlink,
        )
    }

    /// Set the user and/or group ownership of an open handle to a file or directory, which is equivalent to `fchown(2)`.
    pub fn fchown<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
        uid: Option<Uid>,
        gid: Option<Gid>,
    ) -> PendingIo<'a, io::Result<()>> {
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
            nix::unistd::fchown(fd.as_file_descriptor(), uid, gid)?;
            Ok(())
        }))
    }

    /// Set the user and/or group ownership of a file or directory relative to a directory fd, which is equivalent to `fchownat(2)`.
    pub fn fchown_at<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
        path: impl AsRef<Path>,
        uid: Option<Uid>,
        gid: Option<Gid>,
        flags: FchownatFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        let path = path.as_ref().to_owned();
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
            nix::unistd::fchownat(fd.as_file_descriptor(), &path, uid, gid, flags)?;
            Ok(())
        }))
    }

    /// Set the user and/or group ownership of a file or directory using path syntax, which is equivalent to `chown(2)`.
    pub fn chown<'a>(
        &'a self,
        path: impl AsRef<Path>,
        uid: Option<Uid>,
        gid: Option<Gid>,
    ) -> PendingIo<'a, io::Result<()>> {
        self.fchown_at(
            &Self::AT_FDCWD,
            path,
            uid,
            gid,
            FchownatFlags::AT_EMPTY_PATH,
        )
    }

    /// Set the user and/ or group ownership of a file or directory using path syntax, but can target a symlink.
    pub fn lchown<'a>(
        &'a self,
        path: impl AsRef<Path>,
        uid: Option<Uid>,
        gid: Option<Gid>,
    ) -> PendingIo<'a, io::Result<()>> {
        self.fchown_at(
            &Self::AT_FDCWD,
            path,
            uid,
            gid,
            FchownatFlags::AT_EMPTY_PATH | FchownatFlags::AT_SYMLINK_NOFOLLOW,
        )
    }

    /// Alias for [`Self::futimens`].
    #[inline]
    pub fn set_times<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
    ) -> PendingIo<'a, io::Result<()>> {
        self.futimens(fd, atime, mtime)
    }

    /// Set file times of an open handle to a file or directory, which is equivalent to [`std::fs::File::set_times`] or `futimens(2)`.
    pub fn futimens<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
    ) -> PendingIo<'a, io::Result<()>> {
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
            nix::sys::stat::futimens(
                fd.as_file_descriptor(),
                &atime.unwrap_or(TimeSpec::UTIME_OMIT),
                &mtime.unwrap_or(TimeSpec::UTIME_OMIT),
            )?;
            Ok(())
        }))
    }

    /// Set file times of a file or directory using a path relative to a directory fd, which is equivalent to `utimensat(2)`.
    pub fn utimens_at<'a>(
        &'a self,
        dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        path: impl AsRef<Path>,
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
        flags: UtimensatFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        let path = path.as_ref().to_owned();
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
            nix::sys::stat::utimensat(
                dir_fd.as_file_descriptor(),
                &path,
                &atime.unwrap_or(TimeSpec::UTIME_OMIT),
                &mtime.unwrap_or(TimeSpec::UTIME_OMIT),
                flags,
            )?;
            Ok(())
        }))
    }

    /// Canonicalize a path. This is equivalent to `realpath(3)`.
    pub fn canonicalize<'a>(
        &'a self,
        path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<PathBuf>> {
        let path = path.as_ref().to_owned();
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<PathBuf> {
            std::fs::canonicalize(&path)
        }))
    }

    /// Read a symbolic link. This is equivalent to `readlink(2)`.
    pub fn read_link<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<PathBuf>> {
        self.read_link_at(&Self::AT_FDCWD, path)
    }

    /// Read a symbolic link at a relative path to a directory fd, which is equivalent to `readlinkat(2)`.
    pub fn read_link_at<'a>(
        &'a self,
        dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<PathBuf>> {
        let path = path.as_ref().to_owned();
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<PathBuf> {
            Ok(PathBuf::from(nix::fcntl::readlinkat(
                dir_fd.as_file_descriptor(),
                &path,
            )?))
        }))
    }

    /// Read a symbolic link at a file descriptor opened with [`OFlag::O_PATH`] and [`OFlag::O_NOFOLLOW`].
    pub fn read_link_file<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
    ) -> PendingIo<'a, io::Result<PathBuf>> {
        self.read_link_at(fd, Path::new(""))
    }

    /// Create a device node at a relative path to a directory fd, which is equivalent to `mknodat(2)`.
    pub fn mknod_at<'a>(
        &'a self,
        dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        path: impl AsRef<Path>,
        kind: MknodType,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        let path_owned = path.as_ref().to_owned();
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<()> {
            let (kind, device) = kind.to_sflag_and_device();
            nix::sys::stat::mknodat(
                dir_fd.as_file_descriptor(),
                &path_owned,
                kind,
                Mode::from_bits_retain(permissions.mode()),
                device.into(),
            )?;
            Ok(())
        }))
    }

    /// Create a device node using path syntax, which is equivalent to `mknod(2)`.
    pub fn mknod<'a>(
        &'a self,
        path: impl AsRef<Path>,
        kind: MknodType,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        self.mknod_at(&Self::AT_FDCWD, path, kind, permissions)
    }

    /// Perform a `fcntl` syscall on a file descriptor, which is equivalent to `fcntl(2)`.
    pub fn fcntl<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
        cmd: FcntlArg<'a>,
    ) -> PendingIo<'a, io::Result<i32>> {
        PendingIo::new(TokioScopedPendingIo::new(move || -> io::Result<i32> {
            let res = nix::fcntl::fcntl(fd.as_file_descriptor(), cmd)?;
            Ok(res)
        }))
    }

    /// Set the status flags of a file descriptor, which is equivalent to `F_SETFL(2)`.
    pub fn set_status_flags<'a>(
        &'a self,
        fd: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        flags: OFlag,
    ) -> PendingIo<'a, io::Result<()>> {
        self.fcntl(fd, FcntlArg::F_SETFL(flags))
            .map(|r| r.map(|_| ()))
    }

    /// Get the status flags of a file descriptor, which is equivalent to `F_GETFL(2)`.
    pub fn get_status_flags<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
    ) -> PendingIo<'a, io::Result<OFlag>> {
        self.fcntl(fd, FcntlArg::F_GETFL)
            .map(|r| r.map(|flags| OFlag::from_bits_retain(flags)))
    }

    /// Set the descriptor flags of a file descriptor, which is equivalent to `F_SETFD(2)`.
    pub fn set_descriptor_flags<'a>(
        &'a self,
        fd: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        flags: FdFlag,
    ) -> PendingIo<'a, io::Result<()>> {
        self.fcntl(fd, FcntlArg::F_SETFD(flags))
            .map(|r| r.map(|_| ()))
    }

    /// Get the descriptor flags of a file descriptor, which is equivalent to `F_GETFD(2)`.
    pub fn get_descriptor_flags<'a>(
        &'a self,
        fd: &'a (impl UringTarget + Sync + ?Sized),
    ) -> PendingIo<'a, io::Result<FdFlag>> {
        self.fcntl(fd, FcntlArg::F_GETFD)
            .map(|r| r.map(|flags| FdFlag::from_bits_retain(flags)))
    }

    /// Set the nonblocking status flag of a file descriptor.
    pub async fn set_nonblocking<'a>(
        &'a self,
        fd: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        nonblocking: bool,
    ) -> io::Result<()> {
        let old_flags = self
            .get_status_flags(fd)
            .completion()
            .expect("no completion future returned")
            .await?;
        let new_flags = if nonblocking {
            old_flags | OFlag::O_NONBLOCK
        } else {
            old_flags & !OFlag::O_NONBLOCK
        };
        self.set_status_flags(fd, new_flags)
            .completion()
            .expect("no completion future returned")
            .await?;
        Ok(())
    }

    /// Set the close-on-exec descriptor flag of a file descriptor.
    pub async fn set_close_on_exec<'a>(
        &'a self,
        fd: &'a mut (impl UringTarget + Sync + Send + ?Sized),
        close_on_exec: bool,
    ) -> io::Result<()> {
        let old_flags = self
            .get_descriptor_flags(fd)
            .completion()
            .expect("no completion future returned")
            .await?;
        let new_flags = if close_on_exec {
            old_flags | FdFlag::FD_CLOEXEC
        } else {
            old_flags & !FdFlag::FD_CLOEXEC
        };
        self.set_descriptor_flags(fd, new_flags)
            .completion()
            .expect("no completion future returned")
            .await?;
        Ok(())
    }
}
