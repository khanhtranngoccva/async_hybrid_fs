use super::command::Command;
use crate::{
    Target,
    borrowed_buf::BorrowedBuf,
    client::{
        Client, URING_LEN_MAX, UringTarget,
        completion::{ReadResult, ReadvResult, WriteResult, WritevResult},
        requests::{
            CloseRequest, FadviseRequest, FallocateRequest, FtruncateRequest, IovecArray,
            LinkAtRequest, MkdirAtRequest, OpenAtRequest, ReadRequest, ReadvRequest,
            RenameAtRequest, StatxPathRequest, StatxRequest, SymlinkAtRequest, SyncRequest,
            UnlinkAtRequest, WriteRequest, WritevRequest,
        },
    },
    flags::MetadataAtFlags,
    helpers,
    iobuf::{IoBuf, IoBufMut},
    metadata::{Metadata, MknodType, Permissions},
};
use core::fmt;
use io_uring::opcode;
use nix::{
    fcntl::{AtFlags, FallocateFlags, OFlag, PosixFadviseAdvice, RenameFlags},
    sys::{
        stat::{FchmodatFlags, Mode, UtimensatFlags},
        time::TimeSpec,
    },
    unistd::{FchownatFlags, Gid, LinkatFlags, Uid, UnlinkatFlags},
};
use std::{
    cmp,
    io::{self, IoSlice, IoSliceMut, SeekFrom},
    mem::MaybeUninit,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd},
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    u32, u64,
};
use tokio::sync::oneshot;

impl Client {
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
            None => Target::Fd(target.as_fd().as_raw_fd()),
            Some(uring) => unsafe { target.as_target(&uring.identity) },
        }
    }

    fn send(&self, command: Command) {
        self.uring
            .as_ref()
            .expect("uring not supported, callers must ensure uring support first")
            .sender
            .send(command)
            .expect("uring submission thread dead");
    }

    /// Indicates how much extra capacity is needed to read the rest of the file.
    async fn _buffer_capacity_required(
        &self,
        file: &mut (impl UringTarget + ?Sized),
    ) -> Option<usize> {
        let size = self.metadata(&file.as_fd()).await.ok()?.size();
        let pos = self
            .seek(&mut file.as_fd(), SeekFrom::Current(0))
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
    pub async fn read_into_at<B: IoBufMut>(
        &self,
        file: &(impl UringTarget + ?Sized),
        mut buf: B,
        offset: impl TryInto<u64>,
    ) -> io::Result<ReadResult<B>> {
        let ptr = buf.as_mut_ptr();
        let cap = buf.capacity();
        let offset: u64 = offset
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset exceeds i64::MAX"))?;
        // Need to cast to off_t
        if offset > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ));
        }
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Read::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            self.send(Command::Read {
                req: ReadRequest {
                    target,
                    buf_ptr: ptr,
                    // u32::MAX
                    buf_len: cap.try_into().unwrap_or(u32::MAX),
                    offset,
                },
                res: tx,
            });
            let bytes_read = rx.await.expect("uring completion channel dropped")?;
            Ok(ReadResult {
                buf,
                bytes_read: bytes_read as usize,
            })
        } else {
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, cap) };
            let raw = file.as_fd().as_raw_fd();
            let bytes_read = tokio::task::spawn_blocking(move || unsafe {
                nix::sys::uio::pread(BorrowedFd::borrow_raw(raw), slice, offset as i64)
            })
            .await??;
            Ok(ReadResult { buf, bytes_read })
        }
    }

    /// Standard-library compatible method for reading from a file at the specified offset using a zero-copy buffer.
    pub async fn read_at<'buf>(
        &self,
        file: &(impl UringTarget + ?Sized),
        buf: &'buf mut [u8],
        offset: impl TryInto<u64>,
    ) -> io::Result<usize> {
        <u32>::try_from(buf.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "len exceeds u32::MAX"))?;
        self.read_into_at(file, buf, offset)
            .await
            .map(|result| result.bytes_read)
    }

    /// Read into multiple user-provided buffers at the specified offset. This is the primitive read operation that accepts any buffer type implementing [`IoBufMut`].
    ///
    /// The buffer is returned along with the number of bytes read. This allows buffer reuse and supports custom allocators (e.g., aligned buffers for O_DIRECT).
    pub async fn read_into_vectored_at<B: IoBufMut>(
        &self,
        file: &(impl UringTarget + ?Sized),
        mut bufs: Vec<B>,
        offset: impl TryInto<u64>,
    ) -> io::Result<ReadvResult<B>> {
        let offset: u64 = offset
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset exceeds u64::MAX"))?;
        // Need to cast to off_t
        if offset > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ));
        }
        let io_slices_len: u32 = bufs.len().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "count of buffers exceeds u32::MAX",
            )
        })?;
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Readv::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            let iovec_buffers = IovecArray(
                bufs.iter_mut()
                    .map(|buf| libc::iovec {
                        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                        iov_len: buf.capacity().into(),
                    })
                    .collect::<Vec<_>>(),
            );
            self.send(Command::Readv {
                req: ReadvRequest {
                    target,
                    io_slices: iovec_buffers.as_ptr(),
                    io_slices_len,
                    offset,
                },
                res: tx,
            });
            let bytes_read = rx.await.expect("uring completion channel dropped")?;
            Ok(ReadvResult {
                bufs,
                bytes_read: bytes_read as usize,
            })
        } else {
            let fd = file.as_fd().as_raw_fd();
            let mut transmuted = bufs
                .iter_mut()
                .map(|buf| {
                    IoSliceMut::<'static>::new(unsafe {
                        std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.capacity())
                    })
                })
                .collect::<Vec<_>>();
            let bytes_read = tokio::task::spawn_blocking(move || unsafe {
                nix::sys::uio::preadv(BorrowedFd::borrow_raw(fd), &mut transmuted, offset as i64)
            })
            .await??;
            Ok(ReadvResult {
                bufs,
                bytes_read: bytes_read as usize,
            })
        }
    }

    /// Read into multiple user-provided buffers at the internal seek cursor and advance the seek position. This is the primitive read operation that accepts any buffer type implementing [`IoBufMut`].
    ///
    /// The buffer is returned along with the number of bytes read. This allows buffer reuse and supports custom allocators (e.g., aligned buffers for O_DIRECT).
    pub async fn read_into_vectored<B: IoBufMut>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        mut bufs: Vec<B>,
    ) -> io::Result<ReadvResult<B>> {
        let io_slices_len: u32 = bufs.len().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "count of buffers exceeds u32::MAX",
            )
        })?;
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Readv::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            let iovec_buffers = IovecArray(
                bufs.iter_mut()
                    .map(|buf| libc::iovec {
                        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                        iov_len: buf.capacity().into(),
                    })
                    .collect::<Vec<_>>(),
            );
            self.send(Command::Readv {
                req: ReadvRequest {
                    target,
                    io_slices: iovec_buffers.as_ptr(),
                    io_slices_len,
                    offset: (-1i64) as u64,
                },
                res: tx,
            });
            let bytes_read = rx.await.expect("uring completion channel dropped")?;
            Ok(ReadvResult {
                bufs,
                bytes_read: bytes_read as usize,
            })
        } else {
            let fd = file.as_fd().as_raw_fd();
            let mut transmuted = bufs
                .iter_mut()
                .map(|buf| {
                    IoSliceMut::<'static>::new(unsafe {
                        std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.capacity())
                    })
                })
                .collect::<Vec<_>>();
            let bytes_read = tokio::task::spawn_blocking(move || unsafe {
                nix::sys::uio::readv(BorrowedFd::borrow_raw(fd), &mut transmuted)
            })
            .await??;
            Ok(ReadvResult {
                bufs,
                bytes_read: bytes_read as usize,
            })
        }
    }

    /// Standard-library compatible method for reading from a file at the specified offset using a zero-copy buffer, ensuring that the entire buffer is filled.
    pub async fn read_exact_at<'buf>(
        &self,
        file: &(impl UringTarget + ?Sized),
        mut buf: &'buf mut [u8],
        offset: impl TryInto<u64>,
    ) -> io::Result<()> {
        let mut offset: u64 = offset
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset exceeds u64::MAX"))?;
        while !buf.is_empty() {
            match self.read_at(file, buf, offset).await {
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
    pub async fn read_into<B: IoBufMut>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        mut buf: B,
    ) -> io::Result<ReadResult<B>> {
        let ptr = buf.as_mut_ptr();
        let cap = buf.capacity();
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Read::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            self.send(Command::Read {
                req: ReadRequest {
                    target,
                    buf_ptr: ptr,
                    // u32::MAX
                    buf_len: cap.try_into().unwrap_or(u32::MAX),
                    // -1 is the offset for seek-based read calls
                    offset: (-1i64) as u64,
                },
                res: tx,
            });
            let bytes_read = rx.await.expect("uring completion channel dropped")?;
            Ok(ReadResult {
                buf,
                bytes_read: bytes_read as usize,
            })
        } else {
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, cap) };
            let raw = file.as_fd().as_raw_fd();
            let bytes_read = tokio::task::spawn_blocking(move || unsafe {
                nix::unistd::read(BorrowedFd::borrow_raw(raw), slice)
            })
            .await??;
            Ok(ReadResult { buf, bytes_read })
        }
    }

    /// Standard library compatible method for reading from a file using a zero-copy buffer and the file's internal seek cursor.
    pub async fn read<'buf>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        buf: &'buf mut [u8],
    ) -> io::Result<usize> {
        <u32>::try_from(buf.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "len exceeds u32::MAX"))?;
        self.read_into(file, buf)
            .await
            .map(|result| result.bytes_read)
    }

    /// Standard library compatible method for reading from a file into a vector of buffers.
    pub async fn read_vectored<'buf>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        bufs: &'buf mut [IoSliceMut<'_>],
    ) -> io::Result<usize> {
        let total_len = bufs.iter().map(|b| b.len()).sum::<usize>();
        <u32>::try_from(total_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "total length exceeds u32::MAX")
        })?;
        let into_form = bufs
            .iter_mut()
            .map(|buf| {
                IoSliceMut::<'static>::new(unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.capacity())
                })
            })
            .collect::<Vec<_>>();
        self.read_into_vectored(file, into_form)
            .await
            .map(|result| result.bytes_read)
    }

    /// Standard library compatible method for reading from a file into a vector of buffers at a specified offset.
    pub async fn read_vectored_at<'buf>(
        &self,
        file: &(impl UringTarget + ?Sized),
        bufs: &'buf mut [IoSliceMut<'_>],
        offset: impl TryInto<u64>,
    ) -> io::Result<usize> {
        let total_len = bufs.iter().map(|b| b.len()).sum::<usize>();
        <u32>::try_from(total_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "total length exceeds u32::MAX")
        })?;
        let into_form = bufs
            .iter_mut()
            .map(|buf| {
                IoSliceMut::<'static>::new(unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.capacity())
                })
            })
            .collect::<Vec<_>>();
        self.read_into_vectored_at(file, into_form, offset)
            .await
            .map(|result| result.bytes_read)
    }

    /// Standard library compatible method for reading from a file into a vector.
    pub async fn read_to_end<'buf>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
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
            file: &mut (impl UringTarget + ?Sized),
            buf: &mut Vec<u8>,
        ) -> io::Result<usize> {
            let mut probe = [0u8; PROBE_SIZE];

            loop {
                match instance.read(file, &mut probe).await {
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
                match self
                    .read(file, cursor.reborrow().ensure_init().init_mut())
                    .await
                {
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
        file: &mut (impl UringTarget + ?Sized),
        str: &'buf mut String,
    ) -> io::Result<usize> {
        unsafe { helpers::append_to_string(str, async |b| self.read_to_end(file, b).await) }.await
    }

    /// Standard library compatible method for reading exactly the number of bytes required to fill the buffer.
    pub async fn read_exact<'buf>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        mut buf: &'buf mut [u8],
    ) -> io::Result<()> {
        while !buf.is_empty() {
            match self.read(file, buf).await {
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
    pub async fn write_from_at<B: IoBuf>(
        &self,
        file: &(impl UringTarget + ?Sized),
        buf: B,
        offset: impl TryInto<u64>,
    ) -> io::Result<WriteResult<B>> {
        let ptr = buf.as_ptr();
        let len = buf.len();
        let offset: u64 = offset
            .try_into()
            .map_err(|_| io::Error::other("offset exceeds u64::MAX"))?;
        // Need to cast to off_t. This also prevents setting offset at -1,
        // which is reserved for seek-based write calls.
        if offset > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ));
        }
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Write::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            self.send(Command::Write {
                req: WriteRequest {
                    target,
                    buf_ptr: ptr,
                    buf_len: len.try_into().unwrap(),
                    offset,
                },
                res: tx,
            });
            let bytes_written = rx.await.expect("uring completion channel dropped")?;
            Ok(WriteResult {
                buf,
                bytes_written: bytes_written as usize,
            })
        } else {
            let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
            let raw = file.as_fd().as_raw_fd();
            let bytes_written = tokio::task::spawn_blocking(move || unsafe {
                nix::unistd::write(BorrowedFd::borrow_raw(raw), slice)
            })
            .await??;
            Ok(WriteResult { buf, bytes_written })
        }
    }

    /// Write multiple buffers to a file at the specified offset. Accepts any buffer type implementing [`IoBuf`].
    ///
    /// The buffer is returned along with the number of bytes written. This allows buffer reuse and supports custom allocators.
    pub async fn write_from_vectored_at<B: IoBuf>(
        &self,
        file: &(impl UringTarget + ?Sized),
        mut bufs: Vec<B>,
        offset: impl TryInto<u64>,
    ) -> io::Result<WritevResult<B>> {
        let offset: u64 = offset
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset exceeds u64::MAX"))?;
        // Need to cast to off_t
        if offset > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ));
        }
        let io_slices_len: u32 = bufs
            .len()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "len exceeds u32::MAX"))?;
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Writev::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            let iovec_buffers = IovecArray(
                bufs.iter_mut()
                    .map(|buf| libc::iovec {
                        iov_base: buf.as_ptr() as *mut libc::c_void,
                        iov_len: buf.len().into(),
                    })
                    .collect::<Vec<_>>(),
            );
            self.send(Command::Writev {
                req: WritevRequest {
                    target,
                    io_slices: iovec_buffers.as_ptr(),
                    io_slices_len,
                    offset,
                },
                res: tx,
            });
            let bytes_written = rx.await.expect("uring completion channel dropped")?;
            Ok(WritevResult {
                bufs,
                bytes_written: bytes_written as usize,
            })
        } else {
            let slice = bufs
                .iter_mut()
                .map(|buf| unsafe {
                    IoSlice::<'static>::new(std::slice::from_raw_parts(buf.as_ptr(), buf.len()))
                })
                .collect::<Vec<_>>();
            let raw = file.as_fd().as_raw_fd();
            let bytes_written = tokio::task::spawn_blocking(move || unsafe {
                nix::sys::uio::pwritev(BorrowedFd::borrow_raw(raw), &slice, offset as i64)
            })
            .await??;
            Ok(WritevResult {
                bufs,
                bytes_written,
            })
        }
    }

    /// Standard library compatible method for writing to a file using a zero-copy buffer and an offset.
    pub async fn write_at<'buf>(
        &self,
        file: &(impl UringTarget + ?Sized),
        buf: &'buf [u8],
        offset: impl TryInto<u64>,
    ) -> io::Result<usize> {
        <u32>::try_from(buf.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "len exceeds u32::MAX"))?;
        self.write_from_at(file, buf, offset)
            .await
            .map(|result| result.bytes_written)
    }

    /// Standard library compatible method for writing multiple buffers to a file at the specified offset.
    pub async fn write_vectored_at<'buf>(
        &self,
        file: &(impl UringTarget + ?Sized),
        bufs: &'buf [IoSlice<'_>],
        offset: impl TryInto<u64>,
    ) -> io::Result<usize> {
        let total_len = bufs.iter().map(|b| b.len()).sum::<usize>();
        <u32>::try_from(total_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "total length exceeds u32::MAX")
        })?;
        let into_form = bufs
            .iter()
            .map(|buf| {
                IoSlice::<'static>::new(unsafe {
                    std::slice::from_raw_parts(buf.as_ptr(), buf.len())
                })
            })
            .collect::<Vec<_>>();
        self.write_from_vectored_at(file, into_form, offset)
            .await
            .map(|result| result.bytes_written)
    }

    /// Standard library compatible method for writing an entire buffer to a file using a zero-copy buffer and an offset.
    pub async fn write_all_at<'buf>(
        &self,
        file: &(impl UringTarget + ?Sized),
        mut buf: &'buf [u8],
        offset: impl TryInto<u64>,
    ) -> io::Result<()> {
        let mut offset: u64 = offset
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset exceeds u64::MAX"))?;
        while !buf.is_empty() {
            match self.write_at(file, buf, offset).await {
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
    pub async fn write_from<B: IoBuf>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        buf: B,
    ) -> io::Result<WriteResult<B>> {
        let ptr = buf.as_ptr();
        let len = buf.len();
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Write::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            self.send(Command::Write {
                req: WriteRequest {
                    target,
                    buf_ptr: ptr,
                    buf_len: len.try_into().unwrap(),
                    offset: (-1i64) as u64,
                },
                res: tx,
            });
            let bytes_written = rx.await.expect("uring completion channel dropped")?;
            Ok(WriteResult {
                buf,
                bytes_written: bytes_written as usize,
            })
        } else {
            let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
            let raw = file.as_fd().as_raw_fd();
            let bytes_written = tokio::task::spawn_blocking(move || unsafe {
                nix::unistd::write(BorrowedFd::borrow_raw(raw), slice)
            })
            .await??;
            Ok(WriteResult { buf, bytes_written })
        }
    }

    /// Write multiple buffers to a file using the file's internal seek cursor. Accepts any buffer type implementing [`IoBuf`].
    ///
    /// The buffers are returned along with the number of bytes written. This allows buffer reuse and supports custom allocators.
    pub async fn write_from_vectored<B: IoBuf>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        bufs: Vec<B>,
    ) -> io::Result<WritevResult<B>> {
        let io_slices_len: u32 = bufs.len().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "count of buffers exceeds u32::MAX",
            )
        })?;
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Write::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            let transmuted = IovecArray(
                bufs.iter()
                    .map(|buf| libc::iovec {
                        iov_base: buf.as_ptr() as *mut libc::c_void,
                        iov_len: buf.len() as usize,
                    })
                    .collect::<Vec<_>>(),
            );
            self.send(Command::Writev {
                req: WritevRequest {
                    target,
                    io_slices: transmuted.as_ptr(),
                    io_slices_len,
                    offset: (-1i64) as u64,
                },
                res: tx,
            });
            let bytes_written = rx.await.expect("uring completion channel dropped")?;
            Ok(WritevResult {
                bufs,
                bytes_written: bytes_written as usize,
            })
        } else {
            let slices = bufs
                .iter()
                .map(|buf| unsafe {
                    IoSlice::<'static>::new(std::slice::from_raw_parts(buf.as_ptr(), buf.len()))
                })
                .collect::<Vec<_>>();
            let raw = file.as_fd().as_raw_fd();
            let bytes_written = tokio::task::spawn_blocking(move || unsafe {
                nix::sys::uio::writev(BorrowedFd::borrow_raw(raw), &slices)
            })
            .await??;
            Ok(WritevResult {
                bufs,
                bytes_written,
            })
        }
    }

    /// Standard-library compatible method for writing to a file using a zero-copy buffer and the file's internal seek cursor.
    pub async fn write<'buf>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        buf: &'buf [u8],
    ) -> io::Result<usize> {
        self.write_from(file, buf)
            .await
            .map(|result| result.bytes_written)
    }

    /// Standard library compatible method for writing multiple buffers to a file.
    pub async fn write_vectored<'buf>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        bufs: &'buf [IoSlice<'_>],
    ) -> io::Result<usize> {
        let total_len = bufs.iter().map(|b| b.len()).sum::<usize>();
        <u32>::try_from(total_len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "total length exceeds u32::MAX")
        })?;
        let into_form = bufs
            .iter()
            .map(|buf| {
                IoSlice::<'static>::new(unsafe {
                    std::slice::from_raw_parts(buf.as_ptr(), buf.len())
                })
            })
            .collect::<Vec<_>>();
        self.write_from_vectored(file, into_form)
            .await
            .map(|result| result.bytes_written)
    }

    /// Standard-library compatible method for writing everything in a slice to a file.
    pub async fn write_all<'buf>(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        mut buf: &'buf [u8],
    ) -> io::Result<()> {
        while !buf.is_empty() {
            match self.write(file, buf).await {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => buf = &buf[n..],
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Standard library compatible method for writing a formatted string to a file.
    pub async fn write_fmt(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        args: fmt::Arguments<'_>,
    ) -> io::Result<()> {
        self.write_all(file, args.to_string().as_bytes()).await
    }

    /// Standard library compatible method for flushing the file.
    pub async fn flush(&self, _file: &mut (impl UringTarget + ?Sized)) -> io::Result<()> {
        Ok(())
    }

    /// A more low-level flexible method for seeking to a specific offset in the file, which uses the [`nix`] crate.
    /// This is useful if SEEK_HOLE or SEEK_END is needed.
    pub async fn seek_ll(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        whence: nix::unistd::Whence,
        offset: impl TryInto<i64>,
    ) -> io::Result<u64> {
        let offset: i64 = offset
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset exceeds i64::MAX"))?;
        let fd = file.as_fd().as_raw_fd();
        let new_offset = tokio::task::spawn_blocking(move || unsafe {
            nix::unistd::lseek(BorrowedFd::borrow_raw(fd), offset, whence)
        })
        .await??;
        Ok(new_offset as u64)
    }

    /// Standard library compatible method for seeking to a specific offset in the file.
    pub async fn seek(
        &self,
        file: &mut (impl UringTarget + ?Sized),
        seek: SeekFrom,
    ) -> io::Result<u64> {
        let fd = file.as_fd().as_raw_fd();
        let (whence, offset) = match seek {
            SeekFrom::Start(offset) => (
                nix::unistd::Whence::SeekSet,
                <i64>::try_from(offset).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "offset exceeds i64::MAX")
                })?,
            ),
            SeekFrom::End(offset) => (nix::unistd::Whence::SeekEnd, offset as i64),
            SeekFrom::Current(offset) => (nix::unistd::Whence::SeekCur, offset as i64),
        };
        // io_uring does not have a method to seek the file, fallback to spawn_blocking
        let new_offset = tokio::task::spawn_blocking(move || unsafe {
            nix::unistd::lseek(BorrowedFd::borrow_raw(fd), offset, whence)
        })
        .await??;
        Ok(new_offset as u64)
    }

    /// Synchronize file data and metadata to disk (fsync). This ensures that all data and metadata modifications are flushed to the underlying storage device. Even when using direct I/O, this is necessary to ensure the device itself has flushed any internal caches.
    ///
    /// **Note on ordering**: io_uring does not guarantee ordering between operations. If you need to ensure writes complete before fsync, you should await the write first, then call fsync.
    pub async fn sync_all(&self, file: &(impl UringTarget + ?Sized)) -> io::Result<()> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Fsync::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            self.send(Command::Sync {
                req: SyncRequest {
                    target,
                    datasync: false,
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let fd = file.as_fd().as_raw_fd();
            tokio::task::spawn_blocking(move || unsafe {
                nix::unistd::fsync(BorrowedFd::borrow_raw(fd))
            })
            .await??;
            Ok(())
        }
    }

    /// Synchronize file data to disk (fdatasync). This ensures that only data modifications are flushed to the underlying storage device. This is useful for ensuring that data is written but not metadata.
    ///
    /// **Note on ordering**: io_uring does not guarantee ordering between operations. If you need to ensure writes complete before fdatasync, you should await the write first, then call fdatasync.
    pub async fn sync_data(&self, file: &(impl UringTarget + ?Sized)) -> io::Result<()> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Fsync::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            self.send(Command::Sync {
                req: SyncRequest {
                    target,
                    datasync: true,
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let fd = file.as_fd().as_raw_fd();
            tokio::task::spawn_blocking(move || unsafe {
                nix::unistd::fdatasync(BorrowedFd::borrow_raw(fd))
            })
            .await??;
            Ok(())
        }
    }

    /// Standard library compatible method for getting metadata of an open file handle (statx). This is the io_uring equivalent of the low-level [`libc::statx()`] or [`std::fs::File::metadata`] functions.
    pub async fn metadata(&self, file: &(impl UringTarget + ?Sized)) -> io::Result<Metadata> {
        let mut statx_buf = Box::new(MaybeUninit::<libc::statx>::uninit());
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Statx::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            self.send(Command::Statx {
                req: StatxRequest {
                    target,
                    statx_buf: statx_buf,
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let fd = file.as_fd().as_raw_fd();
            let metadata = tokio::task::spawn_blocking(move || -> io::Result<Metadata> {
                unsafe {
                    helpers::syscall_cvt(libc::statx(
                        fd,
                        c"".as_ptr(),
                        libc::AT_EMPTY_PATH,
                        libc::STATX_BASIC_STATS,
                        statx_buf.as_mut_ptr(),
                    ))?;
                    let statx = (*statx_buf).assume_init();
                    Ok(Metadata(statx))
                }
            })
            .await??;
            Ok(metadata)
        }
    }

    /// Retrieves metadata from a file with the path relative to the specified directory file descriptor.
    pub async fn statx_at(
        &self,
        fd: &(impl UringTarget + ?Sized),
        path: impl AsRef<Path>,
        flags: MetadataAtFlags,
    ) -> io::Result<Metadata> {
        let mut statx_buf = Box::new(MaybeUninit::<libc::statx>::uninit());
        let path_cstr = helpers::path_to_cstring(path.as_ref())?;
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Statx::CODE)
        {
            let (tx, rx) = oneshot::channel();
            self.send(Command::StatxPath {
                req: StatxPathRequest {
                    dir_fd: fd.as_fd().as_raw_fd(),
                    flags: flags.bits(),
                    path: path_cstr,
                    statx_buf: statx_buf,
                },
                res: tx,
            });
            let metadata = rx.await.expect("uring completion channel dropped")?;
            Ok(metadata)
        } else {
            let fd = fd.as_fd().as_raw_fd();
            let metadata = tokio::task::spawn_blocking(move || -> io::Result<Metadata> {
                unsafe {
                    helpers::syscall_cvt(libc::statx(
                        fd,
                        path_cstr.as_ptr(),
                        flags.bits(),
                        libc::STATX_BASIC_STATS,
                        statx_buf.as_mut_ptr(),
                    ))?;
                    let statx = (*statx_buf).assume_init();
                    Ok(Metadata(statx))
                }
            })
            .await??;
            Ok(metadata)
        }
    }

    /// Standard library compatible method for getting metadata of a file path after following all symlinks, equivalent to [`std::fs::metadata`].
    pub async fn metadata_path(&self, path: impl AsRef<Path>) -> io::Result<Metadata> {
        self.statx_at(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            MetadataAtFlags::AT_EMPTY_PATH,
        )
        .await
    }

    /// Standard library compatible method for getting metadata of a file path without following any symlinks, equivalent to [`std::fs::symlink_metadata`].
    pub async fn symlink_metadata_path(&self, path: impl AsRef<Path>) -> io::Result<Metadata> {
        self.statx_at(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            MetadataAtFlags::AT_SYMLINK_NOFOLLOW | MetadataAtFlags::AT_EMPTY_PATH,
        )
        .await
    }

    /// Pre-allocate or deallocate space for a file (fallocate). This can be used to pre-allocate space to avoid fragmentation, punch holes in sparse files, or zero-fill regions. Use `libc::FALLOC_FL_*` constants for mode flags.
    pub async fn fallocate(
        &self,
        file: &(impl UringTarget + ?Sized),
        mode: FallocateFlags,
        offset: impl TryInto<u64>,
        len: impl TryInto<u64>,
    ) -> io::Result<()> {
        let offset: u64 = offset
            .try_into()
            .map_err(|_| io::Error::other("offset exceeds u64::MAX"))?;
        if offset > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ));
        }
        let len: u64 = len
            .try_into()
            .map_err(|_| io::Error::other("len exceeds u64::MAX"))?;
        if len > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "len exceeds i64::MAX",
            ));
        }
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Fallocate::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            self.send(Command::Fallocate {
                req: FallocateRequest {
                    target,
                    offset,
                    len,
                    mode: mode.bits(),
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let fd = file.as_fd().as_raw_fd();
            tokio::task::spawn_blocking(move || unsafe {
                nix::fcntl::fallocate(
                    BorrowedFd::borrow_raw(fd),
                    nix::fcntl::FallocateFlags::from_bits_retain(mode.bits()),
                    offset as i64,
                    len as i64,
                )
            })
            .await??;
            Ok(())
        }
    }

    /// Advise the kernel about expected file access patterns (fadvise). This is a hint to the kernel about how you intend to access a file region. The kernel may use this to optimize readahead, caching, etc. Use `libc::POSIX_FADV_*` constants for advice values.
    pub async fn fadvise(
        &self,
        file: &(impl UringTarget + ?Sized),
        offset: impl TryInto<u64>,
        len: impl TryInto<i64>,
        advice: PosixFadviseAdvice,
    ) -> io::Result<()> {
        let offset: u64 = offset
            .try_into()
            .map_err(|_| io::Error::other("offset exceeds u64::MAX"))?;
        let len: i64 = len
            .try_into()
            .map_err(|_| io::Error::other("len exceeds i64::MAX"))?;
        if offset > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX",
            ));
        }
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Fadvise::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            self.send(Command::Fadvise {
                req: FadviseRequest {
                    target,
                    offset,
                    len,
                    advice: advice as i32,
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let fd = file.as_fd().as_raw_fd();
            tokio::task::spawn_blocking(move || unsafe {
                nix::fcntl::posix_fadvise(
                    BorrowedFd::borrow_raw(fd),
                    offset as i64,
                    len as i64,
                    advice.into(),
                )
            })
            .await??;
            Ok(())
        }
    }

    /// Truncate a file to a specified length (ftruncate). If the file is larger than the specified length, the extra data is lost. If the file is smaller, it is extended and the extended part reads as zeros.
    pub async fn ftruncate(
        &self,
        file: &(impl UringTarget + ?Sized),
        len: impl TryInto<u64>,
    ) -> io::Result<()> {
        let len: u64 = len
            .try_into()
            .map_err(|_| io::Error::other("len exceeds u64::MAX"))?;
        if len > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "len exceeds i64::MAX",
            ));
        }
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Ftruncate::CODE)
        {
            let uring = self.uring.as_ref().expect(
                "should be able to resolve uring because is_uring_operation_supported() is true",
            );
            let target = unsafe { file.as_target(&uring.identity) };
            let (tx, rx) = oneshot::channel();
            self.send(Command::Ftruncate {
                req: FtruncateRequest { target, len },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let fd = file.as_fd().as_raw_fd();
            tokio::task::spawn_blocking(move || unsafe {
                nix::unistd::ftruncate(BorrowedFd::borrow_raw(fd), len as i64)
            })
            .await??;
            Ok(())
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
    pub async fn open_at_ll(
        &self,
        dir_fd: &(impl UringTarget + ?Sized),
        path: impl AsRef<Path>,
        flags: OFlag,
        permissions: Permissions,
    ) -> io::Result<OwnedFd> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::OpenAt::CODE)
        {
            let path = helpers::path_to_cstring(path.as_ref())?;
            let (tx, rx) = oneshot::channel();
            self.send(Command::OpenAt {
                req: OpenAtRequest {
                    dir_fd: dir_fd.as_fd().as_raw_fd(),
                    path,
                    flags: flags.bits(),
                    mode: permissions.mode(),
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let raw_fd = dir_fd.as_fd().as_raw_fd();
            let path = path.as_ref().to_owned();
            let fd = tokio::task::spawn_blocking(move || {
                nix::fcntl::openat(
                    unsafe { BorrowedFd::borrow_raw(raw_fd) },
                    &path,
                    OFlag::from_bits_retain(flags.bits()),
                    Mode::from_bits_retain(permissions.mode()),
                )
            })
            .await??;
            Ok(fd)
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
    pub async fn open_path_ll(
        &self,
        path: impl AsRef<Path>,
        flags: OFlag,
        permissions: Permissions,
    ) -> io::Result<OwnedFd> {
        self.open_at_ll(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            flags,
            permissions,
        )
        .await
    }

    /// Open a file relative to a directory fd similar to [`Self::open_at_ll`], but with [`O_CLOEXEC`](libc::O_CLOEXEC) flag.
    pub async fn open_at(
        &self,
        dir_fd: &(impl UringTarget + ?Sized),
        path: impl AsRef<Path>,
        flags: OFlag,
        permissions: Permissions,
    ) -> io::Result<OwnedFd> {
        self.open_at_ll(dir_fd, path, flags | OFlag::O_CLOEXEC, permissions)
            .await
    }

    /// Open a file using path syntax similar to [`Self::open_at_ll`]. This provides a convenient interface close to the standard library's [`std::fs::OpenOptions`] object, which includes the [`O_CLOEXEC`](libc::O_CLOEXEC) flag to prevent fd leaks.
    pub async fn open_path(
        &self,
        path: impl AsRef<Path>,
        flags: OFlag,
        permissions: Permissions,
    ) -> io::Result<OwnedFd> {
        self.open_at_ll(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            flags | OFlag::O_CLOEXEC,
            permissions,
        )
        .await
    }

    /// Standard library compatible method for opening a file, equivalent to [`std::fs::File::open`] (read-only open API),
    /// which also includes the `O_CLOEXEC` flag to provide parity with the library.
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<OwnedFd> {
        self.open_at_ll(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            OFlag::O_RDONLY | OFlag::O_CLOEXEC,
            Permissions::from_mode(0o666),
        )
        .await
    }

    /// Standard library compatible method for creating a file or truncate an already existing file, equivalent to [`std::fs::File::create`], which also includes the [`O_CLOEXEC`](libc::O_CLOEXEC) flag to provide parity with the library.
    pub async fn create(&self, path: impl AsRef<Path>) -> io::Result<OwnedFd> {
        self.open_at_ll(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_CLOEXEC,
            Permissions::from_mode(0o666),
        )
        .await
    }

    /// Standard library compatible method for creating a file and failing if the file already exists, equivalent to [`std::fs::File::create_new`], which also includes the [`O_CLOEXEC`](libc::O_CLOEXEC) flag to provide parity with the library.
    pub async fn create_new(&self, path: impl AsRef<Path>) -> io::Result<OwnedFd> {
        self.open_at_ll(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_CLOEXEC,
            Permissions::from_mode(0o666),
        )
        .await
    }

    /// Close a file descriptor asynchronously. This is the io_uring equivalent of `close(2)`
    ///
    /// Takes ownership of the fd to prevent the automatic synchronous close on drop. This is useful when you want to:
    /// - Handle close errors (which are silently ignored by `OwnedFd::drop`)
    /// - Batch close operations with other io_uring operations
    /// - Avoid blocking the async runtime on close
    pub async fn close(&self, fd: impl IntoRawFd) -> io::Result<()> {
        let raw_fd = fd.into_raw_fd();
        let (tx, rx) = oneshot::channel();
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::Close::CODE)
        {
            self.send(Command::Close {
                req: CloseRequest { fd: raw_fd },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            tokio::task::spawn_blocking(move || {
                nix::unistd::close(unsafe { OwnedFd::from_raw_fd(raw_fd) })
            })
            .await??;
            Ok(())
        }
    }

    /// Rename a file relative to directory fds. This is the io_uring equivalent of `renameat2(2)`.
    ///
    /// Flags can include [`RenameFlags`](nix::fcntl::RenameFlags) constants.
    pub async fn rename_at(
        &self,
        old_dir_fd: &(impl UringTarget + ?Sized),
        old_path: impl AsRef<Path>,
        new_dir_fd: &(impl UringTarget + ?Sized),
        new_path: impl AsRef<Path>,
        flags: RenameFlags,
    ) -> io::Result<()> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::RenameAt::CODE)
        {
            let old_path = helpers::path_to_cstring(old_path.as_ref())?;
            let new_path = helpers::path_to_cstring(new_path.as_ref())?;
            let (tx, rx) = oneshot::channel();
            self.send(Command::RenameAt {
                req: RenameAtRequest {
                    old_dir_fd: old_dir_fd.as_fd().as_raw_fd(),
                    old_path,
                    new_dir_fd: new_dir_fd.as_fd().as_raw_fd(),
                    new_path,
                    flags: flags.bits(),
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let old_dir_fd = old_dir_fd.as_fd().as_raw_fd();
            let new_dir_fd = new_dir_fd.as_fd().as_raw_fd();
            let old_path = old_path.as_ref().to_owned();
            let new_path = new_path.as_ref().to_owned();
            tokio::task::spawn_blocking(move || unsafe {
                nix::fcntl::renameat2(
                    BorrowedFd::borrow_raw(old_dir_fd),
                    &old_path,
                    BorrowedFd::borrow_raw(new_dir_fd),
                    &new_path,
                    RenameFlags::from_bits_retain(flags.bits()),
                )
            })
            .await??;
            Ok(())
        }
    }

    /// Rename a file asynchronously. This is the io_uring equivalent of `rename(2)`.
    pub async fn rename(
        &self,
        old_path: impl AsRef<Path>,
        new_path: impl AsRef<Path>,
    ) -> io::Result<()> {
        self.rename_at(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            old_path,
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            new_path,
            RenameFlags::empty(),
        )
        .await
    }

    /// Delete a file or directory relative to a directory fd. This is the io_uring equivalent of `unlinkat(2)`.
    ///
    /// Use [`UnlinkatFlags::RemoveDir`](nix::unistd::UnlinkatFlags::RemoveDir) flag to remove directories.
    pub async fn unlink_at(
        &self,
        dir_fd: &(impl UringTarget + ?Sized),
        path: impl AsRef<Path>,
        flags: UnlinkatFlags,
    ) -> io::Result<()> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::UnlinkAt::CODE)
        {
            let path = helpers::path_to_cstring(path.as_ref())?;
            let (tx, rx) = oneshot::channel();
            self.send(Command::UnlinkAt {
                req: UnlinkAtRequest {
                    dir_fd: dir_fd.as_fd().as_raw_fd(),
                    path,
                    flags: match flags {
                        UnlinkatFlags::RemoveDir => libc::AT_REMOVEDIR,
                        UnlinkatFlags::NoRemoveDir => 0,
                    },
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let dir_fd = dir_fd.as_fd().as_raw_fd();
            let path = path.as_ref().to_owned();
            tokio::task::spawn_blocking(move || unsafe {
                nix::unistd::unlinkat(BorrowedFd::borrow_raw(dir_fd), &path, flags)
            })
            .await??;
            Ok(())
        }
    }

    /// Delete a file or empty directory. This is the io_uring equivalent of `unlink(2)`.
    pub async fn unlink(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.unlink_at(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            UnlinkatFlags::NoRemoveDir,
        )
        .await
    }

    /// Delete a directory. This is the io_uring equivalent of `rmdir(2)`.
    pub async fn rmdir(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.unlink_at(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            UnlinkatFlags::RemoveDir,
        )
        .await
    }

    /// Delete a directory. Alias for [`Self::rmdir`].
    #[inline]
    pub async fn remove_dir(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.rmdir(path).await
    }

    /// Create a directory relative to a directory fd. This is the io_uring equivalent of `mkdirat(2)`.
    pub async fn mkdir_at(
        &self,
        dir_fd: &(impl UringTarget + ?Sized),
        path: impl AsRef<Path>,
        permissions: Permissions,
    ) -> io::Result<()> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::MkDirAt::CODE)
        {
            let path = helpers::path_to_cstring(path.as_ref())?;
            let (tx, rx) = oneshot::channel();
            self.send(Command::MkdirAt {
                req: MkdirAtRequest {
                    dir_fd: dir_fd.as_fd().as_raw_fd(),
                    path,
                    mode: permissions.mode(),
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let dir_fd = dir_fd.as_fd().as_raw_fd();
            let path = path.as_ref().to_owned();
            tokio::task::spawn_blocking(move || unsafe {
                nix::sys::stat::mkdirat(
                    BorrowedFd::borrow_raw(dir_fd),
                    &path,
                    Mode::from_bits_retain(permissions.mode()),
                )
            })
            .await??;
            Ok(())
        }
    }

    /// Create a directory using path syntax. This is the io_uring equivalent of `mkdir(2)`.
    pub async fn mkdir(&self, path: impl AsRef<Path>, permissions: Permissions) -> io::Result<()> {
        self.mkdir_at(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            permissions,
        )
        .await
    }

    /// Standard library compatible method for creating a directory, equivalent to [`std::fs::create_dir`].
    pub async fn create_dir(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.mkdir_at(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            Permissions::from_mode(0o777),
        )
        .await
    }

    /// Standard library compatible method for ensuring that a directory exists by creating it and all missing parent directories,
    /// equivalent to [`std::fs::create_dir_all`].
    pub async fn create_dir_all(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();
        if path == Path::new("") {
            return Ok(());
        }

        match self.create_dir(path).await {
            Ok(()) => return Ok(()),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) if path.is_dir() => return Ok(()),
            Err(e) => return Err(e),
        }
        match path.parent() {
            Some(p) => self.create_dir_all(p).await?,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "failed to create whole tree",
                ));
            }
        }
        match self.create_dir(path).await {
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

        match self.mkdir(path, permissions).await {
            Ok(()) => return Ok(()),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) if path.is_dir() => return Ok(()),
            Err(e) => return Err(e),
        }
        match path.parent() {
            Some(p) => self.create_dir_all_with_permissions(p, permissions).await?,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "failed to create whole tree",
                ));
            }
        }
        match self.mkdir(path, permissions).await {
            Ok(()) => Ok(()),
            Err(_) if path.is_dir() => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Create a symbolic link relative to a directory fd. This is the io_uring equivalent of `symlinkat(2)`.
    pub async fn symlink_at(
        &self,
        target: impl AsRef<Path>,
        new_dir_fd: &(impl UringTarget + ?Sized),
        link_path: impl AsRef<Path>,
    ) -> io::Result<()> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::SymlinkAt::CODE)
        {
            let target = helpers::path_to_cstring(target.as_ref())?;
            let link_path = helpers::path_to_cstring(link_path.as_ref())?;
            let (tx, rx) = oneshot::channel();
            self.send(Command::SymlinkAt {
                req: SymlinkAtRequest {
                    target,
                    new_dir_fd: new_dir_fd.as_fd().as_raw_fd(),
                    link_path,
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let target = target.as_ref().to_owned();
            let new_dir_fd = new_dir_fd.as_fd().as_raw_fd();
            let link_path = link_path.as_ref().to_owned();
            tokio::task::spawn_blocking(move || unsafe {
                nix::unistd::symlinkat(&target, BorrowedFd::borrow_raw(new_dir_fd), &link_path)
            })
            .await??;
            Ok(())
        }
    }

    /// Standard library compatible method for creating a symbolic link, equivalent to [`std::os::unix::fs::symlink`] or `symlink(2)`.
    pub async fn symlink(
        &self,
        target: impl AsRef<Path>,
        link_path: impl AsRef<Path>,
    ) -> io::Result<()> {
        self.symlink_at(
            target,
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            link_path,
        )
        .await
    }

    /// Create a hard link to the location relative to the target directory at a location relative to the specified directory fd. This is the io_uring equivalent of `linkat(2)`.
    pub async fn hard_link_at(
        &self,
        old_dir_fd: &(impl UringTarget + ?Sized),
        old_path: impl AsRef<Path>,
        new_dir_fd: &(impl UringTarget + ?Sized),
        new_path: impl AsRef<Path>,
        flags: LinkatFlags,
    ) -> io::Result<()> {
        if self.is_uring_available_and_active()
            && self.is_uring_operation_supported(opcode::LinkAt::CODE)
        {
            let old_path = helpers::path_to_cstring(old_path.as_ref())?;
            let new_path = helpers::path_to_cstring(new_path.as_ref())?;
            let (tx, rx) = oneshot::channel();
            self.send(Command::LinkAt {
                req: LinkAtRequest {
                    old_dir_fd: old_dir_fd.as_fd().as_raw_fd(),
                    old_path,
                    new_dir_fd: new_dir_fd.as_fd().as_raw_fd(),
                    new_path,
                    flags: flags.bits(),
                },
                res: tx,
            });
            rx.await.expect("uring completion channel dropped")
        } else {
            let old_dir_fd = old_dir_fd.as_fd().as_raw_fd();
            let new_dir_fd = new_dir_fd.as_fd().as_raw_fd();
            let old_path = old_path.as_ref().to_owned();
            let new_path = new_path.as_ref().to_owned();
            tokio::task::spawn_blocking(move || unsafe {
                nix::unistd::linkat(
                    BorrowedFd::borrow_raw(old_dir_fd),
                    &old_path,
                    BorrowedFd::borrow_raw(new_dir_fd),
                    &new_path,
                    flags,
                )
            })
            .await??;
            Ok(())
        }
    }

    /// Create a hard link to the open file handle at a location relative to the specified directory fd.
    pub async fn hard_link_file_at(
        &self,
        file: &(impl UringTarget + ?Sized),
        new_dir_fd: &(impl UringTarget + ?Sized),
        new_path: impl AsRef<Path>,
    ) -> io::Result<()> {
        self.hard_link_at(
            file,
            Path::new(""),
            new_dir_fd,
            new_path,
            // Points to the target file.
            LinkatFlags::AT_EMPTY_PATH,
        )
        .await
    }

    /// Create a hard link to the open file handle at the specified path.
    pub async fn hard_link_file(
        &self,
        file: &(impl UringTarget + ?Sized),
        new_path: impl AsRef<Path>,
    ) -> io::Result<()> {
        self.hard_link_at(
            file,
            Path::new(""),
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            new_path,
            LinkatFlags::empty(),
        )
        .await
    }

    /// Standard library compatible method for creating a hard link using path syntax. This is the io_uring equivalent of `link(2)`.
    pub async fn hard_link(
        &self,
        old_path: impl AsRef<Path>,
        new_path: impl AsRef<Path>,
    ) -> io::Result<()> {
        self.hard_link_at(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            old_path,
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            new_path,
            LinkatFlags::empty(),
        )
        .await
    }

    /// Alias for [`Self::fchmod`].
    #[inline]
    pub async fn set_permissions(
        &self,
        fd: &(impl UringTarget + ?Sized),
        permissions: Permissions,
    ) -> io::Result<()> {
        self.fchmod(fd, permissions).await
    }

    /// Set the permissions of an open handle to a file or directory, which is equivalent to [`std::fs::File::set_permissions`] or `fchmod(2)`.
    pub async fn fchmod(
        &self,
        fd: &(impl UringTarget + ?Sized),
        permissions: Permissions,
    ) -> io::Result<()> {
        let raw_fd = fd.as_fd().as_raw_fd();
        tokio::task::spawn_blocking(move || {
            nix::sys::stat::fchmod(
                unsafe { BorrowedFd::borrow_raw(raw_fd) },
                Mode::from_bits_retain(permissions.mode()),
            )
        })
        .await??;
        Ok(())
    }

    /// Set the permissions of a file or directory relative to a directory fd, which is equivalent to `fchmodat(2)`.
    pub async fn fchmod_at(
        &self,
        fd: &(impl UringTarget + ?Sized),
        path: impl AsRef<Path>,
        permissions: Permissions,
        flags: FchmodatFlags,
    ) -> io::Result<()> {
        let path = path.as_ref().to_owned();
        let raw_fd = fd.as_fd().as_raw_fd();
        tokio::task::spawn_blocking(move || unsafe {
            nix::sys::stat::fchmodat(
                BorrowedFd::borrow_raw(raw_fd),
                &path,
                Mode::from_bits_retain(permissions.mode()),
                flags,
            )
        })
        .await??;
        Ok(())
    }

    /// Set the permissions of a file or directory using path syntax, equivalent to `chmod(2)`.
    pub async fn chmod(&self, path: impl AsRef<Path>, permissions: Permissions) -> io::Result<()> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || unsafe {
            nix::sys::stat::fchmodat(
                BorrowedFd::borrow_raw(libc::AT_FDCWD),
                &path,
                Mode::from_bits_retain(permissions.mode()),
                FchmodatFlags::FollowSymlink,
            )
        })
        .await??;
        Ok(())
    }

    /// Set the permissions of a file or directory using path syntax, but can target a symlink.
    pub async fn lchmod(&self, path: impl AsRef<Path>, permissions: Permissions) -> io::Result<()> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || unsafe {
            nix::sys::stat::fchmodat(
                BorrowedFd::borrow_raw(libc::AT_FDCWD),
                &path,
                Mode::from_bits_retain(permissions.mode()),
                FchmodatFlags::NoFollowSymlink,
            )
        })
        .await??;
        Ok(())
    }

    /// Set the user and/or group ownership of an open handle to a file or directory, which is equivalent to `fchown(2)`.
    pub async fn fchown(
        &self,
        fd: &(impl UringTarget + ?Sized),
        uid: Option<Uid>,
        gid: Option<Gid>,
    ) -> io::Result<()> {
        let raw_fd = fd.as_fd().as_raw_fd();
        tokio::task::spawn_blocking(move || unsafe {
            nix::unistd::fchown(BorrowedFd::borrow_raw(raw_fd), uid, gid)
        })
        .await??;
        Ok(())
    }

    /// Set the user and/or group ownership of a file or directory relative to a directory fd, which is equivalent to `fchownat(2)`.
    pub async fn fchown_at(
        &self,
        fd: &(impl UringTarget + ?Sized),
        path: impl AsRef<Path>,
        uid: Option<Uid>,
        gid: Option<Gid>,
        flags: FchownatFlags,
    ) -> io::Result<()> {
        let path = path.as_ref().to_owned();
        let raw_fd = fd.as_fd().as_raw_fd();
        tokio::task::spawn_blocking(move || unsafe {
            nix::unistd::fchownat(BorrowedFd::borrow_raw(raw_fd), &path, uid, gid, flags)
        })
        .await??;
        Ok(())
    }

    /// Set the user and/or group ownership of a file or directory using path syntax, which is equivalent to `chown(2)`.
    pub async fn chown(
        &self,
        path: impl AsRef<Path>,
        uid: Option<Uid>,
        gid: Option<Gid>,
    ) -> io::Result<()> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || unsafe {
            nix::unistd::fchownat(
                BorrowedFd::borrow_raw(libc::AT_FDCWD),
                &path,
                uid,
                gid,
                AtFlags::AT_SYMLINK_FOLLOW,
            )
        })
        .await??;
        Ok(())
    }

    /// Set the user and/ or group ownership of a file or directory using path syntax, but can target a symlink.
    pub async fn lchown(
        &self,
        path: impl AsRef<Path>,
        uid: Option<Uid>,
        gid: Option<Gid>,
    ) -> io::Result<()> {
        let path = path.as_ref().to_owned();
        tokio::task::spawn_blocking(move || unsafe {
            nix::unistd::fchownat(
                BorrowedFd::borrow_raw(libc::AT_FDCWD),
                &path,
                uid,
                gid,
                AtFlags::AT_SYMLINK_NOFOLLOW,
            )
        })
        .await??;
        Ok(())
    }

    /// Alias for [`Self::futimens`].
    #[inline]
    pub async fn set_times(
        &self,
        fd: &(impl UringTarget + ?Sized),
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
    ) -> io::Result<()> {
        self.futimens(fd, atime, mtime).await
    }

    /// Set file times of an open handle to a file or directory, which is equivalent to [`std::fs::File::set_times`] or `futimens(2)`.
    pub async fn futimens(
        &self,
        fd: &(impl UringTarget + ?Sized),
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
    ) -> io::Result<()> {
        let raw_fd = fd.as_fd().as_raw_fd();
        let _atime = atime.unwrap_or(TimeSpec::UTIME_OMIT);
        let _mtime = mtime.unwrap_or(TimeSpec::UTIME_OMIT);
        tokio::task::spawn_blocking(move || {
            nix::sys::stat::futimens(unsafe { BorrowedFd::borrow_raw(raw_fd) }, &_atime, &_mtime)
        })
        .await??;
        Ok(())
    }

    /// Set file times of a file or directory using a path relative to a directory fd, which is equivalent to `utimensat(2)`.
    pub async fn utimens_at(
        &self,
        fd: &(impl UringTarget + ?Sized),
        path: impl AsRef<Path>,
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
        flags: UtimensatFlags,
    ) -> io::Result<()> {
        let path = path.as_ref().to_owned();
        let _atime = atime.unwrap_or(TimeSpec::UTIME_OMIT);
        let _mtime = mtime.unwrap_or(TimeSpec::UTIME_OMIT);
        let raw_fd = fd.as_fd().as_raw_fd();
        tokio::task::spawn_blocking(move || unsafe {
            nix::sys::stat::utimensat(
                BorrowedFd::borrow_raw(raw_fd),
                &path,
                &_atime,
                &_mtime,
                flags,
            )
        })
        .await??;
        Ok(())
    }

    /// Canonicalize a path. This is equivalent to `realpath(3)`.
    pub async fn canonicalize(&self, path: impl AsRef<Path>) -> io::Result<PathBuf> {
        tokio::fs::canonicalize(path).await
    }

    /// Read a symbolic link. This is equivalent to `readlink(2)`.
    pub async fn read_link(&self, path: impl AsRef<Path>) -> io::Result<PathBuf> {
        let path = path.as_ref().to_owned();
        let output = tokio::task::spawn_blocking(move || unsafe {
            nix::fcntl::readlinkat(BorrowedFd::borrow_raw(libc::AT_FDCWD), &path)
        })
        .await??;
        Ok(PathBuf::from(output))
    }

    /// Read a symbolic link at a relative path to a directory fd, which is equivalent to `readlinkat(2)`.
    pub async fn read_link_at(
        &self,
        dir_fd: &(impl UringTarget + ?Sized),
        path: impl AsRef<Path>,
    ) -> io::Result<PathBuf> {
        let path = path.as_ref().to_owned();
        let raw_fd = dir_fd.as_fd().as_raw_fd();
        let output = tokio::task::spawn_blocking(move || unsafe {
            nix::fcntl::readlinkat(BorrowedFd::borrow_raw(raw_fd), &path)
        })
        .await??;
        Ok(PathBuf::from(output))
    }

    /// Read a symbolic link at a file descriptor opened with [`OFlag::O_PATH`] and [`OFlag::O_NOFOLLOW`].
    pub async fn read_link_file(&self, fd: &(impl UringTarget + ?Sized)) -> io::Result<PathBuf> {
        let raw_fd = fd.as_fd().as_raw_fd();
        let output = tokio::task::spawn_blocking(move || unsafe {
            nix::fcntl::readlinkat(BorrowedFd::borrow_raw(raw_fd), Path::new(""))
        })
        .await??;
        Ok(PathBuf::from(output))
    }

    /// Create a device node at a relative path to a directory fd, which is equivalent to `mknodat(2)`.
    pub async fn mknod_at(
        &self,
        dir_fd: &(impl UringTarget + ?Sized),
        path: impl AsRef<Path>,
        kind: MknodType,
        permissions: Permissions,
    ) -> io::Result<()> {
        let path = path.as_ref().to_owned();
        let raw_fd = dir_fd.as_fd().as_raw_fd();
        tokio::task::spawn_blocking(move || unsafe {
            let (kind, device) = kind.to_sflag_and_device();
            nix::sys::stat::mknodat(
                BorrowedFd::borrow_raw(raw_fd),
                &path,
                kind,
                Mode::from_bits_retain(permissions.mode()),
                device.into(),
            )
        })
        .await??;
        Ok(())
    }

    /// Create a device node using path syntax, which is equivalent to `mknod(2)`.
    pub async fn mknod(
        &self,
        path: impl AsRef<Path>,
        kind: MknodType,
        permissions: Permissions,
    ) -> io::Result<()> {
        self.mknod_at(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            kind,
            permissions,
        )
        .await
    }
}
