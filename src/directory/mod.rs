//! Asynchronous port of Rustix's directory structure for Linux
#![cfg(target_os = "linux")]
use std::{
    ffi::{CStr, OsStr, OsString},
    io,
    ops::{Deref, DerefMut},
    os::{
        fd::{AsFd, BorrowedFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    Client, FileType, IoBufMut, PendingIo, UringTarget,
    client::ReadResult,
    default_client,
    helpers::{self, as_ptr},
};
use futures::{
    Stream,
    stream::{BoxStream, StreamExt},
};
use linux_raw_sys::general::linux_dirent64;
use nix::unistd::Whence;

/// A directory stream.
pub struct Dir {
    /// The `OwnedFd` that we read directory entries from.
    fd: OwnedFd,
    /// Have we seen any errors in this iteration?
    any_errors: bool,
    /// Should we rewind the stream on the next iteration?
    rewind: bool,
    /// The buffer for `linux_dirent64` entries.
    buf: Vec<u8>,
    /// Where we are in the buffer.
    pos: usize,
}

impl Dir {
    /// Take ownership of `fd` and construct a `Dir` that reads entries from
    /// the given directory file descriptor.
    #[inline]
    pub fn new<Fd: Into<OwnedFd>>(fd: Fd) -> io::Result<Self> {
        Self::_new(fd.into())
    }

    #[inline]
    fn _new(fd: OwnedFd) -> io::Result<Self> {
        Ok(Self {
            fd,
            any_errors: false,
            rewind: false,
            buf: Vec::new(),
            pos: 0,
        })
    }

    /// Retrieve the current position on the directory
    pub async fn tell(&self) -> io::Result<u64> {
        default_client().tell_dir(self).await
    }

    /// Seek to a position on the directory, used for pagination
    #[cfg(target_pointer_width = "64")]
    #[cfg_attr(docsrs, doc(cfg(target_pointer_width = "64")))]
    pub async fn seek(&mut self, offset: i64) -> io::Result<()> {
        default_client().seek_dir(self, offset).await
    }

    /// Read the next entry from the directory (excluding any special entries)
    pub fn read(&mut self) -> impl Future<Output = Option<io::Result<DirEntry>>> + Send {
        default_client().read_dir(self)
    }

    /// Use the directory as an asynchronous stream
    pub fn stream<'a>(&'a mut self) -> ReadDir<'a> {
        ReadDir {
            // SAFETY: If the stream is being polled, the directory can't be accessed
            dir: unsafe { std::mem::transmute::<&mut Dir, &'a mut Dir>(self) },
            stream: async_stream::stream! {
                let client = default_client();
                while let Some(entry) = client.read_dir_libc(self).await {
                    yield entry;
                }
            }
            .boxed(),
            read_special_entries: false,
        }
    }

    /// Convert the directory into an owned asynchronous stream
    pub fn into_stream(self) -> ReadDirOwned {
        let mut pinned = Box::pin(self);
        // SAFETY: If the stream is being polled, the directory can't be accessed
        let escaped =
            unsafe { std::mem::transmute::<&mut Dir, &'static mut Dir>(pinned.as_mut().get_mut()) };
        ReadDirOwned {
            stream: async_stream::stream! {
                let client = default_client();
                while let Some(entry) = client.read_dir_libc(escaped).await {
                    yield entry;
                }
            }
            .boxed(),
            dir: pinned,
            read_special_entries: false,
        }
    }
}

impl AsFd for Dir {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

/// Structure for iterating over a directory
pub struct ReadDir<'a> {
    // Stream for the directory entries
    stream: BoxStream<'a, io::Result<DirEntry>>,
    // Read special entries ("." and "..")
    read_special_entries: bool,
    // Backing pointer for the stream
    dir: &'a mut Dir,
}

impl<'a> Stream for ReadDir<'a> {
    type Item = io::Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = self.get_mut();
        poll_dir_stream(&mut inner.stream, cx, &mut inner.read_special_entries)
    }
}

impl<'a> AsFd for ReadDir<'a> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.dir.as_fd()
    }
}

impl<'a> Deref for ReadDir<'a> {
    type Target = Dir;
    fn deref(&self) -> &Self::Target {
        self.dir
    }
}

impl<'a> DerefMut for ReadDir<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dir
    }
}

impl<'a> ReadDir<'a> {
    /// Convert the directory into an asynchronous stream with path of the parent directory
    pub fn with_parent(self, parent: impl AsRef<Path>) -> ReadDirWithParent<'a> {
        ReadDirWithParent {
            inner: self,
            parent: parent.as_ref().to_owned(),
        }
    }

    /// Read special entries ("." and "..")
    pub fn with_read_special_entries(self, read_special_entries: bool) -> ReadDir<'a> {
        ReadDir {
            stream: self.stream,
            dir: self.dir,
            read_special_entries,
        }
    }
}

/// Structure for iterating over a directory, with access to the parent dir path
pub struct ReadDirWithParent<'a> {
    // Path to the parent directory
    parent: PathBuf,
    // Inner object for the directory entries
    inner: ReadDir<'a>,
}

impl<'a> Stream for ReadDirWithParent<'a> {
    type Item = io::Result<DirEntryWithParent>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map_ok(|entry| DirEntryWithParent {
                entry,
                parent: self.parent.clone(),
            })
    }
}

impl<'a> AsFd for ReadDirWithParent<'a> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl<'a> Deref for ReadDirWithParent<'a> {
    type Target = Dir;
    fn deref(&self) -> &Self::Target {
        self.inner.dir
    }
}

impl<'a> DerefMut for ReadDirWithParent<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.dir
    }
}

impl<'a> ReadDirWithParent<'a> {
    /// Read special entries ("." and "..")
    pub fn with_read_special_entries(self, read_special_entries: bool) -> ReadDirWithParent<'a> {
        ReadDirWithParent {
            inner: self.inner.with_read_special_entries(read_special_entries),
            parent: self.parent,
        }
    }
}

/// Owned structure for iterating over a directory
pub struct ReadDirOwned {
    // Stream for the directory entries
    stream: BoxStream<'static, io::Result<DirEntry>>,
    // Read special entries ("." and "..")
    read_special_entries: bool,
    // Backing pointer for the stream
    dir: Pin<Box<Dir>>,
}

impl Stream for ReadDirOwned {
    type Item = io::Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = self.get_mut();
        poll_dir_stream(&mut inner.stream, cx, &mut inner.read_special_entries)
    }
}

impl AsFd for ReadDirOwned {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.dir.as_fd()
    }
}

impl Deref for ReadDirOwned {
    type Target = Dir;
    fn deref(&self) -> &Self::Target {
        &self.dir
    }
}

impl DerefMut for ReadDirOwned {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dir
    }
}

impl ReadDirOwned {
    /// Convert the directory into an owned asynchronous stream with path of the parent directory
    pub fn with_parent(self, parent: impl AsRef<Path>) -> ReadDirOwnedWithParent {
        ReadDirOwnedWithParent {
            inner: self,
            parent: parent.as_ref().to_owned(),
        }
    }

    /// Read special entries ("." and "..")
    pub fn with_read_special_entries(self, read_special_entries: bool) -> ReadDirOwned {
        ReadDirOwned {
            stream: self.stream,
            dir: self.dir,
            read_special_entries,
        }
    }
}

/// Owned structure for iterating over a directory, with access to the parent dir path
pub struct ReadDirOwnedWithParent {
    parent: PathBuf,
    // Inner object for the directory entries
    inner: ReadDirOwned,
}

impl Stream for ReadDirOwnedWithParent {
    type Item = io::Result<DirEntryWithParent>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map_ok(|entry| DirEntryWithParent {
                entry,
                parent: self.parent.clone(),
            })
    }
}

impl AsFd for ReadDirOwnedWithParent {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl Deref for ReadDirOwnedWithParent {
    type Target = ReadDirOwned;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ReadDirOwnedWithParent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl ReadDirOwnedWithParent {
    /// Read special entries ("." and "..")
    pub fn with_read_special_entries(self, read_special_entries: bool) -> ReadDirOwnedWithParent {
        ReadDirOwnedWithParent {
            inner: self.inner.with_read_special_entries(read_special_entries),
            parent: self.parent,
        }
    }
}

/// A directory entry.
#[derive(Debug, Clone)]
pub struct DirEntry {
    d_ino: u64,
    d_type: u8,
    d_off: i64,
    name: OsString,
}

impl DirEntry {
    /// Returns the file name of this directory entry.
    #[inline]
    pub fn file_name(&self) -> &OsStr {
        &self.name
    }

    /// Returns the “offset” of this directory entry. This is not a true
    /// numerical offset but an opaque cookie that identifies a position in the
    /// given stream.
    #[inline]
    pub fn offset(&self) -> i64 {
        self.d_off
    }

    /// Returns the type of this directory entry.
    #[inline]
    pub fn file_type(&self) -> FileType {
        FileType(self.d_type as u16)
    }

    /// Return the inode number of this directory entry.
    #[inline]
    pub fn ino(&self) -> u64 {
        self.d_ino
    }
}

/// A directory entry with path of the original parent directory
#[derive(Debug, Clone)]
pub struct DirEntryWithParent {
    entry: DirEntry,
    parent: PathBuf,
}

impl Deref for DirEntryWithParent {
    type Target = DirEntry;
    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

impl DirEntryWithParent {
    /// Returns the path of this directory entry.
    pub fn path(&self) -> PathBuf {
        self.parent.join(self.entry.file_name())
    }
}

/// Low level implementations of directories
impl Client {
    /// Retrieve the current position on the directory
    pub async fn tell_dir(&self, dir: &Dir) -> io::Result<u64> {
        let mut escape = dir.as_fd();
        helpers::m_retry_on_eintr! {
            self.seek_ll(&mut escape, Whence::SeekCur, 0)
                .completion()
                .expect("no completion future returned")
                .await
        }
    }

    /// Seek to a position on the directory, used for pagination
    #[cfg(target_pointer_width = "64")]
    #[cfg_attr(docsrs, doc(cfg(target_pointer_width = "64")))]
    pub async fn seek_dir(&self, dir: &mut Dir, offset: i64) -> io::Result<()> {
        dir.any_errors = false;
        dir.rewind = false;
        dir.pos = dir.buf.len();
        helpers::m_retry_on_eintr! {
            self.seek_ll(&mut dir.fd, Whence::SeekSet, offset)
                .map(|r| {
                    r.inspect_err(|e| {
                        // Should catch errors and mark directory as dirty even when the pending I/O object is dropped
                        // Do not terminate directory iteration if EINTR is encountered
                        if e.kind() == io::ErrorKind::Interrupted {
                            return;
                        }
                        dir.any_errors = true;
                    })
                    .map(|_| ())
                })
                .completion()
                .expect("no completion future returned")
                .await
        }
    }

    /// Read the next entry from the directory using the low-level API
    ///
    /// # Notes
    /// - Return entries contain special entries like "." and "..", as well as deleted entries (marked with ino == 0).
    pub async fn read_dir_ll(&self, dir: &mut Dir) -> Option<io::Result<DirEntry>> {
        if dir.any_errors {
            return None;
        }
        if dir.rewind {
            dir.rewind = false;
            let seek_res = helpers::m_retry_on_eintr! {
                self.seek_ll(&mut dir.fd, Whence::SeekSet, 0)
                    .map(|r| {
                        r.inspect_err(|e| {
                            if e.kind() == io::ErrorKind::Interrupted {
                                return;
                            }
                            dir.any_errors = true;
                        })
                        .map(|_| ())
                    })
                    .completion()
                    .expect("no completion future returned")
                    .await
            };
            if let Err(e) = seek_res {
                return Some(Err(e));
            }
        }
        let z = linux_dirent64 {
            d_ino: 0_u64,
            d_off: 0_i64,
            d_type: 0_u8,
            d_reclen: 0_u16,
            d_name: Default::default(),
        };
        let base = as_ptr(&z) as usize;
        let offsetof_d_reclen = (as_ptr(&z.d_reclen) as usize) - base;
        let offsetof_d_name = (as_ptr(&z.d_name) as usize) - base;
        let offsetof_d_ino = (as_ptr(&z.d_ino) as usize) - base;
        let offsetof_d_off = (as_ptr(&z.d_off) as usize) - base;
        let offsetof_d_type = (as_ptr(&z.d_type) as usize) - base;

        // Test if we need more entries, and if so, read more.
        if dir.buf.len() - dir.pos < size_of::<linux_dirent64>() {
            match self.read_dir_more(dir).await? {
                Ok(()) => (),
                Err(err) => return Some(Err(err)),
            };
        }

        // We successfully read an entry. Extract the fields.
        let pos = dir.pos;

        // Do an unaligned u16 load.
        let d_reclen = u16::from_ne_bytes([
            dir.buf[pos + offsetof_d_reclen],
            dir.buf[pos + offsetof_d_reclen + 1],
        ]);
        assert!(dir.buf.len() - pos >= d_reclen as usize);
        dir.pos += d_reclen as usize;

        // Read the NUL-terminated name from the `d_name` field. Without
        // `unsafe`, we need to scan for the NUL twice: once to obtain a size
        // for the slice, and then once within `CStr::from_bytes_with_nul`.
        let name_start = pos + offsetof_d_name;
        let name_len = dir.buf[name_start..]
            .iter()
            .position(|x| *x == b'\0')
            .unwrap();
        let name = CStr::from_bytes_with_nul(&dir.buf[name_start..][..=name_len]).unwrap();
        let name_bytes = name.to_bytes();
        assert!(name_bytes.len() <= dir.buf.len() - name_start);
        let name_owned = OsStr::from_bytes(name_bytes).to_owned();

        let d_ino = u64::from_ne_bytes(
            *dir.buf[pos + offsetof_d_ino..pos + offsetof_d_ino + 8]
                .as_array()
                .unwrap(),
        );
        let d_type = dir.buf[pos + offsetof_d_type];
        let d_off = i64::from_ne_bytes(
            *dir.buf[pos + offsetof_d_off..pos + offsetof_d_off + 8]
                .as_array()
                .unwrap(),
        );
        // Check that our types correspond to the `linux_dirent64` types.
        let _ = linux_dirent64 {
            d_ino,
            d_off,
            d_type,
            d_reclen,
            d_name: Default::default(),
        };

        Some(Ok(DirEntry {
            d_ino,
            d_type,
            d_off,
            name: name_owned,
        }))
    }

    /// Read the next entry from the directory (including special entries like "." and "..").
    ///
    /// # Notes
    /// - This API pairs with [`libc::readdir`].
    /// - Return entries contain special entries like "." and "..", excluding deleted entries.
    pub async fn read_dir_libc(&self, dir: &mut Dir) -> Option<io::Result<DirEntry>> {
        loop {
            let entry = match self.read_dir_ll(dir).await {
                Some(Ok(entry)) => entry,
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            };
            if entry.ino() == 0 {
                // println!("skipped deleted entry: {:?}", entry);
                continue;
            }
            break Some(Ok(entry));
        }
    }

    /// Read the next entry from the directory, excluding special entries
    pub async fn read_dir(&self, dir: &mut Dir) -> Option<io::Result<DirEntry>> {
        loop {
            let entry = match self.read_dir_libc(dir).await {
                Some(Ok(entry)) => entry,
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            };
            let filename = entry.file_name();
            if filename == OsStr::new(".") || filename == OsStr::new("..") {
                continue;
            }
            break Some(Ok(entry));
        }
    }

    async fn read_dir_more(&self, dir: &mut Dir) -> Option<io::Result<()>> {
        // The first few times we're called, we allocate a relatively small
        // buffer, because many directories are small. If we're called more,
        // use progressively larger allocations, up to a fixed maximum.
        //
        // The specific sizes and policy here have not been tuned in detail yet
        // and may need to be adjusted. In doing so, we should be careful to
        // avoid unbounded buffer growth. This buffer only exists to share the
        // cost of a `getdents` call over many entries, so if it gets too big,
        // cache and heap usage will outweigh the benefit. And ultimately,
        // directories can contain more entries than we can allocate contiguous
        // memory for, so we'll always need to cap the size at some point.
        if dir.buf.len() < 1024 * size_of::<linux_dirent64>() {
            dir.buf.reserve(32 * size_of::<linux_dirent64>());
        }
        dir.buf.resize(dir.buf.capacity(), 0);
        match helpers::m_retry_on_eintr! {
            {
                let borrowed = unsafe { std::mem::transmute::<&mut [u8], &mut [u8]>(dir.buf.as_mut_slice()) };
                self.getdents(&dir.fd, borrowed)
                    .map(|r| {
                        r.inspect_err(|e| {
                            if e.kind() == io::ErrorKind::Interrupted {
                                return;
                            }
                            dir.any_errors = true;
                        })
                        .map(|result| {
                            dir.pos = 0;
                            dir.buf.resize(result.bytes_read, 0);
                            result.bytes_read
                        })
                    })
                    .completion()
                    .expect("no completion future returned")
                    .await
            }
        } {
            Ok(0) => None,
            Ok(_nread) => Some(Ok(())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn getdents<'a, B>(
        &'a self,
        target: &'a (impl UringTarget + Sync + ?Sized),
        mut buffer: B,
    ) -> PendingIo<'a, io::Result<ReadResult<B>>>
    where
        B: IoBufMut + 'a,
    {
        // FIXME: getdents not yet supported in io_uring
        self.spawn_fallback(move || {
            let descriptor = target.as_file_descriptor();
            let result = helpers::syscall_cvt(unsafe {
                libc::syscall(
                    libc::SYS_getdents64,
                    descriptor,
                    buffer.as_mut_ptr(),
                    buffer.capacity().min(u32::MAX as usize) as u32,
                )
            })?;
            Ok(unsafe { ReadResult::new(buffer, result as usize) })
        })
    }
}

fn poll_dir_stream(
    mut stream: &mut BoxStream<'_, io::Result<DirEntry>>,
    cx: &mut Context<'_>,
    read_special_entries: &mut bool,
) -> Poll<Option<io::Result<DirEntry>>> {
    loop {
        match Pin::new(&mut stream).poll_next(cx) {
            Poll::Ready(Some(Ok(entry))) => {
                if !*read_special_entries
                    && (entry.file_name() == OsStr::new(".")
                        || entry.file_name() == OsStr::new(".."))
                {
                    continue;
                }
                return Poll::Ready(Some(Ok(entry)));
            }
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        }
    }
}
