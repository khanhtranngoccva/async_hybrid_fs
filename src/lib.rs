//! Linux-first asynchronous I/O library via optional use of `io_uring`.
//!
//! This crate aims to enhance performance for asynchronous I/O operations by leveraging true asynchronous APIs whenever possible, while allowing ease of use by working as a drop-in replacement for [`tokio::fs`] and [`std::fs`] and falling back to the async runtime when asynchronous APIs are not available or blocked.
//!
//! Unlike other `io_uring` crates which primarily target servers, this library is primarily targeted at applications that can benefit from fewer syscalls and run on end-user/consumer devices like a custom filesystem, on which support for `io_uring` is not guaranteed or `io_uring` is restricted by policy.
//!
//! # Comparison with original library
//! This crate is directly based on the `uring_file`(https://docs.rs/uring-file/latest/uring_file) crate by [wilsonzlin](https://github.com/wilsonzlin), but with some modifications:
//! - Features a more complete set of filesystem APIs, which is a superset of [`std::fs`]. This includes race-free APIs like the "*at" syscall family, as well as the vectored I/O APIs.
//! - Dynamically detects whether `io_uring` is available on the system supported for the operation, and falls back to the async runtime's methods when it is not.
//!     - The user may prevent unintentional use of fallback implementations in custom io_uring instances by setting [`UringCfg::allow_fallback`] to `false` if they are running in environments that should guarantee `io_uring`. Doing that will result in an error being returned when `io_uring` is not available, allowing for diagnosis.
//! - Registered files use safe implementations. They have two variants with different lifetime properties: [`RegisteredFile`] and [`OwnedRegisteredFile`]. Both file types are automatically unregistered when they are dropped, and a [`RegisteredFile`] can be upgraded to an [`OwnedRegisteredFile`] to allow the file to be stored independently.
//!
//! # Requirements
//! - Tokio runtime with at least the `rt` feature enabled. This is because this library uses Tokio's blocking executor for fallback implementations when `io_uring` is not available or supported for the operation.
//! ```toml
//! tokio = { version = "1", features = ["rt", "macros"] }
//! ```
#![cfg(unix)]
mod borrowed_buf;
pub mod client;
mod default;
pub mod flags;
pub mod fs;
mod helpers;
pub mod iobuf;
pub mod metadata;

use nix::fcntl::RenameFlags;
use nix::sys::time::TimeSpec;
use nix::unistd::LinkatFlags;
use nix::unistd::UnlinkatFlags;
use nix::unistd::{Gid, Uid};
use std::io::IoSlice;
use std::io::IoSliceMut;
use std::io::SeekFrom;
use std::io::{self};
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::os::fd::FromRawFd;
use std::os::fd::IntoRawFd;
use std::os::fd::OwnedFd;
use std::path::Path;

pub use crate::metadata::Metadata;
pub use crate::metadata::Permissions;
pub use client::Client;
pub use client::ClientBuildError;
pub use client::ClientUring;
pub use client::OwnedRegisteredFile;
pub use client::RegisterError;
pub use client::RegisteredFile;
pub use client::UringCfg;
pub use client::UringTarget;
pub use default::default_client;

#[async_trait::async_trait]
pub trait HybridRead: UringTarget {
    /// Asynchronous version of [`std::io::Read::read`].
    #[inline]
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.hybrid_read(buf).await
    }

    /// Alias for [`HybridRead::read`].
    async fn hybrid_read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        default_client().read(self, buf).await
    }

    /// Asynchronous version of [`std::io::Read::read_to_end`].
    #[inline]
    async fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.hybrid_read_to_end(buf).await
    }

    /// Alias for [`HybridRead::read_to_end`].
    async fn hybrid_read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        default_client().read_to_end(self, buf).await
    }

    /// Asynchronous version of [`std::io::Read::read_to_string`].
    #[inline]
    async fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        self.hybrid_read_to_string(buf).await
    }

    /// Alias for [`HybridRead::read_to_string`].
    async fn hybrid_read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        default_client().read_to_string(self, buf).await
    }

    /// Asynchronous version of [`std::io::Read::read_exact`].
    #[inline]
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.hybrid_read_exact(buf).await
    }

    /// Alias for [`HybridRead::read_exact`].
    async fn hybrid_read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        default_client().read_exact(self, buf).await
    }

    /// Asynchronous version of [`std::os::unix::fs::FileExt::read_at`].
    #[inline]
    async fn read_at(&self, buf: &mut [u8], offset: impl TryInto<u64> + Send) -> io::Result<usize> {
        self.hybrid_read_at(buf, offset).await
    }

    /// Alias for [`HybridRead::read_at`].
    async fn hybrid_read_at(
        &self,
        buf: &mut [u8],
        offset: impl TryInto<u64> + Send,
    ) -> io::Result<usize> {
        default_client().read_at(self, buf, offset).await
    }

    /// Asynchronous version of [`std::os::unix::fs::FileExt::read_exact_at`].
    #[inline]
    async fn read_exact_at(
        &self,
        buf: &mut [u8],
        offset: impl TryInto<u64> + Send,
    ) -> io::Result<()> {
        self.hybrid_read_exact_at(buf, offset).await
    }

    /// Alias for [`HybridRead::read_exact_at`].
    async fn hybrid_read_exact_at(
        &self,
        buf: &mut [u8],
        offset: impl TryInto<u64> + Send,
    ) -> io::Result<()> {
        default_client().read_exact_at(self, buf, offset).await
    }

    /// Asynchronous version of [`std::io::Read::read_vectored`].
    async fn read_vectored(&mut self, buf: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.hybrid_read_vectored(buf).await
    }

    /// Alias for [`HybridRead::read_vectored`].
    async fn hybrid_read_vectored(&mut self, buf: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        default_client().read_vectored(self, buf).await
    }

    /// Asynchronous version of [`std::os::unix::fs::FileExt::read_vectored_at`].
    async fn read_vectored_at(
        &self,
        buf: &mut [IoSliceMut<'_>],
        offset: impl TryInto<u64> + Send,
    ) -> io::Result<usize> {
        self.hybrid_read_vectored_at(buf, offset).await
    }

    /// Alias for [`HybridRead::read_vectored_at`].
    async fn hybrid_read_vectored_at(
        &self,
        buf: &mut [IoSliceMut<'_>],
        offset: impl TryInto<u64> + Send,
    ) -> io::Result<usize> {
        default_client().read_vectored_at(self, buf, offset).await
    }

    /// Standard library compatible method for creating a reference to the reader.
    #[inline]
    fn by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self.hybrid_by_ref()
    }

    /// Alias for [`HybridRead::by_ref`].
    #[inline]
    fn hybrid_by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self
    }

    // TODO: Implement bytes(), chain() and take()
}

#[async_trait::async_trait]
pub trait HybridWrite: UringTarget {
    /// Asynchronous version of [`std::io::Write::write`].
    #[inline]
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hybrid_write(buf).await
    }

    /// Alias for [`HybridWrite::write`].
    async fn hybrid_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        default_client().write(self, buf).await
    }

    /// Asynchronous version of [`std::io::Write::write_all`].
    #[inline]
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.hybrid_write_all(buf).await
    }

    /// Alias for [`HybridWrite::write_all`].
    async fn hybrid_write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        default_client().write_all(self, buf).await
    }

    /// Asynchronous version of [`std::io::Write::write_all_at`].
    #[inline]
    async fn write_all_at(&self, buf: &[u8], offset: impl TryInto<u64> + Send) -> io::Result<()> {
        self.hybrid_write_all_at(buf, offset).await
    }

    /// Alias for [`HybridWrite::write_all_at`].
    async fn hybrid_write_all_at(
        &self,
        buf: &[u8],
        offset: impl TryInto<u64> + Send,
    ) -> io::Result<()> {
        default_client().write_all_at(self, buf, offset).await
    }

    /// Asynchronous version of [`std::io::Write::write_at`].
    #[inline]
    async fn write_at(&self, buf: &[u8], offset: impl TryInto<u64> + Send) -> io::Result<usize> {
        self.hybrid_write_at(buf, offset).await
    }

    /// Alias for [`HybridWrite::write_at`].
    async fn hybrid_write_at(
        &self,
        buf: &[u8],
        offset: impl TryInto<u64> + Send,
    ) -> io::Result<usize> {
        default_client().write_at(self, buf, offset).await
    }

    /// Asynchronous version of [`std::io::Write::write_vectored`].
    async fn write_vectored(&mut self, buf: &[IoSlice<'_>]) -> io::Result<usize> {
        self.hybrid_write_vectored(buf).await
    }

    /// Alias for [`HybridWrite::write_vectored`].
    async fn hybrid_write_vectored(&mut self, buf: &[IoSlice<'_>]) -> io::Result<usize> {
        default_client().write_vectored(self, buf).await
    }

    /// Asynchronous version of [`std::os::unix::fs::FileExt::write_vectored_at`].
    async fn write_vectored_at(
        &self,
        buf: &[IoSlice<'_>],
        offset: impl TryInto<u64> + Send,
    ) -> io::Result<usize> {
        self.hybrid_write_vectored_at(buf, offset).await
    }

    /// Alias for [`HybridWrite::write_vectored_at`].
    async fn hybrid_write_vectored_at(
        &self,
        buf: &[IoSlice<'_>],
        offset: impl TryInto<u64> + Send,
    ) -> io::Result<usize> {
        default_client().write_vectored_at(self, buf, offset).await
    }

    /// Asynchronous version of [`std::io::Write::flush`].
    #[inline]
    async fn flush(&mut self) -> io::Result<()> {
        self.hybrid_flush().await
    }

    /// Alias for [`HybridWrite::flush`].
    async fn hybrid_flush(&mut self) -> io::Result<()> {
        default_client().flush(self).await
    }

    /// Standard library compatible method for creating a reference to the writer.
    #[inline]
    fn by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self.hybrid_by_ref()
    }

    /// Alias for [`HybridWrite::by_ref`].
    #[inline]
    fn hybrid_by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self
    }
}

#[async_trait::async_trait]
pub trait HybridSeek: UringTarget {
    /// Asynchronous version of [`std::io::Seek::seek`].
    #[inline]
    async fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> {
        self.hybrid_seek(offset).await
    }

    /// Alias for [`HybridSeek::seek`].
    async fn hybrid_seek(&mut self, offset: SeekFrom) -> io::Result<u64> {
        default_client().seek(self, offset).await
    }

    /// Asynchronous version of [`std::io::Seek::seek_ll`].
    #[inline]
    async fn seek_ll(
        &mut self,
        whence: nix::unistd::Whence,
        offset: impl TryInto<i64> + Send,
    ) -> io::Result<u64> {
        self.hybrid_seek_ll(whence, offset).await
    }

    /// Alias for [`HybridSeek::seek_ll`].
    async fn hybrid_seek_ll(
        &mut self,
        whence: nix::unistd::Whence,
        offset: impl TryInto<i64> + Send,
    ) -> io::Result<u64> {
        default_client().seek_ll(self, whence, offset).await
    }
}

#[async_trait::async_trait]
pub trait HybridFile: UringTarget {
    async fn close(self) -> io::Result<()>
    where
        Self: IntoRawFd + Sized,
    {
        default_client().close(self).await
    }

    /// Asynchronous version of [`std::io::File::metadata`].
    #[inline]
    async fn metadata(&self) -> io::Result<Metadata> {
        self.hybrid_metadata().await
    }

    /// Alias for [`HybridFile::metadata`].
    async fn hybrid_metadata(&self) -> io::Result<Metadata> {
        default_client().metadata(self).await
    }

    /// Asynchronous version of [`std::io::File::set_len`].
    #[inline]
    async fn set_len(&self, size: u64) -> io::Result<()> {
        self.hybrid_set_len(size).await
    }

    /// Alias for [`HybridFile::set_len`].
    async fn hybrid_set_len(&self, size: u64) -> io::Result<()> {
        default_client().ftruncate(self, size).await
    }

    /// Asynchronous version of [`std::io::File::set_times`].
    #[inline]
    async fn set_times(&self, atime: Option<TimeSpec>, mtime: Option<TimeSpec>) -> io::Result<()> {
        self.hybrid_set_times(atime, mtime).await
    }

    /// Alias for [`HybridFile::set_times`].
    async fn hybrid_set_times(
        &self,
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
    ) -> io::Result<()> {
        default_client().futimens(self, atime, mtime).await
    }

    /// Asynchronous version of [`std::io::File::set_permissions`].
    #[inline]
    async fn set_permissions(&self, permissions: Permissions) -> io::Result<()> {
        self.hybrid_set_permissions(permissions).await
    }

    /// Alias for [`HybridFile::set_permissions`].
    async fn hybrid_set_permissions(&self, permissions: Permissions) -> io::Result<()> {
        default_client().fchmod(self, permissions).await
    }

    /// Asynchronous version of [`std::os::unix::fs::fchown`].
    #[inline]
    async fn set_owner(&self, uid: Option<Uid>, gid: Option<Gid>) -> io::Result<()> {
        self.hybrid_set_owner(uid, gid).await
    }

    /// Alias for [`HybridFile::set_owner`].
    async fn hybrid_set_owner(&self, uid: Option<Uid>, gid: Option<Gid>) -> io::Result<()> {
        default_client().fchown(self, uid, gid).await
    }

    /// Method for creating a hard link to the file at the specified path relative to the specified directory fd.
    #[inline]
    async fn hard_link_at(
        &self,
        old_path: impl AsRef<Path> + Send,
        new_dir_fd: &(impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path> + Send,
        flags: LinkatFlags,
    ) -> io::Result<()> {
        self.hybrid_hard_link_at(old_path, new_dir_fd, new_path, flags)
            .await
    }

    /// Alias for [`HybridFile::hard_link_at`].
    async fn hybrid_hard_link_at(
        &self,
        old_path: impl AsRef<Path> + Send,
        new_dir_fd: &(impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path> + Send,
        flags: LinkatFlags,
    ) -> io::Result<()> {
        default_client()
            .hard_link_at(self, old_path, new_dir_fd, new_path, flags)
            .await
    }

    /// Method for creating a hard link to the file at the specified path.
    #[inline]
    async fn hard_link(&self, new_path: impl AsRef<Path> + Send) -> io::Result<()> {
        self.hybrid_hard_link(new_path).await
    }

    /// Alias for [`HybridFile::hard_link`].
    async fn hybrid_hard_link(&self, new_path: impl AsRef<Path> + Send) -> io::Result<()> {
        default_client().hard_link_file(self, new_path).await
    }

    /// Register a file for use with the global io_uring instance.
    fn register<'a>(&'a self) -> Result<RegisteredFile<'a>, RegisterError>
    where
        Self: AsFd,
    {
        default_client().register(self)
    }

    /// Register a file for use with the global io_uring instance.
    fn register_owned(self) -> Result<OwnedRegisteredFile, (RegisterError, OwnedFd)>
    where
        Self: IntoRawFd + Sized,
    {
        default_client().register_owned(unsafe { OwnedFd::from_raw_fd(self.into_raw_fd()) })
    }
}

#[async_trait::async_trait]
pub trait HybridDir: UringTarget {
    /// Method for unlinking a file at the specified path relative to the specified directory fd.
    #[inline]
    async fn unlink_at(&self, path: impl AsRef<Path> + Send) -> io::Result<()> {
        default_client()
            .unlink_at(self, path, UnlinkatFlags::NoRemoveDir)
            .await
    }

    /// Alias for [`HybridDir::unlink_at`].
    async fn hybrid_unlink_at(&self, path: impl AsRef<Path> + Send) -> io::Result<()> {
        default_client()
            .unlink_at(self, path, UnlinkatFlags::NoRemoveDir)
            .await
    }

    /// Method for unlinking a directory at the specified path relative to the specified directory fd.
    #[inline]
    async fn remove_dir_at(&self, path: impl AsRef<Path> + Send) -> io::Result<()> {
        default_client()
            .unlink_at(self, path, UnlinkatFlags::RemoveDir)
            .await
    }

    /// Alias for [`HybridDir::remove_dir_at`].
    async fn hybrid_remove_dir_at(&self, path: impl AsRef<Path> + Send) -> io::Result<()> {
        default_client()
            .unlink_at(self, path, UnlinkatFlags::RemoveDir)
            .await
    }

    /// Method for creating a symbolic link at the specified path relative to the specified directory fd.
    #[inline]
    async fn symlink_at(
        &self,
        target: impl AsRef<Path> + Send,
        link_path: impl AsRef<Path> + Send,
    ) -> io::Result<()> {
        default_client().symlink_at(target, self, link_path).await
    }

    /// Alias for [`HybridDir::symlink_at`].
    async fn hybrid_symlink_at(
        &self,
        target: impl AsRef<Path> + Send,
        link_path: impl AsRef<Path> + Send,
    ) -> io::Result<()> {
        default_client().symlink_at(target, self, link_path).await
    }

    /// Method for renaming a file at the specified path relative to the specified directory fd.
    #[inline]
    async fn rename_at(
        &self,
        old_path: impl AsRef<Path> + Send,
        new_dir_fd: &(impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path> + Send,
        flags: RenameFlags,
    ) -> io::Result<()> {
        self.hybrid_rename_at(old_path, new_dir_fd, new_path, flags)
            .await
    }

    /// Alias for [`HybridDir::rename_at`].
    async fn hybrid_rename_at(
        &self,
        old_path: impl AsRef<Path> + Send,
        new_dir_fd: &(impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path> + Send,
        flags: RenameFlags,
    ) -> io::Result<()> {
        default_client()
            .rename_at(self, old_path, new_dir_fd, new_path, flags)
            .await
    }

    /// Method for renaming a file at the specified path.
    #[inline]
    async fn rename(
        &self,
        old_path: impl AsRef<Path> + Send,
        new_path: impl AsRef<Path> + Send,
        flags: RenameFlags,
    ) -> io::Result<()> {
        self.hybrid_rename(old_path, new_path, flags).await
    }

    /// Alias for [`HybridDir::rename`].
    async fn hybrid_rename(
        &self,
        old_path: impl AsRef<Path> + Send,
        new_path: impl AsRef<Path> + Send,
        flags: RenameFlags,
    ) -> io::Result<()> {
        default_client()
            .rename_at(
                self,
                old_path,
                &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
                new_path,
                flags,
            )
            .await
    }
}

#[async_trait::async_trait]
impl<T> HybridRead for T where T: UringTarget + ?Sized {}
#[async_trait::async_trait]
impl<T> HybridWrite for T where T: UringTarget + ?Sized {}
#[async_trait::async_trait]
impl<T> HybridSeek for T where T: UringTarget + ?Sized {}
#[async_trait::async_trait]
impl<T> HybridFile for T where T: UringTarget + ?Sized {}
#[async_trait::async_trait]
impl<T> HybridDir for T where T: UringTarget + ?Sized {}

// Dynamic trait object compiler checks.
type _DynCompatibleHybridRead = Box<dyn HybridRead>;
type _DynCompatibleHybridWrite = Box<dyn HybridWrite>;
type _DynCompatibleHybridSeek = Box<dyn HybridSeek>;
type _DynCompatibleHybridFile = Box<dyn HybridFile>;
type _DynCompatibleHybridDir = Box<dyn HybridDir>;

#[cfg(test)]
mod tests {
    use crate::{HybridFile, HybridRead, HybridSeek, HybridWrite, fs::OpenOptions};
    use nix::sys::time::TimeSpec;
    use std::{
        io::{IoSlice, IoSliceMut, SeekFrom},
        time::{SystemTime, UNIX_EPOCH},
    };
    use tokio::fs::File;

    #[tokio::test]
    async fn is_uring_available() {
        println!(
            "uring available: {}",
            crate::default_client().is_uring_available_and_active()
        );
    }

    #[tokio::test]
    async fn test_hybrid_create_and_read() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(temp_dir.path().join("test.txt"))
            .await
            .unwrap();
        file.hybrid_write_all(b"Hello, world!").await.unwrap();
        file.hybrid_flush().await.unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .open(temp_dir.path().join("test.txt"))
            .await
            .unwrap();
        let mut buffer = Vec::new();
        file.hybrid_read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"Hello, world!");
    }

    #[tokio::test]
    async fn test_hybrid_seek() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(temp_dir.path().join("test.txt"))
            .await
            .unwrap();
        file.hybrid_write_all(b"Hello, world!").await.unwrap();
        file.hybrid_flush().await.unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .open(temp_dir.path().join("test.txt"))
            .await
            .unwrap();
        file.hybrid_seek(SeekFrom::Start(7)).await.unwrap();
        let mut buffer = Vec::new();
        file.hybrid_read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, b"world!");
    }

    #[tokio::test]
    async fn test_hybrid_set_len() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(temp_dir.path().join("test.txt"))
            .await
            .unwrap();
        file.hybrid_set_len(10).await.unwrap();
        assert_eq!(file.hybrid_metadata().await.unwrap().len(), 10);
    }

    #[tokio::test]
    async fn test_hybrid_set_times() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(temp_dir.path().join("test.txt"))
            .await
            .unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let tnow = TimeSpec::from_duration(now);
        file.set_times(Some(tnow), Some(tnow)).await.unwrap();

        let file = File::open(temp_dir.path().join("test.txt")).await.unwrap();
        let metadata = <File as HybridFile>::metadata(&file).await.unwrap();
        let accessed = metadata.accessed().unwrap();
        assert!(accessed.duration_since(UNIX_EPOCH).unwrap() == now);
        let modified = metadata.modified().unwrap();
        assert!(modified.duration_since(UNIX_EPOCH).unwrap() == now);
    }

    #[tokio::test]
    async fn test_hybrid_read_write_vectored() {
        // TODO: test may be flaky if reads are short (which is very rare)
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(temp_dir.path().join("test.txt"))
            .await
            .unwrap();
        file.hybrid_write_vectored(&[IoSlice::new(b"Hello, world!")])
            .await
            .unwrap();
        file.hybrid_flush().await.unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .open(temp_dir.path().join("test.txt"))
            .await
            .unwrap();
        let mut buffer = vec![0u8; 128];
        let slice = IoSliceMut::new(&mut buffer);
        let bytes_read = file.hybrid_read_vectored(&mut [slice]).await.unwrap();
        assert_eq!(&buffer[..bytes_read], b"Hello, world!");
    }
}
