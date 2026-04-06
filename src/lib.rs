//! Linux-first asynchronous I/O library via optional use of `io_uring`.
//!
//! This crate aims to enhance performance for asynchronous I/O operations by leveraging true asynchronous APIs whenever possible, while allowing ease of use by working as a drop-in replacement for [`tokio::fs`] and [`std::fs`] and falling back to the async runtime when asynchronous APIs are not available or not permitted.
//!
//! Unlike other `io_uring` crates which primarily target servers, this library is primarily targeted at applications that can benefit from fewer syscalls and run on end-user/consumer devices like a custom filesystem, on which support for `io_uring` is not guaranteed or `io_uring` is restricted by policy.
//!
//! # Feature comparison
//! This crate is directly based on the `uring_file`(https://docs.rs/uring-file/latest/uring_file) crate by [wilsonzlin](https://github.com/wilsonzlin), but with some modifications:
//! - Features a more complete set of filesystem APIs, which is a superset of [`std::fs`]. This includes race-free APIs like the "*at" syscall family, as well as the vectored I/O APIs.
//! - Dynamically detects whether `io_uring` is available on the system supported for the operation, and falls back to the async runtime's methods when it is not.
//!     - The user may prevent unintentional use of fallback implementations in custom io_uring instances by setting [`UringCfg::allow_fallback`] to `false` if they are running in environments that should guarantee `io_uring`. Doing that will result in an error being returned when `io_uring` is not available, allowing for diagnosis.
//! - Registered files use safe implementations.
//!     - They have two variants with different lifetime properties: [`RegisteredFile`] and [`OwnedRegisteredFile`].
//!     - Both file types are automatically unregistered when they are dropped, and a [`RegisteredFile`] can be upgraded to an [`OwnedRegisteredFile`] to allow the file to be stored independently.
//! - Pending I/O objects returned from primitive operations ([`PendingIoObject`], e.g. [`Client::write_from`]) is designed to be memory-safe, cancellation-safe and atomic:
//!     - The APIs takes full advantage of Rust's borrow checker.
//!     - The io_uring system offers a cancellation mechanism for operations that are in progress, but cancellation might fail because an operation has progressed too far, and waiting for completion is required. Calling the [`PendingIo::cancel`] method handles pitfalls by invoking the cancellation mechanism and waiting for completion and returning the result if necessary.
//!     - While [`uring_file`] used await points when receiving from completion channels, which may result in use-after-free errors if its pending futures are dropped mid-operation, this library implements low-level futures which invoke [`PendingIo::cancel`] on drop or wait for completion, depending on low-level API capabilities.
//!     - When multiplexing using macros like [`tokio::select`] with a cancellation token or a timeout timer, futures may be implicitly dropped. If pending I/O object could be polled directly, it may lead to missed completions, which may lead to subtle bugs where states are out of sync with the underlying file descriptor. To counteract that, the [`PendingIo::completion`] method returns a trivially droppable future that can be multiplexed with these futures without triggering the cancellation mechanism.
//!     - The [`PendingIo::map`] method allows running `FnOnce` code that has to run after the operation is completed, regardless of whether the pending I/O operation is dropped early or not.
//!         - This is useful for structures where there is a user-mode offset field that requires atomic synchronization with the kernel-mode seek position.
//!     - [`PendingIo`] objects do not implement [`Future`] by default.
//!         - If you wish to await the I/O operation directly, you can use the crate feature `pending-io-futures`.
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
pub mod fs;
mod helpers;
pub mod iobuf;
pub mod metadata;
mod runtime;

use crate::client::pending_io::fixed_value::FixedValuePendingIo;
use nix::fcntl::AT_FDCWD;
use nix::fcntl::RenameFlags;
use nix::sys::time::TimeSpec;
use nix::unistd::LinkatFlags;
use nix::unistd::UnlinkatFlags;
use nix::unistd::{Gid, Uid};
use std::io::IoSlice;
use std::io::IoSliceMut;
use std::io::PipeReader;
use std::io::PipeWriter;
use std::io::SeekFrom;
use std::io::{self};
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::os::fd::FromRawFd;
use std::os::fd::IntoRawFd;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::path::PathBuf;
use tokio::fs::File;

pub use crate::metadata::DeviceNumber;
pub use crate::metadata::FileType;
pub use crate::metadata::Metadata;
pub use crate::metadata::MknodType;
pub use crate::metadata::Permissions;
pub use client::Client;
pub use client::ClientBuildError;
pub use client::ClientUring;
pub use client::OwnedRegisteredFile;
pub use client::RegisterError;
pub use client::RegisteredFile;
pub use client::Target;
pub use client::UringCfg;
pub use client::UringTarget;
pub use client::pending_io::PendingIo;
pub use default::default_client;
pub use fs::OpenOptions;
pub use iobuf::IoBuf;
pub use iobuf::IoBufMut;

#[async_trait::async_trait]
pub trait HybridRead: UringTarget + Sync + Send {
    /// Asynchronous version of [`std::io::Read::read`].
    #[inline]
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> PendingIo<'a, io::Result<usize>> {
        self.hybrid_read(buf)
    }

    /// Alias for [`HybridRead::read`].
    fn hybrid_read<'a>(&'a mut self, buf: &'a mut [u8]) -> PendingIo<'a, io::Result<usize>> {
        default_client().read(self, buf)
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
    fn read_at<'a>(
        &'a self,
        buf: &'a mut [u8],
        offset: impl TryInto<u64> + Send,
    ) -> PendingIo<'a, io::Result<usize>> {
        self.hybrid_read_at(buf, offset)
    }

    /// Alias for [`HybridRead::read_at`].
    fn hybrid_read_at<'a>(
        &'a self,
        buf: &'a mut [u8],
        offset: impl TryInto<u64> + Send,
    ) -> PendingIo<'a, io::Result<usize>> {
        default_client().read_at(self, buf, offset)
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
    fn read_vectored<'a>(
        &'a mut self,
        buf: &'a mut [IoSliceMut<'a>],
    ) -> PendingIo<'a, io::Result<usize>> {
        self.hybrid_read_vectored(buf)
    }

    /// Alias for [`HybridRead::read_vectored`].
    fn hybrid_read_vectored<'a>(
        &'a mut self,
        buf: &'a mut [IoSliceMut<'a>],
    ) -> PendingIo<'a, io::Result<usize>> {
        default_client().read_vectored(self, buf)
    }

    /// Asynchronous version of [`std::os::unix::fs::FileExt::read_vectored_at`].
    fn read_vectored_at<'a>(
        &'a self,
        buf: &'a mut [IoSliceMut<'a>],
        offset: impl TryInto<u64> + Send,
    ) -> PendingIo<'a, io::Result<usize>> {
        self.hybrid_read_vectored_at(buf, offset)
    }

    /// Alias for [`HybridRead::read_vectored_at`].
    fn hybrid_read_vectored_at<'a>(
        &'a self,
        buf: &'a mut [IoSliceMut<'a>],
        offset: impl TryInto<u64> + Send,
    ) -> PendingIo<'a, io::Result<usize>> {
        default_client().read_vectored_at(self, buf, offset)
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
pub trait HybridWrite: UringTarget + Sync + Send {
    /// Asynchronous version of [`std::io::Write::write`].
    #[inline]
    fn write<'a>(&'a mut self, buf: &'a [u8]) -> PendingIo<'a, io::Result<usize>> {
        self.hybrid_write(buf)
    }

    /// Alias for [`HybridWrite::write`].
    fn hybrid_write<'a>(&'a mut self, buf: &'a [u8]) -> PendingIo<'a, io::Result<usize>> {
        default_client().write(self, buf)
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
    fn write_at<'a>(
        &'a self,
        buf: &'a [u8],
        offset: impl TryInto<u64> + Send,
    ) -> PendingIo<'a, io::Result<usize>> {
        self.hybrid_write_at(buf, offset)
    }

    /// Alias for [`HybridWrite::write_at`].
    fn hybrid_write_at<'a>(
        &'a self,
        buf: &'a [u8],
        offset: impl TryInto<u64> + Send,
    ) -> PendingIo<'a, io::Result<usize>> {
        default_client().write_at(self, buf, offset)
    }

    /// Asynchronous version of [`std::io::Write::write_vectored`].
    fn write_vectored<'a>(
        &'a mut self,
        buf: &'a [IoSlice<'a>],
    ) -> PendingIo<'a, io::Result<usize>> {
        self.hybrid_write_vectored(buf)
    }

    /// Alias for [`HybridWrite::write_vectored`].
    fn hybrid_write_vectored<'a>(
        &'a mut self,
        buf: &'a [IoSlice<'a>],
    ) -> PendingIo<'a, io::Result<usize>> {
        default_client().write_vectored(self, buf)
    }

    /// Asynchronous version of [`std::os::unix::fs::FileExt::write_vectored_at`].
    fn write_vectored_at<'a>(
        &'a self,
        buf: &'a [IoSlice<'a>],
        offset: impl TryInto<u64> + Send,
    ) -> PendingIo<'a, io::Result<usize>> {
        self.hybrid_write_vectored_at(buf, offset)
    }

    /// Alias for [`HybridWrite::write_vectored_at`].
    fn hybrid_write_vectored_at<'a>(
        &'a self,
        buf: &'a [IoSlice<'a>],
        offset: impl TryInto<u64> + Send,
    ) -> PendingIo<'a, io::Result<usize>> {
        default_client().write_vectored_at(self, buf, offset)
    }

    /// Asynchronous version of [`std::io::Write::flush`].
    #[inline]
    fn flush<'a>(&'a mut self) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_flush()
    }

    /// Alias for [`HybridWrite::flush`].
    fn hybrid_flush<'a>(&'a mut self) -> PendingIo<'a, io::Result<()>> {
        default_client().flush(self)
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
pub trait HybridSeek: UringTarget + Sync + Send {
    /// Asynchronous version of [`std::io::Seek::seek`].
    #[inline]
    fn seek<'a>(&'a mut self, offset: SeekFrom) -> PendingIo<'a, io::Result<u64>> {
        self.hybrid_seek(offset)
    }

    /// Alias for [`HybridSeek::seek`].
    fn hybrid_seek<'a>(&'a mut self, offset: SeekFrom) -> PendingIo<'a, io::Result<u64>> {
        default_client().seek(self, offset)
    }

    /// Low-level method for seeking to a specific offset in the file.
    #[inline]
    fn seek_ll<'a>(
        &'a mut self,
        whence: nix::unistd::Whence,
        offset: impl TryInto<i64> + Send,
    ) -> PendingIo<'a, io::Result<u64>> {
        self.hybrid_seek_ll(whence, offset)
    }

    /// Alias for [`HybridSeek::seek_ll`].
    fn hybrid_seek_ll<'a>(
        &'a mut self,
        whence: nix::unistd::Whence,
        offset: impl TryInto<i64> + Send,
    ) -> PendingIo<'a, io::Result<u64>> {
        default_client().seek_ll(self, whence, offset)
    }
}

#[async_trait::async_trait]
pub trait HybridFile: UringTarget + Sync {
    fn close<'a>(self) -> PendingIo<'a, io::Result<()>>
    where
        Self: IntoRawFd + Sized + Send + 'a,
    {
        default_client().close(self)
    }

    /// Asynchronous version of [`std::io::File::metadata`].
    #[inline]
    fn metadata<'a>(&'a self) -> PendingIo<'a, io::Result<Metadata>> {
        self.hybrid_metadata()
    }

    /// Alias for [`HybridFile::metadata`].
    fn hybrid_metadata<'a>(&'a self) -> PendingIo<'a, io::Result<Metadata>> {
        default_client().metadata(self)
    }

    /// Asynchronous version of [`std::io::File::set_len`].
    #[inline]
    fn set_len<'a>(&'a self, size: u64) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_set_len(size)
    }

    /// Alias for [`HybridFile::set_len`].
    fn hybrid_set_len<'a>(&'a self, size: u64) -> PendingIo<'a, io::Result<()>> {
        default_client().ftruncate(self, size)
    }

    /// Asynchronous version of [`std::io::File::set_times`].
    #[inline]
    fn set_times<'a>(
        &'a self,
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
    ) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_set_times(atime, mtime)
    }

    /// Alias for [`HybridFile::set_times`].
    fn hybrid_set_times<'a>(
        &'a self,
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
    ) -> PendingIo<'a, io::Result<()>> {
        default_client().futimens(self, atime, mtime)
    }

    /// Asynchronous version of [`std::io::File::set_permissions`].
    #[inline]
    fn set_permissions<'a>(&'a self, permissions: Permissions) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_set_permissions(permissions)
    }

    /// Alias for [`HybridFile::set_permissions`].
    fn hybrid_set_permissions<'a>(
        &'a self,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        default_client().fchmod(self, permissions)
    }

    /// Asynchronous version of [`std::os::unix::fs::fchown`].
    #[inline]
    fn set_owner<'a>(
        &'a self,
        uid: Option<Uid>,
        gid: Option<Gid>,
    ) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_set_owner(uid, gid)
    }

    /// Alias for [`HybridFile::set_owner`].    
    fn hybrid_set_owner<'a>(
        &'a self,
        uid: Option<Uid>,
        gid: Option<Gid>,
    ) -> PendingIo<'a, io::Result<()>> {
        default_client().fchown(self, uid, gid)
    }

    /// Method for creating a hard link to the file at the specified path.
    #[inline]
    fn hard_link<'a>(&'a self, new_path: impl AsRef<Path>) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_hard_link(new_path)
    }

    /// Alias for [`HybridFile::hard_link`].
    fn hybrid_hard_link<'a>(&'a self, new_path: impl AsRef<Path>) -> PendingIo<'a, io::Result<()>> {
        default_client().hard_link_file(self, new_path)
    }

    /// Method for reading the target of the symlink file descriptor (requires opening via [`OFlag::O_PATH`] and [`OFlag::O_NOFOLLOW`]).
    #[inline]
    fn read_link<'a>(&'a self) -> PendingIo<'a, io::Result<PathBuf>> {
        self.hybrid_read_link()
    }

    /// Alias for [`HybridFile::read_link`].
    fn hybrid_read_link<'a>(&'a self) -> PendingIo<'a, io::Result<PathBuf>> {
        default_client().read_link_file(self)
    }

    /// Method for syncing the metadata and data of a file.
    #[inline]
    fn sync_all<'a>(&'a self) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_sync_all()
    }

    /// Alias for [`HybridFile::sync_all`].
    fn hybrid_sync_all<'a>(&'a self) -> PendingIo<'a, io::Result<()>> {
        default_client().sync_all(self)
    }

    /// Method for syncing the data of a file.
    #[inline]
    fn sync_data<'a>(&'a self) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_sync_data()
    }

    /// Alias for [`HybridFile::sync_data`].
    fn hybrid_sync_data<'a>(&'a self) -> PendingIo<'a, io::Result<()>> {
        default_client().sync_data(self)
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
pub trait HybridDir: UringTarget + Sync {
    /// Method for creating a hard link to the file at the specified path relative to the specified directory fd.
    #[inline]
    fn hard_link_at<'a>(
        &'a self,
        old_path: impl AsRef<Path>,
        new_dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path>,
        flags: LinkatFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_hard_link_at(old_path, new_dir_fd, new_path, flags)
    }

    /// Alias for [`HybridFile::hard_link_at`].
    fn hybrid_hard_link_at<'a>(
        &'a self,
        old_path: impl AsRef<Path>,
        new_dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path>,
        flags: LinkatFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        default_client().hard_link_at(self, old_path, new_dir_fd, new_path, flags)
    }

    /// Method for unlinking a file at the specified path relative to the specified directory fd.
    #[inline]
    fn unlink_at<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_unlink_at(path)
    }

    /// Alias for [`HybridDir::unlink_at`].
    fn hybrid_unlink_at<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<()>> {
        default_client().unlink_at(self, path, UnlinkatFlags::NoRemoveDir)
    }

    /// Method for unlinking a directory at the specified path relative to the specified directory fd.
    #[inline]
    fn remove_dir_at<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_remove_dir_at(path)
    }

    /// Alias for [`HybridDir::remove_dir_at`].
    fn hybrid_remove_dir_at<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<()>> {
        default_client().unlink_at(self, path, UnlinkatFlags::RemoveDir)
    }

    /// Method for creating a symbolic link at the specified path relative to the specified directory fd.
    #[inline]
    fn symlink_at<'a>(
        &'a self,
        target: impl AsRef<Path>,
        link_path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<()>> {
        default_client().symlink_at(target, self, link_path)
    }

    /// Alias for [`HybridDir::symlink_at`].
    fn hybrid_symlink_at<'a>(
        &'a self,
        target: impl AsRef<Path>,
        link_path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<()>> {
        default_client().symlink_at(target, self, link_path)
    }

    /// Method for renaming a file at the specified path relative to the specified directory fd.
    #[inline]
    fn rename_at<'a>(
        &'a self,
        old_path: impl AsRef<Path>,
        new_dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path>,
        flags: RenameFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_rename_at(old_path, new_dir_fd, new_path, flags)
    }

    /// Alias for [`HybridDir::rename_at`].
    fn hybrid_rename_at<'a>(
        &'a self,
        old_path: impl AsRef<Path>,
        new_dir_fd: &'a (impl UringTarget + Sync + ?Sized),
        new_path: impl AsRef<Path>,
        flags: RenameFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        default_client().rename_at(self, old_path, new_dir_fd, new_path, flags)
    }

    /// Method for renaming a file at the specified path to a location pointed to by new_path.
    #[inline]
    fn rename<'a>(
        &'a self,
        old_path: impl AsRef<Path>,
        new_path: impl AsRef<Path>,
        flags: RenameFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_rename(old_path, new_path, flags)
    }

    /// Alias for [`HybridDir::rename`].
    fn hybrid_rename<'a>(
        &'a self,
        old_path: impl AsRef<Path>,
        new_path: impl AsRef<Path>,
        flags: RenameFlags,
    ) -> PendingIo<'a, io::Result<()>> {
        default_client().rename_at(self, old_path, &AT_FDCWD, new_path, flags)
    }

    /// Method for opening a file from a relative path to the directory fd.
    /// Unlike the client methods, this method always returns a [`tokio::fs::File`].
    #[inline]
    fn open_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
        options: &OpenOptions,
    ) -> PendingIo<'a, io::Result<File>> {
        self.hybrid_open_at(path, options)
    }

    /// Alias for [`HybridDir::open_at`].
    fn hybrid_open_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
        options: &OpenOptions,
    ) -> PendingIo<'a, io::Result<File>> {
        let flags = match options.get_flags() {
            Ok(flags) => flags,
            Err(e) => return PendingIo::new(FixedValuePendingIo::new(Err(e))),
        };
        default_client()
            .open_at(self, path, flags, options.get_creation_permissions())
            .map(|r| r.map(|f| File::from_std(std::fs::File::from(f))))
    }

    /// Method for creating a file using a relative path to the directory fd.
    /// Unlike the client methods, this method always returns a [`tokio::fs::File`].
    #[inline]
    fn create_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
        options: &OpenOptions,
    ) -> PendingIo<'a, io::Result<File>> {
        self.hybrid_create_at(path, options)
    }

    /// Alias for [`HybridDir::create_at`].
    fn hybrid_create_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
        options: &OpenOptions,
    ) -> PendingIo<'a, io::Result<File>> {
        let mut options = options.clone();
        options.create(true);
        self.hybrid_open_at(path, &options)
    }

    /// Method for creating a file using a relative path to the directory fd,
    /// ensuring that the file is always created or an error is returned.
    /// Unlike the client methods, this method always returns a [`tokio::fs::File`].
    #[inline]
    fn create_new_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
        options: &OpenOptions,
    ) -> PendingIo<'a, io::Result<File>> {
        self.hybrid_create_new_at(path, options)
    }

    /// Alias method for [`HybridDir::create_new_at`].
    fn hybrid_create_new_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
        options: &OpenOptions,
    ) -> PendingIo<'a, io::Result<File>> {
        let mut options = options.clone();
        options.create_new(true);
        options.create(true);
        self.hybrid_open_at(path, &options)
    }

    /// Method for creating a directory using a relative path to the directory fd.
    #[inline]
    fn create_dir_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_create_dir_at(path, permissions)
    }

    /// Alias for [`HybridDir::create_dir_at`].
    fn hybrid_create_dir_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        default_client().mkdir_at(self, path, permissions)
    }

    /// Method for creating a node at a relative path to the directory fd.
    #[inline]
    fn create_node_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
        kind: MknodType,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        self.hybrid_create_node_at(path, kind, permissions)
    }

    /// Alias for [`HybridDir::create_node_at`].
    fn hybrid_create_node_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
        kind: MknodType,
        permissions: Permissions,
    ) -> PendingIo<'a, io::Result<()>> {
        default_client().mknod_at(self, path, kind, permissions)
    }

    /// Method for reading the target of the symlink at the specified path relative to the specified directory fd.
    /// If the path is empty, the call is identical to [`HybridFile::read_link`].
    #[inline]
    fn read_link_at<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<PathBuf>> {
        default_client().read_link_at(self, path)
    }

    /// Alias for [`HybridFile::read_link_at`].
    fn hybrid_read_link_at<'a>(
        &'a self,
        path: impl AsRef<Path>,
    ) -> PendingIo<'a, io::Result<PathBuf>> {
        default_client().read_link_at(self, path)
    }
}

#[async_trait::async_trait]
pub trait HybridExt:
    UringTarget + HybridRead + HybridWrite + HybridSeek + HybridFile + HybridDir
{
}

macro_rules! hybrid_impl {
    ($struct:ty) => {
        #[async_trait::async_trait]
        impl HybridRead for $struct {}
        #[async_trait::async_trait]
        impl HybridWrite for $struct {}
        #[async_trait::async_trait]
        impl HybridSeek for $struct {}
        #[async_trait::async_trait]
        impl HybridFile for $struct {}
        #[async_trait::async_trait]
        impl HybridDir for $struct {}
        #[async_trait::async_trait]
        impl HybridExt for $struct {}
    };
}

// Implementations for raw file types (do not allow support for arbitrary AsFd/UringTarget type because the trait methods are *much* easier to misuse for wrapper types)
hybrid_impl!(std::fs::File);
hybrid_impl!(tokio::fs::File);
hybrid_impl!(BorrowedFd<'_>);
hybrid_impl!(OwnedFd);
hybrid_impl!(PipeReader);
hybrid_impl!(PipeWriter);
hybrid_impl!(RegisteredFile<'_>);
hybrid_impl!(OwnedRegisteredFile);
hybrid_impl!(Target);

// Dynamic trait object compiler checks.
type _DynCompatibleHybridRead = Box<dyn HybridRead>;
type _DynCompatibleHybridWrite = Box<dyn HybridWrite>;
type _DynCompatibleHybridSeek = Box<dyn HybridSeek>;
type _DynCompatibleHybridFile = Box<dyn HybridFile>;
type _DynCompatibleHybridDir = Box<dyn HybridDir>;
type _DynCompatibleHybridExt = Box<dyn HybridExt>;

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
    async fn test_hybrid_create_and_read_write() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(temp_dir.path().join("test.txt"))
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        file.hybrid_write_all(b"Hello, world!").await.unwrap();
        file.hybrid_flush()
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .open(temp_dir.path().join("test.txt"))
            .completion()
            .expect("no completion future returned")
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
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        file.hybrid_write_all(b"Hello, world!").await.unwrap();
        file.hybrid_flush()
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .open(temp_dir.path().join("test.txt"))
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        file.hybrid_seek(SeekFrom::Start(7))
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
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
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        file.hybrid_set_len(10)
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        assert_eq!(
            file.hybrid_metadata()
                .completion()
                .expect("no completion future returned")
                .await
                .unwrap()
                .len(),
            10
        );
    }

    #[tokio::test]
    async fn test_hybrid_set_times() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(temp_dir.path().join("test.txt"))
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let tnow = TimeSpec::from_duration(now);
        file.set_times(Some(tnow), Some(tnow))
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();

        let file = OpenOptions::new()
            .read(true)
            .open(temp_dir.path().join("test.txt"))
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        let metadata = <File as HybridFile>::metadata(&file)
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
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
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        file.hybrid_write_vectored(&[IoSlice::new(b"Hello, world!")])
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        file.hybrid_flush()
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .open(temp_dir.path().join("test.txt"))
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        let mut buffer = vec![0u8; 128];
        let slice = IoSliceMut::new(&mut buffer);
        let bytes_read = file
            .hybrid_read_vectored(&mut [slice])
            .completion()
            .expect("no completion future returned")
            .await
            .unwrap();
        assert_eq!(&buffer[..bytes_read], b"Hello, world!");
    }
}
