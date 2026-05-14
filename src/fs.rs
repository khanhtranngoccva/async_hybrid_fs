//! High-level filesystem operations, which use default [`Client`](crate::client::Client) instances.
//!
use nix::{
    fcntl::{AT_FDCWD, OFlag},
    sys::{
        stat::{Mode, UtimensatFlags},
        time::TimeSpec,
    },
    unistd::{Gid, Uid},
};
use std::{
    io,
    path::{Path, PathBuf},
    pin::Pin,
};
use tokio::fs::File;

use crate::{
    PendingIo,
    client::pending_io::fixed_value::FixedValuePendingIo,
    default::{self},
    metadata::{Metadata, MknodType, Permissions},
};

/// Options for opening files, mirroring [`std::fs::OpenOptions`].
///
/// # Example
///
/// ```ignore
/// use uring_file::fs::OpenOptions;
///
/// // Open for reading
/// let file = OpenOptions::new().read(true).open("foo.txt").await?;
///
/// // Create for writing
/// let file = OpenOptions::new()
///     .write(true)
///     .create(true)
///     .truncate(true)
///     .open("bar.txt")
///     .await?;
///
/// // Append to existing
/// let file = OpenOptions::new()
///     .append(true)
///     .open("log.txt")
///     .await?;
///
/// // Create new (fails if exists)
/// let file = OpenOptions::new()
///     .write(true)
///     .create_new(true)
///     .open("new.txt")
///     .await?;
/// ```
#[derive(Clone, Debug)]
pub struct OpenOptions {
    /// Whether to open the file for reading.
    pub read: bool,
    /// Whether to open the file for writing.
    pub write: bool,
    /// Whether to open the file for appending.
    pub append: bool,
    /// Whether to truncate the file to zero length.
    pub truncate: bool,
    /// Whether to create the file if it doesn't exist.
    pub create: bool,
    /// Whether to create the file if it doesn't exist.
    pub create_new: bool,
    /// The permissions of the file.
    pub permissions: Permissions,
    /// Custom flags to pass to the underlying `open` syscall.
    pub custom_flags: OFlag,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
            permissions: Permissions::from_mode(0o666),
            custom_flags: OFlag::empty(),
        }
    }
}

impl OpenOptions {
    /// Creates a blank new set of options.
    ///
    /// All options are initially set to `false`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets read access.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    /// Sets write access.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    /// Sets append mode.
    ///
    /// Writes will append to the file instead of overwriting.
    /// Implies `write(true)`.
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }

    /// Sets truncate mode.
    ///
    /// If the file exists, it will be truncated to zero length.
    /// Requires `write(true)`.
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    /// Sets create mode.
    ///
    /// Creates the file if it doesn't exist. Requires `write(true)` or `append(true)`.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    /// Sets create-new mode.
    ///
    /// Creates a new file, failing if it already exists.
    /// Implies `create(true)` and requires `write(true)`.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    /// Sets the file mode (permissions) for newly created files.
    ///
    /// Default is `0o666`.
    pub fn permissions(&mut self, permissions: Permissions) -> &mut Self {
        self.permissions = permissions;
        self
    }

    /// Sets custom flags to pass to the underlying `open` syscall.
    ///
    /// This allows flags like `O_DIRECT`, `O_SYNC`, `O_NOFOLLOW`, `O_CLOEXEC`, etc.
    /// The flags are OR'd with the flags derived from other options.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let file = OpenOptions::new()
    ///     .read(true)
    ///     .write(true)
    ///     .create(true)
    ///     .custom_flags(libc::O_DIRECT | libc::O_SYNC)
    ///     .open("data.bin")
    ///     .await?;
    /// ```
    pub fn custom_flags(&mut self, flags: i32) -> &mut Self {
        self.custom_flags = OFlag::from_bits_truncate(flags & !libc::O_ACCMODE);
        self
    }

    /// Opens a file with the configured options.
    ///
    /// Returns a [`tokio::fs::File`] for async operations.
    ///
    /// # Cancellation safety
    /// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
    pub fn open<'a>(&'a self, path: impl AsRef<Path>) -> PendingIo<'a, io::Result<File>> {
        let flags = match self.get_flags() {
            Ok(flags) => flags,
            Err(e) => return PendingIo::new(FixedValuePendingIo::new(Err(e))),
        };
        let mode = self.get_creation_permissions();
        default::default_client()
            .open_path(path.as_ref(), flags, mode)
            .map(|result| result.map(|f| File::from_std(std::fs::File::from(f))))
    }

    /// Get access mode flags
    pub fn get_access_flags(&self) -> io::Result<OFlag> {
        match (self.read, self.write, self.append) {
            (true, false, false) => Ok(OFlag::O_RDONLY),
            (false, true, false) => Ok(OFlag::O_WRONLY),
            (true, true, false) => Ok(OFlag::O_RDWR),
            (false, _, true) => Ok(OFlag::O_WRONLY | OFlag::O_APPEND),
            (true, _, true) => Ok(OFlag::O_RDWR | OFlag::O_APPEND),
            (false, false, false) => {
                // If no access mode is set, check if any creation flags are set
                // to provide a more descriptive error message
                if self.create || self.create_new || self.truncate {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "creating or truncating a file requires write or append access",
                    ))
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "must specify at least one of read, write, or append access",
                    ))
                }
            }
        }
    }

    /// Get creation flags
    pub fn get_creation_flags(&self) -> io::Result<OFlag> {
        match (self.write, self.append) {
            (true, false) => {}
            (false, false) => {
                if self.truncate || self.create || self.create_new {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "creating or truncating a file requires write or append access",
                    ));
                }
            }
            (_, true) => {
                if self.truncate && !self.create_new {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "creating or truncating a file requires write or append access",
                    ));
                }
            }
        }

        Ok(match (self.create, self.truncate, self.create_new) {
            (false, false, false) => OFlag::empty(),
            (true, false, false) => OFlag::O_CREAT,
            (false, true, false) => OFlag::O_TRUNC,
            (true, true, false) => OFlag::O_CREAT | OFlag::O_TRUNC,
            (_, _, true) => OFlag::O_CREAT | OFlag::O_EXCL,
        })
    }

    /// Get creation permissions
    pub fn get_creation_permissions(&self) -> Permissions {
        self.permissions
    }

    /// Create an OpenOptions from low-level [`OFlag`] and [`Mode`] structs
    pub fn from_flag_and_mode(flags: OFlag, mode: Mode) -> Self {
        Self {
            read: flags.contains(OFlag::O_RDONLY) || flags.contains(OFlag::O_RDWR),
            write: flags.contains(OFlag::O_WRONLY) || flags.contains(OFlag::O_RDWR),
            append: flags.contains(OFlag::O_APPEND),
            truncate: flags.contains(OFlag::O_TRUNC),
            create: flags.contains(OFlag::O_CREAT),
            create_new: flags.contains(OFlag::O_EXCL),
            permissions: Permissions::from_mode(mode.bits()),
            custom_flags: flags & !Self::get_managed_flags(),
        }
    }

    #[inline]
    fn get_managed_flags() -> OFlag {
        OFlag::O_ACCMODE
            | OFlag::O_APPEND
            | OFlag::O_CREAT
            | OFlag::O_TRUNC
            | OFlag::O_EXCL
            | OFlag::O_CLOEXEC
    }

    /// Returns the flags to pass to the underlying `open` syscall.
    pub fn get_flags(&self) -> io::Result<OFlag> {
        Ok(OFlag::O_CLOEXEC
            | self.get_access_flags()?
            | self.get_creation_flags()?
            | (self.custom_flags & !Self::get_managed_flags()))
    }
}

/// Asynchronous version of [`std::fs::canonicalize`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn canonicalize(path: impl AsRef<Path>) -> PendingIo<'static, io::Result<PathBuf>> {
    default::default_client().canonicalize(path)
}

/// Asynchronous version of [`std::fs::copy`].
///
/// # Cancellation safety
/// This method is not cancellable (runs on a blocking thread).
pub async fn copy(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<u64> {
    // FIXME: Implement an io_uring version, currently we prefer std::fs::copy to take advantage of copy_file_range on supported OSes.
    let source = src.as_ref().to_owned();
    let destination = dst.as_ref().to_owned();
    tokio::task::spawn_blocking(move || std::fs::copy(source, destination)).await?
}

/// Asynchronous version of [`std::os::unix::fs::chown`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn chown(
    path: impl AsRef<Path>,
    uid: Option<Uid>,
    gid: Option<Gid>,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().chown(path, uid, gid)
}

/// Asynchronous version of [`std::os::unix::fs::lchown`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn lchown(
    path: impl AsRef<Path>,
    uid: Option<Uid>,
    gid: Option<Gid>,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().lchown(path, uid, gid)
}

/// Asynchronous version of [`std::fs::create_dir`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn create_dir(path: impl AsRef<Path>) -> PendingIo<'static, io::Result<()>> {
    default::default_client().create_dir(path)
}

/// Asynchronous version of [`std::fs::create_dir_all`].
///
/// # Cancellation safety
/// This method is not cancellation-safe.
pub fn create_dir_all(
    path: impl AsRef<Path>,
) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
    default::default_client().create_dir_all(path)
}

/// Asynchronous version of [`std::fs::create_dir`] with permissions.
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn create_dir_with_permissions(
    path: impl AsRef<Path>,
    permissions: Permissions,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().mkdir(path, permissions)
}

/// Asynchronous version of [`std::fs::create_dir_all`] with permissions.
///
/// # Cancellation safety
/// This method is not cancellation-safe.
pub fn create_dir_all_with_permissions(
    path: impl AsRef<Path>,
    permissions: Permissions,
) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
    default::default_client().create_dir_all_with_permissions(path, permissions)
}

/// Asynchronous version of [`libc::mknod`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn create_node(
    path: impl AsRef<Path>,
    kind: MknodType,
    permissions: Permissions,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().mknod(path, kind, permissions)
}

/// Asynchronous version of [`std::fs::exists`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn exists(path: impl AsRef<Path>) -> PendingIo<'static, io::Result<bool>> {
    default::default_client()
        .metadata_path(path)
        .map(|result| match result {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        })
}

/// Asynchronous version of [`std::fs::hard_link`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn hard_link(
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().hard_link(src, dst)
}

/// Asynchronous version of [`std::fs::metadata`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn metadata(path: impl AsRef<Path>) -> PendingIo<'static, io::Result<Metadata>> {
    default::default_client().metadata_path(path)
}

/// Asynchronous version of [`std::fs::symlink_metadata`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn symlink_metadata(path: impl AsRef<Path>) -> PendingIo<'static, io::Result<Metadata>> {
    default::default_client().symlink_metadata_path(path)
}

/// Asynchronous version of [`std::fs::read_dir`].
///
/// # Cancellation safety
/// This method is not cancellable (runs on a blocking thread).
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<tokio::fs::ReadDir> {
    // FIXME: This function depends on tokio
    tokio::fs::read_dir(path).await
}

/// Asynchronous version of [`std::fs::read_link`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn read_link(path: impl AsRef<Path>) -> PendingIo<'static, io::Result<PathBuf>> {
    default::default_client().read_link(path)
}

/// Asynchronous version of [`std::fs::read_to_string`].
///
/// # Cancellation safety
/// This method is not cancellation-safe.
pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(path)
        .completion()
        .expect("no completion future returned")
        .await?;
    let mut buf = String::new();
    default::default_client()
        .read_to_string(&mut file, &mut buf)
        .await?;
    Ok(buf)
}

/// Asynchronous version of [`std::fs::remove_dir`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn remove_dir(path: impl AsRef<Path>) -> PendingIo<'static, io::Result<()>> {
    default::default_client().rmdir(path)
}

/// Asynchronous version of [`std::fs::remove_dir_all`].
///
/// # Cancellation safety
/// This method is not cancellation-safe.
pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let client = default::default_client();
    let metadata = client
        .metadata_path(path.as_ref())
        .completion()
        .expect("no completion future returned")
        .await?;
    let filetype = metadata.file_type();
    if filetype.is_symlink() {
        client
            .unlink(path)
            .completion()
            .expect("no completion future returned")
            .await
    } else {
        remove_dir_all_recursive(path.as_ref()).await
    }
}

async fn remove_dir_all_recursive(path: &Path) -> io::Result<()> {
    let client = default::default_client();
    let mut dir = read_dir(path).await?;
    loop {
        let entry_res = dir.next_entry().await;
        let child = match entry_res {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        match child.file_type().await {
            Ok(file_type) if file_type.is_dir() => {
                match Box::pin(remove_dir_all_recursive(&child.path())).await {
                    Ok(_) => continue,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                }
            }
            Ok(_) => match client
                .unlink(child.path())
                .completion()
                .expect("no completion future returned")
                .await
            {
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        }
    }
    match client
        .rmdir(path)
        .completion()
        .expect("no completion future returned")
        .await
    {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Asynchronous version of [`std::fs::remove_file`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn remove_file(path: impl AsRef<Path>) -> PendingIo<'static, io::Result<()>> {
    default::default_client().unlink(path)
}

/// Asynchronous version of [`std::fs::rename`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn rename(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> PendingIo<'static, io::Result<()>> {
    default::default_client().rename(src, dst)
}

/// Asynchronous version of [`std::os::unix::fs::chown`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn set_owner(
    path: impl AsRef<Path>,
    uid: Option<Uid>,
    gid: Option<Gid>,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().chown(path, uid, gid)
}

/// Asynchronous version of [`std::os::unix::fs::lchown`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn set_owner_nofollow(
    path: impl AsRef<Path>,
    uid: Option<Uid>,
    gid: Option<Gid>,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().lchown(path, uid, gid)
}

/// Asynchronous version of [`std::fs::set_permissions`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn set_permissions(
    path: impl AsRef<Path>,
    permissions: Permissions,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().chmod(path, permissions)
}

/// Asynchronous version of [`std::fs::set_permissions_nofollow`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn set_permissions_nofollow(
    path: impl AsRef<Path>,
    permissions: Permissions,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().lchmod(path, permissions)
}

/// Asynchronous version of [`std::fs::set_times`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn set_times(
    path: impl AsRef<Path>,
    atime: Option<TimeSpec>,
    mtime: Option<TimeSpec>,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().utimens_at(
        &AT_FDCWD,
        path,
        atime,
        mtime,
        UtimensatFlags::FollowSymlink,
    )
}

/// Asynchronous version of [`std::fs::set_times_nofollow`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn set_times_nofollow(
    path: impl AsRef<Path>,
    atime: Option<TimeSpec>,
    mtime: Option<TimeSpec>,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().utimens_at(
        &AT_FDCWD,
        path,
        atime,
        mtime,
        UtimensatFlags::NoFollowSymlink,
    )
}

/// Asynchronous version of [`std::fs::soft_link`]. The [`symlink`] method should be used instead.
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn soft_link(
    target: impl AsRef<Path>,
    link: impl AsRef<Path>,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().symlink(target, link)
}

/// Asynchronous version of [`std::os::unix::fs::symlink`].
///
/// # Cancellation safety
/// This method is partially cancellation-safe. See [cancellation safety notes](`crate#cancellation-safety-and-correctness`) for details.
pub fn symlink(
    target: impl AsRef<Path>,
    link: impl AsRef<Path>,
) -> PendingIo<'static, io::Result<()>> {
    default::default_client().symlink(target, link)
}

/// Asynchronous version of [`std::fs::write`].
///
/// # Cancellation safety
/// This method is not cancellation-safe.
pub async fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .completion()
        .expect("no completion future returned")
        .await?;
    default::default_client()
        .write_all(&mut file, contents.as_ref())
        .await?;
    Ok(())
}
