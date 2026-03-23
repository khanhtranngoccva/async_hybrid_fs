use nix::{
    fcntl::OFlag,
    sys::{stat::UtimensatFlags, time::TimeSpec},
    unistd::{Gid, Uid},
};
use std::{
    io,
    os::fd::BorrowedFd,
    path::{Path, PathBuf},
};
use tokio::fs::File;

use crate::{
    default::{self},
    metadata::{Metadata, Permissions},
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
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub truncate: bool,
    pub create: bool,
    pub create_new: bool,
    pub permissions: Permissions,
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
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let flags = self.get_flags()?;
        let mode = self.get_creation_permissions();
        let fd = default::default_client()
            .open_path(path.as_ref(), flags, mode)
            .await?;
        Ok(File::from_std(std::fs::File::from(fd)))
    }

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

    pub fn get_creation_permissions(&self) -> Permissions {
        self.permissions
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

pub async fn canonicalize(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    default::default_client().canonicalize(path).await
}

pub async fn copy(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<u64> {
    // FIXME: Implement an io_uring version, currently we prefer std::fs::copy to take advantage of copy_file_range on supported OSes.
    let source = src.as_ref().to_owned();
    let destination = dst.as_ref().to_owned();
    tokio::task::spawn_blocking(move || std::fs::copy(source, destination)).await?
}

pub async fn chown(path: impl AsRef<Path>, uid: Option<Uid>, gid: Option<Gid>) -> io::Result<()> {
    default::default_client().chown(path, uid, gid).await
}

pub async fn lchown(path: impl AsRef<Path>, uid: Option<Uid>, gid: Option<Gid>) -> io::Result<()> {
    default::default_client().lchown(path, uid, gid).await
}

pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    default::default_client().create_dir(path).await
}

pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    default::default_client().create_dir_all(path).await
}

pub async fn exists(path: impl AsRef<Path>) -> io::Result<bool> {
    match default::default_client().metadata_path(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

pub async fn hard_link(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    default::default_client().hard_link(src, dst).await
}

pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    default::default_client().metadata_path(path).await
}

pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let mut file = OpenOptions::new().read(true).open(path).await?;
    let mut buf = Vec::new();
    default::default_client()
        .read_to_end(&mut file, &mut buf)
        .await?;
    Ok(buf)
}

pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<tokio::fs::ReadDir> {
    // FIXME: This function depends on tokio
    tokio::fs::read_dir(path).await
}

pub async fn read_link(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    default::default_client().read_link(path).await
}

pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    let mut file = OpenOptions::new().read(true).open(path).await?;
    let mut buf = String::new();
    default::default_client()
        .read_to_string(&mut file, &mut buf)
        .await?;
    Ok(buf)
}

pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    default::default_client().rmdir(path).await
}

pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let client = default::default_client();
    let metadata = client.metadata_path(path.as_ref()).await?;
    let filetype = metadata.file_type();
    if filetype.is_symlink() {
        client.unlink(path).await
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
            Ok(_) => match client.unlink(&child.path()).await {
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        }
    }
    match client.rmdir(path).await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    default::default_client().unlink(path).await
}

pub async fn rename(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    default::default_client().rename(src, dst).await
}

pub async fn set_owner(
    path: impl AsRef<Path>,
    uid: Option<Uid>,
    gid: Option<Gid>,
) -> io::Result<()> {
    default::default_client().chown(path, uid, gid).await
}

pub async fn set_owner_nofollow(
    path: impl AsRef<Path>,
    uid: Option<Uid>,
    gid: Option<Gid>,
) -> io::Result<()> {
    default::default_client().lchown(path, uid, gid).await
}

pub async fn set_permissions(path: impl AsRef<Path>, permissions: Permissions) -> io::Result<()> {
    default::default_client().chmod(path, permissions).await
}

pub async fn set_permissions_nofollow(
    path: impl AsRef<Path>,
    permissions: Permissions,
) -> io::Result<()> {
    default::default_client().lchmod(path, permissions).await
}

pub async fn set_times(
    path: impl AsRef<Path>,
    atime: Option<TimeSpec>,
    mtime: Option<TimeSpec>,
) -> io::Result<()> {
    default::default_client()
        .utimensat(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            atime,
            mtime,
            UtimensatFlags::FollowSymlink,
        )
        .await
}

pub async fn set_times_nofollow(
    path: impl AsRef<Path>,
    atime: Option<TimeSpec>,
    mtime: Option<TimeSpec>,
) -> io::Result<()> {
    default::default_client()
        .utimensat(
            &unsafe { BorrowedFd::borrow_raw(libc::AT_FDCWD) },
            path,
            atime,
            mtime,
            UtimensatFlags::FollowSymlink,
        )
        .await
}

pub async fn soft_link(target: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    default::default_client().symlink(target, link).await
}

pub async fn symlink(target: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    default::default_client().symlink(target, link).await
}

pub async fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    default::default_client().symlink_metadata_path(path).await
}

pub async fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .await?;
    default::default_client()
        .write_all(&mut file, contents.as_ref())
        .await?;
    Ok(())
}
