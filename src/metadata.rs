//! File metadata types compatible with `std::fs::Metadata`.

use nix::sys::stat::SFlag;
use std::fmt;
use std::io;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

// =============================================================================
// Metadata
// =============================================================================

/// File metadata returned by statx. Provides an interface compatible with [`std::fs::Metadata`] and [`std::os::unix::fs::MetadataExt`].
#[derive(Clone)]
pub struct Metadata(
    #[cfg(target_os = "linux")] pub(crate) libc::statx,
    #[cfg(not(target_os = "linux"))] pub(crate) libc::stat,
);

impl Metadata {
    // ===========================================================================
    // std::fs::Metadata interface
    // ===========================================================================

    /// Returns the file type for this metadata.
    pub fn file_type(&self) -> FileType {
        #[cfg(target_os = "linux")]
        {
            FileType(self.0.stx_mode)
        }
        #[cfg(not(target_os = "linux"))]
        {
            FileType(self.0.st_mode)
        }
    }

    /// Returns `true` if this metadata is for a directory.
    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }

    /// Returns `true` if this metadata is for a regular file.
    pub fn is_file(&self) -> bool {
        self.file_type().is_file()
    }

    /// Returns `true` if this metadata is for a symbolic link.
    pub fn is_symlink(&self) -> bool {
        self.file_type().is_symlink()
    }

    /// Returns the size of the file, in bytes.
    pub fn len(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_size
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_size.max(0) as u64
        }
    }

    /// Returns `true` if the file size is 0 bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the permissions of the file.
    pub fn permissions(&self) -> Permissions {
        let mode = {
            #[cfg(target_os = "linux")]
            {
                self.0.stx_mode
            }
            #[cfg(not(target_os = "linux"))]
            {
                self.0.st_mode
            }
        };
        Permissions(mode as u32 & 0o7777)
    }

    /// Returns the last modification time.
    pub fn modified(&self) -> io::Result<SystemTime> {
        #[cfg(target_os = "linux")]
        {
            Ok(system_time_from_unix(
                self.0.stx_mtime.tv_sec,
                self.0.stx_mtime.tv_nsec,
            ))
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(system_time_from_unix(
                self.0.st_mtime,
                u32::try_from(self.0.st_mtime_nsec)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
            ))
        }
    }

    /// Returns the last access time.
    pub fn accessed(&self) -> io::Result<SystemTime> {
        #[cfg(target_os = "linux")]
        {
            Ok(system_time_from_unix(
                self.0.stx_atime.tv_sec,
                self.0.stx_atime.tv_nsec,
            ))
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(system_time_from_unix(
                self.0.st_atime,
                u32::try_from(self.0.st_atime_nsec)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
            ))
        }
    }

    /// Returns the creation time (if supported by filesystem).
    pub fn created(&self) -> io::Result<SystemTime> {
        #[cfg(target_os = "linux")]
        {
            Ok(system_time_from_unix(
                self.0.stx_btime.tv_sec,
                self.0.stx_btime.tv_nsec,
            ))
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(system_time_from_unix(
                self.0.st_birthtime,
                u32::try_from(self.0.st_birthtime_nsec)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
            ))
        }
    }

    // ===========================================================================
    // std::os::unix::fs::MetadataExt interface
    // ===========================================================================

    /// Returns the number of the device that the file resides in.
    pub fn dev(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            libc::makedev(self.0.stx_dev_major, self.0.stx_dev_minor) as u64
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_dev as u32 as u64
        }
    }

    /// Returns the inode number of the file.
    pub fn ino(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_ino
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_ino
        }
    }

    /// Returns the mode of the file.
    pub fn mode(&self) -> u32 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_mode as u32
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_mode as u32
        }
    }

    /// Returns the number of hard links to the file.
    pub fn nlink(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_nlink as u64
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_nlink as u64
        }
    }

    /// Returns the user ID of the file.
    pub fn uid(&self) -> u32 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_uid
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_uid
        }
    }

    /// Returns the group ID of the file.
    pub fn gid(&self) -> u32 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_gid
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_gid
        }
    }

    /// Returns the device number that the file points to (if it is a device file).
    pub fn rdev(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            libc::makedev(self.0.stx_rdev_major, self.0.stx_rdev_minor) as u64
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_rdev as u32 as u64
        }
    }

    /// Returns the size of the file, in bytes.
    pub fn size(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_size
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_size.max(0) as u64
        }
    }

    /// Returns the last access time.
    pub fn atime(&self) -> i64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_atime.tv_sec
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_atime
        }
    }

    /// Returns the nanoseconds part of the last access time.
    pub fn atime_nsec(&self) -> i64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_atime.tv_nsec as i64
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_atime_nsec
        }
    }

    /// Returns the last modification time.
    pub fn mtime(&self) -> i64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_mtime.tv_sec
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_mtime
        }
    }

    /// Returns the nanoseconds part of the last modification time.
    pub fn mtime_nsec(&self) -> i64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_mtime.tv_nsec as i64
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_mtime_nsec
        }
    }

    /// Returns the creation time.
    pub fn ctime(&self) -> i64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_ctime.tv_sec
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_ctime
        }
    }

    /// Returns the nanoseconds part of the creation time.
    pub fn ctime_nsec(&self) -> i64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_ctime.tv_nsec as i64
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_ctime_nsec
        }
    }

    /// Returns the block size of the file.
    pub fn blksize(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_blksize as u64
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_blksize.max(0) as u64
        }
    }

    /// Returns the number of blocks allocated for the file.
    pub fn blocks(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            self.0.stx_blocks
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.0.st_blocks.max(0) as u64
        }
    }
}

impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Metadata")
            .field("file_type", &self.file_type())
            .field("permissions", &self.permissions())
            .field("len", &self.len())
            .field("uid", &self.uid())
            .field("gid", &self.gid())
            .field("ino", &self.ino())
            .finish_non_exhaustive()
    }
}

#[cfg(target_os = "linux")]
// Conversion traits to and from libc::statx for flexible low-level mutation (e.g. if custom wrappers want to modify values in the structure)
impl From<libc::statx> for Metadata {
    fn from(statx: libc::statx) -> Self {
        Self(statx)
    }
}

#[cfg(target_os = "linux")]
impl From<Metadata> for libc::statx {
    fn from(val: Metadata) -> Self {
        val.0
    }
}

// =============================================================================
// FileType
// =============================================================================

/// Representation of file types. Equivalent to [`std::fs::FileType`].
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileType(pub(crate) u16);

impl FileType {
    /// Returns `true` if this file type is a directory.
    pub fn is_dir(&self) -> bool {
        (self.0 & libc::S_IFMT as u16) == libc::S_IFDIR as u16
    }

    /// Returns `true` if this file type is a regular file.
    pub fn is_file(&self) -> bool {
        (self.0 & libc::S_IFMT as u16) == libc::S_IFREG as u16
    }

    /// Returns `true` if this file type is a symbolic link.
    pub fn is_symlink(&self) -> bool {
        (self.0 & libc::S_IFMT as u16) == libc::S_IFLNK as u16
    }

    /// Returns `true` if this file type is a block device.
    pub fn is_block_device(&self) -> bool {
        (self.0 & libc::S_IFMT as u16) == libc::S_IFBLK as u16
    }

    /// Returns `true` if this file type is a character device.
    pub fn is_char_device(&self) -> bool {
        (self.0 & libc::S_IFMT as u16) == libc::S_IFCHR as u16
    }

    /// Returns `true` if this file type is a FIFO pipe.
    pub fn is_fifo(&self) -> bool {
        (self.0 & libc::S_IFMT as u16) == libc::S_IFIFO as u16
    }

    /// Returns `true` if this file type is a socket.
    pub fn is_socket(&self) -> bool {
        (self.0 & libc::S_IFMT as u16) == libc::S_IFSOCK as u16
    }
}

impl fmt::Debug for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = if self.is_file() {
            "file"
        } else if self.is_dir() {
            "directory"
        } else if self.is_symlink() {
            "symlink"
        } else if self.is_block_device() {
            "block_device"
        } else if self.is_char_device() {
            "char_device"
        } else if self.is_fifo() {
            "fifo"
        } else if self.is_socket() {
            "socket"
        } else {
            "unknown"
        };
        write!(f, "FileType({kind})")
    }
}

// =============================================================================
// CreateNodeType
// =============================================================================

/// Representation of device numbers in the major-minor form. Defaults to 0 for both major and minor. Can be converted to and from [`libc::dev_t`] with the [`From`] and [`Into`] traits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct DeviceNumber {
    /// The major number of the device.
    pub major: u32,
    /// The minor number of the device.
    pub minor: u32,
}

impl From<libc::dev_t> for DeviceNumber {
    fn from(dev: libc::dev_t) -> Self {
        Self {
            major: libc::major(dev) as u32,
            minor: libc::minor(dev) as u32,
        }
    }
}

impl From<DeviceNumber> for libc::dev_t {
    fn from(val: DeviceNumber) -> Self {
        #[cfg(target_os = "linux")]
        {
            libc::makedev(val.major as libc::c_uint, val.minor as libc::c_uint)
        }
        #[cfg(not(target_os = "linux"))]
        {
            val.major as libc::dev_t | val.minor as libc::dev_t
        }
    }
}

/// High-level representation of types that can be created with `mknodat(2)`.
///
/// # Note
/// - Symlinks are not supported by this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MknodType {
    /// A block device.
    BlockDevice(DeviceNumber),
    /// A character device.
    CharDevice(DeviceNumber),
    /// A FIFO pipe.
    Fifo,
    /// A socket.
    Socket,
    /// A regular file.
    RegularFile,
    /// A directory.
    Directory,
}

impl MknodType {
    /// Converts this `MknodType` to a [`SFlag`] and a [`DeviceNumber`].
    pub fn to_sflag_and_device(&self) -> (SFlag, DeviceNumber) {
        match self {
            MknodType::BlockDevice(device) => (SFlag::S_IFBLK, *device),
            MknodType::CharDevice(device) => (SFlag::S_IFCHR, *device),
            MknodType::Fifo => (SFlag::S_IFIFO, DeviceNumber::default()),
            MknodType::Socket => (SFlag::S_IFSOCK, DeviceNumber::default()),
            MknodType::RegularFile => (SFlag::S_IFREG, DeviceNumber::default()),
            MknodType::Directory => (SFlag::S_IFDIR, DeviceNumber::default()),
        }
    }

    /// Create a `MknodType` from a [`SFlag`] and a [`DeviceNumber`].
    pub fn from_sflag_and_device(sflag: SFlag, device: DeviceNumber) -> io::Result<Self> {
        match (sflag, device) {
            (SFlag::S_IFBLK, device) => Ok(MknodType::BlockDevice(device)),
            (SFlag::S_IFCHR, device) => Ok(MknodType::CharDevice(device)),
            (SFlag::S_IFIFO, _) => Ok(MknodType::Fifo),
            (SFlag::S_IFSOCK, _) => Ok(MknodType::Socket),
            (SFlag::S_IFREG, _) => Ok(MknodType::RegularFile),
            (SFlag::S_IFDIR, _) => Ok(MknodType::Directory),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid SFlag and device",
            )),
        }
    }
}

// =============================================================================
// Permissions
// =============================================================================

/// Representation of file permissions. Equivalent to [`std::fs::Permissions`].
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Permissions(u32);

impl Permissions {
    /// Returns `true` if this permissions object indicates the file is readonly.
    pub fn readonly(&self) -> bool {
        (self.0 & 0o200) == 0
    }

    /// Returns the mode numberof the file.
    pub fn mode(&self) -> u32 {
        self.0
    }

    /// Creates a new `Permissions` object from a mode number.
    pub fn from_mode(mode: u32) -> Self {
        Self(mode & 0o7777)
    }

    /// Sets the readonly flag of the permissions object.
    pub fn set_readonly(&mut self, readonly: bool) {
        if readonly {
            self.0 &= !0o222;
        } else {
            self.0 |= 0o200;
        }
    }
}

impl fmt::Debug for Permissions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Permissions({:04o})", self.0)
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn system_time_from_unix(sec: i64, nsec: u32) -> SystemTime {
    if sec >= 0 {
        UNIX_EPOCH + Duration::new(sec as u64, nsec)
    } else {
        UNIX_EPOCH - Duration::new((-sec) as u64, nsec)
    }
}
