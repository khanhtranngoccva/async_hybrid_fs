bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    /// Flags used when getting file metadata.
    pub struct MetadataAtFlags: i32 {
        const AT_EMPTY_PATH = libc::AT_EMPTY_PATH;
        const AT_SYMLINK_NOFOLLOW = libc::AT_SYMLINK_NOFOLLOW;
    }
}
