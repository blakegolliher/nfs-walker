//! NFS entry types and data structures
//!
//! These types represent filesystem entries returned from NFS operations
//! and are shaped for bulk transfer into the sharded Parquet writers.

/// Type of filesystem entry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Default)]
#[repr(u8)]
pub enum EntryType {
    /// Regular file
    #[default]
    File = 0,
    /// Directory
    Directory = 1,
    /// Symbolic link
    Symlink = 2,
    /// Block device
    BlockDevice = 3,
    /// Character device
    CharDevice = 4,
    /// Named pipe (FIFO)
    Fifo = 5,
    /// Unix socket
    Socket = 6,
    /// Unknown type
    Unknown = 255,
}

impl EntryType {
    /// Check if this is a regular file
    pub fn is_file(&self) -> bool {
        *self == EntryType::File
    }

    /// Check if this is a directory
    pub fn is_dir(&self) -> bool {
        *self == EntryType::Directory
    }

    /// Get database integer representation
    pub fn as_db_int(&self) -> i32 {
        *self as i32
    }
}

/// Extract a lowercase extension from a filename.
///
/// Deliberately conservative: a name with no dot, a leading-dot name
/// (`.bashrc`), or a trailing-dot name (`file.`) has no extension.
/// Extensions of 10+ characters are treated as not-an-extension (they
/// are almost always version suffixes or hashes, and they bloat the
/// dictionary-encoded column).
pub fn extract_extension(name: &str) -> Option<String> {
    let (stem, ext) = name.rsplit_once('.')?;
    if stem.is_empty() || ext.is_empty() || ext.len() >= 10 {
        return None;
    }
    Some(ext.to_lowercase())
}

/// Statistics for a filesystem entry.
///
/// Timestamps are carried as (seconds, nanoseconds) pairs exactly as
/// libnfs hands them over; the Parquet writer derives the legacy
/// microsecond columns from these at flush time.
#[derive(Debug, Clone, Default)]
pub struct NfsStat {
    /// File size in bytes
    pub size: u64,

    /// Inode number
    pub inode: u64,

    /// Number of hard links
    pub nlink: u64,

    /// User ID
    pub uid: u32,

    /// Group ID
    pub gid: u32,

    /// File mode (type + permissions)
    pub mode: u32,

    /// Modification time: seconds since Unix epoch + nanoseconds.
    pub mtime_sec: Option<i64>,
    pub mtime_nsec: Option<i32>,
    /// Access time: seconds since Unix epoch + nanoseconds.
    pub atime_sec: Option<i64>,
    pub atime_nsec: Option<i32>,
    /// Status-change time: seconds since Unix epoch + nanoseconds.
    pub ctime_sec: Option<i64>,
    pub ctime_nsec: Option<i32>,

    /// Number of 512-byte blocks allocated
    pub blocks: u64,
}

/// A directory entry returned from readdir operations
#[derive(Debug, Clone)]
pub struct NfsDirEntry {
    /// Entry name (not full path)
    pub name: String,

    /// Entry type
    pub entry_type: EntryType,

    /// File statistics (if available from READDIRPLUS)
    pub stat: Option<NfsStat>,

    /// Inode number (always available)
    pub inode: u64,

    /// File handle from READDIRPLUS name_handle (for directories)
    /// Used to skip LOOKUP RPCs when opening subdirectories
    pub file_handle: Option<Vec<u8>>,
}

impl NfsDirEntry {
    /// Get file size (0 if stat not available)
    pub fn size(&self) -> u64 {
        self.stat.as_ref().map(|s| s.size).unwrap_or(0)
    }

    /// High-precision mtime: seconds component
    pub fn mtime_sec(&self) -> Option<i64> {
        self.stat.as_ref().and_then(|s| s.mtime_sec)
    }
    /// High-precision mtime: nanoseconds component
    pub fn mtime_nsec(&self) -> Option<i32> {
        self.stat.as_ref().and_then(|s| s.mtime_nsec)
    }
    /// High-precision atime: seconds component
    pub fn atime_sec(&self) -> Option<i64> {
        self.stat.as_ref().and_then(|s| s.atime_sec)
    }
    /// High-precision atime: nanoseconds component
    pub fn atime_nsec(&self) -> Option<i32> {
        self.stat.as_ref().and_then(|s| s.atime_nsec)
    }
    /// High-precision ctime: seconds component
    pub fn ctime_sec(&self) -> Option<i64> {
        self.stat.as_ref().and_then(|s| s.ctime_sec)
    }
    /// High-precision ctime: nanoseconds component
    pub fn ctime_nsec(&self) -> Option<i32> {
        self.stat.as_ref().and_then(|s| s.ctime_nsec)
    }

    /// Get mode bits
    pub fn mode(&self) -> Option<u32> {
        self.stat.as_ref().map(|s| s.mode)
    }

    /// Get uid
    pub fn uid(&self) -> Option<u32> {
        self.stat.as_ref().map(|s| s.uid)
    }

    /// Get gid
    pub fn gid(&self) -> Option<u32> {
        self.stat.as_ref().map(|s| s.gid)
    }

    /// Get nlink
    pub fn nlink(&self) -> Option<u64> {
        self.stat.as_ref().map(|s| s.nlink)
    }

    /// Get blocks (512-byte blocks allocated)
    pub fn blocks(&self) -> u64 {
        self.stat.as_ref().map(|s| s.blocks).unwrap_or(0)
    }
}

/// A database entry ready for insertion.
///
/// Timestamps are (sec, nsec) pairs only; the Parquet writer derives the
/// microsecond `*_us` columns from them so the walker's hot path never
/// carries a third redundant representation.
#[derive(Debug, Clone, Default)]
pub struct DbEntry {
    /// Parent directory path. `None` means "derive from `path`" — the
    /// writer recomputes it zero-copy, so the walker never has to clone
    /// the parent string per entry.
    pub parent_path: Option<String>,

    /// Entry name (just the filename, not full path)
    pub name: String,

    /// Full path from mount point
    pub path: String,

    /// Entry type
    pub entry_type: EntryType,

    /// File size in bytes
    pub size: u64,

    /// Modification time: seconds component since Unix epoch.
    pub mtime_sec: Option<i64>,

    /// Modification time: nanoseconds component (0..999_999_999).
    pub mtime_nsec: Option<i32>,

    /// Access time: seconds component. Pairs with `atime_nsec`.
    pub atime_sec: Option<i64>,

    /// Access time: nanoseconds component (0..999_999_999).
    pub atime_nsec: Option<i32>,

    /// Change time: seconds component. Pairs with `ctime_nsec`.
    pub ctime_sec: Option<i64>,

    /// Change time: nanoseconds component (0..999_999_999).
    pub ctime_nsec: Option<i32>,

    /// Permission mode
    pub mode: Option<u32>,

    /// Owner user ID
    pub uid: Option<u32>,

    /// Owner group ID
    pub gid: Option<u32>,

    /// Number of hard links
    pub nlink: Option<u64>,

    /// Inode number
    pub inode: u64,

    /// Directory depth from root
    pub depth: u32,

    /// File extension (without dot, lowercase)
    pub extension: Option<String>,

    /// Number of 512-byte blocks allocated
    pub blocks: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_extension() {
        assert_eq!(extract_extension("file.txt"), Some("txt".to_string()));
        assert_eq!(extract_extension("archive.tar.gz"), Some("gz".to_string()));
        assert_eq!(extract_extension("UPPER.TXT"), Some("txt".to_string()));
        // No dot → no extension (previously returned the whole name).
        assert_eq!(extract_extension("README"), None);
        // Leading-dot names are hidden files, not extensions.
        assert_eq!(extract_extension(".bashrc"), None);
        // Trailing dot → empty extension → none.
        assert_eq!(extract_extension("file."), None);
        // Overlong "extensions" are noise, not extensions.
        assert_eq!(extract_extension("blob.0123456789abcdef"), None);
    }

    #[test]
    fn test_entry_type_helpers() {
        assert!(EntryType::File.is_file());
        assert!(!EntryType::File.is_dir());
        assert!(EntryType::Directory.is_dir());
        assert_eq!(EntryType::File.as_db_int(), 0);
        assert_eq!(EntryType::Directory.as_db_int(), 1);
    }
}
