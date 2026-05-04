//! NFS entry types and data structures
//!
//! These types represent filesystem entries returned from NFS operations
//! and are designed to be efficient for bulk transfers to SQLite.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Convert a microseconds-since-epoch value to a SystemTime, handling
/// pre-epoch (negative) inputs by walking backwards from UNIX_EPOCH.
fn micros_to_system_time(us: i64) -> SystemTime {
    if us >= 0 {
        UNIX_EPOCH + Duration::from_micros(us as u64)
    } else {
        UNIX_EPOCH - Duration::from_micros((-us) as u64)
    }
}

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
    /// Convert from u8 (database value)
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => EntryType::File,
            1 => EntryType::Directory,
            2 => EntryType::Symlink,
            3 => EntryType::BlockDevice,
            4 => EntryType::CharDevice,
            5 => EntryType::Fifo,
            6 => EntryType::Socket,
            _ => EntryType::Unknown,
        }
    }

    /// Convert from libnfs file type constant
    pub fn from_nfs_type(nfs_type: u32) -> Self {
        // libnfs uses standard Unix type values
        match nfs_type & 0o170000 {
            0o100000 => EntryType::File,      // S_IFREG
            0o040000 => EntryType::Directory, // S_IFDIR
            0o120000 => EntryType::Symlink,   // S_IFLNK
            0o060000 => EntryType::BlockDevice, // S_IFBLK
            0o020000 => EntryType::CharDevice, // S_IFCHR
            0o010000 => EntryType::Fifo,      // S_IFIFO
            0o140000 => EntryType::Socket,    // S_IFSOCK
            _ => EntryType::Unknown,
        }
    }

    /// Convert from mode bits
    pub fn from_mode(mode: u32) -> Self {
        Self::from_nfs_type(mode)
    }

    /// Check if this is a regular file
    pub fn is_file(&self) -> bool {
        *self == EntryType::File
    }

    /// Check if this is a directory
    pub fn is_dir(&self) -> bool {
        *self == EntryType::Directory
    }

    /// Check if this is a symbolic link
    pub fn is_symlink(&self) -> bool {
        *self == EntryType::Symlink
    }

    /// Get database integer representation
    pub fn as_db_int(&self) -> i32 {
        *self as i32
    }
}

/// File permissions (Unix mode bits without type)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Permissions(pub u32);

impl Permissions {
    /// Create from full mode (strips type bits)
    pub fn from_mode(mode: u32) -> Self {
        Self(mode & 0o7777)
    }

    /// Get the raw permission bits
    pub fn bits(&self) -> u32 {
        self.0
    }

    /// Check if owner can read
    pub fn owner_read(&self) -> bool {
        self.0 & 0o400 != 0
    }

    /// Check if owner can write
    pub fn owner_write(&self) -> bool {
        self.0 & 0o200 != 0
    }

    /// Check if owner can execute
    pub fn owner_exec(&self) -> bool {
        self.0 & 0o100 != 0
    }
}

/// Statistics for a filesystem entry
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

    /// Last access time (microseconds since Unix epoch)
    ///
    /// Packs the libnfs sec + nsec fields together via the helpers in
    /// `connection.rs` so sub-second precision is preserved end-to-end
    /// (RocksDB → Parquet `atime_us` → migration `nfs_utimes`).
    pub atime: Option<i64>,

    /// Last modification time (microseconds since Unix epoch)
    pub mtime: Option<i64>,

    /// Status change time (microseconds since Unix epoch)
    pub ctime: Option<i64>,

    // High-precision time companions. When `Some`, these carry full
    // nanosecond resolution from libnfs (the `*_us` fields above are
    // a microsecond-truncated form of the same instant). When `None`
    // (e.g. legacy DBs or sources that did not expose nsec), consumers
    // fall back to splitting the `*_us` values.
    pub mtime_sec: Option<i64>,
    pub mtime_nsec: Option<i32>,
    pub atime_sec: Option<i64>,
    pub atime_nsec: Option<i32>,
    pub ctime_sec: Option<i64>,
    pub ctime_nsec: Option<i32>,

    /// Block size
    pub blksize: u64,

    /// Number of 512-byte blocks allocated
    pub blocks: u64,
}

impl NfsStat {
    /// Get the entry type from mode
    pub fn entry_type(&self) -> EntryType {
        EntryType::from_mode(self.mode)
    }

    /// Get permissions from mode
    pub fn permissions(&self) -> Permissions {
        Permissions::from_mode(self.mode)
    }

    /// Convert atime (microseconds since epoch) to SystemTime
    pub fn atime_as_system_time(&self) -> Option<SystemTime> {
        self.atime.map(micros_to_system_time)
    }

    /// Convert mtime (microseconds since epoch) to SystemTime
    pub fn mtime_as_system_time(&self) -> Option<SystemTime> {
        self.mtime.map(micros_to_system_time)
    }
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
    /// Check if this is the "." entry
    pub fn is_dot(&self) -> bool {
        self.name == "."
    }

    /// Check if this is the ".." entry
    pub fn is_dotdot(&self) -> bool {
        self.name == ".."
    }

    /// Check if this should be skipped (. or ..)
    pub fn is_special(&self) -> bool {
        self.is_dot() || self.is_dotdot()
    }

    /// Get file size (0 if stat not available)
    pub fn size(&self) -> u64 {
        self.stat.as_ref().map(|s| s.size).unwrap_or(0)
    }

    /// Get mtime as Unix timestamp
    pub fn mtime(&self) -> Option<i64> {
        self.stat.as_ref().and_then(|s| s.mtime)
    }

    /// Get atime as Unix timestamp
    pub fn atime(&self) -> Option<i64> {
        self.stat.as_ref().and_then(|s| s.atime)
    }

    /// Get ctime as Unix timestamp
    pub fn ctime(&self) -> Option<i64> {
        self.stat.as_ref().and_then(|s| s.ctime)
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

/// Entry for big directory hunting mode
#[derive(Debug, Clone)]
pub struct BigDirEntry {
    /// Full path of the directory
    pub path: String,
    /// Number of files (non-directory entries) in this directory
    pub file_count: u64,
}

/// Statistics collected while walking a directory
#[derive(Debug, Clone, Default)]
pub struct DirStats {
    /// Number of regular files directly in this directory
    pub file_count: u64,

    /// Number of subdirectories directly in this directory
    pub dir_count: u64,

    /// Number of symlinks directly in this directory
    pub symlink_count: u64,

    /// Number of other entry types
    pub other_count: u64,

    /// Total size of files directly in this directory
    pub total_bytes: u64,
}

impl DirStats {
    /// Add an entry to these stats
    pub fn add_entry(&mut self, entry: &NfsDirEntry) {
        match entry.entry_type {
            EntryType::File => {
                self.file_count += 1;
                self.total_bytes += entry.size();
            }
            EntryType::Directory => {
                self.dir_count += 1;
            }
            EntryType::Symlink => {
                self.symlink_count += 1;
            }
            _ => {
                self.other_count += 1;
            }
        }
    }

    /// Total number of entries
    pub fn total_entries(&self) -> u64 {
        self.file_count + self.dir_count + self.symlink_count + self.other_count
    }
}

/// A database entry ready for insertion
#[derive(Debug, Clone, Default)]
pub struct DbEntry {
    /// Parent directory path (None for root, resolved to ID by writer)
    pub parent_path: Option<String>,

    /// Entry name (just the filename, not full path)
    pub name: String,

    /// Full path from mount point
    pub path: String,

    /// Entry type
    pub entry_type: EntryType,

    /// File size in bytes
    pub size: u64,

    /// Last modification time (microseconds since Unix epoch)
    pub mtime: Option<i64>,

    /// Last access time (microseconds since Unix epoch)
    pub atime: Option<i64>,

    /// Status change time (microseconds since Unix epoch)
    pub ctime: Option<i64>,

    /// High-precision modification time: seconds component since Unix epoch.
    /// When `Some`, paired with `mtime_nsec` for nanosecond precision.
    /// When `None`, consumers should fall back to splitting `mtime` (us).
    pub mtime_sec: Option<i64>,

    /// High-precision modification time: nanoseconds component (0..999_999_999).
    /// See `mtime_sec` for pairing semantics.
    pub mtime_nsec: Option<i32>,

    /// High-precision access time: seconds component. Pairs with `atime_nsec`.
    pub atime_sec: Option<i64>,

    /// High-precision access time: nanoseconds component (0..999_999_999).
    pub atime_nsec: Option<i32>,

    /// High-precision change time: seconds component. Pairs with `ctime_nsec`.
    pub ctime_sec: Option<i64>,

    /// High-precision change time: nanoseconds component (0..999_999_999).
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

    /// gxhash checksum (hex-encoded, 32 chars for 128-bit hash)
    pub checksum: Option<String>,

    /// Detected MIME type from magic bytes
    pub file_type: Option<String>,
}

impl DbEntry {
    /// Create a DbEntry from an NfsDirEntry
    pub fn from_nfs_entry(
        dir_entry: &NfsDirEntry,
        parent_path: &str,
        depth: u32,
    ) -> Self {
        let path = if parent_path == "/" {
            format!("/{}", dir_entry.name)
        } else {
            format!("{}/{}", parent_path, dir_entry.name)
        };

        // Store parent_path for later resolution to parent_id
        let parent = if parent_path == "/" || parent_path.is_empty() {
            Some("/".to_string())
        } else {
            Some(parent_path.to_string())
        };

        // Extract extension from filename (for files only)
        let extension = if dir_entry.entry_type == EntryType::File {
            dir_entry.name.rsplit('.').next()
                .filter(|ext| ext.len() < 10 && !ext.contains('/'))
                .map(|s| s.to_lowercase())
        } else {
            None
        };

        Self {
            parent_path: parent,
            name: dir_entry.name.clone(),
            path,
            entry_type: dir_entry.entry_type,
            size: dir_entry.size(),
            mtime: dir_entry.mtime(),
            atime: dir_entry.atime(),
            ctime: dir_entry.ctime(),
            mtime_sec: dir_entry.mtime_sec(),
            mtime_nsec: dir_entry.mtime_nsec(),
            atime_sec: dir_entry.atime_sec(),
            atime_nsec: dir_entry.atime_nsec(),
            ctime_sec: dir_entry.ctime_sec(),
            ctime_nsec: dir_entry.ctime_nsec(),
            mode: dir_entry.mode(),
            uid: dir_entry.uid(),
            gid: dir_entry.gid(),
            nlink: dir_entry.nlink(),
            inode: dir_entry.inode,
            depth,
            extension,
            blocks: dir_entry.blocks(),
            checksum: None,
            file_type: None,
        }
    }

    /// Create a DbEntry from an NfsStat (used for parallel GETATTR)
    pub fn from_stat(
        full_path: &str,
        name: &str,
        stat: &NfsStat,
        depth: u32,
    ) -> Self {
        // Compute parent path from full path
        let parent = if depth == 0 || full_path == "/" {
            None
        } else if let Some(pos) = full_path.rfind('/') {
            if pos == 0 {
                Some("/".to_string())
            } else {
                Some(full_path[..pos].to_string())
            }
        } else {
            Some("/".to_string())
        };

        let entry_type = stat.entry_type();

        // Extract extension from filename (for files only)
        let extension = if entry_type == EntryType::File {
            name.rsplit('.').next()
                .filter(|ext| ext.len() < 10 && !ext.contains('/'))
                .map(|s| s.to_lowercase())
        } else {
            None
        };

        Self {
            parent_path: parent,
            name: name.to_string(),
            path: full_path.to_string(),
            entry_type,
            size: stat.size,
            mtime: stat.mtime,
            atime: stat.atime,
            ctime: stat.ctime,
            mtime_sec: stat.mtime_sec,
            mtime_nsec: stat.mtime_nsec,
            atime_sec: stat.atime_sec,
            atime_nsec: stat.atime_nsec,
            ctime_sec: stat.ctime_sec,
            ctime_nsec: stat.ctime_nsec,
            mode: Some(stat.mode),
            uid: Some(stat.uid),
            gid: Some(stat.gid),
            nlink: Some(stat.nlink),
            inode: stat.inode,
            depth,
            extension,
            blocks: stat.blocks,
            checksum: None,
            file_type: None,
        }
    }

    /// Create a root entry
    pub fn root(path: &str) -> Self {
        Self {
            parent_path: None, // Root has no parent
            name: path.rsplit('/').next().unwrap_or(path).to_string(),
            path: path.to_string(),
            entry_type: EntryType::Directory,
            size: 0,
            mtime: None,
            atime: None,
            ctime: None,
            mtime_sec: None,
            mtime_nsec: None,
            atime_sec: None,
            atime_nsec: None,
            ctime_sec: None,
            ctime_nsec: None,
            mode: None,
            uid: None,
            gid: None,
            nlink: None,
            inode: 0,
            depth: 0,
            extension: None,
            blocks: 0,
            checksum: None,
            file_type: None,
        }
    }

    /// Get the parent path (computed from this entry's path)
    pub fn compute_parent_path(&self) -> Option<String> {
        if self.depth == 0 || self.path == "/" {
            return None;
        }
        // Find last slash and take everything before it
        if let Some(pos) = self.path.rfind('/') {
            if pos == 0 {
                Some("/".to_string())
            } else {
                Some(self.path[..pos].to_string())
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_type_from_mode() {
        assert_eq!(EntryType::from_mode(0o100644), EntryType::File);
        assert_eq!(EntryType::from_mode(0o040755), EntryType::Directory);
        assert_eq!(EntryType::from_mode(0o120777), EntryType::Symlink);
    }

    #[test]
    fn test_permissions() {
        let perms = Permissions::from_mode(0o100755);
        assert_eq!(perms.bits(), 0o755);
        assert!(perms.owner_read());
        assert!(perms.owner_write());
        assert!(perms.owner_exec());
    }

    #[test]
    fn test_dir_stats() {
        let mut stats = DirStats::default();

        let file_entry = NfsDirEntry {
            name: "test.txt".into(),
            entry_type: EntryType::File,
            stat: Some(NfsStat {
                size: 1024,
                ..Default::default()
            }),
            inode: 1,
            file_handle: None,
        };

        let dir_entry = NfsDirEntry {
            name: "subdir".into(),
            entry_type: EntryType::Directory,
            stat: None,
            inode: 2,
            file_handle: None,
        };

        stats.add_entry(&file_entry);
        stats.add_entry(&dir_entry);

        assert_eq!(stats.file_count, 1);
        assert_eq!(stats.dir_count, 1);
        assert_eq!(stats.total_bytes, 1024);
        assert_eq!(stats.total_entries(), 2);
    }

    #[test]
    fn test_db_entry_path_construction() {
        let nfs_entry = NfsDirEntry {
            name: "file.txt".into(),
            entry_type: EntryType::File,
            stat: None,
            inode: 123,
            file_handle: None,
        };

        let db_entry = DbEntry::from_nfs_entry(&nfs_entry, "/data/subdir", 2);
        assert_eq!(db_entry.path, "/data/subdir/file.txt");
        assert_eq!(db_entry.parent_path, Some("/data/subdir".to_string()));
        assert_eq!(db_entry.depth, 2);

        let root_child = DbEntry::from_nfs_entry(&nfs_entry, "/", 1);
        assert_eq!(root_child.path, "/file.txt");
        assert_eq!(root_child.parent_path, Some("/".to_string()));
    }

    #[test]
    fn test_db_entry_parent_path() {
        let root = DbEntry::root("/export");
        assert_eq!(root.parent_path, None);
        assert_eq!(root.compute_parent_path(), None);

        let nfs_entry = NfsDirEntry {
            name: "subdir".into(),
            entry_type: EntryType::Directory,
            stat: None,
            inode: 1,
            file_handle: None,
        };
        let child = DbEntry::from_nfs_entry(&nfs_entry, "/data", 2);
        assert_eq!(child.path, "/data/subdir");
        assert_eq!(child.compute_parent_path(), Some("/data".to_string()));

        let deep = DbEntry::from_nfs_entry(&nfs_entry, "/a/b/c", 4);
        assert_eq!(deep.compute_parent_path(), Some("/a/b/c".to_string()));
    }
}
