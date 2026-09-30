//! NFS entry types and data structures
//!
//! These types represent filesystem entries returned from NFS operations
//! and are shaped for bulk transfer into the sharded Parquet writers.

use std::borrow::Cow;

/// Type of filesystem entry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
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

    /// Classify an NFSv3 `ftype3` value. Anything outside the seven
    /// protocol types is `Unknown`, which is never written to a scan.
    pub fn from_nfs3(ftype: u32) -> Self {
        match ftype {
            1 => EntryType::File,        // NF3REG
            2 => EntryType::Directory,   // NF3DIR
            3 => EntryType::BlockDevice, // NF3BLK
            4 => EntryType::CharDevice,  // NF3CHR
            5 => EntryType::Symlink,     // NF3LNK
            6 => EntryType::Socket,      // NF3SOCK
            7 => EntryType::Fifo,        // NF3FIFO
            _ => EntryType::Unknown,
        }
    }

    /// The stable `file_type` string of the Parquet output, and the
    /// only place a type is turned into one. Consumers (the mongoose
    /// rewrite, the dashboard queries) match on these exact values.
    ///
    /// `Unknown` has no name: a scan never contains an entry whose
    /// type could not be established, so the builder rejects it
    /// rather than write a row that claims to describe something.
    pub fn file_type_name(self) -> Option<&'static str> {
        match self {
            EntryType::File => Some("file"),
            EntryType::Directory => Some("directory"),
            EntryType::Symlink => Some("symlink"),
            EntryType::BlockDevice => Some("block_device"),
            EntryType::CharDevice => Some("char_device"),
            EntryType::Fifo => Some("fifo"),
            EntryType::Socket => Some("socket"),
            EntryType::Unknown => None,
        }
    }
}

/// Extract a lowercase extension from a UTF-8 filename.
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

/// Extract a lowercase extension only when the complete filename is UTF-8.
///
/// `extension` is an analytics-only UTF-8 column. The authoritative filename
/// bytes are kept separately, so a non-UTF-8 name must never be made lossy just
/// to populate this optional value.
pub fn extract_extension_bytes(name: &[u8]) -> Option<String> {
    std::str::from_utf8(name).ok().and_then(extract_extension)
}

/// Render path bytes for logs and the legacy UTF-8 analytics columns.
///
/// Valid UTF-8 is unchanged. Invalid paths use a diagnostic byte escape: a
/// literal backslash becomes `\\`, and non-printable/non-ASCII bytes become
/// `\xNN`. Migration code must use the Binary columns instead of this display
/// representation.
pub fn display_path(path: &[u8]) -> Cow<'_, str> {
    if let Ok(path) = std::str::from_utf8(path) {
        return Cow::Borrowed(path);
    }

    let mut display = String::with_capacity(path.len());
    for &byte in path {
        match byte {
            b'\\' => display.push_str("\\\\"),
            0x20..=0x7e => display.push(char::from(byte)),
            _ => {
                use std::fmt::Write as _;
                let _ = write!(display, "\\x{byte:02x}");
            }
        }
    }
    Cow::Owned(display)
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

    /// Filesystem identifier. Inodes are unique only within this scope.
    pub fsid: u64,

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

/// An object's type and attributes as one NFS reply reported them:
/// READDIRPLUS entry attributes, a GETATTR, or a LOOKUP's
/// `obj_attributes`.
#[derive(Debug, Clone)]
pub struct EntryAttrs {
    pub entry_type: EntryType,
    /// The raw `ftype3` value, kept for the error message when it is
    /// not one of the seven protocol types.
    pub nfs_type: u32,
    pub stat: NfsStat,
}

/// What a LOOKUP of one name in a directory returned.
#[derive(Debug, Clone)]
pub struct LookupReply {
    pub file_handle: Vec<u8>,
    /// `obj_attributes` is optional in the protocol.
    pub attrs: Option<EntryAttrs>,
}

/// A directory entry returned from readdir operations
#[derive(Debug, Clone)]
pub struct NfsDirEntry {
    /// Entry name bytes (not full path). NFS/POSIX names are not required to
    /// be UTF-8.
    pub name: Vec<u8>,

    /// Entry type
    pub entry_type: EntryType,

    /// File statistics. READDIRPLUS may omit an entry's attributes;
    /// the walker then fetches them (see `walker::resolve`) before
    /// the entry is classified, emitted, or descended into.
    pub stat: Option<NfsStat>,

    /// Inode number (always available)
    pub inode: u64,

    /// File handle from READDIRPLUS name_handle (for directories)
    /// Used to skip LOOKUP RPCs when opening subdirectories
    pub file_handle: Option<Vec<u8>>,
}

impl NfsDirEntry {
    /// Replace everything READDIRPLUS would have supplied with
    /// attributes fetched afterwards. The identity (`inode`, and the
    /// `fsid` inside `stat`) comes from those attributes too, so the
    /// row describes one object: the one the name refers to now. That
    /// matters when the name is a mountpoint, where the listing carries
    /// the mounted-on directory's number and the attributes the root of
    /// the mounted filesystem.
    pub fn apply_attrs(&mut self, attrs: EntryAttrs, file_handle: Option<Vec<u8>>) {
        self.entry_type = attrs.entry_type;
        self.inode = attrs.stat.inode;
        self.stat = Some(attrs.stat);
        if file_handle.is_some() {
            self.file_handle = file_handle;
        }
    }

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

    /// Get the filesystem identifier used with inode for hardlink identity.
    pub fn fsid(&self) -> Option<u64> {
        self.stat.as_ref().map(|s| s.fsid)
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
    /// the parent bytes per entry.
    pub parent_path: Option<Vec<u8>>,

    /// Entry name (just the filename, not full path)
    pub name: Vec<u8>,

    /// Full path from mount point
    pub path: Vec<u8>,

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

    /// Filesystem identifier paired with `inode` for hardlink identity.
    pub fsid: Option<u64>,

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
    fn byte_extension_requires_utf8_without_losing_name_bytes() {
        assert_eq!(extract_extension_bytes(b"file.TXT"), Some("txt".into()));
        assert_eq!(extract_extension_bytes(b"bad-\xff.txt"), None);
    }

    #[test]
    fn display_path_is_stable_and_unambiguous_for_invalid_utf8() {
        assert_eq!(display_path("/d/é.txt".as_bytes()), "/d/é.txt");
        assert_eq!(display_path(b"/d/bad-\xff\\name"), r"/d/bad-\xff\\name");
    }

    /// The exact `file_type` strings of the Parquet output. The mongoose
    /// rewrite translates these seven and rejects everything else.
    #[test]
    fn file_type_name_is_exact_for_all_seven_types() {
        let table = [
            (EntryType::File, "file"),
            (EntryType::Directory, "directory"),
            (EntryType::Symlink, "symlink"),
            (EntryType::BlockDevice, "block_device"),
            (EntryType::CharDevice, "char_device"),
            (EntryType::Fifo, "fifo"),
            (EntryType::Socket, "socket"),
        ];
        for (entry_type, name) in table {
            assert_eq!(entry_type.file_type_name(), Some(name), "{entry_type:?}");
        }
        let names: std::collections::BTreeSet<_> = table.iter().map(|(_, n)| *n).collect();
        assert_eq!(names.len(), 7, "every type has its own name");
    }

    #[test]
    fn unknown_type_has_no_file_type_name() {
        assert_eq!(EntryType::Unknown.file_type_name(), None);
    }

    #[test]
    fn from_nfs3_maps_the_seven_protocol_types_and_nothing_else() {
        assert_eq!(EntryType::from_nfs3(1), EntryType::File);
        assert_eq!(EntryType::from_nfs3(2), EntryType::Directory);
        assert_eq!(EntryType::from_nfs3(3), EntryType::BlockDevice);
        assert_eq!(EntryType::from_nfs3(4), EntryType::CharDevice);
        assert_eq!(EntryType::from_nfs3(5), EntryType::Symlink);
        assert_eq!(EntryType::from_nfs3(6), EntryType::Socket);
        assert_eq!(EntryType::from_nfs3(7), EntryType::Fifo);
        for other in [0, 8, 255, u32::MAX] {
            assert_eq!(EntryType::from_nfs3(other), EntryType::Unknown, "{other}");
        }
    }

    #[test]
    fn apply_attrs_takes_identity_from_the_resolved_object() {
        let mut entry = NfsDirEntry {
            name: b"mnt".to_vec(),
            entry_type: EntryType::Unknown,
            stat: None,
            inode: 11, // the mounted-on directory, as listed
            file_handle: None,
        };
        entry.apply_attrs(
            EntryAttrs {
                entry_type: EntryType::Directory,
                nfs_type: 2,
                stat: NfsStat {
                    inode: 2, // the root of the mounted filesystem
                    fsid: 99,
                    mode: 0o755,
                    ..NfsStat::default()
                },
            },
            Some(vec![7, 7]),
        );
        assert_eq!(entry.entry_type, EntryType::Directory);
        assert_eq!(entry.inode, 2);
        assert_eq!(entry.fsid(), Some(99));
        assert_eq!(entry.mode(), Some(0o755));
        assert_eq!(entry.file_handle.as_deref(), Some(&[7u8, 7][..]));
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
