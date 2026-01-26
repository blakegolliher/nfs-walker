//! RocksDB baseline reader for incremental scans
//!
//! Provides O(log n) lookups by path and inode for comparing
//! current scan results against a previous baseline.

use crate::error::RocksError;
use crate::rocksdb::schema::{encode_inode_key, encode_path_key, RocksEntry, RocksHandle};
use std::path::Path;

/// Baseline reader for incremental scan comparisons
///
/// Provides efficient lookups to determine:
/// - New entries (not in baseline)
/// - Modified entries (mtime/size changed)
/// - Renamed entries (same inode, different path)
/// - Deleted entries (in baseline but not in current scan)
pub struct RocksBaseline {
    handle: RocksHandle,
}

impl RocksBaseline {
    /// Open an existing RocksDB as a read-only baseline
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, RocksError> {
        let handle = RocksHandle::open_readonly(path).map_err(RocksError::Rocks)?;
        Ok(Self { handle })
    }

    /// Get entry by path - O(log n) lookup
    pub fn get_by_path(&self, path: &str) -> Result<Option<RocksEntry>, RocksError> {
        self.handle.get_by_path(path)
    }

    /// Get entry by inode - O(log n) lookup for rename detection
    pub fn get_by_inode(&self, inode: u64) -> Result<Option<RocksEntry>, RocksError> {
        self.handle.get_by_inode(inode)
    }

    /// Check if path exists in baseline
    pub fn contains_path(&self, path: &str) -> Result<bool, RocksError> {
        let key = encode_path_key(path);
        self.handle
            .db
            .get_cf(self.handle.cf_entries_by_path(), &key)
            .map(|v| v.is_some())
            .map_err(RocksError::Rocks)
    }

    /// Check if inode exists in baseline
    pub fn contains_inode(&self, inode: u64) -> Result<bool, RocksError> {
        let key = encode_inode_key(inode);
        self.handle
            .db
            .get_cf(self.handle.cf_entries_by_inode(), &key)
            .map(|v| v.is_some())
            .map_err(RocksError::Rocks)
    }

    /// Iterate all paths in baseline
    /// Returns (path, entry) pairs in lexicographic order
    pub fn iter_paths(
        &self,
    ) -> impl Iterator<Item = Result<(String, RocksEntry), RocksError>> + '_ {
        use rocksdb::IteratorMode;

        self.handle
            .db
            .iterator_cf(self.handle.cf_entries_by_path(), IteratorMode::Start)
            .map(|result| {
                let (key, value) = result.map_err(RocksError::Rocks)?;
                let path = String::from_utf8(key.to_vec())
                    .map_err(|e| RocksError::Bincode(e.to_string()))?;
                let entry = RocksEntry::from_bytes(&value)
                    .map_err(|e| RocksError::Bincode(e.to_string()))?;
                Ok((path, entry))
            })
    }

    /// Iterate paths with a given prefix
    pub fn iter_paths_with_prefix(
        &self,
        prefix: &str,
    ) -> impl Iterator<Item = Result<(String, RocksEntry), RocksError>> + '_ {
        use rocksdb::IteratorMode;

        let prefix_bytes = prefix.as_bytes().to_vec();

        self.handle
            .db
            .iterator_cf(
                self.handle.cf_entries_by_path(),
                IteratorMode::From(&prefix_bytes, rocksdb::Direction::Forward),
            )
            .take_while(move |result| {
                match result {
                    Ok((key, _)) => key.starts_with(&prefix_bytes),
                    Err(_) => true, // Continue to propagate errors
                }
            })
            .map(|result| {
                let (key, value) = result.map_err(RocksError::Rocks)?;
                let path = String::from_utf8(key.to_vec())
                    .map_err(|e| RocksError::Bincode(e.to_string()))?;
                let entry = RocksEntry::from_bytes(&value)
                    .map_err(|e| RocksError::Bincode(e.to_string()))?;
                Ok((path, entry))
            })
    }

    /// Get metadata value
    pub fn get_metadata(&self, key: &str) -> Result<Option<String>, RocksError> {
        self.handle.get_metadata(key).map_err(RocksError::Rocks)
    }

    /// Get approximate entry count (fast, but not exact)
    pub fn approx_entry_count(&self) -> u64 {
        self.handle
            .db
            .property_int_value_cf(
                self.handle.cf_entries_by_path(),
                "rocksdb.estimate-num-keys",
            )
            .unwrap_or(None)
            .unwrap_or(0)
    }
}

/// Result of comparing a current entry against the baseline
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeType {
    /// Entry is new (not in baseline)
    New,
    /// Entry was modified (mtime or size changed)
    Modified,
    /// Entry was renamed (same inode, different path)
    Renamed { old_path: String },
    /// Entry is unchanged
    Unchanged,
}

/// Compare a current entry against the baseline
pub fn compare_entry(
    baseline: &RocksBaseline,
    path: &str,
    inode: u64,
    mtime: Option<i64>,
    size: u64,
) -> Result<ChangeType, RocksError> {
    // First check by path
    if let Some(baseline_entry) = baseline.get_by_path(path)? {
        // Path exists - check if modified
        if baseline_entry.inode != inode {
            // Different inode at same path - replaced file
            return Ok(ChangeType::Modified);
        }
        if baseline_entry.mtime != mtime || baseline_entry.size != size {
            return Ok(ChangeType::Modified);
        }
        return Ok(ChangeType::Unchanged);
    }

    // Path not found - check if renamed by looking up inode
    if let Some(baseline_entry) = baseline.get_by_inode(inode)? {
        // Same inode exists at different path - renamed
        return Ok(ChangeType::Renamed {
            old_path: baseline_entry.path,
        });
    }

    // Not found anywhere - new entry
    Ok(ChangeType::New)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::{DbEntry, EntryType};
    use crate::rocksdb::writer::{RocksWriter, RocksWriterConfig};
    use tempfile::tempdir;

    fn create_test_baseline() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("baseline.rocks");

        let config = RocksWriterConfig::default();
        let writer = RocksWriter::open(&db_path, config).unwrap();

        let entries = vec![
            DbEntry {
                parent_path: Some("/".to_string()),
                name: "file1.txt".to_string(),
                path: "/file1.txt".to_string(),
                entry_type: EntryType::File,
                size: 1024,
                mtime: Some(1000),
                atime: None,
                ctime: None,
                mode: Some(0o644),
                uid: Some(1000),
                gid: Some(1000),
                nlink: Some(1),
                inode: 100,
                depth: 1,
                extension: Some("txt".to_string()),
                blocks: 8,
            },
            DbEntry {
                parent_path: Some("/".to_string()),
                name: "file2.txt".to_string(),
                path: "/file2.txt".to_string(),
                entry_type: EntryType::File,
                size: 2048,
                mtime: Some(2000),
                atime: None,
                ctime: None,
                mode: Some(0o644),
                uid: Some(1000),
                gid: Some(1000),
                nlink: Some(1),
                inode: 200,
                depth: 1,
                extension: Some("txt".to_string()),
                blocks: 16,
            },
        ];

        writer.write_batch(&entries).unwrap();
        writer.flush().unwrap();

        (dir, db_path)
    }

    #[test]
    fn test_baseline_lookup_by_path() {
        let (_dir, db_path) = create_test_baseline();
        let baseline = RocksBaseline::open(&db_path).unwrap();

        let entry = baseline.get_by_path("/file1.txt").unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().size, 1024);

        let missing = baseline.get_by_path("/nonexistent.txt").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_baseline_lookup_by_inode() {
        let (_dir, db_path) = create_test_baseline();
        let baseline = RocksBaseline::open(&db_path).unwrap();

        let entry = baseline.get_by_inode(100).unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().path, "/file1.txt");

        let missing = baseline.get_by_inode(999).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_compare_entry_unchanged() {
        let (_dir, db_path) = create_test_baseline();
        let baseline = RocksBaseline::open(&db_path).unwrap();

        let change = compare_entry(&baseline, "/file1.txt", 100, Some(1000), 1024).unwrap();
        assert_eq!(change, ChangeType::Unchanged);
    }

    #[test]
    fn test_compare_entry_modified() {
        let (_dir, db_path) = create_test_baseline();
        let baseline = RocksBaseline::open(&db_path).unwrap();

        // Different mtime
        let change = compare_entry(&baseline, "/file1.txt", 100, Some(2000), 1024).unwrap();
        assert_eq!(change, ChangeType::Modified);

        // Different size
        let change = compare_entry(&baseline, "/file1.txt", 100, Some(1000), 2048).unwrap();
        assert_eq!(change, ChangeType::Modified);
    }

    #[test]
    fn test_compare_entry_new() {
        let (_dir, db_path) = create_test_baseline();
        let baseline = RocksBaseline::open(&db_path).unwrap();

        let change = compare_entry(&baseline, "/new_file.txt", 300, Some(3000), 512).unwrap();
        assert_eq!(change, ChangeType::New);
    }

    #[test]
    fn test_compare_entry_renamed() {
        let (_dir, db_path) = create_test_baseline();
        let baseline = RocksBaseline::open(&db_path).unwrap();

        // Same inode (100), different path
        let change = compare_entry(&baseline, "/renamed_file.txt", 100, Some(1000), 1024).unwrap();
        assert_eq!(
            change,
            ChangeType::Renamed {
                old_path: "/file1.txt".to_string()
            }
        );
    }
}
