//! RocksDB schema definitions
//!
//! Defines the column families, key encoding, and RocksEntry struct
//! for storing filesystem entries in RocksDB.

use crate::nfs::types::{DbEntry, EntryType};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Column family names
pub const CF_ENTRIES_BY_PATH: &str = "entries_by_path";
pub const CF_ENTRIES_BY_INODE: &str = "entries_by_inode";
pub const CF_METADATA: &str = "metadata";
pub const CF_BIG_DIRS: &str = "big_directories";

/// Metadata keys
pub mod meta_keys {
    pub const SOURCE_URL: &str = "source_url";
    pub const START_TIME: &str = "start_time";
    pub const END_TIME: &str = "end_time";
    pub const STATUS: &str = "status";
    pub const DURATION_SECS: &str = "duration_secs";
    pub const TOTAL_DIRS: &str = "total_dirs";
    pub const TOTAL_FILES: &str = "total_files";
    pub const TOTAL_BYTES: &str = "total_bytes";
    pub const ERROR_COUNT: &str = "error_count";
    pub const WORKER_COUNT: &str = "worker_count";
}

/// Entry stored in RocksDB with bincode serialization
/// Designed for compact storage (~100 bytes/entry)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksEntry {
    pub name: String,
    pub path: String,
    pub entry_type: u8,
    pub size: u64,
    pub mtime: Option<i64>,
    pub atime: Option<i64>,
    pub ctime: Option<i64>,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub nlink: Option<u64>,
    pub inode: u64,
    pub depth: u32,
    pub extension: Option<String>,
    pub blocks: u64,
    /// gxhash checksum (hex-encoded)
    pub checksum: Option<String>,
    /// Detected MIME type from magic bytes
    pub file_type: Option<String>,
}

impl RocksEntry {
    /// Convert from DbEntry
    pub fn from_db_entry(entry: &DbEntry) -> Self {
        Self {
            name: entry.name.clone(),
            path: entry.path.clone(),
            entry_type: entry.entry_type as u8,
            size: entry.size,
            mtime: entry.mtime,
            atime: entry.atime,
            ctime: entry.ctime,
            mode: entry.mode,
            uid: entry.uid,
            gid: entry.gid,
            nlink: entry.nlink,
            inode: entry.inode,
            depth: entry.depth,
            extension: entry.extension.clone(),
            blocks: entry.blocks,
            checksum: entry.checksum.clone(),
            file_type: entry.file_type.clone(),
        }
    }

    /// Convert to DbEntry
    pub fn to_db_entry(&self) -> DbEntry {
        // Compute parent_path from path
        let parent_path = if self.depth == 0 || self.path == "/" {
            None
        } else if let Some(pos) = self.path.rfind('/') {
            if pos == 0 {
                Some("/".to_string())
            } else {
                Some(self.path[..pos].to_string())
            }
        } else {
            Some("/".to_string())
        };

        DbEntry {
            parent_path,
            name: self.name.clone(),
            path: self.path.clone(),
            entry_type: EntryType::from_u8(self.entry_type),
            size: self.size,
            mtime: self.mtime,
            atime: self.atime,
            ctime: self.ctime,
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
            nlink: self.nlink,
            inode: self.inode,
            depth: self.depth,
            extension: self.extension.clone(),
            blocks: self.blocks,
            checksum: self.checksum.clone(),
            file_type: self.file_type.clone(),
        }
    }

    /// Serialize to bytes using bincode
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

/// Encode path as key (UTF-8 bytes)
pub fn encode_path_key(path: &str) -> Vec<u8> {
    path.as_bytes().to_vec()
}

/// Decode path from key
pub fn decode_path_key(key: &[u8]) -> Result<String, std::string::FromUtf8Error> {
    String::from_utf8(key.to_vec())
}

/// Encode inode as key (big-endian u64 for proper ordering)
pub fn encode_inode_key(inode: u64) -> [u8; 8] {
    inode.to_be_bytes()
}

/// Decode inode from key
pub fn decode_inode_key(key: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[..8]);
    u64::from_be_bytes(bytes)
}

/// Get column family options for entries (write-optimized)
fn entries_cf_options() -> Options {
    let mut opts = Options::default();

    // Write buffer: 64MB total (2 buffers x 32MB)
    // Smaller buffers = more frequent flushes = less memory
    // This is better for huge scans (100M+ files)
    opts.set_write_buffer_size(32 * 1024 * 1024);
    opts.set_max_write_buffer_number(2);
    opts.set_min_write_buffer_number_to_merge(1);

    // Level compaction settings
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_max_bytes_for_level_base(256 * 1024 * 1024);

    // Bloom filter for point lookups (10 bits/key)
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_cache_index_and_filter_blocks(true);
    opts.set_block_based_table_factory(&block_opts);

    // Compression: LZ4 for speed
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

    opts
}

/// Get column family options for metadata (small, infrequent writes)
fn metadata_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(4 * 1024 * 1024);
    opts.set_max_write_buffer_number(2);
    opts
}

/// Database configuration for write-optimized scans
pub fn get_db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Increase parallelism
    opts.increase_parallelism(num_cpus::get() as i32);
    opts.set_max_background_jobs(4);

    // Disable WAL for scan workloads (data is repeatable)
    // WAL is disabled per-write via WriteBatch options

    // Allow concurrent memtable writes
    opts.set_allow_concurrent_memtable_write(true);
    opts.set_enable_write_thread_adaptive_yield(true);

    opts
}

/// Open or create a RocksDB database with all column families
pub fn open_rocks_db<P: AsRef<Path>>(path: P) -> Result<DB, rocksdb::Error> {
    let db_opts = get_db_options();

    let cf_descriptors = vec![
        ColumnFamilyDescriptor::new(CF_ENTRIES_BY_PATH, entries_cf_options()),
        ColumnFamilyDescriptor::new(CF_ENTRIES_BY_INODE, entries_cf_options()),
        ColumnFamilyDescriptor::new(CF_METADATA, metadata_cf_options()),
        ColumnFamilyDescriptor::new(CF_BIG_DIRS, metadata_cf_options()),
    ];

    DB::open_cf_descriptors(&db_opts, path, cf_descriptors)
}

/// Open existing RocksDB database for reading
pub fn open_rocks_db_readonly<P: AsRef<Path>>(path: P) -> Result<DB, rocksdb::Error> {
    let db_opts = get_db_options();

    let cf_names = vec![CF_ENTRIES_BY_PATH, CF_ENTRIES_BY_INODE, CF_METADATA, CF_BIG_DIRS];

    DB::open_cf_for_read_only(&db_opts, path, cf_names, false)
}

/// RocksDB handle wrapper with column family accessors
pub struct RocksHandle {
    pub db: DB,
}

impl RocksHandle {
    /// Open or create database
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, rocksdb::Error> {
        let db = open_rocks_db(path)?;
        Ok(Self { db })
    }

    /// Open for read-only access
    pub fn open_readonly<P: AsRef<Path>>(path: P) -> Result<Self, rocksdb::Error> {
        let db = open_rocks_db_readonly(path)?;
        Ok(Self { db })
    }

    /// Get entries_by_path column family
    pub fn cf_entries_by_path(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_ENTRIES_BY_PATH)
            .expect("entries_by_path CF missing")
    }

    /// Get entries_by_inode column family
    pub fn cf_entries_by_inode(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_ENTRIES_BY_INODE)
            .expect("entries_by_inode CF missing")
    }

    /// Get metadata column family
    pub fn cf_metadata(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_METADATA)
            .expect("metadata CF missing")
    }

    /// Get big directories column family
    pub fn cf_big_dirs(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_BIG_DIRS)
            .expect("big_directories CF missing")
    }

    /// Set metadata value
    pub fn set_metadata(&self, key: &str, value: &str) -> Result<(), rocksdb::Error> {
        self.db
            .put_cf(self.cf_metadata(), key.as_bytes(), value.as_bytes())
    }

    /// Get metadata value
    pub fn get_metadata(&self, key: &str) -> Result<Option<String>, rocksdb::Error> {
        match self.db.get_cf(self.cf_metadata(), key.as_bytes())? {
            Some(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).to_string())),
            None => Ok(None),
        }
    }

    /// Put entry (writes to both column families)
    pub fn put_entry(&self, entry: &RocksEntry) -> Result<(), crate::error::RocksError> {
        let value = entry
            .to_bytes()
            .map_err(|e| crate::error::RocksError::Bincode(e.to_string()))?;
        let path_key = encode_path_key(&entry.path);
        let inode_key = encode_inode_key(entry.inode);

        let mut batch = WriteBatch::default();
        batch.put_cf(self.cf_entries_by_path(), &path_key, &value);
        batch.put_cf(self.cf_entries_by_inode(), &inode_key, &value);

        self.db
            .write(batch)
            .map_err(crate::error::RocksError::Rocks)
    }

    /// Get entry by path
    pub fn get_by_path(&self, path: &str) -> Result<Option<RocksEntry>, crate::error::RocksError> {
        let key = encode_path_key(path);
        match self
            .db
            .get_cf(self.cf_entries_by_path(), &key)
            .map_err(crate::error::RocksError::Rocks)?
        {
            Some(bytes) => {
                let entry = RocksEntry::from_bytes(&bytes)
                    .map_err(|e| crate::error::RocksError::Bincode(e.to_string()))?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Get entry by inode
    pub fn get_by_inode(&self, inode: u64) -> Result<Option<RocksEntry>, crate::error::RocksError> {
        let key = encode_inode_key(inode);
        match self
            .db
            .get_cf(self.cf_entries_by_inode(), &key)
            .map_err(crate::error::RocksError::Rocks)?
        {
            Some(bytes) => {
                let entry = RocksEntry::from_bytes(&bytes)
                    .map_err(|e| crate::error::RocksError::Bincode(e.to_string()))?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Iterate all entries by path
    pub fn iter_by_path(
        &self,
    ) -> impl Iterator<Item = Result<RocksEntry, crate::error::RocksError>> + '_ {
        self.db
            .iterator_cf(self.cf_entries_by_path(), rocksdb::IteratorMode::Start)
            .map(|result| {
                let (_, value) = result.map_err(crate::error::RocksError::Rocks)?;
                RocksEntry::from_bytes(&value)
                    .map_err(|e| crate::error::RocksError::Bincode(e.to_string()))
            })
    }

    /// Count entries (by iterating - O(n))
    pub fn count_entries(&self) -> Result<u64, rocksdb::Error> {
        let mut count = 0u64;
        let iter = self
            .db
            .iterator_cf(self.cf_entries_by_path(), rocksdb::IteratorMode::Start);
        for _ in iter {
            count += 1;
        }
        Ok(count)
    }

    /// Put a big directory entry (path -> file_count)
    pub fn put_big_dir(&self, path: &str, file_count: u64) -> Result<(), rocksdb::Error> {
        let key = encode_path_key(path);
        let value = file_count.to_be_bytes();
        self.db.put_cf(self.cf_big_dirs(), &key, &value)
    }

    /// Iterate all big directories
    pub fn iter_big_dirs(&self) -> impl Iterator<Item = Result<(String, u64), crate::error::RocksError>> + '_ {
        self.db
            .iterator_cf(self.cf_big_dirs(), rocksdb::IteratorMode::Start)
            .map(|result| {
                let (key, value) = result.map_err(crate::error::RocksError::Rocks)?;
                let path = decode_path_key(&key)
                    .map_err(|e| crate::error::RocksError::Bincode(e.to_string()))?;
                let file_count = if value.len() == 8 {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&value);
                    u64::from_be_bytes(bytes)
                } else {
                    0
                };
                Ok((path, file_count))
            })
    }

    /// Count big directories (by iterating - O(n))
    pub fn count_big_dirs(&self) -> Result<u64, rocksdb::Error> {
        let mut count = 0u64;
        let iter = self
            .db
            .iterator_cf(self.cf_big_dirs(), rocksdb::IteratorMode::Start);
        for _ in iter {
            count += 1;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_key_encoding() {
        let path = "/data/subdir/file.txt";
        let key = encode_path_key(path);
        let decoded = decode_path_key(&key).unwrap();
        assert_eq!(path, decoded);
    }

    #[test]
    fn test_inode_key_encoding() {
        let inode = 12345678901234u64;
        let key = encode_inode_key(inode);
        let decoded = decode_inode_key(&key);
        assert_eq!(inode, decoded);
    }

    #[test]
    fn test_rocks_entry_serialization() {
        let entry = RocksEntry {
            name: "test.txt".to_string(),
            path: "/data/test.txt".to_string(),
            entry_type: 0,
            size: 1024,
            mtime: Some(1234567890),
            atime: None,
            ctime: Some(1234567890),
            mode: Some(0o644),
            uid: Some(1000),
            gid: Some(1000),
            nlink: Some(1),
            inode: 123456,
            depth: 2,
            extension: Some("txt".to_string()),
            blocks: 8,
            checksum: Some("0123456789abcdef0123456789abcdef".to_string()),
            file_type: Some("text/plain".to_string()),
        };

        let bytes = entry.to_bytes().unwrap();
        let decoded = RocksEntry::from_bytes(&bytes).unwrap();

        assert_eq!(entry.name, decoded.name);
        assert_eq!(entry.path, decoded.path);
        assert_eq!(entry.inode, decoded.inode);
        assert_eq!(entry.extension, decoded.extension);
        assert_eq!(entry.blocks, decoded.blocks);
    }
}
