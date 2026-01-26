//! RocksDB writer thread
//!
//! Handles all RocksDB writes from a dedicated thread, mirroring
//! the SQLite writer pattern for consistency.

use crate::error::RocksError;
use crate::nfs::types::DbEntry;
use crate::rocksdb::schema::{
    encode_inode_key, encode_path_key, meta_keys, RocksEntry, RocksHandle,
};
use crossbeam_channel::Receiver;
use rocksdb::{WriteBatch, WriteOptions};
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tracing::{debug, info};

/// Configuration for the RocksDB writer
pub struct RocksWriterConfig {
    /// Batch size before flushing
    pub batch_size: usize,
    /// Disable WAL for better write performance (data is repeatable)
    pub disable_wal: bool,
}

impl Default for RocksWriterConfig {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            disable_wal: true,
        }
    }
}

/// RocksDB writer handle
pub struct RocksWriter {
    handle: RocksHandle,
    config: RocksWriterConfig,
}

impl RocksWriter {
    /// Open or create RocksDB for writing
    pub fn open<P: AsRef<Path>>(path: P, config: RocksWriterConfig) -> Result<Self, RocksError> {
        // Remove existing database if present (fresh scan)
        let path_ref = path.as_ref();
        if path_ref.exists() {
            std::fs::remove_dir_all(path_ref)
                .map_err(|e| RocksError::Io(e.to_string()))?;
        }

        let handle = RocksHandle::open(path).map_err(RocksError::Rocks)?;
        Ok(Self { handle, config })
    }

    /// Get reference to the underlying handle
    pub fn handle(&self) -> &RocksHandle {
        &self.handle
    }

    /// Set walk metadata
    pub fn set_metadata(&self, key: &str, value: &str) -> Result<(), RocksError> {
        self.handle.set_metadata(key, value).map_err(RocksError::Rocks)
    }

    /// Write a batch of entries
    pub fn write_batch(&self, entries: &[DbEntry]) -> Result<(), RocksError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut batch = WriteBatch::default();
        let cf_path = self.handle.cf_entries_by_path();
        let cf_inode = self.handle.cf_entries_by_inode();

        for entry in entries {
            let rocks_entry = RocksEntry::from_db_entry(entry);
            let value = rocks_entry.to_bytes()
                .map_err(|e| RocksError::Bincode(e.to_string()))?;

            let path_key = encode_path_key(&entry.path);
            let inode_key = encode_inode_key(entry.inode);

            batch.put_cf(cf_path, &path_key, &value);
            batch.put_cf(cf_inode, &inode_key, &value);
        }

        // Write with options
        let mut write_opts = WriteOptions::default();
        if self.config.disable_wal {
            write_opts.disable_wal(true);
        }

        self.handle.db.write_opt(batch, &write_opts).map_err(RocksError::Rocks)
    }

    /// Flush memtables to disk
    pub fn flush(&self) -> Result<(), RocksError> {
        self.handle.db.flush().map_err(RocksError::Rocks)
    }

    /// Compact the database (optional, for smaller final size)
    pub fn compact(&self) -> Result<(), RocksError> {
        self.handle.db.compact_range::<&[u8], &[u8]>(None, None);
        Ok(())
    }

    /// Get the underlying RocksHandle (consumes self)
    pub fn into_handle(self) -> RocksHandle {
        self.handle
    }
}

/// Spawn RocksDB writer thread
pub fn spawn_rocks_writer<P: AsRef<Path> + Send + 'static>(
    path: P,
    entry_rx: Receiver<Vec<DbEntry>>,
    config: RocksWriterConfig,
) -> JoinHandle<Result<RocksHandle, RocksError>> {
    thread::Builder::new()
        .name("rocks-writer".to_string())
        .spawn(move || rocks_writer_loop(path, entry_rx, config))
        .expect("Failed to spawn RocksDB writer thread")
}

/// RocksDB writer thread main loop
fn rocks_writer_loop<P: AsRef<Path>>(
    path: P,
    entry_rx: Receiver<Vec<DbEntry>>,
    config: RocksWriterConfig,
) -> Result<RocksHandle, RocksError> {
    debug!("RocksDB writer thread started");

    let batch_size = config.batch_size;
    let writer = RocksWriter::open(path, config)?;

    let mut total_written = 0u64;
    let mut pending: Vec<DbEntry> = Vec::with_capacity(batch_size * 2);

    // Receive batches and write to DB
    while let Ok(entries) = entry_rx.recv() {
        pending.extend(entries);

        // Write when we have enough
        if pending.len() >= batch_size {
            writer.write_batch(&pending)?;
            total_written += pending.len() as u64;
            pending.clear();
        }
    }

    // Write remaining entries
    if !pending.is_empty() {
        writer.write_batch(&pending)?;
        total_written += pending.len() as u64;
    }

    // Final flush
    writer.flush()?;

    info!("RocksDB writer thread finished, wrote {} entries", total_written);
    Ok(writer.into_handle())
}

/// Finalize RocksDB after walk completion
pub fn finalize_rocks_db(
    handle: &RocksHandle,
    duration: Duration,
    completed: bool,
    stats: &WalkStatsSnapshot,
) -> Result<(), RocksError> {
    use chrono::Utc;

    handle.set_metadata(meta_keys::END_TIME, &Utc::now().to_rfc3339())
        .map_err(RocksError::Rocks)?;
    handle.set_metadata(meta_keys::DURATION_SECS, &duration.as_secs().to_string())
        .map_err(RocksError::Rocks)?;
    handle.set_metadata(meta_keys::TOTAL_DIRS, &stats.dirs.to_string())
        .map_err(RocksError::Rocks)?;
    handle.set_metadata(meta_keys::TOTAL_FILES, &stats.files.to_string())
        .map_err(RocksError::Rocks)?;
    handle.set_metadata(meta_keys::TOTAL_BYTES, &stats.bytes.to_string())
        .map_err(RocksError::Rocks)?;
    handle.set_metadata(meta_keys::ERROR_COUNT, &stats.errors.to_string())
        .map_err(RocksError::Rocks)?;
    handle.set_metadata(meta_keys::STATUS, if completed { "completed" } else { "interrupted" })
        .map_err(RocksError::Rocks)?;

    Ok(())
}

/// Snapshot of walk statistics for finalization
#[derive(Debug, Clone, Default)]
pub struct WalkStatsSnapshot {
    pub dirs: u64,
    pub files: u64,
    pub bytes: u64,
    pub errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::EntryType;
    use tempfile::tempdir;

    #[test]
    fn test_rocks_writer_basic() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.rocks");

        let config = RocksWriterConfig::default();
        let writer = RocksWriter::open(&db_path, config).unwrap();

        let entries = vec![
            DbEntry {
                parent_path: Some("/".to_string()),
                name: "test.txt".to_string(),
                path: "/test.txt".to_string(),
                entry_type: EntryType::File,
                size: 1024,
                mtime: Some(1234567890),
                atime: None,
                ctime: None,
                mode: Some(0o644),
                uid: Some(1000),
                gid: Some(1000),
                nlink: Some(1),
                inode: 123,
                depth: 1,
                extension: Some("txt".to_string()),
                blocks: 8,
            },
        ];

        writer.write_batch(&entries).unwrap();
        writer.flush().unwrap();

        // Verify entry was written
        let entry = writer.handle().get_by_path("/test.txt").unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().name, "test.txt");
    }
}
