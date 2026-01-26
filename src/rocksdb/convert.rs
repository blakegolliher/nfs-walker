//! RocksDB to SQLite conversion
//!
//! Converts a RocksDB scan database to SQLite for queries and analysis.

use crate::db::schema::{create_database, create_indexes, keys, optimize_for_reads, set_walk_info};
use crate::error::{DbError, WalkerError};
use crate::rocksdb::schema::{meta_keys, RocksHandle};
use rusqlite::{params, Connection};
use std::path::Path;
use tracing::info;

/// Configuration for conversion
pub struct ConvertConfig {
    /// SQLite batch size for inserts
    pub batch_size: usize,
    /// Show progress callback
    pub progress: bool,
}

impl Default for ConvertConfig {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            progress: true,
        }
    }
}

/// Progress callback type
pub type ProgressCallback = Box<dyn Fn(u64, u64) + Send>;

/// Convert RocksDB database to SQLite
pub fn convert_rocks_to_sqlite<P1, P2>(
    rocks_path: P1,
    sqlite_path: P2,
    config: ConvertConfig,
    progress_callback: Option<ProgressCallback>,
) -> Result<ConvertStats, WalkerError>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let rocks_path = rocks_path.as_ref();
    let sqlite_path = sqlite_path.as_ref();

    info!("Opening RocksDB: {}", rocks_path.display());
    let rocks = RocksHandle::open_readonly(rocks_path)
        .map_err(|e| WalkerError::Database(DbError::Schema(format!("Failed to open RocksDB: {}", e))))?;

    // Get approximate entry count for progress
    let approx_count = rocks
        .db
        .property_int_value_cf(
            rocks.cf_entries_by_path(),
            "rocksdb.estimate-num-keys",
        )
        .unwrap_or(None)
        .unwrap_or(0);

    info!("Approximate entries to convert: {}", approx_count);

    // Remove existing SQLite file
    if sqlite_path.exists() {
        std::fs::remove_file(sqlite_path).map_err(WalkerError::Io)?;
    }

    info!("Creating SQLite database: {}", sqlite_path.display());
    let mut conn = Connection::open(sqlite_path)
        .map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

    // Create schema
    create_database(&conn).map_err(WalkerError::Database)?;

    // Copy metadata from RocksDB to SQLite
    copy_metadata(&rocks, &conn)?;

    // Optimize for bulk loading
    conn.execute_batch(
        "PRAGMA synchronous = OFF;
         PRAGMA journal_mode = OFF;
         PRAGMA cache_size = -64000;
         PRAGMA temp_store = MEMORY;"
    ).map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

    // Convert entries
    let mut stats = convert_entries(&rocks, &mut conn, config.batch_size, progress_callback)?;

    // Convert big directories if present
    let big_dirs_count = convert_big_dirs(&rocks, &mut conn)?;
    stats.big_dirs_converted = big_dirs_count;

    if big_dirs_count > 0 {
        info!("Converted {} big directories", big_dirs_count);
    }

    // Finalize
    info!("Creating indexes...");
    create_indexes(&conn).map_err(WalkerError::Database)?;

    info!("Optimizing for reads...");
    optimize_for_reads(&conn).map_err(WalkerError::Database)?;

    // Re-enable safety
    conn.execute_batch("PRAGMA synchronous = NORMAL;")
        .map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

    info!(
        "Conversion complete: {} entries converted",
        stats.entries_converted
    );

    Ok(stats)
}

/// Copy metadata from RocksDB to SQLite
fn copy_metadata(rocks: &RocksHandle, conn: &Connection) -> Result<(), WalkerError> {
    let metadata_keys = [
        (meta_keys::SOURCE_URL, keys::SOURCE_URL),
        (meta_keys::START_TIME, keys::START_TIME),
        (meta_keys::END_TIME, keys::END_TIME),
        (meta_keys::STATUS, keys::STATUS),
        (meta_keys::DURATION_SECS, keys::DURATION_SECS),
        (meta_keys::TOTAL_DIRS, keys::TOTAL_DIRS),
        (meta_keys::TOTAL_FILES, keys::TOTAL_FILES),
        (meta_keys::TOTAL_BYTES, keys::TOTAL_BYTES),
        (meta_keys::ERROR_COUNT, keys::ERROR_COUNT),
        (meta_keys::WORKER_COUNT, keys::WORKER_COUNT),
    ];

    for (rocks_key, sqlite_key) in metadata_keys.iter() {
        if let Some(value) = rocks.get_metadata(rocks_key)
            .map_err(|e| WalkerError::Database(DbError::Schema(format!("Failed to read metadata: {}", e))))?
        {
            set_walk_info(conn, sqlite_key, &value).map_err(WalkerError::Database)?;
        }
    }

    Ok(())
}

/// Convert entries from RocksDB to SQLite
fn convert_entries(
    rocks: &RocksHandle,
    conn: &mut Connection,
    _batch_size: usize,
    progress_callback: Option<ProgressCallback>,
) -> Result<ConvertStats, WalkerError> {
    let mut stats = ConvertStats::default();
    let mut batch_count = 0;

    // Iterate all entries
    let iter = rocks.iter_by_path();

    let tx = conn.transaction()
        .map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO entries (parent_id, name, path, entry_type, size, mtime, atime, ctime, mode, uid, gid, nlink, inode, depth, extension, blocks)
             VALUES (NULL, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
        ).map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

        for result in iter {
            let entry = result
                .map_err(|e| WalkerError::Database(DbError::Schema(format!("Failed to read entry: {}", e))))?;

            stmt.execute(params![
                entry.name,
                entry.path,
                entry.entry_type as i32,
                entry.size as i64,
                entry.mtime,
                entry.atime,
                entry.ctime,
                entry.mode.map(|m| m as i64),
                entry.uid.map(|u| u as i64),
                entry.gid.map(|g| g as i64),
                entry.nlink.map(|n| n as i64),
                entry.inode as i64,
                entry.depth as i64,
                entry.extension,
                entry.blocks as i64,
            ]).map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

            stats.entries_converted += 1;
            batch_count += 1;

            // Report progress periodically
            if let Some(ref callback) = progress_callback {
                if batch_count >= 10_000 {
                    callback(stats.entries_converted, 0);
                    batch_count = 0;
                }
            }
        }
    }

    tx.commit()
        .map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

    // Final progress
    if let Some(callback) = progress_callback {
        callback(stats.entries_converted, stats.entries_converted);
    }

    Ok(stats)
}

/// Convert big directories from RocksDB to SQLite
fn convert_big_dirs(rocks: &RocksHandle, conn: &mut Connection) -> Result<u64, WalkerError> {
    let mut count = 0u64;

    let tx = conn
        .transaction()
        .map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

    {
        let mut stmt = tx
            .prepare_cached(
                "INSERT INTO big_directories (path, file_count) VALUES (?1, ?2)",
            )
            .map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

        for result in rocks.iter_big_dirs() {
            let (path, file_count) = result.map_err(|e| {
                WalkerError::Database(DbError::Schema(format!("Failed to read big dir: {}", e)))
            })?;

            stmt.execute(params![path, file_count as i64])
                .map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

            count += 1;
        }
    }

    tx.commit()
        .map_err(|e| WalkerError::Database(DbError::Sqlite(e)))?;

    Ok(count)
}

/// Statistics from conversion
#[derive(Debug, Clone, Default)]
pub struct ConvertStats {
    pub entries_converted: u64,
    pub big_dirs_converted: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::{DbEntry, EntryType};
    use crate::rocksdb::writer::{RocksWriter, RocksWriterConfig};
    use tempfile::tempdir;

    #[test]
    fn test_convert_rocks_to_sqlite() {
        let dir = tempdir().unwrap();
        let rocks_path = dir.path().join("test.rocks");
        let sqlite_path = dir.path().join("test.db");

        // Create RocksDB with test data
        let config = RocksWriterConfig::default();
        let writer = RocksWriter::open(&rocks_path, config).unwrap();

        let entries = vec![
            DbEntry {
                parent_path: Some("/".to_string()),
                name: "file1.txt".to_string(),
                path: "/file1.txt".to_string(),
                entry_type: EntryType::File,
                size: 1024,
                mtime: Some(1234567890),
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
                name: "dir1".to_string(),
                path: "/dir1".to_string(),
                entry_type: EntryType::Directory,
                size: 0,
                mtime: Some(1234567890),
                atime: None,
                ctime: None,
                mode: Some(0o755),
                uid: Some(1000),
                gid: Some(1000),
                nlink: Some(2),
                inode: 200,
                depth: 1,
                extension: None,
                blocks: 0,
            },
        ];

        writer.set_metadata(meta_keys::SOURCE_URL, "nfs://test/export").unwrap();
        writer.set_metadata(meta_keys::TOTAL_FILES, "1").unwrap();
        writer.set_metadata(meta_keys::TOTAL_DIRS, "1").unwrap();
        writer.write_batch(&entries).unwrap();
        writer.flush().unwrap();
        drop(writer);

        // Convert to SQLite
        let config = ConvertConfig {
            batch_size: 1000,
            progress: false,
        };
        let stats = convert_rocks_to_sqlite(&rocks_path, &sqlite_path, config, None).unwrap();

        assert_eq!(stats.entries_converted, 2);

        // Verify SQLite contents
        let conn = Connection::open(&sqlite_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }
}
