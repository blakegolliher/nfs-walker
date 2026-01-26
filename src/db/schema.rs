//! Database schema definitions and creation
//!
//! This module defines the SQLite schema for storing filesystem entries
//! and provides functions to create and configure the database.

use crate::error::DbResult;
use rusqlite::Connection;

/// Current schema version for migrations
pub const SCHEMA_VERSION: u32 = 1;

/// SQL to create the main entries table
/// Note: Using INTEGER PRIMARY KEY (without AUTOINCREMENT) for speed.
/// This still auto-assigns IDs but without the sqlite_sequence overhead.
const CREATE_ENTRIES_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    entry_type INTEGER NOT NULL,  -- 0=file, 1=dir, 2=symlink, 3+=other
    size INTEGER DEFAULT 0,
    mtime INTEGER,                -- Unix timestamp
    atime INTEGER,
    ctime INTEGER,
    mode INTEGER,                 -- Permission bits
    uid INTEGER,
    gid INTEGER,
    nlink INTEGER,
    inode INTEGER,
    depth INTEGER NOT NULL,
    extension TEXT,               -- File extension (without dot, lowercase)
    blocks INTEGER DEFAULT 0      -- Number of 512-byte blocks allocated
)
"#;

/// SQL to create directory statistics table
const CREATE_DIR_STATS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS dir_stats (
    entry_id INTEGER PRIMARY KEY,
    direct_file_count INTEGER DEFAULT 0,
    direct_dir_count INTEGER DEFAULT 0,
    direct_symlink_count INTEGER DEFAULT 0,
    direct_other_count INTEGER DEFAULT 0,
    direct_bytes INTEGER DEFAULT 0,

    FOREIGN KEY (entry_id) REFERENCES entries(id)
)
"#;

/// SQL to create walk metadata table
const CREATE_WALK_INFO_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS walk_info (
    key TEXT PRIMARY KEY,
    value TEXT
)
"#;

/// SQL to create big directories table (for big-dir-hunt mode)
const CREATE_BIG_DIRS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS big_directories (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    file_count INTEGER NOT NULL
)
"#;

/// SQL to create indexes for common queries
const CREATE_INDEXES: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_entries_parent ON entries(parent_id)",
    "CREATE INDEX IF NOT EXISTS idx_entries_depth ON entries(depth)",
    "CREATE INDEX IF NOT EXISTS idx_entries_size ON entries(size) WHERE entry_type = 0",
    "CREATE INDEX IF NOT EXISTS idx_entries_type ON entries(entry_type)",
    "CREATE INDEX IF NOT EXISTS idx_entries_path ON entries(path)",
    "CREATE INDEX IF NOT EXISTS idx_entries_mtime ON entries(mtime) WHERE entry_type = 0",
];

/// SQLite pragmas for optimal write performance
const WRITE_PRAGMAS: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = OFF;
PRAGMA cache_size = -128000;     -- 128MB cache
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 536870912;    -- 512MB mmap
PRAGMA page_size = 4096;
PRAGMA auto_vacuum = NONE;
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA wal_autocheckpoint = 10000;
"#;

/// SQLite pragmas for read-optimized queries (applied after walk completes)
const READ_PRAGMAS: &str = r#"
PRAGMA synchronous = FULL;
PRAGMA locking_mode = NORMAL;
"#;

/// Create and configure a new database for writing
pub fn create_database(conn: &Connection) -> DbResult<()> {
    // Apply write-optimized pragmas
    conn.execute_batch(WRITE_PRAGMAS)?;

    // Create tables
    conn.execute(CREATE_ENTRIES_TABLE, [])?;
    conn.execute(CREATE_DIR_STATS_TABLE, [])?;
    conn.execute(CREATE_WALK_INFO_TABLE, [])?;
    conn.execute(CREATE_BIG_DIRS_TABLE, [])?;

    Ok(())
}

/// Create indexes (called after walk completes for better insert performance)
pub fn create_indexes(conn: &Connection) -> DbResult<()> {
    for sql in CREATE_INDEXES {
        conn.execute(sql, [])?;
    }
    Ok(())
}

/// Apply read-optimized settings
pub fn optimize_for_reads(conn: &Connection) -> DbResult<()> {
    conn.execute_batch(READ_PRAGMAS)?;

    // Run ANALYZE to update statistics for query planner
    conn.execute("ANALYZE", [])?;

    Ok(())
}

/// Store walk metadata
pub fn set_walk_info(conn: &Connection, key: &str, value: &str) -> DbResult<()> {
    conn.execute(
        "INSERT OR REPLACE INTO walk_info (key, value) VALUES (?1, ?2)",
        [key, value],
    )?;
    Ok(())
}

/// Get walk metadata
pub fn get_walk_info(conn: &Connection, key: &str) -> DbResult<Option<String>> {
    let result = conn.query_row(
        "SELECT value FROM walk_info WHERE key = ?1",
        [key],
        |row| row.get(0),
    );

    match result {
        Ok(value) => Ok(Some(value)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Metadata keys used by the walker
pub mod keys {
    /// NFS URL that was scanned
    pub const SOURCE_URL: &str = "source_url";

    /// Timestamp when walk started (ISO 8601)
    pub const START_TIME: &str = "start_time";

    /// Timestamp when walk completed (ISO 8601)
    pub const END_TIME: &str = "end_time";

    /// Total duration in seconds
    pub const DURATION_SECS: &str = "duration_secs";

    /// Number of worker threads used
    pub const WORKER_COUNT: &str = "worker_count";

    /// Total entries scanned
    pub const TOTAL_ENTRIES: &str = "total_entries";

    /// Total directories scanned
    pub const TOTAL_DIRS: &str = "total_dirs";

    /// Total files scanned
    pub const TOTAL_FILES: &str = "total_files";

    /// Total bytes (sum of file sizes)
    pub const TOTAL_BYTES: &str = "total_bytes";

    /// Number of errors encountered
    pub const ERROR_COUNT: &str = "error_count";

    /// Schema version
    pub const SCHEMA_VERSION: &str = "schema_version";

    /// Walker version
    pub const WALKER_VERSION: &str = "walker_version";

    /// Walk status: "running", "completed", "interrupted"
    pub const STATUS: &str = "status";
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_create_database() {
        let conn = Connection::open_in_memory().unwrap();
        create_database(&conn).unwrap();

        // Verify tables exist
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='entries'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_walk_info() {
        let conn = Connection::open_in_memory().unwrap();
        create_database(&conn).unwrap();

        set_walk_info(&conn, "test_key", "test_value").unwrap();

        let value = get_walk_info(&conn, "test_key").unwrap();
        assert_eq!(value, Some("test_value".to_string()));

        let missing = get_walk_info(&conn, "nonexistent").unwrap();
        assert_eq!(missing, None);
    }

    #[test]
    fn test_create_indexes() {
        let conn = Connection::open_in_memory().unwrap();
        create_database(&conn).unwrap();
        create_indexes(&conn).unwrap();

        // Verify indexes exist
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name LIKE 'idx_entries_%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(count >= 5);
    }
}
