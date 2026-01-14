//! Integration tests for nfs-walker
//!
//! Note: Most tests require an actual NFS server to be available.
//! These tests use local SQLite operations only.

use nfs_walker::config::NfsUrl;
use nfs_walker::db::schema;
use nfs_walker::nfs::types::{DbEntry, EntryType};
use rusqlite::Connection;
use tempfile::tempdir;

#[test]
fn test_nfs_url_parsing() {
    // Standard URL
    let url = NfsUrl::parse("nfs://server.local/export").unwrap();
    assert_eq!(url.server, "server.local");
    assert_eq!(url.export, "/export");

    // With subpath
    let url = NfsUrl::parse("nfs://server/export/data/subdir").unwrap();
    assert_eq!(url.export, "/export");
    assert_eq!(url.subpath, "/data/subdir");
    assert_eq!(url.full_path(), "/export/data/subdir");

    // Legacy format
    let url = NfsUrl::parse("192.168.1.100:/data").unwrap();
    assert_eq!(url.server, "192.168.1.100");
    assert_eq!(url.export, "/data");
}

#[test]
fn test_database_schema_creation() {
    let conn = Connection::open_in_memory().unwrap();
    schema::create_database(&conn).unwrap();
    schema::create_indexes(&conn).unwrap();

    // Verify tables exist
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(tables.contains(&"entries".to_string()));
    assert!(tables.contains(&"dir_stats".to_string()));
    assert!(tables.contains(&"walk_info".to_string()));
}

#[test]
fn test_entry_insertion() {
    let conn = Connection::open_in_memory().unwrap();
    schema::create_database(&conn).unwrap();

    // Insert a test entry
    conn.execute(
        "INSERT INTO entries (name, path, entry_type, size, depth) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params!["test.txt", "/data/test.txt", 0, 1024, 1],
    )
    .unwrap();

    // Verify
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_db_entry_types() {
    let entry = DbEntry {
        parent_path: Some("/data".to_string()),
        name: "file.txt".into(),
        path: "/data/file.txt".into(),
        entry_type: EntryType::File,
        size: 1024,
        mtime: Some(1234567890),
        atime: None,
        ctime: None,
        mode: Some(0o644),
        uid: Some(1000),
        gid: Some(1000),
        nlink: Some(1),
        inode: 12345,
        depth: 2,
    };

    assert!(entry.entry_type.is_file());
    assert!(!entry.entry_type.is_dir());
    assert_eq!(entry.entry_type.as_db_int(), 0);
}

#[test]
fn test_walk_info_metadata() {
    let conn = Connection::open_in_memory().unwrap();
    schema::create_database(&conn).unwrap();

    // Set metadata
    schema::set_walk_info(&conn, "test_key", "test_value").unwrap();

    // Get metadata
    let value = schema::get_walk_info(&conn, "test_key").unwrap();
    assert_eq!(value, Some("test_value".to_string()));

    // Missing key
    let missing = schema::get_walk_info(&conn, "nonexistent").unwrap();
    assert_eq!(missing, None);

    // Update existing
    schema::set_walk_info(&conn, "test_key", "new_value").unwrap();
    let updated = schema::get_walk_info(&conn, "test_key").unwrap();
    assert_eq!(updated, Some("new_value".to_string()));
}

#[test]
fn test_output_database_creation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_walk.db");

    // Create database
    let conn = Connection::open(&db_path).unwrap();
    schema::create_database(&conn).unwrap();
    schema::create_indexes(&conn).unwrap();
    schema::optimize_for_reads(&conn).unwrap();
    drop(conn);

    // Verify file was created
    assert!(db_path.exists());

    // Reopen and verify schema
    let conn = Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='entries'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}
