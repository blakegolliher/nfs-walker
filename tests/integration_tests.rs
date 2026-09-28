//! Integration tests for nfs-walker
//!
//! Note: Most tests require an actual NFS server to be available.
//! These tests cover NFS URL parsing and DbEntry semantics — the
//! end-to-end walker behavior is exercised by `pipelined_walker_test.rs`.

use nfs_walker::config::NfsUrl;
use nfs_walker::nfs::types::{DbEntry, EntryType};

#[test]
fn test_nfs_url_parsing() {
    // Standard URL
    let url = NfsUrl::parse("nfs://server.local/export").unwrap();
    assert_eq!(url.server, "server.local");
    assert_eq!(url.export, "/export");

    // Multi-component path: parsed as one export today.
    // `--export /explicit` overrides at WalkConfig::from_args time.
    let url = NfsUrl::parse("nfs://server/export/data/subdir").unwrap();
    assert_eq!(url.full_path(), "/export/data/subdir");

    // Legacy format
    let url = NfsUrl::parse("192.168.1.100:/data").unwrap();
    assert_eq!(url.server, "192.168.1.100");
    assert_eq!(url.export, "/data");
}

#[test]
fn test_db_entry_types() {
    let entry = DbEntry {
        parent_path: Some("/data".to_string()),
        name: "file.txt".into(),
        path: "/data/file.txt".into(),
        entry_type: EntryType::File,
        size: 1024,
        mtime_sec: Some(1234567890),
        mtime_nsec: Some(0),
        mode: Some(0o644),
        uid: Some(1000),
        gid: Some(1000),
        nlink: Some(1),
        inode: 12345,
        depth: 2,
        extension: Some("txt".to_string()),
        blocks: 8,
        ..Default::default()
    };

    assert!(entry.entry_type.is_file());
    assert!(!entry.entry_type.is_dir());
    assert_eq!(entry.entry_type.as_db_int(), 0);
}
