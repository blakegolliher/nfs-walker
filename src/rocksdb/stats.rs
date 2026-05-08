//! RocksDB scan overview statistics.
//!
//! Returns the high-level summary (counts, total size, max depth) used by
//! the `stats` subcommand. Per-flag analytics (largest files, by extension,
//! by UID, duplicates, etc.) were removed in favour of `export-parquet`
//! followed by DuckDB / DataFusion queries.

use crate::error::RocksError;
use crate::rocksdb::schema::{default_secondary_path, OpenMode, RocksHandle};
use crate::rocksdb::summary::SummaryReader;
use std::path::Path;
use tracing::debug;

/// Open a query handle in the requested mode. In secondary mode, prepares
/// the secondary state dir and replays the latest MANIFEST/WAL state from
/// the live primary before returning.
fn open_query_handle<P: AsRef<Path>>(path: P, mode: OpenMode) -> Result<RocksHandle, RocksError> {
    match mode {
        OpenMode::Readonly => RocksHandle::open_readonly(path).map_err(RocksError::Rocks),
        OpenMode::Secondary => {
            let secondary = default_secondary_path(&path);
            std::fs::create_dir_all(&secondary).map_err(|e| {
                RocksError::Io(format!(
                    "Failed to create secondary state dir {}: {}",
                    secondary.display(),
                    e
                ))
            })?;
            let handle = RocksHandle::open_secondary(path.as_ref(), secondary.as_path())
                .map_err(RocksError::Rocks)?;
            handle.try_catch_up_with_primary().map_err(RocksError::Rocks)?;
            Ok(handle)
        }
    }
}

/// Overall database statistics.
#[derive(Debug, Clone, Default)]
pub struct DbStats {
    pub total_entries: u64,
    pub total_files: u64,
    pub total_dirs: u64,
    pub total_symlinks: u64,
    pub total_bytes: u64,
    pub total_blocks: u64,
    pub max_depth: u32,
}

/// Compute statistics from a RocksDB database.
///
/// **Per-path semantics**: every directory entry contributes, so
/// hardlinked files count once per name. Matches the writer's natural
/// view (one row per `DbEntry` batch). Both the summary fast path and
/// the iteration fallback below return identical numbers.
///
/// Fast path: returns instantly from the summary CF if present.
/// Slow path: full path-CF iteration (legacy DBs without summary).
pub fn compute_stats<P: AsRef<Path>>(path: P, mode: OpenMode) -> Result<DbStats, RocksError> {
    let handle = open_query_handle(path, mode)?;

    if let Some(s) = SummaryReader::try_load(&handle)? {
        return Ok(DbStats {
            total_entries: s.total.total_entries,
            total_files: s.total.total_files,
            total_dirs: s.total.total_dirs,
            total_symlinks: s.total.total_symlinks,
            total_bytes: s.total.total_bytes,
            total_blocks: s.total.total_blocks,
            max_depth: s.total.max_depth,
        });
    }
    debug!(
        "compute_stats: summary CF not available, falling back to full-scan iteration \
         (this may take a while on large databases)"
    );

    let mut stats = DbStats::default();

    for result in handle.iter_by_path() {
        let entry = result?;
        stats.total_entries += 1;

        match entry.entry_type {
            0 => {
                stats.total_files += 1;
                stats.total_bytes += entry.size;
                stats.total_blocks += entry.blocks;
            }
            1 => stats.total_dirs += 1,
            2 => stats.total_symlinks += 1,
            _ => {}
        }

        if entry.depth > stats.max_depth {
            stats.max_depth = entry.depth;
        }
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::{DbEntry, EntryType};
    use crate::rocksdb::schema::{RocksEntry, CF_SUMMARY};
    use crate::rocksdb::summary::SummaryAccumulator;
    use rocksdb::WriteBatch;
    use tempfile::tempdir;

    fn sample_entries() -> Vec<DbEntry> {
        let mk_file = |path: &str, size: u64, ext: &str, uid: u32| DbEntry {
            parent_path: Some("/".to_string()),
            name: path.trim_start_matches('/').to_string(),
            path: path.to_string(),
            entry_type: EntryType::File,
            size,
            mtime: Some(1700000000),
            mode: Some(0o644),
            uid: Some(uid),
            gid: Some(uid),
            nlink: Some(1),
            inode: path.bytes().fold(1u64, |a, b| a.wrapping_add(b as u64)),
            depth: 1,
            extension: Some(ext.to_string()),
            blocks: size.div_ceil(512),
            ..Default::default()
        };
        let mk_dir = |path: &str, depth: u32, uid: u32| DbEntry {
            parent_path: Some("/".to_string()),
            name: path.trim_start_matches('/').to_string(),
            path: path.to_string(),
            entry_type: EntryType::Directory,
            size: 4096,
            mode: Some(0o755),
            uid: Some(uid),
            gid: Some(uid),
            nlink: Some(2),
            inode: path.bytes().fold(2u64, |a, b| a.wrapping_add(b as u64)),
            depth,
            blocks: 8,
            ..Default::default()
        };
        vec![
            mk_file("/a.txt", 100, "txt", 1000),
            mk_file("/b.txt", 200, "txt", 1000),
            mk_file("/c.txt", 300, "txt", 1000),
            mk_file("/d.bin", 400, "bin", 2000),
            mk_file("/e.bin", 500, "bin", 2000),
            mk_dir("/sub", 1, 1000),
            mk_dir("/sub/deep", 2, 1000),
        ]
    }

    fn build_db(entries: &[DbEntry]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.rocks");
        {
            let handle = RocksHandle::open(&db_path).unwrap();
            for entry in entries {
                let rocks_entry = RocksEntry::from_db_entry(entry);
                handle.put_entry(&rocks_entry).unwrap();
            }
            handle.db.flush().unwrap();
        }
        (dir, db_path)
    }

    fn flush_summary(db_path: &std::path::Path, summary: &SummaryAccumulator) {
        let handle = RocksHandle::open(db_path).unwrap();
        let cf = handle.db.cf_handle(CF_SUMMARY).expect("summary CF missing");
        let mut batch = WriteBatch::default();
        for (k, v) in summary.serialize_kv().unwrap() {
            batch.put_cf(cf, k, &v);
        }
        handle.db.write(batch).unwrap();
        handle.db.flush().unwrap();
    }

    #[test]
    fn compute_stats_matches_between_summary_and_iteration() {
        let entries = sample_entries();
        let (_dir, db) = build_db(&entries);

        let from_iter = compute_stats(&db, OpenMode::Readonly).unwrap();
        assert_eq!(from_iter.total_entries, 7);
        assert_eq!(from_iter.total_files, 5);
        assert_eq!(from_iter.total_dirs, 2);
        assert_eq!(from_iter.total_bytes, 100 + 200 + 300 + 400 + 500);

        let mut acc = SummaryAccumulator::new();
        acc.update(&entries);
        flush_summary(&db, &acc);

        let from_summary = compute_stats(&db, OpenMode::Readonly).unwrap();
        assert_eq!(from_summary.total_entries, from_iter.total_entries);
        assert_eq!(from_summary.total_files, from_iter.total_files);
        assert_eq!(from_summary.total_dirs, from_iter.total_dirs);
        assert_eq!(from_summary.total_bytes, from_iter.total_bytes);
        assert_eq!(from_summary.max_depth, from_iter.max_depth);
    }

    #[test]
    fn legacy_db_without_summary_cf_falls_back_to_iteration() {
        let entries = sample_entries();
        let (_dir, db) = build_db(&entries);
        let stats = compute_stats(&db, OpenMode::Readonly).unwrap();
        assert_eq!(stats.total_entries, 7);
        assert_eq!(stats.total_files, 5);
    }

    #[test]
    fn hardlinks_count_per_path() {
        let template = DbEntry {
            parent_path: Some("/".to_string()),
            entry_type: EntryType::File,
            size: 1000,
            mode: Some(0o644),
            uid: Some(1000),
            gid: Some(1000),
            nlink: Some(2),
            inode: 99,
            depth: 1,
            extension: Some("bin".to_string()),
            blocks: 2,
            ..Default::default()
        };
        let entries = vec![
            DbEntry {
                name: "primary.bin".into(),
                path: "/primary.bin".into(),
                ..template.clone()
            },
            DbEntry {
                name: "alias.bin".into(),
                path: "/alias.bin".into(),
                ..template
            },
        ];
        let (_dir, db) = build_db(&entries);

        let from_iter = compute_stats(&db, OpenMode::Readonly).unwrap();
        assert_eq!(from_iter.total_entries, 2);
        assert_eq!(from_iter.total_files, 2);
        assert_eq!(from_iter.total_bytes, 2000);

        let mut acc = SummaryAccumulator::new();
        acc.update(&entries);
        flush_summary(&db, &acc);

        let from_summary = compute_stats(&db, OpenMode::Readonly).unwrap();
        assert_eq!(from_summary.total_entries, from_iter.total_entries);
        assert_eq!(from_summary.total_bytes, from_iter.total_bytes);
    }
}
