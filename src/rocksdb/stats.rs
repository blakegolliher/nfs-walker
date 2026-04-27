//! RocksDB statistics and queries
//!
//! Compute common filesystem statistics directly from RocksDB without conversion.

use crate::error::RocksError;
use crate::rocksdb::schema::{default_secondary_path, OpenMode, RocksHandle};
use crate::rocksdb::summary::SummaryReader;
use std::collections::HashMap;
use std::path::Path;
use tracing::debug;

/// Try to load the per-DB summary snapshot. Returns `Ok(None)` for legacy
/// DBs without the summary CF and for DBs whose writer never finished a
/// flush (callers fall back to iteration in either case).
fn try_load_summary(handle: &RocksHandle) -> Result<Option<SummaryReader>, RocksError> {
    SummaryReader::try_load(handle)
}

/// Logged once per stats function when the summary CF is missing.
fn note_summary_fallback(fn_name: &str) {
    debug!(
        "{}: summary CF not available, falling back to full-scan iteration \
         (this may take a while on large databases)",
        fn_name
    );
}

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

/// Statistics about files grouped by extension
#[derive(Debug, Clone, Default)]
pub struct ExtensionStats {
    pub extension: String,
    pub count: u64,
    pub total_bytes: u64,
    pub total_blocks: u64,
}

/// Overall database statistics
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

    if let Some(s) = try_load_summary(&handle)? {
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
    note_summary_fallback("compute_stats");

    let mut stats = DbStats::default();

    for result in handle.iter_by_path() {
        let entry = result?;
        stats.total_entries += 1;

        match entry.entry_type {
            0 => {
                // File
                stats.total_files += 1;
                stats.total_bytes += entry.size;
                stats.total_blocks += entry.blocks;
            }
            1 => stats.total_dirs += 1,     // Directory
            2 => stats.total_symlinks += 1, // Symlink
            _ => {}
        }

        if entry.depth > stats.max_depth {
            stats.max_depth = entry.depth;
        }
    }

    Ok(stats)
}

/// Compute file statistics grouped by extension.
///
/// **Per-path semantics**: hardlinked files count once per name.
/// Both the summary fast path and the iteration fallback below return
/// identical numbers.
///
/// Fast path: project the summary CF's by_extension map.
/// Slow path: full path-CF iteration.
pub fn stats_by_extension<P: AsRef<Path>>(
    path: P,
    top_n: usize,
    mode: OpenMode,
) -> Result<Vec<ExtensionStats>, RocksError> {
    let handle = open_query_handle(path, mode)?;

    if let Some(s) = try_load_summary(&handle)? {
        let mut results: Vec<ExtensionStats> = s
            .by_extension
            .into_iter()
            .map(|(ext, c)| ExtensionStats {
                extension: ext,
                count: c.count,
                total_bytes: c.bytes,
                total_blocks: c.blocks,
            })
            .collect();
        results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
        results.truncate(top_n);
        return Ok(results);
    }
    note_summary_fallback("stats_by_extension");

    let mut ext_map: HashMap<String, ExtensionStats> = HashMap::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        // Only count files
        if entry.entry_type != 0 {
            continue;
        }

        let ext = entry.extension.unwrap_or_default();
        let stats = ext_map.entry(ext.clone()).or_insert_with(|| ExtensionStats {
            extension: ext,
            count: 0,
            total_bytes: 0,
            total_blocks: 0,
        });

        stats.count += 1;
        stats.total_bytes += entry.size;
        stats.total_blocks += entry.blocks;
    }

    // Sort by total bytes descending
    let mut results: Vec<_> = ext_map.into_values().collect();
    results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
    results.truncate(top_n);

    Ok(results)
}

/// Find the largest files
pub fn largest_files<P: AsRef<Path>>(
    path: P,
    top_n: usize,
    mode: OpenMode,
) -> Result<Vec<(String, u64)>, RocksError> {
    let handle = open_query_handle(path, mode)?;
    let mut files: Vec<(String, u64)> = Vec::new();

    for result in handle.iter_by_inode() {
        let entry = result?;

        // Only count files
        if entry.entry_type != 0 {
            continue;
        }

        // Keep track of largest files
        if files.len() < top_n || entry.size > files.last().map(|f| f.1).unwrap_or(0) {
            files.push((entry.path.clone(), entry.size));
            files.sort_by(|a, b| b.1.cmp(&a.1));
            files.truncate(top_n);
        }
    }

    Ok(files)
}

/// Find directories with the most direct children
pub fn largest_directories<P: AsRef<Path>>(
    path: P,
    top_n: usize,
    mode: OpenMode,
) -> Result<Vec<(String, u64)>, RocksError> {
    let handle = open_query_handle(path, mode)?;
    let mut dir_counts: HashMap<String, u64> = HashMap::new();

    // Iterate by path: a directory's child count is per-name, not per-inode.
    // Hardlinked aliases each contribute to their own parent's count.
    for result in handle.iter_by_path() {
        let entry = result?;

        // Get parent directory
        if let Some(pos) = entry.path.rfind('/') {
            let parent = if pos == 0 {
                "/".to_string()
            } else {
                entry.path[..pos].to_string()
            };
            *dir_counts.entry(parent).or_insert(0) += 1;
        }
    }

    // Sort by count descending
    let mut results: Vec<_> = dir_counts.into_iter().collect();
    results.sort_by(|a, b| b.1.cmp(&a.1));
    results.truncate(top_n);

    Ok(results)
}

/// Find oldest files by mtime
pub fn oldest_files<P: AsRef<Path>>(
    path: P,
    top_n: usize,
    mode: OpenMode,
) -> Result<Vec<(String, Option<i64>, u64)>, RocksError> {
    let handle = open_query_handle(path, mode)?;
    let mut files: Vec<(String, Option<i64>, u64)> = Vec::new();

    for result in handle.iter_by_inode() {
        let entry = result?;

        // Only files
        if entry.entry_type != 0 {
            continue;
        }

        // Keep track of oldest files (smallest mtime)
        let dominated = files.len() >= top_n
            && entry.mtime >= files.last().and_then(|f| f.1);

        if !dominated {
            files.push((entry.path.clone(), entry.mtime, entry.size));
            files.sort_by(|a, b| a.1.cmp(&b.1));
            files.truncate(top_n);
        }
    }

    Ok(files)
}

/// Find files with the most hard links
pub fn most_hardlinks<P: AsRef<Path>>(
    path: P,
    top_n: usize,
    mode: OpenMode,
) -> Result<Vec<(String, u64, u64)>, RocksError> {
    let handle = open_query_handle(path, mode)?;
    let mut files: Vec<(String, u64, u64)> = Vec::new();

    for result in handle.iter_by_inode() {
        let entry = result?;

        // Only files
        if entry.entry_type != 0 {
            continue;
        }

        let nlink = entry.nlink.unwrap_or(1);

        // Keep track of files with most links
        if files.len() < top_n || nlink > files.last().map(|f| f.1).unwrap_or(0) {
            files.push((entry.path.clone(), nlink, entry.size));
            files.sort_by(|a, b| b.1.cmp(&a.1));
            files.truncate(top_n);
        }
    }

    Ok(files)
}

/// Statistics for a specific user
#[derive(Debug, Clone, Default)]
pub struct OwnerStats {
    pub id: u32,
    pub file_count: u64,
    pub dir_count: u64,
    pub total_bytes: u64,
}

/// Get file statistics by user ID.
///
/// **Per-path semantics**: hardlinked files count once per name.
/// Both the summary fast path and the iteration fallback return
/// identical numbers.
///
/// Fast path: project the summary CF's by_uid map.
/// Slow path: full path-CF iteration.
pub fn stats_by_uid<P: AsRef<Path>>(
    path: P,
    top_n: usize,
    mode: OpenMode,
) -> Result<Vec<OwnerStats>, RocksError> {
    let handle = open_query_handle(path, mode)?;

    if let Some(s) = try_load_summary(&handle)? {
        let mut results: Vec<OwnerStats> = s
            .by_uid
            .into_iter()
            .map(|(uid, c)| OwnerStats {
                id: uid,
                file_count: c.file_count,
                dir_count: c.dir_count,
                total_bytes: c.bytes,
            })
            .collect();
        results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
        results.truncate(top_n);
        return Ok(results);
    }
    note_summary_fallback("stats_by_uid");

    let mut uid_map: HashMap<u32, OwnerStats> = HashMap::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        let uid = entry.uid.unwrap_or(0);
        let stats = uid_map.entry(uid).or_insert_with(|| OwnerStats {
            id: uid,
            ..Default::default()
        });

        match entry.entry_type {
            0 => {
                stats.file_count += 1;
                stats.total_bytes += entry.size;
            }
            1 => stats.dir_count += 1,
            _ => {}
        }
    }

    // Sort by total bytes descending
    let mut results: Vec<_> = uid_map.into_values().collect();
    results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
    results.truncate(top_n);

    Ok(results)
}

/// Get file statistics by group ID.
///
/// **Per-path semantics**: hardlinked files count once per name.
/// Both the summary fast path and the iteration fallback return
/// identical numbers.
///
/// Fast path: project the summary CF's by_gid map.
/// Slow path: full path-CF iteration.
pub fn stats_by_gid<P: AsRef<Path>>(
    path: P,
    top_n: usize,
    mode: OpenMode,
) -> Result<Vec<OwnerStats>, RocksError> {
    let handle = open_query_handle(path, mode)?;

    if let Some(s) = try_load_summary(&handle)? {
        let mut results: Vec<OwnerStats> = s
            .by_gid
            .into_iter()
            .map(|(gid, c)| OwnerStats {
                id: gid,
                file_count: c.file_count,
                dir_count: c.dir_count,
                total_bytes: c.bytes,
            })
            .collect();
        results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
        results.truncate(top_n);
        return Ok(results);
    }
    note_summary_fallback("stats_by_gid");

    let mut gid_map: HashMap<u32, OwnerStats> = HashMap::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        let gid = entry.gid.unwrap_or(0);
        let stats = gid_map.entry(gid).or_insert_with(|| OwnerStats {
            id: gid,
            ..Default::default()
        });

        match entry.entry_type {
            0 => {
                stats.file_count += 1;
                stats.total_bytes += entry.size;
            }
            1 => stats.dir_count += 1,
            _ => {}
        }
    }

    // Sort by total bytes descending
    let mut results: Vec<_> = gid_map.into_values().collect();
    results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
    results.truncate(top_n);

    Ok(results)
}

/// A group of duplicate files (same checksum)
#[derive(Debug, Clone)]
pub struct DuplicateGroup {
    pub checksum: String,
    pub file_size: u64,
    pub paths: Vec<String>,
    /// Wasted bytes = size * (count - 1)
    pub wasted_bytes: u64,
}

/// Find duplicate files by checksum
///
/// Only considers files with checksums (requires --checksum during scan).
/// Returns groups sorted by wasted bytes descending.
pub fn find_duplicates<P: AsRef<Path>>(
    path: P,
    min_size: u64,
    top_n: usize,
    mode: OpenMode,
) -> Result<Vec<DuplicateGroup>, RocksError> {
    let handle = open_query_handle(path, mode)?;
    let mut checksum_map: HashMap<String, (u64, Vec<String>)> = HashMap::new();

    for result in handle.iter_by_inode() {
        let entry = result?;

        // Only files with checksums
        if entry.entry_type != 0 {
            continue;
        }

        // Skip small files
        if entry.size < min_size {
            continue;
        }

        if let Some(ref checksum) = entry.checksum {
            let (size, paths) = checksum_map
                .entry(checksum.clone())
                .or_insert_with(|| (entry.size, Vec::new()));
            paths.push(entry.path.clone());
            *size = entry.size; // All files with same checksum should have same size
        }
    }

    // Filter to only groups with 2+ files, convert to DuplicateGroup
    let mut results: Vec<DuplicateGroup> = checksum_map
        .into_iter()
        .filter(|(_, (_, paths))| paths.len() > 1)
        .map(|(checksum, (size, paths))| {
            let wasted = size * (paths.len() as u64 - 1);
            DuplicateGroup {
                checksum,
                file_size: size,
                paths,
                wasted_bytes: wasted,
            }
        })
        .collect();

    // Sort by wasted bytes descending
    results.sort_by(|a, b| b.wasted_bytes.cmp(&a.wasted_bytes));
    results.truncate(top_n);

    Ok(results)
}

/// Statistics about files grouped by detected MIME type
#[derive(Debug, Clone, Default)]
pub struct FileTypeStats {
    pub mime_type: String,
    pub count: u64,
    pub total_bytes: u64,
}

/// Compute file statistics grouped by detected MIME type.
///
/// Only considers files with file_type set (requires --file-type during scan).
/// Files without a detected type are bucketed under "unknown".
///
/// **Per-path semantics**: hardlinked files count once per name.
/// Both the summary fast path and the iteration fallback return
/// identical numbers.
///
/// Fast path: project the summary CF's by_file_type map.
/// Slow path: full path-CF iteration.
pub fn stats_by_file_type<P: AsRef<Path>>(
    path: P,
    top_n: usize,
    mode: OpenMode,
) -> Result<Vec<FileTypeStats>, RocksError> {
    let handle = open_query_handle(path, mode)?;

    if let Some(s) = try_load_summary(&handle)? {
        let mut results: Vec<FileTypeStats> = s
            .by_file_type
            .into_iter()
            .map(|(ft, c)| FileTypeStats {
                mime_type: ft,
                count: c.count,
                total_bytes: c.bytes,
            })
            .collect();
        results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
        results.truncate(top_n);
        return Ok(results);
    }
    note_summary_fallback("stats_by_file_type");

    let mut type_map: HashMap<String, FileTypeStats> = HashMap::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        // Only files
        if entry.entry_type != 0 {
            continue;
        }

        let file_type = entry.file_type.unwrap_or_else(|| "unknown".to_string());
        let stats = type_map.entry(file_type.clone()).or_insert_with(|| FileTypeStats {
            mime_type: file_type,
            count: 0,
            total_bytes: 0,
        });

        stats.count += 1;
        stats.total_bytes += entry.size;
    }

    // Sort by total bytes descending
    let mut results: Vec<_> = type_map.into_values().collect();
    results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
    results.truncate(top_n);

    Ok(results)
}

/// A group of files sharing the same inode (hard links)
#[derive(Debug, Clone)]
pub struct HardLinkGroup {
    pub inode: u64,
    pub nlink: u64,
    pub size: u64,
    pub paths: Vec<String>,
}

/// Find hard link groups (files sharing the same inode)
///
/// Returns groups with at least min_links hard links, sorted by size descending.
pub fn find_hardlink_groups<P: AsRef<Path>>(
    path: P,
    min_links: u64,
    top_n: usize,
    mode: OpenMode,
) -> Result<Vec<HardLinkGroup>, RocksError> {
    let handle = open_query_handle(path, mode)?;
    let mut inode_map: HashMap<u64, (u64, u64, Vec<String>)> = HashMap::new();

    // Iterate by path: this query specifically needs to see every name an
    // inode is hardlinked under, which the inode CF deduplicates away.
    for result in handle.iter_by_path() {
        let entry = result?;

        // Only files
        if entry.entry_type != 0 {
            continue;
        }

        let nlink = entry.nlink.unwrap_or(1);

        // Only track files with multiple hard links
        if nlink < min_links {
            continue;
        }

        let (stored_nlink, size, paths) = inode_map
            .entry(entry.inode)
            .or_insert_with(|| (nlink, entry.size, Vec::new()));
        paths.push(entry.path.clone());
        *stored_nlink = nlink;
        *size = entry.size;
    }

    // Convert to HardLinkGroup
    let mut results: Vec<HardLinkGroup> = inode_map
        .into_iter()
        .filter(|(_, (_, _, paths))| paths.len() > 1)
        .map(|(inode, (nlink, size, paths))| HardLinkGroup {
            inode,
            nlink,
            size,
            paths,
        })
        .collect();

    // Sort by size descending
    results.sort_by(|a, b| b.size.cmp(&a.size));
    results.truncate(top_n);

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::{DbEntry, EntryType};
    use crate::rocksdb::schema::{RocksEntry, CF_SUMMARY};
    use crate::rocksdb::summary::SummaryAccumulator;
    use rocksdb::WriteBatch;
    use tempfile::tempdir;

    /// Build a small mixed batch of entries -- 3 files with txt extension
    /// from uid 1000, 2 binaries from uid 2000, 2 dirs.
    fn sample_entries() -> Vec<DbEntry> {
        let mk_file = |path: &str, size: u64, ext: &str, uid: u32, ft: Option<&str>| DbEntry {
            parent_path: Some("/".to_string()),
            name: path.trim_start_matches('/').to_string(),
            path: path.to_string(),
            entry_type: EntryType::File,
            size,
            mtime: Some(1700000000),
            atime: None,
            ctime: None,
            mode: Some(0o644),
            uid: Some(uid),
            gid: Some(uid),
            nlink: Some(1),
            inode: path.bytes().fold(1u64, |a, b| a.wrapping_add(b as u64)),
            depth: 1,
            extension: Some(ext.to_string()),
            blocks: size.div_ceil(512),
            checksum: None,
            file_type: ft.map(String::from),
        };
        let mk_dir = |path: &str, depth: u32, uid: u32| DbEntry {
            parent_path: Some("/".to_string()),
            name: path.trim_start_matches('/').to_string(),
            path: path.to_string(),
            entry_type: EntryType::Directory,
            size: 4096,
            mtime: None,
            atime: None,
            ctime: None,
            mode: Some(0o755),
            uid: Some(uid),
            gid: Some(uid),
            nlink: Some(2),
            inode: path.bytes().fold(2u64, |a, b| a.wrapping_add(b as u64)),
            depth,
            extension: None,
            blocks: 8,
            checksum: None,
            file_type: None,
        };
        vec![
            mk_file("/a.txt", 100, "txt", 1000, Some("text/plain")),
            mk_file("/b.txt", 200, "txt", 1000, Some("text/plain")),
            mk_file("/c.txt", 300, "txt", 1000, None),
            mk_file("/d.bin", 400, "bin", 2000, None),
            mk_file("/e.bin", 500, "bin", 2000, None),
            mk_dir("/sub", 1, 1000),
            mk_dir("/sub/deep", 2, 1000),
        ]
    }

    /// Open a fresh DB, write entries directly via put_entry, return handle.
    /// Importantly does NOT flush the summary CF -- that lets us exercise
    /// the iteration fallback first.
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

    /// Manually flush a SummaryAccumulator to the DB so subsequent stats
    /// calls take the summary-CF fast path.
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

        // Fallback path (no summary keys yet).
        let from_iter = compute_stats(&db, OpenMode::Readonly).unwrap();
        assert_eq!(from_iter.total_entries, 7);
        assert_eq!(from_iter.total_files, 5);
        assert_eq!(from_iter.total_dirs, 2);
        assert_eq!(from_iter.total_bytes, 100 + 200 + 300 + 400 + 500);

        // Summary path.
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
    fn stats_by_extension_summary_matches_iteration() {
        let entries = sample_entries();
        let (_dir, db) = build_db(&entries);

        let from_iter = stats_by_extension(&db, 100, OpenMode::Readonly).unwrap();

        let mut acc = SummaryAccumulator::new();
        acc.update(&entries);
        flush_summary(&db, &acc);

        let from_summary = stats_by_extension(&db, 100, OpenMode::Readonly).unwrap();

        // Compare as sorted (extension, count, bytes, blocks) tuples.
        let to_tuples = |v: Vec<ExtensionStats>| {
            let mut t: Vec<_> = v
                .into_iter()
                .map(|s| (s.extension, s.count, s.total_bytes, s.total_blocks))
                .collect();
            t.sort();
            t
        };
        assert_eq!(to_tuples(from_iter), to_tuples(from_summary));
    }

    #[test]
    fn stats_by_uid_summary_matches_iteration() {
        let entries = sample_entries();
        let (_dir, db) = build_db(&entries);

        let from_iter = stats_by_uid(&db, 100, OpenMode::Readonly).unwrap();

        let mut acc = SummaryAccumulator::new();
        acc.update(&entries);
        flush_summary(&db, &acc);

        let from_summary = stats_by_uid(&db, 100, OpenMode::Readonly).unwrap();

        let to_tuples = |v: Vec<OwnerStats>| {
            let mut t: Vec<_> = v
                .into_iter()
                .map(|s| (s.id, s.file_count, s.dir_count, s.total_bytes))
                .collect();
            t.sort();
            t
        };
        assert_eq!(to_tuples(from_iter), to_tuples(from_summary));
    }

    #[test]
    fn legacy_db_without_summary_cf_still_opens_and_falls_back() {
        // Build a DB the new-binary way then drop CF_SUMMARY by removing
        // the underlying CF. RocksDB doesn't have a clean drop API on a
        // closed handle, so simulate by writing entries and never
        // flushing summary keys. SummaryReader::try_load returns None
        // when KEY_TOTAL is absent, so the iteration fallback executes.
        let entries = sample_entries();
        let (_dir, db) = build_db(&entries);

        let stats = compute_stats(&db, OpenMode::Readonly).unwrap();
        assert_eq!(stats.total_entries, 7);
        assert_eq!(stats.total_files, 5);

        let by_ext = stats_by_extension(&db, 100, OpenMode::Readonly).unwrap();
        let txt = by_ext.iter().find(|s| s.extension == "txt").unwrap();
        assert_eq!(txt.count, 3);
        assert_eq!(txt.total_bytes, 600);
    }

    /// Two directory entries pointing at the same inode -- a hardlink.
    /// Both summary-hit and summary-miss must count the file twice
    /// (once per name) for the totals to agree.
    fn hardlink_entries() -> Vec<DbEntry> {
        let template = DbEntry {
            parent_path: Some("/".to_string()),
            name: String::new(),
            path: String::new(),
            entry_type: EntryType::File,
            size: 1000,
            mtime: None,
            atime: None,
            ctime: None,
            mode: Some(0o644),
            uid: Some(1000),
            gid: Some(1000),
            nlink: Some(2),
            inode: 99, // shared
            depth: 1,
            extension: Some("bin".to_string()),
            blocks: 2,
            checksum: None,
            file_type: None,
        };
        let primary = DbEntry {
            name: "primary.bin".to_string(),
            path: "/primary.bin".to_string(),
            ..template.clone()
        };
        let alias = DbEntry {
            name: "alias.bin".to_string(),
            path: "/alias.bin".to_string(),
            ..template
        };
        vec![primary, alias]
    }

    #[test]
    fn hardlinks_count_per_path_in_both_summary_and_iteration() {
        let entries = hardlink_entries();
        let (_dir, db) = build_db(&entries);

        // Iteration fallback (no summary keys flushed yet) must see
        // both names and count them separately.
        let from_iter = compute_stats(&db, OpenMode::Readonly).unwrap();
        assert_eq!(from_iter.total_entries, 2);
        assert_eq!(from_iter.total_files, 2);
        assert_eq!(from_iter.total_bytes, 2000);

        let ext_iter = stats_by_extension(&db, 100, OpenMode::Readonly).unwrap();
        let bin = ext_iter.iter().find(|s| s.extension == "bin").unwrap();
        assert_eq!(bin.count, 2);
        assert_eq!(bin.total_bytes, 2000);

        // Now flush a summary built from the same entries. The summary
        // path must yield identical numbers.
        let mut acc = SummaryAccumulator::new();
        acc.update(&entries);
        flush_summary(&db, &acc);

        let from_summary = compute_stats(&db, OpenMode::Readonly).unwrap();
        assert_eq!(from_summary.total_entries, from_iter.total_entries);
        assert_eq!(from_summary.total_files, from_iter.total_files);
        assert_eq!(from_summary.total_bytes, from_iter.total_bytes);

        let ext_summary = stats_by_extension(&db, 100, OpenMode::Readonly).unwrap();
        let bin_summary = ext_summary.iter().find(|s| s.extension == "bin").unwrap();
        assert_eq!(bin_summary.count, bin.count);
        assert_eq!(bin_summary.total_bytes, bin.total_bytes);
    }
}
