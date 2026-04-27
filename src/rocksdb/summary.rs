//! Pre-computed summary aggregates for fast `nfs-walker stats` queries.
//!
//! Maintains in-memory running totals during ingest, periodically flushed
//! to a dedicated RocksDB column family (`CF_SUMMARY`). Stats functions
//! check the summary CF first and only fall back to full-scan iteration
//! when the keys are missing (e.g. databases written by an older binary).
//!
//! **Per-path semantics.** Counts include every directory entry, so
//! hardlinked files count once per name. The five stats functions that
//! consume this summary (`compute_stats`, `stats_by_extension`,
//! `stats_by_uid`, `stats_by_gid`, `stats_by_file_type`) iterate the
//! `entries_by_path` CF on fallback, guaranteeing summary-hit and
//! summary-miss return identical numbers regardless of hardlink
//! density.

use crate::nfs::types::{DbEntry, EntryType};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Key in CF_SUMMARY for the global totals struct.
pub const KEY_TOTAL: &[u8] = b"total";
/// Key in CF_SUMMARY for the by-extension map.
pub const KEY_BY_EXTENSION: &[u8] = b"by_extension";
/// Key in CF_SUMMARY for the by-UID map.
pub const KEY_BY_UID: &[u8] = b"by_uid";
/// Key in CF_SUMMARY for the by-GID map.
pub const KEY_BY_GID: &[u8] = b"by_gid";
/// Key in CF_SUMMARY for the by-file-type map.
pub const KEY_BY_FILE_TYPE: &[u8] = b"by_file_type";

/// Global totals across the entire scan.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SummaryTotal {
    pub total_entries: u64,
    pub total_files: u64,
    pub total_dirs: u64,
    pub total_symlinks: u64,
    /// Sum of file sizes (directories and other types do not contribute).
    pub total_bytes: u64,
    /// Sum of file allocated blocks (directories and other types skipped).
    pub total_blocks: u64,
    pub max_depth: u32,
    /// Microseconds-since-epoch of the last accumulator->CF flush.
    pub last_updated_us: i64,
}

/// Counters accumulated per file extension (only files contribute).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExtCounters {
    pub count: u64,
    pub bytes: u64,
    pub blocks: u64,
}

/// Counters accumulated per owner (UID or GID).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OwnerCounters {
    pub file_count: u64,
    pub dir_count: u64,
    pub bytes: u64,
}

/// Counters accumulated per detected MIME type (only files contribute).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileTypeCounters {
    pub count: u64,
    pub bytes: u64,
}

/// In-memory accumulator updated on each writer batch.
///
/// `update()` is cheap (HashMap-backed under the hood would be faster but
/// we use BTreeMap for deterministic serialization). `serialize_kv()`
/// returns the five (key, value) pairs to flush via WriteBatch -- no disk
/// sync, just memtable writes. Visibility to RocksDB secondary readers
/// requires the periodic full `db.flush()` in the writer loop, which is
/// independent of this accumulator's flush cadence.
#[derive(Debug, Clone, Default)]
pub struct SummaryAccumulator {
    pub total: SummaryTotal,
    pub by_extension: BTreeMap<String, ExtCounters>,
    pub by_uid: BTreeMap<u32, OwnerCounters>,
    pub by_gid: BTreeMap<u32, OwnerCounters>,
    pub by_file_type: BTreeMap<String, FileTypeCounters>,
}

impl SummaryAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold a batch of entries into the running totals.
    pub fn update(&mut self, entries: &[DbEntry]) {
        for entry in entries {
            self.total.total_entries += 1;
            if entry.depth > self.total.max_depth {
                self.total.max_depth = entry.depth;
            }

            let uid = entry.uid.unwrap_or(0);
            let gid = entry.gid.unwrap_or(0);

            match entry.entry_type {
                EntryType::File => {
                    self.total.total_files += 1;
                    self.total.total_bytes += entry.size;
                    self.total.total_blocks += entry.blocks;

                    let ext = entry.extension.clone().unwrap_or_default();
                    let ext_c = self.by_extension.entry(ext).or_default();
                    ext_c.count += 1;
                    ext_c.bytes += entry.size;
                    ext_c.blocks += entry.blocks;

                    let uid_c = self.by_uid.entry(uid).or_default();
                    uid_c.file_count += 1;
                    uid_c.bytes += entry.size;

                    let gid_c = self.by_gid.entry(gid).or_default();
                    gid_c.file_count += 1;
                    gid_c.bytes += entry.size;

                    let ft = entry
                        .file_type
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    let ft_c = self.by_file_type.entry(ft).or_default();
                    ft_c.count += 1;
                    ft_c.bytes += entry.size;
                }
                EntryType::Directory => {
                    self.total.total_dirs += 1;

                    let uid_c = self.by_uid.entry(uid).or_default();
                    uid_c.dir_count += 1;

                    let gid_c = self.by_gid.entry(gid).or_default();
                    gid_c.dir_count += 1;
                }
                EntryType::Symlink => {
                    self.total.total_symlinks += 1;
                }
                _ => {}
            }
        }
    }

    /// Stamp the accumulator's `last_updated_us` to the current wall-clock.
    pub fn touch_now(&mut self) {
        self.total.last_updated_us = chrono::Utc::now().timestamp_micros();
    }

    /// Serialize the five summary keys ready for a single WriteBatch flush.
    pub fn serialize_kv(&self) -> Result<Vec<(&'static [u8], Vec<u8>)>, bincode::Error> {
        Ok(vec![
            (KEY_TOTAL, bincode::serialize(&self.total)?),
            (KEY_BY_EXTENSION, bincode::serialize(&self.by_extension)?),
            (KEY_BY_UID, bincode::serialize(&self.by_uid)?),
            (KEY_BY_GID, bincode::serialize(&self.by_gid)?),
            (KEY_BY_FILE_TYPE, bincode::serialize(&self.by_file_type)?),
        ])
    }
}

/// Snapshot loaded from `CF_SUMMARY` for the reader-side fast path.
///
/// Loaded with `try_load(handle)`. Returns `Ok(None)` when the CF is
/// missing entirely (legacy DB) so callers fall back to iteration.
#[derive(Debug, Clone, Default)]
pub struct SummaryReader {
    pub total: SummaryTotal,
    pub by_extension: BTreeMap<String, ExtCounters>,
    pub by_uid: BTreeMap<u32, OwnerCounters>,
    pub by_gid: BTreeMap<u32, OwnerCounters>,
    pub by_file_type: BTreeMap<String, FileTypeCounters>,
}

impl SummaryReader {
    /// Try to load all five summary keys from the given handle. Returns
    /// `Ok(None)` if the `summary` CF is absent (legacy DB) and an Err
    /// for read or decode failures. Missing individual keys default to
    /// empty -- a partially-populated CF is treated as "no summary".
    pub fn try_load(
        handle: &super::schema::RocksHandle,
    ) -> Result<Option<Self>, crate::error::RocksError> {
        use crate::error::RocksError;

        let cf = match handle.cf_summary() {
            Some(cf) => cf,
            None => return Ok(None),
        };

        let total_bytes = handle
            .db
            .get_cf(cf, KEY_TOTAL)
            .map_err(RocksError::Rocks)?;

        // No total key means the writer never flushed -- treat as missing.
        let total: SummaryTotal = match total_bytes {
            Some(b) => bincode::deserialize(&b).map_err(|e| RocksError::Bincode(e.to_string()))?,
            None => return Ok(None),
        };

        let load_map = |key: &[u8]| -> Result<Option<Vec<u8>>, RocksError> {
            handle.db.get_cf(cf, key).map_err(RocksError::Rocks)
        };

        let by_extension = match load_map(KEY_BY_EXTENSION)? {
            Some(b) => bincode::deserialize(&b).map_err(|e| RocksError::Bincode(e.to_string()))?,
            None => BTreeMap::new(),
        };
        let by_uid = match load_map(KEY_BY_UID)? {
            Some(b) => bincode::deserialize(&b).map_err(|e| RocksError::Bincode(e.to_string()))?,
            None => BTreeMap::new(),
        };
        let by_gid = match load_map(KEY_BY_GID)? {
            Some(b) => bincode::deserialize(&b).map_err(|e| RocksError::Bincode(e.to_string()))?,
            None => BTreeMap::new(),
        };
        let by_file_type = match load_map(KEY_BY_FILE_TYPE)? {
            Some(b) => bincode::deserialize(&b).map_err(|e| RocksError::Bincode(e.to_string()))?,
            None => BTreeMap::new(),
        };

        Ok(Some(Self {
            total,
            by_extension,
            by_uid,
            by_gid,
            by_file_type,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::EntryType;

    fn file_entry(path: &str, size: u64, ext: Option<&str>, uid: u32, ft: Option<&str>) -> DbEntry {
        DbEntry {
            parent_path: Some("/".to_string()),
            name: path.trim_start_matches('/').to_string(),
            path: path.to_string(),
            entry_type: EntryType::File,
            size,
            mtime: None,
            atime: None,
            ctime: None,
            mode: Some(0o644),
            uid: Some(uid),
            gid: Some(uid),
            nlink: Some(1),
            inode: 0,
            depth: 1,
            extension: ext.map(String::from),
            blocks: size.div_ceil(512),
            checksum: None,
            file_type: ft.map(String::from),
        }
    }

    fn dir_entry(path: &str, depth: u32, uid: u32) -> DbEntry {
        DbEntry {
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
            inode: 0,
            depth,
            extension: None,
            blocks: 8,
            checksum: None,
            file_type: None,
        }
    }

    #[test]
    fn accumulator_counts_by_dimension() {
        let mut acc = SummaryAccumulator::new();
        acc.update(&[
            file_entry("/a.txt", 100, Some("txt"), 1000, Some("text/plain")),
            file_entry("/b.txt", 200, Some("txt"), 1000, Some("text/plain")),
            file_entry("/c.bin", 50, Some("bin"), 2000, None),
            dir_entry("/sub", 1, 1000),
            dir_entry("/sub/deep", 2, 1000),
        ]);

        assert_eq!(acc.total.total_entries, 5);
        assert_eq!(acc.total.total_files, 3);
        assert_eq!(acc.total.total_dirs, 2);
        assert_eq!(acc.total.total_bytes, 350);
        assert_eq!(acc.total.max_depth, 2);

        assert_eq!(acc.by_extension.get("txt").unwrap().count, 2);
        assert_eq!(acc.by_extension.get("txt").unwrap().bytes, 300);
        assert_eq!(acc.by_extension.get("bin").unwrap().count, 1);

        assert_eq!(acc.by_uid.get(&1000).unwrap().file_count, 2);
        assert_eq!(acc.by_uid.get(&1000).unwrap().dir_count, 2);
        assert_eq!(acc.by_uid.get(&1000).unwrap().bytes, 300);
        assert_eq!(acc.by_uid.get(&2000).unwrap().file_count, 1);

        assert_eq!(acc.by_file_type.get("text/plain").unwrap().count, 2);
        assert_eq!(acc.by_file_type.get("unknown").unwrap().count, 1);
    }

    #[test]
    fn accumulator_round_trip_via_serialize_kv() {
        let mut acc = SummaryAccumulator::new();
        acc.update(&[
            file_entry("/a.txt", 100, Some("txt"), 1000, Some("text/plain")),
            dir_entry("/sub", 1, 1000),
        ]);
        acc.touch_now();

        let kv = acc.serialize_kv().unwrap();
        assert_eq!(kv.len(), 5);

        // Decode each blob back and verify equality.
        let total: SummaryTotal = bincode::deserialize(&kv[0].1).unwrap();
        assert_eq!(total.total_entries, 2);
        assert_eq!(total.total_files, 1);
        assert!(total.last_updated_us > 0);

        let ext: BTreeMap<String, ExtCounters> = bincode::deserialize(&kv[1].1).unwrap();
        assert_eq!(ext.get("txt").unwrap().count, 1);

        let uids: BTreeMap<u32, OwnerCounters> = bincode::deserialize(&kv[2].1).unwrap();
        assert_eq!(uids.get(&1000).unwrap().file_count, 1);
        assert_eq!(uids.get(&1000).unwrap().dir_count, 1);
    }
}
