//! Parallel RocksDB → Parquet conversion.
//!
//! The single-threaded `convert_rocks_to_parquet` is bottlenecked on the
//! one iterator + one ZSTD encoder per process — fine on small databases,
//! a 99% CPU waste on large ones.
//!
//! This module shards the path-CF keyspace by SST file boundaries. Each
//! shard gets its own bounded iterator and its own Parquet writer, so a
//! 64-way export on a 160-core box pegs all the cores it actually needs.
//!
//! ## Output layout
//!
//! Files land at `<output_dir>/scans/<scan_id>/part-rNN-SSSSS.parquet`
//! where `NN` is the shard rank and `SSSSS` is the sequence within the
//! shard (writers split on file size just like the single-threaded path).
//! `metadata.json` lists the union of all files so DuckDB / DataFusion
//! see one logical scan.
//!
//! ## Shard balancing
//!
//! `db.live_files()` exposes per-SST size + start/end keys. We sort the
//! path-CF SSTs by start key and walk them left-to-right, picking split
//! points whenever cumulative size crosses `total_size / N`. The split
//! points become `set_iterate_lower_bound` / `set_iterate_upper_bound`
//! values for the per-shard iterators, so RocksDB transparently handles
//! cross-level SST overlap — each user-key is emitted by exactly one
//! shard.
//!
//! ## Consistency
//!
//! Reads go through `RocksHandle::open_readonly`, which pins the manifest
//! at open time. No concurrent writers can disturb the export.

use crate::error::{ParquetError, WalkerError};
use crate::parquet::builder::{RowBuilder, RowContext};
use crate::parquet::convert::ExportStats;
use crate::parquet::schema::{parquet_schema_ref, seconds_to_microseconds};
use crate::rocksdb::schema::{cf_name_for_path_shard, meta_keys, RocksHandle};

/// Progress callback for the parallel exporter.
///
/// Distinct from `convert::ProgressCallback` (which is `Box<dyn Fn + Send>`,
/// fine for single-threaded use). The parallel exporter shares one
/// callback across N worker threads, so it needs to be both `Send` and
/// `Sync` and held in an `Arc`.
pub type ParallelProgressCallback = Arc<dyn Fn(u64, u64) + Send + Sync>;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rocksdb::ReadOptions;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use tracing::{info, warn};
use uuid::Uuid;

/// Configuration for parallel Parquet export.
pub struct ParallelExportConfig {
    /// Number of shards (worker threads). 0 → auto-detect num_cpus.
    pub parallelism: usize,
    /// Rows per Arrow row group / Parquet row group.
    pub row_group_size: usize,
    /// Target file size before splitting to a new part within a shard.
    pub target_file_size: usize,
    /// ZSTD compression level (1-22). The repo default is 3.
    pub compression_level: i32,
    /// Emit periodic progress updates via the callback.
    pub progress: bool,
}

impl Default for ParallelExportConfig {
    fn default() -> Self {
        Self {
            parallelism: 0,
            row_group_size: 1_000_000,
            target_file_size: 256 * 1024 * 1024,
            compression_level: 3,
            progress: true,
        }
    }
}

/// Convert a RocksDB scan to Parquet using N parallel shards.
///
/// On success returns aggregate stats covering all shards. The output
/// `metadata.json` lists every part file so a DataFusion / DuckDB
/// query sees one unified table.
pub fn parallel_convert_rocks_to_parquet<P1, P2>(
    rocks_path: P1,
    output_dir: P2,
    config: ParallelExportConfig,
    progress_callback: Option<ParallelProgressCallback>,
) -> Result<ExportStats, WalkerError>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let rocks_path = rocks_path.as_ref();
    let output_dir = output_dir.as_ref();

    let shard_count = if config.parallelism == 0 {
        num_cpus::get()
    } else {
        config.parallelism
    }
    .max(1);

    info!(
        "Opening RocksDB read-only for parallel export: {} (shards={})",
        rocks_path.display(),
        shard_count
    );

    let rocks = Arc::new(RocksHandle::open_readonly(rocks_path).map_err(|e| {
        WalkerError::Parquet(ParquetError::Other(format!(
            "Failed to open RocksDB: {}",
            e
        )))
    })?);

    // Reuse the persisted scan_id when present (consistent with the
    // single-threaded converter).
    let scan_id = rocks
        .get_metadata(meta_keys::SCAN_ID)
        .ok()
        .flatten()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let scan_timestamp_us = rocks
        .get_metadata(meta_keys::START_TIME)
        .ok()
        .flatten()
        .and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(&s)
                .ok()
                .map(|dt| seconds_to_microseconds(dt.timestamp()))
                .or_else(|| s.parse::<i64>().ok().map(seconds_to_microseconds))
        })
        .unwrap_or(0);

    let source_url = rocks
        .get_metadata(meta_keys::SOURCE_URL)
        .ok()
        .flatten()
        .unwrap_or_default();

    let scan_dir = output_dir.join("scans").join(&scan_id);
    if scan_dir.exists() {
        return Err(WalkerError::Parquet(ParquetError::Other(format!(
            "Refusing to convert: {} already exists. Delete it (or pass a different output_dir) \
             before re-running.",
            scan_dir.display()
        ))));
    }
    fs::create_dir_all(&scan_dir).map_err(ParquetError::Io)?;

    info!(
        "Parallel exporting to Parquet: {} (scan_id={}, shards={})",
        scan_dir.display(),
        scan_id,
        shard_count
    );

    // Compute the per-task work plan. For a single-CF DB this is just a
    // list of key ranges over that CF; for a multi-CF (sharded) DB we
    // compute SST-balanced ranges per CF and spread the requested
    // parallelism across them.
    let tasks = compute_export_tasks(&rocks, shard_count)?;
    let actual_tasks = tasks.len();
    info!(
        "Computed {} export tasks across {} path-CF shard(s) (requested parallelism {})",
        actual_tasks,
        rocks.shards(),
        shard_count
    );

    let abort = Arc::new(AtomicBool::new(false));
    let total_exported = Arc::new(AtomicU64::new(0));
    let last_progress_emit = Arc::new(Mutex::new(Instant::now()));

    let row_ctx = RowContext {
        scan_id: scan_id.clone(),
        scan_timestamp_us,
        mtime_scale: crate::parquet::convert::detect_mtime_scale(&rocks)?,
    };

    let mut handles = Vec::with_capacity(actual_tasks);
    for (rank, task) in tasks.into_iter().enumerate() {
        let rocks = Arc::clone(&rocks);
        let scan_dir = scan_dir.clone();
        let abort = Arc::clone(&abort);
        let total = Arc::clone(&total_exported);
        let last_emit = Arc::clone(&last_progress_emit);
        let row_ctx = row_ctx.clone();
        let row_group_size = config.row_group_size;
        let target_file_size = config.target_file_size;
        let compression_level = config.compression_level;
        let cb_opt = progress_callback.clone();
        let progress_enabled = config.progress;

        let h = thread::Builder::new()
            .name(format!("parquet-shard-{:02}", rank))
            .spawn(move || {
                run_shard(
                    rank,
                    rocks,
                    &scan_dir,
                    task,
                    row_ctx,
                    row_group_size,
                    target_file_size,
                    compression_level,
                    abort,
                    total,
                    last_emit,
                    cb_opt,
                    progress_enabled,
                )
            })
            .map_err(|e| {
                WalkerError::Parquet(ParquetError::Other(format!(
                    "Failed to spawn shard {}: {}",
                    rank, e
                )))
            })?;

        handles.push(h);
    }

    // Join all shards. If any fails, set abort so others can stop early.
    let mut shard_files: u32 = 0;
    let mut first_err: Option<WalkerError> = None;
    for (rank, h) in handles.into_iter().enumerate() {
        match h.join() {
            Ok(Ok(n)) => shard_files += n,
            Ok(Err(e)) => {
                abort.store(true, Ordering::SeqCst);
                if first_err.is_none() {
                    first_err = Some(e);
                }
                warn!("Shard {} failed", rank);
            }
            Err(_) => {
                abort.store(true, Ordering::SeqCst);
                if first_err.is_none() {
                    first_err = Some(WalkerError::Parquet(ParquetError::Other(format!(
                        "Shard {} panicked",
                        rank
                    ))));
                }
            }
        }
    }

    if let Some(e) = first_err {
        return Err(e);
    }

    let total = total_exported.load(Ordering::SeqCst);
    let total_bytes = recalculate_total_bytes(&scan_dir);

    if let Some(ref cb) = progress_callback {
        cb(total, total);
    }
    drop(progress_callback);

    let parquet_files = list_parquet_files(&scan_dir);
    write_metadata_json(
        &scan_dir,
        &scan_id,
        scan_timestamp_us,
        &source_url,
        total,
        &parquet_files,
    )?;

    info!(
        "Parallel export complete: {} entries in {} files ({} bytes) across {} tasks",
        total, shard_files, total_bytes, actual_tasks
    );

    Ok(ExportStats {
        entries_exported: total,
        files_written: shard_files,
        total_bytes_written: total_bytes,
        scan_id,
    })
}

/// One unit of export work — a (path-CF shard index, key range) pair.
///
/// Output files for this task land at `part-rNN-SSSSS.parquet` where
/// `NN` is the global rank assigned at spawn time.
#[derive(Clone)]
struct ExportTask {
    /// Index of the path-CF shard this task reads from. 0 for legacy
    /// single-CF DBs.
    cf_shard: usize,
    /// Inclusive lower bound, or `None` for the start of the CF.
    lower: Option<Vec<u8>>,
    /// Exclusive upper bound, or `None` for the end of the CF.
    upper: Option<Vec<u8>>,
}

/// Per-task worker: opens a bounded iterator over the supplied key
/// range (within one path-CF shard) and writes one or more
/// `part-rNN-SSSSS.parquet` files.
///
/// Returns the number of part files this task produced.
#[allow(clippy::too_many_arguments)]
fn run_shard(
    rank: usize,
    rocks: Arc<RocksHandle>,
    scan_dir: &Path,
    task: ExportTask,
    row_ctx: RowContext,
    row_group_size: usize,
    target_file_size: usize,
    compression_level: i32,
    abort: Arc<AtomicBool>,
    total_exported: Arc<AtomicU64>,
    last_progress_emit: Arc<Mutex<Instant>>,
    progress_callback: Option<ParallelProgressCallback>,
    progress_enabled: bool,
) -> Result<u32, WalkerError> {
    let props = writer_properties(compression_level)?;
    let schema = parquet_schema_ref();

    let mut row_builder = RowBuilder::new(row_ctx);
    let mut part_seq: u32 = 0;
    let mut writer = open_part_writer(scan_dir, rank, part_seq, &schema, props.clone())?;
    part_seq += 1;
    let mut shard_total: u64 = 0;
    let mut since_last_progress: u64 = 0;

    // Build ReadOptions with the task's bounds. Note these own their
    // bound buffers — we keep ReadOptions alive for the full iteration.
    let mut read_opts = ReadOptions::default();
    if let Some(lower) = task.lower.as_ref() {
        read_opts.set_iterate_lower_bound(lower.clone());
    }
    if let Some(upper) = task.upper.as_ref() {
        read_opts.set_iterate_upper_bound(upper.clone());
    }

    let cf = rocks.cf_entries_by_path_shard(task.cf_shard);
    let iter = rocks
        .db
        .iterator_cf_opt(cf, read_opts, rocksdb::IteratorMode::Start);

    for kv in iter {
        if abort.load(Ordering::Relaxed) {
            return Err(WalkerError::Parquet(ParquetError::Other(format!(
                "shard {} aborted by peer failure",
                rank
            ))));
        }

        let (_key, value) = kv.map_err(|e| {
            WalkerError::Parquet(ParquetError::Other(format!(
                "shard {} iterator error: {}",
                rank, e
            )))
        })?;

        let entry =
            crate::rocksdb::schema::RocksEntry::from_bytes(&value).map_err(|e| {
                WalkerError::Parquet(ParquetError::Other(format!(
                    "shard {} bincode error: {}",
                    rank, e
                )))
            })?;

        row_builder.push_rocks_entry(&entry);

        if row_builder.row_count() >= row_group_size {
            let rows = row_builder.row_count();
            let batch = row_builder.finish()?;
            writer.write(&batch).map_err(ParquetError::Parquet)?;
            shard_total += rows as u64;
            since_last_progress += rows as u64;

            // Bytes-based file split, mirroring the single-threaded path.
            if writer.bytes_written() as usize >= target_file_size {
                writer.close().map_err(ParquetError::Parquet)?;
                writer =
                    open_part_writer(scan_dir, rank, part_seq, &schema, props.clone())?;
                part_seq += 1;
            }
        }

        if since_last_progress >= 100_000 {
            let global = total_exported.fetch_add(since_last_progress, Ordering::Relaxed)
                + since_last_progress;
            since_last_progress = 0;

            // Throttle progress callbacks to ~1/sec across all shards.
            if progress_enabled {
                if let Some(ref cb) = progress_callback {
                    let mut guard = last_progress_emit.lock().unwrap();
                    if guard.elapsed() >= std::time::Duration::from_secs(1) {
                        cb(global, 0);
                        *guard = Instant::now();
                    }
                }
            }
        }
    }

    if !row_builder.is_empty() {
        let rows = row_builder.row_count();
        let batch = row_builder.finish()?;
        writer.write(&batch).map_err(ParquetError::Parquet)?;
        shard_total += rows as u64;
        since_last_progress += rows as u64;
    }

    writer.close().map_err(ParquetError::Parquet)?;

    if since_last_progress > 0 {
        total_exported.fetch_add(since_last_progress, Ordering::Relaxed);
    }

    info!(
        "shard {:02} (cf_shard={}) complete: {} entries in {} part files",
        rank, task.cf_shard, shard_total, part_seq
    );

    Ok(part_seq)
}

/// Plan one task per (path-CF shard, key range). For a single-CF DB
/// this is just the legacy `compute_shard_ranges` output bound to
/// shard 0. For an N-shard DB we compute SST-balanced ranges per CF
/// using a per-CF target parallelism of roughly `requested / N`,
/// floor-clamped to 1, so every CF gets at least one task.
fn compute_export_tasks(
    rocks: &RocksHandle,
    requested: usize,
) -> Result<Vec<ExportTask>, WalkerError> {
    let n_cf = rocks.shards().max(1);
    let mut tasks = Vec::new();

    // How much parallelism to allocate per CF. With requested=64 and
    // n_cf=8: 8 tasks per CF, 64 total. Floor clamp to 1 so a low
    // requested value still yields one task per CF.
    let per_cf = (requested / n_cf).max(1);

    let live = rocks.db.live_files().map_err(|e| {
        WalkerError::Parquet(ParquetError::Other(format!("Failed to list SSTs: {}", e)))
    })?;

    for cf_shard in 0..n_cf {
        let cf_name = cf_name_for_path_shard(cf_shard, n_cf);
        let cf_live: Vec<&rocksdb::LiveFile> = live
            .iter()
            .filter(|f| f.column_family_name == cf_name)
            .filter(|f| f.start_key.is_some() && f.end_key.is_some())
            .collect();

        let cf_tasks = compute_per_cf_ranges(&cf_live, per_cf);
        for (lower, upper) in cf_tasks {
            tasks.push(ExportTask {
                cf_shard,
                lower,
                upper,
            });
        }
    }

    if tasks.is_empty() {
        // Empty DB or all CFs missing SST metadata. Emit one full-range
        // task per CF so we still iterate them all (cheap when empty).
        for cf_shard in 0..n_cf {
            tasks.push(ExportTask {
                cf_shard,
                lower: None,
                upper: None,
            });
        }
    }
    Ok(tasks)
}

/// Compute SST-balanced (lower, upper) ranges for a single path-CF
/// shard's live files. Returns at least one entry. The legacy
/// SST-walking algorithm — pick split points whenever cumulative size
/// crosses `total_size / requested`.
fn compute_per_cf_ranges(
    sst_refs: &[&rocksdb::LiveFile],
    requested: usize,
) -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> {
    if requested <= 1 || sst_refs.is_empty() {
        return vec![(None, None)];
    }

    let mut sorted: Vec<&rocksdb::LiveFile> = sst_refs.to_vec();
    sorted.sort_by(|a, b| {
        a.start_key
            .as_ref()
            .unwrap()
            .cmp(b.start_key.as_ref().unwrap())
    });

    let total_size: u64 = sorted.iter().map(|f| f.size as u64).sum();
    if total_size == 0 {
        return vec![(None, None)];
    }

    let target = total_size / requested as u64;
    let mut splits: Vec<Vec<u8>> = Vec::with_capacity(requested.saturating_sub(1));
    let mut cumulative: u64 = 0;
    let mut next_threshold = target;
    let mut prev_split: Option<&Vec<u8>> = None;

    for sst in &sorted {
        cumulative += sst.size as u64;
        if cumulative >= next_threshold && splits.len() + 1 < requested {
            let start = sst.start_key.as_ref().unwrap();
            if prev_split.is_none_or(|p| p != start) {
                splits.push(start.clone());
                prev_split = splits.last();
                next_threshold += target;
            }
        }
    }

    let mut ranges = Vec::with_capacity(splits.len() + 1);
    let mut prev_lower: Option<Vec<u8>> = None;
    for sp in splits {
        ranges.push((prev_lower.clone(), Some(sp.clone())));
        prev_lower = Some(sp);
    }
    ranges.push((prev_lower, None));
    ranges
}

fn writer_properties(compression_level: i32) -> Result<WriterProperties, WalkerError> {
    let zstd_level = ZstdLevel::try_new(compression_level).map_err(|e| {
        WalkerError::Parquet(ParquetError::Other(format!(
            "Invalid ZSTD level {}: {}",
            compression_level, e
        )))
    })?;

    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level))
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .set_max_row_group_size(1_000_000)
        .build())
}

fn open_part_writer(
    scan_dir: &Path,
    rank: usize,
    part_seq: u32,
    schema: &Arc<arrow::datatypes::Schema>,
    props: WriterProperties,
) -> Result<ArrowWriter<File>, WalkerError> {
    let filename = format!("part-r{:02}-{:05}.parquet", rank, part_seq);
    let path: PathBuf = scan_dir.join(&filename);
    let file = File::create(&path).map_err(ParquetError::Io)?;
    let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .map_err(ParquetError::Parquet)?;
    Ok(writer)
}

fn list_parquet_files(dir: &Path) -> Vec<String> {
    let mut files: Vec<String> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".parquet") {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    files.sort();
    files
}

fn recalculate_total_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.metadata().ok().map(|m| m.len()))
        .sum()
}

fn write_metadata_json(
    scan_dir: &Path,
    scan_id: &str,
    scan_timestamp_us: i64,
    source_url: &str,
    total_entries: u64,
    parquet_files: &[String],
) -> Result<(), WalkerError> {
    let metadata = serde_json::json!({
        "scan_id": scan_id,
        "scan_timestamp_us": scan_timestamp_us,
        "source_url": source_url,
        "total_entries": total_entries,
        "parquet_files": parquet_files,
    });

    let path = scan_dir.join("metadata.json");
    let file = File::create(&path).map_err(ParquetError::Io)?;
    serde_json::to_writer_pretty(file, &metadata).map_err(ParquetError::Json)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::{DbEntry, EntryType};
    use crate::rocksdb::writer::{RocksWriter, RocksWriterConfig};
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn make_test_rocks(dir: &Path, n_entries: usize) -> PathBuf {
        let rocks_path = dir.join("test.rocks");
        let writer = RocksWriter::open(&rocks_path, RocksWriterConfig::default()).unwrap();

        let entries: Vec<DbEntry> = (0..n_entries)
            .map(|i| DbEntry {
                parent_path: Some("/data".to_string()),
                name: format!("file-{:08}.bin", i),
                path: format!("/data/file-{:08}.bin", i),
                entry_type: EntryType::File,
                size: i as u64,
                mtime: Some(1_700_000_000 + i as i64),
                mode: Some(0o644),
                uid: Some(1000),
                gid: Some(1000),
                nlink: Some(1),
                inode: i as u64 + 1,
                depth: 2,
                extension: Some("bin".to_string()),
                ..Default::default()
            })
            .collect();

        writer.write_batch(&entries).unwrap();

        writer
            .set_metadata(meta_keys::SOURCE_URL, "nfs://test.local/export")
            .unwrap();
        writer
            .set_metadata(
                meta_keys::START_TIME,
                &chrono::Utc::now().to_rfc3339(),
            )
            .unwrap();

        // Force a flush so live_files() sees actual SSTs (not just memtable).
        writer.handle().db.flush().unwrap();
        drop(writer);

        rocks_path
    }

    #[test]
    fn parallel_export_writes_all_entries() {
        let work = tempdir().unwrap();
        let rocks_path = make_test_rocks(work.path(), 5_000);
        let out = work.path().join("out");

        let cfg = ParallelExportConfig {
            parallelism: 4,
            row_group_size: 200,
            ..Default::default()
        };
        let stats = parallel_convert_rocks_to_parquet(&rocks_path, &out, cfg, None).unwrap();
        assert_eq!(stats.entries_exported, 5_000);
        assert!(stats.files_written >= 1);
    }

    #[test]
    fn parallel_export_falls_back_to_single_shard_on_empty_db() {
        let work = tempdir().unwrap();
        let rocks_path = make_test_rocks(work.path(), 0);
        let out = work.path().join("out");

        let cfg = ParallelExportConfig {
            parallelism: 8,
            row_group_size: 100,
            ..Default::default()
        };
        let stats = parallel_convert_rocks_to_parquet(&rocks_path, &out, cfg, None).unwrap();
        assert_eq!(stats.entries_exported, 0);
    }

    #[test]
    fn parallel_export_shard_count_one_matches_full_range() {
        let work = tempdir().unwrap();
        let rocks_path = make_test_rocks(work.path(), 1_000);
        let out = work.path().join("out");

        let cfg = ParallelExportConfig {
            parallelism: 1,
            row_group_size: 200,
            ..Default::default()
        };
        let stats = parallel_convert_rocks_to_parquet(&rocks_path, &out, cfg, None).unwrap();
        assert_eq!(stats.entries_exported, 1_000);
    }
}
