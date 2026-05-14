//! Direct-write streaming Parquet writers.
//!
//! Variant of `parallel_convert` that consumes `Vec<DbEntry>` batches
//! from a crossbeam channel (one per shard) instead of iterating a
//! RocksDB column family. Used by the walker when
//! `--output-format parquet` is set, so entries flow:
//!
//! ```text
//! workers → ShardedSender → per-shard channel → per-shard Parquet writer
//!                                                  → part-rNN-SSSSS.parquet
//! ```
//!
//! Each writer owns one Arrow `RowBuilder`, one `ArrowWriter`, and
//! rotates to a new part file once the current one crosses
//! `target_file_size`. Row-group flush is triggered by row count.
//!
//! No RocksDB is involved. Resume/incremental rescan is unavailable in
//! this mode — that capability lives in `--output-format rocksdb`.
//!
//! See `tasks/todo.md` for the design and the motivating customer
//! benchmark (libnfs + DuckDB → 3 M files/sec with 32 parallel writers).

use crate::error::{ParquetError, WalkerError};
use crate::nfs::types::DbEntry;
use crate::parquet::builder::{RowBuilder, RowContext};
use crate::parquet::schema::parquet_schema_ref;
use crate::scanlog::ScanMetrics;
use arrow::datatypes::Schema;
use crossbeam_channel::{Receiver, Sender};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;
use tracing::{debug, info};

/// Per-shard summary returned to the spawning thread after the channel
/// closes. Aggregated into the run-wide `metadata.json` so the layout
/// matches the post-hoc converter (parallel_convert.rs).
#[derive(Debug, Default, Clone)]
pub struct ShardSummary {
    pub shard_index: usize,
    pub entries_written: u64,
    pub bytes_written: u64,
    /// Just the filenames (relative to `scans/<scan_id>/`), in the order
    /// they were emitted. The aggregator concats and sorts across shards.
    pub part_files: Vec<String>,
}

/// Configuration shared across the per-shard writer threads.
#[derive(Clone)]
pub struct DirectWriteConfig {
    /// Output destination — the writer creates `scans/<scan_id>/` under
    /// this directory.
    pub output_dir: PathBuf,
    /// Stable identifier for this scan. Embedded in every row and used
    /// to name the output sub-directory.
    pub scan_id: String,
    /// Microseconds-since-epoch timestamp for the scan start. Embedded
    /// in every row.
    pub scan_timestamp_us: i64,
    /// Number of writer shards (== channel count).
    pub shards: usize,
    /// Rows per row-group flush. `1_000_000` matches the post-hoc
    /// converter default.
    pub row_group_size: usize,
    /// File rotation threshold in bytes. The writer closes the current
    /// part once `bytes_written` crosses this value.
    pub target_file_size: usize,
    /// ZSTD level. `3` matches the post-hoc converter default; cheaper
    /// levels are an option if profiling shows compression CPU pinning
    /// the host.
    pub compression_level: i32,
}

impl Default for DirectWriteConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::new(),
            scan_id: String::new(),
            scan_timestamp_us: 0,
            shards: 1,
            row_group_size: 1_000_000,
            target_file_size: 256 * 1024 * 1024,
            compression_level: 3,
        }
    }
}

/// Result of spawning the direct-write writer pool.
///
/// `senders` is handed to the walker (`ShardedSender::new(senders, ..)`),
/// `joins` is held by the spawning thread for shutdown / summary collection,
/// and `scan_dir` is the resolved path where `metadata.json` will land.
#[allow(clippy::type_complexity)]
pub struct DirectWritePool {
    pub senders: Vec<Sender<Vec<DbEntry>>>,
    pub joins: Vec<JoinHandle<Result<ShardSummary, WalkerError>>>,
    pub scan_dir: PathBuf,
}

/// Spawn the per-shard streaming Parquet writer pool.
///
/// Mirrors `walker::simple::spawn_sharded_rocksdb_writers` but produces
/// Parquet files directly. The walker funnels `Vec<DbEntry>` batches
/// into `senders[shard]` via the existing `ShardedSender`; each writer
/// drains its channel and flushes row groups to its part files.
///
/// The output sub-directory `<output_dir>/scans/<scan_id>/` is created
/// here (not by the per-shard thread) so concurrent writers don't race
/// on `create_dir_all`. We refuse to spawn if the directory already
/// exists, mirroring the post-hoc converter's safety check.
pub fn spawn_direct_parquet_writers(
    config: DirectWriteConfig,
    metrics: Arc<ScanMetrics>,
) -> Result<DirectWritePool, WalkerError> {
    if config.shards == 0 {
        return Err(WalkerError::Parquet(ParquetError::Other(
            "direct-write requires shards >= 1".into(),
        )));
    }

    let scan_dir = config.output_dir.join("scans").join(&config.scan_id);
    if scan_dir.exists() {
        return Err(WalkerError::Parquet(ParquetError::Other(format!(
            "Refusing to scan: {} already exists. Use a different --output or delete the existing scan directory.",
            scan_dir.display()
        ))));
    }
    fs::create_dir_all(&scan_dir).map_err(ParquetError::Io)?;

    info!(
        "Direct-write Parquet output: {} (scan_id={}, shards={})",
        scan_dir.display(),
        config.scan_id,
        config.shards
    );

    let schema = parquet_schema_ref();
    let props = writer_properties(config.compression_level, config.row_group_size)?;
    // Wrapping properties in `Arc` so each writer thread can clone the
    // handle cheaply when rotating part files.
    let props = Arc::new(props);

    let mut senders: Vec<Sender<Vec<DbEntry>>> = Vec::with_capacity(config.shards);
    let mut joins: Vec<JoinHandle<Result<ShardSummary, WalkerError>>> =
        Vec::with_capacity(config.shards);

    for shard_idx in 0..config.shards {
        // Matches the RocksDB writer's channel sizing exactly so we
        // measure backend-only differences when comparing throughput.
        let (tx, rx) = crossbeam_channel::bounded::<Vec<DbEntry>>(1024);
        senders.push(tx);

        let scan_dir = scan_dir.clone();
        let schema = schema.clone();
        let props = Arc::clone(&props);
        let metrics = Arc::clone(&metrics);
        let row_group_size = config.row_group_size;
        let target_file_size = config.target_file_size;
        let row_ctx = RowContext {
            scan_id: config.scan_id.clone(),
            scan_timestamp_us: config.scan_timestamp_us,
            // DbEntry mtime/atime/ctime are already microseconds
            // (see nfs/types.rs); the seconds→micros multiplier only
            // applies to the RocksDB → Parquet post-hoc converter
            // reading legacy databases. Direct-write must use 1 or
            // every row gets scaled twice.
            mtime_scale: 1,
        };

        let join = thread::Builder::new()
            .name(format!("parquet-writer-{:02}", shard_idx))
            .spawn(move || {
                writer_loop(
                    shard_idx,
                    rx,
                    scan_dir,
                    schema,
                    props,
                    row_ctx,
                    row_group_size,
                    target_file_size,
                    metrics,
                )
            })
            .map_err(|e| {
                WalkerError::Parquet(ParquetError::Other(format!(
                    "Failed to spawn parquet writer {}: {}",
                    shard_idx, e
                )))
            })?;

        joins.push(join);
    }

    Ok(DirectWritePool {
        senders,
        joins,
        scan_dir,
    })
}

#[allow(clippy::too_many_arguments)]
fn writer_loop(
    shard_idx: usize,
    rx: Receiver<Vec<DbEntry>>,
    scan_dir: PathBuf,
    schema: Arc<Schema>,
    props: Arc<WriterProperties>,
    row_ctx: RowContext,
    row_group_size: usize,
    target_file_size: usize,
    metrics: Arc<ScanMetrics>,
) -> Result<ShardSummary, WalkerError> {
    debug!(
        "parquet writer {} started (row_group={}, target_file={}B)",
        shard_idx, row_group_size, target_file_size
    );

    let mut row_builder = RowBuilder::new(row_ctx);
    let mut part_seq: u32 = 0;
    let (mut writer, mut current_filename) =
        open_part_writer(&scan_dir, shard_idx, part_seq, &schema, &props)?;
    part_seq += 1;

    let mut summary = ShardSummary {
        shard_index: shard_idx,
        ..ShardSummary::default()
    };

    while let Ok(batch) = rx.recv() {
        // Empty batches are legal end-of-walker drains (ShardedSender
        // never sends one but we accept them defensively). Skip without
        // touching the writer.
        if batch.is_empty() {
            continue;
        }

        for entry in &batch {
            row_builder.push_db_entry(entry);

            // Row-group flush by count. Flushing here (inside the
            // per-batch loop) keeps memory bounded on a giant batch
            // and avoids accumulating row_group_size × shards entries
            // before the first write.
            if row_builder.row_count() >= row_group_size {
                flush_row_group(
                    shard_idx,
                    &mut row_builder,
                    &mut writer,
                    &metrics,
                    &mut summary,
                )?;

                // File rotation runs after the row group is written —
                // never mid-group, so the closed file always has a
                // valid footer.
                if writer.bytes_written() as usize >= target_file_size {
                    let (closed_bytes, closed_filename) = close_writer_take_name(
                        writer,
                        current_filename,
                    )?;
                    summary.bytes_written += closed_bytes;
                    summary.part_files.push(closed_filename);

                    let opened = open_part_writer(
                        &scan_dir,
                        shard_idx,
                        part_seq,
                        &schema,
                        &props,
                    )?;
                    writer = opened.0;
                    current_filename = opened.1;
                    part_seq += 1;
                }
            }
        }
    }

    // Channel closed: drain residual rows, close the final part.
    if !row_builder.is_empty() {
        flush_row_group(
            shard_idx,
            &mut row_builder,
            &mut writer,
            &metrics,
            &mut summary,
        )?;
    }

    let (closed_bytes, closed_filename) = close_writer_take_name(writer, current_filename)?;
    summary.bytes_written += closed_bytes;
    summary.part_files.push(closed_filename);

    debug!(
        "parquet writer {} finished: {} entries, {} bytes, {} part files",
        shard_idx,
        summary.entries_written,
        summary.bytes_written,
        summary.part_files.len()
    );
    Ok(summary)
}

fn flush_row_group(
    shard_idx: usize,
    row_builder: &mut RowBuilder,
    writer: &mut ArrowWriter<File>,
    metrics: &ScanMetrics,
    summary: &mut ShardSummary,
) -> Result<(), WalkerError> {
    let rows = row_builder.row_count() as u64;
    let batch = row_builder.finish()?;
    let t = Instant::now();
    writer.write(&batch).map_err(ParquetError::Parquet)?;
    metrics.record_write_latency(shard_idx, t.elapsed());
    summary.entries_written += rows;
    Ok(())
}

fn open_part_writer(
    scan_dir: &Path,
    shard_idx: usize,
    part_seq: u32,
    schema: &Arc<Schema>,
    props: &Arc<WriterProperties>,
) -> Result<(ArrowWriter<File>, String), WalkerError> {
    let filename = format!("part-r{:02}-{:05}.parquet", shard_idx, part_seq);
    let path: PathBuf = scan_dir.join(&filename);
    let file = File::create(&path).map_err(ParquetError::Io)?;
    let writer =
        ArrowWriter::try_new(file, schema.clone(), Some(props.as_ref().clone()))
            .map_err(ParquetError::Parquet)?;
    Ok((writer, filename))
}

/// Close a writer and return the (bytes_written, filename).
///
/// `ArrowWriter::close` consumes the writer and finalizes the footer,
/// so the caller must use the returned bytes count instead of probing
/// the closed handle.
fn close_writer_take_name(
    writer: ArrowWriter<File>,
    filename: String,
) -> Result<(u64, String), WalkerError> {
    let bytes = writer.bytes_written() as u64;
    writer.close().map_err(ParquetError::Parquet)?;
    Ok((bytes, filename))
}

/// Build the per-shard `WriterProperties`.
///
/// `max_row_group_size` is wired to our `row_group_size` so each
/// `RowBuilder::finish()` → `ArrowWriter::write` pair commits a row
/// group to disk immediately. The naive default (1 M) makes the
/// writer buffer rows internally until *it* hits 1 M, which silently
/// breaks our `bytes_written`-driven file rotation when the caller
/// uses a smaller row group.
fn writer_properties(
    compression_level: i32,
    row_group_size: usize,
) -> Result<WriterProperties, WalkerError> {
    let zstd_level = ZstdLevel::try_new(compression_level).map_err(|e| {
        WalkerError::Parquet(ParquetError::Other(format!(
            "Invalid ZSTD level {}: {}",
            compression_level, e
        )))
    })?;
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(zstd_level))
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
        .set_max_row_group_size(row_group_size.max(1))
        .build())
}

/// Merge per-shard summaries and write `<scan_dir>/metadata.json`.
///
/// Layout is intentionally identical to what the post-hoc converters
/// emit (`convert.rs`, `parallel_convert.rs`) so downstream
/// DuckDB / DataFusion / Polars readers don't care which path produced
/// the scan.
pub fn write_metadata_json(
    scan_dir: &Path,
    scan_id: &str,
    scan_timestamp_us: i64,
    source_url: &str,
    summaries: &[ShardSummary],
) -> Result<(u64, u64, Vec<String>), WalkerError> {
    let total_entries: u64 = summaries.iter().map(|s| s.entries_written).sum();
    let total_bytes: u64 = summaries.iter().map(|s| s.bytes_written).sum();

    let mut parquet_files: Vec<String> = summaries
        .iter()
        .flat_map(|s| s.part_files.iter().cloned())
        .collect();
    parquet_files.sort();

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
    Ok((total_entries, total_bytes, parquet_files))
}

/// Helper used by tests to assemble a config without typing every field.
#[cfg(test)]
pub(crate) fn test_config(output_dir: PathBuf, shards: usize) -> DirectWriteConfig {
    DirectWriteConfig {
        output_dir,
        scan_id: uuid::Uuid::new_v4().to_string(),
        scan_timestamp_us: 0,
        shards,
        row_group_size: 1_000_000,
        target_file_size: 256 * 1024 * 1024,
        compression_level: 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::{DbEntry, EntryType};
    use crate::scanlog::{CounterRefs, ScanMetrics};
    use std::sync::atomic::{AtomicU64, AtomicUsize};
    use tempfile::tempdir;

    fn entry(i: u64) -> DbEntry {
        DbEntry {
            parent_path: Some("/data".to_string()),
            name: format!("file-{:08}.bin", i),
            path: format!("/data/file-{:08}.bin", i),
            entry_type: EntryType::File,
            size: i,
            mtime: Some(1_700_000_000_000_000 + i as i64),
            mode: Some(0o644),
            uid: Some(1000),
            gid: Some(1000),
            nlink: Some(1),
            inode: i + 1,
            depth: 2,
            extension: Some("bin".to_string()),
            blocks: 8,
            ..Default::default()
        }
    }

    fn build_metrics(shards: usize) -> Arc<ScanMetrics> {
        ScanMetrics::new(
            1,
            shards,
            CounterRefs {
                dirs: Arc::new(AtomicU64::new(0)),
                files: Arc::new(AtomicU64::new(0)),
                bytes: Arc::new(AtomicU64::new(0)),
                errors: Arc::new(AtomicU64::new(0)),
                active_workers: Arc::new(AtomicUsize::new(0)),
            },
        )
    }

    #[test]
    fn direct_writer_writes_all_entries_single_shard() {
        let dir = tempdir().unwrap();
        let cfg = test_config(dir.path().to_path_buf(), 1);
        let metrics = build_metrics(1);
        let pool = spawn_direct_parquet_writers(cfg.clone(), metrics).unwrap();

        // Send 2 batches of 100 entries each → 200 rows, well under
        // the 1M row-group size.
        let batch1: Vec<DbEntry> = (0..100).map(entry).collect();
        let batch2: Vec<DbEntry> = (100..200).map(entry).collect();
        pool.senders[0].send(batch1).unwrap();
        pool.senders[0].send(batch2).unwrap();
        drop(pool.senders);

        let mut summaries = Vec::new();
        for h in pool.joins {
            summaries.push(h.join().unwrap().unwrap());
        }
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].entries_written, 200);
        assert_eq!(summaries[0].part_files.len(), 1);

        let (total, bytes, files) = write_metadata_json(
            &pool.scan_dir,
            &cfg.scan_id,
            cfg.scan_timestamp_us,
            "nfs://test/export",
            &summaries,
        )
        .unwrap();
        assert_eq!(total, 200);
        assert!(bytes > 0);
        assert_eq!(files.len(), 1);
        assert!(pool.scan_dir.join("metadata.json").exists());
    }

    #[test]
    fn direct_writer_handles_empty_scan() {
        let dir = tempdir().unwrap();
        let cfg = test_config(dir.path().to_path_buf(), 4);
        let metrics = build_metrics(4);
        let pool = spawn_direct_parquet_writers(cfg.clone(), metrics).unwrap();

        // Drop senders without ever sending — workers exit immediately,
        // each must still close a valid (empty) part file so metadata.json
        // reflects the layout that downstream readers expect.
        drop(pool.senders);

        let mut summaries = Vec::new();
        for h in pool.joins {
            summaries.push(h.join().unwrap().unwrap());
        }
        assert_eq!(summaries.len(), 4);
        for s in &summaries {
            assert_eq!(s.entries_written, 0);
            assert_eq!(s.part_files.len(), 1, "empty shard still emits one part");
        }

        let (total, _, files) = write_metadata_json(
            &pool.scan_dir,
            &cfg.scan_id,
            cfg.scan_timestamp_us,
            "",
            &summaries,
        )
        .unwrap();
        assert_eq!(total, 0);
        assert_eq!(files.len(), 4);
    }

    #[test]
    fn direct_writer_rotates_files_at_target_size() {
        let dir = tempdir().unwrap();
        // Force frequent rotation. ZSTD compresses the synthetic
        // uniform-ish paths aggressively, so we use a small
        // target_file_size and pump enough rows that even after
        // compression the disk position must cross it. Row group is
        // tiny too so the per-group flush actually checks file size
        // many times across the run.
        let cfg = DirectWriteConfig {
            output_dir: dir.path().to_path_buf(),
            scan_id: uuid::Uuid::new_v4().to_string(),
            scan_timestamp_us: 0,
            shards: 1,
            row_group_size: 200,
            target_file_size: 1024,
            // ZSTD level 1 — cheaper compression so the on-disk size
            // tracks the row count more linearly. The point of the
            // test is rotation correctness, not compression ratio.
            compression_level: 1,
        };
        let metrics = build_metrics(1);
        let pool = spawn_direct_parquet_writers(cfg.clone(), metrics).unwrap();

        // 50K entries with distinct content per row keep ZSTD from
        // collapsing the file to nothing. Without this the test ran
        // 10K identical-pattern rows into a single ~1.5KB file and
        // never rotated.
        let batch: Vec<DbEntry> = (0..50_000)
            .map(|i| {
                let mut e = entry(i);
                // Stir in a per-row salt so the path column doesn't
                // dictionary-encode into a handful of bytes.
                e.path = format!("/data/shard-{:x}/file-{:08x}.bin", i % 997, i);
                e.name = format!("file-{:08x}.bin", i);
                e
            })
            .collect();
        pool.senders[0].send(batch).unwrap();
        drop(pool.senders);

        let summary = pool.joins.into_iter().next().unwrap().join().unwrap().unwrap();
        assert_eq!(summary.entries_written, 50_000);
        assert!(
            summary.part_files.len() >= 2,
            "expected file rotation, got {} parts (bytes={})",
            summary.part_files.len(),
            summary.bytes_written
        );
    }

    #[test]
    fn direct_writer_rejects_existing_scan_dir() {
        let dir = tempdir().unwrap();
        let cfg = test_config(dir.path().to_path_buf(), 1);

        // Pre-create the target scan_dir.
        let scan_dir = cfg.output_dir.join("scans").join(&cfg.scan_id);
        fs::create_dir_all(&scan_dir).unwrap();

        let metrics = build_metrics(1);
        let result = spawn_direct_parquet_writers(cfg, metrics);
        assert!(result.is_err(), "must refuse to overwrite existing scan_dir");
    }
}

