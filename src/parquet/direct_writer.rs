//! Streaming Parquet writers fed directly from the walker pipeline.
//!
//! Consumes `Vec<DbEntry>` batches from a crossbeam channel (one per
//! shard). Entries flow:
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
//! No incremental rescan / resume capability — a crashed scan starts
//! over. The throughput win — 690 K files/sec on the 810 M-file prod
//! bench — is the trade.
//!
//! # Failure handling
//!
//! Each in-flight part file is owned by an `InProgressPart` RAII guard.
//! If a writer thread returns `Err` (e.g. `flush_row_group` fails or
//! `ArrowWriter::close` fails on footer write), the guard's `Drop`
//! removes the partial file from disk before the thread exits. The
//! caller (`run_parquet`) skips `write_metadata_json` whenever any
//! shard returns `Err`, so the scan-dir cannot end up containing
//! footer-less part files plus a `metadata.json` that lies about
//! completeness.
//!
//! Caveat: `Cargo.toml` sets `panic = "abort"` on the release profile,
//! so a panic inside the writer thread terminates the process before
//! `Drop` runs. Under abort, a partial part file may persist as
//! `scans/<uuid>/part-rNN-SSSSS.parquet`, but `metadata.json` is also
//! absent (it's written from the main thread only after every shard
//! has joined cleanly) — the artifacts are orphan files, not lying
//! summaries. `dev` builds (and any build switched to `panic =
//! "unwind"`) get full panic cleanup via `Drop`.

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
use tracing::{debug, info, warn};

/// Compression algorithm choice for the direct-write Parquet pipeline.
///
/// Mid-scan write latency was effectively zero at ZSTD-3 on our 580K/s
/// bench, but end-of-scan tail-flush wall-clock was 50s+ as 32 shards
/// simultaneously encoded their final row groups. The tunable matters
/// less for steady-state throughput than for the tail.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ParquetCompression {
    Zstd(i32),
    Snappy,
    Lz4Raw,
    None,
}

impl ParquetCompression {
    fn to_parquet(self) -> Result<Compression, WalkerError> {
        Ok(match self {
            ParquetCompression::Zstd(level) => {
                let zstd_level = ZstdLevel::try_new(level).map_err(|e| {
                    WalkerError::Parquet(ParquetError::Other(format!(
                        "Invalid ZSTD level {}: {}",
                        level, e
                    )))
                })?;
                Compression::ZSTD(zstd_level)
            }
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Lz4Raw => Compression::LZ4_RAW,
            ParquetCompression::None => Compression::UNCOMPRESSED,
        })
    }
}

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

/// Bounded depth for each per-shard `Vec<DbEntry>` channel.
///
/// Each channel slot holds one batch (`Vec<DbEntry>` of up to
/// `batch_size`), so total peak buffer memory across all shards is
/// roughly `channel_depth × shards × batch_size × bytes_per_entry`.
///
/// 64 keeps the worst-case at ~2 GiB on the default config (32 shards,
/// batch_size 5000, ~200 B/entry); large enough to absorb short
/// writer hiccups, small enough that a 23 GiB host doesn't OOM when
/// the writer drains slower than walkers produce.
///
/// Larger hosts (production transfer hosts at 1.4 TiB+) can override
/// via `--parquet-channel-depth`.
pub const DEFAULT_CHANNEL_DEPTH: usize = 64;

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
    /// Rows per row-group flush. Default is 256K — small enough that
    /// each end-of-scan tail flush is fast (~25-50 MB per shard
    /// post-compression vs 200 MB+ at 1M), large enough that downstream
    /// analytical queries still benefit from row-group statistics for
    /// predicate pushdown. The post-hoc converters use 1M because
    /// they're not concurrent with a hot walker and can afford to
    /// accumulate.
    pub row_group_size: usize,
    /// File rotation threshold in bytes. The writer closes the current
    /// part once `bytes_written` crosses this value.
    pub target_file_size: usize,
    /// Compression algorithm + level.
    pub compression: ParquetCompression,
    /// Per-shard channel depth (in batches, not entries). See
    /// [`DEFAULT_CHANNEL_DEPTH`] for the memory math.
    pub channel_depth: usize,
}

impl Default for DirectWriteConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::new(),
            scan_id: String::new(),
            scan_timestamp_us: 0,
            shards: 1,
            row_group_size: 256_000,
            target_file_size: 512 * 1024 * 1024,
            // Snappy matches the streaming-Parquet default in DuckDB,
            // PyArrow, and Polars. Faster encoder than ZSTD with the
            // trade-off of ~30% larger files. The post-hoc converters
            // keep ZSTD-3 because they're not concurrent with a hot
            // walker and can afford the slower encoder for smaller
            // archived output.
            compression: ParquetCompression::Snappy,
            channel_depth: DEFAULT_CHANNEL_DEPTH,
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
/// Spawns one writer thread per shard. Each owns a single `ArrowWriter`
/// rotating part files within its shard, and produces
/// Parquet files directly. The walker funnels `Vec<DbEntry>` batches
/// into `senders[shard]` via the existing `ShardedSender`; each writer
/// drains its channel and flushes row groups to its part files.
///
/// The output sub-directory `<output_dir>/scans/<scan_id>/` is created
/// here (not by the per-shard thread) so concurrent writers don't race
/// on `create_dir_all`. We refuse to spawn if the directory already
/// exists.
pub fn spawn_direct_parquet_writers(
    config: DirectWriteConfig,
    metrics: Arc<ScanMetrics>,
) -> Result<DirectWritePool, WalkerError> {
    if config.shards == 0 {
        return Err(WalkerError::Parquet(ParquetError::Other(
            "direct-write requires shards >= 1".into(),
        )));
    }

    let scans_root = config.output_dir.join("scans");
    let scan_dir = scans_root.join(&config.scan_id);
    // Create the parent first (idempotent), then atomically create the
    // scan_id leaf with `create_dir` (not `_all`). This closes the
    // exists()/create_dir_all() TOCTOU: two concurrent invocations
    // with the same UUID can't both pass exists() and silently share
    // the directory — the second `create_dir` will fail with
    // AlreadyExists.
    fs::create_dir_all(&scans_root).map_err(ParquetError::Io)?;
    match fs::create_dir(&scan_dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(WalkerError::Parquet(ParquetError::Other(format!(
                "Refusing to scan: {} already exists. Use a different --output or delete the existing scan directory.",
                scan_dir.display()
            ))));
        }
        Err(e) => return Err(WalkerError::Parquet(ParquetError::Io(e))),
    }

    info!(
        "Direct-write Parquet output: {} (scan_id={}, shards={})",
        scan_dir.display(),
        config.scan_id,
        config.shards
    );

    let schema = parquet_schema_ref();
    let props = writer_properties(config.compression, config.row_group_size)?;
    // Wrapping properties in `Arc` so each writer thread can clone the
    // handle cheaply when rotating part files.
    let props = Arc::new(props);

    let mut senders: Vec<Sender<Vec<DbEntry>>> = Vec::with_capacity(config.shards);
    let mut joins: Vec<JoinHandle<Result<ShardSummary, WalkerError>>> =
        Vec::with_capacity(config.shards);

    // Filename widths (`part-r{:02}-{:05}.parquet`) are wired into the
    // lexicographic-sort guarantee in `write_metadata_json`. If anyone
    // bumps MAX_WRITER_SHARDS above 99 or row_group_size below ~4 KiB
    // (so part_seq exceeds 100K per shard on a billion-row scan), the
    // widths need to grow too — otherwise sorts mis-order at the
    // boundary. Caught here at spawn time, not silently at sort time.
    debug_assert!(
        config.shards <= 99,
        "part-r{{:02}} width overflows at shards={} (>99); widen the format",
        config.shards
    );

    let channel_depth = config.channel_depth.max(1);
    for shard_idx in 0..config.shards {
        // Channel depth caps the per-shard buffer; see
        // `DEFAULT_CHANNEL_DEPTH` for the memory budget rationale.
        // Walkers backpressure-block on `sender.push(entry)` when the
        // channel is full, so we trade a little walker throughput
        // (rarely; only when the writer is briefly stalled) for a
        // bounded memory ceiling and no OOM kills.
        let (tx, rx) = crossbeam_channel::bounded::<Vec<DbEntry>>(channel_depth);
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
            // DbEntry mtime/atime/ctime are already microseconds (see
            // nfs/types.rs), so no rescale.
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

/// RAII guard for an in-flight `part-rNN-SSSSS.parquet`. Removes the
/// file on `Drop` unless `commit()` has been called. Catches the Err
/// paths of P0-5 fully and panic-unwind paths of P0-4 on `dev` /
/// `panic = "unwind"` builds — see the module-level note on
/// `panic = "abort"` for the release-build gap.
struct InProgressPart {
    path: PathBuf,
    filename: String,
    committed: bool,
}

impl InProgressPart {
    fn new(scan_dir: &Path, shard_idx: usize, part_seq: u32) -> Self {
        let filename = format!("part-r{:02}-{:05}.parquet", shard_idx, part_seq);
        let path = scan_dir.join(&filename);
        Self { path, filename, committed: false }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// Mark the part file as successfully closed. Returns the filename
    /// so the caller can record it on its `ShardSummary`. Consumes
    /// `self`; subsequent `Drop` is a no-op.
    fn commit(mut self) -> String {
        self.committed = true;
        std::mem::take(&mut self.filename)
    }
}

impl Drop for InProgressPart {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        match std::fs::remove_file(&self.path) {
            Ok(_) => debug!(
                "removed in-progress part file {} (writer failed before commit)",
                self.path.display()
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // File::create may have failed before any bytes hit
                // disk; nothing to clean up.
            }
            Err(e) => warn!(
                "failed to remove in-progress part file {}: {}",
                self.path.display(),
                e
            ),
        }
    }
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
    let mut inprogress = InProgressPart::new(&scan_dir, shard_idx, part_seq);
    let mut writer = open_part_writer(inprogress.path(), &schema, &props)?;
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
                // valid footer. Close + commit the current part before
                // opening the next; if close() errors, `?` propagates
                // out and `inprogress`'s Drop removes the partial.
                if writer.bytes_written() as usize >= target_file_size {
                    // bytes_written() is captured before close() because
                    // close() consumes the writer. The footer adds a few
                    // KB that bytes_written doesn't see, so we prefer
                    // stat() of the closed file when available and fall
                    // back to the pre-close count if stat fails.
                    let pre_close_bytes = writer.bytes_written() as u64;
                    writer.close().map_err(ParquetError::Parquet)?;
                    let filename = inprogress.commit();
                    let closed_bytes = std::fs::metadata(scan_dir.join(&filename))
                        .map(|m| m.len())
                        .unwrap_or(pre_close_bytes);
                    summary.bytes_written += closed_bytes;
                    summary.part_files.push(filename);

                    inprogress = InProgressPart::new(&scan_dir, shard_idx, part_seq);
                    writer = open_part_writer(inprogress.path(), &schema, &props)?;
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

    // Same pattern as the rotation path: prefer post-close stat so the
    // footer bytes get counted; fall back to pre-close bytes_written().
    let pre_close_bytes = writer.bytes_written() as u64;
    writer.close().map_err(ParquetError::Parquet)?;
    let filename = inprogress.commit();
    let closed_bytes = std::fs::metadata(scan_dir.join(&filename))
        .map(|m| m.len())
        .unwrap_or(pre_close_bytes);
    summary.bytes_written += closed_bytes;
    summary.part_files.push(filename);

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

/// Open a fresh part-file writer at `path`. The caller pairs this with
/// an `InProgressPart` guard that owns the same path, so a returned
/// `Err` (or a later Err propagated up the writer loop before commit)
/// causes the guard's `Drop` to remove the partial file.
fn open_part_writer(
    path: &Path,
    schema: &Arc<Schema>,
    props: &Arc<WriterProperties>,
) -> Result<ArrowWriter<File>, WalkerError> {
    let file = File::create(path).map_err(ParquetError::Io)?;
    let writer = ArrowWriter::try_new(file, schema.clone(), Some(props.as_ref().clone()))
        .map_err(ParquetError::Parquet)?;
    Ok(writer)
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
    compression: ParquetCompression,
    row_group_size: usize,
) -> Result<WriterProperties, WalkerError> {
    Ok(WriterProperties::builder()
        .set_compression(compression.to_parquet()?)
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
        "total_bytes": total_bytes,
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
        compression: ParquetCompression::Zstd(3),
        channel_depth: DEFAULT_CHANNEL_DEPTH,
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
            compression: ParquetCompression::Zstd(1),
            channel_depth: DEFAULT_CHANNEL_DEPTH,
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

    #[test]
    fn inprogress_part_drop_removes_uncommitted_file() {
        let dir = tempdir().unwrap();
        let scan_dir = dir.path().to_path_buf();

        let path_on_disk = {
            let inprogress = InProgressPart::new(&scan_dir, 0, 0);
            // Simulate the writer touching the file.
            File::create(inprogress.path()).unwrap();
            assert!(inprogress.path().exists(), "setup: file must exist");
            inprogress.path().to_path_buf()
            // inprogress drops here without commit() — should remove file
        };
        assert!(
            !path_on_disk.exists(),
            "uncommitted Drop must remove {}",
            path_on_disk.display()
        );
    }

    #[test]
    fn inprogress_part_commit_preserves_file() {
        let dir = tempdir().unwrap();
        let scan_dir = dir.path().to_path_buf();

        let inprogress = InProgressPart::new(&scan_dir, 0, 0);
        File::create(inprogress.path()).unwrap();
        let path_on_disk = inprogress.path().to_path_buf();
        let returned_filename = inprogress.commit();

        assert!(
            path_on_disk.exists(),
            "committed file must persist after Drop"
        );
        assert_eq!(returned_filename, "part-r00-00000.parquet");
    }

    #[test]
    fn inprogress_part_drop_tolerates_missing_file() {
        // File::create may have failed before the guard saw bytes; Drop
        // must not warn or error when the path doesn't exist.
        let dir = tempdir().unwrap();
        let scan_dir = dir.path().to_path_buf();
        let inprogress = InProgressPart::new(&scan_dir, 3, 42);
        // Don't create the file — let Drop run on an absent path.
        drop(inprogress);
        // No assertion needed; we're checking the call doesn't panic.
    }
}

