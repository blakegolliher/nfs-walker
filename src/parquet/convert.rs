//! RocksDB to Parquet conversion
//!
//! Streams entries from RocksDB and writes Parquet files with ZSTD compression,
//! column statistics, and automatic file splitting.

use crate::error::{ParquetError, WalkerError};
use crate::parquet::builder::{RowBuilder, RowContext};
use crate::parquet::schema::{parquet_schema_ref, seconds_to_microseconds};
use crate::rocksdb::schema::{meta_keys, RocksHandle};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

/// Configuration for Parquet export
pub struct ExportConfig {
    /// Number of rows per row group
    pub row_group_size: usize,
    /// Target file size in bytes before splitting to a new part
    pub target_file_size: usize,
    /// ZSTD compression level (1-22)
    pub compression_level: i32,
    /// Show progress
    pub progress: bool,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            row_group_size: 1_000_000,
            target_file_size: 256 * 1024 * 1024,
            compression_level: 3,
            progress: true,
        }
    }
}

/// Statistics from an export operation
#[derive(Debug, Clone)]
pub struct ExportStats {
    pub entries_exported: u64,
    pub files_written: u32,
    pub total_bytes_written: u64,
    pub scan_id: String,
}

/// Progress callback type
pub type ProgressCallback = Box<dyn Fn(u64, u64) + Send>;

/// Decide whether the RocksDB row times are already microseconds or
/// still in seconds (legacy databases written before the walker
/// captured sub-second precision). Returns the multiplier the row
/// builder should apply: `1` for microseconds, `1_000_000` for seconds.
///
/// New databases stamp `meta_keys::MTIME_FORMAT = "microseconds"`.
/// Anything else — absent, empty, the literal `"seconds"`, or an
/// unrecognized value — is treated as legacy seconds. We deliberately
/// don't error on unrecognized values: rescaling preserves the existing
/// behavior of older binaries (no precision loss for whole-second data,
/// and a clear flag in metadata hints at why values look stale).
pub(crate) fn detect_mtime_scale(rocks: &RocksHandle) -> Result<i64, WalkerError> {
    let value = rocks
        .get_metadata(meta_keys::MTIME_FORMAT)
        .map_err(|e| {
            WalkerError::Parquet(ParquetError::Other(format!(
                "Failed to read mtime_format metadata: {}",
                e
            )))
        })?
        .unwrap_or_default();
    Ok(if value == meta_keys::MTIME_FORMAT_MICROSECONDS {
        1
    } else {
        1_000_000
    })
}

/// Convert a RocksDB database to Parquet files.
///
/// Writes Parquet files to `<output_dir>/scans/<scan_id>/` with automatic
/// file splitting when files exceed `target_file_size`.
pub fn convert_rocks_to_parquet<P1, P2>(
    rocks_path: P1,
    output_dir: P2,
    config: ExportConfig,
    progress_callback: Option<ProgressCallback>,
) -> Result<ExportStats, WalkerError>
where
    P1: AsRef<Path>,
    P2: AsRef<Path>,
{
    let rocks_path = rocks_path.as_ref();
    let output_dir = output_dir.as_ref();

    info!("Opening RocksDB: {}", rocks_path.display());
    let rocks = RocksHandle::open_readonly(rocks_path).map_err(|e| {
        WalkerError::Parquet(ParquetError::Other(format!("Failed to open RocksDB: {}", e)))
    })?;

    // Reuse the scan_id persisted at scan start (when --stream-parquet
    // ran), otherwise mint a fresh one. Sharing the id lets a streamed
    // scan and a later export point at the same logical artifact.
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
            // Try RFC3339/ISO8601 first (current format), then numeric epoch
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

    // Create output directory. Refuse to overwrite an existing scan dir
    // with the same id -- this guards against converting on top of a
    // streamed Parquet directory (which would mix old and new parts
    // sharing the scan_id) and against re-running convert without first
    // cleaning up.
    let scan_dir = output_dir.join("scans").join(&scan_id);
    if scan_dir.exists() {
        return Err(WalkerError::Parquet(ParquetError::Other(format!(
            "Refusing to convert: {} already exists. Delete it (or pass a different output_dir) \
             before re-running. If --stream-parquet was used during the scan the directory is \
             already populated and another conversion would mix new parts under the same scan_id.",
            scan_dir.display()
        ))));
    }
    fs::create_dir_all(&scan_dir).map_err(ParquetError::Io)?;

    info!(
        "Exporting to Parquet: {} (scan_id={})",
        scan_dir.display(),
        scan_id
    );

    // Build writer properties
    let props = writer_properties(config.compression_level)?;

    // Stream entries and write Parquet
    let schema = parquet_schema_ref();
    let mut part_number: u32 = 0;
    let mut total_exported: u64 = 0;
    let mut progress_counter: u64 = 0;

    let mut writer = open_part_writer(&scan_dir, part_number, &schema, props.clone())?;
    part_number += 1;

    let mut row_builder = RowBuilder::new(RowContext {
        scan_id: scan_id.clone(),
        scan_timestamp_us,
        mtime_scale: detect_mtime_scale(&rocks)?,
    });

    for result in rocks.iter_by_path() {
        let entry = result.map_err(|e| {
            WalkerError::Parquet(ParquetError::Other(format!("Failed to read entry: {}", e)))
        })?;

        row_builder.push_rocks_entry(&entry);

        // Flush row group when buffer is full
        if row_builder.row_count() >= config.row_group_size {
            let rows = row_builder.row_count();
            let batch = row_builder.finish()?;
            writer.write(&batch).map_err(ParquetError::Parquet)?;
            total_exported += rows as u64;

            // Check if file exceeds target size — split to new part
            let bytes_written = writer.bytes_written() as usize;
            if bytes_written >= config.target_file_size {
                writer.close().map_err(ParquetError::Parquet)?;
                writer = open_part_writer(&scan_dir, part_number, &schema, props.clone())?;
                part_number += 1;
            }
        }

        // Report progress
        progress_counter += 1;
        if progress_counter >= 100_000 {
            if let Some(ref cb) = progress_callback {
                cb(total_exported + row_builder.row_count() as u64, 0);
            }
            progress_counter = 0;
        }
    }

    // Flush remaining rows
    if !row_builder.is_empty() {
        let rows = row_builder.row_count();
        let batch = row_builder.finish()?;
        writer.write(&batch).map_err(ParquetError::Parquet)?;
        total_exported += rows as u64;
    }

    writer.close().map_err(ParquetError::Parquet)?;
    let total_bytes = recalculate_total_bytes(&scan_dir);

    // Final progress
    if let Some(cb) = progress_callback {
        cb(total_exported, total_exported);
    }

    // Write metadata.json
    let parquet_files = list_parquet_files(&scan_dir);
    write_metadata_json(
        &scan_dir,
        &scan_id,
        scan_timestamp_us,
        &source_url,
        total_exported,
        &parquet_files,
    )?;

    info!(
        "Export complete: {} entries in {} files ({} bytes)",
        total_exported, part_number, total_bytes
    );

    Ok(ExportStats {
        entries_exported: total_exported,
        files_written: part_number,
        total_bytes_written: total_bytes,
        scan_id,
    })
}

/// Build Parquet writer properties with ZSTD compression and column statistics.
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

/// Open a new Parquet part file writer.
fn open_part_writer(
    scan_dir: &Path,
    part_number: u32,
    schema: &Arc<arrow::datatypes::Schema>,
    props: WriterProperties,
) -> Result<ArrowWriter<File>, WalkerError> {
    let filename = format!("part-{:05}.parquet", part_number);
    let path = scan_dir.join(&filename);
    let file = File::create(&path).map_err(ParquetError::Io)?;
    let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .map_err(ParquetError::Parquet)?;
    Ok(writer)
}

/// List all .parquet files in a directory, sorted by name.
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

/// Recalculate total bytes of all files in a directory.
fn recalculate_total_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| entry.metadata().ok().map(|m| m.len()))
        .sum()
}

/// Write metadata.json alongside the Parquet files.
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
    use crate::rocksdb::schema::meta_keys;
    use crate::rocksdb::writer::{RocksWriter, RocksWriterConfig};
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn create_test_rocks(dir: &Path) -> PathBuf {
        let rocks_path = dir.join("test.rocks");
        let config = RocksWriterConfig::default();
        let writer = RocksWriter::open(&rocks_path, config).unwrap();

        let entries = vec![
            DbEntry {
                parent_path: Some("/".to_string()),
                name: "file1.txt".to_string(),
                path: "/file1.txt".to_string(),
                entry_type: EntryType::File,
                size: 1024,
                mtime: Some(1700000000),
                atime: Some(1700000100),
                ctime: Some(1699999000),
                mode: Some(0o644),
                uid: Some(1000),
                gid: Some(1000),
                nlink: Some(1),
                inode: 100,
                depth: 1,
                extension: Some("txt".to_string()),
                blocks: 8,
                checksum: None,
                file_type: None,
            },
            DbEntry {
                parent_path: Some("/".to_string()),
                name: "dir1".to_string(),
                path: "/dir1".to_string(),
                entry_type: EntryType::Directory,
                size: 4096,
                mtime: Some(1700000000),
                atime: None,
                ctime: None,
                mode: Some(0o755),
                uid: Some(0),
                gid: Some(0),
                nlink: Some(2),
                inode: 200,
                depth: 1,
                extension: None,
                blocks: 8,
                checksum: None,
                file_type: None,
            },
            DbEntry {
                parent_path: Some("/dir1".to_string()),
                name: "nested.log".to_string(),
                path: "/dir1/nested.log".to_string(),
                entry_type: EntryType::File,
                size: 2048,
                mtime: Some(1700001000),
                atime: None,
                ctime: Some(1700001000),
                mode: Some(0o600),
                uid: Some(1000),
                gid: Some(100),
                nlink: None,
                inode: 300,
                depth: 2,
                extension: Some("log".to_string()),
                blocks: 16,
                checksum: None,
                file_type: None,
            },
        ];

        writer
            .set_metadata(meta_keys::SOURCE_URL, "nfs://test/export")
            .unwrap();
        writer
            .set_metadata(meta_keys::START_TIME, "1700000000")
            .unwrap();
        writer
            .set_metadata(meta_keys::TOTAL_FILES, "2")
            .unwrap();
        writer
            .set_metadata(meta_keys::TOTAL_DIRS, "1")
            .unwrap();
        writer.write_batch(&entries).unwrap();
        writer.flush().unwrap();
        drop(writer);

        rocks_path
    }

    #[test]
    fn test_export_creates_parquet_files() {
        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("parquet_output");

        let config = ExportConfig {
            row_group_size: 100,
            target_file_size: 256 * 1024 * 1024,
            compression_level: 3,
            progress: false,
        };

        let stats = convert_rocks_to_parquet(&rocks_path, &output_dir, config, None).unwrap();

        assert_eq!(stats.entries_exported, 3);
        assert_eq!(stats.files_written, 1);
        assert!(stats.total_bytes_written > 0);

        // Verify scan directory structure
        let scan_dir = output_dir.join("scans").join(&stats.scan_id);
        assert!(scan_dir.exists());
        assert!(scan_dir.join("part-00000.parquet").exists());
        assert!(scan_dir.join("metadata.json").exists());
    }

    #[test]
    fn test_export_metadata_json() {
        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("parquet_output");

        let config = ExportConfig::default();
        let stats = convert_rocks_to_parquet(&rocks_path, &output_dir, config, None).unwrap();

        let scan_dir = output_dir.join("scans").join(&stats.scan_id);
        let metadata_str = fs::read_to_string(scan_dir.join("metadata.json")).unwrap();
        let metadata: serde_json::Value = serde_json::from_str(&metadata_str).unwrap();

        assert_eq!(metadata["scan_id"], stats.scan_id);
        assert_eq!(metadata["source_url"], "nfs://test/export");
        assert_eq!(metadata["total_entries"], 3);
        let files = metadata["parquet_files"].as_array().unwrap();
        assert!(!files.is_empty());
        assert_eq!(files[0], "part-00000.parquet");
    }

    #[test]
    fn test_export_parquet_readable() {
        use arrow::datatypes::DataType;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("parquet_output");

        let config = ExportConfig::default();
        let stats = convert_rocks_to_parquet(&rocks_path, &output_dir, config, None).unwrap();

        // Read back the Parquet file
        let scan_dir = output_dir.join("scans").join(&stats.scan_id);
        let file = File::open(scan_dir.join("part-00000.parquet")).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let mut total_rows = 0;
        for batch_result in reader {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();

            // Verify schema matches
            assert_eq!(batch.num_columns(), 18);
            assert_eq!(batch.schema().field(0).name(), "path");
            assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
            assert_eq!(batch.schema().field(5).name(), "size");
            assert_eq!(batch.schema().field(5).data_type(), &DataType::UInt64);
        }

        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_export_with_progress_callback() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("parquet_output");

        let config = ExportConfig {
            row_group_size: 2, // small to trigger flush
            target_file_size: 256 * 1024 * 1024,
            compression_level: 3,
            progress: true,
        };

        let last_count = Arc::new(AtomicU64::new(0));
        let cb_count = last_count.clone();
        let callback: ProgressCallback = Box::new(move |converted, _total| {
            cb_count.store(converted, Ordering::SeqCst);
        });

        let stats =
            convert_rocks_to_parquet(&rocks_path, &output_dir, config, Some(callback)).unwrap();

        // Final callback should have been called with total entries
        assert_eq!(last_count.load(Ordering::SeqCst), stats.entries_exported);
    }

    #[test]
    fn detect_mtime_scale_returns_one_for_new_dbs() {
        // RocksWriter::open stamps MTIME_FORMAT = "microseconds". So a
        // freshly-opened DB must drive scale=1 (no rescale at export).
        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());

        let rocks = RocksHandle::open_readonly(&rocks_path).unwrap();
        assert_eq!(detect_mtime_scale(&rocks).unwrap(), 1);
    }

    #[test]
    fn detect_mtime_scale_returns_million_for_legacy_dbs() {
        // Drop the MTIME_FORMAT key to simulate an old database written
        // before the walker captured sub-second precision. The exporter
        // must rescale by 1_000_000 so legacy seconds turn into the
        // microseconds the Parquet schema expects.
        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        {
            let h = RocksHandle::open(&rocks_path).unwrap();
            h.db
                .delete_cf(h.cf_metadata(), meta_keys::MTIME_FORMAT.as_bytes())
                .unwrap();
        }

        let rocks = RocksHandle::open_readonly(&rocks_path).unwrap();
        assert_eq!(detect_mtime_scale(&rocks).unwrap(), 1_000_000);
    }

    #[test]
    fn legacy_db_export_rescales_seconds_to_microseconds() {
        use arrow::array::{Array, Int64Array};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // Build a DB the way create_test_rocks does, then strip the
        // MTIME_FORMAT key so the exporter treats it as legacy seconds.
        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        {
            let h = RocksHandle::open(&rocks_path).unwrap();
            h.db
                .delete_cf(h.cf_metadata(), meta_keys::MTIME_FORMAT.as_bytes())
                .unwrap();
        }

        let output_dir = dir.path().join("parquet_output");
        let stats = convert_rocks_to_parquet(
            &rocks_path,
            &output_dir,
            ExportConfig {
                row_group_size: 100,
                target_file_size: 256 * 1024 * 1024,
                compression_level: 3,
                progress: false,
            },
            None,
        )
        .unwrap();

        // Read the part file and confirm mtime_us = 1700000000 * 1_000_000
        // for the rows that had mtime: Some(1700000000) in the fixture.
        let scan_dir = output_dir.join("scans").join(&stats.scan_id);
        let file = File::open(scan_dir.join("part-00000.parquet")).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let mut saw_rescaled = false;
        for batch_result in reader {
            let batch = batch_result.unwrap();
            let mtime_idx = batch
                .schema()
                .index_of("mtime_us")
                .expect("mtime_us column missing");
            let col = batch
                .column(mtime_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("mtime_us is not Int64");
            for i in 0..col.len() {
                if !col.is_null(i) && col.value(i) == 1_700_000_000_000_000 {
                    saw_rescaled = true;
                }
            }
        }
        assert!(
            saw_rescaled,
            "legacy export must multiply seconds by 1_000_000 -- expected mtime_us == 1700000000000000 in at least one row"
        );
    }

    #[test]
    fn fresh_db_export_passes_microseconds_through() {
        use arrow::array::{Array, Int64Array};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // The fixture's mtime values now mean microseconds (= 1970), and
        // a fresh DB has MTIME_FORMAT set, so the exporter must NOT
        // rescale. mtime_us must equal the input verbatim.
        let dir = tempdir().unwrap();
        let rocks_path = create_test_rocks(dir.path());
        let output_dir = dir.path().join("parquet_output");
        let stats = convert_rocks_to_parquet(
            &rocks_path,
            &output_dir,
            ExportConfig {
                row_group_size: 100,
                target_file_size: 256 * 1024 * 1024,
                compression_level: 3,
                progress: false,
            },
            None,
        )
        .unwrap();

        let scan_dir = output_dir.join("scans").join(&stats.scan_id);
        let file = File::open(scan_dir.join("part-00000.parquet")).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        let mut saw_passthrough = false;
        for batch_result in reader {
            let batch = batch_result.unwrap();
            let mtime_idx = batch.schema().index_of("mtime_us").unwrap();
            let col = batch
                .column(mtime_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                if !col.is_null(i) && col.value(i) == 1_700_000_000 {
                    saw_passthrough = true;
                }
            }
        }
        assert!(
            saw_passthrough,
            "fresh-DB export must pass microseconds through unchanged -- expected mtime_us == 1700000000 in at least one row"
        );
    }
}
