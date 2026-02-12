//! RocksDB to Parquet conversion
//!
//! Streams entries from RocksDB and writes Parquet files with ZSTD compression,
//! column statistics, and automatic file splitting.

use crate::error::{ParquetError, WalkerError};
use crate::parquet::schema::{
    compute_parent_path, file_type_string, parquet_schema_ref, seconds_to_microseconds,
};
use crate::rocksdb::schema::{meta_keys, RocksHandle};
use arrow::array::{
    ArrayRef, Int64Builder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::record_batch::RecordBatch;
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

    // Generate scan ID and read metadata
    let scan_id = Uuid::new_v4().to_string();
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

    // Create output directory
    let scan_dir = output_dir.join("scans").join(&scan_id);
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

    // Array builders — one per column
    let mut b_path = StringBuilder::new();
    let mut b_filename = StringBuilder::new();
    let mut b_extension = StringBuilder::new();
    let mut b_inode = UInt64Builder::new();
    let mut b_file_type = StringBuilder::new();
    let mut b_size = UInt64Builder::new();
    let mut b_alloc_blocks = UInt64Builder::new();
    let mut b_nlink = UInt32Builder::new();
    let mut b_uid = UInt32Builder::new();
    let mut b_gid = UInt32Builder::new();
    let mut b_permissions = UInt16Builder::new();
    let mut b_mtime = Int64Builder::new();
    let mut b_atime = Int64Builder::new();
    let mut b_ctime = Int64Builder::new();
    let mut b_depth = UInt16Builder::new();
    let mut b_parent_path = StringBuilder::new();
    let mut b_scan_id = StringBuilder::new();
    let mut b_scan_ts = Int64Builder::new();

    let mut buffered_rows: usize = 0;

    for result in rocks.iter_by_path() {
        let entry = result.map_err(|e| {
            WalkerError::Parquet(ParquetError::Other(format!("Failed to read entry: {}", e)))
        })?;

        // Append to builders
        b_path.append_value(&entry.path);
        b_filename.append_value(&entry.name);
        match &entry.extension {
            Some(ext) => b_extension.append_value(ext),
            None => b_extension.append_null(),
        }
        b_inode.append_value(entry.inode);
        b_file_type.append_value(file_type_string(entry.entry_type));
        b_size.append_value(entry.size);
        b_alloc_blocks.append_value(entry.blocks);
        b_nlink.append_value(entry.nlink.unwrap_or(1) as u32);
        b_uid.append_value(entry.uid.unwrap_or(0));
        b_gid.append_value(entry.gid.unwrap_or(0));
        b_permissions.append_value(entry.mode.map(|m| (m & 0o7777) as u16).unwrap_or(0o644));
        match entry.mtime {
            Some(s) => b_mtime.append_value(seconds_to_microseconds(s)),
            None => b_mtime.append_null(),
        }
        match entry.atime {
            Some(s) => b_atime.append_value(seconds_to_microseconds(s)),
            None => b_atime.append_null(),
        }
        match entry.ctime {
            Some(s) => b_ctime.append_value(seconds_to_microseconds(s)),
            None => b_ctime.append_null(),
        }
        b_depth.append_value(entry.depth as u16);
        b_parent_path.append_value(compute_parent_path(&entry.path));
        b_scan_id.append_value(&scan_id);
        b_scan_ts.append_value(scan_timestamp_us);

        buffered_rows += 1;

        // Flush row group when buffer is full
        if buffered_rows >= config.row_group_size {
            let batch = finish_batch(
                &schema,
                &mut b_path,
                &mut b_filename,
                &mut b_extension,
                &mut b_inode,
                &mut b_file_type,
                &mut b_size,
                &mut b_alloc_blocks,
                &mut b_nlink,
                &mut b_uid,
                &mut b_gid,
                &mut b_permissions,
                &mut b_mtime,
                &mut b_atime,
                &mut b_ctime,
                &mut b_depth,
                &mut b_parent_path,
                &mut b_scan_id,
                &mut b_scan_ts,
            )?;

            writer.write(&batch).map_err(ParquetError::Parquet)?;
            total_exported += buffered_rows as u64;
            buffered_rows = 0;

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
                cb(total_exported + buffered_rows as u64, 0);
            }
            progress_counter = 0;
        }
    }

    // Flush remaining rows
    if buffered_rows > 0 {
        let batch = finish_batch(
            &schema,
            &mut b_path,
            &mut b_filename,
            &mut b_extension,
            &mut b_inode,
            &mut b_file_type,
            &mut b_size,
            &mut b_alloc_blocks,
            &mut b_nlink,
            &mut b_uid,
            &mut b_gid,
            &mut b_permissions,
            &mut b_mtime,
            &mut b_atime,
            &mut b_ctime,
            &mut b_depth,
            &mut b_parent_path,
            &mut b_scan_id,
            &mut b_scan_ts,
        )?;

        writer.write(&batch).map_err(ParquetError::Parquet)?;
        total_exported += buffered_rows as u64;
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

/// Finish the current batch by converting builders to a RecordBatch.
#[allow(clippy::too_many_arguments)]
fn finish_batch(
    schema: &Arc<arrow::datatypes::Schema>,
    b_path: &mut StringBuilder,
    b_filename: &mut StringBuilder,
    b_extension: &mut StringBuilder,
    b_inode: &mut UInt64Builder,
    b_file_type: &mut StringBuilder,
    b_size: &mut UInt64Builder,
    b_alloc_blocks: &mut UInt64Builder,
    b_nlink: &mut UInt32Builder,
    b_uid: &mut UInt32Builder,
    b_gid: &mut UInt32Builder,
    b_permissions: &mut UInt16Builder,
    b_mtime: &mut Int64Builder,
    b_atime: &mut Int64Builder,
    b_ctime: &mut Int64Builder,
    b_depth: &mut UInt16Builder,
    b_parent_path: &mut StringBuilder,
    b_scan_id: &mut StringBuilder,
    b_scan_ts: &mut Int64Builder,
) -> Result<RecordBatch, WalkerError> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(b_path.finish()),
        Arc::new(b_filename.finish()),
        Arc::new(b_extension.finish()),
        Arc::new(b_inode.finish()),
        Arc::new(b_file_type.finish()),
        Arc::new(b_size.finish()),
        Arc::new(b_alloc_blocks.finish()),
        Arc::new(b_nlink.finish()),
        Arc::new(b_uid.finish()),
        Arc::new(b_gid.finish()),
        Arc::new(b_permissions.finish()),
        Arc::new(b_mtime.finish()),
        Arc::new(b_atime.finish()),
        Arc::new(b_ctime.finish()),
        Arc::new(b_depth.finish()),
        Arc::new(b_parent_path.finish()),
        Arc::new(b_scan_id.finish()),
        Arc::new(b_scan_ts.finish()),
    ];

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| WalkerError::Parquet(ParquetError::Arrow(e)))
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
}
