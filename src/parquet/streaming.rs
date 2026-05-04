//! Live Parquet writer used during an active scan.
//!
//! Consumes batches of `DbEntry` from the walker pipeline, accumulates
//! them into 1M-row Parquet row groups, and rotates to a new part file
//! once `target_file_size` bytes are on disk. Each part is written to
//! `<scan_dir>/.part-NNNNN.parquet.tmp` and atomically renamed to
//! `part-NNNNN.parquet` only after a clean close, so DuckDB glob queries
//! over the directory naturally skip in-progress files.

use crate::error::{ParquetError, WalkerError};
use crate::nfs::types::DbEntry;
use crate::parquet::builder::{RowBuilder, RowContext};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::PathBuf;
use tracing::{debug, info, warn};

/// Configuration for the streaming Parquet writer.
#[derive(Debug, Clone)]
pub struct StreamingParquetConfig {
    /// Directory where part files are written. Created if absent.
    pub scan_dir: PathBuf,
    /// Number of rows per row group inside a part file.
    pub row_group_size: usize,
    /// Approximate target part file size before rotating to a new part.
    pub target_file_size: usize,
    /// ZSTD compression level (1-22).
    pub compression_level: i32,
    /// Scan UUID written into every row.
    pub scan_id: String,
    /// Scan start timestamp (microseconds since epoch) written into every row.
    pub scan_timestamp_us: i64,
}

impl StreamingParquetConfig {
    pub fn defaults_for(scan_dir: PathBuf, scan_id: String, scan_timestamp_us: i64) -> Self {
        Self {
            scan_dir,
            row_group_size: 1_000_000,
            target_file_size: 256 * 1024 * 1024,
            compression_level: 3,
            scan_id,
            scan_timestamp_us,
        }
    }
}

/// Stats reported when the streaming writer finishes.
#[derive(Debug, Clone, Default)]
pub struct StreamingParquetStats {
    pub rows_written: u64,
    pub parts_written: u32,
    pub bytes_written: u64,
}

/// Live Parquet writer that owns one in-progress part file at a time
/// and rolls it over once the target size is hit.
pub struct StreamingParquetWriter {
    config: StreamingParquetConfig,
    writer_props: WriterProperties,
    builder: RowBuilder,

    current_writer: Option<ArrowWriter<File>>,
    current_tmp_path: Option<PathBuf>,
    current_final_path: Option<PathBuf>,
    part_number: u32,

    rows_written: u64,
    bytes_written: u64,
}

impl StreamingParquetWriter {
    /// Open the writer and prepare the first part file. The scan dir is
    /// created if missing; an existing dir is left alone (caller is
    /// responsible for collision detection -- see scan_id docs).
    pub fn open(config: StreamingParquetConfig) -> Result<Self, WalkerError> {
        fs::create_dir_all(&config.scan_dir).map_err(ParquetError::Io)?;

        let zstd = ZstdLevel::try_new(config.compression_level).map_err(|e| {
            WalkerError::Parquet(ParquetError::Other(format!(
                "Invalid ZSTD level {}: {}",
                config.compression_level, e
            )))
        })?;

        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd))
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
            .set_max_row_group_size(config.row_group_size)
            .build();

        // Streaming writer is fed from the live walker pipeline, where
        // `DbEntry` already carries microseconds. Pass-through scale.
        let builder = RowBuilder::new(RowContext {
            scan_id: config.scan_id.clone(),
            scan_timestamp_us: config.scan_timestamp_us,
            mtime_scale: 1,
        });

        let mut me = Self {
            config,
            writer_props,
            builder,
            current_writer: None,
            current_tmp_path: None,
            current_final_path: None,
            part_number: 0,
            rows_written: 0,
            bytes_written: 0,
        };

        me.start_new_part()?;
        Ok(me)
    }

    /// Append a batch of entries. Flushes a row group when the buffered
    /// row count reaches `row_group_size`, and rotates to a new part
    /// file when the current part exceeds `target_file_size`.
    pub fn write_batch(&mut self, entries: &[DbEntry]) -> Result<(), WalkerError> {
        for entry in entries {
            self.builder.push_db_entry(entry);

            if self.builder.row_count() >= self.config.row_group_size {
                self.flush_row_group()?;
                if self.should_rotate() {
                    self.rotate()?;
                }
            }
        }
        Ok(())
    }

    /// Flush whatever's buffered as a final row group, close the open
    /// part (atomically renaming it into place), and return totals.
    pub fn close(mut self) -> Result<StreamingParquetStats, WalkerError> {
        if !self.builder.is_empty() {
            self.flush_row_group()?;
        }
        self.close_current_part()?;

        Ok(StreamingParquetStats {
            rows_written: self.rows_written,
            parts_written: self.part_number,
            bytes_written: self.bytes_written,
        })
    }

    fn start_new_part(&mut self) -> Result<(), WalkerError> {
        let final_name = format!("part-{:05}.parquet", self.part_number);
        let tmp_name = format!(".part-{:05}.parquet.tmp", self.part_number);
        let final_path = self.config.scan_dir.join(&final_name);
        let tmp_path = self.config.scan_dir.join(&tmp_name);

        if final_path.exists() {
            return Err(WalkerError::Parquet(ParquetError::Other(format!(
                "Streaming Parquet collision: {} already exists",
                final_path.display()
            ))));
        }
        if tmp_path.exists() {
            // Stale .tmp from a previous crashed run -- remove and warn.
            warn!(
                "Removing stale temp Parquet file from prior run: {}",
                tmp_path.display()
            );
            let _ = fs::remove_file(&tmp_path);
        }

        let file = File::create(&tmp_path).map_err(ParquetError::Io)?;
        let writer = ArrowWriter::try_new(
            file,
            self.builder.schema().clone(),
            Some(self.writer_props.clone()),
        )
        .map_err(ParquetError::Parquet)?;

        debug!("Opened streaming Parquet part: {}", tmp_path.display());
        self.current_writer = Some(writer);
        self.current_tmp_path = Some(tmp_path);
        self.current_final_path = Some(final_path);
        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<(), WalkerError> {
        if self.builder.is_empty() {
            return Ok(());
        }
        let rows = self.builder.row_count();
        let batch = self.builder.finish()?;

        let writer = self
            .current_writer
            .as_mut()
            .expect("streaming writer not open");
        writer.write(&batch).map_err(ParquetError::Parquet)?;
        self.rows_written += rows as u64;
        Ok(())
    }

    fn should_rotate(&self) -> bool {
        let bytes = self
            .current_writer
            .as_ref()
            .map(|w| w.bytes_written() as usize)
            .unwrap_or(0);
        bytes >= self.config.target_file_size
    }

    fn rotate(&mut self) -> Result<(), WalkerError> {
        self.close_current_part()?;
        self.start_new_part()?;
        Ok(())
    }

    fn close_current_part(&mut self) -> Result<(), WalkerError> {
        let writer = match self.current_writer.take() {
            Some(w) => w,
            None => return Ok(()),
        };
        let bytes = writer.bytes_written() as u64;
        writer.close().map_err(ParquetError::Parquet)?;

        let tmp = self.current_tmp_path.take().expect("tmp path missing");
        let final_path = self.current_final_path.take().expect("final path missing");

        // fsync the temp file so the rename publishes durable bytes.
        if let Ok(file) = File::open(&tmp) {
            let _ = file.sync_all();
        }
        fs::rename(&tmp, &final_path).map_err(ParquetError::Io)?;
        info!(
            "Closed streaming Parquet part {} ({} rows total, {} bytes this part)",
            final_path.display(),
            self.rows_written,
            bytes
        );

        self.part_number += 1;
        self.bytes_written += bytes;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::EntryType;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    fn mk_file(path: &str, size: u64) -> DbEntry {
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
            uid: Some(1000),
            gid: Some(1000),
            nlink: Some(1),
            inode: 0,
            depth: 1,
            extension: Some("txt".to_string()),
            blocks: 1,
            checksum: None,
            file_type: None,
        }
    }

    #[test]
    fn write_one_part_and_read_back() {
        let dir = tempdir().unwrap();
        let scan_dir = dir.path().join("scans/abc");

        let cfg = StreamingParquetConfig {
            scan_dir: scan_dir.clone(),
            row_group_size: 100,
            target_file_size: 1_000_000_000, // never rotate in this test
            compression_level: 3,
            scan_id: "abc".to_string(),
            scan_timestamp_us: 1_700_000_000_000_000,
        };

        let mut writer = StreamingParquetWriter::open(cfg).unwrap();
        let batch: Vec<DbEntry> = (0..50)
            .map(|i| mk_file(&format!("/file_{}.txt", i), i as u64 * 10))
            .collect();
        writer.write_batch(&batch).unwrap();
        let stats = writer.close().unwrap();

        assert_eq!(stats.rows_written, 50);
        assert_eq!(stats.parts_written, 1);

        let part = scan_dir.join("part-00000.parquet");
        assert!(part.exists(), "expected {}", part.display());

        let file = File::open(&part).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut total = 0;
        for batch in reader {
            total += batch.unwrap().num_rows();
        }
        assert_eq!(total, 50);
    }

    #[test]
    fn rotates_when_target_size_exceeded() {
        let dir = tempdir().unwrap();
        let scan_dir = dir.path().join("scans/rot");

        let cfg = StreamingParquetConfig {
            scan_dir: scan_dir.clone(),
            row_group_size: 100,
            target_file_size: 4096, // tiny -- forces rotation
            compression_level: 3,
            scan_id: "rot".to_string(),
            scan_timestamp_us: 1_700_000_000_000_000,
        };

        let mut writer = StreamingParquetWriter::open(cfg).unwrap();
        let mut total_rows = 0;
        // Push enough rows that bytes_written easily blows past 4096.
        for chunk in 0..5 {
            let batch: Vec<DbEntry> = (0..200)
                .map(|i| mk_file(&format!("/c{}/file_{}.txt", chunk, i), i as u64))
                .collect();
            writer.write_batch(&batch).unwrap();
            total_rows += batch.len();
        }
        let stats = writer.close().unwrap();

        assert_eq!(stats.rows_written, total_rows as u64);
        assert!(
            stats.parts_written >= 2,
            "expected rotation but got {} parts",
            stats.parts_written
        );

        // No leftover .tmp files in the scan dir.
        for entry in fs::read_dir(&scan_dir).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().to_string();
            assert!(
                !name.ends_with(".tmp"),
                "stray .tmp file remained: {}",
                name
            );
        }
    }
}
