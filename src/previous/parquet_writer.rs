//! Parquet/Arrow writer for high-throughput columnar output
//!
//! This module provides a writer that outputs filesystem entries to Apache Parquet
//! format. Parquet offers:
//! - Excellent compression (2-10x smaller than SQLite)
//! - Fast columnar queries (ideal for analytics)
//! - Native support in DuckDB, Pandas, Polars, Spark, etc.

use crate::error::{DbError, DbResult};
use crate::nfs::types::{DbEntry, DirStats};
use arrow::array::{
    ArrayRef, Int32Builder, Int64Builder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Message types sent to the writer thread
#[derive(Debug)]
pub enum ParquetWriterMessage {
    /// Insert a new entry
    Entry(DbEntry),

    /// Directory statistics (stored in separate file or ignored for simplicity)
    DirStats { path: String, stats: DirStats },

    /// Flush pending writes
    Flush,

    /// Shutdown the writer
    Shutdown,
}

/// Statistics about write operations
#[derive(Debug, Default)]
pub struct ParquetWriterStats {
    pub entries_written: AtomicU64,
    pub bytes_processed: AtomicU64,
    pub batches_written: AtomicU64,
}

impl ParquetWriterStats {
    pub fn entries_written(&self) -> u64 {
        self.entries_written.load(Ordering::Relaxed)
    }

    pub fn bytes_processed(&self) -> u64 {
        self.bytes_processed.load(Ordering::Relaxed)
    }
}

/// Handle for sending messages to the Parquet writer
#[derive(Clone)]
pub struct ParquetWriterHandle {
    sender: Sender<ParquetWriterMessage>,
    stats: Arc<ParquetWriterStats>,
    shutdown: Arc<AtomicBool>,
}

impl ParquetWriterHandle {
    /// Send an entry to be written
    pub fn send_entry(&self, entry: DbEntry) -> DbResult<()> {
        self.sender
            .send(ParquetWriterMessage::Entry(entry))
            .map_err(|_| DbError::ChannelClosed)
    }

    /// Send directory stats (stored separately or ignored)
    pub fn send_dir_stats(&self, path: String, stats: DirStats) -> DbResult<()> {
        self.sender
            .send(ParquetWriterMessage::DirStats { path, stats })
            .map_err(|_| DbError::ChannelClosed)
    }

    /// Request a flush of pending writes
    pub fn flush(&self) -> DbResult<()> {
        self.sender
            .send(ParquetWriterMessage::Flush)
            .map_err(|_| DbError::ChannelClosed)
    }

    /// Request shutdown
    pub fn shutdown(&self) -> DbResult<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        self.sender
            .send(ParquetWriterMessage::Shutdown)
            .map_err(|_| DbError::ChannelClosed)
    }

    /// Get writer statistics
    pub fn stats(&self) -> &ParquetWriterStats {
        &self.stats
    }
}

/// Parquet schema for filesystem entries
fn entries_schema() -> Schema {
    Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("parent_path", DataType::Utf8, true),
        Field::new("entry_type", DataType::Int32, false),
        Field::new("size", DataType::UInt64, false),
        Field::new("mtime", DataType::Int64, true),
        Field::new("atime", DataType::Int64, true),
        Field::new("ctime", DataType::Int64, true),
        Field::new("mode", DataType::UInt32, true),
        Field::new("uid", DataType::UInt32, true),
        Field::new("gid", DataType::UInt32, true),
        Field::new("nlink", DataType::UInt64, true),
        Field::new("inode", DataType::UInt64, false),
        Field::new("depth", DataType::UInt32, false),
    ])
}

/// Parquet schema for directory stats
fn dir_stats_schema() -> Schema {
    Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("direct_file_count", DataType::UInt64, false),
        Field::new("direct_dir_count", DataType::UInt64, false),
        Field::new("direct_symlink_count", DataType::UInt64, false),
        Field::new("direct_other_count", DataType::UInt64, false),
        Field::new("direct_bytes", DataType::UInt64, false),
    ])
}

/// Batched Parquet writer
pub struct BatchedParquetWriter {
    handle: Option<JoinHandle<DbResult<()>>>,
    writer_handle: ParquetWriterHandle,
}

impl BatchedParquetWriter {
    /// Create a new batched Parquet writer
    pub fn new(output_path: &Path, batch_size: usize, channel_size: usize) -> DbResult<Self> {
        let (sender, receiver) = bounded(channel_size);
        let stats = Arc::new(ParquetWriterStats::default());
        let shutdown = Arc::new(AtomicBool::new(false));

        let writer_handle = ParquetWriterHandle {
            sender,
            stats: Arc::clone(&stats),
            shutdown: Arc::clone(&shutdown),
        };

        let stats_clone = Arc::clone(&stats);

        // Create the Parquet file
        let file = File::create(output_path).map_err(|e| DbError::CreateFailed {
            path: output_path.to_path_buf(),
            reason: e.to_string(),
        })?;

        let schema = Arc::new(entries_schema());

        // Configure Parquet writer properties for optimal compression and speed
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_dictionary_enabled(true)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .set_max_row_group_size(100_000)
            .build();

        let arrow_writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).map_err(|e| {
                DbError::CreateFailed {
                    path: output_path.to_path_buf(),
                    reason: e.to_string(),
                }
            })?;

        let shutdown_clone = Arc::clone(&shutdown);

        // Spawn writer thread
        let handle = thread::Builder::new()
            .name("parquet-writer".into())
            .spawn(move || parquet_writer_thread(arrow_writer, receiver, stats_clone, batch_size, shutdown_clone))
            .map_err(|e| DbError::CreateFailed {
                path: output_path.to_path_buf(),
                reason: format!("Failed to spawn writer thread: {}", e),
            })?;

        Ok(Self {
            handle: Some(handle),
            writer_handle,
        })
    }

    /// Get a handle for sending messages to the writer
    pub fn handle(&self) -> ParquetWriterHandle {
        self.writer_handle.clone()
    }

    /// Wait for the writer to finish
    pub fn finish(mut self) -> DbResult<()> {
        let _ = self.writer_handle.shutdown();

        // Wait for thread to finish with timeout
        if let Some(handle) = self.handle.take() {
            let start = std::time::Instant::now();
            let timeout = Duration::from_secs(60);

            loop {
                if handle.is_finished() {
                    match handle.join() {
                        Ok(result) => result?,
                        Err(_) => {
                            tracing::warn!("Parquet writer thread panicked");
                        }
                    }
                    break;
                }

                if start.elapsed() > timeout {
                    tracing::warn!("Parquet writer thread did not finish in time");
                    break;
                }

                std::thread::sleep(Duration::from_millis(50));
            }
        }

        Ok(())
    }
}

/// Internal Parquet writer thread function
fn parquet_writer_thread(
    mut writer: ArrowWriter<File>,
    receiver: Receiver<ParquetWriterMessage>,
    stats: Arc<ParquetWriterStats>,
    batch_size: usize,
    shutdown: Arc<AtomicBool>,
) -> DbResult<()> {
    let mut entry_buffer: Vec<DbEntry> = Vec::with_capacity(batch_size);

    loop {
        // Check shutdown flag on EVERY iteration - critical for clean exit
        if shutdown.load(Ordering::Relaxed) {
            if !entry_buffer.is_empty() {
                let _ = flush_entries(&mut writer, &mut entry_buffer, &stats);
            }
            break;
        }

        match receiver.try_recv() {
            Ok(msg) => match msg {
                ParquetWriterMessage::Entry(entry) => {
                    if entry.entry_type.is_file() {
                        stats.bytes_processed.fetch_add(entry.size, Ordering::Relaxed);
                    }
                    entry_buffer.push(entry);

                    if entry_buffer.len() >= batch_size {
                        flush_entries(&mut writer, &mut entry_buffer, &stats)?;
                    }
                }
                ParquetWriterMessage::DirStats { .. } => {
                    // For simplicity, we skip dir_stats in Parquet output
                    // Could write to a separate file if needed
                }
                ParquetWriterMessage::Flush => {
                    if !entry_buffer.is_empty() {
                        flush_entries(&mut writer, &mut entry_buffer, &stats)?;
                    }
                }
                ParquetWriterMessage::Shutdown => {
                    if !entry_buffer.is_empty() {
                        flush_entries(&mut writer, &mut entry_buffer, &stats)?;
                    }
                    break;
                }
            },
            Err(TryRecvError::Empty) => {
                if !entry_buffer.is_empty() && entry_buffer.len() >= batch_size / 4 {
                    flush_entries(&mut writer, &mut entry_buffer, &stats)?;
                }

                match receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(msg) => match msg {
                        ParquetWriterMessage::Entry(entry) => {
                            if entry.entry_type.is_file() {
                                stats.bytes_processed.fetch_add(entry.size, Ordering::Relaxed);
                            }
                            entry_buffer.push(entry);
                        }
                        ParquetWriterMessage::DirStats { .. } => {}
                        ParquetWriterMessage::Flush => {
                            if !entry_buffer.is_empty() {
                                flush_entries(&mut writer, &mut entry_buffer, &stats)?;
                            }
                        }
                        ParquetWriterMessage::Shutdown => {
                            if !entry_buffer.is_empty() {
                                flush_entries(&mut writer, &mut entry_buffer, &stats)?;
                            }
                            break;
                        }
                    },
                    Err(_) => {}
                }
            }
            Err(TryRecvError::Disconnected) => {
                if !entry_buffer.is_empty() {
                    flush_entries(&mut writer, &mut entry_buffer, &stats)?;
                }
                break;
            }
        }
    }

    // Close the Parquet file
    writer.close().map_err(|e| {
        DbError::Transaction(format!("Failed to close Parquet file: {}", e))
    })?;

    Ok(())
}

/// Flush entry buffer to Parquet
fn flush_entries(
    writer: &mut ArrowWriter<File>,
    buffer: &mut Vec<DbEntry>,
    stats: &ParquetWriterStats,
) -> DbResult<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    let len = buffer.len();

    // Build arrays for each column
    let mut path_builder = StringBuilder::with_capacity(len, len * 64);
    let mut name_builder = StringBuilder::with_capacity(len, len * 32);
    let mut parent_path_builder = StringBuilder::with_capacity(len, len * 64);
    let mut entry_type_builder = Int32Builder::with_capacity(len);
    let mut size_builder = UInt64Builder::with_capacity(len);
    let mut mtime_builder = Int64Builder::with_capacity(len);
    let mut atime_builder = Int64Builder::with_capacity(len);
    let mut ctime_builder = Int64Builder::with_capacity(len);
    let mut mode_builder = UInt32Builder::with_capacity(len);
    let mut uid_builder = UInt32Builder::with_capacity(len);
    let mut gid_builder = UInt32Builder::with_capacity(len);
    let mut nlink_builder = UInt64Builder::with_capacity(len);
    let mut inode_builder = UInt64Builder::with_capacity(len);
    let mut depth_builder = UInt32Builder::with_capacity(len);

    for entry in buffer.drain(..) {
        path_builder.append_value(&entry.path);
        name_builder.append_value(&entry.name);

        match &entry.parent_path {
            Some(p) => parent_path_builder.append_value(p),
            None => parent_path_builder.append_null(),
        }

        entry_type_builder.append_value(entry.entry_type.as_db_int());
        size_builder.append_value(entry.size);

        match entry.mtime {
            Some(v) => mtime_builder.append_value(v),
            None => mtime_builder.append_null(),
        }
        match entry.atime {
            Some(v) => atime_builder.append_value(v),
            None => atime_builder.append_null(),
        }
        match entry.ctime {
            Some(v) => ctime_builder.append_value(v),
            None => ctime_builder.append_null(),
        }
        match entry.mode {
            Some(v) => mode_builder.append_value(v),
            None => mode_builder.append_null(),
        }
        match entry.uid {
            Some(v) => uid_builder.append_value(v),
            None => uid_builder.append_null(),
        }
        match entry.gid {
            Some(v) => gid_builder.append_value(v),
            None => gid_builder.append_null(),
        }
        match entry.nlink {
            Some(v) => nlink_builder.append_value(v),
            None => nlink_builder.append_null(),
        }

        inode_builder.append_value(entry.inode);
        depth_builder.append_value(entry.depth);
    }

    // Create record batch
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(path_builder.finish()),
        Arc::new(name_builder.finish()),
        Arc::new(parent_path_builder.finish()),
        Arc::new(entry_type_builder.finish()),
        Arc::new(size_builder.finish()),
        Arc::new(mtime_builder.finish()),
        Arc::new(atime_builder.finish()),
        Arc::new(ctime_builder.finish()),
        Arc::new(mode_builder.finish()),
        Arc::new(uid_builder.finish()),
        Arc::new(gid_builder.finish()),
        Arc::new(nlink_builder.finish()),
        Arc::new(inode_builder.finish()),
        Arc::new(depth_builder.finish()),
    ];

    let schema = Arc::new(entries_schema());
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| DbError::Transaction(format!("Failed to create record batch: {}", e)))?;

    writer.write(&batch).map_err(|e| {
        DbError::Transaction(format!("Failed to write Parquet batch: {}", e))
    })?;

    stats.entries_written.fetch_add(len as u64, Ordering::Relaxed);
    stats.batches_written.fetch_add(1, Ordering::Relaxed);

    Ok(())
}

/// Write directory stats to a separate Parquet file
pub fn write_dir_stats_parquet(
    output_path: &Path,
    stats_list: Vec<(String, DirStats)>,
) -> DbResult<()> {
    if stats_list.is_empty() {
        return Ok(());
    }

    let file = File::create(output_path).map_err(|e| DbError::CreateFailed {
        path: output_path.to_path_buf(),
        reason: e.to_string(),
    })?;

    let schema = Arc::new(dir_stats_schema());

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).map_err(|e| {
        DbError::CreateFailed {
            path: output_path.to_path_buf(),
            reason: e.to_string(),
        }
    })?;

    let len = stats_list.len();
    let mut path_builder = StringBuilder::with_capacity(len, len * 64);
    let mut file_count_builder = UInt64Builder::with_capacity(len);
    let mut dir_count_builder = UInt64Builder::with_capacity(len);
    let mut symlink_count_builder = UInt64Builder::with_capacity(len);
    let mut other_count_builder = UInt64Builder::with_capacity(len);
    let mut bytes_builder = UInt64Builder::with_capacity(len);

    for (path, ds) in stats_list {
        path_builder.append_value(&path);
        file_count_builder.append_value(ds.file_count);
        dir_count_builder.append_value(ds.dir_count);
        symlink_count_builder.append_value(ds.symlink_count);
        other_count_builder.append_value(ds.other_count);
        bytes_builder.append_value(ds.total_bytes);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(path_builder.finish()),
        Arc::new(file_count_builder.finish()),
        Arc::new(dir_count_builder.finish()),
        Arc::new(symlink_count_builder.finish()),
        Arc::new(other_count_builder.finish()),
        Arc::new(bytes_builder.finish()),
    ];

    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| DbError::Transaction(format!("Failed to create record batch: {}", e)))?;

    writer.write(&batch).map_err(|e| {
        DbError::Transaction(format!("Failed to write Parquet batch: {}", e))
    })?;

    writer.close().map_err(|e| {
        DbError::Transaction(format!("Failed to close Parquet file: {}", e))
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::EntryType;
    use tempfile::tempdir;

    #[test]
    fn test_parquet_writer_basic() {
        let dir = tempdir().unwrap();
        let output_path = dir.path().join("test.parquet");

        let writer = BatchedParquetWriter::new(&output_path, 100, 1000).unwrap();
        let handle = writer.handle();

        // Insert some entries
        for i in 0..10 {
            let entry = DbEntry {
                parent_path: if i == 0 { None } else { Some("/test".to_string()) },
                name: format!("file{}.txt", i),
                path: format!("/test/file{}.txt", i),
                entry_type: EntryType::File,
                size: 1024 * i as u64,
                mtime: Some(1234567890),
                atime: None,
                ctime: None,
                mode: Some(0o644),
                uid: Some(1000),
                gid: Some(1000),
                nlink: Some(1),
                inode: i as u64,
                depth: 1,
            };
            handle.send_entry(entry).unwrap();
        }

        writer.finish().unwrap();

        // Verify file exists and has content
        assert!(output_path.exists());
        let metadata = std::fs::metadata(&output_path).unwrap();
        assert!(metadata.len() > 0);
    }
}
