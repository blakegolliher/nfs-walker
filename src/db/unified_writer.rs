//! Unified writer abstraction for multiple output formats
//!
//! This module provides a common interface for writing filesystem entries
//! to different storage backends (SQLite, Parquet, Arrow).

use crate::config::OutputFormat;
use crate::db::parquet_writer::{BatchedParquetWriter, ParquetWriterHandle};
use crate::db::writer::{BatchedWriter, WriterHandle};
use crate::error::DbResult;
use crate::nfs::types::{DbEntry, DirStats};
use std::path::Path;

/// Unified writer handle that abstracts over different output formats
#[derive(Clone)]
pub enum UnifiedWriterHandle {
    Sqlite(WriterHandle),
    Parquet(ParquetWriterHandle),
}

impl UnifiedWriterHandle {
    /// Send an entry to be written
    pub fn send_entry(&self, entry: DbEntry) -> DbResult<()> {
        match self {
            UnifiedWriterHandle::Sqlite(h) => h.send_entry(entry),
            UnifiedWriterHandle::Parquet(h) => h.send_entry(entry),
        }
    }

    /// Send directory stats
    pub fn send_dir_stats(&self, path: String, stats: DirStats) -> DbResult<()> {
        match self {
            UnifiedWriterHandle::Sqlite(h) => h.send_dir_stats(path, stats),
            UnifiedWriterHandle::Parquet(h) => h.send_dir_stats(path, stats),
        }
    }

    /// Request a flush of pending writes
    pub fn flush(&self) -> DbResult<()> {
        match self {
            UnifiedWriterHandle::Sqlite(h) => h.flush(),
            UnifiedWriterHandle::Parquet(h) => h.flush(),
        }
    }

    /// Get entries written count
    pub fn entries_written(&self) -> u64 {
        match self {
            UnifiedWriterHandle::Sqlite(h) => h.stats().entries_written(),
            UnifiedWriterHandle::Parquet(h) => h.stats().entries_written(),
        }
    }
}

/// Unified batched writer that can use either SQLite or Parquet backend
pub enum UnifiedWriter {
    Sqlite(BatchedWriter),
    Parquet(BatchedParquetWriter),
}

impl UnifiedWriter {
    /// Create a new unified writer based on output format
    pub fn new(
        output_path: &Path,
        batch_size: usize,
        channel_size: usize,
        format: OutputFormat,
    ) -> DbResult<Self> {
        match format {
            OutputFormat::Sqlite => {
                let writer = BatchedWriter::new(output_path, batch_size, channel_size)?;
                Ok(UnifiedWriter::Sqlite(writer))
            }
            OutputFormat::Parquet | OutputFormat::Arrow => {
                // For Arrow format, we still use Parquet (Arrow IPC could be added later)
                let writer = BatchedParquetWriter::new(output_path, batch_size, channel_size)?;
                Ok(UnifiedWriter::Parquet(writer))
            }
        }
    }

    /// Get a handle for sending messages to the writer
    pub fn handle(&self) -> UnifiedWriterHandle {
        match self {
            UnifiedWriter::Sqlite(w) => UnifiedWriterHandle::Sqlite(w.handle()),
            UnifiedWriter::Parquet(w) => UnifiedWriterHandle::Parquet(w.handle()),
        }
    }

    /// Wait for the writer to finish and finalize output
    pub fn finish(self) -> DbResult<()> {
        match self {
            UnifiedWriter::Sqlite(w) => w.finish(),
            UnifiedWriter::Parquet(w) => w.finish(),
        }
    }
}
