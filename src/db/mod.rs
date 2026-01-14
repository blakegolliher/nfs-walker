//! Database module for SQLite and Parquet storage
//!
//! This module provides high-performance storage for filesystem entries.
//! It supports multiple output formats:
//! - SQLite: Optimal for ad-hoc queries and small-medium datasets
//! - Parquet: Optimal for analytics, compression, and large datasets
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │              Worker Threads (N)                      │
//! │  - Send entries via channel                         │
//! └─────────────────────┬───────────────────────────────┘
//!                       │ WriterMessage
//!                       ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │         BatchedWriter / ParquetWriter Thread         │
//! │  - Receives entries from channel                    │
//! │  - Buffers entries in memory                        │
//! │  - Flushes batches                                  │
//! └─────────────────────┬───────────────────────────────┘
//!                       │
//!                       ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │              SQLite / Parquet File                   │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Performance
//!
//! SQLite (10K batch size, WAL mode):
//! - >50K inserts/second sustained
//! - <200MB memory for batching
//!
//! Parquet (ZSTD compression):
//! - >100K inserts/second
//! - 2-10x smaller output files
//! - Excellent for analytics with DuckDB

pub mod parquet_writer;
pub mod schema;
pub mod unified_writer;
pub mod writer;

pub use parquet_writer::{BatchedParquetWriter, ParquetWriterHandle, ParquetWriterStats};
pub use schema::{create_database, create_indexes, keys, optimize_for_reads};
pub use unified_writer::{UnifiedWriter, UnifiedWriterHandle};
pub use writer::{BatchedWriter, WriterHandle, WriterMessage, WriterStats};
