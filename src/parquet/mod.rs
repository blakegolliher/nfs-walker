//! Parquet export module
//!
//! Converts RocksDB scan data to Parquet files optimized for DataFusion queries.
//!
//! # Module Structure
//!
//! - `schema`: Canonical Arrow schema definition (24 columns)
//! - `builder`: Shared row-builder used by every parquet code path
//! - `convert`: RocksDB → Parquet streaming conversion (post-scan, single-threaded)
//! - `parallel_convert`: SST-balanced multi-shard post-scan converter
//! - `direct_writer`: in-process streaming writer used by the walker
//!   when `--output-format parquet` is selected (no RocksDB involved)

pub mod builder;
pub mod convert;
pub mod direct_writer;
pub mod parallel_convert;
pub mod schema;

pub use builder::{RowBuilder, RowContext};
pub use convert::{convert_rocks_to_parquet, ExportConfig, ExportStats};
pub use direct_writer::{
    spawn_direct_parquet_writers, write_metadata_json as write_direct_metadata_json,
    DirectWriteConfig, DirectWritePool, ParquetCompression, ShardSummary,
};
pub use parallel_convert::{
    parallel_convert_rocks_to_parquet, ParallelExportConfig, ParallelProgressCallback,
};
pub use schema::{parquet_schema, parquet_schema_ref};
