//! Parquet export module
//!
//! Converts RocksDB scan data to Parquet files optimized for DataFusion queries.
//!
//! # Module Structure
//!
//! - `schema`: Canonical Arrow schema definition (18 columns)
//! - `builder`: Shared row-builder used by both convert and streaming
//! - `convert`: RocksDB → Parquet streaming conversion (post-scan)
//! - `streaming`: Live-during-scan rotating Parquet writer

pub mod builder;
pub mod convert;
pub mod parallel_convert;
pub mod schema;
pub mod streaming;

pub use builder::{RowBuilder, RowContext};
pub use convert::{convert_rocks_to_parquet, ExportConfig, ExportStats};
pub use parallel_convert::{
    parallel_convert_rocks_to_parquet, ParallelExportConfig, ParallelProgressCallback,
};
pub use schema::{parquet_schema, parquet_schema_ref};
pub use streaming::{StreamingParquetConfig, StreamingParquetStats, StreamingParquetWriter};
