//! Parquet export module
//!
//! Converts RocksDB scan data to Parquet files optimized for DataFusion queries.
//!
//! # Module Structure
//!
//! - `schema`: Canonical Arrow schema definition (18 columns)
//! - `convert`: RocksDB → Parquet streaming conversion

pub mod convert;
pub mod schema;

pub use convert::{convert_rocks_to_parquet, ExportConfig, ExportStats};
pub use schema::{parquet_schema, parquet_schema_ref};
