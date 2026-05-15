//! Parquet output module.
//!
//! Streams walker output into sharded parquet files. The layout matches
//! what DuckDB / DataFusion / Polars expect:
//! `scans/<scan_id>/part-rNN-SSSSS.parquet` + a `metadata.json` listing
//! the union of part files.
//!
//! # Module structure
//!
//! - `schema`: Canonical Arrow schema definition (24 columns)
//! - `builder`: Shared row-builder used by the writer
//! - `direct_writer`: In-process streaming writer fed by the walker

pub mod builder;
pub mod direct_writer;
pub mod schema;

pub use builder::{RowBuilder, RowContext};
pub use direct_writer::{
    spawn_direct_parquet_writers, write_metadata_json as write_direct_metadata_json,
    DirectWriteConfig, DirectWritePool, ParquetCompression, ShardSummary,
};
pub use schema::{parquet_schema, parquet_schema_ref};
