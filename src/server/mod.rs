//! Analytics server module.
//!
//! Provides a REST API backed by DataFusion for querying Parquet scan data.

pub mod catalog;
pub mod context;
pub mod executor;
pub mod routes;

pub use routes::serve;
