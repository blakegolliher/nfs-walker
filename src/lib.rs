//! nfs-walker - Fast NFS Filesystem Scanner
//!
//! A high-performance tool for scanning NFS filesystems at scale, using
//! READDIRPLUS for efficient directory traversal with work-stealing parallelism.
//! Output streams directly to sharded Parquet files
//! (`scans/<scan_id>/part-rNN-SSSSS.parquet` + `metadata.json`), which
//! DuckDB / DataFusion / Polars can read without any post-hoc step.
//!
//! # Features
//!
//! - **Direct NFS Protocol Access**: Uses libnfs for direct NFS protocol
//!   communication, bypassing the kernel NFS client for better performance.
//!
//! - **READDIRPLUS**: Gets directory entries AND attributes in a single RPC,
//!   eliminating separate GETATTR calls for dramatic speedup.
//!
//! - **Work-Stealing Parallelism**: Multiple workers with work-stealing deques
//!   for efficient load balancing across workers.
//!
//! - **Sharded Parquet Output**: N independent writer threads each owning
//!   their own Arrow builder + ZSTD encoder; the path-keyspace is split
//!   via `gxhash % N` so writers never contend.
//!
//! # Architecture
//!
//! ```text
//! Directory Queue (crossbeam deque - work stealing)
//! │
//! ├── Worker 0: pop dir → READDIRPLUS → ShardedSender(entry) → push subdirs
//! ├── Worker 1: pop dir → READDIRPLUS → ShardedSender(entry) → push subdirs
//! └── Worker N: pop dir → READDIRPLUS → ShardedSender(entry) → push subdirs
//! │
//! └── N Parquet Writers: recv batch → row group → part file
//! ```
//!
//! # Example
//!
//! ```bash
//! # Scan to Parquet
//! nfs-walker nfs://server/export -o scan.parquet -w 32
//!
//! # Query directly with DuckDB — no conversion step
//! duckdb -c "SELECT count(*) FROM 'scan.parquet/scans/*/part-*.parquet'"
//! ```

pub mod config;
pub mod error;
pub mod nfs;
pub mod parquet;
pub mod progress;
pub mod scanlog;
#[cfg(feature = "server")]
pub mod server;
pub mod walker;

pub use config::{CliArgs, NfsUrl, WalkConfig};
pub use error::{Result, WalkerError};
pub use walker::{SimpleWalker, WalkProgress, WalkStats};
