//! nfs-walker - Fast NFS Filesystem Scanner
//!
//! A high-performance tool for scanning NFS filesystems at scale, using
//! READDIRPLUS for efficient directory traversal with work-stealing parallelism.
//! Outputs to RocksDB; convert to SQLite or export to Parquet after the scan.
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
//! - **RocksDB Storage**: Fast primary storage; export to SQLite (`export-sql`)
//!   or Parquet (`export-parquet`) after the scan.
//!
//! - **Big-Dir Hunt Mode**: Specialized mode to quickly find directories
//!   with many files (useful for identifying hot spots).
//!
//! # Architecture
//!
//! ```text
//! Directory Queue (crossbeam deque - work stealing)
//! │
//! ├── Worker 0: pop dir → READDIRPLUS → send entries → push subdirs
//! ├── Worker 1: pop dir → READDIRPLUS → send entries → push subdirs
//! └── Worker N: pop dir → READDIRPLUS → send entries → push subdirs
//! │
//! └── Writer Thread: recv entries → batch insert to RocksDB
//! ```
//!
//! # Example
//!
//! ```bash
//! # Basic scan to RocksDB
//! nfs-walker nfs://server/export -o scan.rocks
//!
//! # Export to SQLite for ad-hoc SQL
//! nfs-walker export-sql scan.rocks scan.db --progress
//!
//! ```

pub mod config;
pub mod content;
pub mod db;
pub mod error;
pub mod nfs;
pub mod progress;
pub mod rocksdb;
pub mod scanlog;
#[cfg(feature = "parquet")]
pub mod parquet;
#[cfg(feature = "server")]
pub mod server;
pub mod walker;

pub use config::{CliArgs, NfsUrl, WalkConfig};
pub use error::{Result, WalkerError};
pub use walker::{SimpleWalker, WalkProgress, WalkStats};
