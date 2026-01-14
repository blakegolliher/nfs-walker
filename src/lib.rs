//! nfs-walker - High-Performance NFS Filesystem Scanner
//!
//! A tool for scanning NFS filesystems at scale, outputting results to SQLite
//! for later analysis. Designed to handle billions of files with minimal
//! memory footprint.
//!
//! # Features
//!
//! - **Direct NFS Protocol Access**: Uses libnfs for direct NFS protocol
//!   communication, bypassing the kernel NFS client for better performance.
//!
//! - **Parallel Scanning**: Multiple worker threads each with their own
//!   NFS connection for maximum throughput.
//!
//! - **Memory Efficient**: Bounded work queue with backpressure prevents
//!   memory explosion on deep/wide filesystems.
//!
//! - **SQLite Output**: Results stored in SQLite for powerful post-walk
//!   analysis with SQL queries.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        NFS Server                                │
//! │                    (NFSv3 or NFSv4)                              │
//! └─────────────────────────────┬───────────────────────────────────┘
//!                               │
//!                               │ READDIRPLUS
//!                               ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Worker Threads                              │
//! │  ┌─────────┐  ┌─────────┐  ┌─────────┐         ┌─────────┐     │
//! │  │Worker 1 │  │Worker 2 │  │Worker 3 │  ...    │Worker N │     │
//! │  │ libnfs  │  │ libnfs  │  │ libnfs  │         │ libnfs  │     │
//! │  └────┬────┘  └────┬────┘  └────┬────┘         └────┬────┘     │
//! │       │            │            │                    │          │
//! │       └────────────┼────────────┼────────────────────┘          │
//! │                    │            │                               │
//! │                    ▼            ▼                               │
//! │            ┌──────────────────────────┐                         │
//! │            │     Work Queue           │                         │
//! │            │  (crossbeam bounded)     │                         │
//! │            │  - Backpressure support  │                         │
//! │            └──────────────────────────┘                         │
//! │                         │                                       │
//! │                         ▼                                       │
//! │            ┌──────────────────────────┐                         │
//! │            │    Batched DB Writer     │                         │
//! │            │  - 10K entries/batch     │                         │
//! │            │  - WAL mode              │                         │
//! │            └──────────────────────────┘                         │
//! └─────────────────────────────────────────────────────────────────┘
//!                               │
//!                               ▼
//!                    ┌──────────────────┐
//!                    │   SQLite DB      │
//!                    │   (walk.db)      │
//!                    └──────────────────┘
//! ```
//!
//! # Example
//!
//! ```bash
//! # Basic scan
//! nfs-walker nfs://server/export -o scan.db
//!
//! # With progress and high parallelism
//! nfs-walker nfs://192.168.1.100/data -w 64 -p -o scan.db
//!
//! # Query results
//! sqlite3 scan.db "SELECT path, size FROM entries WHERE size > 1000000000"
//! ```

pub mod config;
pub mod db;
pub mod error;
pub mod nfs;
pub mod progress;
pub mod walker;

pub use config::{CliArgs, NfsUrl, OutputFormat, WalkConfig};
pub use error::{Result, WalkerError};
pub use walker::{AsyncWalkCoordinator, AsyncWalkResult, WalkCoordinator, WalkResult};
