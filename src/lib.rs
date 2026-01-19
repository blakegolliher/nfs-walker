//! nfs-walker - Simple NFS Filesystem Scanner
//!
//! A tool for scanning NFS filesystems at scale, outputting results to SQLite
//! for later analysis. Uses READDIR + parallel GETATTR for simplicity.
//!
//! # Features
//!
//! - **Direct NFS Protocol Access**: Uses libnfs for direct NFS protocol
//!   communication, bypassing the kernel NFS client for better performance.
//!
//! - **Parallel GETATTR**: Multiple worker threads each with their own
//!   NFS connection for parallel stat calls.
//!
//! - **SQLite Output**: Results stored in SQLite for powerful post-walk
//!   analysis with SQL queries.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        NFS Server                                │
//! └─────────────────────────────┬───────────────────────────────────┘
//!                               │
//!             ┌─────────────────┼─────────────────┐
//!             │                 │                 │
//!       READDIR            GETATTR            GETATTR
//!      (names)             Worker 1           Worker N
//!             │                 │                 │
//!             └─────────────────┼─────────────────┘
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
//! # With progress and more workers
//! nfs-walker nfs://192.168.1.100/data -w 8 -p -o scan.db
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

pub use config::{CliArgs, NfsUrl, WalkConfig};
pub use error::{Result, WalkerError};
pub use walker::{AsyncWalker, SimpleWalker, WalkProgress, WalkStats};
