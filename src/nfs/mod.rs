//! NFS access module
//!
//! This module provides direct NFS protocol access using libnfs,
//! bypassing the kernel NFS client for better performance and control.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │                    NfsConnection                     │
//! │  - One per worker thread (not thread-safe)          │
//! │  - RAII cleanup (unmount + destroy on drop)         │
//! │  - Uses READDIRPLUS for efficient directory listing │
//! └─────────────────────────────────────────────────────┘
//!                          │
//!                          ▼
//! ┌─────────────────────────────────────────────────────┐
//! │                   libnfs (C FFI)                     │
//! │  - Direct NFS protocol implementation               │
//! │  - NFSv3 and NFSv4 support                          │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```no_run
//! use nfs_walker::nfs::{NfsConnection, NfsConnectionBuilder};
//! use nfs_walker::config::NfsUrl;
//! use std::time::Duration;
//!
//! let url = NfsUrl::parse("nfs://server/export").unwrap();
//!
//! // Simple connection
//! let conn = NfsConnection::connect_to(&url, Duration::from_secs(30)).unwrap();
//!
//! // With retries
//! let conn = NfsConnectionBuilder::new(url)
//!     .timeout(Duration::from_secs(30))
//!     .retries(3)
//!     .connect()
//!     .unwrap();
//!
//! // Read a directory
//! let entries = conn.readdir_plus("/data").unwrap();
//! for entry in entries {
//!     if !entry.is_special() {
//!         println!("{}: {:?}", entry.name, entry.entry_type);
//!     }
//! }
//! ```

mod connection;
pub mod pool;
pub mod types;

pub use connection::{NfsConnection, NfsConnectionBuilder};
pub use pool::NfsConnectionPool;
pub use types::{DbEntry, DirStats, EntryType, NfsDirEntry, NfsStat, Permissions};
