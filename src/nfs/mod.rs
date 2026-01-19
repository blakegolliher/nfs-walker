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
//! │  - Uses READDIR for names, GETATTR for attributes   │
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
//! // Read a directory (names only)
//! let handle = conn.opendir_names_only("/data").unwrap();
//! while let Some(entry) = handle.readdir() {
//!     if !entry.is_special() {
//!         println!("{}", entry.name);
//!     }
//! }
//! ```

pub mod connection;
pub mod dns_resolver;
pub mod types;

pub use connection::{resolve_dns, NfsConnection, NfsConnectionBuilder, NfsDirHandle};
pub use connection::ffi;
pub use dns_resolver::{DnsResolver, DEFAULT_DNS_REFRESH_SECS};
pub use types::{DbEntry, DirStats, EntryType, NfsDirEntry, NfsStat, Permissions};
