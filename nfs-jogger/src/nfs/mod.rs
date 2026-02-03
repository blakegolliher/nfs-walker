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
//! use nfs_jogger::nfs::{NfsConnection, NfsConnectionBuilder};
//! use nfs_jogger::config::NfsUrl;
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
//! let handle = conn.opendir("/data").unwrap();
//! while let Some(entry) = handle.readdir() {
//!     if !entry.is_special() {
//!         println!("{}", entry.name);
//!     }
//! }
//! ```

// Pre-generated FFI bindings for libnfs
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(dead_code)]
#[allow(clippy::all)]
pub mod bindings;

pub mod connection;
pub mod dns_resolver;
pub mod types;

pub use connection::{resolve_dns, BigDirScanResult, NfsConnection, NfsConnectionBuilder, NfsDirHandle, RawRpcContext};
pub use connection::ffi;
pub use dns_resolver::{DnsResolver, DEFAULT_DNS_REFRESH_SECS};
pub use types::{DbEntry, DirStats, EntryType, NfsDirEntry, NfsStat, Permissions};
