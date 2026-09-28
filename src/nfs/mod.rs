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
//! │  - READDIRPLUS: names + attributes + file handles   │
//! │    in one RPC (sync loop or pipelined submit/pump)  │
//! └─────────────────────────────────────────────────────┘
//!                          │
//!                          ▼
//! ┌─────────────────────────────────────────────────────┐
//! │                   libnfs (C FFI)                     │
//! │  - Direct NFS protocol implementation               │
//! │  - NFSv3 only (nfs_set_version(3) at init)          │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```no_run
//! use nfs_walker::nfs::NfsConnectionBuilder;
//! use nfs_walker::config::NfsUrl;
//! use std::time::Duration;
//!
//! let url = NfsUrl::parse("nfs://server/export").unwrap();
//! let conn = NfsConnectionBuilder::new(url)
//!     .timeout(Duration::from_secs(30))
//!     .retries(3)
//!     .connect()
//!     .unwrap();
//!
//! // Stream a directory with cached-file-handle READDIRPLUS.
//! let n = conn
//!     .readdir_plus_with_fh("/data", 5000, |chunk| {
//!         for entry in &chunk {
//!             println!("{}", entry.name);
//!         }
//!         true // keep reading
//!     })
//!     .unwrap();
//! println!("{n} entries");
//! ```

// Pre-generated FFI bindings for libnfs
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(dead_code)]
#[allow(clippy::all)]
pub mod bindings;

pub mod connection;
pub mod types;

pub use connection::ffi;
pub use connection::{resolve_dns, NfsConnection, NfsConnectionBuilder};
pub use types::{DbEntry, EntryType, NfsDirEntry, NfsStat};
