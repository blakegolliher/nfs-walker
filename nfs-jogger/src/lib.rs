//! nfs-jogger - Distributed NFS filesystem walker
//!
//! A distributed system for parallel filesystem processing, designed for
//! massive data migrations and indexing that scales with additional resources.
//!
//! # Architecture
//!
//! The system operates in two phases:
//!
//! ## Phase 1: Discovery (Bundler)
//! - Scans the filesystem to discover all paths
//! - Creates "bundles" of work (2000 files or 2TB by default)
//! - Pushes bundles to a Redis queue
//!
//! ## Phase 2: Processing (Workers)
//! - Workers pull bundles from the queue
//! - Each worker processes its bundle independently
//! - Results are written to a shared output
//!
//! # Scaling
//!
//! Add more resources by:
//! - Running multiple discovery processes on different subtrees
//! - Running multiple worker processes on different hosts
//! - All workers share the same Redis queue for coordination

pub mod bundle;
pub mod config;
pub mod coordinator;
pub mod discovery;
pub mod error;
pub mod nfs;
pub mod queue;
pub mod worker;

#[cfg(feature = "rocksdb")]
pub mod rocksdb;

pub use config::{JoggerConfig, NfsUrl};
pub use error::{JoggerError, Result};
