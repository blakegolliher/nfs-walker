//! NFS filesystem walker.
//!
//! Work-stealing parallel walker built on libnfs READDIRPLUS. Output
//! streams to sharded Parquet via `crate::parquet::direct_writer` —
//! one writer thread per shard, routed by `sharding::path_to_shard`.
//!
//! See `docs/ARCHITECTURE.md` for the full architecture diagram.

pub mod sharding;
pub mod simple;

pub use simple::{SimpleWalker, WalkProgress, WalkStats};
