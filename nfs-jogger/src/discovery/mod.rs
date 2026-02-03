//! Discovery module for filesystem scanning and bundle creation
//!
//! The discovery phase scans the NFS filesystem and creates work bundles
//! that are pushed to the Redis queue for processing by workers.

mod scanner;

pub use scanner::{DiscoveryScanner, DiscoveryProgress, DiscoveryStats};
