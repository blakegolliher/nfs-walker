//! Worker module for bundle processing
//!
//! Workers pull bundles from the Redis queue and process them,
//! writing results to a local database.

mod processor;

pub use processor::{BundleWorker, WorkerProgress, WorkerStats};
