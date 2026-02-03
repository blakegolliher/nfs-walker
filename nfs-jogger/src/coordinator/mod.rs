//! Coordinator module for monitoring and managing the distributed system
//!
//! The coordinator provides:
//! - Status monitoring for all workers
//! - Queue statistics and health checks
//! - Failed bundle retry management
//! - System-wide progress tracking

mod manager;

pub use manager::{Coordinator, SystemStatus, WorkerInfo};
