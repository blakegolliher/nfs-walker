//! Parallel filesystem walker
//!
//! This module implements a high-performance parallel directory walker
//! using a work-stealing queue pattern with backpressure support.
//!
//! # Architecture
//!
//! ```text
//!                     ┌─────────────────────────┐
//!                     │     WalkCoordinator     │
//!                     │  - Setup & teardown     │
//!                     │  - Progress monitoring  │
//!                     │  - Signal handling      │
//!                     └───────────┬─────────────┘
//!                                 │
//!                     ┌───────────▼─────────────┐
//!                     │       WorkQueue         │
//!                     │  - Bounded capacity     │
//!                     │  - Backpressure         │
//!                     │  - Completion detection │
//!                     └───────────┬─────────────┘
//!                                 │
//!       ┌─────────────────────────┼─────────────────────────┐
//!       │                         │                         │
//! ┌─────▼─────┐             ┌─────▼─────┐             ┌─────▼─────┐
//! │  Worker 1 │             │  Worker 2 │             │  Worker N │
//! │  - NFS    │             │  - NFS    │             │  - NFS    │
//! │  - Stats  │             │  - Stats  │             │  - Stats  │
//! └───────────┘             └───────────┘             └───────────┘
//! ```
//!
//! # Work Distribution
//!
//! 1. Queue is seeded with root directory
//! 2. Workers pull directories from queue
//! 3. Workers read directory contents via READDIRPLUS
//! 4. Subdirectories are pushed back to queue
//! 5. When queue is full, backpressure is applied
//! 6. Walk completes when queue is empty and all workers idle

pub mod async_coordinator;
pub mod coordinator;
pub mod queue;
pub mod worker;

pub use async_coordinator::{AsyncWalkCoordinator, AsyncWalkResult, AsyncWalkStats};
pub use coordinator::{WalkCoordinator, WalkProgress, WalkResult};
pub use queue::{DirTask, EntryQueue, QueueStats, WorkQueue, LARGE_DIR_THRESHOLD};
pub use worker::{Worker, WorkerStats};
