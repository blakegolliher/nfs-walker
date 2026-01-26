//! Simple NFS filesystem walker
//!
//! This module implements a straightforward parallel directory walker
//! using READDIR for names and parallel GETATTR for file attributes.
//!
//! # Architecture
//!
//! ```text
//!                     ┌─────────────────────────┐
//!                     │     SimpleWalker        │
//!                     │  - Coordinator thread   │
//!                     │  - READDIR (names only) │
//!                     └───────────┬─────────────┘
//!                                 │
//!       ┌─────────────────────────┼─────────────────────────┐
//!       │                         │                         │
//! ┌─────▼─────┐             ┌─────▼─────┐             ┌─────▼─────┐
//! │  Worker 1 │             │  Worker 2 │             │  Worker N │
//! │  GETATTR  │             │  GETATTR  │             │  GETATTR  │
//! │  SQLite   │             │  SQLite   │             │  SQLite   │
//! └───────────┘             └───────────┘             └───────────┘
//! ```

pub mod simple;

pub use simple::{SimpleWalker, WalkProgress, WalkStats};
