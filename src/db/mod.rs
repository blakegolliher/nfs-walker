//! Database module for SQLite storage
//!
//! This module provides storage for filesystem entries using SQLite.
//!
//! # Architecture
//!
//! The simple walker writes directly to SQLite using batched transactions
//! for efficient bulk inserts.

pub mod schema;

pub use schema::{create_database, create_indexes, keys, optimize_for_reads, set_walk_info};
