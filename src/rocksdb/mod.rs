//! RocksDB storage module
//!
//! Provides high-performance storage for fast writes during scans,
//! with dual indexes (path + inode) for incremental scan baseline lookups.
//!
//! # Features
//!
//! - **Fast writes**: Write-optimized configuration for scan workloads
//! - **Dual indexes**: Lookup by path or inode in O(log n)
//! - **Compact storage**: bincode serialization (~100 bytes/entry)
//! - **Conversion**: Export to SQLite for queries and analysis
//!
//! # Module Structure
//!
//! - `schema`: Column families, key encoding, RocksEntry struct
//! - `writer`: RocksDB writer thread for scan output
//! - `reader`: Baseline reader for incremental scans
//! - `convert`: RocksDB to SQLite conversion

pub mod convert;
pub mod reader;
pub mod schema;
pub mod stats;
pub mod writer;

pub use convert::{convert_rocks_to_sqlite, ConvertConfig, ConvertStats};
pub use reader::{compare_entry, ChangeType, RocksBaseline};
pub use schema::{meta_keys, RocksEntry, RocksHandle};
pub use stats::{
    compute_stats, find_duplicates, find_hardlink_groups, largest_directories, largest_files,
    most_hardlinks, oldest_files, stats_by_extension, stats_by_file_type, stats_by_gid,
    stats_by_uid, DbStats, DuplicateGroup, ExtensionStats, FileTypeStats, HardLinkGroup,
    OwnerStats,
};
pub use writer::{finalize_rocks_db, RocksWriter, RocksWriterConfig, WalkStatsSnapshot};
