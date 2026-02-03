//! Bundle module for work distribution
//!
//! Bundles are the unit of work distribution in nfs-jogger.
//! Each bundle contains a set of filesystem paths to process.

mod types;

pub use types::{
    Bundle, BundleBuilder, BundleMetadata, BundleState, BundleStats,
    PathEntry, DEFAULT_MAX_PATHS, DEFAULT_MAX_SIZE_BYTES,
};
