//! Content analysis module for checksum and file type detection
//!
//! This module provides functions for:
//! - Computing gxhash checksums for duplicate detection
//! - Detecting file types using magic bytes (MIME type detection)

pub mod checksum;
pub mod filetype;

pub use checksum::compute_gxhash;
pub use filetype::detect_file_type;
