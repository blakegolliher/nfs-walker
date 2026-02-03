//! Bundle types and data structures
//!
//! A Bundle represents a discrete unit of work containing filesystem paths
//! to be processed by a worker node.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{BundleError, Result};

/// Default maximum number of paths per bundle
pub const DEFAULT_MAX_PATHS: usize = 2000;

/// Default maximum total size per bundle (2TB)
pub const DEFAULT_MAX_SIZE_BYTES: u64 = 2 * 1024 * 1024 * 1024 * 1024;

/// State of a bundle in the processing pipeline
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BundleState {
    /// Bundle has been created but not yet queued
    Created,
    /// Bundle is in the queue waiting to be processed
    Queued,
    /// Bundle is currently being processed by a worker
    Processing,
    /// Bundle has been successfully processed
    Completed,
    /// Bundle processing failed
    Failed,
    /// Bundle processing was abandoned (worker died)
    Abandoned,
}

impl std::fmt::Display for BundleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleState::Created => write!(f, "created"),
            BundleState::Queued => write!(f, "queued"),
            BundleState::Processing => write!(f, "processing"),
            BundleState::Completed => write!(f, "completed"),
            BundleState::Failed => write!(f, "failed"),
            BundleState::Abandoned => write!(f, "abandoned"),
        }
    }
}

/// A single path entry in a bundle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathEntry {
    /// Full path from the NFS mount root
    pub path: String,
    /// Whether this is a directory (true) or file (false)
    pub is_directory: bool,
    /// Estimated size in bytes (0 for directories)
    pub estimated_size: u64,
    /// Directory depth from root
    pub depth: u32,
}

impl PathEntry {
    /// Create a new file path entry
    pub fn file(path: String, size: u64, depth: u32) -> Self {
        Self {
            path,
            is_directory: false,
            estimated_size: size,
            depth,
        }
    }

    /// Create a new directory path entry
    pub fn directory(path: String, depth: u32) -> Self {
        Self {
            path,
            is_directory: true,
            estimated_size: 0,
            depth,
        }
    }
}

/// Metadata about a bundle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleMetadata {
    /// Unique bundle identifier
    pub id: String,
    /// When the bundle was created
    pub created_at: DateTime<Utc>,
    /// NFS server this bundle is from
    pub nfs_server: String,
    /// NFS export path
    pub nfs_export: String,
    /// Common path prefix for all entries (optimization)
    pub path_prefix: String,
    /// Current state of the bundle
    pub state: BundleState,
    /// Worker ID currently processing (if any)
    pub worker_id: Option<String>,
    /// When processing started (if applicable)
    pub processing_started: Option<DateTime<Utc>>,
    /// When processing completed (if applicable)
    pub processing_completed: Option<DateTime<Utc>>,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Error message if failed
    pub error_message: Option<String>,
}

impl BundleMetadata {
    /// Create new metadata for a bundle
    pub fn new(nfs_server: &str, nfs_export: &str, path_prefix: &str) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            created_at: Utc::now(),
            nfs_server: nfs_server.to_string(),
            nfs_export: nfs_export.to_string(),
            path_prefix: path_prefix.to_string(),
            state: BundleState::Created,
            worker_id: None,
            processing_started: None,
            processing_completed: None,
            retry_count: 0,
            error_message: None,
        }
    }

    /// Mark as queued
    pub fn mark_queued(&mut self) {
        self.state = BundleState::Queued;
    }

    /// Mark as being processed by a worker
    pub fn mark_processing(&mut self, worker_id: &str) {
        self.state = BundleState::Processing;
        self.worker_id = Some(worker_id.to_string());
        self.processing_started = Some(Utc::now());
    }

    /// Mark as completed
    pub fn mark_completed(&mut self) {
        self.state = BundleState::Completed;
        self.processing_completed = Some(Utc::now());
    }

    /// Mark as failed
    pub fn mark_failed(&mut self, error: &str) {
        self.state = BundleState::Failed;
        self.error_message = Some(error.to_string());
        self.processing_completed = Some(Utc::now());
    }

    /// Mark as abandoned (worker died)
    pub fn mark_abandoned(&mut self) {
        self.state = BundleState::Abandoned;
        self.worker_id = None;
        self.retry_count += 1;
    }
}

/// Statistics about a bundle
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BundleStats {
    /// Number of file paths
    pub file_count: u64,
    /// Number of directory paths
    pub dir_count: u64,
    /// Total estimated size in bytes
    pub total_bytes: u64,
    /// Minimum depth
    pub min_depth: u32,
    /// Maximum depth
    pub max_depth: u32,
}

impl BundleStats {
    /// Add a path entry to the stats
    pub fn add_entry(&mut self, entry: &PathEntry) {
        if entry.is_directory {
            self.dir_count += 1;
        } else {
            self.file_count += 1;
            self.total_bytes += entry.estimated_size;
        }
        if self.file_count + self.dir_count == 1 {
            self.min_depth = entry.depth;
            self.max_depth = entry.depth;
        } else {
            self.min_depth = self.min_depth.min(entry.depth);
            self.max_depth = self.max_depth.max(entry.depth);
        }
    }

    /// Total number of entries
    pub fn total_entries(&self) -> u64 {
        self.file_count + self.dir_count
    }
}

/// A bundle of filesystem paths to process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bundle {
    /// Bundle metadata
    pub metadata: BundleMetadata,
    /// Path entries to process
    pub paths: Vec<PathEntry>,
    /// Bundle statistics
    pub stats: BundleStats,
}

impl Bundle {
    /// Create a new empty bundle
    pub fn new(nfs_server: &str, nfs_export: &str, path_prefix: &str) -> Self {
        Self {
            metadata: BundleMetadata::new(nfs_server, nfs_export, path_prefix),
            paths: Vec::new(),
            stats: BundleStats::default(),
        }
    }

    /// Get the bundle ID
    pub fn id(&self) -> &str {
        &self.metadata.id
    }

    /// Get the bundle state
    pub fn state(&self) -> BundleState {
        self.metadata.state
    }

    /// Check if bundle is empty
    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.paths.len()
    }

    /// Add a path entry
    pub fn add_path(&mut self, entry: PathEntry) {
        self.stats.add_entry(&entry);
        self.paths.push(entry);
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| {
            crate::error::JoggerError::Serialization(e.to_string())
        })
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|e| {
            crate::error::JoggerError::Serialization(e.to_string())
        })
    }

    /// Serialize to bytes (bincode for efficiency)
    #[cfg(feature = "rocksdb")]
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| {
            crate::error::JoggerError::Serialization(e.to_string())
        })
    }

    /// Deserialize from bytes
    #[cfg(feature = "rocksdb")]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| {
            crate::error::JoggerError::Serialization(e.to_string())
        })
    }
}

/// Builder for creating bundles with size/count limits
pub struct BundleBuilder {
    bundle: Bundle,
    max_paths: usize,
    max_bytes: u64,
}

impl BundleBuilder {
    /// Create a new bundle builder
    pub fn new(nfs_server: &str, nfs_export: &str, path_prefix: &str) -> Self {
        Self {
            bundle: Bundle::new(nfs_server, nfs_export, path_prefix),
            max_paths: DEFAULT_MAX_PATHS,
            max_bytes: DEFAULT_MAX_SIZE_BYTES,
        }
    }

    /// Set maximum number of paths
    pub fn max_paths(mut self, max: usize) -> Self {
        self.max_paths = max;
        self
    }

    /// Set maximum total bytes
    pub fn max_bytes(mut self, max: u64) -> Self {
        self.max_bytes = max;
        self
    }

    /// Check if the bundle can accept more entries
    pub fn can_add(&self, entry: &PathEntry) -> bool {
        if self.bundle.len() >= self.max_paths {
            return false;
        }
        if self.bundle.stats.total_bytes + entry.estimated_size > self.max_bytes {
            return false;
        }
        true
    }

    /// Try to add an entry, returns false if bundle would exceed limits
    pub fn try_add(&mut self, entry: PathEntry) -> bool {
        if !self.can_add(&entry) {
            return false;
        }
        self.bundle.add_path(entry);
        true
    }

    /// Add an entry without checking limits
    pub fn add(&mut self, entry: PathEntry) {
        self.bundle.add_path(entry);
    }

    /// Check if the bundle is full
    pub fn is_full(&self) -> bool {
        self.bundle.len() >= self.max_paths || self.bundle.stats.total_bytes >= self.max_bytes
    }

    /// Check if the bundle is empty
    pub fn is_empty(&self) -> bool {
        self.bundle.is_empty()
    }

    /// Get current path count
    pub fn len(&self) -> usize {
        self.bundle.len()
    }

    /// Get current total bytes
    pub fn total_bytes(&self) -> u64 {
        self.bundle.stats.total_bytes
    }

    /// Build the bundle
    pub fn build(self) -> std::result::Result<Bundle, BundleError> {
        if self.bundle.is_empty() {
            return Err(BundleError::Empty);
        }
        Ok(self.bundle)
    }

    /// Build even if empty
    pub fn build_allow_empty(self) -> Bundle {
        self.bundle
    }

    /// Take the current bundle and start a new one
    pub fn take(&mut self) -> std::result::Result<Bundle, BundleError> {
        if self.bundle.is_empty() {
            return Err(BundleError::Empty);
        }
        let bundle = std::mem::replace(
            &mut self.bundle,
            Bundle::new(
                &self.bundle.metadata.nfs_server,
                &self.bundle.metadata.nfs_export,
                &self.bundle.metadata.path_prefix,
            ),
        );
        Ok(bundle)
    }

    /// Reset the builder with a new path prefix
    pub fn reset(&mut self, path_prefix: &str) {
        self.bundle = Bundle::new(
            &self.bundle.metadata.nfs_server,
            &self.bundle.metadata.nfs_export,
            path_prefix,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bundle_builder() {
        let mut builder = BundleBuilder::new("server", "/export", "/")
            .max_paths(3)
            .max_bytes(1000);

        assert!(builder.try_add(PathEntry::file("/a".into(), 100, 1)));
        assert!(builder.try_add(PathEntry::file("/b".into(), 200, 1)));
        assert!(builder.try_add(PathEntry::file("/c".into(), 300, 1)));

        // Should fail - at max paths
        assert!(!builder.try_add(PathEntry::file("/d".into(), 100, 1)));

        let bundle = builder.build().unwrap();
        assert_eq!(bundle.len(), 3);
        assert_eq!(bundle.stats.total_bytes, 600);
    }

    #[test]
    fn test_bundle_builder_bytes_limit() {
        let mut builder = BundleBuilder::new("server", "/export", "/")
            .max_paths(100)
            .max_bytes(500);

        assert!(builder.try_add(PathEntry::file("/a".into(), 200, 1)));
        assert!(builder.try_add(PathEntry::file("/b".into(), 200, 1)));

        // Should fail - would exceed bytes limit
        assert!(!builder.try_add(PathEntry::file("/c".into(), 200, 1)));

        let bundle = builder.build().unwrap();
        assert_eq!(bundle.len(), 2);
        assert_eq!(bundle.stats.total_bytes, 400);
    }

    #[test]
    fn test_bundle_serialization() {
        let mut bundle = Bundle::new("server", "/export", "/data");
        bundle.add_path(PathEntry::file("/data/file1.txt".into(), 1024, 1));
        bundle.add_path(PathEntry::directory("/data/subdir".into(), 1));

        let json = bundle.to_json().unwrap();
        let restored = Bundle::from_json(&json).unwrap();

        assert_eq!(restored.id(), bundle.id());
        assert_eq!(restored.len(), 2);
        assert_eq!(restored.stats.file_count, 1);
        assert_eq!(restored.stats.dir_count, 1);
    }

    #[test]
    fn test_bundle_metadata_state_transitions() {
        let mut metadata = BundleMetadata::new("server", "/export", "/");
        assert_eq!(metadata.state, BundleState::Created);

        metadata.mark_queued();
        assert_eq!(metadata.state, BundleState::Queued);

        metadata.mark_processing("worker-1");
        assert_eq!(metadata.state, BundleState::Processing);
        assert_eq!(metadata.worker_id.as_deref(), Some("worker-1"));
        assert!(metadata.processing_started.is_some());

        metadata.mark_completed();
        assert_eq!(metadata.state, BundleState::Completed);
        assert!(metadata.processing_completed.is_some());
    }
}
