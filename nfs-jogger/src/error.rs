//! Error types for nfs-jogger
//!
//! Comprehensive error hierarchy covering:
//! - NFS connection and protocol errors
//! - Redis queue errors
//! - Configuration errors
//! - Worker and coordination errors

use std::path::PathBuf;
use thiserror::Error;

/// Top-level error type for nfs-jogger
#[derive(Error, Debug)]
pub enum JoggerError {
    /// NFS-related errors
    #[error("NFS error: {0}")]
    Nfs(#[from] NfsError),

    /// Queue errors (Redis)
    #[error("Queue error: {0}")]
    Queue(#[from] QueueError),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    /// Worker errors
    #[error("Worker error: {0}")]
    Worker(#[from] WorkerError),

    /// Discovery errors
    #[error("Discovery error: {0}")]
    Discovery(#[from] DiscoveryError),

    /// Bundle errors
    #[error("Bundle error: {0}")]
    Bundle(#[from] BundleError),

    /// I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Interrupted by signal
    #[error("Operation interrupted by signal")]
    Interrupted,

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// NFS connection and protocol errors
#[derive(Error, Debug, Clone)]
pub enum NfsError {
    /// Failed to parse NFS URL
    #[error("Invalid NFS URL '{url}': {reason}")]
    InvalidUrl { url: String, reason: String },

    /// Failed to initialize NFS context
    #[error("Failed to initialize NFS context: {0}")]
    InitFailed(String),

    /// Connection failed
    #[error("Failed to connect to NFS server '{server}': {reason}")]
    ConnectionFailed { server: String, reason: String },

    /// Mount failed
    #[error("Failed to mount export '{export}' on '{server}': {reason}")]
    MountFailed {
        server: String,
        export: String,
        reason: String,
    },

    /// Directory operation failed
    #[error("Failed to read directory '{path}': {reason}")]
    ReadDirFailed { path: String, reason: String },

    /// Stat operation failed
    #[error("Failed to stat '{path}': {reason}")]
    StatFailed { path: String, reason: String },

    /// Permission denied
    #[error("Permission denied: '{path}'")]
    PermissionDenied { path: String },

    /// Path not found
    #[error("Path not found: '{path}'")]
    NotFound { path: String },

    /// Stale file handle
    #[error("Stale file handle for '{path}' - filesystem changed during scan")]
    StaleHandle { path: String },

    /// Operation timed out
    #[error("Operation timed out after {attempts} attempts: '{path}'")]
    Timeout { path: String, attempts: u32 },

    /// Generic NFS error
    #[error("NFS error {code}: {message}")]
    Protocol { code: i32, message: String },
}

impl NfsError {
    /// Check if this error is recoverable
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            NfsError::PermissionDenied { .. }
                | NfsError::NotFound { .. }
                | NfsError::StaleHandle { .. }
                | NfsError::Timeout { .. }
        )
    }
}

/// Queue (Redis) errors
#[derive(Error, Debug)]
pub enum QueueError {
    /// Redis connection failed
    #[error("Failed to connect to Redis at '{url}': {reason}")]
    ConnectionFailed { url: String, reason: String },

    /// Redis operation failed
    #[error("Redis operation failed: {0}")]
    OperationFailed(String),

    /// Queue is empty
    #[error("Queue '{name}' is empty")]
    Empty { name: String },

    /// Serialization error
    #[error("Failed to serialize/deserialize: {0}")]
    Serialization(String),

    /// Queue timeout
    #[error("Queue operation timed out")]
    Timeout,

    /// Redis error
    #[error("Redis error: {0}")]
    Redis(String),
}

impl From<redis::RedisError> for QueueError {
    fn from(err: redis::RedisError) -> Self {
        QueueError::Redis(err.to_string())
    }
}

/// Configuration errors
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Invalid worker count
    #[error("Invalid worker count {count}: must be between 1 and {max}")]
    InvalidWorkerCount { count: usize, max: usize },

    /// Invalid bundle size
    #[error("Invalid bundle size: {0}")]
    InvalidBundleSize(String),

    /// Invalid Redis URL
    #[error("Invalid Redis URL: {0}")]
    InvalidRedisUrl(String),

    /// Missing required configuration
    #[error("Missing required configuration: {0}")]
    MissingRequired(String),

    /// Invalid output path
    #[error("Invalid output path '{path}': {reason}")]
    InvalidOutputPath { path: PathBuf, reason: String },
}

/// Worker errors
#[derive(Error, Debug)]
pub enum WorkerError {
    /// Worker panicked
    #[error("Worker '{id}' panicked: {message}")]
    Panicked { id: String, message: String },

    /// Worker initialization failed
    #[error("Failed to initialize worker '{id}': {reason}")]
    InitFailed { id: String, reason: String },

    /// Worker heartbeat timeout
    #[error("Worker '{id}' heartbeat timeout")]
    HeartbeatTimeout { id: String },

    /// Bundle processing failed
    #[error("Worker '{id}' failed to process bundle '{bundle_id}': {reason}")]
    ProcessingFailed {
        id: String,
        bundle_id: String,
        reason: String,
    },

    /// No workers available
    #[error("No workers available to process bundles")]
    NoWorkersAvailable,
}

/// Discovery/bundler errors
#[derive(Error, Debug)]
pub enum DiscoveryError {
    /// Discovery phase failed
    #[error("Discovery failed at path '{path}': {reason}")]
    ScanFailed { path: String, reason: String },

    /// Bundle creation failed
    #[error("Failed to create bundle: {0}")]
    BundleCreationFailed(String),

    /// Queue submission failed
    #[error("Failed to submit bundle to queue: {0}")]
    QueueSubmitFailed(String),
}

/// Bundle errors
#[derive(Error, Debug)]
pub enum BundleError {
    /// Bundle too large
    #[error("Bundle exceeds maximum size: {size} bytes > {max} bytes")]
    TooLarge { size: u64, max: u64 },

    /// Bundle empty
    #[error("Cannot create empty bundle")]
    Empty,

    /// Invalid bundle format
    #[error("Invalid bundle format: {0}")]
    InvalidFormat(String),

    /// Bundle not found
    #[error("Bundle '{id}' not found")]
    NotFound { id: String },

    /// Bundle already processed
    #[error("Bundle '{id}' has already been processed")]
    AlreadyProcessed { id: String },
}

/// Result type alias
pub type Result<T> = std::result::Result<T, JoggerError>;

/// Result type for queue operations
pub type QueueResult<T> = std::result::Result<T, QueueError>;

/// Result type for NFS operations
pub type NfsResult<T> = std::result::Result<T, NfsError>;
