//! Error types for nfs-walker
//!
//! This module defines a comprehensive error hierarchy that covers:
//! - NFS connection and protocol errors
//! - SQLite database errors
//! - Configuration and CLI errors
//! - Worker thread errors
//!
//! Design philosophy:
//! - Use thiserror for structured error types in library code
//! - Errors should be actionable - include context about what to do
//! - Preserve error chains for debugging

use std::path::PathBuf;
use thiserror::Error;

/// Top-level error type for the nfs-walker application
#[derive(Error, Debug)]
pub enum WalkerError {
    /// NFS-related errors
    #[error("NFS error: {0}")]
    Nfs(#[from] NfsError),

    /// Database errors
    #[error("Database error: {0}")]
    Database(#[from] DbError),

    /// RocksDB errors
    #[cfg(feature = "rocksdb")]
    #[error("RocksDB error: {0}")]
    Rocks(#[from] RocksError),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    /// Worker/concurrency errors
    #[error("Worker error: {0}")]
    Worker(#[from] WorkerError),

    /// I/O errors (file operations, etc.)
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Interrupted by signal
    #[error("Operation interrupted by signal")]
    Interrupted,

    /// Channel closed unexpectedly
    #[error("Channel closed unexpectedly")]
    ChannelClosed,
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

    /// File read operation failed
    #[error("Failed to read file '{path}': {reason}")]
    ReadFailed { path: String, reason: String },

    /// Permission denied
    #[error("Permission denied: '{path}'")]
    PermissionDenied { path: String },

    /// Path not found
    #[error("Path not found: '{path}'")]
    NotFound { path: String },

    /// Stale file handle (server-side change detected)
    #[error("Stale file handle for '{path}' - filesystem changed during scan")]
    StaleHandle { path: String },

    /// Operation timed out
    #[error("Operation timed out after {attempts} attempts: '{path}'")]
    Timeout { path: String, attempts: u32 },

    /// Generic NFS error with error code
    #[error("NFS error {code}: {message}")]
    Protocol { code: i32, message: String },
}

impl NfsError {
    /// Check if this error is recoverable (can retry or skip)
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            NfsError::PermissionDenied { .. }
                | NfsError::NotFound { .. }
                | NfsError::StaleHandle { .. }
                | NfsError::Timeout { .. }
        )
    }

    /// Check if this error should trigger a reconnection attempt
    pub fn should_reconnect(&self) -> bool {
        matches!(
            self,
            NfsError::StaleHandle { .. } | NfsError::ConnectionFailed { .. }
        )
    }
}

/// Database errors
#[derive(Error, Debug)]
pub enum DbError {
    /// SQLite error
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Failed to create database file
    #[error("Failed to create database at '{path}': {reason}")]
    CreateFailed { path: PathBuf, reason: String },

    /// Schema error
    #[error("Database schema error: {0}")]
    Schema(String),

    /// Transaction failed
    #[error("Transaction failed: {0}")]
    Transaction(String),

    /// Writer channel closed unexpectedly
    #[error("Database writer channel closed unexpectedly")]
    ChannelClosed,

    /// Database is locked
    #[error("Database is locked - another process may be using it")]
    Locked,
}

/// Configuration and CLI errors
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Invalid worker count
    #[error("Invalid worker count {count}: must be between 1 and {max}")]
    InvalidWorkerCount { count: usize, max: usize },

    /// Invalid queue size
    #[error("Invalid queue size {size}: must be at least {min}")]
    InvalidQueueSize { size: usize, min: usize },

    /// Invalid batch size
    #[error("Invalid batch size {size}: must be between {min} and {max}")]
    InvalidBatchSize { size: usize, min: usize, max: usize },

    /// Invalid exclude pattern
    #[error("Invalid exclude pattern '{pattern}': {reason}")]
    InvalidExcludePattern { pattern: String, reason: String },

    /// Output path error
    #[error("Invalid output path '{path}': {reason}")]
    InvalidOutputPath { path: PathBuf, reason: String },

    /// Resume database not found or invalid
    #[error("Cannot resume from '{path}': {reason}")]
    InvalidResumeDb { path: PathBuf, reason: String },
}

/// Worker thread errors
#[derive(Error, Debug)]
pub enum WorkerError {
    /// Worker panicked
    #[error("Worker {id} panicked: {message}")]
    Panicked { id: usize, message: String },

    /// Work queue send failed
    #[error("Failed to send work item: queue full or closed")]
    QueueSendFailed,

    /// Result channel closed
    #[error("Result channel closed unexpectedly")]
    ResultChannelClosed,

    /// Worker initialization failed
    #[error("Failed to initialize worker {id}: {reason}")]
    InitFailed { id: usize, reason: String },

    /// All workers died
    #[error("All workers have terminated unexpectedly")]
    AllWorkersDead,

    /// NFS error during worker operation
    #[error("Worker {id} NFS error: {source}")]
    NfsError { id: usize, source: NfsError },
}

/// RocksDB errors
#[cfg(feature = "rocksdb")]
#[derive(Error, Debug)]
pub enum RocksError {
    /// RocksDB operation failed
    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),

    /// Bincode serialization/deserialization error
    #[error("Serialization error: {0}")]
    Bincode(String),

    /// I/O error (file operations)
    #[error("I/O error: {0}")]
    Io(String),

    /// Database not found
    #[error("Database not found: {0}")]
    NotFound(String),

    /// Invalid database format
    #[error("Invalid database format: {0}")]
    InvalidFormat(String),
}

/// Result type alias for RocksError
#[cfg(feature = "rocksdb")]
pub type RocksResult<T> = std::result::Result<T, RocksError>;

/// Result type alias for WalkerError
pub type Result<T> = std::result::Result<T, WalkerError>;

/// Result type alias for NfsError
pub type NfsResult<T> = std::result::Result<T, NfsError>;

/// Result type alias for DbError
pub type DbResult<T> = std::result::Result<T, DbError>;

/// Represents the outcome of walking a single directory
#[derive(Debug)]
pub enum WalkOutcome {
    /// Successfully processed the directory
    Success {
        path: String,
        entries: usize,
        subdirs: usize,
    },

    /// Skipped due to recoverable error
    Skipped { path: String, reason: String },

    /// Failed with error
    Failed { path: String, error: NfsError },
}

impl WalkOutcome {
    /// Returns true if this outcome represents success
    pub fn is_success(&self) -> bool {
        matches!(self, WalkOutcome::Success { .. })
    }

    /// Returns the path associated with this outcome
    pub fn path(&self) -> &str {
        match self {
            WalkOutcome::Success { path, .. } => path,
            WalkOutcome::Skipped { path, .. } => path,
            WalkOutcome::Failed { path, .. } => path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nfs_error_recoverable() {
        let perm_denied = NfsError::PermissionDenied {
            path: "/test".into(),
        };
        assert!(perm_denied.is_recoverable());

        let conn_failed = NfsError::ConnectionFailed {
            server: "server".into(),
            reason: "timeout".into(),
        };
        assert!(!conn_failed.is_recoverable());
    }

    #[test]
    fn test_error_conversion() {
        let nfs_err = NfsError::NotFound {
            path: "/missing".into(),
        };
        let walker_err: WalkerError = nfs_err.into();
        assert!(matches!(walker_err, WalkerError::Nfs(_)));
    }
}
