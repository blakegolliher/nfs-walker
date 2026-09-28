//! Error types for nfs-walker
//!
//! This module defines a comprehensive error hierarchy that covers:
//! - NFS connection and protocol errors
//! - Parquet writer errors
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

    /// Parquet writer errors
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),

    /// Server errors
    #[cfg(feature = "server")]
    #[error("Server error: {0}")]
    Server(#[from] ServerError),

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

    /// The scan ran to the end but at least one directory could not
    /// be read after the retry policy was exhausted. The Parquet
    /// output on disk is intact for diagnosis but is **not** a complete
    /// index of the tree: consumers must not treat it as one.
    /// `failures` is a bounded sample; `failure_log` has every record.
    #[error(
        "scan incomplete: {} director{} could not be read ({} vanished during the scan){}",
        stats.errors,
        if stats.errors == 1 { "y" } else { "ies" },
        stats.vanished,
        failure_log
            .as_ref()
            .map(|p| format!("; see {}", p.display()))
            .unwrap_or_default()
    )]
    ScanIncomplete {
        stats: crate::walker::WalkStats,
        failures: Vec<DirFailure>,
        failure_log: Option<PathBuf>,
    },
}

/// Why a directory could not be read. Drives the retry policy
/// ([`crate::walker::retry::plan`]) and names the failure in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    /// EACCES/EPERM: never retried.
    PermissionDenied,
    /// ENOENT/ENOTDIR: verified by a path LOOKUP; a confirmed
    /// disappearance is "vanished", not a failure.
    NotFound,
    /// ESTALE/BADHANDLE: re-resolved by path, then retried.
    StaleHandle,
    Timeout,
    /// Connection, mount, or RPC-transport trouble: retried.
    Connection,
    /// The server asked for a retry (JUKEBOX, IO, SERVERFAULT, EAGAIN).
    Transient,
    /// Any other NFS3 status: not retried.
    Protocol,
    Other,
}

impl FailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PermissionDenied => "permission_denied",
            Self::NotFound => "not_found",
            Self::StaleHandle => "stale_handle",
            Self::Timeout => "timeout",
            Self::Connection => "connection",
            Self::Transient => "transient",
            Self::Protocol => "protocol",
            Self::Other => "other",
        }
    }
}

impl std::fmt::Display for FailureKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One directory the scan could not read.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DirFailure {
    pub path: String,
    pub kind: FailureKind,
    /// The last error's message.
    pub error: String,
    /// Attempts made, including the first.
    pub attempts: u32,
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
    /// Classify for the directory retry policy.
    pub fn failure_kind(&self) -> FailureKind {
        match self {
            NfsError::PermissionDenied { .. } => FailureKind::PermissionDenied,
            NfsError::NotFound { .. } => FailureKind::NotFound,
            NfsError::StaleHandle { .. } => FailureKind::StaleHandle,
            NfsError::Timeout { .. } => FailureKind::Timeout,
            NfsError::ConnectionFailed { .. }
            | NfsError::MountFailed { .. }
            | NfsError::InitFailed(_) => FailureKind::Connection,
            NfsError::Protocol { code, .. } => match -code {
                // EIO, EAGAIN, ETIMEDOUT, and EINTR (libnfs's per-RPC
                // timeout) are worth another try.
                5 | 11 | 110 | 4 => FailureKind::Transient,
                _ => FailureKind::Protocol,
            },
            NfsError::ReadDirFailed { reason, .. } => classify_reason(reason),
            NfsError::StatFailed { .. } | NfsError::ReadFailed { .. } | NfsError::InvalidUrl { .. } => {
                FailureKind::Other
            }
        }
    }

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

/// `ReadDirFailed` carries the NFS3 status name (or a transport
/// message) in its reason; classify from that text.
fn classify_reason(reason: &str) -> FailureKind {
    let r = reason;
    if r.contains("timed out") || r.contains("timeout") || r.contains("Timeout") {
        FailureKind::Timeout
    } else if r.contains("NFS3ERR_STALE") || r.contains("NFS3ERR_BADHANDLE") {
        FailureKind::StaleHandle
    } else if r.contains("NFS3ERR_NOENT") {
        FailureKind::NotFound
    } else if r.contains("NFS3ERR_ACCES") || r.contains("NFS3ERR_PERM") {
        FailureKind::PermissionDenied
    } else if r.contains("NFS3ERR_JUKEBOX")
        || r.contains("NFS3ERR_IO")
        || r.contains("NFS3ERR_SERVERFAULT")
    {
        FailureKind::Transient
    } else if r.contains("Not mounted")
        || r.contains("RPC context")
        || r.contains("Failed to queue")
        || r.contains("READDIRPLUS failed")
        || r.contains("Invalid RPC fd")
    {
        FailureKind::Connection
    } else {
        // NOTDIR (the entry is no longer a directory), BADTYPE, and the
        // rest: the server answered, and answering again will not help.
        FailureKind::Protocol
    }
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

    /// Pipeline depth out of range
    #[error("Invalid pipeline depth {depth}: must be between 0 and {max}")]
    InvalidPipelineDepth { depth: usize, max: usize },

    /// Writer-shard count out of range
    #[error("Invalid writer-shards {shards}: must be between 1 and {max}")]
    InvalidWriterShards { shards: usize, max: usize },

    /// `--server-ips` entry could not be parsed
    #[error("Invalid --server-ips entry '{entry}': {reason}")]
    InvalidServerIps { entry: String, reason: String },
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

/// Parquet writer errors
#[derive(Error, Debug)]
pub enum ParquetError {
    /// Arrow error
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Parquet writer error
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// General error with context
    #[error("{0}")]
    Other(String),
}

/// Result type alias for ParquetError
pub type ParquetResult<T> = std::result::Result<T, ParquetError>;

/// Analytics server errors
#[cfg(feature = "server")]
#[derive(Error, Debug)]
pub enum ServerError {
    /// DataFusion query error
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// Arrow error
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Query not found in catalog
    #[error("Query not found: {0}")]
    QueryNotFound(String),

    /// Invalid query parameter
    #[error("Invalid parameter '{name}': {reason}")]
    InvalidParameter { name: String, reason: String },

    /// Scan not found
    #[error("Scan not found: {0}")]
    ScanNotFound(String),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

#[cfg(feature = "server")]
impl axum::response::IntoResponse for ServerError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        use axum::Json;

        let (status, message) = match &self {
            ServerError::QueryNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            ServerError::ScanNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            ServerError::InvalidParameter { .. } => (StatusCode::BAD_REQUEST, self.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        let body = serde_json::json!({ "error": message });
        (status, Json(body)).into_response()
    }
}

/// Result type alias for ServerError
#[cfg(feature = "server")]
pub type ServerResult<T> = std::result::Result<T, ServerError>;

/// Result type alias for WalkerError
pub type Result<T> = std::result::Result<T, WalkerError>;

/// Result type alias for NfsError
pub type NfsResult<T> = std::result::Result<T, NfsError>;

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
    fn failure_kind_classification() {
        let k = |e: NfsError| e.failure_kind();
        assert_eq!(k(NfsError::PermissionDenied { path: "/p".into() }), FailureKind::PermissionDenied);
        assert_eq!(k(NfsError::NotFound { path: "/p".into() }), FailureKind::NotFound);
        assert_eq!(k(NfsError::StaleHandle { path: "/p".into() }), FailureKind::StaleHandle);
        assert_eq!(k(NfsError::Timeout { path: "/p".into(), attempts: 3 }), FailureKind::Timeout);
        assert_eq!(
            k(NfsError::ConnectionFailed { server: "s".into(), reason: "r".into() }),
            FailureKind::Connection
        );
        assert_eq!(k(NfsError::Protocol { code: -5, message: "EIO".into() }), FailureKind::Transient);
        assert_eq!(k(NfsError::Protocol { code: -4, message: "timed out".into() }), FailureKind::Transient);
        assert_eq!(k(NfsError::Protocol { code: -22, message: "EINVAL".into() }), FailureKind::Protocol);
        let rd = |reason: &str| NfsError::ReadDirFailed { path: "/p".into(), reason: reason.into() };
        assert_eq!(k(rd("NFS3ERR_JUKEBOX (jukebox/try again later)")), FailureKind::Transient);
        assert_eq!(k(rd("READDIRPLUS failed: RPC timeout")), FailureKind::Timeout);
        assert_eq!(k(rd("READDIRPLUS failed: poll error")), FailureKind::Connection);
        assert_eq!(k(rd("NFS3ERR_BADHANDLE (illegal NFS file handle)")), FailureKind::StaleHandle);
        assert_eq!(k(rd("NFS3ERR_NOENT (no such file or directory)")), FailureKind::NotFound);
        assert_eq!(k(rd("NFS3ERR_NOTDIR (not a directory)")), FailureKind::Protocol);
        assert_eq!(k(rd("NFS3ERR_BADTYPE (bad type)")), FailureKind::Protocol);
        assert_eq!(FailureKind::PermissionDenied.to_string(), "permission_denied");
        assert_eq!(serde_json::to_string(&FailureKind::StaleHandle).unwrap(), "\"stale_handle\"");
    }

    #[test]
    fn scan_incomplete_names_counts_and_log() {
        let e = WalkerError::ScanIncomplete {
            stats: crate::walker::WalkStats {
                errors: 2,
                vanished: 1,
                ..Default::default()
            },
            failures: vec![],
            failure_log: Some(PathBuf::from("/w/scans/x/errors.jsonl")),
        };
        let msg = e.to_string();
        assert!(msg.contains("2 directories could not be read"), "{msg}");
        assert!(msg.contains("1 vanished"), "{msg}");
        assert!(msg.contains("/w/scans/x/errors.jsonl"), "{msg}");
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
