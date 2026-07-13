//! Error types for nfs-walker
//!
//! Structured error types (thiserror) for the NFS layer, the Parquet
//! writers, configuration, and the optional analytics server. Every
//! variant here has at least one construction site — resist the urge to
//! add speculative ones.

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

    /// I/O errors (file operations, etc.)
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
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

    /// Permission denied
    #[error("Permission denied: '{path}'")]
    PermissionDenied { path: String },

    /// Path not found
    #[error("Path not found: '{path}'")]
    NotFound { path: String },

    /// Stale file handle (server-side change detected)
    #[error("Stale file handle for '{path}' - filesystem changed during scan")]
    StaleHandle { path: String },
}

/// Configuration and CLI errors
///
/// Numeric range checks live in clap `value_parser` ranges on the CLI
/// definition; this enum only covers validation clap can't express.
#[derive(Error, Debug)]
pub enum ConfigError {
    /// NFS URL missing or unparseable
    #[error("Invalid NFS URL '{url}': {reason}")]
    InvalidNfsUrl { url: String, reason: String },

    /// Invalid exclude pattern
    #[error("Invalid exclude pattern '{pattern}': {reason}")]
    InvalidExcludePattern { pattern: String, reason: String },

    /// Output path error
    #[error("Invalid output path '{path}': {reason}")]
    InvalidOutputPath { path: PathBuf, reason: String },

    /// `--server-ips` entry could not be parsed
    #[error("Invalid --server-ips entry '{entry}': {reason}")]
    InvalidServerIps { entry: String, reason: String },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_conversion() {
        let nfs_err = NfsError::NotFound {
            path: "/missing".into(),
        };
        let walker_err: WalkerError = nfs_err.into();
        assert!(matches!(walker_err, WalkerError::Nfs(_)));
    }
}
