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

    /// The scan ran to the end but at least one directory could not
    /// be read, or one entry's type could not be established, after the
    /// retry policy was exhausted. The Parquet output on disk is intact
    /// for diagnosis but is **not** a complete index of the tree:
    /// consumers must not treat it as one.
    /// `failures` is a bounded sample; `failure_log` has every record.
    #[error("scan incomplete: {}{}", incomplete_reason(stats), failure_log
            .as_ref()
            .map(|p| format!("; see {}", p.display()))
            .unwrap_or_default()
    )]
    ScanIncomplete {
        /// Boxed to keep `WalkerError` small: it travels through every
        /// `Result` in the crate.
        stats: Box<crate::walker::WalkStats>,
        failures: Vec<DirFailure>,
        failure_log: Option<PathBuf>,
    },
}

/// What an incomplete scan is missing, for its error message and the
/// CLI summary: unreadable directories, entries without a type, and how
/// many directories and entries vanished along the way.
pub fn incomplete_reason(stats: &crate::walker::WalkStats) -> String {
    let plural = |n: u64, one: &'static str, many: &'static str| if n == 1 { one } else { many };
    let dirs = stats.errors.saturating_sub(stats.unresolved_entries);
    let entries = stats.unresolved_entries;
    let mut parts = Vec::new();
    if dirs > 0 || entries == 0 {
        parts.push(format!(
            "{dirs} director{} could not be read",
            plural(dirs, "y", "ies")
        ));
    }
    if entries > 0 {
        parts.push(format!(
            "the type of {entries} entr{} could not be established",
            plural(entries, "y", "ies")
        ));
    }
    format!(
        "{} ({} vanished during the scan)",
        parts.join(" and "),
        stats.vanished
    )
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
    /// The connection is poisoned (a timed-out RPC) and will never be
    /// serviced again: not retried; the worker exits and its queue is
    /// stolen by workers with live connections.
    ConnectionLost,
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
            Self::ConnectionLost => "connection_lost",
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

/// One directory the scan could not read, or one entry whose type it
/// could not establish.
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

    /// GETATTR or LOOKUP for one entry failed
    #[error("Failed to get attributes of '{path}': {reason}")]
    AttrFailed { path: String, reason: String },

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

impl NfsError {
    /// Classify for the directory retry policy.
    pub fn failure_kind(&self) -> FailureKind {
        match self {
            NfsError::PermissionDenied { .. } => FailureKind::PermissionDenied,
            NfsError::NotFound { .. } => FailureKind::NotFound,
            NfsError::StaleHandle { .. } => FailureKind::StaleHandle,
            NfsError::ConnectionFailed { .. }
            | NfsError::MountFailed { .. }
            | NfsError::InitFailed(_) => FailureKind::Connection,
            NfsError::ReadDirFailed { reason, .. } | NfsError::AttrFailed { reason, .. } => {
                classify_reason(reason)
            }
            NfsError::InvalidUrl { .. } => FailureKind::Other,
        }
    }
}

/// `ReadDirFailed` carries the NFS3 status name (or a transport
/// message) in its reason; classify from that text.
fn classify_reason(reason: &str) -> FailureKind {
    let r = reason;
    if r.contains("poisoned") {
        // NfsConnection::poison: a timed-out RPC leaves the context
        // unusable for good. Nothing on it can succeed; the worker
        // exits and its queue is stolen.
        FailureKind::ConnectionLost
    } else if r.contains("timed out") || r.contains("timeout") || r.contains("Timeout") {
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
        || r.contains("GETATTR failed")
        || r.contains("LOOKUP failed")
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
    fn failure_kind_classification() {
        let k = |e: NfsError| e.failure_kind();
        assert_eq!(
            k(NfsError::PermissionDenied { path: "/p".into() }),
            FailureKind::PermissionDenied
        );
        assert_eq!(
            k(NfsError::NotFound { path: "/p".into() }),
            FailureKind::NotFound
        );
        assert_eq!(
            k(NfsError::StaleHandle { path: "/p".into() }),
            FailureKind::StaleHandle
        );
        assert_eq!(
            k(NfsError::ConnectionFailed {
                server: "s".into(),
                reason: "r".into()
            }),
            FailureKind::Connection
        );
        let rd = |reason: &str| NfsError::ReadDirFailed {
            path: "/p".into(),
            reason: reason.into(),
        };
        assert_eq!(
            k(rd("NFS3ERR_JUKEBOX (jukebox/try again later)")),
            FailureKind::Transient
        );
        assert_eq!(
            k(rd("READDIRPLUS failed: RPC timeout")),
            FailureKind::Timeout
        );
        assert_eq!(
            k(rd("READDIRPLUS failed: poll error")),
            FailureKind::Connection
        );
        assert_eq!(
            k(rd("READDIRPLUS failed: RPC timeout (connection poisoned)")),
            FailureKind::ConnectionLost,
            "a poisoned connection is never retried"
        );
        assert_eq!(
            k(rd("connection poisoned (not mounted)")),
            FailureKind::ConnectionLost
        );
        assert_eq!(
            k(rd("NFS3ERR_BADHANDLE (illegal NFS file handle)")),
            FailureKind::StaleHandle
        );
        assert_eq!(
            k(rd("NFS3ERR_NOENT (no such file or directory)")),
            FailureKind::NotFound
        );
        assert_eq!(
            k(rd("NFS3ERR_NOTDIR (not a directory)")),
            FailureKind::Protocol
        );
        assert_eq!(k(rd("NFS3ERR_BADTYPE (bad type)")), FailureKind::Protocol);
        assert_eq!(
            FailureKind::PermissionDenied.to_string(),
            "permission_denied"
        );
        assert_eq!(
            serde_json::to_string(&FailureKind::StaleHandle).unwrap(),
            "\"stale_handle\""
        );
    }

    #[test]
    fn scan_incomplete_names_counts_and_log() {
        let e = WalkerError::ScanIncomplete {
            stats: Box::new(crate::walker::WalkStats {
                errors: 2,
                vanished: 1,
                ..Default::default()
            }),
            failures: vec![],
            failure_log: Some(PathBuf::from("/w/scans/x/errors.jsonl")),
        };
        let msg = e.to_string();
        assert!(msg.contains("2 directories could not be read"), "{msg}");
        assert!(msg.contains("1 vanished"), "{msg}");
        assert!(msg.contains("/w/scans/x/errors.jsonl"), "{msg}");
    }

    #[test]
    fn scan_incomplete_names_entries_whose_type_is_unknown() {
        let msg = |errors, unresolved_entries| {
            WalkerError::ScanIncomplete {
                stats: Box::new(crate::walker::WalkStats {
                    errors,
                    unresolved_entries,
                    vanished: 3,
                    ..Default::default()
                }),
                failures: vec![],
                failure_log: None,
            }
            .to_string()
        };
        assert_eq!(
            msg(1, 1),
            "scan incomplete: the type of 1 entry could not be established \
             (3 vanished during the scan)"
        );
        assert_eq!(
            msg(5, 2),
            "scan incomplete: 3 directories could not be read and the type of 2 entries \
             could not be established (3 vanished during the scan)"
        );
        assert_eq!(
            msg(1, 0),
            "scan incomplete: 1 directory could not be read (3 vanished during the scan)"
        );
    }

    /// GETATTR and LOOKUP failures go through the same retry policy as
    /// a directory read, so they must classify the same way.
    #[test]
    fn attr_failures_classify_like_directory_failures() {
        let k = |reason: &str| {
            NfsError::AttrFailed {
                path: "/p".into(),
                reason: reason.into(),
            }
            .failure_kind()
        };
        assert_eq!(
            k("GETATTR: NFS3ERR_JUKEBOX (jukebox/try again later)"),
            FailureKind::Transient
        );
        assert_eq!(k("LOOKUP: NFS3ERR_IO (I/O error)"), FailureKind::Transient);
        assert_eq!(
            k("GETATTR: NFS3ERR_BADHANDLE (illegal NFS file handle)"),
            FailureKind::StaleHandle
        );
        assert_eq!(
            k("GETATTR failed: RPC timeout (connection poisoned)"),
            FailureKind::ConnectionLost
        );
        assert_eq!(k("Failed to queue GETATTR"), FailureKind::Connection);
        assert_eq!(k("Not mounted"), FailureKind::Connection);
        assert_eq!(
            k("LOOKUP: NFS3ERR_NOTDIR (not a directory)"),
            FailureKind::Protocol
        );
        assert_eq!(k("LOOKUP name contains a NUL byte"), FailureKind::Protocol);
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
