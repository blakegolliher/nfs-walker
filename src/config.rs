//! Configuration types for nfs-walker
//!
//! This module defines:
//! - CLI argument parsing using clap derive macros
//! - Runtime configuration with validation
//! - NFS URL parsing

use crate::error::{ConfigError, NfsError};
use clap::Parser;
use regex::Regex;
use std::path::PathBuf;
use std::sync::LazyLock;

/// Maximum reasonable worker count
const MAX_WORKERS: usize = 512;

/// Minimum queue size
const MIN_QUEUE_SIZE: usize = 100;

/// Batch size limits
const MIN_BATCH_SIZE: usize = 100;
const MAX_BATCH_SIZE: usize = 100_000;

/// Regex for parsing NFS URLs
static NFS_URL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    // Matches: nfs://server/export/path or server:/export/path
    Regex::new(r"^(?:nfs://)?([^:/]+)(:\d+)?(/[^\s]*)$").expect("Invalid NFS URL regex")
});

/// Simple NFS filesystem walker with SQLite/RocksDB output
#[derive(Parser, Debug, Clone)]
#[command(
    name = "nfs-walker",
    version,
    about = "Simple NFS filesystem walker with SQLite/RocksDB output",
    long_about = "Walks an NFS filesystem using direct libnfs access and outputs results to a database.\n\n\
                  Uses READDIR for directory names and parallel GETATTR for file attributes.\n\n\
                  With RocksDB feature: Default output is RocksDB (.rocks) for fast writes.\n\
                  Convert to SQLite using the 'convert' subcommand for queries.",
    after_help = "EXAMPLES:\n    \
        nfs-walker nfs://server/export -o scan.db\n    \
        nfs-walker 192.168.1.100:/data -w 8 -p\n    \
        nfs-walker nfs://cluster/share --exclude '.snapshot' --dirs-only\n    \
        nfs-walker nfs://server/export -o scan.rocks -p  # RocksDB output (with feature)\n    \
        nfs-walker convert scan.rocks scan.db --progress  # Convert to SQLite",
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true
)]
pub struct CliArgs {
    /// NFS path to scan (nfs://server/export or server:/export/path)
    #[arg(value_name = "NFS_URL")]
    pub nfs_url: Option<String>,

    /// Subcommand (convert, etc.)
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Output database file (.db for SQLite, .rocks for RocksDB)
    #[arg(short, long, default_value = "walk.db", value_name = "FILE")]
    pub output: PathBuf,

    /// Force SQLite output mode (slower, but single step)
    #[cfg(feature = "rocksdb")]
    #[arg(long, help = "Use SQLite directly instead of RocksDB (slower but simpler)")]
    pub sqlite: bool,

    /// RocksDB baseline for incremental scans
    #[cfg(feature = "rocksdb")]
    #[arg(long, value_name = "PATH", help = "RocksDB baseline for incremental scan comparison")]
    pub baseline: Option<PathBuf>,

    /// Number of worker threads for parallel GETATTR
    #[arg(
        short = 'w',
        long,
        default_value_t = default_workers(),
        value_name = "NUM"
    )]
    pub workers: usize,

    /// Work queue size (controls memory usage)
    #[arg(long, default_value = "10000", value_name = "NUM")]
    pub queue_size: usize,

    /// SQLite batch insert size
    #[arg(short = 'b', long, default_value = "1000", value_name = "NUM")]
    pub batch_size: usize,

    /// Maximum directory depth (unlimited if not set)
    #[arg(short = 'd', long, value_name = "NUM")]
    pub max_depth: Option<usize>,

    /// Quiet mode - suppress progress output
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Verbose output (show errors and warnings)
    #[arg(short = 'v', long)]
    pub verbose: bool,

    /// Only record directories (creates smaller database)
    #[arg(long)]
    pub dirs_only: bool,

    /// Skip atime attribute (for NFS servers that don't support it)
    #[arg(long)]
    pub no_atime: bool,

    /// Exclude paths matching pattern (can be repeated)
    #[arg(long = "exclude", value_name = "PATTERN", action = clap::ArgAction::Append)]
    pub exclude_patterns: Vec<String>,

    /// NFS connection timeout in seconds
    #[arg(long, default_value = "30", value_name = "SECS")]
    pub timeout: u32,

    /// Number of retry attempts for transient errors
    #[arg(long, default_value = "3", value_name = "NUM")]
    pub retries: u32,

    /// Hunt for directories with more than threshold files (stores only big dirs)
    #[arg(long)]
    pub big_dir_hunt: bool,

    /// File count threshold for big-dir-hunt mode (default 1M)
    #[arg(long, default_value = "1000000", value_name = "COUNT")]
    pub threshold: u64,

    /// Explicit NFS export path (overrides auto-detection from URL)
    /// Use when the export has multiple path components, e.g., /volumes/uuid
    #[arg(long, value_name = "PATH")]
    pub export: Option<String>,
}

/// Subcommands
#[derive(clap::Subcommand, Debug, Clone)]
pub enum Command {
    /// Convert RocksDB database to SQLite
    #[cfg(feature = "rocksdb")]
    Convert {
        /// Input RocksDB directory
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output SQLite file
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,

        /// Show conversion progress
        #[arg(short = 'p', long)]
        progress: bool,
    },

    /// Export RocksDB database to Parquet files
    #[cfg(feature = "parquet")]
    ExportParquet {
        /// Input RocksDB directory
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output directory for Parquet files
        #[arg(value_name = "OUTPUT_DIR")]
        output_dir: PathBuf,

        /// Show export progress
        #[arg(short = 'p', long)]
        progress: bool,

        /// Target file size in MB before splitting
        #[arg(long, default_value = "256")]
        file_size_mb: usize,

        /// Rows per row group
        #[arg(long, default_value = "1000000")]
        row_group_size: usize,

        /// ZSTD compression level (1-22)
        #[arg(long, default_value = "3")]
        compression_level: i32,
    },

    /// Start analytics server for querying scan data
    #[cfg(feature = "server")]
    Serve {
        /// Directory containing exported Parquet scans
        #[arg(long, value_name = "DIR")]
        data_dir: std::path::PathBuf,

        /// Port to listen on
        #[arg(long, default_value = "8080")]
        port: u16,

        /// Bind address
        #[arg(long, default_value = "0.0.0.0")]
        bind: String,
    },

    /// Show statistics from a RocksDB database
    #[cfg(feature = "rocksdb")]
    Stats {
        /// RocksDB database path
        #[arg(value_name = "DB")]
        db: PathBuf,

        /// Show files grouped by extension
        #[arg(long)]
        by_extension: bool,

        /// Show largest files
        #[arg(long)]
        largest_files: bool,

        /// Show directories with most files
        #[arg(long)]
        largest_dirs: bool,

        /// Show oldest files (by mtime)
        #[arg(long)]
        oldest_files: bool,

        /// Show files with most hard links
        #[arg(long)]
        most_links: bool,

        /// Show usage by user ID
        #[arg(long)]
        by_uid: bool,

        /// Show usage by group ID
        #[arg(long)]
        by_gid: bool,

        /// Number of results to show
        #[arg(short = 'n', long, default_value = "20")]
        top: usize,
    },
}

/// Output format for scan results
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// SQLite database (.db)
    Sqlite,
    /// RocksDB directory (.rocks)
    #[cfg(feature = "rocksdb")]
    RocksDb,
}

impl CliArgs {
    /// Determine output format from file extension and flags
    pub fn output_format(&self) -> OutputFormat {
        #[cfg(feature = "rocksdb")]
        {
            // --sqlite flag forces SQLite mode
            if self.sqlite {
                return OutputFormat::Sqlite;
            }

            // Check file extension
            if let Some(ext) = self.output.extension() {
                if ext == "rocks" {
                    return OutputFormat::RocksDb;
                }
            }

            // Default to RocksDB when feature is enabled
            OutputFormat::RocksDb
        }

        #[cfg(not(feature = "rocksdb"))]
        {
            OutputFormat::Sqlite
        }
    }
}

fn default_workers() -> usize {
    // Default to 2x CPU cores, as NFS operations are I/O bound
    num_cpus::get() * 2
}

/// Parsed NFS URL components
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NfsUrl {
    /// NFS server hostname or IP
    pub server: String,

    /// Optional port (default is 2049)
    pub port: Option<u16>,

    /// Export path (must start with /)
    pub export: String,

    /// Subpath within the export (may be empty)
    pub subpath: String,
}

impl NfsUrl {
    /// Parse an NFS URL string
    ///
    /// Accepts formats:
    /// - nfs://server/export
    /// - nfs://server/export/subpath
    /// - nfs://server:port/export
    /// - server:/export
    /// - server:/export/subpath
    pub fn parse(url: &str) -> Result<Self, NfsError> {
        let url = url.trim();

        // Try the regex first
        if let Some(caps) = NFS_URL_REGEX.captures(url) {
            let server = caps
                .get(1)
                .ok_or_else(|| NfsError::InvalidUrl {
                    url: url.to_string(),
                    reason: "Missing server".into(),
                })?
                .as_str()
                .to_string();

            let port = caps.get(2).and_then(|m| {
                m.as_str()
                    .trim_start_matches(':')
                    .parse::<u16>()
                    .ok()
            });

            let full_path = caps
                .get(3)
                .ok_or_else(|| NfsError::InvalidUrl {
                    url: url.to_string(),
                    reason: "Missing export path".into(),
                })?
                .as_str();

            // Split path into export and subpath
            // The export is typically the first path component
            let (export, subpath) = Self::split_export_path(full_path);

            return Ok(Self {
                server,
                port,
                export,
                subpath,
            });
        }

        // Try legacy format: server:/export
        // First strip nfs:// prefix if present to avoid matching the :// in nfs://
        let legacy_url = url.strip_prefix("nfs://").unwrap_or(url);
        if let Some(idx) = legacy_url.find(":/") {
            let server = legacy_url[..idx].to_string();
            let full_path = &legacy_url[idx + 1..];
            let (export, subpath) = Self::split_export_path(full_path);

            if server.is_empty() {
                return Err(NfsError::InvalidUrl {
                    url: url.to_string(),
                    reason: "Empty server name".into(),
                });
            }

            return Ok(Self {
                server,
                port: None,
                export,
                subpath,
            });
        }

        Err(NfsError::InvalidUrl {
            url: url.to_string(),
            reason: "Expected format: nfs://server/export or server:/export".into(),
        })
    }

    /// Split a full path into export and subpath
    ///
    /// The export is assumed to be the first path component.
    /// For example: /export/foo/bar -> export="/export", subpath="/foo/bar"
    fn split_export_path(path: &str) -> (String, String) {
        let path = path.trim_end_matches('/');

        if path.is_empty() || path == "/" {
            return ("/".to_string(), String::new());
        }

        // Find the second slash (end of export)
        let without_leading = path.trim_start_matches('/');
        if let Some(idx) = without_leading.find('/') {
            let export = format!("/{}", &without_leading[..idx]);
            let subpath = without_leading[idx..].to_string();
            (export, subpath)
        } else {
            // Single component - it's all export
            (path.to_string(), String::new())
        }
    }

    /// Get the full path (export + subpath) for display purposes
    pub fn full_path(&self) -> String {
        if self.subpath.is_empty() {
            self.export.clone()
        } else {
            format!("{}{}", self.export, self.subpath)
        }
    }

    /// Get the path to start walking from (within the mounted export)
    /// After mounting an export, the root is "/", so we return:
    /// - "/" if no subpath specified
    /// - The subpath if specified (e.g., "/subdir")
    pub fn walk_start_path(&self) -> String {
        if self.subpath.is_empty() {
            "/".to_string()
        } else {
            self.subpath.clone()
        }
    }

    /// Format as a connection string for display
    pub fn to_display_string(&self) -> String {
        match self.port {
            Some(p) => format!("nfs://{}:{}{}", self.server, p, self.full_path()),
            None => format!("nfs://{}{}", self.server, self.full_path()),
        }
    }
}

/// Validated runtime configuration
#[derive(Debug, Clone)]
pub struct WalkConfig {
    /// Parsed NFS URL
    pub nfs_url: NfsUrl,

    /// Output database path
    pub output_path: PathBuf,

    /// Output format (SQLite or RocksDB)
    pub output_format: OutputFormat,

    /// RocksDB baseline path for incremental scans
    #[cfg(feature = "rocksdb")]
    pub baseline_path: Option<PathBuf>,

    /// Number of worker threads
    pub worker_count: usize,

    /// Work queue capacity
    pub queue_size: usize,

    /// SQLite batch size
    pub batch_size: usize,

    /// Maximum traversal depth
    pub max_depth: Option<usize>,

    /// Show progress indicator
    pub show_progress: bool,

    /// Verbose logging
    pub verbose: bool,

    /// Only record directories
    pub dirs_only: bool,

    /// Skip atime
    pub skip_atime: bool,

    /// Compiled exclude patterns
    pub exclude_patterns: Vec<Regex>,

    /// Connection timeout (seconds)
    pub timeout_secs: u32,

    /// Retry count for transient errors
    pub retry_count: u32,

    /// Big directory hunt mode (only stores dirs over threshold)
    pub big_dir_hunt: bool,

    /// File count threshold for big-dir-hunt mode
    pub big_dir_threshold: u64,
}

impl WalkConfig {
    /// Create and validate configuration from CLI arguments
    pub fn from_args(args: CliArgs) -> Result<Self, ConfigError> {
        // Parse NFS URL (required for scan command)
        let nfs_url_str = args.nfs_url.as_ref().ok_or_else(|| ConfigError::InvalidOutputPath {
            path: PathBuf::from(""),
            reason: "NFS URL is required for scan".to_string(),
        })?;

        let mut nfs_url = NfsUrl::parse(nfs_url_str).map_err(|e| ConfigError::InvalidOutputPath {
            path: PathBuf::from(nfs_url_str),
            reason: e.to_string(),
        })?;

        // Override export path if explicitly specified
        if let Some(explicit_export) = &args.export {
            // The explicit export replaces the auto-detected export
            // Recalculate subpath based on the new export
            let full_path = nfs_url.full_path();
            let explicit_export = if explicit_export.starts_with('/') {
                explicit_export.clone()
            } else {
                format!("/{}", explicit_export)
            };

            // Check if full_path starts with the explicit export
            if full_path.starts_with(&explicit_export) {
                nfs_url.export = explicit_export.clone();
                let remainder = &full_path[explicit_export.len()..];
                nfs_url.subpath = if remainder.is_empty() {
                    String::new()
                } else {
                    remainder.to_string()
                };
            } else {
                // Just use the explicit export as-is
                nfs_url.export = explicit_export;
                nfs_url.subpath = String::new();
            }
        }

        // Validate worker count
        if args.workers == 0 || args.workers > MAX_WORKERS {
            return Err(ConfigError::InvalidWorkerCount {
                count: args.workers,
                max: MAX_WORKERS,
            });
        }

        // Validate queue size
        if args.queue_size < MIN_QUEUE_SIZE {
            return Err(ConfigError::InvalidQueueSize {
                size: args.queue_size,
                min: MIN_QUEUE_SIZE,
            });
        }

        // Validate batch size
        if args.batch_size < MIN_BATCH_SIZE || args.batch_size > MAX_BATCH_SIZE {
            return Err(ConfigError::InvalidBatchSize {
                size: args.batch_size,
                min: MIN_BATCH_SIZE,
                max: MAX_BATCH_SIZE,
            });
        }

        // Compile exclude patterns
        let exclude_patterns = args
            .exclude_patterns
            .iter()
            .map(|p| {
                Regex::new(p).map_err(|e| ConfigError::InvalidExcludePattern {
                    pattern: p.clone(),
                    reason: e.to_string(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Validate output path
        if let Some(parent) = args.output.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(ConfigError::InvalidOutputPath {
                    path: args.output.clone(),
                    reason: format!("Parent directory '{}' does not exist", parent.display()),
                });
            }
        }

        // Validate baseline path if provided
        #[cfg(feature = "rocksdb")]
        if let Some(ref baseline) = args.baseline {
            if !baseline.exists() {
                return Err(ConfigError::InvalidResumeDb {
                    path: baseline.clone(),
                    reason: "Baseline database does not exist".to_string(),
                });
            }
        }

        let output_format = args.output_format();

        Ok(Self {
            nfs_url,
            output_path: args.output,
            output_format,
            #[cfg(feature = "rocksdb")]
            baseline_path: args.baseline,
            worker_count: args.workers,
            queue_size: args.queue_size,
            batch_size: args.batch_size,
            max_depth: args.max_depth,
            show_progress: !args.quiet,
            verbose: args.verbose,
            dirs_only: args.dirs_only,
            skip_atime: args.no_atime,
            exclude_patterns,
            timeout_secs: args.timeout,
            retry_count: args.retries,
            big_dir_hunt: args.big_dir_hunt,
            big_dir_threshold: args.threshold,
        })
    }

    /// Check if a path should be excluded
    pub fn is_excluded(&self, path: &str) -> bool {
        self.exclude_patterns.iter().any(|re| re.is_match(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_nfs_url_standard() {
        let url = NfsUrl::parse("nfs://server.local/export").unwrap();
        assert_eq!(url.server, "server.local");
        assert_eq!(url.export, "/export");
        assert_eq!(url.subpath, "");
        assert_eq!(url.port, None);
    }

    #[test]
    fn test_parse_nfs_url_with_subpath() {
        let url = NfsUrl::parse("nfs://server/export/data/subdir").unwrap();
        assert_eq!(url.server, "server");
        assert_eq!(url.export, "/export");
        assert_eq!(url.subpath, "/data/subdir");
    }

    #[test]
    fn test_parse_nfs_url_with_port() {
        let url = NfsUrl::parse("nfs://server:2049/export").unwrap();
        assert_eq!(url.server, "server");
        assert_eq!(url.port, Some(2049));
        assert_eq!(url.export, "/export");
    }

    #[test]
    fn test_parse_legacy_format() {
        let url = NfsUrl::parse("192.168.1.100:/data").unwrap();
        assert_eq!(url.server, "192.168.1.100");
        assert_eq!(url.export, "/data");
    }

    #[test]
    fn test_parse_invalid_url() {
        assert!(NfsUrl::parse("invalid").is_err());
        assert!(NfsUrl::parse("://server/export").is_err());
    }

    #[test]
    fn test_full_path() {
        let url = NfsUrl::parse("nfs://server/export/subdir").unwrap();
        assert_eq!(url.full_path(), "/export/subdir");
    }

    #[test]
    fn test_exclude_pattern() {
        let config = WalkConfig {
            nfs_url: NfsUrl::parse("nfs://s/e").unwrap(),
            output_path: PathBuf::from("test.db"),
            output_format: OutputFormat::Sqlite,
            #[cfg(feature = "rocksdb")]
            baseline_path: None,
            worker_count: 4,
            queue_size: 1000,
            batch_size: 1000,
            max_depth: None,
            show_progress: false,
            verbose: false,
            dirs_only: false,
            skip_atime: false,
            exclude_patterns: vec![Regex::new(r"\.snapshot").unwrap()],
            timeout_secs: 30,
            retry_count: 3,
            big_dir_hunt: false,
            big_dir_threshold: 1_000_000,
        };

        assert!(config.is_excluded("/data/.snapshot/hourly.0"));
        assert!(!config.is_excluded("/data/myfile.txt"));
    }
}
