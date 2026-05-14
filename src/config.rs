//! Configuration types for nfs-walker
//!
//! This module defines:
//! - CLI argument parsing using clap derive macros
//! - Runtime configuration with validation
//! - NFS URL parsing

use crate::error::{ConfigError, NfsError};
use clap::{Parser, ValueEnum};
use regex::Regex;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;

/// Maximum reasonable worker count.
/// Past ~1000-2000 the work-stealing loop scans every other worker's
/// stealer on each idle tick, so returns diminish sharply. 4096 is a
/// hard cap to catch fat-finger typos, not a recommended setting.
const MAX_WORKERS: usize = 4096;

/// Minimum queue size
const MIN_QUEUE_SIZE: usize = 100;

/// Batch size limits
const MIN_BATCH_SIZE: usize = 100;
const MAX_BATCH_SIZE: usize = 100_000;

/// Maximum READDIRPLUS pipeline depth per worker. Above ~64 libnfs's
/// internal queue sizing isn't tuned for hundreds of in-flight PDUs
/// per context and we'd risk hitting per-context server-side caps.
const MAX_PIPELINE_DEPTH: usize = 64;

/// Maximum writer-shard count. RocksDB's compaction thread pool is
/// shared across CFs; with shards beyond ~32 the pool starts to thrash
/// and per-shard memtable memory grows superlinearly with no further
/// throughput gain. The Parquet direct-write path has no shared
/// compaction pool, so this cap is artificially generous for it; the
/// limit there is per-shard memory for in-flight Arrow builders.
const MAX_WRITER_SHARDS: usize = 32;

/// Default writer-shard count when `--output-format parquet` is selected
/// and the user did not pass `--writer-shards`. 32 matches the customer
/// tuning that motivated this code path (libnfs+DuckDB → 3M files/sec on
/// a comparable target).
const DEFAULT_PARQUET_SHARDS: usize = 32;

/// Regex for parsing NFS URLs
static NFS_URL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    // Matches: nfs://server/export/path or server:/export/path
    Regex::new(r"^(?:nfs://)?([^:/]+)(:\d+)?(/[^\s]*)$").expect("Invalid NFS URL regex")
});

/// High-performance NFS filesystem walker. Scans to RocksDB; export to Parquet or SQLite afterwards.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "nfs-walker",
    version,
    about = "High-performance NFS filesystem scanner (RocksDB output, Parquet/SQLite export)",
    long_about = "Walks an NFS filesystem using direct libnfs READDIRPLUS calls with parallel workers.\n\n\
                  Output is always RocksDB (.rocks). Export to Parquet for analytics, or SQLite for ad-hoc SQL, after the scan.\n\n\
                  Typical workflow:\n\
                  1. Scan:    nfs-walker nfs://server/export -o scan.rocks -w 32\n\
                  2. Export:  nfs-walker export-parquet scan.rocks ./parquet-out -p --parallelism 64\n\
                              (or: nfs-walker export-sql scan.rocks scan.db -p)\n\
                  3. Stats:   nfs-walker stats scan.rocks",
    after_help = "\
EXAMPLES:
  Scan an NFS export to RocksDB:
    nfs-walker nfs://server/export -o scan.rocks -w 32

  Scan with exclusions and depth limit:
    nfs-walker nfs://server/data --exclude '.snapshot' --exclude '\\.Trash' -d 10 -o scan.rocks

  Export RocksDB to Parquet for analytics (DuckDB / DataFusion):
    nfs-walker export-parquet scan.rocks ./parquet-out -p --parallelism 64

  Export RocksDB to SQLite for ad-hoc SQL queries:
    nfs-walker export-sql scan.rocks scan.db -p

  Show scan overview (counts, total size, max depth):
    nfs-walker stats scan.rocks

  Disable the per-scan progress logfile (default writes <output>.log):
    nfs-walker nfs://server/export -o scan.rocks --no-log

  Emit JSON-lines progress log every 10s instead of text every 5s:
    nfs-walker nfs://server/export -o scan.rocks --log-fmt json --log-interval-secs 10

NOTE: For large scans (>100M files), write output to a filesystem with enough space.
      Use 'ulimit -n 65536' if you hit 'too many open files' errors.",
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

    /// Output RocksDB directory (default: walk.rocks)
    #[arg(short, long, default_value = "walk.rocks", value_name = "PATH")]
    pub output: PathBuf,

    /// RocksDB baseline for incremental scans
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

    /// Batch size sent from each worker to the writer thread. Larger
    /// batches mean fewer cross-thread sends and bigger RocksDB
    /// WriteBatch transactions, which reduces contention from
    /// ≥hundreds of producers. 5000 is a good default for billion-entry
    /// scans on multi-core hosts; drop to 1000 for tiny exports or
    /// low-memory environments.
    #[arg(short = 'b', long, default_value = "5000", value_name = "NUM")]
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

    /// Explicit NFS export path (overrides auto-detection from URL)
    /// Use when the export has multiple path components, e.g., /volumes/uuid
    #[arg(long, value_name = "PATH")]
    pub export: Option<String>,

    /// Calculate gxhash checksum for each file (reads full file content)
    #[arg(long, short = 'c')]
    pub checksum: bool,

    /// Detect file type using magic bytes (reads first 8KB of each file)
    #[arg(long, short = 't')]
    pub file_type: bool,

    /// Maximum file size for checksum calculation (default: 1GB)
    /// Files larger than this will have checksum set to None
    #[arg(long, default_value = "1073741824", value_name = "BYTES")]
    pub max_checksum_size: u64,

    /// Number of READDIRPLUS RPCs to keep in flight per worker.
    /// 0 disables pipelining (uses the legacy serial worker loop, current
    /// behavior). 8 is the recommended setting once validated.
    #[arg(long, default_value = "0", value_name = "N")]
    pub pipeline_depth: usize,

    /// Stop reading any one directory at the next page boundary once
    /// this many entries have been returned, then push a continuation
    /// work item (file handle + cookie) onto the deque so other workers
    /// can resume the same directory in parallel. Targets giant flat
    /// directories where one worker would otherwise serialize the
    /// entire scan tail. 0 disables; default 1_000_000. Pipelined-mode
    /// only (--pipeline-depth > 0); ignored by the legacy serial
    /// worker.
    #[arg(long, default_value = "1000000", value_name = "N")]
    pub big_dir_split_after: u64,

    /// Number of writer shards. 1 = legacy single-writer path (RocksDB
    /// only). Higher values split the entries-by-path keyspace into N
    /// independent shards each owned by its own writer thread. For
    /// `--output-format rocksdb` (default) the recommended range is 8-16
    /// and the hard cap is 32 (compaction-pool thrash above that). For
    /// `--output-format parquet` the per-shard cost is just an Arrow
    /// builder + ZSTD encoder so 32 is the typical sweet spot (matches
    /// the customer baseline this path was designed to chase).
    ///
    /// When unset, the default is 1 in rocksdb mode and 32 in parquet
    /// mode.
    #[arg(long, value_name = "N")]
    pub writer_shards: Option<usize>,

    /// Output backend. `rocksdb` (default) writes a RocksDB directory
    /// suitable for incremental rescans and post-hoc export. `parquet`
    /// streams entries directly to sharded Parquet files (one set per
    /// `--writer-shards`) — much faster on writer-bound scans, but loses
    /// the incremental-rescan capability (no baseline state store).
    #[arg(long, value_enum, default_value_t = OutputFormat::Rocksdb, value_name = "FMT")]
    pub output_format: OutputFormat,

    /// Override the per-scan progress logfile path. Default is `<output>.log`
    /// (sidecar next to the RocksDB directory). Disabled with --no-log.
    #[arg(long, value_name = "PATH")]
    pub log: Option<PathBuf>,

    /// Disable the per-scan progress logfile entirely.
    #[arg(long)]
    pub no_log: bool,

    /// Logfile record format. `text` (default) emits human-readable snapshot
    /// blocks; `json` emits one JSON object per snapshot (newline-delimited).
    #[arg(long, value_enum, default_value_t = LogFormat::Text, value_name = "FMT")]
    pub log_fmt: LogFormat,

    /// Snapshot interval in seconds for the progress logfile.
    #[arg(long, default_value = "5", value_name = "SECS")]
    pub log_interval_secs: u64,
}

/// Output format for the per-scan progress logfile.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    /// Human-readable multi-line snapshot blocks.
    Text,
    /// JSON-Lines: one JSON object per snapshot.
    Json,
}

/// Walker output backend. Selected at scan time; cannot be mixed in one
/// scan.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Write to a RocksDB directory (default; supports incremental
    /// rescans, post-hoc export to Parquet/SQLite, and resume).
    Rocksdb,
    /// Stream entries directly to sharded Parquet files. Faster but
    /// drops the incremental-rescan capability.
    Parquet,
}

/// Subcommands
#[derive(clap::Subcommand, Debug, Clone)]
pub enum Command {
    /// Export RocksDB scan to a SQLite database (for ad-hoc SQL queries).
    ExportSql {
        /// Input RocksDB directory from a previous scan
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output SQLite file (.db)
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,

        /// Show conversion progress
        #[arg(short = 'p', long)]
        progress: bool,
    },

    /// Export RocksDB scan to Parquet files (for analytics/DataFusion)
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

        /// Number of parallel shards. 1 selects the legacy single-threaded
        /// path; values above 1 enable the SST-balanced parallel exporter.
        /// Set to roughly num_cpus on many-core boxes; on a 160-core /
        /// NVMe-backed RocksDB the useful range is 64-128 (NVMe saturates
        /// past that). Bump `ulimit -n` first — RocksDB read-only mode
        /// keeps every SST file open, and large databases may exceed the
        /// default file-descriptor limit.
        #[arg(long, default_value = "1", value_name = "N")]
        parallelism: usize,
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

    /// Show overview statistics for a RocksDB scan (counts, total size, max depth).
    Stats {
        /// RocksDB database path from a previous scan
        #[arg(value_name = "DB")]
        db: PathBuf,

        /// Open the database in RocksDB secondary mode for live querying
        /// while a scan is still writing to it. Slightly slower than the
        /// default read-only mode but tolerates concurrent compactions.
        #[arg(long)]
        live: bool,
    },
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
    /// The entire path is treated as the export by default, since we cannot
    /// auto-detect where the export boundary is (multi-component exports like
    /// /volumes/uuid are common). Use --export to override if needed.
    fn split_export_path(path: &str) -> (String, String) {
        let path = path.trim_end_matches('/');

        if path.is_empty() || path == "/" {
            return ("/".to_string(), String::new());
        }

        // Treat the full path as the export
        (path.to_string(), String::new())
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

    /// Output RocksDB directory path
    pub output_path: PathBuf,

    /// RocksDB baseline path for incremental scans
    pub baseline_path: Option<PathBuf>,

    /// Number of worker threads
    pub worker_count: usize,

    /// Work queue capacity
    pub queue_size: usize,

    /// Writer batch size
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

    /// Calculate gxhash checksum for files
    pub compute_checksum: bool,

    /// Detect file type using magic bytes
    pub detect_file_type: bool,

    /// Maximum file size for checksum calculation
    pub max_checksum_size: u64,

    /// Number of READDIRPLUS RPCs to keep in flight per worker.
    /// 0 = legacy serial worker loop. >0 selects the pipelined worker.
    pub pipeline_depth: usize,

    /// Number of writer shards (1 = legacy single-writer path).
    /// Validated to 1..=32 in `from_args`.
    pub writer_shards: usize,

    /// Output backend (RocksDB or direct-write Parquet).
    pub output_format: OutputFormat,

    /// Threshold for splitting a giant directory into a continuation
    /// work item (pipelined worker only). 0 disables.
    pub big_dir_split_after: u64,

    /// Resolved progress-logfile config, or `None` if `--no-log` was passed.
    pub log: Option<LogSettings>,
}

/// Resolved progress-logfile settings (after CLI parsing).
#[derive(Debug, Clone)]
pub struct LogSettings {
    pub path: PathBuf,
    pub format: LogFormat,
    pub interval: Duration,
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

        // Validate pipeline depth (0 disables, MAX_PIPELINE_DEPTH cap)
        if args.pipeline_depth > MAX_PIPELINE_DEPTH {
            return Err(ConfigError::InvalidPipelineDepth {
                depth: args.pipeline_depth,
                max: MAX_PIPELINE_DEPTH,
            });
        }

        // Resolve writer-shard count. Default depends on backend:
        // rocksdb → 1 (legacy behavior), parquet → DEFAULT_PARQUET_SHARDS.
        // An explicit `--writer-shards N` always wins; we validate after
        // applying the default so the cap applies uniformly.
        let writer_shards = args.writer_shards.unwrap_or(match args.output_format {
            OutputFormat::Rocksdb => 1,
            OutputFormat::Parquet => DEFAULT_PARQUET_SHARDS,
        });
        if writer_shards == 0 || writer_shards > MAX_WRITER_SHARDS {
            return Err(ConfigError::InvalidWriterShards {
                shards: writer_shards,
                max: MAX_WRITER_SHARDS,
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
        if let Some(ref baseline) = args.baseline {
            if !baseline.exists() {
                return Err(ConfigError::InvalidResumeDb {
                    path: baseline.clone(),
                    reason: "Baseline database does not exist".to_string(),
                });
            }
        }

        // Resolve the progress-logfile destination.
        // Default sidecar path is `<output>.log` (e.g. scan.rocks.log).
        let log = if args.no_log {
            None
        } else {
            let path = args.log.clone().unwrap_or_else(|| {
                let mut p = args.output.as_os_str().to_owned();
                p.push(".log");
                PathBuf::from(p)
            });
            Some(LogSettings {
                path,
                format: args.log_fmt,
                interval: Duration::from_secs(args.log_interval_secs.max(1)),
            })
        };

        Ok(Self {
            nfs_url,
            output_path: args.output,
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
            compute_checksum: args.checksum,
            detect_file_type: args.file_type,
            max_checksum_size: args.max_checksum_size,
            pipeline_depth: args.pipeline_depth,
            writer_shards,
            big_dir_split_after: args.big_dir_split_after,
            output_format: args.output_format,
            log,
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
        assert_eq!(url.export, "/export/data/subdir");
        assert_eq!(url.subpath, "");
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
        assert_eq!(url.export, "/export/subdir");
        assert_eq!(url.subpath, "");
    }

    #[test]
    fn test_exclude_pattern() {
        let config = WalkConfig {
            nfs_url: NfsUrl::parse("nfs://s/e").unwrap(),
            output_path: PathBuf::from("test.rocks"),
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
            compute_checksum: false,
            detect_file_type: false,
            max_checksum_size: 1_073_741_824,
            pipeline_depth: 0,
            writer_shards: 1,
            big_dir_split_after: 0,
            output_format: OutputFormat::Rocksdb,
            log: None,
        };

        assert!(config.is_excluded("/data/.snapshot/hourly.0"));
        assert!(!config.is_excluded("/data/myfile.txt"));
    }
}
