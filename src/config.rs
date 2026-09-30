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

// Numeric CLI ranges are enforced declaratively via clap value_parser
// ranges on the args below:
//   --workers        1..=4096   (past ~1000-2000 the work-stealing loop
//                                scans every peer's stealer per idle tick;
//                                the cap catches fat-finger typos)
//   --batch-size     100..=100_000
//   --pipeline-depth 0..=64     (libnfs isn't tuned for hundreds of
//                                in-flight PDUs per context)
//   --writer-shards  1..=32     (per-shard cost is one Arrow builder +
//                                ZSTD encoder; 32 is the prod sweet spot
//                                from the 810 M-file bench)

/// Regex for parsing NFS URLs
static NFS_URL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    // Matches: nfs://server/export/path or server:/export/path
    Regex::new(r"^(?:nfs://)?([^:/]+)(:\d+)?(/[^\s]*)$").expect("Invalid NFS URL regex")
});

/// High-performance NFS filesystem walker. Streams directly to sharded Parquet.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "nfs-walker",
    version,
    about = "High-performance NFS filesystem scanner (sharded Parquet output)",
    long_about = "Walks an NFS filesystem using direct libnfs READDIRPLUS calls with parallel workers.\n\n\
                  Output is sharded Parquet: scans/<scan_id>/part-rNN-SSSSS.parquet + metadata.json.\n\
                  Read directly with DuckDB / DataFusion / Polars; no post-hoc conversion step.\n\n\
                  Typical workflow:\n\
                  1. Scan:   nfs-walker nfs://server/export -o scan.parquet -w 32\n\
                  2. Stats:  nfs-walker stats scan.parquet\n\
                  3. Query:  duckdb -c \"SELECT count(*) FROM 'scan.parquet/scans/*/part-*.parquet'\"",
    after_help = "\
EXAMPLES:
  Scan an NFS export:
    nfs-walker nfs://server/export -o scan.parquet -w 32

  Skip snapshot directories by name (glob), with a depth limit:
    nfs-walker nfs://server/data --exclude-dir .snapshot --exclude-dir .zfs -d 10 -o scan.parquet

  Exclude by path regex:
    nfs-walker nfs://server/data --exclude '/\\.Trash(/|$)' -o scan.parquet

  Show scan overview (counts, total size, max depth):
    nfs-walker stats scan.parquet

  Disable the per-scan progress logfile (default writes <output>.log):
    nfs-walker nfs://server/export -o scan.parquet --no-log

  Emit JSON-lines progress log every 10s instead of text every 5s:
    nfs-walker nfs://server/export -o scan.parquet --log-fmt json --log-interval-secs 10

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

    /// Output directory for the Parquet scan (default: walk.parquet)
    #[arg(short, long, default_value = "walk.parquet", value_name = "PATH")]
    pub output: PathBuf,

    /// Number of worker threads (each owns one NFS mount)
    #[arg(
        short = 'w',
        long,
        default_value_t = default_workers(),
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=4096),
        value_name = "NUM"
    )]
    pub workers: usize,

    /// Batch size sent from each worker to the writer threads. Larger
    /// batches mean fewer cross-thread sends and larger Arrow row groups,
    /// which reduces contention from ≥hundreds of producers. 5000 is a
    /// good default for billion-entry scans on multi-core hosts; drop to
    /// 1000 for tiny exports or low-memory environments.
    #[arg(
        short = 'b',
        long,
        default_value = "5000",
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(100..=100_000),
        value_name = "NUM"
    )]
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

    /// Exclude paths matching a regular expression (can be repeated).
    /// Matched against the full path; a match is neither emitted nor
    /// descended into.
    #[arg(long = "exclude", value_name = "REGEX", action = clap::ArgAction::Append)]
    pub exclude_patterns: Vec<String>,

    /// Skip directories whose NAME matches a glob, and everything under
    /// them (can be repeated). A glob, not a regex: `*`, `?`, `[...]`,
    /// matched against the directory's own name only, never its path
    /// (a pattern containing `/` is rejected). `--exclude-dir .snapshot`
    /// skips every directory named exactly `.snapshot` and nothing
    /// else; `--exclude-dir '.*'` skips every dot directory.
    #[arg(long = "exclude-dir", value_name = "GLOB", action = clap::ArgAction::Append)]
    pub exclude_dirs: Vec<String>,

    /// NFS connection timeout in seconds
    #[arg(long, default_value = "30", value_name = "SECS")]
    pub timeout: u32,

    /// Number of retry attempts for transient errors
    #[arg(long, default_value = "3", value_name = "NUM")]
    pub retries: u32,

    /// Explicit server VIPs (comma-separated), bypasses DNS round-robin
    /// discovery. Use when the auth DNS returns a single A record per
    /// query and the local resolver caches it, hiding the rest of the
    /// pool. Example: --server-ips 172.200.202.1,172.200.202.2,172.200.202.4
    #[arg(long, value_delimiter = ',', value_name = "IPS")]
    pub server_ips: Vec<String>,

    /// Explicit NFS export path (overrides auto-detection from URL)
    /// Use when the export has multiple path components, e.g., /volumes/uuid
    #[arg(long, value_name = "PATH")]
    pub export: Option<String>,

    /// Number of READDIRPLUS RPCs to keep in flight per worker.
    /// 0 disables pipelining (uses the legacy serial worker loop, current
    /// behavior). 8 is the recommended setting once validated.
    #[arg(
        long,
        default_value = "0",
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(0..=64),
        value_name = "N"
    )]
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

    /// Number of writer shards. Splits the entries-by-path keyspace into
    /// N independent shards each owned by its own writer thread. The
    /// per-shard cost is one Arrow builder + ZSTD encoder, so 32 (the
    /// default) is the typical sweet spot and matches the customer
    /// baseline this code path was designed to chase. The hard cap is
    /// 32; raising it on a fat host is plausible but unproven.
    #[arg(
        long,
        default_value = "32",
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=32),
        value_name = "N"
    )]
    pub writer_shards: usize,

    /// Parquet compression algorithm. `zstd3` (the default) is the
    /// production recommendation after the 810 M-file bench: 49 GiB
    /// output vs 59 GiB for Snappy, only 2.3% slower wall on 128 cores.
    /// On small (≤16-core) hosts Snappy may win because ZSTD goes
    /// CPU-bound; use `none` to diagnose whether the encoder is the
    /// bottleneck on a given host.
    #[arg(long, value_enum, default_value_t = ParquetCompression::Zstd3, value_name = "ALG")]
    pub parquet_compression: ParquetCompression,

    /// Override the per-scan progress logfile path. Default is
    /// `<output>.log` (sidecar next to the scan output directory).
    /// Disabled with --no-log.
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

/// Compression algorithm for the streaming Parquet writer.
///
/// Maps to `parquet::basic::Compression` in `direct_writer.rs`. CLI
/// values include the explicit ZSTD levels we care about so users
/// don't have to remember the integer mapping.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum ParquetCompression {
    /// ZSTD level 1 (fastest, still reasonable ratio).
    Zstd1,
    /// ZSTD level 3 (production default; best balance on 128c).
    Zstd3,
    /// ZSTD level 6 (best ratio, slow).
    Zstd6,
    /// Snappy (faster than ZSTD, ~30% larger files).
    Snappy,
    /// LZ4 (raw frame; faster than Snappy, ~10% larger again).
    Lz4Raw,
    /// No compression. Useful for diagnosing whether the encoder is
    /// the tail-flush bottleneck.
    None,
}

impl ParquetCompression {
    /// Translate to the direct-writer's internal enum (also exposed
    /// for downstream code that needs a deeper representation).
    pub fn to_direct_writer(self) -> crate::parquet::direct_writer::ParquetCompression {
        use crate::parquet::direct_writer::ParquetCompression as PC;
        match self {
            Self::Zstd1 => PC::Zstd(1),
            Self::Zstd3 => PC::Zstd(3),
            Self::Zstd6 => PC::Zstd(6),
            Self::Snappy => PC::Snappy,
            Self::Lz4Raw => PC::Lz4Raw,
            Self::None => PC::None,
        }
    }
}

/// Subcommands
#[derive(clap::Subcommand, Debug, Clone)]
pub enum Command {
    /// Start analytics server for querying scan data
    #[cfg(feature = "server")]
    Serve {
        /// Directory containing Parquet scan output
        #[arg(long, value_name = "DIR")]
        data_dir: std::path::PathBuf,

        /// Port to listen on
        #[arg(long, default_value = "8080")]
        port: u16,

        /// Bind address. Defaults to loopback because the server has no
        /// auth; pass 0.0.0.0 explicitly to expose it on the LAN.
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
    },

    /// Show overview statistics for a Parquet scan (counts, total size, max depth).
    Stats {
        /// Parquet scan directory from a previous scan
        #[arg(value_name = "SCAN_DIR")]
        scan_dir: PathBuf,
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
    /// Accepts `nfs://server[:port]/path` and legacy `server:/path`.
    /// The entire path becomes the export (multi-component exports like
    /// `/volumes/uuid` are common and the export boundary can't be
    /// auto-detected); `subpath` is only ever set when `--export`
    /// overrides the boundary in `WalkConfig::from_args`.
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

            let port = caps
                .get(2)
                .and_then(|m| m.as_str().trim_start_matches(':').parse::<u16>().ok());

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

    /// Output Parquet scan directory path
    pub output_path: PathBuf,

    /// Number of worker threads
    pub worker_count: usize,

    /// Writer batch size
    pub batch_size: usize,

    /// Maximum traversal depth
    pub max_depth: Option<usize>,

    /// Show progress indicator
    pub show_progress: bool,

    /// Only record directories
    pub dirs_only: bool,

    /// Compiled `--exclude` patterns (regex over the full path).
    pub exclude_patterns: Vec<Regex>,

    /// Compiled `--exclude-dir` globs (anchored regex over a directory
    /// name; see [`compile_dir_glob`]).
    pub exclude_dirs: Vec<Regex>,

    /// Connection timeout (seconds)
    pub timeout_secs: u32,

    /// Retry count for transient errors
    pub retry_count: u32,

    /// Number of READDIRPLUS RPCs to keep in flight per worker.
    /// 0 = legacy serial worker loop. >0 selects the pipelined worker.
    pub pipeline_depth: usize,

    /// Number of writer shards (clap-validated to 1..=32).
    pub writer_shards: usize,

    /// Compression algorithm for parquet output.
    pub parquet_compression: ParquetCompression,

    /// Explicit server VIPs to use, bypassing DNS resolution. Empty
    /// means use DNS as normal.
    pub server_ips: Vec<String>,

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
        let nfs_url_str = args
            .nfs_url
            .as_ref()
            .ok_or_else(|| ConfigError::InvalidNfsUrl {
                url: String::new(),
                reason: "NFS URL is required for scan".to_string(),
            })?;

        let mut nfs_url = NfsUrl::parse(nfs_url_str).map_err(|e| ConfigError::InvalidNfsUrl {
            url: nfs_url_str.clone(),
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

        // Numeric ranges (workers, batch size, pipeline depth, writer
        // shards) are enforced by clap value_parser ranges on CliArgs.

        // Validate --server-ips: trim each entry, drop empties, parse as
        // IpAddr, dedupe while preserving first-seen order. If the flag
        // was passed but every entry was whitespace/empty, reject rather
        // than silently fall back to DNS.
        let server_ips = {
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut out: Vec<String> = Vec::new();
            for raw in &args.server_ips {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }
                trimmed
                    .parse::<std::net::IpAddr>()
                    .map_err(|e| ConfigError::InvalidServerIps {
                        entry: raw.clone(),
                        reason: format!("not a valid IP address: {}", e),
                    })?;
                if seen.insert(trimmed.to_string()) {
                    out.push(trimmed.to_string());
                }
            }
            if !args.server_ips.is_empty() && out.is_empty() {
                return Err(ConfigError::InvalidServerIps {
                    entry: args.server_ips.join(","),
                    reason: "flag was passed but no usable IP entries remained after trimming"
                        .to_string(),
                });
            }
            out
        };

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

        // Compile directory-name globs. Rejected here, before anything
        // is mounted or written.
        let exclude_dirs = args
            .exclude_dirs
            .iter()
            .map(|g| compile_dir_glob(g))
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

        // Resolve the progress-logfile destination.
        // Default sidecar path is `<output>.log` (e.g. scan.parquet.log).
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
            worker_count: args.workers,
            batch_size: args.batch_size,
            max_depth: args.max_depth,
            show_progress: !args.quiet,
            dirs_only: args.dirs_only,
            exclude_patterns,
            exclude_dirs,
            timeout_secs: args.timeout,
            retry_count: args.retries,
            pipeline_depth: args.pipeline_depth,
            writer_shards: args.writer_shards,
            big_dir_split_after: args.big_dir_split_after,
            parquet_compression: args.parquet_compression,
            server_ips,
            log,
        })
    }

    /// Check if a path should be excluded
    pub fn is_excluded(&self, path: &str) -> bool {
        self.exclude_patterns.iter().any(|re| re.is_match(path))
    }
}

/// Compile one `--exclude-dir` glob into an anchored regex over a
/// directory name.
///
/// Glob syntax: `*` matches any run of characters, `?` exactly one,
/// `[...]` a character class (`[!...]` negated), and `\x` the literal
/// `x`. Everything else is literal. The pattern is matched against a
/// directory's name only, so it must not contain `/`; an empty pattern
/// and an unterminated class are rejected. The contract mongoose
/// documents as `--exclude GLOB`.
pub fn compile_dir_glob(glob: &str) -> Result<Regex, ConfigError> {
    let invalid = |reason: &str| ConfigError::InvalidExcludePattern {
        pattern: glob.to_string(),
        reason: reason.to_string(),
    };
    if glob.is_empty() {
        return Err(invalid("empty pattern"));
    }
    if glob.contains('/') {
        return Err(invalid(
            "a directory-name glob cannot contain '/': it matches one name, not a path",
        ));
    }
    let mut re = String::with_capacity(glob.len() * 2 + 2);
    re.push('^');
    let mut chars = glob.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            '\\' => match chars.next() {
                Some(escaped) => re.push_str(&regex::escape(&escaped.to_string())),
                None => return Err(invalid("trailing backslash")),
            },
            '[' => {
                let mut class = String::from("[");
                let mut first = true;
                let mut closed = false;
                for k in chars.by_ref() {
                    if first && k == '!' {
                        class.push('^');
                        first = false;
                        continue;
                    }
                    if k == ']' && !first {
                        closed = true;
                        break;
                    }
                    first = false;
                    if k == '\\' || k == '[' {
                        class.push('\\');
                    }
                    class.push(k);
                }
                if !closed {
                    return Err(invalid("unterminated character class '['"));
                }
                class.push(']');
                re.push_str(&class);
            }
            other => re.push_str(&regex::escape(&other.to_string())),
        }
    }
    re.push('$');
    Regex::new(&re).map_err(|e| invalid(&e.to_string()))
}

/// Should this entry be left out of the index? `--exclude` (regex over
/// the full path) applies to every entry; `--exclude-dir` (glob over
/// the name) to directories only. A skipped directory is neither
/// emitted nor descended into, so its whole subtree is absent.
pub fn excluded_entry(
    name: &str,
    is_dir: bool,
    full_path: &str,
    path_patterns: &[Regex],
    dir_globs: &[Regex],
) -> bool {
    (!path_patterns.is_empty() && path_patterns.iter().any(|re| re.is_match(full_path)))
        || (is_dir && !dir_globs.is_empty() && dir_globs.iter().any(|g| g.is_match(name)))
}

/// Byte-safe variant used by the NFS walker.
///
/// Regex and glob flags are Unicode interfaces, so they apply only when the
/// relevant server-provided bytes are valid UTF-8. An invalid name is retained
/// rather than matched against a lossy surrogate that could exclude a distinct
/// path accidentally.
pub fn excluded_entry_bytes(
    name: &[u8],
    is_dir: bool,
    full_path: &[u8],
    path_patterns: &[Regex],
    dir_globs: &[Regex],
) -> bool {
    let path_match = std::str::from_utf8(full_path).is_ok_and(|path| {
        !path_patterns.is_empty() && path_patterns.iter().any(|re| re.is_match(path))
    });
    let dir_match = is_dir
        && std::str::from_utf8(name).is_ok_and(|name| {
            !dir_globs.is_empty() && dir_globs.iter().any(|glob| glob.is_match(name))
        });
    path_match || dir_match
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
    fn dir_glob_matches_names_only() {
        let m = |g: &str, name: &str| compile_dir_glob(g).unwrap().is_match(name);
        assert!(m(".snapshot", ".snapshot"));
        assert!(
            !m(".snapshot", "mysnapshot"),
            "a glob is not a regex: '.' is literal"
        );
        assert!(!m(".snapshot", "xsnapshot"));
        assert!(!m(".snapshot", ".snapshots"), "anchored");
        assert!(m("*.bak", "data.bak"));
        assert!(!m("*.bak", "data.bak.old"));
        assert!(m(".*", ".zfs") && !m(".*", "zfs"));
        assert!(m("tmp?", "tmp1") && !m("tmp?", "tmp") && !m("tmp?", "tmp12"));
        assert!(m("[ab]cd", "acd") && m("[ab]cd", "bcd") && !m("[ab]cd", "ccd"));
        assert!(m("[!.]*", "plain") && !m("[!.]*", ".hidden"));
        assert!(m("a[]]b", "a]b"), "']' first in a class is literal");
        assert!(
            m(r"star\*", "star*") && !m(r"star\*", "starx"),
            "escaped glob char"
        );
        assert!(m("with space", "with space"));
        assert!(
            m("(paren)+", "(paren)+"),
            "regex metacharacters are literal"
        );
    }

    #[test]
    fn dir_glob_rejects_bad_patterns_by_name() {
        for (bad, why) in [
            ("", "empty"),
            ("a/b", "cannot contain '/'"),
            ("[abc", "unterminated"),
            ("trail\\", "trailing backslash"),
        ] {
            let err = compile_dir_glob(bad).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains(why), "{bad:?}: {msg}");
            assert!(
                msg.contains(&format!("'{bad}'")) || bad.is_empty(),
                "{bad:?}: {msg}"
            );
        }
    }

    /// The fixture from mongoose's acceptance list: `.snapshot/a`,
    /// `.zfs/b`, and ordinary siblings, with the documented patterns.
    /// Only the ordinary siblings survive; the excluded directories are
    /// dropped at the entry (so never emitted, never queued), and
    /// order of patterns does not matter.
    #[test]
    fn excluded_entry_drops_the_snapshot_subtrees_only() {
        let globs_a: Vec<Regex> = [".snapshot", ".zfs"]
            .iter()
            .map(|g| compile_dir_glob(g).unwrap())
            .collect();
        let globs_b: Vec<Regex> = [".zfs", ".snapshot"]
            .iter()
            .map(|g| compile_dir_glob(g).unwrap())
            .collect();
        // (name, is_dir, full_path)
        let listing = [
            (".snapshot", true, "/data/.snapshot"),
            (".zfs", true, "/data/.zfs"),
            ("docs", true, "/data/docs"),
            ("notes.txt", false, "/data/notes.txt"),
            (".snapshot", false, "/data/.snapshot"), // a FILE of that name is data
            ("snapshot-2026", true, "/data/snapshot-2026"),
        ];
        for globs in [&globs_a, &globs_b] {
            let kept: Vec<&str> = listing
                .iter()
                .filter(|(name, is_dir, path)| !excluded_entry(name, *is_dir, path, &[], globs))
                .map(|(name, _, _)| *name)
                .collect();
            assert_eq!(
                kept,
                vec!["docs", "notes.txt", ".snapshot", "snapshot-2026"]
            );
        }
        // `.snapshot/a` would only ever be reached by descending into
        // `.snapshot`, which the filter refused; the path regex still
        // catches it if someone asks by path.
        let by_path = vec![Regex::new(r"/\.snapshot(/|$)").unwrap()];
        assert!(excluded_entry(
            "a",
            false,
            "/data/.snapshot/a",
            &by_path,
            &[]
        ));
        assert!(!excluded_entry("a", false, "/data/docs/a", &by_path, &[]));
    }

    #[test]
    fn byte_filter_never_matches_a_lossy_invalid_name() {
        let by_path = vec![Regex::new("replacement").unwrap()];
        let by_name = vec![compile_dir_glob("bad-*").unwrap()];
        assert!(!excluded_entry_bytes(
            b"bad-\xff",
            true,
            b"/data/replacement-\xff",
            &by_path,
            &by_name,
        ));
        assert!(excluded_entry_bytes(
            b"bad-name",
            true,
            b"/data/bad-name",
            &[],
            &by_name,
        ));
    }

    #[test]
    fn test_exclude_pattern() {
        let config = WalkConfig {
            nfs_url: NfsUrl::parse("nfs://s/e").unwrap(),
            output_path: PathBuf::from("test.parquet"),
            worker_count: 4,
            batch_size: 1000,
            max_depth: None,
            show_progress: false,
            dirs_only: false,
            exclude_patterns: vec![Regex::new(r"\.snapshot").unwrap()],
            exclude_dirs: vec![],
            timeout_secs: 30,
            retry_count: 3,
            pipeline_depth: 0,
            writer_shards: 1,
            big_dir_split_after: 0,
            parquet_compression: ParquetCompression::Zstd3,
            server_ips: vec![],
            log: None,
        };

        assert!(config.is_excluded("/data/.snapshot/hourly.0"));
        assert!(!config.is_excluded("/data/myfile.txt"));
    }

    fn parse_with_server_ips(value: &str) -> Result<WalkConfig, ConfigError> {
        let args =
            CliArgs::parse_from(["nfs-walker", "nfs://server/export", "--server-ips", value]);
        WalkConfig::from_args(args)
    }

    #[test]
    fn server_ips_empty_string_rejected() {
        let err = parse_with_server_ips("").expect_err("empty string must reject");
        assert!(
            matches!(err, ConfigError::InvalidServerIps { .. }),
            "got {:?}",
            err
        );
    }

    #[test]
    fn server_ips_only_commas_rejected() {
        let err = parse_with_server_ips(",,").expect_err("commas-only must reject");
        assert!(
            matches!(err, ConfigError::InvalidServerIps { .. }),
            "got {:?}",
            err
        );
    }

    #[test]
    fn server_ips_drops_empties_between_commas() {
        let cfg = parse_with_server_ips("10.0.0.1,,10.0.0.2").unwrap();
        assert_eq!(cfg.server_ips, vec!["10.0.0.1", "10.0.0.2"]);
    }

    #[test]
    fn server_ips_trims_whitespace() {
        let cfg = parse_with_server_ips("10.0.0.1, 10.0.0.2").unwrap();
        assert_eq!(cfg.server_ips, vec!["10.0.0.1", "10.0.0.2"]);
    }

    #[test]
    fn server_ips_rejects_malformed() {
        let err =
            parse_with_server_ips("10.0.0.1,not-an-ip").expect_err("malformed entry must reject");
        match err {
            ConfigError::InvalidServerIps { entry, .. } => assert_eq!(entry, "not-an-ip"),
            other => panic!("expected InvalidServerIps, got {:?}", other),
        }
    }

    #[test]
    fn server_ips_dedupes_preserving_order() {
        let cfg = parse_with_server_ips("10.0.0.1,10.0.0.1,10.0.0.2").unwrap();
        assert_eq!(cfg.server_ips, vec!["10.0.0.1", "10.0.0.2"]);
    }

    #[test]
    fn server_ips_accepts_ipv6() {
        let cfg = parse_with_server_ips("::1,10.0.0.1").unwrap();
        assert_eq!(cfg.server_ips, vec!["::1", "10.0.0.1"]);
    }
}
