//! Configuration types for nfs-jogger
//!
//! Defines CLI arguments, runtime configuration, and NFS URL parsing.

use crate::bundle::{DEFAULT_MAX_PATHS, DEFAULT_MAX_SIZE_BYTES};
use crate::error::{ConfigError, NfsError};
use crate::queue::RedisQueueConfig;
use clap::{Parser, Subcommand};
use regex::Regex;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;

/// Maximum reasonable worker count
const MAX_WORKERS: usize = 512;

/// Regex for parsing NFS URLs
static NFS_URL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(?:nfs://)?([^:/]+)(:\d+)?(/[^\s]*)$").expect("Invalid NFS URL regex")
});

/// Distributed NFS filesystem walker for massive data migrations
#[derive(Parser, Debug, Clone)]
#[command(
    name = "nfs-jogger",
    version,
    about = "Distributed NFS filesystem walker for massive data migrations and indexing",
    long_about = "A distributed system for parallel filesystem processing.\n\n\
                  Operates in two phases:\n\
                  1. Discovery: Scans filesystem and creates work bundles\n\
                  2. Processing: Workers pull bundles and process them in parallel\n\n\
                  Scales horizontally by adding more worker nodes.",
    after_help = "EXAMPLES:\n    \
        # Start discovery to scan and bundle work\n    \
        nfs-jogger discover nfs://server/export --redis redis://localhost:6379\n\n    \
        # Start a worker to process bundles\n    \
        nfs-jogger work --redis redis://localhost:6379 -o results.db\n\n    \
        # Check queue status\n    \
        nfs-jogger status --redis redis://localhost:6379"
)]
pub struct CliArgs {
    /// Subcommand to run
    #[command(subcommand)]
    pub command: Command,

    /// Redis URL for queue coordination
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379", global = true)]
    pub redis: String,

    /// Quiet mode - suppress progress output
    #[arg(short = 'q', long, global = true)]
    pub quiet: bool,

    /// Verbose output
    #[arg(short = 'v', long, global = true)]
    pub verbose: bool,
}

/// Available subcommands
#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Discover filesystem paths and create work bundles
    Discover {
        /// NFS path to scan (nfs://server/export or server:/export/path)
        #[arg(value_name = "NFS_URL")]
        nfs_url: String,

        /// Maximum paths per bundle
        #[arg(long, default_value_t = DEFAULT_MAX_PATHS, value_name = "NUM")]
        max_paths: usize,

        /// Maximum bytes per bundle (e.g., "2TB", "500GB")
        #[arg(long, default_value = "2TB", value_name = "SIZE")]
        max_size: String,

        /// Number of parallel discovery workers
        #[arg(short = 'w', long, default_value_t = default_workers(), value_name = "NUM")]
        workers: usize,

        /// Maximum directory depth (unlimited if not set)
        #[arg(short = 'd', long, value_name = "NUM")]
        max_depth: Option<usize>,

        /// Exclude paths matching pattern (can be repeated)
        #[arg(long = "exclude", value_name = "PATTERN", action = clap::ArgAction::Append)]
        exclude_patterns: Vec<String>,

        /// NFS connection timeout in seconds
        #[arg(long, default_value = "30", value_name = "SECS")]
        timeout: u32,

        /// Explicit NFS export path
        #[arg(long, value_name = "PATH")]
        export: Option<String>,

        /// Only create bundles for directories (faster discovery)
        #[arg(long)]
        dirs_only: bool,
    },

    /// Run as a worker to process bundles from queue
    Work {
        /// Output database file
        #[arg(short, long, default_value = "walk.db", value_name = "FILE")]
        output: PathBuf,

        /// NFS connection timeout in seconds
        #[arg(long, default_value = "30", value_name = "SECS")]
        timeout: u32,

        /// Number of retry attempts for transient errors
        #[arg(long, default_value = "3", value_name = "NUM")]
        retries: u32,

        /// Number of local worker threads
        #[arg(short = 'w', long, default_value_t = default_workers(), value_name = "NUM")]
        workers: usize,

        /// Worker ID (auto-generated if not specified)
        #[arg(long, value_name = "ID")]
        worker_id: Option<String>,

        /// Run continuously (don't exit when queue is empty)
        #[arg(long)]
        continuous: bool,

        /// Exit after processing N bundles (for testing)
        #[arg(long, value_name = "NUM")]
        max_bundles: Option<u64>,
    },

    /// Show queue status and statistics
    Status {
        /// Watch mode - continuously update status
        #[arg(short, long)]
        watch: bool,

        /// Update interval for watch mode (seconds)
        #[arg(long, default_value = "2", value_name = "SECS")]
        interval: u64,

        /// Output format (text, json)
        #[arg(long, default_value = "text", value_name = "FORMAT")]
        format: String,
    },

    /// Reset the queue (clear all pending/failed bundles)
    Reset {
        /// Also clear completed bundle records
        #[arg(long)]
        all: bool,

        /// Force reset without confirmation
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Retry failed bundles
    Retry {
        /// Retry all failed bundles
        #[arg(long)]
        all: bool,

        /// Retry specific bundle ID
        #[arg(value_name = "BUNDLE_ID")]
        bundle_id: Option<String>,
    },
}

fn default_workers() -> usize {
    num_cpus::get() * 2
}

/// Parse a human-readable size string (e.g., "2TB", "500GB")
pub fn parse_size(s: &str) -> Result<u64, ConfigError> {
    let s = s.trim().to_uppercase();

    let (num_str, multiplier) = if s.ends_with("TB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024 * 1024)
    } else if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024u64 * 1024)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024u64)
    } else if s.ends_with("B") {
        (&s[..s.len() - 1], 1u64)
    } else {
        // Assume bytes if no suffix
        (s.as_str(), 1u64)
    };

    let num: f64 = num_str.trim().parse().map_err(|_| {
        ConfigError::InvalidBundleSize(format!("Invalid size value: {}", s))
    })?;

    Ok((num * multiplier as f64) as u64)
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
    /// Subpath within the export
    pub subpath: String,
}

impl NfsUrl {
    /// Parse an NFS URL string
    pub fn parse(url: &str) -> Result<Self, NfsError> {
        let url = url.trim();

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

            let (export, subpath) = Self::split_export_path(full_path);

            return Ok(Self {
                server,
                port,
                export,
                subpath,
            });
        }

        // Try legacy format
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

    fn split_export_path(path: &str) -> (String, String) {
        let path = path.trim_end_matches('/');

        if path.is_empty() || path == "/" {
            return ("/".to_string(), String::new());
        }

        let without_leading = path.trim_start_matches('/');
        if let Some(idx) = without_leading.find('/') {
            let export = format!("/{}", &without_leading[..idx]);
            let subpath = without_leading[idx..].to_string();
            (export, subpath)
        } else {
            (path.to_string(), String::new())
        }
    }

    /// Get the full path
    pub fn full_path(&self) -> String {
        if self.subpath.is_empty() {
            self.export.clone()
        } else {
            format!("{}{}", self.export, self.subpath)
        }
    }

    /// Get the path to start walking from
    pub fn walk_start_path(&self) -> String {
        if self.subpath.is_empty() {
            "/".to_string()
        } else {
            self.subpath.clone()
        }
    }

    /// Format for display
    pub fn to_display_string(&self) -> String {
        match self.port {
            Some(p) => format!("nfs://{}:{}{}", self.server, p, self.full_path()),
            None => format!("nfs://{}{}", self.server, self.full_path()),
        }
    }
}

/// Configuration for the discovery phase
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Parsed NFS URL
    pub nfs_url: NfsUrl,
    /// Maximum paths per bundle
    pub max_paths: usize,
    /// Maximum bytes per bundle
    pub max_bytes: u64,
    /// Number of discovery workers
    pub worker_count: usize,
    /// Maximum traversal depth
    pub max_depth: Option<usize>,
    /// Compiled exclude patterns
    pub exclude_patterns: Vec<Regex>,
    /// Connection timeout
    pub timeout: Duration,
    /// Only bundle directories
    pub dirs_only: bool,
    /// Redis queue config
    pub queue_config: RedisQueueConfig,
    /// Show progress
    pub show_progress: bool,
    /// Verbose logging
    pub verbose: bool,
}

impl DiscoveryConfig {
    /// Create from CLI args
    pub fn from_discover_args(
        nfs_url_str: &str,
        max_paths: usize,
        max_size: &str,
        workers: usize,
        max_depth: Option<usize>,
        exclude_patterns: &[String],
        timeout: u32,
        export: Option<&str>,
        dirs_only: bool,
        redis_url: &str,
        quiet: bool,
        verbose: bool,
    ) -> Result<Self, ConfigError> {
        let mut nfs_url = NfsUrl::parse(nfs_url_str).map_err(|e| {
            ConfigError::InvalidOutputPath {
                path: PathBuf::from(nfs_url_str),
                reason: e.to_string(),
            }
        })?;

        // Override export if specified
        if let Some(explicit_export) = export {
            let explicit_export = if explicit_export.starts_with('/') {
                explicit_export.to_string()
            } else {
                format!("/{}", explicit_export)
            };
            nfs_url.export = explicit_export;
            nfs_url.subpath = String::new();
        }

        // Validate workers
        if workers == 0 || workers > MAX_WORKERS {
            return Err(ConfigError::InvalidWorkerCount {
                count: workers,
                max: MAX_WORKERS,
            });
        }

        // Parse max size
        let max_bytes = parse_size(max_size)?;

        // Compile exclude patterns
        let exclude_patterns = exclude_patterns
            .iter()
            .map(|p| {
                Regex::new(p).map_err(|e| ConfigError::InvalidOutputPath {
                    path: PathBuf::from(p),
                    reason: e.to_string(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            nfs_url,
            max_paths,
            max_bytes,
            worker_count: workers,
            max_depth,
            exclude_patterns,
            timeout: Duration::from_secs(timeout as u64),
            dirs_only,
            queue_config: RedisQueueConfig::with_url(redis_url),
            show_progress: !quiet,
            verbose,
        })
    }

    /// Check if a path should be excluded
    pub fn is_excluded(&self, path: &str) -> bool {
        self.exclude_patterns.iter().any(|re| re.is_match(path))
    }
}

/// Configuration for the worker phase
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Output database path
    pub output_path: PathBuf,
    /// Connection timeout
    pub timeout: Duration,
    /// Retry count
    pub retry_count: u32,
    /// Number of local workers
    pub worker_count: usize,
    /// Worker ID
    pub worker_id: String,
    /// Run continuously
    pub continuous: bool,
    /// Max bundles to process
    pub max_bundles: Option<u64>,
    /// Redis queue config
    pub queue_config: RedisQueueConfig,
    /// Show progress
    pub show_progress: bool,
    /// Verbose logging
    pub verbose: bool,
}

impl WorkerConfig {
    /// Create from CLI args
    pub fn from_work_args(
        output: &PathBuf,
        timeout: u32,
        retries: u32,
        workers: usize,
        worker_id: Option<&str>,
        continuous: bool,
        max_bundles: Option<u64>,
        redis_url: &str,
        quiet: bool,
        verbose: bool,
    ) -> Result<Self, ConfigError> {
        // Validate workers
        if workers == 0 || workers > MAX_WORKERS {
            return Err(ConfigError::InvalidWorkerCount {
                count: workers,
                max: MAX_WORKERS,
            });
        }

        // Generate worker ID if not provided
        let worker_id = worker_id.map(|s| s.to_string()).unwrap_or_else(|| {
            let hostname = hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string());
            format!("{}-{}", hostname, uuid::Uuid::new_v4().to_string()[..8].to_string())
        });

        Ok(Self {
            output_path: output.clone(),
            timeout: Duration::from_secs(timeout as u64),
            retry_count: retries,
            worker_count: workers,
            worker_id,
            continuous,
            max_bundles,
            queue_config: RedisQueueConfig::with_url(redis_url),
            show_progress: !quiet,
            verbose,
        })
    }
}

/// Combined jogger configuration
#[derive(Debug, Clone)]
pub struct JoggerConfig {
    /// Redis URL
    pub redis_url: String,
    /// Quiet mode
    pub quiet: bool,
    /// Verbose mode
    pub verbose: bool,
}

impl From<&CliArgs> for JoggerConfig {
    fn from(args: &CliArgs) -> Self {
        Self {
            redis_url: args.redis.clone(),
            quiet: args.quiet,
            verbose: args.verbose,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("2TB").unwrap(), 2 * 1024 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("500GB").unwrap(), 500 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("100MB").unwrap(), 100 * 1024 * 1024);
        assert_eq!(parse_size("1024KB").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("1024").unwrap(), 1024);
    }

    #[test]
    fn test_parse_nfs_url() {
        let url = NfsUrl::parse("nfs://server/export").unwrap();
        assert_eq!(url.server, "server");
        assert_eq!(url.export, "/export");

        let url = NfsUrl::parse("nfs://server/export/subdir").unwrap();
        assert_eq!(url.export, "/export");
        assert_eq!(url.subpath, "/subdir");

        let url = NfsUrl::parse("192.168.1.1:/data").unwrap();
        assert_eq!(url.server, "192.168.1.1");
        assert_eq!(url.export, "/data");
    }
}
