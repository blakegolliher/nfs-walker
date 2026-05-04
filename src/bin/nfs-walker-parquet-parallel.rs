//! Parallel RocksDB → Parquet exporter.
//!
//! Re-introduced as a [[bin]] target after the original binary on disk
//! went stale (April 27 build kept around, source was being rebuilt
//! into a different location). See M2_NOTES / walker subsec investigation.

use clap::Parser;
use std::path::PathBuf;
use nfs_walker::parquet::{parallel_convert_rocks_to_parquet, ParallelExportConfig};

#[derive(Parser, Debug)]
#[command(about = "Parallel RocksDB → Parquet exporter (SST-balanced shards).")]
struct Args {
    /// Input RocksDB directory (from a previous nfs-walker scan)
    #[arg(short = 'i', long, value_name = "DIR")]
    input: PathBuf,

    /// Output Parquet root directory. Files go in <output>/scans/<scan_id>/
    #[arg(short = 'o', long, value_name = "DIR")]
    output: PathBuf,

    /// Number of shards / worker threads. 0 → auto-detect num_cpus
    #[arg(short = 'p', long, default_value_t = 0)]
    parallelism: usize,

    /// ZSTD compression level (1-22). Repo convention is 3
    #[arg(long, default_value_t = 3)]
    compression_level: i32,

    /// Rows per row group / Parquet batch
    #[arg(long, default_value_t = 1_000_000)]
    row_group_size: usize,

    /// Target Parquet file size in MB before splitting to a new part
    #[arg(long, default_value_t = 256)]
    file_size_mb: usize,

    /// Suppress the periodic progress line
    #[arg(short = 'q', long)]
    quiet: bool,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let config = ParallelExportConfig {
        parallelism: args.parallelism,
        row_group_size: args.row_group_size,
        target_file_size: args.file_size_mb * 1024 * 1024,
        compression_level: args.compression_level,
        progress: !args.quiet,
    };

    match parallel_convert_rocks_to_parquet(&args.input, &args.output, config, None) {
        Ok(stats) => {
            eprintln!("Export complete:");
            eprintln!("  scan_id:        {}", stats.scan_id);
            eprintln!("  entries:        {}", stats.entries_exported);
            eprintln!("  files written:  {}", stats.files_written);
            eprintln!("  bytes written:  {}", stats.total_bytes_written);
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
