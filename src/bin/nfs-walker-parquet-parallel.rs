//! Standalone parallel RocksDB → Parquet exporter.
//!
//! Sharded by SST file boundaries: each worker owns a key range and
//! writes its own Parquet shards (`part-rNN-SSSSS.parquet`) into the
//! same `scans/<scan_id>/` directory as the single-threaded exporter.
//! `metadata.json` lists the union of all shards so DuckDB / DataFusion
//! see a single logical scan.
//!
//! Designed for "I have a 700GB+ RocksDB and 100+ idle cores" — single-
//! threaded `nfs-walker export-parquet` will leave most of the box
//! idle on a database that size.
//!
//! ## Example
//!
//! ```bash
//! nfs-walker-parquet-parallel \
//!     --input /mnt/local-nvme/figure.rocks \
//!     --output /mnt/local-nvme/figure.parquet \
//!     --parallelism 64 \
//!     --compression-level 3 \
//!     --row-group-size 1000000 \
//!     --file-size-mb 256
//! ```
//!
//! Defaults: parallelism = num_cpus, ZSTD level 3, row group 1 M,
//! 256 MB file split. Tune up to ~80 on a 160-core box; past that the
//! NVMe is the bottleneck, not the CPUs.

use clap::Parser;
use nfs_walker::parquet::{
    parallel_convert_rocks_to_parquet, ParallelExportConfig, ParallelProgressCallback,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "nfs-walker-parquet-parallel",
    version,
    about = "Parallel RocksDB → Parquet exporter (SST-balanced shards)."
)]
struct Args {
    /// Input RocksDB directory (from a previous nfs-walker scan).
    #[arg(short = 'i', long, value_name = "DIR")]
    input: PathBuf,

    /// Output Parquet root directory. Files go in <output>/scans/<scan_id>/.
    #[arg(short = 'o', long, value_name = "DIR")]
    output: PathBuf,

    /// Number of shards / worker threads. 0 → auto-detect num_cpus.
    #[arg(short = 'p', long, default_value = "0", value_name = "N")]
    parallelism: usize,

    /// ZSTD compression level (1-22). Repo convention is 3.
    #[arg(long, default_value = "3", value_name = "L")]
    compression_level: i32,

    /// Rows per row group / Parquet batch.
    #[arg(long, default_value = "1000000", value_name = "ROWS")]
    row_group_size: usize,

    /// Target Parquet file size in MB before splitting to a new part.
    #[arg(long, default_value = "256", value_name = "MB")]
    file_size_mb: usize,

    /// Suppress the periodic progress line.
    #[arg(short = 'q', long)]
    quiet: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Default to INFO logging unless overridden via RUST_LOG.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .init();

    let cfg = ParallelExportConfig {
        parallelism: args.parallelism,
        row_group_size: args.row_group_size,
        target_file_size: args.file_size_mb * 1024 * 1024,
        compression_level: args.compression_level,
        progress: !args.quiet,
    };

    let start = Instant::now();
    let progress_cb: Option<ParallelProgressCallback> = if args.quiet {
        None
    } else {
        let start_clock = start;
        Some(Arc::new(move |done: u64, _total: u64| {
            let secs = start_clock.elapsed().as_secs_f64().max(0.001);
            let rate = (done as f64 / secs).round() as u64;
            eprintln!(
                "  {} entries exported  ({} entries/sec, {:.1}s elapsed)",
                fmt_thousands(done),
                fmt_thousands(rate),
                secs
            );
        }))
    };

    let stats = parallel_convert_rocks_to_parquet(&args.input, &args.output, cfg, progress_cb)?;
    let elapsed = start.elapsed();

    let bytes_per_sec = if elapsed.as_secs_f64() > 0.0 {
        (stats.total_bytes_written as f64 / elapsed.as_secs_f64()) as u64
    } else {
        0
    };

    println!();
    println!("Export complete:");
    println!("  scan_id:        {}", stats.scan_id);
    println!(
        "  entries:        {}",
        fmt_thousands(stats.entries_exported)
    );
    println!("  files written:  {}", stats.files_written);
    println!(
        "  bytes written:  {} ({})",
        fmt_thousands(stats.total_bytes_written),
        fmt_bytes(stats.total_bytes_written)
    );
    println!(
        "  elapsed:        {:.1}s ({:.1} min)",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() / 60.0
    );
    println!(
        "  throughput:     {}/sec",
        fmt_bytes(bytes_per_sec)
    );

    Ok(())
}

fn fmt_thousands(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

fn fmt_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut v = n as f64;
    let mut idx = 0;
    while v >= 1024.0 && idx + 1 < UNITS.len() {
        v /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{} {}", n, UNITS[idx])
    } else {
        format!("{:.2} {}", v, UNITS[idx])
    }
}
