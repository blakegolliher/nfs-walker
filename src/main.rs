//! nfs-walker - Simple NFS Filesystem Scanner
//!
//! Entry point for the CLI application.

use anyhow::{Context, Result};
use clap::Parser;
use humansize::{format_size, BINARY};
use nfs_walker::config::{CliArgs, Command, WalkConfig};
use nfs_walker::progress::{format_elapsed, print_header, print_summary, ProgressReporter};
use nfs_walker::walker::{SimpleWalker, WalkStats};
use std::process::ExitCode;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// Raise the soft `RLIMIT_NOFILE` to at least `TARGET` (or the hard limit,
/// whichever is lower). Large RocksDB scans can hold thousands of SST file
/// handles plus per-worker NFS sockets, and the default soft limit of 1024
/// on most distros causes "Too many open files" crashes at multi-billion-
/// entry scale.
#[cfg(unix)]
fn raise_fd_limit() {
    const TARGET: libc::rlim_t = 1_048_576;

    // SAFETY: getrlimit/setrlimit are thread-safe and we pass valid pointers.
    unsafe {
        let mut current = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut current) != 0 {
            warn!("Failed to read RLIMIT_NOFILE; FD-related crashes possible on large scans");
            return;
        }

        let target = TARGET.min(current.rlim_max);
        if current.rlim_cur >= target {
            return;
        }

        let new = libc::rlimit { rlim_cur: target, rlim_max: current.rlim_max };
        if libc::setrlimit(libc::RLIMIT_NOFILE, &new) != 0 {
            warn!(
                "Failed to raise RLIMIT_NOFILE soft limit (current={}, hard={}, requested={}). \
                 Large scans may crash with 'Too many open files'. \
                 Raise the hard limit in /etc/security/limits.conf and re-login.",
                current.rlim_cur, current.rlim_max, target
            );
        } else {
            info!(
                "Raised RLIMIT_NOFILE soft limit: {} -> {} (hard limit: {})",
                current.rlim_cur, target, current.rlim_max
            );
        }
    }
}

#[cfg(not(unix))]
fn raise_fd_limit() {}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{:#}", e);
            eprintln!("Error: {:#}", e);
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    // Parse CLI arguments
    let args = CliArgs::parse();

    // Setup logging
    setup_logging(args.verbose)?;

    // Raise FD limit early so both scans (lots of NFS sockets) and stats
    // queries (lots of SST handles) benefit before opening anything.
    raise_fd_limit();

    // Handle subcommands
    if let Some(ref cmd) = args.command {
        return handle_command(cmd);
    }

    // Validate and create config for scan
    let config = WalkConfig::from_args(args.clone())
        .context("Invalid configuration")?;

    // Print header
    if config.show_progress {
        print_header(
            &config.nfs_url.to_display_string(),
            config.worker_count,
            &config.output_path.display().to_string(),
        );
        eprintln!("Mode: READDIRPLUS (RocksDB)");
    }

    // Save output path before moving config
    let output_path = config.output_path.clone();

    // Run the walker
    let result = run_simple_walker(config)?;

    // Get database file size
    let db_size = get_rocks_db_size(&output_path);

    // Print summary
    print_summary(
        result.dirs,
        result.files,
        result.bytes,
        result.errors,
        result.duration,
        &output_path.display().to_string(),
        db_size,
    );

    Ok(())
}

/// Handle subcommands (export-sql, export-parquet, stats, serve).
fn handle_command(cmd: &Command) -> Result<()> {
    match cmd {
        Command::ExportSql { input, output, progress } => {
            run_export_sql(input, output, *progress)
        }
        #[cfg(feature = "parquet")]
        Command::ExportParquet { input, output_dir, progress, file_size_mb, row_group_size, compression_level, parallelism } => {
            run_export_parquet(input, output_dir, *progress, *file_size_mb, *row_group_size, *compression_level, *parallelism)
        }
        Command::Stats { db, live } => {
            run_stats(db, *live)
        }
        #[cfg(feature = "server")]
        Command::Serve { data_dir, port, bind } => {
            run_server(data_dir, bind, *port)
        }
    }
}

/// Print the RocksDB scan overview (counts, total size, max depth).
///
/// Detail-level analytics (largest files, by-extension, duplicates, etc.) are
/// no longer emitted from here — `nfs-walker export-parquet` followed by
/// DuckDB / DataFusion queries gives a much faster path for that work.
fn run_stats(db: &std::path::Path, live: bool) -> Result<()> {
    use nfs_walker::rocksdb::{compute_stats, OpenMode};

    let mode = if live { OpenMode::Secondary } else { OpenMode::Readonly };

    {
        let stats = compute_stats(db, mode).context("Failed to compute stats")?;
        println!();
        println!("Database Statistics");
        println!("─────────────────────────────────────────────────");
        println!("  Total entries:  {}", format_number(stats.total_entries));
        println!("  Files:          {}", format_number(stats.total_files));
        println!("  Directories:    {}", format_number(stats.total_dirs));
        println!("  Symlinks:       {}", format_number(stats.total_symlinks));
        println!("  Total size:     {}", format_size(stats.total_bytes, BINARY));
        println!("  Allocated:      {}", format_size(stats.total_blocks * 512, BINARY));
        println!("  Max depth:      {}", stats.max_depth);
        println!();
    }

    Ok(())
}

/// Export a RocksDB scan to a SQLite database for ad-hoc SQL.
fn run_export_sql(
    input: &std::path::Path,
    output: &std::path::Path,
    show_progress: bool,
) -> Result<()> {
    use nfs_walker::rocksdb::{convert_rocks_to_sqlite, ConvertConfig};

    eprintln!("Converting RocksDB to SQLite...");
    eprintln!("  Input:  {}", input.display());
    eprintln!("  Output: {}", output.display());

    let config = ConvertConfig {
        batch_size: 10_000,
        progress: show_progress,
    };

    let progress_reporter = if show_progress {
        let reporter = ProgressReporter::new();
        reporter.set_status("Converting...");
        Some(reporter)
    } else {
        None
    };

    let callback: Option<Box<dyn Fn(u64, u64) + Send>> = if let Some(ref p) = progress_reporter {
        let p_clone = p.clone();
        Some(Box::new(move |converted, _total| {
            let msg = format!("Converted {} entries", format_number(converted));
            p_clone.set_status(&msg);
        }))
    } else {
        None
    };

    let stats = convert_rocks_to_sqlite(input, output, config, callback)
        .context("Conversion failed")?;

    if let Some(ref p) = progress_reporter {
        p.finish("Conversion complete");
    }

    let db_size = std::fs::metadata(output)
        .map(|m| format_size(m.len(), BINARY))
        .unwrap_or_else(|_| "unknown".to_string());

    eprintln!("Conversion complete:");
    eprintln!("  Entries: {}", format_number(stats.entries_converted));
    eprintln!("  SQLite size: {}", db_size);

    Ok(())
}

/// Run RocksDB to Parquet export
#[cfg(feature = "parquet")]
fn run_export_parquet(
    input: &std::path::Path,
    output_dir: &std::path::Path,
    show_progress: bool,
    file_size_mb: usize,
    row_group_size: usize,
    compression_level: i32,
    parallelism: usize,
) -> Result<()> {
    eprintln!("Exporting RocksDB to Parquet...");
    eprintln!("  Input:       {}", input.display());
    eprintln!("  Output dir:  {}", output_dir.display());
    eprintln!("  Parallelism: {}", parallelism);

    let progress_reporter = if show_progress {
        let reporter = ProgressReporter::new();
        reporter.set_status("Exporting...");
        Some(reporter)
    } else {
        None
    };

    let stats = if parallelism > 1 {
        run_export_parquet_parallel(
            input,
            output_dir,
            show_progress,
            file_size_mb,
            row_group_size,
            compression_level,
            parallelism,
            progress_reporter.as_ref(),
        )?
    } else {
        run_export_parquet_serial(
            input,
            output_dir,
            show_progress,
            file_size_mb,
            row_group_size,
            compression_level,
            progress_reporter.as_ref(),
        )?
    };

    if let Some(ref p) = progress_reporter {
        p.finish("Export complete");
    }

    eprintln!("Export complete:");
    eprintln!("  Scan ID:    {}", stats.scan_id);
    eprintln!("  Entries:    {}", format_number(stats.entries_exported));
    eprintln!("  Files:      {}", stats.files_written);
    eprintln!(
        "  Total size: {}",
        format_size(stats.total_bytes_written, BINARY)
    );

    Ok(())
}

#[cfg(feature = "parquet")]
fn run_export_parquet_serial(
    input: &std::path::Path,
    output_dir: &std::path::Path,
    show_progress: bool,
    file_size_mb: usize,
    row_group_size: usize,
    compression_level: i32,
    progress_reporter: Option<&ProgressReporter>,
) -> Result<nfs_walker::parquet::ExportStats> {
    use nfs_walker::parquet::{convert_rocks_to_parquet, ExportConfig};

    let config = ExportConfig {
        row_group_size,
        target_file_size: file_size_mb * 1024 * 1024,
        compression_level,
        progress: show_progress,
    };

    let callback: Option<Box<dyn Fn(u64, u64) + Send>> =
        progress_reporter.map(|p| {
            let p_clone = p.clone();
            Box::new(move |exported, _total| {
                let msg = format!("Exported {} entries", format_number(exported));
                p_clone.set_status(&msg);
            }) as Box<dyn Fn(u64, u64) + Send>
        });

    convert_rocks_to_parquet(input, output_dir, config, callback)
        .context("Parquet export failed")
}

#[cfg(feature = "parquet")]
#[allow(clippy::too_many_arguments)]
fn run_export_parquet_parallel(
    input: &std::path::Path,
    output_dir: &std::path::Path,
    show_progress: bool,
    file_size_mb: usize,
    row_group_size: usize,
    compression_level: i32,
    parallelism: usize,
    progress_reporter: Option<&ProgressReporter>,
) -> Result<nfs_walker::parquet::ExportStats> {
    use nfs_walker::parquet::{
        parallel_convert_rocks_to_parquet, ParallelExportConfig, ParallelProgressCallback,
    };
    use std::sync::Arc;

    let config = ParallelExportConfig {
        parallelism,
        row_group_size,
        target_file_size: file_size_mb * 1024 * 1024,
        compression_level,
        progress: show_progress,
    };

    let callback: Option<ParallelProgressCallback> = progress_reporter.map(|p| {
        let p_clone = p.clone();
        Arc::new(move |exported: u64, _total: u64| {
            let msg = format!("Exported {} entries", format_number(exported));
            p_clone.set_status(&msg);
        }) as ParallelProgressCallback
    });

    parallel_convert_rocks_to_parquet(input, output_dir, config, callback)
        .context("Parallel Parquet export failed")
}

/// Start the analytics server
#[cfg(feature = "server")]
fn run_server(
    data_dir: &std::path::Path,
    bind: &str,
    port: u16,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()
        .context("Failed to create tokio runtime")?;
    rt.block_on(nfs_walker::server::serve(data_dir, bind, port))
        .context("Server error")?;
    Ok(())
}

/// Get total size of a RocksDB directory
fn get_rocks_db_size(path: &std::path::Path) -> Option<u64> {
    if !path.is_dir() {
        return None;
    }

    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    Some(total)
}

fn setup_logging(verbose: bool) -> Result<()> {
    let filter = if verbose {
        EnvFilter::new("nfs_walker=info,warn")
    } else {
        EnvFilter::new("nfs_walker=warn")
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    Ok(())
}

fn run_simple_walker(config: WalkConfig) -> Result<WalkStats> {
    let walker = SimpleWalker::new(config.clone());

    // Setup signal handler
    let shutdown_flag = walker.shutdown_flag();
    let ctrl_c_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let ctrl_c_count_handler = Arc::clone(&ctrl_c_count);
    ctrlc::set_handler(move || {
        let count = ctrl_c_count_handler.fetch_add(1, Ordering::SeqCst);
        if count == 0 {
            eprintln!("\nInterrupt received, shutting down gracefully...");
            eprintln!("Press Ctrl+C again to force exit immediately.");
            shutdown_flag.store(true, Ordering::SeqCst);
        } else {
            eprintln!("\nForced exit!");
            std::process::exit(130);
        }
    })
    .context("Failed to set signal handler")?;

    let progress = if config.show_progress {
        Some(ProgressReporter::new())
    } else {
        None
    };

    if let Some(ref p) = progress {
        p.set_status("Connecting to NFS server...");
    }

    let result = if let Some(ref p) = progress {
        let p_clone = p.clone();
        walker.run_with_progress(move |prog| {
            let bytes_str = format_size(prog.bytes, BINARY);
            let entries = prog.dirs + prog.files;
            let elapsed_secs = prog.elapsed.as_secs_f64();
            let rate = if elapsed_secs > 0.0 {
                entries as f64 / elapsed_secs
            } else {
                0.0
            };
            let msg = format!(
                "Dirs: {} | Files: {} | Entries: {} | Size: {} | {} | {:.0} entries/s",
                format_number(prog.dirs),
                format_number(prog.files),
                format_number(entries),
                bytes_str,
                format_elapsed(prog.elapsed),
                rate,
            );
            p_clone.set_status(&msg);
        })
        .context("Walk failed")?
    } else {
        walker.run().context("Walk failed")?
    };

    if let Some(ref p) = progress {
        if result.completed {
            p.finish("Walk completed");
        } else {
            p.finish("Walk interrupted");
        }
    }

    Ok(result)
}

/// Format a number with thousands separators
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let bytes: Vec<_> = s.bytes().rev().collect();
    let chunks: Vec<String> = bytes
        .chunks(3)
        .map(|chunk| {
            chunk.iter().rev().map(|&b| b as char).collect::<String>()
        })
        .collect();
    chunks.into_iter().rev().collect::<Vec<_>>().join(",")
}
