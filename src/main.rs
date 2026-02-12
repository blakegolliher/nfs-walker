//! nfs-walker - Simple NFS Filesystem Scanner
//!
//! Entry point for the CLI application.

use anyhow::{Context, Result};
use clap::Parser;
use humansize::{format_size, BINARY};
use nfs_walker::config::{CliArgs, WalkConfig};
#[cfg(feature = "rocksdb")]
use nfs_walker::config::{Command, OutputFormat};
use nfs_walker::progress::{format_elapsed, print_big_dir_hunt_summary, print_header, print_summary, ProgressReporter};
use nfs_walker::walker::{SimpleWalker, WalkStats};
use std::process::ExitCode;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::error;
use tracing_subscriber::EnvFilter;

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

    // Handle subcommands
    #[cfg(feature = "rocksdb")]
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
        #[cfg(feature = "rocksdb")]
        {
            if config.big_dir_hunt {
                eprintln!("Mode: BIG-DIR-HUNT (threshold: {} files)", config.big_dir_threshold);
            } else {
                let mode = match config.output_format {
                    OutputFormat::Sqlite => "READDIRPLUS (SQLite)",
                    OutputFormat::RocksDb => "READDIRPLUS (RocksDB)",
                };
                eprintln!("Mode: {}", mode);
            }
        }
        #[cfg(not(feature = "rocksdb"))]
        eprintln!("Mode: READDIRPLUS");
    }

    // Save output path before moving config
    let output_path = config.output_path.clone();
    #[cfg(feature = "rocksdb")]
    let output_format = config.output_format;

    // Run the walker
    let result = run_simple_walker(config)?;

    // Get database file size
    #[cfg(feature = "rocksdb")]
    let db_size = match output_format {
        OutputFormat::Sqlite => std::fs::metadata(&output_path).map(|m| m.len()).ok(),
        OutputFormat::RocksDb => get_rocks_db_size(&output_path),
    };
    #[cfg(not(feature = "rocksdb"))]
    let db_size = std::fs::metadata(&output_path).map(|m| m.len()).ok();

    // Print summary
    if result.big_dir_hunt_mode {
        print_big_dir_hunt_summary(
            result.dirs,
            result.big_dirs_found,
            result.errors,
            result.duration,
            &output_path.display().to_string(),
            db_size,
        );
    } else {
        print_summary(
            result.dirs,
            result.files,
            result.bytes,
            result.errors,
            result.duration,
            &output_path.display().to_string(),
            db_size,
        );
    }

    Ok(())
}

/// Handle subcommands (convert, stats, export-parquet, etc.)
#[cfg(feature = "rocksdb")]
fn handle_command(cmd: &Command) -> Result<()> {
    match cmd {
        Command::Convert { input, output, progress } => {
            run_convert(input, output, *progress)
        }
        #[cfg(feature = "parquet")]
        Command::ExportParquet { input, output_dir, progress, file_size_mb, row_group_size, compression_level } => {
            run_export_parquet(input, output_dir, *progress, *file_size_mb, *row_group_size, *compression_level)
        }
        Command::Stats { db, by_extension, largest_files, largest_dirs, oldest_files, most_links, by_uid, by_gid, duplicates, by_file_type, hardlink_groups, min_size, top } => {
            run_stats(db, *by_extension, *largest_files, *largest_dirs, *oldest_files, *most_links, *by_uid, *by_gid, *duplicates, *by_file_type, *hardlink_groups, *min_size, *top)
        }
        #[cfg(feature = "server")]
        Command::Serve { data_dir, port, bind } => {
            run_server(data_dir, bind, *port)
        }
    }
}

/// Run RocksDB stats queries
#[cfg(feature = "rocksdb")]
fn run_stats(
    db: &std::path::Path,
    by_extension: bool,
    largest_files_flag: bool,
    largest_dirs: bool,
    oldest_files_flag: bool,
    most_links: bool,
    by_uid: bool,
    by_gid: bool,
    duplicates: bool,
    by_file_type: bool,
    hardlink_groups: bool,
    min_size: u64,
    top: usize,
) -> Result<()> {
    use nfs_walker::rocksdb::{
        compute_stats, find_duplicates, find_hardlink_groups, largest_directories, largest_files,
        most_hardlinks, oldest_files, stats_by_extension, stats_by_file_type, stats_by_gid,
        stats_by_uid,
    };

    // If no specific query requested, show overall stats
    let show_overview = !by_extension && !largest_files_flag && !largest_dirs
        && !oldest_files_flag && !most_links && !by_uid && !by_gid
        && !duplicates && !by_file_type && !hardlink_groups;

    if show_overview {
        let stats = compute_stats(db).context("Failed to compute stats")?;
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

    if by_extension {
        let ext_stats = stats_by_extension(db, top).context("Failed to get extension stats")?;
        println!();
        println!("Files by Extension (top {}):", top);
        println!("─────────────────────────────────────────────────");
        println!("{:<12} {:>12} {:>14} {:>14}", "Extension", "Count", "Size", "Allocated");
        println!("{:<12} {:>12} {:>14} {:>14}", "---------", "-----", "----", "---------");
        for stat in ext_stats {
            let ext = if stat.extension.is_empty() { "(none)" } else { &stat.extension };
            println!(
                "{:<12} {:>12} {:>14} {:>14}",
                ext,
                format_number(stat.count),
                format_size(stat.total_bytes, BINARY),
                format_size(stat.total_blocks * 512, BINARY),
            );
        }
        println!();
    }

    if largest_files_flag {
        let files = largest_files(db, top).context("Failed to get largest files")?;
        println!();
        println!("Largest Files (top {}):", top);
        println!("─────────────────────────────────────────────────");
        for (path, size) in files {
            println!("{:>14}  {}", format_size(size, BINARY), path);
        }
        println!();
    }

    if largest_dirs {
        let dirs = largest_directories(db, top).context("Failed to get largest directories")?;
        println!();
        println!("Directories with Most Files (top {}):", top);
        println!("─────────────────────────────────────────────────");
        println!("{:>12}  Path", "Files");
        println!("{:>12}  ----", "-----");
        for (path, count) in dirs {
            println!("{:>12}  {}", format_number(count), path);
        }
        println!();
    }

    if oldest_files_flag {
        let files = oldest_files(db, top).context("Failed to get oldest files")?;
        println!();
        println!("Oldest Files (top {}):", top);
        println!("─────────────────────────────────────────────────");
        for (path, mtime, size) in files {
            let time_str = mtime
                .map(|t| {
                    chrono::DateTime::from_timestamp(t, 0)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                        .unwrap_or_else(|| t.to_string())
                })
                .unwrap_or_else(|| "unknown".to_string());
            println!("{:>16}  {:>10}  {}", time_str, format_size(size, BINARY), path);
        }
        println!();
    }

    if most_links {
        let files = most_hardlinks(db, top).context("Failed to get files with most links")?;
        println!();
        println!("Files with Most Hard Links (top {}):", top);
        println!("─────────────────────────────────────────────────");
        println!("{:>8}  {:>12}  Path", "Links", "Size");
        println!("{:>8}  {:>12}  ----", "-----", "----");
        for (path, nlink, size) in files {
            println!("{:>8}  {:>12}  {}", nlink, format_size(size, BINARY), path);
        }
        println!();
    }

    if by_uid {
        let stats = stats_by_uid(db, top).context("Failed to get stats by UID")?;
        println!();
        println!("Usage by User ID (top {}):", top);
        println!("─────────────────────────────────────────────────");
        println!("{:>8}  {:>12}  {:>10}  {:>14}", "UID", "Files", "Dirs", "Total Size");
        println!("{:>8}  {:>12}  {:>10}  {:>14}", "---", "-----", "----", "----------");
        for stat in stats {
            println!(
                "{:>8}  {:>12}  {:>10}  {:>14}",
                stat.id,
                format_number(stat.file_count),
                format_number(stat.dir_count),
                format_size(stat.total_bytes, BINARY),
            );
        }
        println!();
    }

    if by_gid {
        let stats = stats_by_gid(db, top).context("Failed to get stats by GID")?;
        println!();
        println!("Usage by Group ID (top {}):", top);
        println!("─────────────────────────────────────────────────");
        println!("{:>8}  {:>12}  {:>10}  {:>14}", "GID", "Files", "Dirs", "Total Size");
        println!("{:>8}  {:>12}  {:>10}  {:>14}", "---", "-----", "----", "----------");
        for stat in stats {
            println!(
                "{:>8}  {:>12}  {:>10}  {:>14}",
                stat.id,
                format_number(stat.file_count),
                format_number(stat.dir_count),
                format_size(stat.total_bytes, BINARY),
            );
        }
        println!();
    }

    if duplicates {
        let groups = find_duplicates(db, min_size, top).context("Failed to find duplicates")?;
        if groups.is_empty() {
            println!();
            println!("No duplicate files found (min size: {})", format_size(min_size, BINARY));
            println!("Note: Requires --checksum flag during scan.");
            println!();
        } else {
            println!();
            println!("Duplicate Files (top {} groups, min size {}):", top, format_size(min_size, BINARY));
            println!("─────────────────────────────────────────────────");
            for group in groups {
                println!();
                println!("  Checksum: {}  Size: {}  Wasted: {}",
                    &group.checksum[..16], // Show first 16 chars of checksum
                    format_size(group.file_size, BINARY),
                    format_size(group.wasted_bytes, BINARY));
                for path in &group.paths {
                    println!("    {}", path);
                }
            }
            println!();
        }
    }

    if by_file_type {
        let stats = stats_by_file_type(db, top).context("Failed to get file type stats")?;
        if stats.is_empty() || (stats.len() == 1 && stats[0].mime_type == "unknown") {
            println!();
            println!("No file type data available.");
            println!("Note: Requires --file-type flag during scan.");
            println!();
        } else {
            println!();
            println!("Files by Detected Type (top {}):", top);
            println!("─────────────────────────────────────────────────");
            println!("{:<40} {:>12} {:>14}", "MIME Type", "Count", "Size");
            println!("{:<40} {:>12} {:>14}", "---------", "-----", "----");
            for stat in stats {
                println!(
                    "{:<40} {:>12} {:>14}",
                    stat.mime_type,
                    format_number(stat.count),
                    format_size(stat.total_bytes, BINARY),
                );
            }
            println!();
        }
    }

    if hardlink_groups {
        let groups = find_hardlink_groups(db, 2, top).context("Failed to find hardlink groups")?;
        if groups.is_empty() {
            println!();
            println!("No hard link groups found.");
            println!();
        } else {
            println!();
            println!("Hard Link Groups (top {}):", top);
            println!("─────────────────────────────────────────────────");
            for group in groups {
                println!();
                println!("  Inode: {}  Links: {}  Size: {}",
                    group.inode,
                    group.nlink,
                    format_size(group.size, BINARY));
                for path in &group.paths {
                    println!("    {}", path);
                }
            }
            println!();
        }
    }

    Ok(())
}

/// Run RocksDB to SQLite conversion
#[cfg(feature = "rocksdb")]
fn run_convert(
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
) -> Result<()> {
    use nfs_walker::parquet::{convert_rocks_to_parquet, ExportConfig};

    eprintln!("Exporting RocksDB to Parquet...");
    eprintln!("  Input:      {}", input.display());
    eprintln!("  Output dir: {}", output_dir.display());

    let config = ExportConfig {
        row_group_size,
        target_file_size: file_size_mb * 1024 * 1024,
        compression_level,
        progress: show_progress,
    };

    let progress_reporter = if show_progress {
        let reporter = ProgressReporter::new();
        reporter.set_status("Exporting...");
        Some(reporter)
    } else {
        None
    };

    let callback: Option<Box<dyn Fn(u64, u64) + Send>> = if let Some(ref p) = progress_reporter {
        let p_clone = p.clone();
        Some(Box::new(move |exported, _total| {
            let msg = format!("Exported {} entries", format_number(exported));
            p_clone.set_status(&msg);
        }))
    } else {
        None
    };

    let stats = convert_rocks_to_parquet(input, output_dir, config, callback)
        .context("Parquet export failed")?;

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
#[cfg(feature = "rocksdb")]
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
            let msg = format!(
                "Dirs: {} | Files: {} | Entries: {} | Size: {} | {}",
                format_number(prog.dirs),
                format_number(prog.files),
                format_number(entries),
                bytes_str,
                format_elapsed(prog.elapsed),
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
