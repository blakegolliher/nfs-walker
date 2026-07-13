//! nfs-walker - Simple NFS Filesystem Scanner
//!
//! Entry point for the CLI application.

// Route every Rust allocation through mimalloc instead of the system malloc.
// The cargo-zigbuild musl binary links a Zig C-shim malloc backed by Zig's
// SmpAllocator, which returns NULL for ~30 KB requests once thread count
// crosses ~250 (observed reliably at WORKERS=230 + 32 parquet writers on
// se-var-n8). Setting #[global_allocator] makes Rust skip the system malloc
// entirely, so the parquet dict-encoder rehash path no longer crashes.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::{Context, Result};
use clap::Parser;
use humansize::{format_size, BINARY};
use nfs_walker::config::{CliArgs, Command, WalkConfig};
use nfs_walker::progress::{format_elapsed, format_number, print_header, print_summary, ProgressReporter};
use nfs_walker::walker::{SimpleWalker, WalkStats};
use std::process::ExitCode;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// Raise the soft `RLIMIT_NOFILE` to at least `TARGET` (or the hard limit,
/// whichever is lower). Large scans can hold thousands of per-worker NFS
/// sockets plus per-shard Parquet writers, and the default soft limit of
/// 1024 on most distros causes "Too many open files" crashes at
/// multi-billion-entry scale.
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
            eprintln!("Error: {:#}", e);
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    // Parse CLI arguments
    let args = CliArgs::parse();

    // Setup logging
    setup_logging(args.verbose);

    // Raise FD limit early so scans (lots of NFS sockets + Parquet part
    // files per shard) don't hit the default 1024-FD soft limit.
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
        eprintln!("Mode: READDIRPLUS (streaming Parquet, {} shards)", config.writer_shards);
    }

    // Save output path before moving config
    let output_path = config.output_path.clone();

    // Run the walker
    let result = run_simple_walker(config)?;

    // Get on-disk scan output size (recursive — handles scans/<id>/*.parquet)
    let db_size = get_scan_size(&output_path);

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

/// Handle subcommands (stats, serve).
fn handle_command(cmd: &Command) -> Result<()> {
    match cmd {
        Command::Stats { scan_dir } => run_stats(scan_dir),
        #[cfg(feature = "server")]
        Command::Serve { data_dir, port, bind } => run_server(data_dir, bind, *port),
    }
}

/// Print a scan-overview from a Parquet scan directory.
///
/// Reads `metadata.json` to find part files, then projects four columns
/// (`file_type`, `size`, `allocated_blocks`, `depth`) and aggregates in
/// one pass. Detail-level analytics live in DuckDB / DataFusion — this
/// just produces the headline counts a caller would otherwise hand-roll.
fn run_stats(scan_dir: &std::path::Path) -> Result<()> {
    use arrow::array::{Array, StringArray, UInt16Array, UInt64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ProjectionMask;
    use std::fs::File;

    let parts = list_parquet_parts(scan_dir)
        .with_context(|| format!("Reading scan dir {}", scan_dir.display()))?;
    if parts.is_empty() {
        anyhow::bail!(
            "No Parquet part files found under {} — is this a scan directory?",
            scan_dir.display()
        );
    }

    let mut total_entries: u64 = 0;
    let mut total_files: u64 = 0;
    let mut total_dirs: u64 = 0;
    let mut total_symlinks: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_blocks: u64 = 0;
    let mut max_depth: u16 = 0;

    for part in &parts {
        let file = File::open(part)
            .with_context(|| format!("opening part {}", part.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("opening Parquet reader for {}", part.display()))?;
        let schema = builder.parquet_schema();
        // Project only the columns we need; cuts >80% of the read.
        let leaves = ["file_type", "size", "allocated_blocks", "depth"];
        let proj_indices: Vec<usize> = leaves
            .iter()
            .map(|name| {
                schema
                    .columns()
                    .iter()
                    .position(|c| c.name() == *name)
                    .ok_or_else(|| anyhow::anyhow!("column {} missing from {}", name, part.display()))
            })
            .collect::<Result<_, _>>()?;
        let mask = ProjectionMask::leaves(schema, proj_indices);
        let reader = builder
            .with_projection(mask)
            .build()
            .with_context(|| format!("building reader for {}", part.display()))?;
        for batch in reader {
            let batch = batch?;
            total_entries += batch.num_rows() as u64;
            let ft = batch
                .column_by_name("file_type")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| anyhow::anyhow!("file_type column type mismatch in {}", part.display()))?;
            let size = batch
                .column_by_name("size")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| anyhow::anyhow!("size column type mismatch in {}", part.display()))?;
            let blocks = batch
                .column_by_name("allocated_blocks")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| anyhow::anyhow!("allocated_blocks column type mismatch in {}", part.display()))?;
            let depth = batch
                .column_by_name("depth")
                .and_then(|c| c.as_any().downcast_ref::<UInt16Array>())
                .ok_or_else(|| anyhow::anyhow!("depth column type mismatch in {}", part.display()))?;
            for i in 0..batch.num_rows() {
                match ft.value(i) {
                    "file" => {
                        total_files += 1;
                        total_bytes += size.value(i);
                        total_blocks += blocks.value(i);
                    }
                    "directory" => total_dirs += 1,
                    "symlink" => total_symlinks += 1,
                    _ => {}
                }
                let d = depth.value(i);
                if d > max_depth {
                    max_depth = d;
                }
            }
        }
    }

    println!();
    println!("Scan Statistics");
    println!("─────────────────────────────────────────────────");
    println!("  Total entries:  {}", format_number(total_entries));
    println!("  Files:          {}", format_number(total_files));
    println!("  Directories:    {}", format_number(total_dirs));
    println!("  Symlinks:       {}", format_number(total_symlinks));
    println!("  Total size:     {}", format_size(total_bytes, BINARY));
    println!("  Allocated:      {}", format_size(total_blocks * 512, BINARY));
    println!("  Max depth:      {}", max_depth);
    println!("  Part files:     {}", parts.len());
    println!();

    Ok(())
}

/// Collect part files from a scan dir. Prefers the `metadata.json` list
/// (canonical), falls back to `scans/*/part-*.parquet` glob if missing.
fn list_parquet_parts(scan_dir: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    // Look for `<scan_dir>/scans/<scan_id>/metadata.json`. We accept any
    // single scan directly (most common) or recurse into all of them.
    let mut roots: Vec<std::path::PathBuf> = Vec::new();
    let scans_dir = scan_dir.join("scans");
    if scans_dir.is_dir() {
        for entry in std::fs::read_dir(&scans_dir)? {
            let p = entry?.path();
            if p.is_dir() {
                roots.push(p);
            }
        }
    } else if scan_dir.is_dir() {
        // Maybe the user pointed directly at a scans/<id>/ dir.
        roots.push(scan_dir.to_path_buf());
    } else {
        anyhow::bail!("Not a directory: {}", scan_dir.display());
    }

    let mut out: Vec<std::path::PathBuf> = Vec::new();
    for root in &roots {
        let meta_path = root.join("metadata.json");
        if meta_path.is_file() {
            let raw = std::fs::read_to_string(&meta_path)
                .with_context(|| format!("reading {}", meta_path.display()))?;
            let v: serde_json::Value = serde_json::from_str(&raw)
                .with_context(|| format!("parsing {}", meta_path.display()))?;
            if let Some(arr) = v.get("parquet_files").and_then(|x| x.as_array()) {
                for f in arr {
                    if let Some(name) = f.as_str() {
                        out.push(root.join(name));
                    }
                }
                continue;
            }
        }
        // Fallback: glob the directory directly.
        for entry in std::fs::read_dir(root)? {
            let p = entry?.path();
            if p.extension().is_some_and(|e| e == "parquet") {
                out.push(p);
            }
        }
    }
    out.sort();
    Ok(out)
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

/// Recursive on-disk size of the scan output directory. Walks
/// subdirectories so the `scans/<id>/part-*.parquet` layout reports
/// honestly. Symlinks are never followed (lstat-based file_type).
fn get_scan_size(path: &std::path::Path) -> Option<u64> {
    if !path.is_dir() {
        return None;
    }
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                // file_type() is lstat-based; never follow symlinks, so
                // a self-referencing or cyclic symlink under the output
                // path can't put us in an infinite walk.
                let Ok(ft) = entry.file_type() else { continue };
                if ft.is_symlink() {
                    continue;
                }
                if ft.is_dir() {
                    stack.push(entry.path());
                } else {
                    let Ok(meta) = entry.metadata() else { continue };
                    total += meta.len();
                }
            }
        }
    }
    Some(total)
}

fn setup_logging(verbose: bool) {
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
