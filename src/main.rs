//! nfs-walker - High-Performance NFS Filesystem Scanner
//!
//! Entry point for the CLI application.

use anyhow::{Context, Result};
use clap::Parser;
use humansize::{format_size, BINARY};
use nfs_walker::config::{CliArgs, OutputFormat, WalkConfig};
use nfs_walker::db::BatchedWriter;
use nfs_walker::progress::{print_header, print_summary, ProgressReporter};
use nfs_walker::walker::{AsyncWalkCoordinator, FastWalker, HfcProgress, WalkCoordinator};
use std::process::ExitCode;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{error, info};
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

    // Validate and create config
    let config = WalkConfig::from_args(args.clone())
        .context("Invalid configuration")?;

    // Log output format
    if config.output_format != OutputFormat::Sqlite {
        info!("Using {} output format", format_name(config.output_format));
    }

    // Print header
    if config.show_progress {
        print_header(
            &config.nfs_url.to_display_string(),
            config.worker_count,
            &config.output_path.display().to_string(),
        );
    }

    // Run in HFC mode, async mode, or sync mode
    if config.hfc_mode {
        run_hfc(config)
    } else if config.use_async {
        run_async(config)
    } else {
        run_sync(config)
    }
}

/// Run the walk in synchronous mode (original implementation)
fn run_sync(config: WalkConfig) -> Result<()> {
    // Create coordinator
    let coordinator = WalkCoordinator::new(config.clone())
        .context("Failed to initialize walker")?;

    // Setup signal handler for graceful shutdown
    let shutdown_flag = coordinator.shutdown_flag();
    ctrlc::set_handler(move || {
        eprintln!("\nInterrupt received, shutting down...");
        shutdown_flag.store(true, Ordering::SeqCst);
    })
    .context("Failed to set signal handler")?;

    // Create progress reporter
    let progress = if config.show_progress {
        Some(ProgressReporter::new())
    } else {
        None
    };

    if let Some(ref p) = progress {
        p.set_status("Connecting to NFS server...");
    }

    // Run the walk with progress updates
    let result = if let Some(ref p) = progress {
        coordinator.run_with_progress(|prog| p.update(&prog))
            .context("Walk failed")?
    } else {
        coordinator.run()
            .context("Walk failed")?
    };

    // Finish progress
    if let Some(ref p) = progress {
        if result.completed {
            p.finish("Walk completed");
        } else {
            p.finish("Walk interrupted");
        }
    }

    // Get database file size
    let db_size = std::fs::metadata(&config.output_path)
        .map(|m| m.len())
        .ok();

    // Print summary
    print_summary(
        result.total_dirs,
        result.total_files,
        result.total_bytes,
        result.errors,
        result.duration,
        &config.output_path.display().to_string(),
        db_size,
    );

    // Report success/failure
    if !result.completed {
        info!("Walk was interrupted before completion");
    }

    if result.errors > 0 {
        info!(errors = result.errors, "Walk completed with errors");
    }

    Ok(())
}

/// Run the walk in async mode with connection pooling
fn run_async(config: WalkConfig) -> Result<()> {
    // Create tokio runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to create async runtime")?;

    runtime.block_on(async {
        run_async_inner(config).await
    })
}

async fn run_async_inner(config: WalkConfig) -> Result<()> {
    // Create async coordinator
    let coordinator = AsyncWalkCoordinator::new(config.clone(), config.connection_count)
        .context("Failed to initialize async walker")?;

    // Setup signal handler for graceful shutdown
    let shutdown_flag = coordinator.shutdown_flag();
    ctrlc::set_handler(move || {
        eprintln!("\nInterrupt received, shutting down...");
        shutdown_flag.store(true, Ordering::SeqCst);
    })
    .context("Failed to set signal handler")?;

    // Create progress reporter
    let progress = if config.show_progress {
        Some(ProgressReporter::new())
    } else {
        None
    };

    if let Some(ref p) = progress {
        p.set_status("Connecting to NFS server (async mode)...");
    }

    // Run the async walk
    let result = coordinator.run().await
        .context("Async walk failed")?;

    // Finish progress
    if let Some(ref p) = progress {
        if result.completed {
            p.finish("Walk completed");
        } else {
            p.finish("Walk interrupted");
        }
    }

    // Get database file size
    let db_size = std::fs::metadata(&config.output_path)
        .map(|m| m.len())
        .ok();

    // Print summary
    print_summary(
        result.total_dirs,
        result.total_files,
        result.total_bytes,
        result.errors,
        result.duration,
        &config.output_path.display().to_string(),
        db_size,
    );

    // Report success/failure
    if !result.completed {
        info!("Walk was interrupted before completion");
    }

    if result.errors > 0 {
        info!(errors = result.errors, "Walk completed with errors");
    }

    Ok(())
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

/// Run in HFC (High File Count) mode - fast scan of single directory
fn run_hfc(config: WalkConfig) -> Result<()> {
    info!("Starting HFC (High File Count) mode");

    // Create tokio runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to create async runtime")?;

    runtime.block_on(async {
        run_hfc_inner(config).await
    })
}

async fn run_hfc_inner(config: WalkConfig) -> Result<()> {
    // Create database writer
    let writer = BatchedWriter::new(
        &config.output_path,
        config.batch_size,
        config.queue_size,
    )
    .context("Failed to create database writer")?;

    let writer_handle = writer.handle();

    // Create fast walker
    let walker = FastWalker::new(config.clone());

    // Setup signal handler for graceful shutdown
    let shutdown_flag = walker.shutdown_flag();
    ctrlc::set_handler(move || {
        eprintln!("\nInterrupt received, shutting down...");
        shutdown_flag.store(true, Ordering::SeqCst);
    })
    .context("Failed to set signal handler")?;

    // Create progress reporter
    let progress_reporter = if config.show_progress {
        Some(ProgressReporter::new())
    } else {
        None
    };

    if let Some(ref p) = progress_reporter {
        p.set_status("Collecting file names...");
    }

    // Get progress counters for live updates
    let hfc_progress = walker.progress();

    // Spawn progress update task
    let progress_handle = if let Some(ref reporter) = progress_reporter {
        let reporter = reporter.clone();
        let progress = hfc_progress.clone();
        Some(tokio::spawn(async move {
            update_hfc_progress(reporter, progress).await;
        }))
    } else {
        None
    };

    // Run the HFC scan
    let result = walker.run_hfc(&writer_handle).await
        .context("HFC scan failed")?;

    // Stop progress update task
    if let Some(handle) = progress_handle {
        handle.abort();
    }

    // Finish progress
    if let Some(ref p) = progress_reporter {
        if result.completed {
            p.finish("HFC scan completed");
        } else {
            p.finish("HFC scan interrupted");
        }
    }

    // Shutdown writer and finalize database
    info!("Finalizing database...");
    writer.finish().context("Failed to finalize database")?;

    // Get database file size
    let db_size = std::fs::metadata(&config.output_path)
        .map(|m| m.len())
        .ok();

    // Print summary
    print_summary(
        result.total_dirs,
        result.total_files,
        result.total_bytes,
        result.errors,
        result.duration,
        &config.output_path.display().to_string(),
        db_size,
    );

    // Report success/failure
    if !result.completed {
        info!("HFC scan was interrupted before completion");
    }

    if result.errors > 0 {
        info!(errors = result.errors, "HFC scan completed with errors");
    }

    Ok(())
}

/// Update progress display for HFC mode
async fn update_hfc_progress(reporter: ProgressReporter, progress: HfcProgress) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));

    loop {
        interval.tick().await;

        let phase = progress.phase.load(Ordering::Relaxed);
        let total = progress.total_names.load(Ordering::Relaxed);
        let processed = progress.processed.load(Ordering::Relaxed);
        let files = progress.files.load(Ordering::Relaxed);
        let dirs = progress.dirs.load(Ordering::Relaxed);
        let bytes = progress.bytes.load(Ordering::Relaxed);

        let msg = if phase == 0 {
            "Collecting file names...".to_string()
        } else if total > 0 {
            let pct = (processed as f64 / total as f64 * 100.0).min(100.0);
            let bytes_str = format_size(bytes, BINARY);
            format!(
                "Files: {} | Dirs: {} | Size: {} | Progress: {:.1}% ({}/{})",
                format_number(files),
                format_number(dirs),
                bytes_str,
                pct,
                format_number(processed),
                format_number(total)
            )
        } else {
            "Processing...".to_string()
        };

        reporter.set_status(&msg);
    }
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

fn format_name(format: OutputFormat) -> &'static str {
    match format {
        OutputFormat::Sqlite => "SQLite",
        OutputFormat::Parquet => "Parquet",
        OutputFormat::Arrow => "Arrow",
    }
}
