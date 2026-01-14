//! nfs-walker - High-Performance NFS Filesystem Scanner
//!
//! Entry point for the CLI application.

use anyhow::{Context, Result};
use clap::Parser;
use nfs_walker::config::{CliArgs, OutputFormat, WalkConfig};
use nfs_walker::progress::{print_header, print_summary, ProgressReporter};
use nfs_walker::walker::{AsyncWalkCoordinator, WalkCoordinator};
use std::process::ExitCode;
use std::sync::atomic::Ordering;
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

    // Run in async or sync mode
    if config.use_async {
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

    // Run the walk
    let result = coordinator.run()
        .context("Walk failed")?;

    // Finish progress
    if let Some(ref p) = progress {
        if result.completed {
            p.finish("Walk completed");
        } else {
            p.finish("Walk interrupted");
        }
    }

    // Print summary
    print_summary(
        result.total_dirs,
        result.total_files,
        result.total_bytes,
        result.errors,
        result.duration,
        &config.output_path.display().to_string(),
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

    // Print summary
    print_summary(
        result.total_dirs,
        result.total_files,
        result.total_bytes,
        result.errors,
        result.duration,
        &config.output_path.display().to_string(),
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
        EnvFilter::new("nfs_walker=debug,warn")
    } else {
        EnvFilter::new("nfs_walker=info,warn")
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

fn format_name(format: OutputFormat) -> &'static str {
    match format {
        OutputFormat::Sqlite => "SQLite",
        OutputFormat::Parquet => "Parquet",
        OutputFormat::Arrow => "Arrow",
    }
}
