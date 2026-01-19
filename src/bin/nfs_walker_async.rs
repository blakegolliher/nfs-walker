//! nfs-walker-async - Async pipelined NFS walker
//!
//! Uses READDIR + pipelined GETATTR for potentially faster scanning.

use anyhow::{Context, Result};
use clap::Parser;
use humansize::{format_size, BINARY};
use nfs_walker::config::{CliArgs, WalkConfig};
use nfs_walker::progress::{print_header, print_summary, ProgressReporter};
use nfs_walker::walker::AsyncWalker;
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
    let args = CliArgs::parse();
    setup_logging(args.verbose)?;

    let config = WalkConfig::from_args(args.clone())
        .context("Invalid configuration")?;

    if config.show_progress {
        print_header(
            &config.nfs_url.to_display_string(),
            config.worker_count,
            &config.output_path.display().to_string(),
        );
        println!("  Mode: READDIR + Async Pipelined GETATTR");
        println!();
    }

    let walker = AsyncWalker::new(config.clone());

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
            let rate = prog.files_per_second();
            let msg = format!(
                "Dirs: {} | Files: {} | Size: {} | Rate: {:.0}/s | Errors: {}",
                format_number(prog.dirs),
                format_number(prog.files),
                bytes_str,
                rate,
                prog.errors,
            );
            p_clone.set_status(&msg);
        })
        .context("Walk failed")?
    } else {
        walker.run()
            .context("Walk failed")?
    };

    if let Some(ref p) = progress {
        if result.completed {
            p.finish("Walk completed");
        } else {
            p.finish("Walk interrupted");
        }
    }

    let db_size = std::fs::metadata(&config.output_path)
        .map(|m| m.len())
        .ok();

    print_summary(
        result.dirs,
        result.files,
        result.bytes,
        result.errors,
        result.duration,
        &config.output_path.display().to_string(),
        db_size,
    );

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
