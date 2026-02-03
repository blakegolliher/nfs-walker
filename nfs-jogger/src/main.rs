//! nfs-jogger - Distributed NFS filesystem walker
//!
//! A distributed system for parallel filesystem processing, designed for
//! massive data migrations and indexing that scales with additional resources.

use clap::Parser;
use console::{style, Term};
use humansize::{format_size, BINARY};
use indicatif::{ProgressBar, ProgressStyle};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use nfs_jogger::config::{CliArgs, Command, DiscoveryConfig, WorkerConfig};
use nfs_jogger::coordinator::Coordinator;
use nfs_jogger::discovery::{DiscoveryProgress, DiscoveryScanner};
use nfs_jogger::queue::RedisQueueConfig;
use nfs_jogger::worker::{BundleWorker, WorkerProgress};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("nfs_jogger=info".parse().unwrap()),
        )
        .init();

    // Parse CLI arguments
    let args = CliArgs::parse();

    // Handle Ctrl+C
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();
    ctrlc::set_handler(move || {
        if shutdown_clone.load(Ordering::Relaxed) {
            eprintln!("\nForce shutdown!");
            std::process::exit(130);
        }
        eprintln!("\nShutting down gracefully... (press Ctrl+C again to force)");
        shutdown_clone.store(true, Ordering::SeqCst);
    })?;

    match args.command {
        Command::Discover {
            nfs_url,
            max_paths,
            max_size,
            workers,
            max_depth,
            exclude_patterns,
            timeout,
            export,
            dirs_only,
        } => {
            run_discover(
                &args.redis,
                &nfs_url,
                max_paths,
                &max_size,
                workers,
                max_depth,
                &exclude_patterns,
                timeout,
                export.as_deref(),
                dirs_only,
                args.quiet,
                args.verbose,
                shutdown,
            )
            .await?
        }

        Command::Work {
            output,
            timeout,
            retries,
            workers,
            worker_id,
            continuous,
            max_bundles,
        } => {
            run_work(
                &args.redis,
                &output,
                timeout,
                retries,
                workers,
                worker_id.as_deref(),
                continuous,
                max_bundles,
                args.quiet,
                args.verbose,
                shutdown,
            )
            .await?
        }

        Command::Status { watch, interval, format } => {
            run_status(&args.redis, watch, interval, &format).await?
        }

        Command::Reset { all, yes } => {
            run_reset(&args.redis, all, yes).await?
        }

        Command::Retry { all, bundle_id } => {
            run_retry(&args.redis, all, bundle_id.as_deref()).await?
        }
    }

    Ok(())
}

async fn run_discover(
    redis_url: &str,
    nfs_url: &str,
    max_paths: usize,
    max_size: &str,
    workers: usize,
    max_depth: Option<usize>,
    exclude_patterns: &[String],
    timeout: u32,
    export: Option<&str>,
    dirs_only: bool,
    quiet: bool,
    verbose: bool,
    shutdown: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    // Create configuration
    let config = DiscoveryConfig::from_discover_args(
        nfs_url,
        max_paths,
        max_size,
        workers,
        max_depth,
        exclude_patterns,
        timeout,
        export,
        dirs_only,
        redis_url,
        quiet,
        verbose,
    )?;

    println!(
        "{} Scanning {} with {} workers",
        style("[Discovery]").cyan().bold(),
        style(config.nfs_url.to_display_string()).green(),
        workers
    );
    println!(
        "  Bundle limits: {} paths or {}",
        style(config.max_paths).yellow(),
        style(format_size(config.max_bytes, BINARY)).yellow()
    );
    println!("  Redis: {}", style(redis_url).dim());
    println!();

    // Create progress bar
    let pb = if !quiet {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap(),
        );
        pb.enable_steady_tick(Duration::from_millis(100));
        Some(pb)
    } else {
        None
    };

    // Create scanner
    let scanner = DiscoveryScanner::new(config);

    // Set up shutdown handler
    let scanner_shutdown = shutdown.clone();
    tokio::spawn(async move {
        while !scanner_shutdown.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Run discovery
    let pb_clone = pb.clone();
    let stats = scanner
        .run(move |progress: DiscoveryProgress| {
            if let Some(ref pb) = pb_clone {
                pb.set_message(format!(
                    "Dirs: {} | Files: {} ({}) | Bundles: {}/{} | Queue: {} | Workers: {} | Errors: {}",
                    style(progress.dirs_scanned).cyan(),
                    style(progress.files_found).green(),
                    style(format_size(progress.bytes_found, BINARY)).dim(),
                    style(progress.bundles_submitted).yellow(),
                    style(progress.bundles_created).dim(),
                    style(progress.queue_depth).blue(),
                    style(progress.active_workers).magenta(),
                    if progress.errors > 0 {
                        style(progress.errors).red().to_string()
                    } else {
                        style(progress.errors).dim().to_string()
                    }
                ));
            }
        })
        .await?;

    if let Some(pb) = pb {
        pb.finish_and_clear();
    }

    // Print summary
    println!();
    println!("{}", style("Discovery Complete").green().bold());
    println!("  Directories scanned: {}", style(stats.dirs_scanned).cyan());
    println!("  Files found: {}", style(stats.files_found).green());
    println!("  Total size: {}", style(format_size(stats.bytes_found, BINARY)).yellow());
    println!("  Bundles created: {}", style(stats.bundles_created).yellow());
    println!(
        "  Avg files/bundle: {:.1}",
        style(stats.avg_files_per_bundle).dim()
    );
    println!(
        "  Avg size/bundle: {}",
        style(format_size(stats.avg_bytes_per_bundle as u64, BINARY)).dim()
    );
    if stats.errors > 0 {
        println!("  Errors: {}", style(stats.errors).red());
    }
    println!("  Duration: {:.1}s", stats.duration.as_secs_f64());

    Ok(())
}

async fn run_work(
    redis_url: &str,
    output: &std::path::Path,
    timeout: u32,
    retries: u32,
    workers: usize,
    worker_id: Option<&str>,
    continuous: bool,
    max_bundles: Option<u64>,
    quiet: bool,
    verbose: bool,
    shutdown: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    // Create configuration
    let config = WorkerConfig::from_work_args(
        &output.to_path_buf(),
        timeout,
        retries,
        workers,
        worker_id,
        continuous,
        max_bundles,
        redis_url,
        quiet,
        verbose,
    )?;

    println!(
        "{} Starting worker {}",
        style("[Worker]").cyan().bold(),
        style(&config.worker_id).green()
    );
    println!("  Output: {}", style(output.display()).yellow());
    println!("  Redis: {}", style(redis_url).dim());
    println!(
        "  Mode: {}",
        if continuous {
            style("continuous").green()
        } else {
            style("until queue empty").yellow()
        }
    );
    println!();

    // Create progress bar
    let pb = if !quiet {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap(),
        );
        pb.enable_steady_tick(Duration::from_millis(100));
        Some(pb)
    } else {
        None
    };

    // Create worker
    let worker = BundleWorker::new(config);

    // Set up shutdown handler
    let worker_shutdown = shutdown.clone();
    tokio::spawn(async move {
        while !worker_shutdown.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Note: actual shutdown signaling would go through worker.shutdown()
    });

    // Run worker
    let pb_clone = pb.clone();
    let stats = worker
        .run(move |progress: WorkerProgress| {
            if let Some(ref pb) = pb_clone {
                let current = progress
                    .current_bundle
                    .as_ref()
                    .map(|s| &s[..8.min(s.len())])
                    .unwrap_or("idle");

                pb.set_message(format!(
                    "Bundles: {} ({} failed) | Files: {} | Dirs: {} | {} | Errors: {} | Current: {}",
                    style(progress.bundles_processed).green(),
                    if progress.bundles_failed > 0 {
                        style(progress.bundles_failed).red().to_string()
                    } else {
                        style(progress.bundles_failed).dim().to_string()
                    },
                    style(progress.files_processed).cyan(),
                    style(progress.dirs_processed).blue(),
                    style(format_size(progress.bytes_processed, BINARY)).dim(),
                    if progress.errors > 0 {
                        style(progress.errors).red().to_string()
                    } else {
                        style(progress.errors).dim().to_string()
                    },
                    style(current).yellow(),
                ));
            }
        })
        .await?;

    if let Some(pb) = pb {
        pb.finish_and_clear();
    }

    // Print summary
    println!();
    println!("{}", style("Worker Complete").green().bold());
    println!("  Bundles processed: {}", style(stats.bundles_processed).green());
    if stats.bundles_failed > 0 {
        println!("  Bundles failed: {}", style(stats.bundles_failed).red());
    }
    println!("  Files processed: {}", style(stats.files_processed).cyan());
    println!("  Directories processed: {}", style(stats.dirs_processed).blue());
    println!(
        "  Total size: {}",
        style(format_size(stats.bytes_processed, BINARY)).yellow()
    );
    if stats.errors > 0 {
        println!("  Errors: {}", style(stats.errors).red());
    }
    println!("  Duration: {:.1}s", stats.duration.as_secs_f64());

    Ok(())
}

async fn run_status(redis_url: &str, watch: bool, interval: u64, format: &str) -> anyhow::Result<()> {
    let config = RedisQueueConfig::with_url(redis_url);
    let coordinator = Coordinator::new(config).await?;

    loop {
        let status = coordinator.status().await?;

        if format == "json" {
            println!("{}", serde_json::to_string_pretty(&status)?);
        } else {
            // Clear screen in watch mode
            if watch {
                let term = Term::stdout();
                let _ = term.clear_screen();
            }

            println!("{}", style("nfs-jogger Status").cyan().bold());
            println!("{}", "=".repeat(50));
            println!();

            // Health indicator
            let health_style = match status.health.as_str() {
                "healthy" => style(&status.health).green(),
                "idle" => style(&status.health).dim(),
                "degraded" => style(&status.health).yellow(),
                _ => style(&status.health).red(),
            };
            println!("Health: {}", health_style.bold());
            println!();

            // Queue stats
            println!("{}", style("Queue").yellow().bold());
            println!("  Pending:    {}", style(status.queue.pending).cyan());
            println!("  Processing: {}", style(status.queue.processing).blue());
            println!("  Completed:  {}", style(status.queue.completed).green());
            println!("  Failed:     {}", if status.queue.failed > 0 {
                style(status.queue.failed).red()
            } else {
                style(status.queue.failed).dim()
            });
            println!();

            // Progress stats
            println!("{}", style("Progress").yellow().bold());
            println!("  Total submitted: {}", style(status.queue.total_submitted).dim());
            println!("  Files processed: {}", style(status.queue.total_files).cyan());
            println!(
                "  Bytes processed: {}",
                style(format_size(status.queue.total_bytes, BINARY)).green()
            );
            println!();

            // Workers
            println!("{}", style("Workers").yellow().bold());
            if status.workers.is_empty() {
                println!("  {}", style("No workers registered").dim());
            } else {
                for worker in &status.workers {
                    let status_icon = if worker.is_alive {
                        style("●").green()
                    } else {
                        style("○").red()
                    };
                    println!(
                        "  {} {} (last seen: {}s ago)",
                        status_icon,
                        worker.id,
                        worker.seconds_since_heartbeat
                    );
                }
            }
            println!();

            // Messages
            if !status.messages.is_empty() {
                println!("{}", style("Messages").yellow().bold());
                for msg in &status.messages {
                    println!("  {} {}", style("!").yellow(), msg);
                }
                println!();
            }

            println!("Last updated: {}", style(status.timestamp.format("%Y-%m-%d %H:%M:%S UTC")).dim());
        }

        if !watch {
            break;
        }

        tokio::time::sleep(Duration::from_secs(interval)).await;
    }

    Ok(())
}

async fn run_reset(redis_url: &str, all: bool, yes: bool) -> anyhow::Result<()> {
    if !yes {
        print!(
            "{} This will clear all {} from the queue. Continue? [y/N] ",
            style("Warning:").yellow().bold(),
            if all { "bundles (including completed)" } else { "pending/failed bundles" }
        );
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let config = RedisQueueConfig::with_url(redis_url);
    let coordinator = Coordinator::new(config).await?;

    coordinator.reset(all).await?;

    println!(
        "{} Queue has been reset.",
        style("Success:").green().bold()
    );

    Ok(())
}

async fn run_retry(redis_url: &str, all: bool, bundle_id: Option<&str>) -> anyhow::Result<()> {
    let config = RedisQueueConfig::with_url(redis_url);
    let coordinator = Coordinator::new(config).await?;

    let retried = if all {
        coordinator.retry_failed(None).await?
    } else if let Some(id) = bundle_id {
        coordinator.retry_failed(Some(id)).await?
    } else {
        println!("Error: Specify --all or provide a bundle ID");
        return Ok(());
    };

    if retried > 0 {
        println!(
            "{} Retried {} bundle(s).",
            style("Success:").green().bold(),
            retried
        );
    } else {
        println!("No failed bundles to retry.");
    }

    Ok(())
}
