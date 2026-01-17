//! Simple parallel directory processing for large flat directories
//!
//! 1. opendir + readdir loop (libnfs handles cookies internally)
//! 2. Split names into batches
//! 3. Workers do GETATTR in parallel

use crate::config::WalkConfig;
use crate::db::WriterHandle;
use crate::error::NfsError;
use crate::nfs::types::{DbEntry, EntryType};
use crate::nfs::{NfsConnection, NfsConnectionBuilder};
use crate::walker::queue::{DirTask, WorkQueueSender};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::info;

/// Process a large flat directory using simple opendir + parallel GETATTR
pub fn process_directory_parallel(
    nfs: &mut NfsConnection,
    path: &str,
    config: &WalkConfig,
    queue_tx: &WorkQueueSender,
    writer: &WriterHandle,
    parent_depth: u32,
    shutdown: &Arc<AtomicBool>,
) -> Result<(u64, u64, Duration), NfsError> {
    let start = Instant::now();

    // Phase 1: Simple opendir + readdir loop
    // libnfs handles cookies internally, returns None when done
    info!(path = %path, "Phase 1: Collecting names with opendir/readdir");

    let handle = nfs.opendir_names_only(path)?;
    let mut names: Vec<String> = Vec::new();

    while let Some(entry) = handle.readdir() {
        if entry.is_special() {
            continue;
        }
        names.push(entry.name);

        // Progress every 100K
        if names.len() % 100_000 == 0 {
            info!(count = names.len(), "Collecting names...");
        }
    }

    let total = names.len();
    let phase1_time = start.elapsed();

    info!(
        path = %path,
        entries = total,
        ms = phase1_time.as_millis(),
        rate = format!("{:.0}/s", total as f64 / phase1_time.as_secs_f64()),
        "Phase 1 complete"
    );

    if total == 0 {
        return Ok((0, 0, start.elapsed()));
    }

    // Phase 2: Parallel GETATTR
    let num_workers = config.worker_count.min(16);
    let entries_per_worker = (total + num_workers - 1) / num_workers;

    info!(path = %path, workers = num_workers, "Phase 2: Parallel GETATTR");

    let names = Arc::new(names);
    let path = Arc::new(path.to_string());
    let config = Arc::new(config.clone());
    let processed = Arc::new(AtomicU64::new(0));
    let dirs_found = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    for worker_id in 0..num_workers {
        let start_idx = worker_id * entries_per_worker;
        let end_idx = ((worker_id + 1) * entries_per_worker).min(total);

        if start_idx >= total {
            break;
        }

        let names = Arc::clone(&names);
        let path = Arc::clone(&path);
        let config = Arc::clone(&config);
        let queue_tx = queue_tx.clone();
        let writer = writer.clone();
        let processed = Arc::clone(&processed);
        let dirs_found = Arc::clone(&dirs_found);
        let shutdown = Arc::clone(shutdown);

        let handle = thread::Builder::new()
            .name(format!("getattr-{}", worker_id))
            .spawn(move || {
                let nfs = match NfsConnectionBuilder::new(config.nfs_url.clone())
                    .timeout(Duration::from_secs(config.timeout_secs as u64))
                    .retries(config.retry_count)
                    .connect()
                {
                    Ok(c) => c,
                    Err(_) => return,
                };

                for i in start_idx..end_idx {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    let name = &names[i];
                    let full_path = if path.as_str() == "/" {
                        format!("/{}", name)
                    } else {
                        format!("{}/{}", path, name)
                    };

                    if let Ok(stat) = nfs.stat(&full_path) {
                        let entry_type = stat.entry_type();
                        let depth = parent_depth + 1;

                        if entry_type == EntryType::Directory {
                            dirs_found.fetch_add(1, Ordering::Relaxed);
                            let should_queue = config.max_depth.map(|m| depth as usize <= m).unwrap_or(true);
                            if should_queue && !config.is_excluded(&full_path) {
                                let _ = queue_tx.try_send(DirTask::new(full_path.clone(), None, depth));
                            }
                        }

                        let db_entry = DbEntry::from_stat(&full_path, name, &stat, depth);
                        if !config.dirs_only || entry_type == EntryType::Directory {
                            let _ = writer.send_entry(db_entry);
                        }

                        processed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
            .expect("spawn worker");

        handles.push(handle);
    }

    // Progress thread
    let processed_clone = Arc::clone(&processed);
    let total_u64 = total as u64;
    let shutdown_clone = Arc::clone(shutdown);

    let progress = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(1));
            let done = processed_clone.load(Ordering::Relaxed);
            info!(done = done, total = total_u64, pct = format!("{:.1}%", done as f64 / total_u64 as f64 * 100.0), "GETATTR progress");
            if done >= total_u64 || shutdown_clone.load(Ordering::Relaxed) {
                break;
            }
        }
    });

    for h in handles {
        let _ = h.join();
    }
    let _ = progress.join();

    let total_time = start.elapsed();
    let done = processed.load(Ordering::Relaxed);
    let dirs = dirs_found.load(Ordering::Relaxed);

    info!(
        entries = done,
        dirs = dirs,
        ms = total_time.as_millis(),
        rate = format!("{:.0}/s", done as f64 / total_time.as_secs_f64()),
        "Complete"
    );

    Ok((done, dirs, total_time))
}
