//! Parallel Readdir Test using NFS Cookies
//!
//! Tests the new opendir_at_cookie() function for parallel directory reading.
//! Uses count-based partitioning since NFS cookies are hash-based (not ordered).
//!
//! Usage: cookie-debug <nfs://server/export/path> [num_workers]

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use nfs_walker::config::NfsUrl;
use nfs_walker::nfs::{NfsConnection, NfsConnectionBuilder};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <nfs://server/export/path> [num_workers]", args[0]);
        eprintln!("Example: {} nfs://server/share/large_dir 4", args[0]);
        std::process::exit(1);
    }

    let url_str = &args[1];
    let num_workers: usize = args.get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    // Parse NFS URL
    let url = match NfsUrl::parse(url_str) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("Failed to parse URL: {}", e);
            std::process::exit(1);
        }
    };

    println!("=== Parallel Readdir Test (Real NFS Cookies) ===\n");
    let subpath_display = if url.subpath.is_empty() {
        String::new()
    } else {
        format!("/{}", url.subpath)
    };
    println!("Target: {}:{}{}", url.server, url.export, subpath_display);
    println!("Workers: {}\n", num_workers);

    let path = url.walk_start_path();

    // === PHASE 1: Sample cookies ===
    println!("=== Phase 1: Sampling cookies ===");
    let phase1_start = Instant::now();

    let conn = connect(&url).expect("Failed to connect");
    let entries = conn.readdir_plus(&path).expect("Failed to read directory");

    let real_entries: Vec<_> = entries.into_iter()
        .filter(|e| e.name != "." && e.name != "..")
        .collect();

    let total_entries = real_entries.len();
    let phase1_duration = phase1_start.elapsed();

    println!("Read {} entries in {:.2}s", total_entries, phase1_duration.as_secs_f64());

    if total_entries < 1000 {
        println!("Directory too small for parallel test. Need at least 1000 entries.");
        return;
    }

    // Sample cookies at intervals for workers
    // Each worker will read `entries_per_worker` entries starting from their cookie
    let entries_per_worker = total_entries / num_workers;
    let mut worker_assignments: Vec<(u64, usize)> = Vec::with_capacity(num_workers);

    // Worker 0 starts at cookie 0
    worker_assignments.push((0, entries_per_worker));

    for i in 1..num_workers {
        let idx = i * entries_per_worker;
        let cookie = real_entries[idx].cookie;
        // Last worker reads any remaining entries
        let count = if i == num_workers - 1 {
            total_entries - idx
        } else {
            entries_per_worker
        };
        worker_assignments.push((cookie, count));
        println!("  Worker {} will start at entry {}, cookie={}, read {} entries",
            i, idx, cookie, count);
    }

    drop(conn);

    // === SEQUENTIAL BASELINE ===
    println!("\n=== Sequential Read (Baseline) ===");
    let seq_result = sequential_read(&url, &path);

    // === PARALLEL READ ===
    println!("\n=== Parallel Read with opendir_at_cookie ===");
    let par_result = parallel_read(&url, &path, num_workers, &worker_assignments);

    // === RESULTS ===
    println!("\n=== Results ===");
    println!("Sequential: {} entries in {:.2}s ({:.0} entries/sec)",
        seq_result.count, seq_result.duration.as_secs_f64(),
        seq_result.count as f64 / seq_result.duration.as_secs_f64());
    println!("Parallel:   {} entries in {:.2}s ({:.0} entries/sec)",
        par_result.count, par_result.duration.as_secs_f64(),
        par_result.count as f64 / par_result.duration.as_secs_f64());

    let speedup = seq_result.duration.as_secs_f64() / par_result.duration.as_secs_f64();
    println!("\nSpeedup: {:.2}x", speedup);

    if par_result.count != seq_result.count {
        println!("\n⚠ Entry count mismatch! Sequential={}, Parallel={}",
            seq_result.count, par_result.count);
    }

    if speedup > 1.0 {
        println!("✓ Parallel reading is FASTER!");
    } else {
        println!("✗ Parallel reading is slower (overhead exceeds benefit)");
    }
}

fn connect(url: &NfsUrl) -> Result<NfsConnection, String> {
    NfsConnectionBuilder::new(url.clone())
        .timeout(Duration::from_secs(30))
        .retries(3)
        .connect()
        .map_err(|e| e.to_string())
}

struct ReadResult {
    count: u64,
    duration: Duration,
}

fn sequential_read(url: &NfsUrl, path: &str) -> ReadResult {
    let conn = connect(url).expect("Failed to connect");

    let start = Instant::now();
    let entries = conn.readdir_plus(path).expect("Failed to read directory");

    let count = entries.iter()
        .filter(|e| e.name != "." && e.name != "..")
        .count() as u64;

    let duration = start.elapsed();
    println!("  Read {} entries in {:.2}s", count, duration.as_secs_f64());

    ReadResult { count, duration }
}

fn parallel_read(
    url: &NfsUrl,
    path: &str,
    num_workers: usize,
    assignments: &[(u64, usize)],  // (start_cookie, max_entries_to_read)
) -> ReadResult {
    let start = Instant::now();

    let total_count = Arc::new(AtomicU64::new(0));
    let url = Arc::new(url.clone());
    let path = Arc::new(path.to_string());
    let assignments = Arc::new(assignments.to_vec());

    let mut handles = Vec::with_capacity(num_workers);

    for worker_id in 0..num_workers {
        let url = Arc::clone(&url);
        let path = Arc::clone(&path);
        let assignments = Arc::clone(&assignments);
        let total_count = Arc::clone(&total_count);

        let handle = thread::spawn(move || {
            let (start_cookie, max_entries) = assignments[worker_id];

            let conn = connect(&url).expect("Worker failed to connect");

            // Use opendir_at_cookie with max_entries limit - libnfs will stop fetching
            // after reaching the limit, so each worker only fetches its portion
            let dir = conn.opendir_at_cookie(&path, start_cookie, max_entries as u32)
                .expect("Worker failed to open directory at cookie");

            let mut count = 0u64;
            while let Some(entry) = dir.readdir() {
                if entry.name == "." || entry.name == ".." {
                    continue;
                }
                count += 1;
            }

            total_count.fetch_add(count, Ordering::Relaxed);
            println!("  Worker {}: read {} entries (cookie {}, limit {})",
                worker_id, count, start_cookie, max_entries);

            count
        });

        handles.push(handle);
    }

    // Wait for all workers
    for handle in handles {
        handle.join().expect("Worker thread panicked");
    }

    let duration = start.elapsed();
    let final_count = total_count.load(Ordering::Relaxed);

    println!("  Total: {} entries in {:.2}s", final_count, duration.as_secs_f64());

    ReadResult {
        count: final_count,
        duration,
    }
}
