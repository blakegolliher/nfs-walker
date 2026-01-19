//! Streaming Pipeline Readdir Test
//!
//! Single fetcher streams batches to worker pool via channel.
//! No Phase 1 needed - starts producing results immediately.
//!
//! Usage: stream-test <nfs://server/export/path> [batch_size] [num_workers]

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use nfs_walker::config::NfsUrl;
use nfs_walker::nfs::{NfsConnection, NfsConnectionBuilder, NfsDirEntry};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <nfs://server/export/path> [batch_size] [num_workers]", args[0]);
        eprintln!("Example: {} nfs://server/share/dir 10000 4", args[0]);
        std::process::exit(1);
    }

    let url_str = &args[1];
    let batch_size: u32 = args.get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let num_workers: usize = args.get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    let url = match NfsUrl::parse(url_str) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("Failed to parse URL: {}", e);
            std::process::exit(1);
        }
    };

    println!("=== Streaming Pipeline Readdir Test ===\n");
    println!("Target: {}:{}", url.server, url.full_path());
    println!("Batch size: {}", batch_size);
    println!("Workers: {}\n", num_workers);

    let path = url.walk_start_path();

    // === SEQUENTIAL BASELINE ===
    println!("=== Sequential Read (Baseline) ===");
    let seq_result = sequential_read(&url, &path);
    println!("  Read {} entries in {:.2}s ({:.0} entries/sec)\n",
        seq_result.count, seq_result.duration.as_secs_f64(),
        seq_result.count as f64 / seq_result.duration.as_secs_f64());

    // === STREAMING PIPELINE ===
    println!("=== Streaming Pipeline ===");
    let stream_result = streaming_read(&url, &path, batch_size, num_workers);
    println!("  Processed {} entries in {:.2}s ({:.0} entries/sec)\n",
        stream_result.count, stream_result.duration.as_secs_f64(),
        stream_result.count as f64 / stream_result.duration.as_secs_f64());

    // === RESULTS ===
    println!("=== Results ===");
    println!("Sequential: {} entries in {:.2}s ({:.0} entries/sec)",
        seq_result.count, seq_result.duration.as_secs_f64(),
        seq_result.count as f64 / seq_result.duration.as_secs_f64());
    println!("Streaming:  {} entries in {:.2}s ({:.0} entries/sec)",
        stream_result.count, stream_result.duration.as_secs_f64(),
        stream_result.count as f64 / stream_result.duration.as_secs_f64());

    let speedup = seq_result.duration.as_secs_f64() / stream_result.duration.as_secs_f64();
    println!("\nSpeedup: {:.2}x", speedup);

    if speedup > 1.0 {
        println!("✓ Streaming pipeline is FASTER!");
    } else {
        println!("✗ Streaming pipeline is slower");
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

    ReadResult { count, duration: start.elapsed() }
}

fn streaming_read(url: &NfsUrl, path: &str, batch_size: u32, num_workers: usize) -> ReadResult {
    use std::sync::mpsc;

    let start = Instant::now();
    let total_count = Arc::new(AtomicU64::new(0));
    let batches_sent = Arc::new(AtomicU64::new(0));

    // Channel for batches: fetcher -> workers
    let (tx, rx) = mpsc::sync_channel::<Vec<NfsDirEntry>>(num_workers * 2);
    let rx = Arc::new(std::sync::Mutex::new(rx));

    // Spawn worker threads
    let mut worker_handles = Vec::with_capacity(num_workers);
    for worker_id in 0..num_workers {
        let rx = Arc::clone(&rx);
        let total_count = Arc::clone(&total_count);

        let handle = thread::spawn(move || {
            let mut local_count = 0u64;
            loop {
                // Try to receive a batch
                let batch = {
                    let rx = rx.lock().unwrap();
                    rx.recv()
                };

                match batch {
                    Ok(entries) => {
                        // Process the batch (just counting for now)
                        for entry in &entries {
                            if entry.name != "." && entry.name != ".." {
                                local_count += 1;
                            }
                        }
                    }
                    Err(_) => break, // Channel closed
                }
            }
            total_count.fetch_add(local_count, Ordering::Relaxed);
            println!("    Worker {}: processed {} entries", worker_id, local_count);
        });
        worker_handles.push(handle);
    }

    // Fetcher: single thread reading batches and sending to channel
    let fetcher_handle = {
        let url = url.clone();
        let path = path.to_string();
        let batches_sent = Arc::clone(&batches_sent);

        thread::spawn(move || {
            let conn = connect(&url).expect("Fetcher failed to connect");
            let mut cookie: u64 = 0;
            let mut total_fetched = 0u64;

            loop {
                // Fetch next batch starting at current cookie
                let dir = conn.opendir_at_cookie(&path, cookie, batch_size)
                    .expect("Failed to open directory at cookie");

                let mut entries = Vec::with_capacity(batch_size as usize);
                let mut last_cookie = cookie;

                while let Some(entry) = dir.readdir() {
                    last_cookie = entry.cookie;
                    entries.push(entry);
                }

                if entries.is_empty() {
                    break; // No more entries
                }

                total_fetched += entries.len() as u64;
                let batch_num = batches_sent.fetch_add(1, Ordering::Relaxed);

                if batch_num % 100 == 0 {
                    println!("  Fetcher: sent batch {}, {} entries so far", batch_num, total_fetched);
                }

                // Update cookie for next batch
                cookie = last_cookie;

                // Send batch to workers
                if tx.send(entries).is_err() {
                    break; // Workers died
                }

                // Check if we got fewer entries than requested (EOF)
                // Actually we can't easily tell - opendir_at_cookie reads until limit or EOF
                // We'll detect EOF when we get 0 entries on next iteration
            }

            println!("  Fetcher: done, sent {} total entries in {} batches",
                total_fetched, batches_sent.load(Ordering::Relaxed));
            drop(tx); // Close channel to signal workers to stop
        })
    };

    // Wait for fetcher
    fetcher_handle.join().expect("Fetcher panicked");

    // Wait for workers
    for handle in worker_handles {
        handle.join().expect("Worker panicked");
    }

    let duration = start.elapsed();
    let count = total_count.load(Ordering::Relaxed);

    ReadResult { count, duration }
}
