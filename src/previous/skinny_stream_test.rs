//! Test binary for streaming directory enumeration using names-only READDIR
//!
//! This tests the `opendir_names_only_at_cookie()` function which combines:
//! - Fast READDIR (no server-side stat)
//! - Cookie-based streaming (batch processing)
//! - Cookie verifier support (works with strict NFS servers)

use nfs_walker::config::NfsUrl;
use nfs_walker::nfs::NfsConnection;
use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <nfs://server/export/path> [batch_size]", args[0]);
        eprintln!("Example: {} nfs://server/export/dir 10000", args[0]);
        std::process::exit(1);
    }

    let url_str = &args[1];
    let batch_size: u32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10000);

    // Parse URL
    let url = match NfsUrl::parse(url_str) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("Failed to parse URL: {}", e);
            std::process::exit(1);
        }
    };

    let path = if url.subpath.is_empty() {
        "/".to_string()
    } else {
        url.subpath.clone()
    };

    println!("=== Skinny Stream Test (READDIR + Cookie + Batch) ===\n");
    println!("Server: {}:{}", url.server, url.export);
    println!("Path: {}", path);
    println!("Batch size: {}\n", batch_size);

    // Connect
    let mut conn = match NfsConnection::new(&url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to create connection: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(e) = conn.connect(Duration::from_secs(30)) {
        eprintln!("Failed to connect: {}", e);
        std::process::exit(1);
    }
    println!("Connected!\n");

    // Test 1: Standard READDIRPLUS (baseline)
    println!("=== Test 1: Standard READDIRPLUS (baseline) ===");
    let start = Instant::now();
    let dir = match conn.readdir_plus(&path) {
        Ok(entries) => entries,
        Err(e) => {
            eprintln!("READDIRPLUS failed: {}", e);
            std::process::exit(1);
        }
    };
    let readdirplus_time = start.elapsed();
    let readdirplus_count = dir.len();
    println!(
        "  Read {} entries in {:.2}s ({:.0} entries/sec)\n",
        readdirplus_count,
        readdirplus_time.as_secs_f64(),
        readdirplus_count as f64 / readdirplus_time.as_secs_f64()
    );

    // Test 2: Skinny streaming (names only + batched)
    println!("=== Test 2: Skinny Streaming (READDIR + batched) ===");
    let start = Instant::now();
    let mut total_count = 0u64;
    let mut batch_num = 0;
    let mut cookie: u64 = 0;
    let mut cookieverf: u64 = 0;
    let mut batch_times: Vec<f64> = Vec::new();

    loop {
        let batch_start = Instant::now();

        let dir_handle = match conn.opendir_names_only_at_cookie(&path, cookie, cookieverf, batch_size) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("opendir_names_only_at_cookie failed: {}", e);
                std::process::exit(1);
            }
        };

        let mut batch_count = 0u32;
        let mut last_cookie = cookie;

        while let Some(entry) = dir_handle.readdir() {
            last_cookie = entry.cookie;
            batch_count += 1;
        }

        // Get verifier for next batch
        let new_verifier = dir_handle.get_cookieverf();

        let batch_time = batch_start.elapsed().as_secs_f64();
        batch_times.push(batch_time);

        if batch_num < 5 || batch_count < batch_size {
            println!(
                "  Batch {}: {} entries in {:.3}s (cookie: {}, verifier: {})",
                batch_num, batch_count, batch_time, last_cookie, new_verifier
            );
        } else if batch_num == 5 {
            println!("  ... (suppressing further batch output) ...");
        }

        total_count += batch_count as u64;
        batch_num += 1;

        // Check for EOF
        // Note: max_entries limit may not be strict, so check for empty batch
        if batch_count == 0 {
            break;
        }

        // Safety limit - stop if we've read more than expected
        if total_count >= readdirplus_count as u64 + 1000 {
            println!("  (stopping - exceeded expected count)");
            break;
        }

        // Update for next batch
        cookie = last_cookie;
        cookieverf = new_verifier;
    }

    let skinny_time = start.elapsed();
    println!(
        "\n  Total: {} entries in {} batches, {:.2}s ({:.0} entries/sec)",
        total_count,
        batch_num,
        skinny_time.as_secs_f64(),
        total_count as f64 / skinny_time.as_secs_f64()
    );

    // Calculate batch statistics
    if !batch_times.is_empty() {
        let avg_batch = batch_times.iter().sum::<f64>() / batch_times.len() as f64;
        let min_batch = batch_times.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_batch = batch_times.iter().cloned().fold(0.0, f64::max);
        println!(
            "  Batch timing: avg={:.3}s, min={:.3}s, max={:.3}s",
            avg_batch, min_batch, max_batch
        );
    }

    // Summary
    println!("\n=== Summary ===");
    println!(
        "READDIRPLUS: {} entries in {:.2}s",
        readdirplus_count,
        readdirplus_time.as_secs_f64()
    );
    println!(
        "Skinny:      {} entries in {:.2}s",
        total_count,
        skinny_time.as_secs_f64()
    );

    if skinny_time < readdirplus_time {
        let speedup = readdirplus_time.as_secs_f64() / skinny_time.as_secs_f64();
        println!("Skinny is {:.2}x FASTER", speedup);
    } else {
        let slowdown = skinny_time.as_secs_f64() / readdirplus_time.as_secs_f64();
        println!("Skinny is {:.2}x slower", slowdown);
    }

    // Verify counts match
    if readdirplus_count != total_count as usize {
        eprintln!(
            "\nWARNING: Entry count mismatch! READDIRPLUS={}, Skinny={}",
            readdirplus_count, total_count
        );
    } else {
        println!("\nEntry counts match: {}", readdirplus_count);
    }
}
