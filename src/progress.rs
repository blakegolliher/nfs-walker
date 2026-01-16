//! Progress reporting for the filesystem walker
//!
//! Provides real-time progress display using indicatif progress bars.

use crate::walker::WalkProgress;
use console::style;
use humansize::{format_size, BINARY};
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Progress reporter that displays walk status
pub struct ProgressReporter {
    /// Progress bar
    bar: ProgressBar,

    /// Stop signal
    stop: Arc<AtomicBool>,
}

impl ProgressReporter {
    /// Create a new progress reporter
    pub fn new() -> Self {
        let bar = ProgressBar::new_spinner();

        bar.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {msg}")
                .expect("Invalid progress template")
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );

        bar.enable_steady_tick(Duration::from_millis(100));

        Self {
            bar,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Update the progress display
    pub fn update(&self, progress: &WalkProgress) {
        let bytes_str = format_size(progress.bytes, BINARY);
        let rate = progress.files_per_second();

        let msg = format!(
            "Dirs: {} | Files: {} | Size: {} | Rate: {:.0}/s | Queue: {} | Workers: {}/{}",
            format_number(progress.dirs),
            format_number(progress.files),
            bytes_str,
            rate,
            progress.queue_size,
            progress.active_workers,
            progress.total_workers,
        );

        self.bar.set_message(msg);
    }

    /// Set a status message
    pub fn set_status(&self, status: &str) {
        self.bar.set_message(status.to_string());
    }

    /// Finish the progress display with a final message
    pub fn finish(&self, message: &str) {
        self.stop.store(true, Ordering::SeqCst);
        self.bar.finish_with_message(message.to_string());
    }

    /// Finish and clear the progress display
    pub fn finish_and_clear(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.bar.finish_and_clear();
    }
}

impl Default for ProgressReporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Format a number with thousands separators
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let bytes: Vec<_> = s.bytes().rev().collect();

    let chunks: Vec<String> = bytes
        .chunks(3)
        .map(|chunk| {
            chunk
                .iter()
                .rev()
                .map(|&b| b as char)
                .collect::<String>()
        })
        .collect();

    chunks.into_iter().rev().collect::<Vec<_>>().join(",")
}

/// Print a summary of the walk results
pub fn print_summary(
    dirs: u64,
    files: u64,
    bytes: u64,
    errors: u64,
    duration: Duration,
    db_path: &str,
    db_size: Option<u64>,
) {
    let bytes_str = format_size(bytes, BINARY);
    let duration_secs = duration.as_secs_f64();
    let rate = if duration_secs > 0.0 {
        files as f64 / duration_secs
    } else {
        0.0
    };

    println!();
    println!("{}", style("Walk Complete").green().bold());
    println!("{}", style("─".repeat(50)).dim());
    println!(
        "  {} {}",
        style("Directories:").bold(),
        format_number(dirs)
    );
    println!("  {} {}", style("Files:").bold(), format_number(files));
    println!("  {} {}", style("Total Size:").bold(), bytes_str);
    println!(
        "  {} {:.1}s ({:.0} files/sec)",
        style("Duration:").bold(),
        duration_secs,
        rate
    );
    if errors > 0 {
        println!(
            "  {} {}",
            style("Errors:").yellow().bold(),
            format_number(errors)
        );
    }
    // Show database path with size if available
    if let Some(size) = db_size {
        let db_size_str = format_size(size, BINARY);
        println!("  {} {} ({})", style("Database:").bold(), db_path, db_size_str);
    } else {
        println!("  {} {}", style("Database:").bold(), db_path);
    }
    println!();
}

/// Print a header at the start of the walk
pub fn print_header(url: &str, workers: usize, output: &str) {
    println!();
    println!(
        "{} {}",
        style("nfs-walker").cyan().bold(),
        env!("CARGO_PKG_VERSION")
    );
    println!("{}", style("─".repeat(50)).dim());
    println!("  {} {}", style("Source:").bold(), url);
    println!("  {} {}", style("Workers:").bold(), workers);
    println!("  {} {}", style("Output:").bold(), output);
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1234567), "1,234,567");
        assert_eq!(format_number(1234567890), "1,234,567,890");
    }
}
