//! Per-scan progress logfile.
//!
//! When a scan starts, the walker creates an `Arc<ScanMetrics>` and a
//! background snapshot thread (via [`start_logger`]). Workers and the
//! RocksDB writer report:
//!
//! - per-worker NFS RPC durations (`record_nfs_latency`),
//! - per-shard write-batch durations (`record_write_latency`),
//! - which directory each worker is currently scanning + entries seen so far
//!   (`enter_dir`, `record_entries`, `exit_dir`),
//! - the bounded crossbeam channel(s) used for write-fanout (`write_senders`),
//!
//! and the snapshot thread emits one record per `LogConfig::interval` to the
//! logfile until [`ScanMetrics::shutdown`] is set. Output format is either
//! human-readable text blocks or JSON Lines.
//!
//! Sample reservoirs are per-source (one per worker / shard) so the hot-loop
//! lock is uncontested. The snapshot thread drains them periodically.

use crate::config::LogFormat;
use crate::nfs::types::DbEntry;
use crossbeam_channel::Sender;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing::warn;

/// Cap on per-source latency reservoirs. Older samples are dropped when the
/// reservoir overflows between snapshots — accuracy is statistical, not exact.
const RESERVOIR_CAP: usize = 8192;

/// Top-K hot directories per snapshot in the logfile.
pub const DEFAULT_TOP_HOT_DIRS: usize = 5;

/// Resolved logfile config used by the snapshot thread.
#[derive(Debug, Clone)]
pub struct LogConfig {
    pub path: PathBuf,
    pub format: LogFormat,
    pub interval: Duration,
    pub top_hot_dirs: usize,
}

impl LogConfig {
    pub fn new(path: PathBuf, format: LogFormat, interval: Duration) -> Self {
        Self {
            path,
            format,
            interval,
            top_hot_dirs: DEFAULT_TOP_HOT_DIRS,
        }
    }
}

/// What a worker is currently doing — a snapshot of its in-progress directory.
#[derive(Debug, Clone)]
pub struct HotDir {
    pub path: String,
    pub started_at: Instant,
    pub entries_seen: u64,
}

/// References to the walker's existing scan counters. Cloned into
/// `ScanMetrics` so the snapshot thread reads what the walker writes,
/// without doubling up on writes.
#[derive(Clone, Default)]
pub struct CounterRefs {
    pub dirs: Arc<AtomicU64>,
    pub files: Arc<AtomicU64>,
    pub bytes: Arc<AtomicU64>,
    pub errors: Arc<AtomicU64>,
    pub active_workers: Arc<AtomicUsize>,
}

/// Per-worker / per-shard state shared between the hot loop and the snapshot
/// thread. All fields are designed for uncontested writes from a single
/// producer; the snapshot thread reads them every interval.
pub struct ScanMetrics {
    /// References to the walker's existing scan counters.
    counters: CounterRefs,

    /// One reservoir per worker. Each worker pushes via its own mutex; the
    /// snapshot thread drains all of them.
    nfs_latency_us: Vec<Mutex<Vec<u64>>>,
    /// One reservoir per writer shard.
    write_latency_us: Vec<Mutex<Vec<u64>>>,
    /// Per-worker map of `tag -> HotDir`. Workers hold up to
    /// `--pipeline-depth` in-flight slots concurrently; tracking per-tag
    /// (not per-worker) so the most-loaded slot surfaces in the snapshot
    /// even when a worker is juggling 16 directories at once. Mutex is
    /// per-worker so the hot path stays uncontested.
    worker_slots: Vec<Mutex<HashMap<u64, HotDir>>>,
    /// Cloned senders used by writers — the snapshot thread reads `.len()`
    /// to estimate write-channel pressure.
    pub write_senders: Mutex<Vec<Sender<Vec<DbEntry>>>>,
    /// Path to the RocksDB output dir (so the snapshot thread can sample
    /// on-disk size).
    output_path: Mutex<Option<PathBuf>>,

    /// Set when the scan finishes — signals the snapshot thread to write a
    /// final record and exit.
    shutdown: AtomicBool,

    /// Total worker count (for the active/total fraction in snapshots).
    total_workers: usize,
}

impl ScanMetrics {
    pub fn new(worker_count: usize, shard_count: usize, counters: CounterRefs) -> Arc<Self> {
        let nfs_latency_us = (0..worker_count)
            .map(|_| Mutex::new(Vec::with_capacity(64)))
            .collect();
        let write_latency_us = (0..shard_count.max(1))
            .map(|_| Mutex::new(Vec::with_capacity(64)))
            .collect();
        let worker_slots = (0..worker_count)
            .map(|_| Mutex::new(HashMap::new()))
            .collect();
        Arc::new(Self {
            counters,
            nfs_latency_us,
            write_latency_us,
            worker_slots,
            write_senders: Mutex::new(Vec::new()),
            output_path: Mutex::new(None),
            shutdown: AtomicBool::new(false),
            total_workers: worker_count,
        })
    }

    pub fn set_output_path(&self, path: PathBuf) {
        if let Ok(mut guard) = self.output_path.lock() {
            *guard = Some(path);
        }
    }

    /// Borrow the active-worker atomic so the walker can share it with
    /// every worker thread (the snapshot thread reads the same atomic).
    pub fn active_workers(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.counters.active_workers)
    }

    pub fn register_write_sender(&self, sender: Sender<Vec<DbEntry>>) {
        if let Ok(mut g) = self.write_senders.lock() {
            g.push(sender);
        }
    }

    /// Drop the sender clones registered for queue-depth observation. MUST
    /// be called after the workers finish but before joining the writer
    /// thread(s). Cloned senders held here keep `entry_rx.recv()` blocking
    /// even after all worker senders have dropped, hanging the writer.
    pub fn release_write_senders(&self) {
        if let Ok(mut g) = self.write_senders.lock() {
            g.clear();
        }
    }

    pub fn record_nfs_latency(&self, worker_id: usize, dur: Duration) {
        if let Some(buf) = self.nfs_latency_us.get(worker_id) {
            if let Ok(mut g) = buf.lock() {
                if g.len() >= RESERVOIR_CAP {
                    // drop-oldest ring without an actual VecDeque to keep
                    // sampling cheap; statistical loss is acceptable.
                    g.swap_remove(0);
                }
                g.push(dur.as_micros() as u64);
            }
        }
    }

    pub fn record_write_latency(&self, shard_id: usize, dur: Duration) {
        if let Some(buf) = self.write_latency_us.get(shard_id) {
            if let Ok(mut g) = buf.lock() {
                if g.len() >= RESERVOIR_CAP {
                    g.swap_remove(0);
                }
                g.push(dur.as_micros() as u64);
            }
        }
    }

    /// Begin tracking a directory submission. `tag` is the per-worker
    /// slot identifier (the same `tag` value passed to
    /// `submit_readdirplus_by_fh`). The legacy worker, which only ever
    /// holds one in-flight directory, can pass `0` since it never has
    /// concurrent slots to disambiguate.
    pub fn enter_dir(&self, worker_id: usize, tag: u64, path: String) {
        if let Some(slot) = self.worker_slots.get(worker_id) {
            if let Ok(mut g) = slot.lock() {
                g.insert(
                    tag,
                    HotDir {
                        path,
                        started_at: Instant::now(),
                        entries_seen: 0,
                    },
                );
            }
        }
    }

    pub fn record_entries(&self, worker_id: usize, tag: u64, n: u64) {
        if let Some(slot) = self.worker_slots.get(worker_id) {
            if let Ok(mut g) = slot.lock() {
                if let Some(hd) = g.get_mut(&tag) {
                    hd.entries_seen += n;
                }
            }
        }
    }

    pub fn exit_dir(&self, worker_id: usize, tag: u64) {
        if let Some(slot) = self.worker_slots.get(worker_id) {
            if let Ok(mut g) = slot.lock() {
                g.remove(&tag);
            }
        }
    }

    pub fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    fn drain_reservoir(buf: &Mutex<Vec<u64>>) -> Vec<u64> {
        match buf.lock() {
            Ok(mut g) => std::mem::take(&mut *g),
            Err(_) => Vec::new(),
        }
    }

    fn collect_snapshot(&self, top_n: usize) -> Snapshot {
        let mut nfs_samples: Vec<u64> = Vec::new();
        for buf in &self.nfs_latency_us {
            nfs_samples.extend(Self::drain_reservoir(buf));
        }
        let mut write_samples: Vec<u64> = Vec::new();
        for buf in &self.write_latency_us {
            write_samples.extend(Self::drain_reservoir(buf));
        }

        let mut hot_dirs: Vec<(usize, HotDir)> = Vec::new();
        for (i, slot) in self.worker_slots.iter().enumerate() {
            if let Ok(g) = slot.lock() {
                for hd in g.values() {
                    hot_dirs.push((i, hd.clone()));
                }
            }
        }
        hot_dirs.sort_by(|a, b| b.1.entries_seen.cmp(&a.1.entries_seen));
        hot_dirs.truncate(top_n);

        let queue_depths: Vec<usize> = match self.write_senders.lock() {
            Ok(g) => g.iter().map(|s| s.len()).collect(),
            Err(_) => Vec::new(),
        };

        let output_size = match self.output_path.lock() {
            Ok(g) => g.as_ref().and_then(|p| dir_size(p)),
            Err(_) => None,
        };

        Snapshot {
            nfs_samples,
            write_samples,
            hot_dirs,
            queue_depths,
            output_size,
            dirs: self.counters.dirs.load(Ordering::Relaxed),
            files: self.counters.files.load(Ordering::Relaxed),
            bytes: self.counters.bytes.load(Ordering::Relaxed),
            errors: self.counters.errors.load(Ordering::Relaxed),
            active_workers: self.counters.active_workers.load(Ordering::Relaxed),
            total_workers: self.total_workers,
        }
    }
}

#[derive(Debug)]
struct Snapshot {
    nfs_samples: Vec<u64>,
    write_samples: Vec<u64>,
    hot_dirs: Vec<(usize, HotDir)>,
    queue_depths: Vec<usize>,
    output_size: Option<u64>,
    dirs: u64,
    files: u64,
    bytes: u64,
    errors: u64,
    active_workers: usize,
    total_workers: usize,
}

#[derive(Debug, Default, Clone, Copy)]
struct LatencySummary {
    count: u64,
    avg_us: u64,
    p99_us: u64,
}

fn summarise(samples: &mut [u64]) -> LatencySummary {
    if samples.is_empty() {
        return LatencySummary::default();
    }
    samples.sort_unstable();
    let count = samples.len() as u64;
    let sum: u128 = samples.iter().map(|v| *v as u128).sum();
    let avg_us = (sum / (count as u128)) as u64;
    // p99 via the standard nearest-rank: ceil(0.99 * N) - 1 (clamped).
    let idx = ((samples.len() as f64) * 0.99).ceil() as usize;
    let idx = idx.saturating_sub(1).min(samples.len() - 1);
    let p99_us = samples[idx];
    LatencySummary {
        count,
        avg_us,
        p99_us,
    }
}

fn dir_size(path: &std::path::Path) -> Option<u64> {
    if !path.is_dir() {
        return None;
    }
    let mut total = 0u64;
    let entries = std::fs::read_dir(path).ok()?;
    for entry in entries.flatten() {
        if let Ok(meta) = entry.metadata() {
            total += meta.len();
        }
    }
    Some(total)
}

/// Spawn the snapshot thread. The returned join handle should be awaited
/// after [`ScanMetrics::signal_shutdown`] is called so the final record is
/// flushed before the process exits.
pub fn start_logger(
    metrics: Arc<ScanMetrics>,
    cfg: LogConfig,
    started_at: Instant,
) -> std::io::Result<JoinHandle<()>> {
    let file = File::create(&cfg.path)?;
    let mut writer = BufWriter::new(file);

    // Header — written once at start so log readers can identify the source.
    match cfg.format {
        LogFormat::Text => {
            let _ = writeln!(
                writer,
                "# nfs-walker scan progress — interval={}s — workers={}",
                cfg.interval.as_secs(),
                metrics.total_workers,
            );
            let _ = writeln!(
                writer,
                "# fields: timestamp, elapsed, dirs/files/bytes/rate, active/total, nfs_lat (avg/p99/n), write_lat, queue, output_size, hot_dirs"
            );
        }
        LogFormat::Json => {
            let _ = writeln!(
                writer,
                r#"{{"event":"start","interval_secs":{},"total_workers":{}}}"#,
                cfg.interval.as_secs(),
                metrics.total_workers
            );
        }
    }
    let _ = writer.flush();

    let handle = thread::Builder::new()
        .name("scan-logger".to_string())
        .spawn(move || {
            let mut last = Instant::now();
            loop {
                let remaining = cfg
                    .interval
                    .checked_sub(last.elapsed())
                    .unwrap_or(Duration::ZERO);
                if remaining > Duration::ZERO {
                    thread::sleep(remaining.min(Duration::from_secs(1)));
                }

                if metrics.shutdown.load(Ordering::SeqCst) {
                    let snap = metrics.collect_snapshot(cfg.top_hot_dirs);
                    if let Err(e) = emit(&mut writer, &cfg, &snap, started_at, true) {
                        warn!("scanlog: final write failed: {}", e);
                    }
                    let _ = writer.flush();
                    return;
                }

                if last.elapsed() < cfg.interval {
                    continue;
                }

                let snap = metrics.collect_snapshot(cfg.top_hot_dirs);
                if let Err(e) = emit(&mut writer, &cfg, &snap, started_at, false) {
                    warn!("scanlog: write failed: {}", e);
                }
                let _ = writer.flush();
                last = Instant::now();
            }
        })?;

    Ok(handle)
}

fn emit(
    writer: &mut BufWriter<File>,
    cfg: &LogConfig,
    snap: &Snapshot,
    started_at: Instant,
    is_final: bool,
) -> std::io::Result<()> {
    let mut nfs_samples = snap.nfs_samples.clone();
    let mut write_samples = snap.write_samples.clone();
    let nfs = summarise(&mut nfs_samples);
    let writes = summarise(&mut write_samples);
    let elapsed = started_at.elapsed();

    match cfg.format {
        LogFormat::Text => emit_text(writer, snap, elapsed, nfs, writes, is_final),
        LogFormat::Json => emit_json(writer, snap, elapsed, nfs, writes, is_final),
    }
}

fn emit_text(
    writer: &mut BufWriter<File>,
    snap: &Snapshot,
    elapsed: Duration,
    nfs: LatencySummary,
    writes: LatencySummary,
    is_final: bool,
) -> std::io::Result<()> {
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let entries = snap.dirs + snap.files;
    let secs = elapsed.as_secs_f64().max(0.001);
    let rate = entries as f64 / secs;
    let bytes_h = humansize::format_size(snap.bytes, humansize::BINARY);
    let size_h = snap
        .output_size
        .map(|s| humansize::format_size(s, humansize::BINARY))
        .unwrap_or_else(|| "(unknown)".to_string());
    let queue_str = if snap.queue_depths.is_empty() {
        "0".to_string()
    } else {
        snap.queue_depths
            .iter()
            .map(|d| d.to_string())
            .collect::<Vec<_>>()
            .join("/")
    };
    let tag = if is_final { " (final)" } else { "" };

    writeln!(
        writer,
        "[{now}] elapsed={:>3}m{:02}s{tag}  active={}/{} dirs={} files={} size={} rate={:.0}/s",
        elapsed.as_secs() / 60,
        elapsed.as_secs() % 60,
        snap.active_workers,
        snap.total_workers,
        snap.dirs,
        snap.files,
        bytes_h,
        rate,
    )?;
    writeln!(
        writer,
        "    nfs_readdirplus  avg={:>5}us  p99={:>5}us  n={}",
        nfs.avg_us, nfs.p99_us, nfs.count
    )?;
    writeln!(
        writer,
        "    write_batch      avg={:>5}us  p99={:>5}us  n={}  queue={}",
        writes.avg_us, writes.p99_us, writes.count, queue_str,
    )?;
    writeln!(writer, "    rocksdb          size={size_h}  errors={}", snap.errors)?;
    if !snap.hot_dirs.is_empty() {
        writeln!(writer, "    hot dirs:")?;
        for (worker_id, hd) in &snap.hot_dirs {
            let age = hd.started_at.elapsed().as_secs();
            writeln!(
                writer,
                "      [W#{worker_id:>3}] {age:>4}s  {entries:>10} entries  {path}",
                age = age,
                entries = hd.entries_seen,
                path = hd.path,
            )?;
        }
    }
    writeln!(writer)?;
    Ok(())
}

fn emit_json(
    writer: &mut BufWriter<File>,
    snap: &Snapshot,
    elapsed: Duration,
    nfs: LatencySummary,
    writes: LatencySummary,
    is_final: bool,
) -> std::io::Result<()> {
    let now = chrono::Local::now().to_rfc3339();
    let entries = snap.dirs + snap.files;
    let secs = elapsed.as_secs_f64().max(0.001);
    let rate = entries as f64 / secs;
    let mut hot_json = String::new();
    hot_json.push('[');
    for (i, (worker_id, hd)) in snap.hot_dirs.iter().enumerate() {
        if i > 0 {
            hot_json.push(',');
        }
        hot_json.push_str(&format!(
            r#"{{"worker":{},"path":{:?},"age_secs":{},"entries":{}}}"#,
            worker_id,
            hd.path,
            hd.started_at.elapsed().as_secs(),
            hd.entries_seen,
        ));
    }
    hot_json.push(']');

    let queue_json = format!(
        "[{}]",
        snap.queue_depths
            .iter()
            .map(|d| d.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    writeln!(
        writer,
        r#"{{"ts":"{now}","is_final":{is_final},"elapsed_secs":{elapsed},"active":{active},"total":{total},"dirs":{dirs},"files":{files},"bytes":{bytes},"errors":{errors},"rate_per_sec":{rate:.0},"nfs_lat":{{"count":{nfs_n},"avg_us":{nfs_avg},"p99_us":{nfs_p99}}},"write_lat":{{"count":{w_n},"avg_us":{w_avg},"p99_us":{w_p99}}},"queue":{queue},"output_size":{size},"hot_dirs":{hot}}}"#,
        elapsed = elapsed.as_secs(),
        active = snap.active_workers,
        total = snap.total_workers,
        dirs = snap.dirs,
        files = snap.files,
        bytes = snap.bytes,
        errors = snap.errors,
        nfs_n = nfs.count,
        nfs_avg = nfs.avg_us,
        nfs_p99 = nfs.p99_us,
        w_n = writes.count,
        w_avg = writes.avg_us,
        w_p99 = writes.p99_us,
        queue = queue_json,
        size = snap
            .output_size
            .map(|s| s.to_string())
            .unwrap_or_else(|| "null".to_string()),
        hot = hot_json,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summarise_basic() {
        let mut samples = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        let s = summarise(&mut samples);
        assert_eq!(s.count, 10);
        assert_eq!(s.avg_us, 55);
        // p99 of 10 samples: ceil(0.99*10)=10, idx=9 → 100us.
        assert_eq!(s.p99_us, 100);
    }

    #[test]
    fn summarise_empty() {
        let mut samples: Vec<u64> = vec![];
        let s = summarise(&mut samples);
        assert_eq!(s.count, 0);
        assert_eq!(s.avg_us, 0);
        assert_eq!(s.p99_us, 0);
    }

    #[test]
    fn metrics_record_and_drain() {
        let m = ScanMetrics::new(2, 1, CounterRefs::default());
        m.record_nfs_latency(0, Duration::from_micros(100));
        m.record_nfs_latency(0, Duration::from_micros(200));
        m.record_nfs_latency(1, Duration::from_micros(300));
        m.record_write_latency(0, Duration::from_micros(400));

        let snap = m.collect_snapshot(5);
        assert_eq!(snap.nfs_samples.len(), 3);
        assert_eq!(snap.write_samples.len(), 1);

        // After draining, the next snapshot must be empty.
        let snap2 = m.collect_snapshot(5);
        assert_eq!(snap2.nfs_samples.len(), 0);
    }

    #[test]
    fn hot_dir_top_n_orders_by_entries_seen() {
        let m = ScanMetrics::new(3, 1, CounterRefs::default());
        m.enter_dir(0, 0, "/small".into());
        m.record_entries(0, 0, 100);
        m.enter_dir(1, 0, "/big".into());
        m.record_entries(1, 0, 1_000_000);
        m.enter_dir(2, 0, "/medium".into());
        m.record_entries(2, 0, 5_000);

        let snap = m.collect_snapshot(2);
        assert_eq!(snap.hot_dirs.len(), 2);
        assert_eq!(snap.hot_dirs[0].1.path, "/big");
        assert_eq!(snap.hot_dirs[1].1.path, "/medium");
    }

    #[test]
    fn pipelined_worker_with_concurrent_slots_tracked_per_tag() {
        // One worker holding 3 in-flight slots concurrently. Each
        // slot should be tracked independently; clearing one must not
        // wipe out the others (this was the regression fixed by going
        // from Vec<Mutex<Option<HotDir>>> to per-tag HashMap).
        let m = ScanMetrics::new(1, 1, CounterRefs::default());
        m.enter_dir(0, 100, "/dir-a".into());
        m.enter_dir(0, 101, "/dir-b".into());
        m.enter_dir(0, 102, "/dir-c".into());
        m.record_entries(0, 100, 50_000);
        m.record_entries(0, 101, 1_000);
        m.record_entries(0, 102, 200);

        // Clear the smallest -- the other two must still be visible.
        m.exit_dir(0, 102);

        let snap = m.collect_snapshot(5);
        assert_eq!(snap.hot_dirs.len(), 2);
        assert_eq!(snap.hot_dirs[0].1.path, "/dir-a");
        assert_eq!(snap.hot_dirs[0].1.entries_seen, 50_000);
        assert_eq!(snap.hot_dirs[1].1.path, "/dir-b");
        assert_eq!(snap.hot_dirs[1].1.entries_seen, 1_000);
    }

    #[test]
    fn text_snapshot_renders_without_panicking() {
        let counters = CounterRefs::default();
        counters.dirs.store(10, Ordering::Relaxed);
        counters.files.store(100, Ordering::Relaxed);
        counters.bytes.store(1024 * 1024, Ordering::Relaxed);
        let m = ScanMetrics::new(2, 1, counters);
        m.record_nfs_latency(0, Duration::from_micros(1500));
        m.record_write_latency(0, Duration::from_micros(800));
        m.enter_dir(0, 0, "/data/large".into());
        m.record_entries(0, 0, 12345);

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("scan.log");
        let cfg = LogConfig::new(path.clone(), LogFormat::Text, Duration::from_millis(50));
        let started = Instant::now();
        let handle = start_logger(m.clone(), cfg, started).unwrap();

        std::thread::sleep(Duration::from_millis(120));
        m.signal_shutdown();
        handle.join().unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("nfs-walker scan progress"));
        assert!(body.contains("hot dirs"));
        assert!(body.contains("/data/large"));
    }

    #[test]
    fn json_snapshot_is_one_object_per_line() {
        let counters = CounterRefs::default();
        counters.files.store(7, Ordering::Relaxed);
        let m = ScanMetrics::new(1, 1, counters);

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("scan.log");
        let cfg = LogConfig::new(path.clone(), LogFormat::Json, Duration::from_millis(50));
        let started = Instant::now();
        let handle = start_logger(m.clone(), cfg, started).unwrap();

        std::thread::sleep(Duration::from_millis(120));
        m.signal_shutdown();
        handle.join().unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        for line in body.lines() {
            assert!(line.starts_with('{'));
            assert!(line.ends_with('}'));
        }
    }
}
