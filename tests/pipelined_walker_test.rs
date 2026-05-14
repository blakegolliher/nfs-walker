//! Pipelined READDIRPLUS integration test.
//!
//! These tests walk a real NFS export with both `--pipeline-depth 0`
//! (legacy serial worker) and `--pipeline-depth 8` (pipelined worker),
//! then compare the resulting databases entry-for-entry.
//!
//! They are `#[ignore]`'d by default because they require a real (or
//! loopback) NFS server. To run:
//!
//! ```bash
//! NFS_TEST_URL=nfs://localhost/export \
//!   cargo test --test pipelined_walker_test -- --ignored
//! ```
//!
//! Optional: `NFS_TEST_SUBPATH` to scope the walk to a subdirectory of
//! the export (defaults to whatever subpath is in the URL).
//!
//! See `docs/PIPELINED_READDIRPLUS_DESIGN.md` §8 for the full test plan.

use nfs_walker::config::{NfsUrl, OutputFormat, ParquetCompression, WalkConfig};
use nfs_walker::rocksdb::RocksHandle;
use nfs_walker::walker::SimpleWalker;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::tempdir;

fn test_nfs_url() -> Option<NfsUrl> {
    let raw = std::env::var("NFS_TEST_URL").ok()?;
    NfsUrl::parse(&raw).ok()
}

fn make_config(url: NfsUrl, output_path: PathBuf, pipeline_depth: usize) -> WalkConfig {
    WalkConfig {
        nfs_url: url,
        output_path,
        baseline_path: None,
        worker_count: 4,
        queue_size: 1000,
        batch_size: 1000,
        max_depth: None,
        show_progress: false,
        verbose: false,
        dirs_only: false,
        skip_atime: false,
        exclude_patterns: vec![],
        timeout_secs: 30,
        retry_count: 1,
        compute_checksum: false,
        detect_file_type: false,
        max_checksum_size: 1_073_741_824,
        pipeline_depth,
        writer_shards: 1,
        big_dir_split_after: 0,
        output_format: OutputFormat::Rocksdb,
        parquet_compression: ParquetCompression::Snappy,
        parquet_row_group_size: 256_000,
        parquet_file_size_bytes: 512 * 1024 * 1024,
        log: None,
    }
}

/// Pull every (path, entry_type) from the RocksDB and return a stable,
/// comparable representation. We use BTreeSet so order differences
/// between the two walkers don't fail equality.
fn paths_in_db(db: &PathBuf) -> BTreeSet<(String, i64)> {
    let handle = RocksHandle::open_readonly(db).expect("open rocksdb readonly");
    handle
        .iter_by_path()
        .map(|res| {
            let entry = res.expect("iterate entry");
            (entry.path, entry.entry_type as i64)
        })
        .collect()
}

#[test]
#[ignore = "requires NFS_TEST_URL=nfs://host/export"]
fn pipelined_and_legacy_produce_equal_path_sets() {
    let url = match test_nfs_url() {
        Some(u) => u,
        None => {
            eprintln!("skip: NFS_TEST_URL not set");
            return;
        }
    };

    let workdir = tempdir().expect("tempdir");
    let baseline_db = workdir.path().join("baseline.rocks");
    let pipelined_db = workdir.path().join("pipelined.rocks");

    // Run baseline (legacy serial worker).
    let baseline_cfg = make_config(url.clone(), baseline_db.clone(), 0);
    let baseline_stats = SimpleWalker::new(baseline_cfg)
        .run()
        .expect("baseline run");
    eprintln!(
        "baseline: dirs={} files={} bytes={} dur={:?}",
        baseline_stats.dirs,
        baseline_stats.files,
        baseline_stats.bytes,
        baseline_stats.duration
    );

    // Run pipelined.
    let pipelined_cfg = make_config(url, pipelined_db.clone(), 8);
    let pipelined_stats = SimpleWalker::new(pipelined_cfg)
        .run()
        .expect("pipelined run");
    eprintln!(
        "pipelined depth=8: dirs={} files={} bytes={} dur={:?}",
        pipelined_stats.dirs,
        pipelined_stats.files,
        pipelined_stats.bytes,
        pipelined_stats.duration
    );

    assert_eq!(
        baseline_stats.dirs, pipelined_stats.dirs,
        "dir count mismatch"
    );
    assert_eq!(
        baseline_stats.files, pipelined_stats.files,
        "file count mismatch"
    );

    let baseline_paths = paths_in_db(&baseline_db);
    let pipelined_paths = paths_in_db(&pipelined_db);

    let only_baseline: Vec<_> = baseline_paths
        .difference(&pipelined_paths)
        .take(20)
        .collect();
    let only_pipelined: Vec<_> = pipelined_paths
        .difference(&baseline_paths)
        .take(20)
        .collect();

    assert!(
        only_baseline.is_empty() && only_pipelined.is_empty(),
        "path sets differ.\n  only in baseline: {:?}\n  only in pipelined: {:?}",
        only_baseline,
        only_pipelined
    );

    let baseline_secs = baseline_stats.duration.as_secs_f64().max(0.001);
    let pipelined_secs = pipelined_stats.duration.as_secs_f64().max(0.001);
    let ratio = baseline_secs / pipelined_secs;
    eprintln!("speedup (baseline / pipelined) = {:.2}x", ratio);
    assert!(
        pipelined_secs < baseline_secs * 1.5 + 5.0,
        "pipelined ran significantly slower than baseline ({:?} vs {:?})",
        Duration::from_secs_f64(pipelined_secs),
        Duration::from_secs_f64(baseline_secs)
    );
}
