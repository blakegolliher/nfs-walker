//! Bounded retry for directory reads, and the per-scan failure log.
//!
//! A directory whose READDIRPLUS failed used to bump a counter and
//! vanish from the index while the scan still reported success. Now
//! every failure is classified ([`crate::error::FailureKind`]):
//!
//! - transient ones — timeouts, connection hiccups, "try again" from
//!   the server, stale or bad handles — are retried a bounded number
//!   of times with backoff (a stale handle is re-resolved by path
//!   first);
//! - a directory that no longer exists (deleted or renamed between its
//!   parent's listing and its own read, confirmed by a path LOOKUP) is
//!   recorded as **vanished**: on a live source that is a race, not a
//!   hole in the index, and its entry row was already emitted by the
//!   parent;
//! - permission denials and other non-transient protocol errors fail
//!   at once;
//! - whatever is left after the policy is exhausted is a structured
//!   [`DirFailure`] that fails the scan as a whole
//!   ([`crate::error::WalkerError::ScanIncomplete`]).
//!
//! A retry only happens while nothing from the directory has reached
//! the writers: a directory that fails after its first page was
//! emitted cannot be re-read without duplicating rows, so it is a
//! failure outright.

use crate::error::{DirFailure, FailureKind, NfsError};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

/// How many failures the in-memory sample keeps for the final error;
/// the log file has every one.
pub const SAMPLE_CAP: usize = 1000;

/// File name of the failure log inside the scan output directory.
pub const FAILURE_LOG_NAME: &str = "errors.jsonl";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts per directory, including the first.
    pub max_attempts: u32,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
}

impl RetryPolicy {
    /// `--retries N` means N retries after the first attempt.
    pub fn from_retries(retries: u32) -> Self {
        Self {
            max_attempts: retries.saturating_add(1).max(1),
            base_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(5),
        }
    }

    /// Exponential: 200 ms, 400 ms, 800 ms, ... capped at `max_backoff`.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(16);
        let d = self.base_backoff.saturating_mul(1u32 << shift);
        d.min(self.max_backoff)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::from_retries(3)
    }
}

/// What to do after one failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Try again after `after`; `refresh_fh` re-resolves the path to a
    /// fresh filehandle first.
    Retry {
        after: Duration,
        refresh_fh: bool,
    },
    /// The server says the directory is not there: confirm by path
    /// LOOKUP before calling it vanished.
    CheckGone,
    Fail(FailureKind),
}

/// The retry decision for `err` on attempt number `attempt` (1-based).
pub fn plan(err: &NfsError, attempt: u32, policy: &RetryPolicy) -> Action {
    let kind = err.failure_kind();
    match kind {
        FailureKind::PermissionDenied => Action::Fail(kind),
        FailureKind::NotFound => Action::CheckGone,
        _ if attempt >= policy.max_attempts => Action::Fail(kind),
        FailureKind::StaleHandle => Action::Retry {
            after: Duration::ZERO,
            refresh_fh: true,
        },
        FailureKind::Timeout | FailureKind::Connection | FailureKind::Transient => Action::Retry {
            after: policy.backoff(attempt),
            refresh_fh: false,
        },
        FailureKind::ConnectionLost | FailureKind::Protocol | FailureKind::Other => {
            Action::Fail(kind)
        }
    }
}

/// Why a directory read did not succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirError {
    /// Confirmed gone between its parent's listing and its own read.
    Vanished {
        attempts: u32,
    },
    Failed(DirFailure),
}

fn failure(path: &str, err: &NfsError, attempts: u32) -> DirError {
    DirError::Failed(DirFailure {
        path: path.to_string(),
        kind: err.failure_kind(),
        error: err.to_string(),
        attempts,
    })
}

/// Drive `attempt` under `policy`.
///
/// - `attempt(fh)` reads the directory, by cached filehandle when one
///   is given and by path otherwise.
/// - `lookup()` resolves the path to a fresh filehandle; `NotFound`
///   from it is the proof that the directory vanished.
/// - `emitted()` reports whether any of this directory's entries have
///   already been handed to the writers; if so, no retry.
/// - `sleep(d)` waits out a backoff (injected so tests never sleep).
pub fn read_dir_with_retry<T>(
    path: &str,
    mut fh: Option<Vec<u8>>,
    policy: &RetryPolicy,
    mut attempt: impl FnMut(Option<&[u8]>) -> Result<T, NfsError>,
    mut lookup: impl FnMut() -> Result<Vec<u8>, NfsError>,
    emitted: impl Fn() -> bool,
    mut sleep: impl FnMut(Duration),
) -> Result<T, DirError> {
    let mut n = 0u32;
    loop {
        n += 1;
        let err = match attempt(fh.as_deref()) {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        if emitted() {
            // Rows from this directory are already on their way to the
            // writers; re-reading would duplicate them.
            return Err(failure(path, &err, n));
        }
        match plan(&err, n, policy) {
            Action::Fail(_) => return Err(failure(path, &err, n)),
            Action::CheckGone => match lookup() {
                Err(e) if e.failure_kind() == FailureKind::NotFound => {
                    return Err(DirError::Vanished { attempts: n });
                }
                Ok(fresh) => {
                    // It is there after all: the read hit a race or a
                    // bad cached handle. Read it through the fresh one.
                    if n >= policy.max_attempts {
                        return Err(failure(path, &err, n));
                    }
                    fh = Some(fresh);
                }
                Err(_) => {
                    // Could not even LOOKUP: treat like a transient read
                    // failure of the original error.
                    if n >= policy.max_attempts {
                        return Err(failure(path, &err, n));
                    }
                    sleep(policy.backoff(n));
                }
            },
            Action::Retry { after, refresh_fh } => {
                if refresh_fh {
                    match lookup() {
                        Ok(fresh) => fh = Some(fresh),
                        Err(e) if e.failure_kind() == FailureKind::NotFound => {
                            return Err(DirError::Vanished { attempts: n });
                        }
                        Err(_) => {}
                    }
                }
                if !after.is_zero() {
                    sleep(after);
                }
            }
        }
    }
}

/// Every directory failure and every vanished directory of one scan,
/// as JSON lines, plus a bounded in-memory sample for the final error.
pub struct FailureLog {
    path: Option<PathBuf>,
    file: Mutex<Option<BufWriter<File>>>,
    samples: Mutex<Vec<DirFailure>>,
    sample_cap: usize,
}

impl FailureLog {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            path: Some(path.to_path_buf()),
            file: Mutex::new(Some(BufWriter::new(file))),
            samples: Mutex::new(Vec::new()),
            sample_cap: SAMPLE_CAP,
        })
    }

    /// Samples only, no file (tests, or a scan with no output dir).
    pub fn in_memory() -> Self {
        Self {
            path: None,
            file: Mutex::new(None),
            samples: Mutex::new(Vec::new()),
            sample_cap: SAMPLE_CAP,
        }
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// A directory that could not be read after the policy was
    /// exhausted. Counted by the caller; sampled and logged here.
    pub fn record(&self, f: &DirFailure) {
        {
            let mut samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
            if samples.len() < self.sample_cap {
                samples.push(f.clone());
            }
        }
        self.write_line(&serde_json::json!({
            "ts": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "path": f.path,
            "kind": f.kind.as_str(),
            "error": f.error,
            "attempts": f.attempts,
        }));
    }

    /// A directory that disappeared during the scan. Logged for the
    /// record; not a failure.
    pub fn record_vanished(&self, path: &str, attempts: u32) {
        self.write_line(&serde_json::json!({
            "ts": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "path": path,
            "kind": "vanished",
            "error": "directory disappeared between its parent's listing and its own read",
            "attempts": attempts,
        }));
    }

    fn write_line(&self, v: &serde_json::Value) {
        let mut guard = self.file.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(w) = guard.as_mut() {
            let _ = serde_json::to_writer(&mut *w, v);
            let _ = w.write_all(b"\n");
        }
    }

    pub fn samples(&self) -> Vec<DirFailure> {
        self.samples
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Flush and fsync the log so it survives the process.
    pub fn flush(&self) {
        let mut guard = self.file.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(w) = guard.as_mut() {
            let _ = w.flush();
            let _ = w.get_ref().sync_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    fn policy() -> RetryPolicy {
        RetryPolicy::from_retries(2) // 3 attempts
    }

    fn denied() -> NfsError {
        NfsError::PermissionDenied { path: "/d".into() }
    }
    fn not_found() -> NfsError {
        NfsError::NotFound { path: "/d".into() }
    }
    fn stale() -> NfsError {
        NfsError::StaleHandle { path: "/d".into() }
    }
    fn timeout() -> NfsError {
        NfsError::ReadDirFailed {
            path: "/d".into(),
            reason: "READDIRPLUS failed: RPC timeout".into(),
        }
    }
    fn poisoned() -> NfsError {
        NfsError::ReadDirFailed {
            path: "/d".into(),
            reason: "READDIRPLUS failed: RPC timeout (connection poisoned)".into(),
        }
    }
    fn jukebox() -> NfsError {
        NfsError::ReadDirFailed {
            path: "/d".into(),
            reason: "NFS3ERR_JUKEBOX (jukebox/try again later)".into(),
        }
    }
    fn notdir() -> NfsError {
        NfsError::ReadDirFailed {
            path: "/d".into(),
            reason: "NFS3ERR_NOTDIR (not a directory)".into(),
        }
    }

    /// Run the retry loop over a scripted sequence of attempt results
    /// (`Some(err)` fails, `None` succeeds) and lookup results.
    struct Script {
        attempts: RefCell<Vec<Option<NfsError>>>,
        lookups: RefCell<Vec<Result<Vec<u8>, NfsError>>>,
        slept: RefCell<Vec<Duration>>,
        seen_fh: RefCell<Vec<Option<Vec<u8>>>>,
        emitted: Cell<bool>,
    }

    impl Script {
        fn new(attempts: Vec<Option<NfsError>>, lookups: Vec<Result<Vec<u8>, NfsError>>) -> Self {
            Self {
                attempts: RefCell::new(attempts),
                lookups: RefCell::new(lookups),
                slept: RefCell::new(Vec::new()),
                seen_fh: RefCell::new(Vec::new()),
                emitted: Cell::new(false),
            }
        }

        fn run(&self, fh: Option<Vec<u8>>) -> Result<u32, DirError> {
            read_dir_with_retry(
                "/d",
                fh,
                &policy(),
                |fh| {
                    self.seen_fh.borrow_mut().push(fh.map(|f| f.to_vec()));
                    let mut a = self.attempts.borrow_mut();
                    assert!(!a.is_empty(), "more attempts than scripted");
                    match a.remove(0) {
                        None => Ok(7),
                        Some(e) => Err(e),
                    }
                },
                || {
                    let mut l = self.lookups.borrow_mut();
                    assert!(!l.is_empty(), "more lookups than scripted");
                    l.remove(0)
                },
                || self.emitted.get(),
                |d| self.slept.borrow_mut().push(d),
            )
        }
    }

    #[test]
    fn plan_matrix() {
        let p = policy();
        assert_eq!(
            plan(&denied(), 1, &p),
            Action::Fail(FailureKind::PermissionDenied)
        );
        assert_eq!(plan(&not_found(), 1, &p), Action::CheckGone);
        assert_eq!(
            plan(&not_found(), 99, &p),
            Action::CheckGone,
            "always verified"
        );
        assert_eq!(
            plan(&stale(), 1, &p),
            Action::Retry {
                after: Duration::ZERO,
                refresh_fh: true
            }
        );
        assert_eq!(
            plan(&timeout(), 1, &p),
            Action::Retry {
                after: Duration::from_millis(200),
                refresh_fh: false
            }
        );
        assert_eq!(
            plan(&jukebox(), 2, &p),
            Action::Retry {
                after: Duration::from_millis(400),
                refresh_fh: false
            }
        );
        assert_eq!(
            plan(&timeout(), 3, &p),
            Action::Fail(FailureKind::Timeout),
            "exhausted"
        );
        assert_eq!(plan(&notdir(), 1, &p), Action::Fail(FailureKind::Protocol));
        assert_eq!(
            plan(&poisoned(), 1, &p),
            Action::Fail(FailureKind::ConnectionLost),
            "nothing on a poisoned connection can succeed"
        );
    }

    #[test]
    fn backoff_grows_and_caps() {
        let p = RetryPolicy::from_retries(10);
        assert_eq!(p.backoff(1), Duration::from_millis(200));
        assert_eq!(p.backoff(2), Duration::from_millis(400));
        assert_eq!(p.backoff(4), Duration::from_millis(1600));
        assert_eq!(p.backoff(8), Duration::from_secs(5), "capped");
        assert_eq!(p.backoff(40), Duration::from_secs(5), "no overflow");
        assert_eq!(RetryPolicy::from_retries(0).max_attempts, 1);
    }

    #[test]
    fn success_first_time_needs_no_lookup() {
        let s = Script::new(vec![None], vec![]);
        assert_eq!(s.run(Some(b"FH".to_vec())).unwrap(), 7);
        assert_eq!(s.seen_fh.borrow().as_slice(), &[Some(b"FH".to_vec())]);
    }

    #[test]
    fn transient_readdir_failures_retry_with_backoff_then_succeed() {
        let s = Script::new(vec![Some(timeout()), Some(jukebox()), None], vec![]);
        assert_eq!(s.run(Some(b"FH".to_vec())).unwrap(), 7);
        assert_eq!(
            s.slept.borrow().as_slice(),
            &[Duration::from_millis(200), Duration::from_millis(400)]
        );
        assert_eq!(s.seen_fh.borrow().len(), 3, "same handle each time");
    }

    #[test]
    fn exhausted_transient_failures_become_a_failure_with_attempt_count() {
        let s = Script::new(
            vec![Some(timeout()), Some(timeout()), Some(timeout())],
            vec![],
        );
        match s.run(None).unwrap_err() {
            DirError::Failed(f) => {
                assert_eq!(f.kind, FailureKind::Timeout);
                assert_eq!(f.attempts, 3);
                assert_eq!(f.path, "/d");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(s.slept.borrow().len(), 2, "no sleep after the last attempt");
    }

    #[test]
    fn permission_denied_fails_at_once() {
        let s = Script::new(vec![Some(denied())], vec![]);
        match s.run(None).unwrap_err() {
            DirError::Failed(f) => {
                assert_eq!(f.kind, FailureKind::PermissionDenied);
                assert_eq!(f.attempts, 1);
                assert!(f.error.contains("Permission denied"), "{}", f.error);
            }
            other => panic!("{other:?}"),
        }
        assert!(s.slept.borrow().is_empty());
    }

    #[test]
    fn not_found_confirmed_by_lookup_is_vanished() {
        let s = Script::new(vec![Some(not_found())], vec![Err(not_found())]);
        assert_eq!(
            s.run(Some(b"OLD".to_vec())).unwrap_err(),
            DirError::Vanished { attempts: 1 }
        );
    }

    #[test]
    fn not_found_but_lookup_succeeds_rereads_through_the_fresh_handle() {
        let s = Script::new(vec![Some(not_found()), None], vec![Ok(b"NEW".to_vec())]);
        assert_eq!(s.run(Some(b"OLD".to_vec())).unwrap(), 7);
        assert_eq!(
            s.seen_fh.borrow().as_slice(),
            &[Some(b"OLD".to_vec()), Some(b"NEW".to_vec())]
        );
        assert!(s.slept.borrow().is_empty(), "immediate");
    }

    #[test]
    fn stale_handle_is_reresolved_before_the_retry() {
        let s = Script::new(vec![Some(stale()), None], vec![Ok(b"NEW".to_vec())]);
        assert_eq!(s.run(Some(b"OLD".to_vec())).unwrap(), 7);
        assert_eq!(s.seen_fh.borrow()[1], Some(b"NEW".to_vec()));
        // Stale, and the path is gone too: vanished, not failed.
        let s = Script::new(vec![Some(stale())], vec![Err(not_found())]);
        assert_eq!(
            s.run(Some(b"OLD".to_vec())).unwrap_err(),
            DirError::Vanished { attempts: 1 }
        );
    }

    #[test]
    fn a_failure_after_rows_were_emitted_is_never_retried() {
        let s = Script::new(vec![Some(timeout())], vec![]);
        s.emitted.set(true);
        match s.run(Some(b"FH".to_vec())).unwrap_err() {
            DirError::Failed(f) => {
                assert_eq!(f.kind, FailureKind::Timeout);
                assert_eq!(f.attempts, 1);
            }
            other => panic!("{other:?}"),
        }
        assert!(s.slept.borrow().is_empty());
    }

    #[test]
    fn failure_log_writes_json_lines_and_bounds_samples() {
        let dir = std::env::temp_dir().join(format!("nfs-walker-retry-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(FAILURE_LOG_NAME);
        let log = FailureLog::open(&path).unwrap();
        for i in 0..(SAMPLE_CAP + 3) {
            log.record(&DirFailure {
                path: format!("/d/{i}"),
                kind: FailureKind::PermissionDenied,
                error: "Permission denied".into(),
                attempts: 1,
            });
        }
        log.record_vanished("/gone", 2);
        log.flush();
        assert_eq!(log.samples().len(), SAMPLE_CAP, "bounded");
        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<serde_json::Value> = body
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), SAMPLE_CAP + 4, "every record is in the file");
        assert_eq!(lines[0]["kind"], "permission_denied");
        assert_eq!(lines[0]["path"], "/d/0");
        assert_eq!(lines.last().unwrap()["kind"], "vanished");
        assert_eq!(lines.last().unwrap()["attempts"], 2);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(FailureLog::in_memory().path().is_none());
    }
}
