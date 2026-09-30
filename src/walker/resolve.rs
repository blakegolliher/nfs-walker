//! Attributes for READDIRPLUS entries the server returned without them.
//!
//! NFSv3 makes both the attributes (`post_op_attr`) and the file handle
//! (`post_op_fh3`) of a READDIRPLUS entry optional. The Linux server,
//! for one, omits both for every entry that is a mountpoint. Such an
//! entry has no type, so the walker could neither classify it nor know
//! whether to descend into it: a directory listed that way used to lose
//! its whole subtree, and its row claimed a type nobody had established.
//!
//! Every entry is therefore resolved before it is classified:
//!
//! 1. READDIRPLUS attributes are used when present;
//! 2. with a file handle but no attributes, GETATTR on the handle;
//! 3. with neither, LOOKUP of the raw name in the parent directory,
//!    then GETATTR on the returned handle if the LOOKUP reply carried
//!    no attributes either;
//! 4. errors [`retry::plan`] classifies as transient are retried under
//!    the scan's [`RetryPolicy`], exactly like a directory read;
//! 5. an entry confirmed gone by a path LOOKUP is **vanished**: a race
//!    on a live tree, recorded, no row, not a failure (the rule for a
//!    directory that disappears before it is read);
//! 6. anything still unresolved is a recorded failure that makes the
//!    scan incomplete. No row is emitted for it and nothing descends
//!    into it.
//!
//! A resolved entry takes all of its attributes, its inode included,
//! from the reply that resolved it (see [`NfsDirEntry::apply_attrs`]).

use crate::error::{DirFailure, FailureKind, NfsError, NfsResult};
use crate::nfs::connection::InflightReaddir;
use crate::nfs::types::{display_path, EntryAttrs, EntryType, LookupReply, NfsDirEntry};
use crate::nfs::NfsConnection;
use crate::walker::retry::{self, Action, FailureLog, RetryPolicy};
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// The NFS operations the worker loops use. [`NfsConnection`] is the
/// only production implementation; the trait exists so the loops can
/// run against a scripted tree in tests, without a server.
pub(crate) trait NfsOps {
    fn is_connected(&self) -> bool;
    fn resolve_path_to_fh(&self, path: &[u8]) -> NfsResult<Vec<u8>>;
    fn readdir_plus_by_fh(
        &self,
        file_handle: &[u8],
        chunk_size: usize,
        callback: &mut dyn FnMut(Vec<NfsDirEntry>) -> bool,
    ) -> NfsResult<usize>;
    fn readdir_plus_with_fh(
        &self,
        path: &[u8],
        chunk_size: usize,
        callback: &mut dyn FnMut(Vec<NfsDirEntry>) -> bool,
    ) -> NfsResult<usize>;
    fn submit_readdirplus_by_fh(
        &self,
        file_handle: &[u8],
        cookie: u64,
        cookieverf: [i8; 8],
        tag: u64,
    ) -> NfsResult<InflightReaddir>;
    fn pump(
        &self,
        slots: &[InflightReaddir],
        min_completions: usize,
        timeout_ms: i32,
    ) -> NfsResult<usize>;
    fn getattr_by_fh(&self, file_handle: &[u8], path: &[u8]) -> NfsResult<EntryAttrs>;
    fn lookup_name(&self, dir_fh: &[u8], name: &[u8], path: &[u8]) -> NfsResult<LookupReply>;
}

impl NfsOps for NfsConnection {
    fn is_connected(&self) -> bool {
        NfsConnection::is_connected(self)
    }
    fn resolve_path_to_fh(&self, path: &[u8]) -> NfsResult<Vec<u8>> {
        NfsConnection::resolve_path_to_fh(self, path)
    }
    fn readdir_plus_by_fh(
        &self,
        file_handle: &[u8],
        chunk_size: usize,
        callback: &mut dyn FnMut(Vec<NfsDirEntry>) -> bool,
    ) -> NfsResult<usize> {
        NfsConnection::readdir_plus_by_fh(self, file_handle, chunk_size, callback)
    }
    fn readdir_plus_with_fh(
        &self,
        path: &[u8],
        chunk_size: usize,
        callback: &mut dyn FnMut(Vec<NfsDirEntry>) -> bool,
    ) -> NfsResult<usize> {
        NfsConnection::readdir_plus_with_fh(self, path, chunk_size, callback)
    }
    fn submit_readdirplus_by_fh(
        &self,
        file_handle: &[u8],
        cookie: u64,
        cookieverf: [i8; 8],
        tag: u64,
    ) -> NfsResult<InflightReaddir> {
        NfsConnection::submit_readdirplus_by_fh(self, file_handle, cookie, cookieverf, tag)
    }
    fn pump(
        &self,
        slots: &[InflightReaddir],
        min_completions: usize,
        timeout_ms: i32,
    ) -> NfsResult<usize> {
        NfsConnection::pump(self, slots, min_completions, timeout_ms)
    }
    fn getattr_by_fh(&self, file_handle: &[u8], path: &[u8]) -> NfsResult<EntryAttrs> {
        NfsConnection::getattr_by_fh(self, file_handle, path)
    }
    fn lookup_name(&self, dir_fh: &[u8], name: &[u8], path: &[u8]) -> NfsResult<LookupReply> {
        NfsConnection::lookup_name(self, dir_fh, name, path)
    }
}

/// Which fallback supplied an entry's attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Via {
    /// GETATTR on the handle READDIRPLUS supplied.
    Getattr,
    /// READDIRPLUS supplied no usable handle: a LOOKUP found the object
    /// (and a GETATTR followed if the LOOKUP reply had no attributes).
    Lookup,
}

/// The outcome of resolving one entry.
#[derive(Debug)]
pub(crate) enum Resolution {
    Resolved {
        attrs: EntryAttrs,
        /// The handle a LOOKUP produced, when one was needed.
        file_handle: Option<Vec<u8>>,
        via: Via,
    },
    /// Confirmed gone between the listing and the resolution.
    Vanished {
        attempts: u32,
    },
    Failed(DirFailure),
}

fn failed(path: &[u8], kind: FailureKind, error: String, attempts: u32) -> Resolution {
    Resolution::Failed(DirFailure {
        path: display_path(path).into_owned(),
        kind,
        error,
        attempts,
    })
}

/// Resolve the attributes of the entry at `path` under `policy`.
///
/// - `handle` is the file handle READDIRPLUS supplied, if any.
/// - `getattr(fh)` issues GETATTR.
/// - `lookup()` issues LOOKUP of the entry's raw name in its parent.
/// - `confirm()` resolves the entry's path to a fresh handle from the
///   export root, the same call that confirms a vanished directory:
///   `NotFound` from it is the proof that the entry is gone.
/// - `sleep(d)` waits out a backoff (injected so tests never sleep).
pub(crate) fn resolve_attrs(
    path: &[u8],
    handle: Option<Vec<u8>>,
    policy: &RetryPolicy,
    mut getattr: impl FnMut(&[u8]) -> Result<EntryAttrs, NfsError>,
    mut lookup: impl FnMut() -> Result<LookupReply, NfsError>,
    mut confirm: impl FnMut() -> Result<Vec<u8>, NfsError>,
    mut sleep: impl FnMut(Duration),
) -> Resolution {
    let mut fh = handle;
    // Set once the handle in use no longer comes from READDIRPLUS.
    let mut looked_up = false;
    let mut n = 0u32;
    loop {
        n += 1;
        let attempt = match fh.as_deref() {
            Some(h) => getattr(h),
            None => lookup().and_then(|reply| {
                looked_up = true;
                // Keep the handle: if the GETATTR below fails, the next
                // attempt goes straight to it.
                let h = fh.insert(reply.file_handle);
                match reply.attrs {
                    Some(attrs) => Ok(attrs),
                    None => getattr(h),
                }
            }),
        };
        let err = match attempt {
            Ok(attrs) if attrs.entry_type == EntryType::Unknown => {
                // The server answered; asking again will not change it.
                return failed(
                    path,
                    FailureKind::Protocol,
                    format!(
                        "the server reports NFSv3 file type {}, which is not one of the seven \
                         protocol types",
                        attrs.nfs_type
                    ),
                    n,
                );
            }
            Ok(attrs) => {
                return Resolution::Resolved {
                    attrs,
                    file_handle: if looked_up { fh } else { None },
                    via: if looked_up { Via::Lookup } else { Via::Getattr },
                };
            }
            Err(e) => e,
        };
        let fail = |n| failed(path, err.failure_kind(), err.to_string(), n);
        match retry::plan(&err, n, policy) {
            Action::Fail(_) => return fail(n),
            Action::CheckGone => match confirm() {
                Err(e) if e.failure_kind() == FailureKind::NotFound => {
                    return Resolution::Vanished { attempts: n };
                }
                Ok(fresh) => {
                    // It is there after all: the attempt hit a race or
                    // a bad handle. Go through the fresh one.
                    if n >= policy.max_attempts {
                        return fail(n);
                    }
                    fh = Some(fresh);
                    looked_up = true;
                }
                Err(_) => {
                    if n >= policy.max_attempts {
                        return fail(n);
                    }
                    sleep(policy.backoff(n));
                }
            },
            Action::Retry { after, refresh_fh } => {
                if refresh_fh {
                    match confirm() {
                        Ok(fresh) => {
                            fh = Some(fresh);
                            looked_up = true;
                        }
                        Err(e) if e.failure_kind() == FailureKind::NotFound => {
                            return Resolution::Vanished { attempts: n };
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

/// How often the fallbacks ran in one scan, and how often they could
/// not establish a type. Shared by every worker.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResolveStats {
    /// Entries resolved by GETATTR on the handle READDIRPLUS supplied.
    pub by_getattr: Arc<AtomicU64>,
    /// Entries READDIRPLUS returned without a usable handle, resolved
    /// through a LOOKUP.
    pub by_lookup: Arc<AtomicU64>,
    /// Entries whose type could not be established. Each one is also
    /// counted in the scan's `errors`, which makes the scan incomplete.
    pub unresolved: Arc<AtomicU64>,
    /// Entries confirmed gone before they could be resolved. Each one
    /// is also counted in the scan's `vanished`.
    pub vanished: Arc<AtomicU64>,
}

/// One worker's resolver: the scan's retry policy and where to report
/// what happened to an entry.
pub(crate) struct EntryResolver<'a> {
    pub worker: usize,
    pub policy: &'a RetryPolicy,
    /// Caps a backoff. The pipelined worker keeps it short so its
    /// in-flight slots stay serviced.
    pub max_sleep: Duration,
    pub failures: &'a FailureLog,
    pub errors: &'a AtomicU64,
    pub vanished: &'a AtomicU64,
    pub stats: &'a ResolveStats,
}

impl EntryResolver<'_> {
    /// Give a READDIRPLUS entry without a type its attributes.
    ///
    /// Returns `false` when the entry must be neither emitted nor
    /// descended into: it vanished, or it could not be resolved. Both
    /// are recorded before returning.
    ///
    /// `parent_fh` is the handle of the directory being read, resolved
    /// from `parent_path` on first use when the directory was opened by
    /// path.
    pub(crate) fn settle<N: NfsOps>(
        &self,
        nfs: &N,
        entry: &mut NfsDirEntry,
        full_path: &[u8],
        parent_path: &[u8],
        parent_fh: &RefCell<Option<Vec<u8>>>,
    ) -> bool {
        let policy = self.policy;
        let max_sleep = self.max_sleep;
        let resolution = if nfs.is_connected() {
            let name = &entry.name;
            resolve_attrs(
                full_path,
                entry.file_handle.clone(),
                policy,
                |fh| nfs.getattr_by_fh(fh, full_path),
                || {
                    let cached = parent_fh.borrow().clone();
                    let dir_fh = match cached {
                        Some(fh) => fh,
                        None => {
                            let fh = nfs.resolve_path_to_fh(parent_path)?;
                            *parent_fh.borrow_mut() = Some(fh.clone());
                            fh
                        }
                    };
                    nfs.lookup_name(&dir_fh, name, full_path)
                },
                || nfs.resolve_path_to_fh(full_path),
                |d| std::thread::sleep(d.min(max_sleep)),
            )
        } else {
            // A poisoned connection answers nothing; retrying against it
            // would only burn the backoff for every remaining entry.
            failed(
                full_path,
                FailureKind::ConnectionLost,
                "connection poisoned before the entry's attributes could be read".into(),
                0,
            )
        };

        match resolution {
            Resolution::Resolved {
                attrs,
                file_handle,
                via,
            } => {
                match via {
                    Via::Getattr => self.stats.by_getattr.fetch_add(1, Ordering::Relaxed),
                    Via::Lookup => self.stats.by_lookup.fetch_add(1, Ordering::Relaxed),
                };
                debug!(
                    "Worker {} resolved {} as {:?} via {:?} (READDIRPLUS sent no attributes)",
                    self.worker,
                    display_path(full_path),
                    attrs.entry_type,
                    via
                );
                entry.apply_attrs(attrs, file_handle);
                true
            }
            Resolution::Vanished { attempts } => {
                self.vanished.fetch_add(1, Ordering::Relaxed);
                self.stats.vanished.fetch_add(1, Ordering::Relaxed);
                self.failures.record_vanished_entry(full_path, attempts);
                debug!(
                    "Worker {} entry vanished before its attributes could be read: {}",
                    self.worker,
                    display_path(full_path)
                );
                false
            }
            Resolution::Failed(f) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                self.stats.unresolved.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "Worker {} could not establish the type of {}: {} ({}, {} attempts) — the scan will be reported incomplete",
                    self.worker, f.path, f.error, f.kind, f.attempts
                );
                self.failures.record(&f);
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs::types::NfsStat;
    use std::cell::Cell;

    const PATH: &[u8] = b"/export/d/name";

    fn policy() -> RetryPolicy {
        RetryPolicy::from_retries(2) // 3 attempts
    }

    fn attrs(entry_type: EntryType, inode: u64) -> EntryAttrs {
        EntryAttrs {
            entry_type,
            nfs_type: match entry_type {
                EntryType::File => 1,
                EntryType::Directory => 2,
                EntryType::BlockDevice => 3,
                EntryType::CharDevice => 4,
                EntryType::Symlink => 5,
                EntryType::Socket => 6,
                EntryType::Fifo => 7,
                EntryType::Unknown => 0,
            },
            stat: NfsStat {
                inode,
                size: 4096,
                mode: 0o750,
                uid: 1000,
                gid: 2000,
                nlink: 2,
                fsid: 77,
                ..NfsStat::default()
            },
        }
    }

    fn transient() -> NfsError {
        NfsError::AttrFailed {
            path: "/export/d/name".into(),
            reason: "GETATTR: NFS3ERR_JUKEBOX (jukebox/try again later)".into(),
        }
    }

    fn not_found() -> NfsError {
        NfsError::NotFound {
            path: "/export/d/name".into(),
        }
    }

    fn stale() -> NfsError {
        NfsError::StaleHandle {
            path: "/export/d/name".into(),
        }
    }

    fn no_lookup() -> Result<LookupReply, NfsError> {
        panic!("LOOKUP must not be issued")
    }

    fn no_confirm() -> Result<Vec<u8>, NfsError> {
        panic!("the path must not be re-resolved")
    }

    fn no_getattr(_: &[u8]) -> Result<EntryAttrs, NfsError> {
        panic!("GETATTR must not be issued")
    }

    fn no_sleep(_: Duration) {
        panic!("nothing to back off from")
    }

    #[test]
    fn handle_without_attributes_is_resolved_by_getattr() {
        let asked = RefCell::new(Vec::new());
        let r = resolve_attrs(
            PATH,
            Some(vec![1, 2, 3]),
            &policy(),
            |fh| {
                asked.borrow_mut().push(fh.to_vec());
                Ok(attrs(EntryType::Directory, 42))
            },
            no_lookup,
            no_confirm,
            no_sleep,
        );
        match r {
            Resolution::Resolved {
                attrs,
                file_handle,
                via,
            } => {
                assert_eq!(attrs.entry_type, EntryType::Directory);
                assert_eq!(attrs.stat.inode, 42);
                assert_eq!(via, Via::Getattr);
                assert_eq!(file_handle, None, "READDIRPLUS's handle stays in place");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(*asked.borrow(), vec![vec![1u8, 2, 3]]);
    }

    #[test]
    fn no_handle_is_resolved_by_lookup_attributes() {
        let r = resolve_attrs(
            PATH,
            None,
            &policy(),
            no_getattr,
            || {
                Ok(LookupReply {
                    file_handle: vec![9],
                    attrs: Some(attrs(EntryType::Fifo, 7)),
                })
            },
            no_confirm,
            no_sleep,
        );
        match r {
            Resolution::Resolved {
                attrs,
                file_handle,
                via,
            } => {
                assert_eq!(attrs.entry_type, EntryType::Fifo);
                assert_eq!(via, Via::Lookup);
                assert_eq!(file_handle, Some(vec![9]));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn lookup_without_attributes_is_followed_by_getattr_on_its_handle() {
        let getattrs = RefCell::new(Vec::new());
        let r = resolve_attrs(
            PATH,
            None,
            &policy(),
            |fh| {
                getattrs.borrow_mut().push(fh.to_vec());
                Ok(attrs(EntryType::Directory, 5))
            },
            || {
                Ok(LookupReply {
                    file_handle: vec![8, 8],
                    attrs: None,
                })
            },
            no_confirm,
            no_sleep,
        );
        match r {
            Resolution::Resolved {
                attrs,
                file_handle,
                via,
            } => {
                assert_eq!(attrs.entry_type, EntryType::Directory);
                assert_eq!(via, Via::Lookup);
                assert_eq!(file_handle, Some(vec![8, 8]));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(*getattrs.borrow(), vec![vec![8u8, 8]]);
    }

    /// A mountpoint: the listing carries the mounted-on directory's
    /// number, the resolved attributes the mounted filesystem's root.
    /// The entry is resolved, and its identity is the resolved one.
    #[test]
    fn a_different_fileid_than_the_listing_is_resolved_not_failed() {
        let listed_inode = 11;
        let r = resolve_attrs(
            PATH,
            None,
            &policy(),
            no_getattr,
            || {
                Ok(LookupReply {
                    file_handle: vec![4],
                    attrs: Some(attrs(EntryType::Directory, 2)),
                })
            },
            no_confirm,
            no_sleep,
        );
        let Resolution::Resolved {
            attrs, file_handle, ..
        } = r
        else {
            panic!("{r:?}");
        };
        let mut entry = NfsDirEntry {
            name: b"name".to_vec(),
            entry_type: EntryType::Unknown,
            stat: None,
            inode: listed_inode,
            file_handle: None,
        };
        entry.apply_attrs(attrs, file_handle);
        assert_eq!(entry.inode, 2);
        assert_eq!(entry.fsid(), Some(77));
    }

    #[test]
    fn transient_error_is_retried_within_the_bound_then_succeeds() {
        let calls = Cell::new(0u32);
        let slept = RefCell::new(Vec::new());
        let r = resolve_attrs(
            PATH,
            Some(vec![1]),
            &policy(),
            |_| {
                calls.set(calls.get() + 1);
                if calls.get() < 3 {
                    Err(transient())
                } else {
                    Ok(attrs(EntryType::Socket, 3))
                }
            },
            no_lookup,
            no_confirm,
            |d| slept.borrow_mut().push(d),
        );
        assert!(
            matches!(r, Resolution::Resolved { ref attrs, .. } if attrs.entry_type == EntryType::Socket),
            "{r:?}"
        );
        assert_eq!(calls.get(), 3);
        assert_eq!(
            *slept.borrow(),
            vec![policy().backoff(1), policy().backoff(2)],
            "the scan's own backoff, once per retry"
        );
    }

    #[test]
    fn transient_lookup_error_is_retried_within_the_bound_then_succeeds() {
        let lookups = Cell::new(0u32);
        let r = resolve_attrs(
            PATH,
            None,
            &policy(),
            no_getattr,
            || {
                lookups.set(lookups.get() + 1);
                if lookups.get() < 3 {
                    Err(NfsError::AttrFailed {
                        path: "/export/d/name".into(),
                        reason: "LOOKUP: NFS3ERR_IO (I/O error)".into(),
                    })
                } else {
                    Ok(LookupReply {
                        file_handle: vec![6],
                        attrs: Some(attrs(EntryType::BlockDevice, 4)),
                    })
                }
            },
            no_confirm,
            |_| {},
        );
        assert!(
            matches!(
                r,
                Resolution::Resolved { ref attrs, via: Via::Lookup, .. }
                    if attrs.entry_type == EntryType::BlockDevice
            ),
            "{r:?}"
        );
        assert_eq!(lookups.get(), 3);
    }

    #[test]
    fn transient_error_that_never_clears_fails_after_the_bound() {
        let calls = Cell::new(0u32);
        let r = resolve_attrs(
            PATH,
            Some(vec![1]),
            &policy(),
            |_| {
                calls.set(calls.get() + 1);
                Err(transient())
            },
            no_lookup,
            no_confirm,
            |_| {},
        );
        match r {
            Resolution::Failed(f) => {
                assert_eq!(f.kind, FailureKind::Transient);
                assert_eq!(f.attempts, 3);
                assert_eq!(f.path, "/export/d/name");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(calls.get(), 3, "bounded by the policy");
    }

    #[test]
    fn non_transient_error_is_not_retried() {
        let calls = Cell::new(0u32);
        let r = resolve_attrs(
            PATH,
            Some(vec![1]),
            &policy(),
            |_| {
                calls.set(calls.get() + 1);
                Err(NfsError::PermissionDenied {
                    path: "/export/d/name".into(),
                })
            },
            no_lookup,
            no_confirm,
            no_sleep,
        );
        match r {
            Resolution::Failed(f) => {
                assert_eq!(f.kind, FailureKind::PermissionDenied);
                assert_eq!(f.attempts, 1);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn not_found_confirmed_by_path_lookup_is_vanished() {
        let r = resolve_attrs(
            PATH,
            None,
            &policy(),
            no_getattr,
            || Err(not_found()),
            || Err(not_found()),
            no_sleep,
        );
        assert!(matches!(r, Resolution::Vanished { attempts: 1 }), "{r:?}");
    }

    #[test]
    fn stale_handle_whose_path_is_gone_is_vanished() {
        let r = resolve_attrs(
            PATH,
            Some(vec![1]),
            &policy(),
            |_| Err(stale()),
            no_lookup,
            || Err(not_found()),
            no_sleep,
        );
        assert!(matches!(r, Resolution::Vanished { attempts: 1 }), "{r:?}");
    }

    /// The server said NOENT, but the path still resolves: not proof of
    /// a disappearance. If it keeps failing the entry is unresolved.
    #[test]
    fn not_found_contradicted_by_the_path_lookup_is_unresolved_not_vanished() {
        let getattrs = RefCell::new(Vec::new());
        let r = resolve_attrs(
            PATH,
            Some(vec![1]),
            &policy(),
            |fh| {
                getattrs.borrow_mut().push(fh.to_vec());
                Err(not_found())
            },
            no_lookup,
            || Ok(vec![2]),
            no_sleep,
        );
        match r {
            Resolution::Failed(f) => {
                assert_eq!(f.kind, FailureKind::NotFound);
                assert_eq!(f.attempts, 3);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            *getattrs.borrow(),
            vec![vec![1u8], vec![2], vec![2]],
            "retried through the freshly resolved handle"
        );
    }

    #[test]
    fn not_found_contradicted_then_resolved_through_the_fresh_handle() {
        let r = resolve_attrs(
            PATH,
            Some(vec![1]),
            &policy(),
            |fh| {
                if fh == [1] {
                    Err(not_found())
                } else {
                    Ok(attrs(EntryType::File, 6))
                }
            },
            no_lookup,
            || Ok(vec![2]),
            no_sleep,
        );
        match r {
            Resolution::Resolved {
                attrs,
                file_handle,
                via,
            } => {
                assert_eq!(attrs.entry_type, EntryType::File);
                assert_eq!(file_handle, Some(vec![2]));
                assert_eq!(via, Via::Lookup);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn stale_handle_is_re_resolved_by_path_and_retried() {
        let r = resolve_attrs(
            PATH,
            Some(vec![1]),
            &policy(),
            |fh| {
                if fh == [1] {
                    Err(stale())
                } else {
                    Ok(attrs(EntryType::Symlink, 8))
                }
            },
            no_lookup,
            || Ok(vec![3]),
            no_sleep,
        );
        assert!(
            matches!(r, Resolution::Resolved { ref attrs, .. } if attrs.entry_type == EntryType::Symlink),
            "{r:?}"
        );
    }

    #[test]
    fn a_failed_getattr_after_lookup_retries_the_getattr_not_the_lookup() {
        let lookups = Cell::new(0u32);
        let getattrs = Cell::new(0u32);
        let r = resolve_attrs(
            PATH,
            None,
            &policy(),
            |_| {
                getattrs.set(getattrs.get() + 1);
                if getattrs.get() == 1 {
                    Err(transient())
                } else {
                    Ok(attrs(EntryType::CharDevice, 9))
                }
            },
            || {
                lookups.set(lookups.get() + 1);
                Ok(LookupReply {
                    file_handle: vec![5],
                    attrs: None,
                })
            },
            no_confirm,
            |_| {},
        );
        match r {
            Resolution::Resolved {
                attrs,
                file_handle,
                via,
            } => {
                assert_eq!(attrs.entry_type, EntryType::CharDevice);
                assert_eq!(file_handle, Some(vec![5]));
                assert_eq!(via, Via::Lookup);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(lookups.get(), 1);
        assert_eq!(getattrs.get(), 2);
    }

    #[test]
    fn a_type_outside_the_protocol_is_a_failure_not_a_guess() {
        let calls = Cell::new(0u32);
        let r = resolve_attrs(
            PATH,
            Some(vec![1]),
            &policy(),
            |_| {
                calls.set(calls.get() + 1);
                Ok(EntryAttrs {
                    nfs_type: 9,
                    ..attrs(EntryType::Unknown, 1)
                })
            },
            no_lookup,
            no_confirm,
            no_sleep,
        );
        match r {
            Resolution::Failed(f) => {
                assert_eq!(f.kind, FailureKind::Protocol);
                assert!(f.error.contains("file type 9"), "{}", f.error);
                assert_eq!(f.attempts, 1);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(calls.get(), 1, "the server answered; not retried");
    }
}
