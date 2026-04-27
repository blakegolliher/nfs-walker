# Pipelined READDIRPLUS Design

Status: design, not yet implemented.
Owner: -
Target: feature-flagged behind `--pipeline-depth N` (0 disables, current behavior).

## 1. Problem

Production walks of multi-billion-file NFS exports show ~265 dirs/sec across 512
workers (~0.5 dirs/sec/worker) on a 160-core / 1.4 TiB host with load average
~3.4 — i.e. CPU is idle and the walker is RPC-latency bound. Each worker holds
one libnfs context (one TCP socket) and issues exactly one RPC at a time:

```
submit READDIRPLUS → poll fd → recv → process → submit next → ...
```

The submit/poll/process loop lives in `src/nfs/connection.rs:541-615`
(`readdir_plus_by_fh`) and is driven by `wait_for_rpc_completion`
(`src/nfs/connection.rs:1387-1475`), which blocks on a single in-flight PDU.

Per-worker throughput is therefore capped at `1 / (server-side READDIRPLUS
prep time + RTT)`. Server prep dominates on huge directories (50–500 ms),
so each worker tops out at 2–20 RPCs/sec regardless of how fast the
network is.

## 2. Goal

Allow each worker to keep N READDIRPLUS RPCs in flight on its single libnfs
context, demuxing replies as they arrive. Target: 4–8× per-worker throughput
on workloads with reasonable directory fan-out (the production workload that
motivated this work — 16M dirs averaging ~250 entries each — is well-shaped
for it).

Non-goal: more sockets. We are not adding a second TCP connection per worker
and we are not rewriting libnfs. Pipelining uses libnfs's existing async
PDU queue.

## 3. Key insight

READDIRPLUS uses a **cookie chain**: each reply contains the cookie that
must be passed to the next call to read more entries from *that same
directory*. You cannot issue page 2 of a directory until page 1 returns —
within a single directory, RPCs are inherently serial.

But across **different** directories, each has its own independent cookie
chain. Worker holds 8 in-flight slots, each on a different directory →
fully parallel. As each directory's pages stream in, the worker either
submits the next page of the same directory (advancing its cookie) or, on
EOF, refills the slot from its work-stealing deque.

A worker stuck on one giant directory degenerates to today's behavior
(serial pages); the rest of the pool keeps moving, so global throughput
holds up.

## 4. Architecture

Two layers. The split matters because the worker — not the connection —
owns work-stealing, batching, and counter updates.

### Layer 1: `NfsConnection` async primitives

Add three methods. They replace the inner loop body of `readdir_plus_by_fh`
(`src/nfs/connection.rs:541-615`) but do not delete it; the existing
blocking method stays for path-lookup and other call sites.

```rust
/// Heap-pinned per-PDU state. Address must remain stable from submit
/// until the libnfs callback fires, because the raw pointer to this
/// struct is handed to libnfs as the PDU's private_data.
pub struct InflightReaddir {
    /// Boxed because libnfs writes into it from rpc_service. Heap
    /// address is stable across worker reshuffles of the slot vec.
    cb_data: Box<ReaddirplusFullData>,
    /// Worker-supplied tag. Opaque here; the worker uses it to map a
    /// completion back to its DirState slot.
    pub tag: u64,
}

impl InflightReaddir {
    pub fn is_completed(&self) -> bool { self.cb_data.completed }
    pub fn status(&self) -> i32 { self.cb_data.status }
    /// Drains the entries vec and the EOF/cookie/cookieverf fields.
    /// Caller invokes this after `is_completed()` returns true. Idempotent.
    pub fn take_result(&mut self) -> ReaddirplusResult { ... }
}

pub struct ReaddirplusResult {
    pub entries: Vec<NfsDirEntry>,
    pub eof: bool,
    pub next_cookie: u64,
    pub next_cookieverf: [i8; 8],
    pub status: i32,
}

impl NfsConnection {
    /// Submit a READDIRPLUS RPC by file handle. Does NOT block.
    ///
    /// On success the returned InflightReaddir holds the cb_data Box;
    /// libnfs holds a raw pointer into it. The caller MUST keep the
    /// InflightReaddir alive until either:
    ///   (a) `is_completed()` returns true, or
    ///   (b) the NfsConnection is dropped (which destroys the rpc_context
    ///       and cancels all in-flight PDUs).
    pub fn submit_readdirplus_by_fh(
        &self,
        file_handle: &[u8],
        cookie: u64,
        cookieverf: [i8; 8],
        tag: u64,
    ) -> NfsResult<InflightReaddir>;

    /// Drive the rpc_context until at least `min_completions` of the
    /// supplied slots have flipped to completed, or until `timeout_ms`
    /// elapses with no progress. Returns the number of slots completed
    /// during this call (may be > min_completions if multiple replies
    /// land in one rpc_service call).
    ///
    /// Implementation: a generalization of wait_for_rpc_completion. Each
    /// loop iteration: rpc_which_events → poll(fd, events, poll_step_ms)
    /// → rpc_service(revents) → count completed slots → return when ≥ min.
    ///
    /// `poll_step_ms` is small (default 10 ms) so a worker holding 1
    /// slow in-flight slot can return promptly to refill empty slots
    /// from its deque.
    pub fn pump(
        &self,
        slots: &[InflightReaddir],
        min_completions: usize,
        timeout_ms: i32,
    ) -> NfsResult<usize>;
}
```

Notes on each:

**`submit_readdirplus_by_fh`**: lifts the inner block of
`readdir_plus_by_fh` (lines 541-574) — build args, call
`rpc_nfs3_readdirplus_task`, return without polling. The args struct
(`READDIRPLUS3args`) can stay stack-allocated because libnfs XDR-encodes
it into the PDU buffer at submit time and does not retain a pointer
afterward. (Verify in libnfs source: `nfs3-mount.c::rpc_nfs3_readdirplus_task`
copies into a freshly allocated `pdu` via `xdr_READDIRPLUS3args`. Args
struct is unused after the call.)

**`InflightReaddir`** owns `Box<ReaddirplusFullData>`. The cb_data type
already exists at `src/nfs/connection.rs:1265-1272`. The PDU's private_data
pointer must be `&mut *box` — taken via `Box::as_mut` and cast to `*mut
c_void`. The Box is moved into `InflightReaddir` and lives there until
the slot is consumed.

**`pump`**: see also "Edge cases" §6. Generalizes
`wait_for_rpc_completion` (lines 1387-1475) by counting completions across
a slice of cb_data flags rather than waiting on a single one. Returns
zero on timeout (no progress) without erroring; only fd-level errors
return Err.

### Layer 2: pipelined worker loop

New function `worker_loop_pipelined` next to existing `worker_loop`
in `src/walker/simple.rs`. Selected by config; both share the same
`run_workers` setup (deque, stealers, counters).

```rust
const POLL_STEP_MS: i32 = 10;

struct DirState {
    work: DirWork,            // path, depth, file_handle (cached fh always present in pipelined mode)
    cookie: u64,
    cookieverf: [i8; 8],
    // EOF lives in the ReaddirplusResult; we drop the slot when we see it.
}

fn worker_loop_pipelined(
    id: usize,
    nfs: NfsConnection,
    local: DequeWorker<DirWork>,
    injector: Arc<Injector<DirWork>>,
    stealers: Arc<Vec<Stealer<DirWork>>>,
    entry_tx: Sender<Vec<DbEntry>>,
    shutdown: Arc<AtomicBool>,
    /* ... counters as today ... */
    pipeline_depth: usize,
    batch_size: usize,
    /* dirs_only, max_depth, content-analysis flags as today */
) {
    let mut slots: Vec<InflightReaddir> = Vec::with_capacity(pipeline_depth);
    let mut states: Vec<DirState> = Vec::with_capacity(pipeline_depth);
    let mut batch: Vec<DbEntry> = Vec::with_capacity(batch_size);

    loop {
        if shutdown.load(Relaxed) { break; }

        // 1. Refill empty slots.
        while slots.len() < pipeline_depth {
            let Some(work) = try_get_work(&local, &injector, &stealers, id) else { break };

            // In pipelined mode, root path-lookup happens on the main
            // thread before workers start. Workers only ever see cached
            // file handles. If a DirWork lacks one, fall back to today's
            // blocking path-lookup, then resume pipelined operation.
            let fh = match work.file_handle.clone() {
                Some(fh) => fh,
                None => match resolve_path_to_fh(&nfs, &work.path) {
                    Ok(fh) => fh,
                    Err(e) => { handle_error(&work, e); pending_work.fetch_sub(1, Release); continue; }
                },
            };

            let tag = next_tag();
            match nfs.submit_readdirplus_by_fh(&fh, 0, [0i8; 8], tag) {
                Ok(slot) => {
                    slots.push(slot);
                    states.push(DirState { work, cookie: 0, cookieverf: [0i8; 8] });
                }
                Err(e) => { handle_error(&work, e); pending_work.fetch_sub(1, Release); }
            }
        }

        if slots.is_empty() {
            // Termination check (today's pattern, slightly stronger ordering).
            if pending_work.load(Acquire) == 0 && active_workers.load(Acquire) == 0 {
                break;
            }
            thread::sleep(Duration::from_micros(100));
            continue;
        }

        // 2. Drive RPCs. Block up to a short window for at least one completion;
        // returning early on timeout lets us refill from the deque.
        let progress = nfs.pump(&slots, 1, POLL_STEP_MS * 5)?;

        if progress == 0 {
            continue; // timeout — loop back to refill / re-check shutdown
        }

        // 3. Drain completed slots. Reverse iter so swap_remove is safe.
        let mut i = slots.len();
        while i > 0 {
            i -= 1;
            if !slots[i].is_completed() { continue; }

            let mut slot = slots.swap_remove(i);
            let mut state = states.swap_remove(i);
            let result = slot.take_result();

            match result.status {
                s if s == ffi::RPC_STATUS_SUCCESS as i32 => {
                    process_entries_pipelined(
                        &state, &result.entries, &mut batch, &local,
                        &entry_tx, batch_size, /* counters, content flags */
                    );

                    if result.eof {
                        dirs_count.fetch_add(1, Relaxed);
                        pending_work.fetch_sub(1, Release);
                        // slot dropped — Box freed
                    } else {
                        // More pages for this same dir. Advance cookie,
                        // re-submit, push back into slots/states.
                        state.cookie = result.next_cookie;
                        state.cookieverf = result.next_cookieverf;
                        let new_slot = nfs.submit_readdirplus_by_fh(
                            state.work.file_handle.as_deref().expect("cached fh"),
                            state.cookie, state.cookieverf, slot.tag,
                        )?;
                        slots.push(new_slot);
                        states.push(state);
                    }
                }
                err_status => {
                    errors_count.fetch_add(1, Relaxed);
                    log_err(&state.work, err_status);
                    pending_work.fetch_sub(1, Release);
                }
            }
        }
    }

    // Drain remaining batch as today's worker_loop does.
    if !batch.is_empty() { let _ = entry_tx.send(batch); }
}
```

`process_entries_pipelined` is `process_entries` (the closure inside
today's `worker_loop`, `src/walker/simple.rs:1074-1164`) lifted to a free
function taking `&DirState` instead of capturing `work` from the outer
scope. Behavior is identical: emit DbEntries, push subdirs into
`local`, increment `pending_work` per subdir, send batches when full.

## 5. Memory safety / FFI lifetimes

The whole point of this design is that `cb_data` outlives the synchronous
function call. Three rules and one verification step.

**Rule 1: cb_data is heap-pinned.** `Box<ReaddirplusFullData>` allocated
on submit, address handed to libnfs as private_data, freed only when the
slot is dropped. Never `mem::swap`, never `mem::replace` the box.

**Rule 2: `InflightReaddir` is `!Send` per-PDU.** The cb_data is mutated
from the libnfs callback, which runs on whatever thread calls
`rpc_service`. In our model that's always the worker thread that owns
the `NfsConnection`. We must not move slots between worker threads.
Mark `InflightReaddir` as `!Send` (or just don't expose anything that
would move it cross-thread; `NfsConnection` is already `!Sync` and bound
to one worker).

**Rule 3: Drop order on shutdown.** `NfsConnection::Drop` calls
`nfs_destroy_context`, which cancels all in-flight PDUs and frees their
internal state without firing callbacks. So: `Vec<InflightReaddir>` MUST
be dropped before `NfsConnection`, otherwise libnfs has freed the PDU
but the Box holding cb_data is still alive — which is fine for memory
(no UAF), but its `completed` flag will never flip, leaking work-tracking
counters. Worker loop exits → slots vec drops → connection drops. Order
is satisfied automatically by stack-frame drop order if both are local
to `worker_loop_pipelined`. **Do not** stash slots in a struct that
outlives the connection.

**Verification step before shipping**: run a 100K-dir scan under
`valgrind --tool=memcheck --track-origins=yes` and a separate run under
`ASAN`. We've had FFI lifetime bugs before (LIBNFS.md mentions one). A
small unit test `pipelined_drop_with_inflight_does_not_segfault` should
submit 8 RPCs against a real loopback NFS server, drop the connection
without pumping, and assert clean exit.

## 6. Edge cases

1. **`drain_pending_events` at top of `readdir_plus_by_fh`**
   (`src/nfs/connection.rs:529`): defensive code for the
   single-RPC-at-a-time model. **Do not** call it in `submit_readdirplus_by_fh`
   or `pump` — it would discard completions for in-flight slots.

2. **`pump` called with all slots already completed**: return immediately
   with the count of completed slots. Don't enter the poll loop.

3. **`pump` called with slots.len() == 0**: return 0. This shouldn't
   happen in the worker loop above (gated by `slots.is_empty()` check)
   but defend against it.

4. **`rpc_which_events` returns 0 with uncompleted slots**: matches
   today's edge case at `connection.rs:1426-1435`. Service with revents=0,
   sleep 10 ms, retry. Same logic — copy-paste, don't try to be clever.

5. **A single `rpc_service` call completes multiple slots**: handled by
   the reverse-iter drain in the worker loop. `pump` returns the actual
   completion count, which may exceed `min_completions`.

6. **Worker steals a `DirWork` whose `file_handle` is `None`**: this
   happens for the root directory (queued without a cached fh in
   `run_workers`, `src/walker/simple.rs:665-669`) and for any dir that
   was injected without a fh. Pipelined mode falls back to a synchronous
   path-lookup (`resolve_path_to_fh` — wraps the same lookup that
   `readdir_plus_with_fh` does today, lines 363-414) and then proceeds
   pipelined from there. Don't try to pipeline path-lookup itself.

7. **Cookie verifier mismatch (NFS3ERR_BAD_COOKIE = 10003)**: server
   has restarted or directory has changed under us. Today's code
   doesn't retry; pipelined version shouldn't either. Just record the
   error, drop the slot, decrement pending_work.

8. **Shutdown mid-pipeline**: `shutdown` flag is checked at top of
   the loop. Slots in flight at shutdown time are dropped; their PDUs
   are cancelled by `NfsConnection::Drop`. Counters for those dirs are
   left as-is (the stats reflect "interrupted").

9. **Slow giant directory blocks a worker's pipeline**: e.g. one slot
   stuck on a 30-second READDIRPLUS over a 10M-file dir. The other 7
   slots complete quickly and refill from new dirs. The worker keeps
   moving. This is the design working as intended — pipelining doesn't
   speed up the giant dir, but it stops that dir from starving the
   worker. Verify via the bench in §8 that throughput stays high
   when one dir in the test set is artificially huge.

## 7. Configuration

Add to `src/config.rs`:

```rust
/// Number of READDIRPLUS RPCs to keep in flight per worker.
/// 0 disables pipelining (uses the legacy worker_loop). 1 = effectively
/// the legacy path with the new code path (useful for A/B without
/// touching two binaries). 8 is the recommended default once shipped.
#[arg(long, default_value = "0", value_name = "N")]
pub pipeline_depth: usize,
```

Validation: 0 ≤ N ≤ 64. Above 64 is wasteful (libnfs's internal queue
sizing isn't tuned for hundreds of in-flight PDUs per context) and risks
hitting MAX_TOO_MANY_OUTSTANDING per-context server-side limits.

In `SimpleWalker::run_workers` (`src/walker/simple.rs:655-761`), branch
on `pipeline_depth > 0` to pick the worker function. Both worker
variants share the same setup (injector, stealers, DNS round-robin,
counters).

## 8. Testing

### Unit tests (in `src/nfs/connection.rs`)

`#[cfg(test)]` against a real loopback NFS server (already used elsewhere
in the suite — see existing tests at the bottom of connection.rs):

1. `pipelined_submit_two_completes_both`: submit 2 readdirplus on 2
   different cached file handles, pump until both done, assert both
   have entries.

2. `pipelined_drop_with_inflight_does_not_segfault`: submit 4 RPCs,
   drop connection without pumping, asan/valgrind clean exit.

3. `pipelined_cookie_chain_advances_correctly`: submit 1 RPC for a
   directory with 5000 entries (more than fits in one RPC at the
   8KB/16KB dircount/maxcount limits — see `connection.rs:538-539`),
   pump to completion, observe `eof=false`, re-submit with returned
   cookie, repeat to EOF, total entries == 5000.

4. `pipelined_one_slow_dir_does_not_block_others`: against a server
   with a directory of 100K entries and several small dirs, submit 8
   slots (1 huge + 7 small), measure that the small dirs complete
   while the huge dir is still streaming pages.

### Integration test (in `tests/`)

Extend `tests/walker_test.rs` (or create `tests/pipelined_walker_test.rs`)
to walk a synthetic tree of 1000 dirs × 100 files each, with both
`pipeline_depth=0` and `pipeline_depth=8`. Assertions:

- Same DbEntry count.
- Same set of paths.
- Same per-entry attributes for a sampled set of paths.
- `pipeline_depth=8` completes in ≤ 0.6× the time of `pipeline_depth=0`
  (loose threshold; tightens once we have real numbers).

### Benchmark (must run before merge)

Add `benches/pipelined_readdirplus.rs` (or a one-off script in
`benches/`). Two configurations:

- baseline: `--pipeline-depth 0`, current `worker_loop`.
- pipelined: `--pipeline-depth 8`, new path.

Workloads:

1. **Wide shallow**: 100K dirs × 50 files, depth 2. Expected: 4-8× throughput.
2. **Narrow deep**: 100 dirs deep × 10 children each. Expected: ~1×
   (work-stealing already handles this; pipelining can't help when only
   the bottom layer has fanout).
3. **One-mega-dir mix**: 1 dir with 1M entries + 1000 dirs with 100
   entries. Expected: pipelined ≥ baseline (the mega-dir takes the same
   time both ways; pipelined wins by overlapping the small dirs with the
   mega-dir's pages).

Result targets to claim success:

- Workload 1: pipelined ≥ 4× baseline files/sec.
- Workload 2: pipelined ≥ 0.95× baseline (no regression).
- Workload 3: pipelined ≥ 2× baseline.

If any target misses, do not merge — the design is wrong somewhere and
needs profiling, not knob-tuning.

## 9. Rollout

Phase 1 (this work): land behind `--pipeline-depth N` defaulting to 0.
Document in README.md under "Performance tuning".

Phase 2 (separate change, after a real production run validates the
gain): switch default to `--pipeline-depth 8`. Add a release-note
warning: pipelining puts more concurrent load on a single TCP connection
to the NFS server; some servers may need their per-client RPC slot
table tuned up.

Phase 3 (only if needed): expose `--pipeline-depth` as a per-worker
auto-tune (start at 8, back off on consecutive timeouts).

## 10. Files to touch

- `src/nfs/connection.rs`
  - Add `InflightReaddir`, `ReaddirplusResult`, `submit_readdirplus_by_fh`,
    `pump` near the existing `readdir_plus_by_fh` (after line 618).
  - Do NOT delete or modify the existing `readdir_plus_by_fh` —
    `worker_loop` (legacy path) and `readdir_plus_with_fh` still call it.
  - Add `resolve_path_to_fh` helper if not already present (extract from
    `readdir_plus_with_fh` body around lines 379-413).

- `src/walker/simple.rs`
  - Add `worker_loop_pipelined` after `worker_loop` (current
    line 953-1267).
  - Lift `process_entries` from inside `worker_loop` into a free
    function `process_entries_pipelined(state: &DirState, ...)` shared
    by both worker variants. Keep behavior bit-for-bit identical.
  - Branch in `run_workers` (lines 655-761) on
    `self.config.pipeline_depth > 0`.

- `src/config.rs`
  - Add `pipeline_depth` field to `Args` and `WalkConfig` (lines around
    115 and 552), with validation 0 ≤ N ≤ 64.

- `tests/` and `benches/` as described in §8.

- `README.md`: one paragraph under "Performance" linking to this doc.

## 10b. Relationship to `patches/libnfs-vast-extensions.patch`

We carry a libnfs patch (`patches/libnfs-vast-extensions.patch`) that adds:

- `nfs_opendir_at_cookie[_async]` — high-level READDIRPLUS streaming with
  cookie + `max_entries`.
- `nfs_opendir_names_only[_async]` — high-level **READDIR** (not READDIRPLUS),
  names-only.
- `nfs_opendir_names_only_at_cookie[_async]` — combined.
- `nfs_readdir_get_cookieverf` — helper.

**This design intentionally does NOT use those APIs.** Reason: all patched
APIs are *path*-based (they internally call `nfs3_lookuppath_async` to
resolve path → file handle, then issue READDIR(PLUS)). Our walker already
threads a cached file handle from each parent's READDIRPLUS response into
its children's `DirWork` (`src/walker/simple.rs:1136`), so we issue zero
LOOKUP RPCs in the steady state. Switching to the patched API would
re-introduce one LOOKUP chain per directory — a regression we cannot
afford at 16M+ dirs.

The pipelining work therefore lives at the raw-RPC layer
(`rpc_nfs3_readdirplus_task`), which already exists in libnfs upstream and
is what `readdir_plus_by_fh` builds on today. **No new libnfs patch is
required to land this design.**

Implementer must still verify before starting:

1. The crate's `build.rs` and `nfs/bindings.rs` link against the patched
   libnfs (in `libnfs/` next to this repo, or wherever `build.rs` points).
   `rpc_nfs3_readdirplus_task` is upstream libnfs and exists in either
   build, but a sanity `nm libnfs.so | grep rpc_nfs3_readdirplus_task`
   confirms the symbol is exported.
2. `nfs_destroy_context` cancels in-flight PDUs cleanly. Read
   `lib/init.c::nfs_destroy_context` in the patched tree to confirm — the
   drop-order argument in §5 depends on it.

### Followup (separate design, separate PR)

The patched `nfs_opendir_names_only*` APIs are independently valuable
**for `big-dir-hunt` mode** (`src/walker/simple.rs:1506`, currently
issues READDIRPLUS just to count files — which is wasteful when all we
need is the count). A follow-up should:

1. Replace big-dir-hunt's READDIRPLUS-via-raw-RPC with
   `nfs_opendir_names_only_at_cookie_async`, pipelined N=8 the same way
   this design pipelines READDIRPLUS.
2. Expected win: big-dir-hunt becomes 5–10× faster on huge directories
   because the server skips per-entry stat.
3. Out of scope here. File a tracking issue after this lands.

A potential second followup: extend the libnfs patch with `_by_fh`
variants of `nfs_opendir_at_cookie_async` so the main walker could use
the high-level streaming API without giving up cached-fh. That's
strictly a code-cleanup win (libnfs handles the cookie chain instead of
us); no throughput delta vs. the raw-RPC approach in this design. Not
worth the patch surface unless we hit a libnfs bug in our manual
cookie handling.

## 11. Out of scope

- Pipelining the path-lookup RPC chain (NFS3 LOOKUP per component).
  Not worth it; the cached-fh path already eliminates LOOKUPs for all
  non-root directories.
- Pipelining `read_file_content` / `read_file_header` for the
  `--compute-checksum` path. That's a separate (larger) win and a
  separate design. Note it for the next iteration.
- Multi-connection-per-worker. Stays at 1 socket per worker. If that
  becomes the bottleneck, the answer is more workers, not more sockets
  per worker (sockets per worker complicate work-stealing for no
  per-connection benefit).
- io_uring. Investigated, rejected: syscall load (~256/sec at 512
  workers) is far below the threshold where io_uring helps. The
  bottleneck is application-level RPC pipelining, which this design
  addresses without going below the libnfs API.

## 12. Risks

1. **libnfs context concurrency**: libnfs documents that one context
   should be used from one thread at a time. We comply (one context
   per worker, all submit/pump calls from the same worker thread).
   Verify by re-reading `libnfs/lib/init.c` and `libnfs/lib/socket.c`
   to confirm there are no global locks that would serialize across
   contexts in different workers. Existing code already runs 512
   contexts in parallel without issue, so this is mostly a sanity
   check.

2. **Server-side per-client slot exhaustion**: NFS3 has no formal
   slot table (that's NFS4.1 sessions), but servers do bound concurrent
   RPCs per client connection. With 512 workers × 8 in-flight = 4096
   concurrent READDIRPLUS to one server. Should be fine for any modern
   NFS server but worth validating on the production target before
   defaulting to depth 8. Monitor server CPU and RTT during the
   benchmark.

3. **Memtable pressure on the writer**: more RPC throughput → more
   DbEntries → more pressure on the single-threaded RocksDB writer
   (`rocksdb_writer_loop` at `src/walker/simple.rs:1322`). If the
   writer becomes the new bottleneck, the existing channel
   (`bounded(100)` at line 307) becomes the regulator and workers
   block on `entry_tx.send`. That's a separate problem with separate
   fixes (raise channel cap, raise batch_size, multi-shard the writer).
   Make sure the §8 bench notes which side is the bottleneck.

4. **Drop-order subtleties on panic**: if a worker panics mid-pipeline
   with slots in flight, stack unwinding drops slots before the
   connection — correct. Confirm with a `should_panic` test that
   triggers a panic inside the entry-processing path.

## 13. Estimated size

- `connection.rs`: ~150 lines added (primitives + helper).
- `simple.rs`: ~250 lines added (new worker loop + lifted helper).
- Config: ~10 lines.
- Tests: ~200 lines.
- Total: roughly 600 lines of new code, no deletions.

Implementation should fit in one focused session. Bench iteration may
take a second session to chase any unexpected slowdowns in workload 2
or 3.
