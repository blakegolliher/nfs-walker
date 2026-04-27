# Pipelined READDIRPLUS — Implementation plan

Reference: `docs/PIPELINED_READDIRPLUS_DESIGN.md`. Kickoff:
`tasks/pipelined-readdirplus-kickoff.md`.

## Constraints (must hold)

- [ ] Stay on raw-RPC path (`rpc_nfs3_readdirplus_task`); do NOT migrate
      to `nfs_opendir_at_cookie_async` (§10b).
- [ ] Default `--pipeline-depth 0` ⇒ legacy `worker_loop` runs unchanged
      and bit-for-bit identical.
- [ ] FFI lifetime contract: `cb_data` heap-pinned, `InflightReaddir`
      `!Send`, slots dropped before `NfsConnection` (§5).
- [ ] All nine §6 edge cases handled.

## Step 1 — Config plumbing

- [ ] Add `pub pipeline_depth: usize` to `CliArgs` with
      `#[arg(long, default_value = "0", value_name = "N")]`.
- [ ] Add `pub pipeline_depth: usize` to `WalkConfig`.
- [ ] Validate `pipeline_depth <= 64` in `from_args` (new `ConfigError`
      variant `InvalidPipelineDepth { depth, max }`).

## Step 2 — Layer 1: `NfsConnection` async primitives

In `src/nfs/connection.rs`, after the existing `readdir_plus_by_fh`:

- [ ] `pub struct ReaddirplusResult { entries, eof, next_cookie,
      next_cookieverf, status }`.
- [ ] `pub struct InflightReaddir { cb_data: Box<ReaddirplusFullData>,
      tag: u64 }` — `!Send` via `PhantomData<*const ()>`. Methods
      `is_completed()`, `status()`, `take_result()` (idempotent).
- [ ] `pub fn submit_readdirplus_by_fh(&self, fh, cookie, cookieverf,
      tag)` — non-blocking submit. Args struct stack-allocated (libnfs
      XDR-encodes at submit, doesn't retain).
- [ ] `pub fn pump(&self, slots: &[InflightReaddir], min_completions,
      timeout_ms)` — generalizes `wait_for_rpc_completion` across N
      slots; returns count completed; 0 on timeout (no error).
- [ ] `pub fn resolve_path_to_fh(&self, path) -> NfsResult<Vec<u8>>` —
      lift the LOOKUP body of `readdir_plus_with_fh` (lines ~391-411).
- [ ] Do NOT call `drain_pending_events` in `submit` or `pump`
      (§6 edge case 1).
- [ ] Edge case: `pump` with empty slot list returns 0; with all
      already-completed returns count without entering loop.
- [ ] Edge case: `rpc_which_events == 0` with uncompleted slots →
      service revents=0, sleep 10ms, retry (mirror existing logic).
- [ ] Existing `readdir_plus_by_fh` and `readdir_plus_with_fh`
      unchanged — legacy `worker_loop` still uses them.

## Step 3 — Layer 2: `worker_loop_pipelined`

In `src/walker/simple.rs`:

- [ ] Lift the `process_entries` closure (current
      `simple.rs:1074-1164`) into a free function shared by both
      paths. Behavior identical to legacy. **If lifting causes any
      behavior delta, duplicate the body inline in the pipelined
      worker instead** so legacy stays bit-for-bit identical.
- [ ] Add `struct DirState { work, cookie, cookieverf }`.
- [ ] Add `worker_loop_pipelined(...)` mirroring §4 sketch.
  - [ ] Refill empty slots from local/injector/stealers.
  - [ ] If `work.file_handle` is None, fall back to
        `nfs.resolve_path_to_fh(&work.path)` (§6 edge 6).
  - [ ] `nfs.pump(&slots, 1, POLL_STEP_MS * 5)`.
  - [ ] Drain completed slots in reverse-iter, emit entries, advance
        cookie, re-submit on non-EOF.
  - [ ] On EOF: `dirs_count++`, `pending_work--`.
  - [ ] On error: `errors_count++`, `pending_work--`, drop slot.
  - [ ] Termination: `pending_work==0 && active_workers==0 &&
        slots.is_empty()` → break.
  - [ ] Drain remaining batch on exit.
  - [ ] `dirs_only`, `max_depth`, `compute_checksum`, `detect_file_type`
        all honored (parity with legacy).
- [ ] Branch in `run_workers` based on
      `self.config.pipeline_depth > 0` to dispatch to either worker
      function. Setup (deque, stealers, DNS round-robin, counters)
      shared.
- [ ] **Legacy `worker_loop` MUST remain unchanged** — verify with
      `git diff src/walker/simple.rs` after the edit.

## Step 4 — Tests

Unit tests (`src/nfs/connection.rs`, `#[cfg(test)]`):

- [ ] `pipelined_submit_two_completes_both`.
- [ ] `pipelined_drop_with_inflight_does_not_segfault`.
- [ ] `pipelined_cookie_chain_advances_correctly` (5000-entry dir).
- [ ] `pipelined_one_slow_dir_does_not_block_others` (best-effort).
- [ ] All gated to skip cleanly if no loopback NFS server is reachable
      (matches the pattern of the existing tests at the bottom of
      `connection.rs`).

Integration test:

- [ ] `tests/pipelined_walker_test.rs`: walk synthetic tree (1000 ×
      100), assert equal DbEntry count, path set, sampled attributes
      between depths 0 and 8.

## Step 5 — Build, unit test, draft PR

- [ ] `cargo build --all-features` clean (no new warnings).
- [ ] `cargo test` (unit) green.
- [ ] `git diff` confirms legacy `worker_loop` body unchanged.
- [ ] Open **draft** PR. Request review. STOP — do not run §8
      benchmark until review feedback is in.

## Step 6 — §8 benchmark (after review approves)

- [ ] Workload 1 (wide shallow): pipelined ≥ 4× baseline files/sec.
- [ ] Workload 2 (narrow deep): pipelined ≥ 0.95× baseline.
- [ ] Workload 3 (one-mega-dir mix): pipelined ≥ 2× baseline.
- [ ] If any miss → do not merge; profile.

## Files touched

- `src/config.rs` (+pipeline_depth field, validation).
- `src/error.rs` (+InvalidPipelineDepth variant).
- `src/nfs/connection.rs` (+~150 lines: primitives + helper).
- `src/walker/simple.rs` (+~250 lines: pipelined worker + branch).
- `tests/pipelined_walker_test.rs` (new).
- `README.md` (one paragraph under Performance — defer to PR).

## Review section

(filled in after implementation)
