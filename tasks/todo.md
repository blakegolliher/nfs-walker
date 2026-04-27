# Working Plan — In-Flight Commits, Then Tier 1A + Tier 2C

Generated 2026-04-27. Drives the work described in
`in-flight-changes-to-commit.md` and `parquet-and-summaries-plan.md`.
This file is the canonical checklist; the other two are reference.

---

## Big problems found during planning (read first)

These are gaps and risks the source plans **did not call out** that will
bite us mid-implementation if we do not handle them up front.

### P1 — Pre-existing WIP is more than "help text"

The triage doc lists `src/config.rs` pre-existing WIP as just a
description rewrite. The actual diff also changes
`NfsUrl::split_export_path()` to treat the **entire path as the export**
instead of splitting on the first `/`. Two unit tests were rewritten to
match. This is a behavior change — anything that consumed `url.subpath`
separately from `url.export` gets a different result now.

**Action:** treat the URL-parsing change as its own logical commit, not
"just docs." Verify it lines up with the rest of the codebase's expectation
(I'm calling it intentional based on the long-about comment, but flag it).

### P2 — RocksDB compression + writer-path FD cap is undocumented WIP

`src/rocksdb/schema.rs` includes two unflagged changes:

- `entries_cf_options()` switched from single-LZ4 to per-level (LZ4 for
  L0/L1, Zstd for L2+). Output SSTs differ from older DBs. RocksDB reads
  mixed-compression fine, so forward-compat — but new scans will not
  bit-identical to old ones.
- `get_db_options()` adds `set_max_open_files(4096)`. This is a **writer-path
  FD cap** that is the dual of Group A's `get_query_options()`'s
  `max_open_files=-1`.

These three changes form one coherent unit:
- writer cap = 4096 (this WIP)
- query cap = -1 (Group A)
- soft RLIMIT raised to 1M (Group B)

Splitting them across commits with no message linkage will look like
unrelated tweaks. The Group B commit message must reference the writer
cap to make the rationale coherent.

### P3 — Two parallel writer implementations; the parquet plan touches the wrong one without acknowledging it

`src/rocksdb/writer.rs::rocks_writer_loop` and
`src/walker/simple.rs::rocksdb_writer_loop` are siblings with the same
purpose. The walker uses the simple.rs version exclusively; writer.rs
only contributes `RocksWriter::open` (DB initialization). The
write-batch logic in writer.rs is dead code on the hot path.

The parquet plan correctly says "modify rocksdb_writer_loop in
simple.rs" but adds the new methods (`update_summary`, `flush_summary`,
`SummaryAccumulator`) into `RocksWriter` (writer.rs). If we follow it
literally, the methods are added but never called during a real scan.

**Action:** put `SummaryAccumulator` in a new module (e.g.,
`src/rocksdb/summary.rs`), and call `update_summary` directly from
`rocksdb_writer_loop` in simple.rs. Optionally clean up the dead code in
writer.rs in a separate commit (not in scope for either PR here).

### P4 — Summary visibility under `--live` is gated by the 1M-entry flush, not the 100-batch flush

WAL is disabled. Secondary mode only sees data after the primary calls
`db.flush()`. The plan's 100-batch `flush_summary()` writes summary keys
to the memtable but does **not** flush — only the existing 1M-entry
periodic `db.flush()` makes those keys visible to secondary readers.

So: under `--live`, the summary CF lags by up to ~1M entries (~1 minute
on this workload). That's actually fine for the user goal ("instant
queries during the scan"), but the plan's "every 100 batches" cadence
contributes nothing to liveness; it just bounds memory in the
accumulator. Document this clearly in the commit and the docs section.

### P5 — Backward-compat fallback should use `DB::list_cf`, not error-message matching

The plan says "try with summary CF first; if fails, retry without."
String-matching on rocksdb error messages is fragile across crate
versions. Use `DB::list_cf(&opts, &path)` to enumerate existing CFs and
open with the intersection. Standard pattern in the rocksdb-rs
ecosystem.

### P6 — Parquet fan-out chokepoint risks ingest stalls

The plan routes batches workers → entry_tx → rocks-writer → parquet_tx
→ parquet-writer. If the parquet writer falls behind, the rocks-writer
blocks on `parquet_tx.send()` and propagates back to entry_tx and the
workers — i.e. **ingest stalls on parquet backpressure**.

The plan acknowledges "fall back to blocking send" but does not call
out that a stall is the consequence. Acceptance test before claiming
done: measure parquet write throughput vs RocksDB write throughput on a
synthetic 10M-entry feed. If parquet < rocks, we need a fix:
- option (a): drop on backpressure, increment a `parquet_drops` metric
  and surface it in finalize stats (data loss; user-visible — probably
  the right call given the streaming use case is "best-effort live
  queries");
- option (b): wider channel (e.g. bounded(1000)) — buys headroom but
  doesn't solve a sustained imbalance.

Decision: implement (a) under a "drop-on-pressure" mode (default on for
streaming) and document it. Do NOT silently stall ingest.

### P7 — scan_id at scan start collides with post-scan convert

If we set scan_id in metadata at scan start, then the existing
`convert_rocks_to_parquet` would reuse it. But streaming parquet during
the scan ALSO writes to `<output>/scans/<scan_id>/`. Two writers, one
directory — a post-scan convert run after a streamed scan should
either:
- refuse (return an error: "streamed parquet already exists for scan_id
  X, delete it or pass --force");
- or no-op (detect the streamed dir and exit successfully).

Pick the refuse-with-message option; it's safer.

### P8 — `tasks/` is untracked but expected to be tracked

`CLAUDE.md` references `tasks/todo.md` and `tasks/lessons.md` as
canonical locations, so the directory is intended to live in the repo.
Do NOT add it to `.gitignore`. Commit it as-is once the planning files
have settled.

---

## Stage 1 — Commit the in-flight working tree

Goal: get to a clean working tree with no mixed-intent files, so the
parquet/summary work has a reviewable diff baseline.

Order matters because some files have all three layers mixed.

### 1.1 Pre-existing WIP cluster — commit on its own

Files (use `git add -p` to stage only these hunks):

- [ ] `Cargo.toml` — `default = ["rocksdb", "csv-export"]` and
      description rewrite.
- [ ] `Makefile` — help-text update only.
- [ ] `README.md` — only the parts that aren't `--live`-related.
- [ ] `src/config.rs` — `about=`/`long_about=`/`after_help=` rewrite,
      `Convert`/`ExportParquet`/`ExportCsv`/`Stats` doc-string tweaks,
      AND the `NfsUrl::split_export_path` behavior change (with its two
      unit tests).
- [ ] `src/nfs/connection.rs` — `nfs3_status_to_string()` +
      `nfs3_status_to_nfs_error()` helpers and their use sites.
- [ ] `src/walker/simple.rs` — `Permission denied` debug log addition
      in `worker_loop` and `big_dir_worker_loop`.
- [ ] `src/rocksdb/schema.rs` — per-level compression in
      `entries_cf_options()` and `set_max_open_files(4096)` in
      `get_db_options()`.

This is a chunky commit. Splitting it further is reasonable if the user
prefers (e.g. URL parsing change separate from compression change),
but bundling is fine since they're all "previous-session WIP."

Suggested message:
```
WIP: bundle pre-session changes (URL parsing, compression, FD cap, NFS error strings)

Picked up uncommitted-from-prior-session changes that don't fit the
--live or RLIMIT_NOFILE work:

- NfsUrl::split_export_path now treats the full path as the export
  rather than splitting on the first slash. Multi-component exports
  like /volumes/<uuid> are common; the auto-split was wrong as often
  as right. Tests updated.
- entries_by_path/inode CFs use per-level compression: LZ4 for L0/L1
  (frequent compaction), Zstd for L2+ (bulk of the data, compacted
  rarely). Roughly halves on-disk size on representative workloads.
- get_db_options() caps max_open_files at 4096 on the writer path. The
  query path overrides this back to -1 (see --live work) since
  read-only/secondary mode requires every referenced SST to stay
  pinned for the duration of a scan iteration.
- nfs3_status_to_string + nfs3_status_to_nfs_error translate libnfs
  status codes into typed errors and readable messages instead of
  bare numbers in log output.
- Minor: Permission denied READDIRPLUS errors logged at debug rather
  than warn (matches existing NotFound treatment).
- Minor: Cargo default features include csv-export so a default build
  ships the export-csv subcommand.
- Minor: Makefile/README help-text refresh.
```

### 1.2 Group A — `--live` + inode-CF perf

Files (session-only hunks):

- [ ] `src/rocksdb/schema.rs` — `OpenMode`, `get_query_options()`,
      `open_rocks_db_secondary()`, `default_secondary_path()`,
      `RocksHandle::open_secondary()`,
      `RocksHandle::try_catch_up_with_primary()`,
      `RocksHandle::iter_by_inode()`.
- [ ] `src/rocksdb/mod.rs` — re-export `OpenMode`.
- [ ] `src/rocksdb/stats.rs` — `open_query_handle()` helper, `OpenMode`
      param on all 11 stats functions, switch 9-of-11 to
      `iter_by_inode()`.
- [ ] `src/config.rs` — `live: bool` field on `Stats` subcommand only.
- [ ] `src/main.rs` — thread `OpenMode` through `run_stats`.
- [ ] `README.md` — `--live` callout.
- [ ] `docs/QUERY_ROCKSDB.md` — "Live Querying During an Active Scan"
      section.

Use the suggested message from `in-flight-changes-to-commit.md` §
"Group A".

### 1.3 Group B — `raise_fd_limit()` + `MAX_WORKERS=4096`

Files:

- [ ] `src/main.rs` — `raise_fd_limit()` function and call site in
      `run()`.
- [ ] `src/config.rs` — `MAX_WORKERS` 512 → 4096 + comment.

The commit message should reference the writer-path 4096 cap committed
in 1.1 and the query-path -1 from 1.2 to make the trio coherent.

### 1.4 Commit `tasks/`

- [ ] `git add tasks/` and commit:
  - `parquet-and-summaries-plan.md`
  - `in-flight-changes-to-commit.md`
  - `todo.md` (this file)
  - `lessons.md` (create with header if not present, per CLAUDE.md)

### 1.5 Tag baseline

- [ ] `git tag pre-summary-cf-work` so the new work has a clear diff
      target.

### 1.6 Verification before moving on

- [ ] `cargo build --all-features` clean
- [ ] `cargo test --all-features` clean (note: test_parse_nfs_url_with_subpath
      was rewritten — verify it passes)
- [ ] `git status` clean
- [ ] Smoke test: scan a small filesystem, run `nfs-walker stats <db>`
      (read-only path) and `nfs-walker stats <db> --live` (secondary
      path) — both succeed.

---

## Stage 2 — PR #1 (Tier 1A): summary CFs

Plan source: `parquet-and-summaries-plan.md` § "Tier 1A".

Implementation order chosen so each step compiles and tests cleanly:

### 2.1 Schema

- [ ] Create `src/rocksdb/summary.rs` (preferred over inline in
      schema.rs; keeps schema.rs from growing further).
- [ ] Add `SummaryTotal`, `ExtCounters`, `OwnerCounters`,
      `FileTypeCounters`, `SummaryAccumulator` with bincode `Serialize`/
      `Deserialize`.
- [ ] Provide `SummaryAccumulator::update(&mut self, entries: &[DbEntry])`
      and `SummaryAccumulator::serialize_kv() -> Vec<(Vec<u8>, Vec<u8>)>`
      returning the 5 (key, value) pairs.
- [ ] Provide reverse: `SummaryReader { total, by_extension, by_uid,
      by_gid, by_file_type }` with `SummaryReader::load(handle: &RocksHandle)
      -> Result<Option<Self>, RocksError>` that returns None if the CF
      is missing.

### 2.2 Schema CF wiring

- [ ] Add `CF_SUMMARY = "summary"` constant.
- [ ] Add `summary_cf_options()` (small CF, low memory).
- [ ] Update `open_rocks_db()` to include CF_SUMMARY in CF descriptors.
- [ ] Update `open_rocks_db_readonly()` and `open_rocks_db_secondary()`
      to use `DB::list_cf` and intersect with `[ENTRIES_BY_PATH,
      ENTRIES_BY_INODE, METADATA, BIG_DIRS, SUMMARY]` so old DBs
      without SUMMARY still open.
- [ ] `RocksHandle::cf_summary() -> Option<&ColumnFamily>` (None for old DBs).

### 2.3 Writer wiring (in simple.rs, NOT writer.rs)

- [ ] In `rocksdb_writer_loop` (simple.rs:1221), instantiate a
      `SummaryAccumulator`.
- [ ] After each `write_rocks_batch`, call
      `accumulator.update(&pending)` BEFORE clearing pending.
- [ ] Maintain `batches_since_summary_flush` counter; every N (=100)
      successful writes, serialize the accumulator and write its 5 keys
      via WriteBatch to CF_SUMMARY (no `db.flush()`; keep it in
      memtable like the rest).
- [ ] At the existing 1M-entry FLUSH_INTERVAL, the periodic
      `db.flush()` is what makes summary visible to secondaries — no
      change needed.
- [ ] At end of loop (after the residual `pending` write), serialize +
      write summary one final time, THEN `db.flush()`. Both the
      entries and the summary are durable on a clean shutdown.

### 2.4 Reader wiring (stats.rs)

- [ ] Helper `try_load_summary(handle: &RocksHandle) -> Option<SummaryReader>`.
- [ ] `compute_stats` short-circuits when summary present:
      `total/files/dirs/symlinks/bytes/blocks/max_depth` from
      `SummaryTotal`.
- [ ] `stats_by_extension`, `stats_by_uid`, `stats_by_gid`,
      `stats_by_file_type` short-circuit similarly.
- [ ] On miss, log
      `"summary CF not available, falling back to full scan"` and use
      the existing iteration path unchanged.

### 2.5 Tests

- [ ] Unit test in `summary.rs`: round-trip `SummaryAccumulator` →
      bytes → `SummaryReader`, equality.
- [ ] Integration test in `stats.rs` (or a new `tests/summary.rs`):
      build a small RocksDB with 100 mixed entries; run `compute_stats`
      with summary present and verify it equals the same call without
      summary (delete the CF or open an old-style DB).
- [ ] Test that a DB written by an old binary (CF set without SUMMARY)
      still opens and falls back gracefully — bake a small fixture or
      programmatically open without CF_SUMMARY then re-open.

### 2.6 Verification

- [ ] `cargo build --all-features --release`
- [ ] `cargo test --all-features` clean
- [ ] Microbench: feed 10M synthetic `DbEntry`s through the writer
      pipeline with and without summary; record entries/sec. Within 1%
      means we're done.
- [ ] Live run: scan ~1M entries; while still scanning, run
      `nfs-walker stats <db> --live --by-extension` and confirm it
      returns in < 1s.

---

## Stage 3 — PR #2 (Tier 2C): streaming parquet

Plan source: `parquet-and-summaries-plan.md` § "Tier 2C".

### 3.1 Refactor row-builders

- [ ] Create `src/parquet/builder.rs` and move all 17 builder slots +
      the per-entry append loop + `finish_batch` out of `convert.rs`.
- [ ] Public surface: `RowBuilder::new(scan_id: &str, scan_ts_us: i64)`,
      `RowBuilder::push(&mut self, entry: &RocksEntry)`,
      `RowBuilder::push_db_entry(&mut self, entry: &DbEntry)`,
      `RowBuilder::finish(&mut self) -> RecordBatch`,
      `RowBuilder::row_count(&self) -> usize`.
- [ ] `convert.rs` now calls `RowBuilder` methods. No behavior change.
- [ ] Run all parquet tests; confirm output bit-identical (or
      structurally identical — Parquet's metadata may differ trivially).

### 3.2 StreamingParquetWriter

- [ ] `src/parquet/streaming.rs`: `StreamingParquetConfig` (fields per
      plan), `StreamingParquetWriter::open`,
      `StreamingParquetWriter::write_batch(&[DbEntry])`,
      `StreamingParquetWriter::close`.
- [ ] Each part written to `.part-NNNN.parquet.tmp`, `fsync`'d on close,
      atomically renamed to `part-NNNN.parquet`.
- [ ] On rotation (current part exceeds `target_file_size`), close
      current writer, open the next.
- [ ] `close()` finalizes the current writer, returns the part count.

### 3.3 Pipeline wiring

- [ ] Add `parquet_tx`/`parquet_rx` channel in `simple.rs::run`-equivalent
      RocksDB path (line ~300).
- [ ] `spawn_parquet_writer` mirrors `spawn_rocksdb_writer`: thread
      consumes batches, writes via `StreamingParquetWriter`, returns
      part count and bytes.
- [ ] Modify `rocksdb_writer_loop` to forward each batch (after the
      RocksDB write succeeds) to `parquet_tx`. Use `try_send`; on a
      full channel, drop the batch and increment a
      `parquet_drops_total` counter (NOT block — see P6). Log a warn
      when drops > 0 at end of loop.
- [ ] On scan completion, log final part count and any drops.

### 3.4 CLI + WalkConfig

- [ ] Add `--stream-parquet` flag to `CliArgs` (top-level, no
      subcommand).
- [ ] Wire through `WalkConfig` (new bool field) → `simple.rs::run` →
      `spawn_parquet_writer` decision.

### 3.5 scan_id at scan start

- [ ] Add `meta_keys::SCAN_ID` constant.
- [ ] In `spawn_rocksdb_writer` (simple.rs:813), generate a UUID,
      `set_metadata(SCAN_ID, ...)` before spawning the thread, and pass
      the scan_id into both writer threads.
- [ ] In `convert_rocks_to_parquet`, prefer the SCAN_ID from metadata
      if present, fall back to fresh UUID.
- [ ] On post-scan convert, refuse with a clear error if
      `<output>/scans/<scan_id>/` already exists (per P7).

### 3.6 Docs

- [ ] `docs/QUERY_ROCKSDB.md` "Live DuckDB queries" section with
      example glob and queries.
- [ ] `README.md` one-liner pointing at `--stream-parquet` and DuckDB.

### 3.7 Tests + verification

- [ ] End-to-end test: tiny scan → `--stream-parquet` enabled → verify
      `<output>.parquet/scans/<id>/part-*.parquet` exists, row count
      matches, schema matches `parquet_schema()`.
- [ ] Microbench (per P6): synthetic 10M-entry feed with parquet on,
      measure entries/sec and `parquet_drops_total`. Acceptance: rocks
      throughput within 5% of baseline; drops zero or close to it on a
      well-provisioned host.
- [ ] Live: real scan with both `--stream-parquet` and a concurrent
      DuckDB query; verify row count climbs.
- [ ] Confirm `--live` overview still works (summary CF) on a streamed
      DB — they're independent code paths but should not interact.

---

## Out of scope (do NOT do)

- Removing or refactoring `src/rocksdb/writer.rs` dead path. Note for
  follow-up: that file's `RocksWriter::write_batch` is no longer called
  during scans; clean up in a separate PR.
- DataFusion server changes; it already queries parquet dirs.
- Compacting many small parquet files at scan end.
- Sampling CFs, checkpoints, materialized aggregations.
- Changing `RocksEntry` fields (would break old DBs).

---

## Review section (filled in as work completes)

_Tracker: update on each commit / PR landing._

- [ ] 1.1 Pre-existing WIP cluster
- [ ] 1.2 Group A `--live` + inode-CF
- [ ] 1.3 Group B FD limit + worker cap
- [ ] 1.4 `tasks/` committed
- [ ] 1.5 Tag `pre-summary-cf-work`
- [ ] 2 PR #1 Tier 1A summary CFs
- [ ] 3 PR #2 Tier 2C streaming parquet
