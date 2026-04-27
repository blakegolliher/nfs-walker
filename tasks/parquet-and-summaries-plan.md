# Tier 1A + Tier 2C — Make Billion-Entry Scans Useful

## Why this exists

Today an `nfs-walker stats` overview on a 1B-entry RocksDB takes 19 minutes
of full iteration (~930K rows/sec). Group-by queries take similar wall time.
At 4–5B entries (the user's actual workload) the overview will hit 80+
minutes. That makes the data effectively unusable for ad-hoc analysis until
the scan completes hours later — and even then full iteration is the wrong
tool for SQL-style analytics.

This plan delivers two complementary improvements:

- **Tier 1A**: Maintain pre-computed summary aggregates inside RocksDB
  during ingest. Overview and common group-by queries become **single-key
  lookups (<1s) regardless of DB size**.
- **Tier 2C**: Stream a parallel rolled-Parquet directory during ingest so
  DuckDB / DataFusion can run arbitrary SQL queries against the scan
  while it is still running, with **seconds-long latency**.

Constraint: **do not slow ingest in any measurable way.** Workers must stay
on the NFS hot path. All extra work happens on dedicated writer threads.

## Background that the implementer needs to know

### Existing architecture (do not change)

- `src/walker/simple.rs` runs N worker threads (default 512). Each worker
  walks NFS via READDIRPLUS and pushes batches of `Vec<DbEntry>` to a
  bounded channel `entry_tx` of capacity 100 (`simple.rs:307`).
- A single `rocksdb-writer` thread (`spawn_rocksdb_writer`,
  `simple.rs:813`) owns a `RocksWriter`, consumes batches from
  `entry_rx`, and calls `write_batch` per batch. Each `write_batch`
  writes the entry twice (once to `entries_by_path` CF, once to
  `entries_by_inode` CF). See `src/rocksdb/writer.rs:65-95`.
- WAL is disabled by default (`RocksWriterConfig::default()` →
  `disable_wal: true`) for ingest speed.

### Existing parquet code (reuse, do not duplicate)

- `src/parquet/schema.rs` — `parquet_schema()` returns the canonical 18-
  column Arrow schema. **Use this as-is.** Includes `path`, `filename`,
  `extension`, `inode`, `file_type` (string), `size`, `allocated_blocks`,
  `nlink`, `uid`, `gid`, `permissions`, `mtime_us`, `atime_us`,
  `ctime_us`, `depth`, `parent_path`, `scan_id`, `scan_timestamp_us`.
- `src/parquet/convert.rs` — `convert_rocks_to_parquet` does a *post-scan*
  RocksDB → Parquet export. Read it carefully: it shows exactly how to
  build `RecordBatch`es from `RocksEntry`/`DbEntry` and write them via
  `arrow::ArrowWriter` with ZSTD compression. The new continuous-export
  code should reuse the same row-building helpers, not reinvent them.
- `ExportConfig` defaults: `row_group_size=1_000_000`, `target_file_size
  =256 * 1024 * 1024`, `compression_level=3`. Reuse these defaults for
  the rolling exporter.

### Existing stats query path

- `src/rocksdb/stats.rs` — 11 functions, each takes a path + `OpenMode`
  and iterates either `iter_by_path()` or `iter_by_inode()`. After this
  plan, the cheap ones short-circuit through summary CFs and only fall
  back to iteration on cache miss.
- `src/main.rs:run_stats` (around line 137-355) is the CLI dispatcher.

### Schema constraints

- The `RocksEntry` struct (`src/rocksdb/schema.rs:50`) is the canonical
  in-RocksDB representation. **Do not modify its fields.** Adding a new
  CF is fine; modifying RocksEntry would break readers of existing DBs.
- Column families are listed in `src/rocksdb/schema.rs` constants
  (`CF_ENTRIES_BY_PATH`, `CF_ENTRIES_BY_INODE`, `CF_METADATA`,
  `CF_BIG_DIRS`). Adding a new CF (e.g., `CF_SUMMARY`) requires
  updating both `open_rocks_db()` and `open_rocks_db_readonly()` /
  `open_rocks_db_secondary()` and adding a CF descriptor.

---

# Tier 1A: Running summary CFs (DO THIS FIRST)

## Design

Add a single new column family `summary` with one bincode-encoded value
per dimension. Update it incrementally in the writer thread after each
batch. Stats functions check for the key first and return instantly on
hit.

### New CF: `summary`

Keys (all bytes, dot-separated namespacing for readability):

| Key | Value (bincode struct) |
|---|---|
| `total` | `SummaryTotal { entries, files, dirs, symlinks, bytes, blocks, max_depth, last_updated_us }` |
| `by_extension` | `BTreeMap<String, ExtCounters>` |
| `by_uid` | `BTreeMap<u32, OwnerCounters>` |
| `by_gid` | `BTreeMap<u32, OwnerCounters>` |
| `by_file_type` | `BTreeMap<String, FileTypeCounters>` |

Where:
```rust
pub struct ExtCounters { pub count: u64, pub bytes: u64, pub blocks: u64 }
pub struct OwnerCounters { pub file_count: u64, pub dir_count: u64, pub bytes: u64 }
pub struct FileTypeCounters { pub count: u64, pub bytes: u64 }
```

Notes:
- `BTreeMap` (not `HashMap`) for deterministic serialization across runs.
- Keep summaries in memory in the writer thread; flush to RocksDB
  periodically (see flush trigger below).
- These are *running* totals, computed from per-path entries. They count
  every name (i.e., hardlinks count N times). This matches existing
  `iter_by_path` semantics, not the inode-CF dedup.

### Writer-side wiring

In `src/rocksdb/writer.rs`:

1. Add a `SummaryAccumulator` struct holding the in-memory counters.
2. Add a method `RocksWriter::update_summary(&mut self, entries: &[DbEntry])`
   that updates the accumulator without any disk writes.
3. Add a method `RocksWriter::flush_summary(&self) -> Result<(), RocksError>`
   that bincode-serializes the accumulator and writes 5 keys to the
   `summary` CF in a single `WriteBatch`.

In the writer loop (`rocksdb_writer_loop` in `src/walker/simple.rs`):

1. After `writer.write_batch(&batch)?`, call
   `writer.update_summary(&batch)`.
2. Maintain a counter: `batches_since_flush`.
3. Every `SUMMARY_FLUSH_EVERY_N_BATCHES` batches (default: 100 — about
   every ~50K-500K entries depending on batch size) call
   `writer.flush_summary()`.
4. Always `flush_summary()` once at end of scan, in
   `finalize_rocks_db` or right before it.

The flush is dirt cheap: 5 small key-value writes per flush, no disk
sync (we already have WAL disabled). No measurable ingest impact.

### Reader-side wiring

In `src/rocksdb/stats.rs`:

1. Add a `try_load_summary<T>(handle, key) -> Option<T>` helper that
   attempts to read a summary key and bincode-decode. Returns `None` on
   missing key or decode error (DBs from older binaries won't have the
   summary CF).
2. In `compute_stats`, `stats_by_extension`, `stats_by_uid`,
   `stats_by_gid`, `stats_by_file_type`: at the top of each function,
   call `try_load_summary`. On hit, return the summarized result. On
   miss, fall back to the existing iteration code unchanged.
3. Print a one-line note when falling back to iteration:
   `"summary CF not available, falling back to full scan (this may take a while)"`.

### Behavior with `--live` (secondary mode)

Secondary mode replays the MANIFEST + WAL up to the last primary flush.
Since WAL is disabled by default, the secondary will only see summary
keys that were *flushed* by the primary (i.e., the periodic
`flush_summary` calls). That's fine — the summary will lag the spinner
counter by up to one flush interval, which is exactly the trade we want
for "instant" queries.

### Backward compatibility

Old DBs (without the `summary` CF) must still be readable. When opening
read-only/secondary, if the `summary` CF is missing, RocksDB returns an
error from `open_cf_*`. Two options:

- **Option A (preferred):** when opening, try with the summary CF
  first; if that fails, retry with the legacy 4-CF set. Handle the CF-
  missing case gracefully.
- **Option B:** add `summary` CF on first read of an old DB by opening
  read-write briefly. Riskier, complicates the read path.

Implement Option A.

### Tasks (in order)

- [ ] 1.1 Add `SummaryAccumulator` and value structs in
  `src/rocksdb/schema.rs` (or new `src/rocksdb/summary.rs` module if it
  keeps schema.rs clean).
- [ ] 1.2 Add `CF_SUMMARY` constant; wire it into
  `open_rocks_db`, `open_rocks_db_readonly`, `open_rocks_db_secondary`.
- [ ] 1.3 Add backward-compat fallback for opening DBs without
  `summary` CF (Option A above).
- [ ] 1.4 Add `RocksWriter::update_summary` and `flush_summary`.
- [ ] 1.5 Modify `rocksdb_writer_loop` to call `update_summary`/
  `flush_summary` per the plan above.
- [ ] 1.6 Modify `finalize_rocks_db` to call `flush_summary` once at end.
- [ ] 1.7 Add `try_load_summary` helper to `src/rocksdb/stats.rs`.
- [ ] 1.8 Modify `compute_stats`, `stats_by_extension`, `stats_by_uid`,
  `stats_by_gid`, `stats_by_file_type` to short-circuit through summary.
- [ ] 1.9 Tests: round-trip summary write+read in `src/rocksdb/writer.rs`
  test module. Add a test that compares summary-derived counts against
  iteration-derived counts on a small DB.

---

# Tier 2C: Continuous Parquet rotation during ingest

## Design

Add a parallel writer thread that consumes the same `Vec<DbEntry>`
batches as the RocksDB writer, accumulates them into Parquet
`RecordBatch`es, and rotates to a new file every `target_file_size`
bytes. Files land in `<output>.parquet/scans/<scan_id>/part-NNNN.parquet`.
DataFusion / DuckDB can query the directory immediately as files appear.

### Writer-side wiring

#### Channel topology

The current topology is:
```
workers (×N) ──► entry_tx (bounded(100)) ──► rocksdb-writer
```

Change to:
```
workers (×N) ──► entry_tx (bounded(100)) ──► rocksdb-writer
                                                   │
                                                   ▼
                                          parquet_tx (bounded(100))
                                                   │
                                                   ▼
                                            parquet-writer
```

The RocksDB writer, after writing a batch, forwards the (now-owned)
batch to `parquet_tx` via `try_send`. If the parquet channel is full
(parquet writer is behind), the rocks writer falls back to a blocking
`send` — in practice this should be rare because Parquet write throughput
exceeds RocksDB write throughput on this workload.

**Why fan-out from the rocks writer rather than tee from workers:**
the bounded entry channel is MPMC (crossbeam_channel), but each batch
goes to only ONE consumer. Teeing from workers would either require
a second `try_send` per batch (extra hot-path cost) or replacing
`Vec<DbEntry>` with `Arc<Vec<DbEntry>>` (wider refactor). Forwarding
from the rocks writer keeps the worker hot path identical.

#### New module: `src/parquet/streaming.rs`

```rust
pub struct StreamingParquetConfig {
    pub output_dir: PathBuf,    // <output>.parquet/scans/<scan_id>/
    pub row_group_size: usize,  // default 1_000_000
    pub target_file_size: usize, // default 256 * 1024 * 1024
    pub compression_level: i32,  // default 3
    pub scan_id: String,
    pub scan_timestamp_us: i64,
    pub source_url: String,
}

pub struct StreamingParquetWriter {
    config: StreamingParquetConfig,
    current_writer: Option<ArrowWriter<File>>,
    current_path: Option<PathBuf>,
    current_bytes_written: u64,
    part_number: u32,
}

impl StreamingParquetWriter {
    pub fn open(config: StreamingParquetConfig) -> Result<Self, WalkerError>;
    pub fn write_batch(&mut self, entries: &[DbEntry]) -> Result<(), WalkerError>;
    pub fn close(self) -> Result<u32 /* total parts written */, WalkerError>;
}
```

The internals reuse `convert_rocks_to_parquet`'s helpers (build
`RecordBatch` from entries, writer properties with ZSTD). Refactor those
helpers from `src/parquet/convert.rs` into a shared place
(`src/parquet/builder.rs`) so both the post-scan converter and the
streaming writer call the same row-building code. **Do not duplicate.**

#### Writer thread

Add `spawn_parquet_writer` in `src/walker/simple.rs` mirroring the
shape of `spawn_rocksdb_writer`. It owns a `StreamingParquetWriter`,
consumes from `parquet_rx`, calls `write_batch`, and `close()`s on
channel disconnect.

### CLI surface

Add a new top-level scan flag in `src/config.rs::CliArgs`:

```rust
/// Stream a rolled Parquet directory alongside RocksDB during the scan.
/// Files land in <output>.parquet/scans/<scan_id>/part-NNNN.parquet.
/// Slightly increases ingest CPU/disk load (~1-3% in worst case) but
/// enables ad-hoc DuckDB / DataFusion queries during the scan.
#[arg(long)]
pub stream_parquet: bool,
```

When set, walker spawns the parquet writer thread in addition to the
RocksDB writer.

### Output layout

```
/mnt/local-nvme/figure.rocks/             # the existing RocksDB
/mnt/local-nvme/figure.rocks.parquet/     # new sibling directory
└── scans/
    └── <scan_id>/                        # uuid, also written into RocksDB metadata
        ├── part-00000.parquet
        ├── part-00001.parquet
        └── ...
```

Why sibling and not nested: keeps the RocksDB directory clean and lets
RocksDB compaction / FD management remain unaffected. The `scan_id` is
the same UUID that `convert_rocks_to_parquet` already generates — write
it into RocksDB metadata at scan start so the post-scan converter can
reuse it (or skip the convert step entirely if streaming was on).

### Crash safety

Each rolled `part-NNNN.parquet` is written to a temp name first
(`.part-NNNN.parquet.tmp`) and renamed atomically on close. The
currently-being-written file at crash time is left as a `.tmp` — DuckDB
glob queries (`scans/<id>/part-*.parquet`) skip it naturally. A
`fsck` step (or simply ignoring `.tmp` files) handles cleanup.

### Documentation

Update `docs/QUERY_ROCKSDB.md` to reference DuckDB usage on the streamed
parquet directory:

```bash
# Once the scan is running with --stream-parquet, query live with DuckDB:
duckdb -c "SELECT extension, COUNT(*), SUM(size)
           FROM read_parquet('/mnt/local-nvme/figure.rocks.parquet/scans/*/part-*.parquet')
           GROUP BY extension ORDER BY 3 DESC LIMIT 20"
```

### Tasks (in order)

- [ ] 2.1 Refactor row-building helpers out of `src/parquet/convert.rs`
  into `src/parquet/builder.rs`. Both convert.rs and the new streaming
  writer call them.
- [ ] 2.2 Implement `StreamingParquetWriter` in
  `src/parquet/streaming.rs`. Atomic rename on file close. Returns
  total parts written.
- [ ] 2.3 Add `spawn_parquet_writer` in `src/walker/simple.rs`.
- [ ] 2.4 Modify `rocksdb_writer_loop` to forward batches to the parquet
  channel when streaming is enabled. Use a feature/runtime check;
  don't change behavior when streaming is off.
- [ ] 2.5 Add `--stream-parquet` flag to `src/config.rs`.
- [ ] 2.6 Wire the flag through `WalkConfig` and `simple.rs::run` to
  decide whether to spawn the parquet writer.
- [ ] 2.7 Write `scan_id` to RocksDB metadata at scan start (currently
  generated only at convert time). The post-scan converter should
  reuse this scan_id when present, so RocksDB and streamed Parquet
  share IDs.
- [ ] 2.8 Update `docs/QUERY_ROCKSDB.md` with a "Live DuckDB queries"
  section showing the glob pattern and example queries.
- [ ] 2.9 Update `README.md` querying section with a one-liner pointing
  at the new flag and DuckDB workflow.
- [ ] 2.10 Tests: end-to-end test that runs a tiny scan with
  `--stream-parquet`, verifies parquet files land, and DataFusion (or
  parquet-rs) reads them back with the right row counts.

---

# Validation plan (run before declaring done)

After implementation:

1. **Microbench: ingest impact.** Build a synthetic test (e.g., feed
   N million pre-generated `DbEntry`s through the writer pipeline)
   measuring entries/sec with each combination:
   - baseline (no summary, no parquet)
   - summary only
   - parquet only
   - summary + parquet

   Acceptance: summary-only ingest is within **1%** of baseline.
   Summary + parquet is within **5%** of baseline.

2. **Query speedup: stats wall time.**
   - Build a 10M-entry test DB (small enough to iterate quickly).
   - Run `time nfs-walker stats <db>` (overview) — record wall time
     for full iteration.
   - Run again — should now hit the summary CF and return in <100ms.
   - Run `--by-extension`, `--by-uid`, `--by-gid` — verify they hit
     summary too.

3. **DuckDB live query.**
   - Start a scan with `--stream-parquet` against any reasonably-sized
     filesystem.
   - While running, query the parquet directory with DuckDB:
     `SELECT COUNT(*), SUM(size) FROM read_parquet('.../part-*.parquet')`
   - Verify the row count climbs as new parts are written.

4. **--live still works for non-summarized queries.**
   - `nfs-walker stats <running_db> --largest-files --live` should
     fall through to iteration (no summary key for this dimension)
     and complete without SST errors (the
     `max_open_files=-1` fix is already in place).

---

# Out of scope (do NOT do in this plan)

- DataFusion server changes — it already queries the parquet
  directory via `src/server/`. Once `--stream-parquet` is on, the
  server can point at the streamed dir without changes.
- Compacting many small parquet files — at 256MB target each, a 5B-row
  scan creates ~50-200 parquet files, which DuckDB handles fine.
- Removing RocksDB. Both stores stay; RocksDB still owns
  path/inode lookups for incremental scans and `--live` overview.
- Sampling CF, checkpoints, materialized arbitrary aggregations —
  separate future plans.

---

# Files the implementer will touch

- `src/rocksdb/schema.rs` — new CF_SUMMARY, CF wiring, summary structs.
- `src/rocksdb/writer.rs` — SummaryAccumulator, update/flush methods.
- `src/rocksdb/stats.rs` — try_load_summary, short-circuit branches.
- `src/walker/simple.rs` — modify rocksdb_writer_loop, add
  spawn_parquet_writer, wire scan_id metadata.
- `src/parquet/builder.rs` (new) — shared row-builder helpers extracted
  from convert.rs.
- `src/parquet/streaming.rs` (new) — StreamingParquetWriter.
- `src/parquet/convert.rs` — refactor to call into builder.rs.
- `src/parquet/mod.rs` — re-exports.
- `src/config.rs` — `--stream-parquet` flag.
- `docs/QUERY_ROCKSDB.md` — live DuckDB query docs.
- `README.md` — one-liner pointing at the new workflow.

---

# Order of work / suggested PRs

Two PRs, in this order:

1. **PR #1: Tier 1A summary CFs.** Self-contained, no parquet changes.
   Mergeable on its own. This is the "no measurable cost, huge query
   speedup" change.
2. **PR #2: Tier 2C streaming parquet.** Builds on PR #1 (uses the
   updated writer loop). Adds the `--stream-parquet` opt-in flag.

Do not bundle them. Each is reviewable independently and PR #1 should
ship even if PR #2 needs more iteration.
