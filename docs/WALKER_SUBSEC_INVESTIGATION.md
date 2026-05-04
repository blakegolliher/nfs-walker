# Walker subsecond mtime — investigation report

**Status:** resolved 2026-05-04. The walker source was correct; the
`nfs-walker-parquet-parallel` binary on disk was a stale build (April 27)
that predated the mtime correctness fixes elsewhere in the source tree.

**Resolution:** re-introduced `[[bin]]` entry in `Cargo.toml` and a thin
`src/bin/nfs-walker-parquet-parallel.rs` wrapper around the public
`parallel_convert_rocks_to_parquet` function. The binary now rebuilds from
current source on every `cargo build --release --features parquet`.

---

## Symptom

Reported observation: every mtime in walker's parquet output ended in
`.000000`, despite source files having nanosecond precision mtimes.

## What we proved (and the order)

1. **Source preservation works.** `touch -d '... 12:34:56.789123456' tiny.txt`
   then `stat tiny.txt` returns full nanoseconds. Linux NFS client
   preserves subseconds against the VAST cluster.

2. **Libnfs receives subseconds via stat.** `examples/probe_subsec.rs`
   (since deleted) called `nfs_stat64()` directly via walker's bindings.
   `nfs_mtime_nsec` came back as `789123456`. Server transmits subseconds.

3. **Libnfs receives subseconds via READDIRPLUS.** `examples/probe_readdir.rs`
   (since deleted) called `nfs_opendir()` + `nfs_readdir()` directly and
   read `nfsdirent.mtime_nsec` = `789123456`. Both libnfs read paths
   preserve subseconds correctly.

4. **Walker's `convert_dirent` reads correctly.** Captures `d.mtime_nsec`
   into `NfsStat.mtime` via `timeval_to_micros(sec, nsec)` =
   `pack_micros(sec, nsec)` = `sec * 1_000_000 + nsec / 1000`. Unit tests
   pin this. Function is correct.

5. **DbEntry construction in scan loop preserves microseconds.** Probe at
   `simple.rs:1345` showed `mtime_at_construction = Some(1705322096789123)`
   for tiny.txt. All 17 entries had full-microsecond values.

6. **Walker writer preserves microseconds.** Probes in
   `write_rocks_batch_shard` showed `mtime_in`, `mtime_after_from`, and
   `mtime_after_roundtrip` (full bincode encode + decode) all equal
   `Some(1705322096789123)`. RocksDB stores correct values.

7. **SQLite convert preserves microseconds.** `nfs-walker convert
   m2.rocks /tmp/m2.db` then `SELECT mtime FROM entries` returned
   `1705322096789123`. Convert path is correct.

8. **Parquet exporter on disk produced wrong values.** Same DB, same data,
   `nfs-walker-parquet-parallel` produced `mtime_us = 9223372036854775807`
   (i64::MAX, saturation result of `1705322096789123 * 1_000_000`).

9. **The binary on disk was stale.** `stat` showed Modify time April 27.
   Cargo's `cargo build --release --bin nfs-walker-parquet-parallel`
   replied "no bin target named nfs-walker-parquet-parallel". Makefile
   has no target that builds it. The binary was an orphan from a
   previous Cargo.toml configuration that no longer existed.

10. **Current source produces correct parquet.** Wrote a thin example
    invoking `parallel_convert_rocks_to_parquet` from current source,
    built with `--features parquet`, ran against the same RocksDB.
    Output: `mtime_us = 1705322096789123, subsec = 789123`. Probe at
    `detect_mtime_scale` showed `metadata_value="microseconds",
    decided_scale=1` — correct path through the exporter.

## Root cause

The orphan binary's age (~7 days) was enough to predate fixes to
`detect_mtime_scale` and the `MTIME_FORMAT` metadata stamping. Calling
that older binary against a current-format RocksDB caused it to fall
back to legacy scale=1_000_000, multiply microseconds (already correct)
by another million, saturate at i64::MAX, and emit the saturated value
to parquet.

The bug was not in source. The bug was that source improvements stopped
flowing into `nfs-walker-parquet-parallel` after its `[[bin]]` entry
was removed from `Cargo.toml` (date and reason unknown).

## Fix

1. `src/bin/nfs-walker-parquet-parallel.rs` — thin clap-based wrapper
   around `nfs_walker::parquet::parallel_convert_rocks_to_parquet`.
   Same CLI shape as the original (--input, --output, --parallelism,
   --compression-level, --row-group-size, --file-size-mb, --quiet).
2. `Cargo.toml` — added:
   ```toml
   [[bin]]
   name = "nfs-walker-parquet-parallel"
   path = "src/bin/nfs-walker-parquet-parallel.rs"
   required-features = ["parquet"]
   ```
3. `cargo build --release --features parquet` produces both binaries
   together.

## Lessons

This is the same shape of bug as yesterday's libnfs FFI signature
mismatch in vamoose. In both cases:

- Source code was correct on careful inspection.
- Unit tests passed.
- A *binary on disk* was diverged from source.
- The divergence was invisible until end-to-end output was checked.
- The fix was to bring the build system back in sync with source.

For libnfs, the binary diverged from headers because two libnfs
versions coexisted on the host. For walker-parquet-parallel, the
binary diverged from source because its build target was removed
without removing the binary.

**Operational rule emerging:** when a system has produced wrong output
end-to-end and source review can't find the bug, check the
last-modified time of every binary in the pipeline against the source
files they should depend on. A binary substantially older than its
source files is suspect.

## Cleanup

- `examples/probe_subsec.rs` — deleted
- `examples/probe_readdir.rs` — deleted
- `examples/parquet_export.rs` — promoted to `src/bin/...`
- All tracing probes in `walker/simple.rs`, `parquet/convert.rs`,
  and `rocksdb/writer.rs` — removed
- The stale binary at
  `target/release/nfs-walker-parquet-parallel` from April 27 — replaced
  by today's build at `2026-05-04 07:35:41`

## Related work

- The vamoose project's `mig-walker-rewrite` shim has been the
  workaround; once walker's parquet output is stable through this fix,
  the shim's transformation work shrinks. Schema differences remain
  (walker uses `mtime_us` int64 micros; vamoose canonical uses
  `mtime_sec` int64 + `mtime_nsec` int32), so the shim is still needed
  for that translation, but no longer needs to deal with subsecond
  loss.
- Future improvement: have walker emit `mtime_sec` + `mtime_nsec`
  alongside `mtime_us` so the canonical schema is a direct match. This
  is a separate, larger work item from the bug investigated here.
