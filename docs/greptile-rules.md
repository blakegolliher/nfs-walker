# Review heuristics for nfs-walker

These complement the structured rules in `greptile.json` at the repo root. The
structured rules drive Greptile's per-PR checks; this file is the long-form
"why" behind them, applied with judgment.

## libnfs FFI

- **Handle lifecycle**: every `struct nfs_context*` returned from
  `nfs_init_context()` has a matching `nfs_destroy_context()`. Same for
  `struct nfsfh*` and `nfs_close()`. Drop impls or scope guards are preferred
  over manual cleanup in long functions — manual cleanup paths get skipped on
  early return or panic.
- **Reentrancy**: libnfs contexts are not thread-safe. If a PR introduces
  sharing of an `nfs_context` across threads (even via `Arc<Mutex<>>`), flag
  it — the intended pattern is one context per worker. `Arc<Mutex<>>` over an
  `nfs_context` is wrong, not a fix.
- **String marshaling**: paths passed to libnfs must be valid CStrings. Real
  NFS exports contain non-UTF-8 bytes and interior NUL bytes. Flag any
  `.unwrap()` on `CString::new()` for path data — must return an error.
- **Errno semantics**: libnfs returns negative errno on failure. `< 0` is
  correct; `!= 0` is wrong (positive returns are byte counts on read paths).

## Performance hot paths

The 180K files/sec baseline was won through:

- File handle caching with LRU eviction (avoids re-lookup syscalls)
- READDIRPLUS instead of READDIR+LOOKUP loops
- Batched SQLite inserts under a single transaction
- Minimal allocation in the per-entry path

PRs touching these areas should justify how they preserve throughput. Ask for
`criterion` numbers if the change is non-trivial.

## SQLite

- Bulk inserts: one `BEGIN`, batched `INSERT`, one `COMMIT`. Per-row commits
  tank throughput.
- Use `INSERT OR REPLACE` / `INSERT ... ON CONFLICT` rather than
  SELECT+INSERT.
- WAL mode is intentional for concurrent readers during long scans; flag any
  PR that switches journal mode.
- Schema migrations must be additive — never drop columns — so in-flight and
  resumed scans survive upgrades.

## Error handling

- Errors crossing FFI should be wrapped with `.context()` describing what was
  attempted and against what path/handle. Bare `?` makes scan-log failures
  unactionable.
- Walk-level errors classify into:
  - **terminal** — abort the scan (e.g. lost mount, OOM)
  - **retriable** — back off and continue (e.g. transient `EAGAIN`)
  - **per-entry** — log and skip (e.g. one bad inode)

  Flag any catch-all `Err(_) => return` in the walk loop — losing one bad
  inode should not kill a multi-million-file scan.

## Tests and benchmarks

- New hot-path features should include a `criterion` benchmark in `benches/`
  alongside the existing `walker_bench.rs`.
- Integration tests against a real libnfs server are gated behind a feature
  flag — don't require them in default `cargo test`.

## What NOT to flag

- Clippy lints that contradict performance (e.g., `needless_collect` where
  the collected `Vec` is intentional for cache locality).
- `unsafe` blocks with clear `SAFETY:` comments and obvious correctness.
- `.unwrap()` inside `#[cfg(test)]` modules.
- Allocations outside the per-entry hot path (startup, scan setup,
  one-shot CLI handling).
