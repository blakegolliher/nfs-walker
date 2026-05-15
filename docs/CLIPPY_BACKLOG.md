# Clippy backlog

Snapshot of `cargo clippy --all-targets` after the RocksDB-removal
landing on `experiment/parquet-direct-write`. **All of these are
pre-existing on `main`** — none were introduced by the parquet refactor.
They're tracked here so the cleanup can be picked up in its own pass.

Run to reproduce:

```bash
cargo clippy --all-targets 2>&1
```

Counts after the rocks removal: **1 error, 22 warnings** (the rocks
removal cut warnings from 35 → 22 by deleting the offending modules
along with them).

---

## Error

### `while_immutable_condition` — `src/nfs/connection.rs:1670`

```rust
while !unsafe { *completed } {
```

This is an FFI pattern: `completed` is a raw pointer whose contents are
flipped from `false → true` by a libnfs C-side callback. Clippy can't
prove the mutation through the unsafe deref, so it lints. The loop is
correct; the fix is either:

- `#[allow(clippy::while_immutable_condition)]` on the loop with a
  comment explaining the FFI-callback mutation, **or**
- restructure to use `nfs_service` with an `AtomicBool` flag so the
  mutation point is visible to clippy.

The first option is the smallest change and is the right call.

---

## Warnings, by file

### `src/nfs/connection.rs` (most of the noise)

- **`unnecessary_cast` × 12** — `e.g. v as i64` where `v` is already
  `i64`. Lines 1028–1035 and 1365–1372. Remove the `as i64` casts.
- **`collapsible_if` × 2** — lines 633, 640. Merge nested `if`s with `&&`.
- **`manual_div_ceil`** — line 1604. Replace
  `(a + b - 1) / b` with `a.div_ceil(b)` (stable in Rust 1.73+).
- **`bool_assert_comparison` × 2** — lines 2099, 2108. Replace
  `assert_eq!(x, false)` with `assert!(!x)` (test code only).

### `src/walker/simple.rs`

- **`empty_line_after_doc_comment`** — line 88. Drop the blank line
  between the `///` doc comment and the item.
- **`redundant_closure`** — line 737. Drop the lambda wrapper.
- **`field_reassign_with_default`** — line 1667 (test fixture). Use
  struct-update syntax in the initializer.

### `src/nfs/dns_resolver.rs`

- **`derivable_impls`** — line 37. The hand-written `impl Default` can
  be replaced with `#[derive(Default)]`.

### `src/server/catalog.rs`

- **`manual_range_contains` × 2** — lines 720, 726. Replace
  `n < lo || n > hi` with `!(lo..=hi).contains(&n)`.

### `src/server/executor.rs` + `src/server/routes.rs`

- **`redundant_closure` × 2** — lines 114 and 250. Drop the lambda
  wrapper, pass the function directly.

### `src/parquet/schema.rs`

- **`inconsistent_digit_grouping`** — line 161. Standardize the
  underscore grouping in a literal (e.g. `1_000_000` not `1000_000`).

---

## Notes for the LLM picking this up

- Almost everything here is mechanical — most fixes are 1-line edits.
  Group them per file in a single pass to keep the diff readable.
- The one judgment call is the **FFI loop error**. The `#[allow]` with
  an explanatory comment is the right answer; do not "fix" it by
  shoehorning an `AtomicBool` into the wrapped libnfs API — that
  reaches into the C boundary and is a much larger change with no
  benefit beyond satisfying clippy.
- Verify after each file: `cargo clippy --all-targets`.
- Don't touch the `assert_eq!(x, false)` calls without re-running the
  affected test (`cargo test --lib -- connection`) — the FFI-test
  fixtures are sensitive.
- Confirm no new behavior changes: `cargo test --workspace` should still
  show 89 lib tests passing (the two pre-existing `content::filetype`
  test failures are inherited from main, see
  `~/.claude/projects/-home-vastdata-projects-nfs-walker/memory/project_parquet_direct_write_known_issues.md`).
