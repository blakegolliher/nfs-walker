# Clippy backlog

Current status: resolved (verified 2026-07-13).

```bash
cargo clippy --all-targets --all-features   # 0 warnings, 0 errors
cargo test  --all-features                  # all green
```

The backlog from the RocksDB-removal snapshot has been cleaned up, and
the 2026-07 code-quality pass removed the last two warnings (a useless
`usize` cast in `direct_writer.rs` and a `sort_by` → `sort_by_key` in
`scanlog.rs`). The `content::filetype` test failures mentioned in older
notes are moot — the content-analysis module was deleted entirely in
that pass (`--checksum` / `--file-type` read data over NFS that the
Parquet writer never persisted).
