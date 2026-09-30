//! Path-to-writer-shard routing.
//!
//! Used by `ShardedSender` to fan walker output across N parquet writer
//! threads. Pure `gxhash(path) % shards`; deterministic across runs so
//! downstream readers can reason about which shard owns which path
//! prefix without consulting any sidecar map.

/// gxhash seed for shard routing. Pinned to 0; cross-process determinism
/// between the writer and any reader that wants to recompute shard
/// ownership depends on this never changing.
pub const PATH_SHARDS_HASH_SEED: i64 = 0;

/// Map a path to its writer-shard index in `0..shards`.
///
/// Determinism contract: identical `(path, shards)` input must produce
/// identical output across processes and runs. Built on
/// `gxhash::gxhash64` with `PATH_SHARDS_HASH_SEED`.
#[inline]
pub fn path_to_shard(path: &[u8], shards: usize) -> usize {
    debug_assert!(shards >= 1);
    if shards <= 1 {
        return 0;
    }
    let h = gxhash::gxhash64(path, PATH_SHARDS_HASH_SEED);
    (h as usize) % shards
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_to_shard_is_deterministic_and_uniform() {
        for path in ["/", "/a", "/a/b", "/data/checkpoints/very/deep/file.bin"] {
            let s4 = path_to_shard(path.as_bytes(), 4);
            assert_eq!(s4, path_to_shard(path.as_bytes(), 4));
            assert!(s4 < 4);
        }

        let n = 8;
        let mut buckets = vec![0u64; n];
        for i in 0..10_000 {
            let p = format!("/data/dir{}/file-{:06}.bin", i % 137, i);
            buckets[path_to_shard(p.as_bytes(), n)] += 1;
        }
        let total: u64 = buckets.iter().sum();
        assert_eq!(total, 10_000);
        for (i, c) in buckets.iter().enumerate() {
            assert!(*c > 0, "bucket {} got zero entries", i);
            assert!(*c >= 500 && *c <= 2000, "bucket {} = {} (skew?)", i, c);
        }
    }

    #[test]
    fn path_to_shard_single_shard_is_zero() {
        assert_eq!(path_to_shard(b"/anything", 1), 0);
    }

    #[test]
    fn path_to_shard_hashes_raw_non_utf8_bytes() {
        let a = b"/data/name-\xff";
        let b = b"/data/name-\xfe";
        assert_eq!(path_to_shard(a, 8), path_to_shard(a, 8));
        assert_ne!(
            gxhash::gxhash64(a, PATH_SHARDS_HASH_SEED),
            gxhash::gxhash64(b, PATH_SHARDS_HASH_SEED)
        );
    }
}
