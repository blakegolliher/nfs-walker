//! Checksum calculation using gxhash
//!
//! gxhash is an extremely fast non-cryptographic hash function that uses
//! hardware AES instructions for maximum throughput (~100 GB/s on modern CPUs).
//!
//! This is ideal for duplicate file detection where we need fast hashing
//! but don't require cryptographic security.

use gxhash::GxHasher;
use std::hash::Hasher;

/// Compute a gxhash checksum for the given content
///
/// Returns a hex-encoded string (32 characters for 128-bit hash).
/// The hash is deterministic and suitable for duplicate detection.
///
/// # Example
///
/// ```
/// use nfs_walker::content::checksum::compute_gxhash;
///
/// let data = b"Hello, World!";
/// let hash = compute_gxhash(data);
/// assert_eq!(hash.len(), 32); // 128-bit hash = 16 bytes = 32 hex chars
/// ```
pub fn compute_gxhash(content: &[u8]) -> String {
    // Use GxHasher with a fixed seed for reproducibility
    let mut hasher = GxHasher::with_seed(0);
    hasher.write(content);
    let hash = hasher.finish();

    // For a stronger hash, we combine two hashes with different data views
    // This gives us effectively 128 bits of entropy
    let mut hasher2 = GxHasher::with_seed(0x517cc1b727220a95);
    hasher2.write(content);
    let hash2 = hasher2.finish();

    // Format as 32-char hex string (128 bits)
    format!("{:016x}{:016x}", hash, hash2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_gxhash() {
        let data = b"Hello, World!";
        let hash = compute_gxhash(data);

        // Hash should be 32 hex characters (128 bits)
        assert_eq!(hash.len(), 32);

        // Same content should produce same hash
        let hash2 = compute_gxhash(data);
        assert_eq!(hash, hash2);

        // Different content should produce different hash
        let different_data = b"Hello, World?";
        let different_hash = compute_gxhash(different_data);
        assert_ne!(hash, different_hash);
    }

    #[test]
    fn test_empty_content() {
        let empty = b"";
        let hash = compute_gxhash(empty);
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_large_content() {
        // Test with 1MB of data
        let large_data = vec![0xABu8; 1024 * 1024];
        let hash = compute_gxhash(&large_data);
        assert_eq!(hash.len(), 32);
    }
}
