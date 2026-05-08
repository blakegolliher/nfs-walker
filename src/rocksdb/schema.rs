//! RocksDB schema definitions
//!
//! Defines the column families, key encoding, and RocksEntry struct
//! for storing filesystem entries in RocksDB.

use crate::nfs::types::{DbEntry, EntryType};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// How to open a RocksDB database for querying.
///
/// `Readonly` is the simplest and fastest mode but is incompatible with an
/// active writer — concurrent compactions can delete SST files out from
/// under the read-only view, producing "No such file or directory" errors.
///
/// `Secondary` opens against the live primary and maintains its own
/// MANIFEST/WAL replay state in a separate directory, so it tolerates
/// concurrent compactions. Slightly slower to open and view is only as
/// fresh as the last `try_catch_up_with_primary()` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    Readonly,
    Secondary,
}

/// Column family names
pub const CF_ENTRIES_BY_PATH: &str = "entries_by_path";
pub const CF_ENTRIES_BY_INODE: &str = "entries_by_inode";
pub const CF_METADATA: &str = "metadata";
/// Pre-computed summary aggregates flushed periodically by the writer
/// thread. Optional -- legacy databases without this CF still open.
pub const CF_SUMMARY: &str = "summary";

/// Prefix for sharded path-CF names. Shard k's CF is
/// `format!("{}{}", CF_ENTRIES_BY_PATH_PREFIX, k)`, e.g.
/// `entries_by_path_0`, `entries_by_path_1`, ...
///
/// A shard count of 1 keeps the legacy unsuffixed `entries_by_path` CF
/// — fresh shards-1 DBs and legacy DBs share that path so the binary
/// stays drop-in compatible with existing scans.
pub const CF_ENTRIES_BY_PATH_PREFIX: &str = "entries_by_path_";

/// Hash version for the `gxhash(path) % shards` routing function. Bumped
/// only if the hash or seed ever changes; on-disk DBs encode this so a
/// reader using a different hash refuses to open instead of silently
/// going to the wrong shard.
pub const PATH_SHARDS_HASH_VERSION: u32 = 1;

/// gxhash seed for shard routing. Pinned to 0; cross-process determinism
/// (between writer and post-scan readers) depends on this.
pub const PATH_SHARDS_HASH_SEED: i64 = 0;

/// All non-sharded column families this binary knows about, in canonical
/// order. Path-shard CFs are discovered separately at open time.
const ALL_CF_NAMES: &[&str] = &[
    CF_ENTRIES_BY_PATH,
    CF_ENTRIES_BY_INODE,
    CF_METADATA,
    CF_SUMMARY,
];

/// Metadata keys
pub mod meta_keys {
    pub const SOURCE_URL: &str = "source_url";
    pub const START_TIME: &str = "start_time";
    pub const END_TIME: &str = "end_time";
    pub const STATUS: &str = "status";
    pub const DURATION_SECS: &str = "duration_secs";
    pub const TOTAL_DIRS: &str = "total_dirs";
    pub const TOTAL_FILES: &str = "total_files";
    pub const TOTAL_BYTES: &str = "total_bytes";
    pub const ERROR_COUNT: &str = "error_count";
    pub const WORKER_COUNT: &str = "worker_count";
    /// UUID generated at scan start. Persisted so post-scan tools
    /// (Parquet converter, streaming writer) can share the same ID.
    pub const SCAN_ID: &str = "scan_id";
    /// Number of path-CF shards used by the writer. 1 = legacy single
    /// path CF. Absent on pre-shards databases (treated as 1).
    pub const PATH_SHARDS: &str = "path_cf_shards";
    /// Hash function version used for shard routing. Stored alongside
    /// PATH_SHARDS so a reader compiled with a different hash refuses
    /// to open instead of routing point lookups to the wrong shard.
    pub const PATH_SHARDS_HASH_VERSION: &str = "path_cf_shards_hash_version";
    /// Time-unit tag for `RocksEntry::{mtime,atime,ctime}` values.
    /// Present on databases written by walkers that capture sub-second
    /// precision; absent on older databases that stored seconds. The
    /// Parquet exporter switches between pass-through and *1_000_000
    /// based on this key. Value is `"microseconds"` for new scans.
    pub const MTIME_FORMAT: &str = "mtime_format";
    /// String value written for `MTIME_FORMAT` by the current walker.
    pub const MTIME_FORMAT_MICROSECONDS: &str = "microseconds";
}

/// Hash a path to a shard index.
///
/// Determinism contract: identical (path, shards) input must produce the
/// identical output across processes and runs. Built on `gxhash::gxhash64`
/// with a pinned seed = 0 (`PATH_SHARDS_HASH_SEED`). If we ever change
/// either, bump `PATH_SHARDS_HASH_VERSION` so existing DBs remain
/// detectable / unreadable rather than silently corrupted.
#[inline]
pub fn path_to_shard(path: &str, shards: usize) -> usize {
    debug_assert!(shards >= 1);
    if shards <= 1 {
        return 0;
    }
    let h = gxhash::gxhash64(path.as_bytes(), PATH_SHARDS_HASH_SEED);
    (h as usize) % shards
}

/// Build the canonical CF name for path-shard `idx` given a `shards`
/// count. Shard 0 with shards == 1 returns the unsuffixed legacy name
/// so single-shard fresh DBs stay byte-compatible with pre-shards DBs.
pub fn cf_name_for_path_shard(idx: usize, shards: usize) -> String {
    if shards <= 1 {
        CF_ENTRIES_BY_PATH.to_string()
    } else {
        format!("{}{}", CF_ENTRIES_BY_PATH_PREFIX, idx)
    }
}

/// Entry stored in RocksDB with bincode serialization
/// Designed for compact storage (~100 bytes/entry)
///
/// `mtime`/`atime`/`ctime` hold microseconds since the Unix epoch on
/// fresh databases (those tagged with `meta_keys::MTIME_FORMAT =
/// "microseconds"`). Legacy databases written before that tag was
/// introduced stored seconds; the Parquet exporter detects the absence
/// of the tag and rescales those values on the way out.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksEntry {
    pub name: String,
    pub path: String,
    pub entry_type: u8,
    pub size: u64,
    pub mtime: Option<i64>,
    pub atime: Option<i64>,
    pub ctime: Option<i64>,
    /// High-precision time companions; see `NfsStat` docs for semantics.
    /// Adding these is a backward-incompatible bincode change: pre-existing
    /// RocksDBs cannot be read by binaries with this struct.
    pub mtime_sec: Option<i64>,
    pub mtime_nsec: Option<i32>,
    pub atime_sec: Option<i64>,
    pub atime_nsec: Option<i32>,
    pub ctime_sec: Option<i64>,
    pub ctime_nsec: Option<i32>,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub nlink: Option<u64>,
    pub inode: u64,
    pub depth: u32,
    pub extension: Option<String>,
    pub blocks: u64,
    /// gxhash checksum (hex-encoded)
    pub checksum: Option<String>,
    /// Detected MIME type from magic bytes
    pub file_type: Option<String>,
}

impl RocksEntry {
    /// Convert from DbEntry
    pub fn from_db_entry(entry: &DbEntry) -> Self {
        Self {
            name: entry.name.clone(),
            path: entry.path.clone(),
            entry_type: entry.entry_type as u8,
            size: entry.size,
            mtime: entry.mtime,
            atime: entry.atime,
            ctime: entry.ctime,
            mtime_sec: entry.mtime_sec,
            mtime_nsec: entry.mtime_nsec,
            atime_sec: entry.atime_sec,
            atime_nsec: entry.atime_nsec,
            ctime_sec: entry.ctime_sec,
            ctime_nsec: entry.ctime_nsec,
            mode: entry.mode,
            uid: entry.uid,
            gid: entry.gid,
            nlink: entry.nlink,
            inode: entry.inode,
            depth: entry.depth,
            extension: entry.extension.clone(),
            blocks: entry.blocks,
            checksum: entry.checksum.clone(),
            file_type: entry.file_type.clone(),
        }
    }

    /// Convert to DbEntry
    pub fn to_db_entry(&self) -> DbEntry {
        // Compute parent_path from path
        let parent_path = if self.depth == 0 || self.path == "/" {
            None
        } else if let Some(pos) = self.path.rfind('/') {
            if pos == 0 {
                Some("/".to_string())
            } else {
                Some(self.path[..pos].to_string())
            }
        } else {
            Some("/".to_string())
        };

        DbEntry {
            parent_path,
            name: self.name.clone(),
            path: self.path.clone(),
            entry_type: EntryType::from_u8(self.entry_type),
            size: self.size,
            mtime: self.mtime,
            atime: self.atime,
            ctime: self.ctime,
            mtime_sec: self.mtime_sec,
            mtime_nsec: self.mtime_nsec,
            atime_sec: self.atime_sec,
            atime_nsec: self.atime_nsec,
            ctime_sec: self.ctime_sec,
            ctime_nsec: self.ctime_nsec,
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
            nlink: self.nlink,
            inode: self.inode,
            depth: self.depth,
            extension: self.extension.clone(),
            blocks: self.blocks,
            checksum: self.checksum.clone(),
            file_type: self.file_type.clone(),
        }
    }

    /// Serialize to bytes using bincode
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

/// Encode path as key (UTF-8 bytes)
pub fn encode_path_key(path: &str) -> Vec<u8> {
    path.as_bytes().to_vec()
}

/// Decode path from key
pub fn decode_path_key(key: &[u8]) -> Result<String, std::string::FromUtf8Error> {
    String::from_utf8(key.to_vec())
}

/// Encode inode as key (big-endian u64 for proper ordering)
pub fn encode_inode_key(inode: u64) -> [u8; 8] {
    inode.to_be_bytes()
}

/// Decode inode from key
pub fn decode_inode_key(key: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[..8]);
    u64::from_be_bytes(bytes)
}

/// Get column family options for entries (write-optimized)
fn entries_cf_options() -> Options {
    let mut opts = Options::default();

    // Write buffer: 64MB total (2 buffers x 32MB)
    // Smaller buffers = more frequent flushes = less memory
    // This is better for huge scans (100M+ files)
    opts.set_write_buffer_size(32 * 1024 * 1024);
    opts.set_max_write_buffer_number(2);
    opts.set_min_write_buffer_number_to_merge(1);

    // Level compaction settings
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_max_bytes_for_level_base(256 * 1024 * 1024);

    // Bloom filter for point lookups (10 bits/key)
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_cache_index_and_filter_blocks(true);
    opts.set_block_based_table_factory(&block_opts);

    // Compression: Zstd for better ratio (~2-3x smaller than LZ4)
    // Use per-level compression: LZ4 for L0-L1 (frequent compaction),
    // Zstd for L2+ (bulk of data, compacted less often)
    opts.set_compression_per_level(&[
        rocksdb::DBCompressionType::Lz4,  // L0
        rocksdb::DBCompressionType::Lz4,  // L1
        rocksdb::DBCompressionType::Zstd, // L2
        rocksdb::DBCompressionType::Zstd, // L3
        rocksdb::DBCompressionType::Zstd, // L4
        rocksdb::DBCompressionType::Zstd, // L5
        rocksdb::DBCompressionType::Zstd, // L6
    ]);

    opts
}

/// Get column family options for metadata (small, infrequent writes)
fn metadata_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(4 * 1024 * 1024);
    opts.set_max_write_buffer_number(2);
    opts
}

/// Column family options for the summary CF: a handful of small bincode
/// blobs flushed periodically. Same shape as metadata.
fn summary_cf_options() -> Options {
    metadata_cf_options()
}

/// Enumerate the CFs that already exist in `path`, intersected with the
/// set of names this binary knows about (including any
/// `entries_by_path_<N>` shard CFs). Returns the legacy four CFs when the
/// listing fails (e.g. brand-new DB) so the caller can still proceed.
fn existing_known_cfs<P: AsRef<Path>>(path: P, opts: &Options) -> Vec<String> {
    let existing = match DB::list_cf(opts, path.as_ref()) {
        Ok(names) => names,
        Err(_) => {
            return vec![
                CF_ENTRIES_BY_PATH.to_string(),
                CF_ENTRIES_BY_INODE.to_string(),
                CF_METADATA.to_string(),
            ];
        }
    };
    let mut out: Vec<String> = ALL_CF_NAMES
        .iter()
        .copied()
        .filter(|cf| existing.iter().any(|e| e == cf))
        .map(|s| s.to_string())
        .collect();
    // Sharded path CFs: any name matching `entries_by_path_<digits>` is
    // ours, even though the digits aren't enumerable up-front.
    for name in &existing {
        if let Some(suffix) = name.strip_prefix(CF_ENTRIES_BY_PATH_PREFIX) {
            if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
                out.push(name.clone());
            }
        }
    }
    out
}

/// Database configuration for write-optimized scans
pub fn get_db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Increase parallelism. `increase_parallelism` sets the size of the
    // global thread pool RocksDB uses for compactions/flushes; we
    // separately need to lift `max_background_jobs`, which caps how many
    // of those slots can be in use concurrently. The historical default
    // of 4 left us bottlenecked at ~5 cores on a 160-core box during a
    // billion-entry scan (writer + 4 compaction threads, all pegged).
    // Scaling with cpu count, capped at 32, restores parity with the
    // walker side without monopolizing a small machine.
    opts.increase_parallelism(num_cpus::get() as i32);
    let bg_jobs = (num_cpus::get() as i32 / 2).clamp(4, 32);
    opts.set_max_background_jobs(bg_jobs);

    // Disable WAL for scan workloads (data is repeatable)
    // WAL is disabled per-write via WriteBatch options

    // Allow concurrent memtable writes
    opts.set_allow_concurrent_memtable_write(true);
    opts.set_enable_write_thread_adaptive_yield(true);

    // Cap open file descriptors to avoid "too many open files" on large scans.
    // RocksDB will use an LRU cache for SST file handles beyond this limit.
    // -1 (default) means unlimited, which can exhaust the OS limit.
    opts.set_max_open_files(4096);

    opts
}

/// Open or create a RocksDB database with all column families
/// (single-shard, legacy schema).
pub fn open_rocks_db<P: AsRef<Path>>(path: P) -> Result<DB, rocksdb::Error> {
    open_rocks_db_with_shards(path, 1)
}

/// Open or create a RocksDB database with `shards` path-CF shards.
///
/// `shards == 1` creates the legacy single `entries_by_path` CF (drop-in
/// compatible with pre-shards databases). `shards > 1` creates one CF
/// per shard named `entries_by_path_0`, `entries_by_path_1`, ... so each
/// shard has independent memtables, flushes and compactions.
pub fn open_rocks_db_with_shards<P: AsRef<Path>>(
    path: P,
    shards: usize,
) -> Result<DB, rocksdb::Error> {
    debug_assert!(shards >= 1);
    let db_opts = get_db_options();

    let mut cf_descriptors: Vec<ColumnFamilyDescriptor> = Vec::with_capacity(4 + shards);
    if shards <= 1 {
        cf_descriptors.push(ColumnFamilyDescriptor::new(
            CF_ENTRIES_BY_PATH,
            entries_cf_options(),
        ));
    } else {
        for i in 0..shards {
            cf_descriptors.push(ColumnFamilyDescriptor::new(
                format!("{}{}", CF_ENTRIES_BY_PATH_PREFIX, i),
                entries_cf_options(),
            ));
        }
    }
    cf_descriptors.push(ColumnFamilyDescriptor::new(
        CF_ENTRIES_BY_INODE,
        entries_cf_options(),
    ));
    cf_descriptors.push(ColumnFamilyDescriptor::new(
        CF_METADATA,
        metadata_cf_options(),
    ));
    cf_descriptors.push(ColumnFamilyDescriptor::new(
        CF_SUMMARY,
        summary_cf_options(),
    ));

    DB::open_cf_descriptors(&db_opts, path, cf_descriptors)
}

/// Database options for query workloads (read-only / secondary).
///
/// Crucially differs from `get_db_options()` by removing the
/// `max_open_files` cap. With a cap, RocksDB's LRU evicts SST file
/// handles mid-iteration. On Linux an unlinked file stays readable only
/// while some process holds it open — so when the cache later tries to
/// reopen an SST that the active primary has compacted away, we get
/// "No such file or directory". Unlimited handles keep every referenced
/// SST pinned for the duration of the iteration. Safe because we raise
/// `RLIMIT_NOFILE` to 1M at startup.
pub fn get_query_options() -> Options {
    let mut opts = get_db_options();
    opts.set_max_open_files(-1);
    opts
}

/// Open existing RocksDB database for reading.
///
/// Uses `DB::list_cf` to discover the CFs actually present so old
/// databases written before `CF_SUMMARY` was introduced still open
/// cleanly -- the absent CF simply means stats functions fall back to
/// full-scan iteration. Sharded path-CFs (`entries_by_path_*`) are
/// likewise discovered automatically.
pub fn open_rocks_db_readonly<P: AsRef<Path>>(path: P) -> Result<DB, rocksdb::Error> {
    let db_opts = get_query_options();
    let cf_names = existing_known_cfs(&path, &db_opts);
    DB::open_cf_for_read_only(&db_opts, path, cf_names, false)
}

/// Open existing RocksDB database in secondary mode for live querying
/// alongside an active writer.
///
/// Unlike read-only mode, secondary mode tolerates concurrent compactions
/// on the primary by maintaining its own MANIFEST/WAL replay state under
/// `secondary_path`. Call `try_catch_up_with_primary()` to refresh.
pub fn open_rocks_db_secondary<P: AsRef<Path>>(
    primary_path: P,
    secondary_path: P,
) -> Result<DB, rocksdb::Error> {
    let db_opts = get_query_options();
    let cf_names = existing_known_cfs(&primary_path, &db_opts);
    DB::open_cf_as_secondary(&db_opts, primary_path, secondary_path, cf_names)
}

/// Build a deterministic secondary state directory under the OS temp dir
/// for the given primary RocksDB path.
///
/// Reusing the same dir between runs lets RocksDB skip re-replay of the
/// MANIFEST it has already seen. Safe to delete at any time.
pub fn default_secondary_path<P: AsRef<Path>>(primary_path: P) -> std::path::PathBuf {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let canonical = std::fs::canonicalize(&primary_path)
        .unwrap_or_else(|_| primary_path.as_ref().to_path_buf());
    let mut hasher = DefaultHasher::new();
    canonical.hash(&mut hasher);
    let hash = hasher.finish();

    std::env::temp_dir().join(format!("nfs-walker-secondary-{:016x}", hash))
}

/// RocksDB handle wrapper with column family accessors
pub struct RocksHandle {
    pub db: DB,
    /// Number of path-CF shards. 1 = legacy single-CF schema. The value
    /// is set at open time:
    ///   - Fresh DBs created by `RocksHandle::open_with_shards`: caller
    ///     supplies the count and writes it to metadata.
    ///   - Reopens (read-only / secondary): read from
    ///     `meta_keys::PATH_SHARDS`. Absent → 1 (pre-shards database).
    shards: usize,
}

impl RocksHandle {
    /// Number of path-CF shards. 1 means the legacy single-CF schema.
    #[inline]
    pub fn shards(&self) -> usize {
        self.shards
    }

    /// Map a path to the shard CF index that owns it.
    #[inline]
    pub fn shard_for_path(&self, path: &str) -> usize {
        path_to_shard(path, self.shards)
    }

    /// Open or create database (single shard, legacy schema).
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, rocksdb::Error> {
        // Single-shard creation never trips the hash-version check, so
        // the underlying rocksdb::Error type is sufficient.
        let db = open_rocks_db_with_shards(path, 1)?;
        Ok(Self { db, shards: 1 })
    }

    /// Open or create database with `shards` path-CF shards. Writes the
    /// shard count to metadata so future read-only / secondary opens
    /// discover it automatically.
    pub fn open_with_shards<P: AsRef<Path>>(
        path: P,
        shards: usize,
    ) -> Result<Self, rocksdb::Error> {
        debug_assert!(shards >= 1);
        let db = open_rocks_db_with_shards(path, shards)?;
        let handle = Self { db, shards };
        // Persist shard metadata so subsequent reopens (read-only /
        // secondary) discover the count without depending on the caller.
        if shards > 1 {
            handle.set_metadata(meta_keys::PATH_SHARDS, &shards.to_string())?;
            handle.set_metadata(
                meta_keys::PATH_SHARDS_HASH_VERSION,
                &PATH_SHARDS_HASH_VERSION.to_string(),
            )?;
        }
        Ok(handle)
    }

    /// Open for read-only access. Shard count is auto-detected from
    /// metadata; legacy DBs without the metadata key open as `shards=1`.
    /// Hash-version mismatches surface as `RocksError::InvalidFormat`.
    pub fn open_readonly<P: AsRef<Path>>(path: P) -> Result<Self, rocksdb::Error> {
        let db = open_rocks_db_readonly(path)?;
        let mut handle = Self { db, shards: 1 };
        handle.shards = read_shards_from_metadata(&handle).unwrap_or(1).max(1);
        // Hash-version mismatch panics at open: a routing error would be
        // strictly worse (silent wrong-shard reads). Realistic only when
        // we actually bump the hash version constant in a future commit.
        if let Err(msg) = verify_shards_hash_version_str(&handle) {
            panic!("{}", msg);
        }
        Ok(handle)
    }

    /// Open in RocksDB secondary mode against a live primary writer.
    ///
    /// `secondary_path` must be a writable directory dedicated to this
    /// secondary instance's MANIFEST/WAL replay state — it must already exist.
    pub fn open_secondary<P: AsRef<Path>>(
        primary_path: P,
        secondary_path: P,
    ) -> Result<Self, rocksdb::Error> {
        let db = open_rocks_db_secondary(primary_path, secondary_path)?;
        let mut handle = Self { db, shards: 1 };
        handle.shards = read_shards_from_metadata(&handle).unwrap_or(1).max(1);
        if let Err(msg) = verify_shards_hash_version_str(&handle) {
            panic!("{}", msg);
        }
        Ok(handle)
    }

    /// In secondary mode, replay any new MANIFEST/WAL entries from the
    /// primary so this view sees the latest committed state. No-op for
    /// other modes (returns Ok).
    pub fn try_catch_up_with_primary(&self) -> Result<(), rocksdb::Error> {
        self.db.try_catch_up_with_primary()
    }

    /// Get entries_by_path column family.
    ///
    /// Single-shard convenience accessor. Panics on multi-shard DBs —
    /// callers in those code paths must use `cf_entries_by_path_shard`
    /// or `cf_entries_by_path_all_shards` instead.
    pub fn cf_entries_by_path(&self) -> &ColumnFamily {
        debug_assert!(
            self.shards == 1,
            "cf_entries_by_path() called on multi-shard DB; use cf_entries_by_path_shard"
        );
        self.db
            .cf_handle(CF_ENTRIES_BY_PATH)
            .expect("entries_by_path CF missing")
    }

    /// Get the path-shard CF for index `idx`. For shards == 1 this
    /// returns the legacy unsuffixed CF.
    pub fn cf_entries_by_path_shard(&self, idx: usize) -> &ColumnFamily {
        debug_assert!(idx < self.shards, "shard index {} >= shards {}", idx, self.shards);
        if self.shards <= 1 {
            return self
                .db
                .cf_handle(CF_ENTRIES_BY_PATH)
                .expect("entries_by_path CF missing");
        }
        let name = format!("{}{}", CF_ENTRIES_BY_PATH_PREFIX, idx);
        self.db
            .cf_handle(&name)
            .unwrap_or_else(|| panic!("missing path shard CF: {}", name))
    }

    /// Iterate every path-CF shard, returning (shard_idx, &CF) pairs in
    /// ascending shard order. Used by readers that need to merge across
    /// shards.
    pub fn cf_entries_by_path_all_shards(&self) -> Vec<(usize, &ColumnFamily)> {
        (0..self.shards)
            .map(|i| (i, self.cf_entries_by_path_shard(i)))
            .collect()
    }

    /// Get entries_by_inode column family
    pub fn cf_entries_by_inode(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_ENTRIES_BY_INODE)
            .expect("entries_by_inode CF missing")
    }

    /// Get metadata column family
    pub fn cf_metadata(&self) -> &ColumnFamily {
        self.db
            .cf_handle(CF_METADATA)
            .expect("metadata CF missing")
    }

    /// Get the summary column family if the DB was opened with it.
    ///
    /// Returns `None` for legacy databases that predate the summary CF;
    /// callers should fall back to full-scan iteration in that case.
    pub fn cf_summary(&self) -> Option<&ColumnFamily> {
        self.db.cf_handle(CF_SUMMARY)
    }

    /// Set metadata value
    pub fn set_metadata(&self, key: &str, value: &str) -> Result<(), rocksdb::Error> {
        self.db
            .put_cf(self.cf_metadata(), key.as_bytes(), value.as_bytes())
    }

    /// Get metadata value
    pub fn get_metadata(&self, key: &str) -> Result<Option<String>, rocksdb::Error> {
        match self.db.get_cf(self.cf_metadata(), key.as_bytes())? {
            Some(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).to_string())),
            None => Ok(None),
        }
    }

    /// Put entry (writes to both column families). Routes the path entry
    /// to its owning shard CF; the inode CF is shared across all shards.
    pub fn put_entry(&self, entry: &RocksEntry) -> Result<(), crate::error::RocksError> {
        let value = entry
            .to_bytes()
            .map_err(|e| crate::error::RocksError::Bincode(e.to_string()))?;
        let path_key = encode_path_key(&entry.path);
        let inode_key = encode_inode_key(entry.inode);

        let shard = self.shard_for_path(&entry.path);
        let mut batch = WriteBatch::default();
        batch.put_cf(self.cf_entries_by_path_shard(shard), &path_key, &value);
        batch.put_cf(self.cf_entries_by_inode(), &inode_key, &value);

        self.db
            .write(batch)
            .map_err(crate::error::RocksError::Rocks)
    }

    /// Get entry by path. Routes to the owning shard CF for an O(log n)
    /// lookup independent of shard count.
    pub fn get_by_path(&self, path: &str) -> Result<Option<RocksEntry>, crate::error::RocksError> {
        let key = encode_path_key(path);
        let shard = self.shard_for_path(path);
        match self
            .db
            .get_cf(self.cf_entries_by_path_shard(shard), &key)
            .map_err(crate::error::RocksError::Rocks)?
        {
            Some(bytes) => {
                let entry = RocksEntry::from_bytes(&bytes)
                    .map_err(|e| crate::error::RocksError::Bincode(e.to_string()))?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Get entry by inode
    pub fn get_by_inode(&self, inode: u64) -> Result<Option<RocksEntry>, crate::error::RocksError> {
        let key = encode_inode_key(inode);
        match self
            .db
            .get_cf(self.cf_entries_by_inode(), &key)
            .map_err(crate::error::RocksError::Rocks)?
        {
            Some(bytes) => {
                let entry = RocksEntry::from_bytes(&bytes)
                    .map_err(|e| crate::error::RocksError::Bincode(e.to_string()))?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Iterate all entries by path in lexicographic order.
    ///
    /// Single-shard DBs: a plain RocksDB CF iterator. Multi-shard DBs:
    /// a k-way merge across all path-shard CFs (each shard yields keys
    /// in order; the merge picks the global minimum at each step).
    /// `O(log shards)` comparisons per row — negligible for the small
    /// shard counts we use (1..=32).
    pub fn iter_by_path(
        &self,
    ) -> Box<dyn Iterator<Item = Result<RocksEntry, crate::error::RocksError>> + '_> {
        if self.shards <= 1 {
            return Box::new(
                self.db
                    .iterator_cf(self.cf_entries_by_path_shard(0), rocksdb::IteratorMode::Start)
                    .map(|result| {
                        let (_, value) = result.map_err(crate::error::RocksError::Rocks)?;
                        RocksEntry::from_bytes(&value)
                            .map_err(|e| crate::error::RocksError::Bincode(e.to_string()))
                    }),
            );
        }
        Box::new(
            iter_by_path_kmerge(self, None, None).map(|res| res.map(|(_k, entry)| entry)),
        )
    }

    /// Iterate (key_bytes, RocksEntry) pairs across path-shard CFs in
    /// lexicographic key order. Used by readers that need the raw key
    /// (e.g. `RocksBaseline::iter_paths`).
    pub fn iter_by_path_kv(
        &self,
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, RocksEntry), crate::error::RocksError>> + '_>
    {
        Box::new(iter_by_path_kmerge(self, None, None))
    }

    /// Iterate (key_bytes, RocksEntry) pairs whose key starts with
    /// `prefix`, in lexicographic order. Lower-bound-aware so each shard
    /// can seek directly past keys preceding the prefix.
    pub fn iter_by_path_prefix(
        &self,
        prefix: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, RocksEntry), crate::error::RocksError>> + '_>
    {
        let prefix_owned = prefix.to_vec();
        Box::new(
            iter_by_path_kmerge(self, Some(prefix_owned.clone()), None).take_while(move |res| {
                match res {
                    Ok((k, _)) => k.starts_with(&prefix_owned),
                    Err(_) => true,
                }
            }),
        )
    }

    /// Iterate all entries by inode.
    ///
    /// Faster than `iter_by_path` for full scans: the inode CF has 8-byte
    /// fixed keys vs the path CF's variable string keys (~80-150B in deep
    /// trees), so SST blocks are denser and there is less disk I/O.
    ///
    /// Caveat: hardlinked files share an inode, and the inode CF's writes
    /// overwrite on collision (writer.rs:84). So this iteration yields one
    /// `RocksEntry` per unique inode — the `path`/`name` fields will be
    /// whichever alias was written last. Use `iter_by_path` if you need
    /// to see every name.
    pub fn iter_by_inode(
        &self,
    ) -> impl Iterator<Item = Result<RocksEntry, crate::error::RocksError>> + '_ {
        self.db
            .iterator_cf(self.cf_entries_by_inode(), rocksdb::IteratorMode::Start)
            .map(|result| {
                let (_, value) = result.map_err(crate::error::RocksError::Rocks)?;
                RocksEntry::from_bytes(&value)
                    .map_err(|e| crate::error::RocksError::Bincode(e.to_string()))
            })
    }

    /// Count entries (by iterating - O(n))
    pub fn count_entries(&self) -> Result<u64, rocksdb::Error> {
        let mut count = 0u64;
        let iter = self
            .db
            .iterator_cf(self.cf_entries_by_path(), rocksdb::IteratorMode::Start);
        for _ in iter {
            count += 1;
        }
        Ok(count)
    }

}

/// Read the persisted shard count from the metadata CF. Absent, empty,
/// or unparseable → returns `Err`; callers fall back to `1` (legacy DB).
/// We intentionally return `Result` so a real RocksDB read failure can
/// be distinguished from "metadata key not present", but we treat both
/// the same in practice.
fn read_shards_from_metadata(handle: &RocksHandle) -> Result<usize, ()> {
    match handle.get_metadata(meta_keys::PATH_SHARDS) {
        Ok(Some(s)) if !s.is_empty() => s.parse::<usize>().map(|n| n.max(1)).map_err(|_| ()),
        _ => Err(()),
    }
}

/// Returns `Err(message)` when the persisted hash version doesn't match
/// the compiled-in version. Routing point lookups under a different hash
/// would silently return wrong-shard "not found" results, so on
/// mismatch the caller panics with this message.
fn verify_shards_hash_version_str(handle: &RocksHandle) -> Result<(), String> {
    if handle.shards <= 1 {
        return Ok(());
    }
    let stored = match handle.get_metadata(meta_keys::PATH_SHARDS_HASH_VERSION) {
        Ok(s) => s,
        Err(e) => {
            return Err(format!(
                "Failed to read shard hash-version metadata: {}",
                e
            ))
        }
    };
    let stored_v = match stored.as_deref() {
        Some(s) if !s.is_empty() => s.parse::<u32>().map_err(|e| {
            format!(
                "Corrupt metadata: '{}' = '{}' (parse error: {})",
                meta_keys::PATH_SHARDS_HASH_VERSION,
                s,
                e
            )
        })?,
        _ => 0,
    };
    if stored_v != PATH_SHARDS_HASH_VERSION {
        return Err(format!(
            "Incompatible shard hash version: DB encoded with v{}, binary expects v{}. \
             Re-export this DB with a binary built at the original version, or re-scan.",
            stored_v, PATH_SHARDS_HASH_VERSION
        ));
    }
    Ok(())
}

/// One element on the k-merge heap: the next pending key from a single
/// shard iterator. `BinaryHeap` is a max-heap, so we invert ordering on
/// the key to get min-heap semantics.
struct KMergeEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    /// Which shard iterator this entry came from. Used to pull the next
    /// element after a `pop()`.
    shard: usize,
}

impl Ord for KMergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse so smallest key wins in a max-heap.
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.shard.cmp(&self.shard))
    }
}
impl PartialOrd for KMergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for KMergeEntry {}
impl PartialEq for KMergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.shard == other.shard
    }
}

/// K-way merge across path-shard CFs. Yields (key_bytes, RocksEntry)
/// pairs in lexicographic order.
///
/// `lower` (inclusive) and `upper` (exclusive) are passed through to
/// each shard iterator as `set_iterate_lower_bound` /
/// `set_iterate_upper_bound`. The bounds are owned-cloned per shard
/// because RocksDB's `ReadOptions` retains them by raw pointer.
fn iter_by_path_kmerge<'a>(
    handle: &'a RocksHandle,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
) -> impl Iterator<Item = Result<(Vec<u8>, RocksEntry), crate::error::RocksError>> + 'a {
    use std::collections::BinaryHeap;

    let shards = handle.shards.max(1);
    let mut cursors: Vec<ShardCursor<'a>> = Vec::with_capacity(shards);
    for i in 0..shards {
        let mut opts = rocksdb::ReadOptions::default();
        if let Some(ref lb) = lower {
            opts.set_iterate_lower_bound(lb.clone());
        }
        if let Some(ref ub) = upper {
            opts.set_iterate_upper_bound(ub.clone());
        }
        let iter = handle.db.iterator_cf_opt(
            handle.cf_entries_by_path_shard(i),
            opts,
            rocksdb::IteratorMode::Start,
        );
        cursors.push(ShardCursor { iter });
    }

    let mut heap: BinaryHeap<KMergeEntry> = BinaryHeap::with_capacity(shards);
    let mut init_err: Option<crate::error::RocksError> = None;
    for (i, cursor) in cursors.iter_mut().enumerate() {
        match cursor.iter.next() {
            Some(Ok((k, v))) => heap.push(KMergeEntry {
                key: k.into_vec(),
                value: v.into_vec(),
                shard: i,
            }),
            Some(Err(e)) => {
                init_err = Some(crate::error::RocksError::Rocks(e));
                break;
            }
            None => {}
        }
    }

    let init_err_iter = init_err.map(Err).into_iter();
    init_err_iter.chain(KMergeIter { cursors, heap })
}

/// State machine for the k-merge: pop the smallest pending head, advance
/// the shard it came from, and yield the popped (key, value).
struct KMergeIter<'a> {
    cursors: Vec<ShardCursor<'a>>,
    heap: std::collections::BinaryHeap<KMergeEntry>,
}

struct ShardCursor<'a> {
    iter: rocksdb::DBIteratorWithThreadMode<'a, rocksdb::DB>,
}

impl<'a> Iterator for KMergeIter<'a> {
    type Item = Result<(Vec<u8>, RocksEntry), crate::error::RocksError>;

    fn next(&mut self) -> Option<Self::Item> {
        let popped = self.heap.pop()?;
        let KMergeEntry { key, value, shard } = popped;
        // Advance this shard.
        match self.cursors[shard].iter.next() {
            Some(Ok((k, v))) => self.heap.push(KMergeEntry {
                key: k.into_vec(),
                value: v.into_vec(),
                shard,
            }),
            Some(Err(e)) => return Some(Err(crate::error::RocksError::Rocks(e))),
            None => {}
        }
        let entry = match RocksEntry::from_bytes(&value) {
            Ok(e) => e,
            Err(e) => {
                return Some(Err(crate::error::RocksError::Bincode(e.to_string())))
            }
        };
        Some(Ok((key, entry)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_key_encoding() {
        let path = "/data/subdir/file.txt";
        let key = encode_path_key(path);
        let decoded = decode_path_key(&key).unwrap();
        assert_eq!(path, decoded);
    }

    #[test]
    fn test_inode_key_encoding() {
        let inode = 12345678901234u64;
        let key = encode_inode_key(inode);
        let decoded = decode_inode_key(&key);
        assert_eq!(inode, decoded);
    }

    #[test]
    fn path_to_shard_is_deterministic_and_uniform() {
        // Identical (path, shards) pair must always map to the same idx.
        for path in ["/", "/a", "/a/b", "/data/checkpoints/very/deep/file.bin"] {
            let s4 = path_to_shard(path, 4);
            assert_eq!(s4, path_to_shard(path, 4));
            assert!(s4 < 4);
        }

        // Distribution sanity over many synthetic paths: every bucket
        // populated, no bucket starves.
        let n = 8;
        let mut buckets = vec![0u64; n];
        for i in 0..10_000 {
            let p = format!("/data/dir{}/file-{:06}.bin", i % 137, i);
            buckets[path_to_shard(&p, n)] += 1;
        }
        let total: u64 = buckets.iter().sum();
        assert_eq!(total, 10_000);
        for (i, c) in buckets.iter().enumerate() {
            assert!(*c > 0, "bucket {} got zero entries", i);
            // Loose ±50% bound — uniformity isn't load-bearing for the
            // test, just an early-warning if gxhash ever degrades.
            assert!(*c >= 500 && *c <= 2000, "bucket {} = {} (skew?)", i, c);
        }
    }

    #[test]
    fn cf_name_for_path_shard_collapses_for_single_shard() {
        assert_eq!(cf_name_for_path_shard(0, 1), CF_ENTRIES_BY_PATH);
        assert_eq!(cf_name_for_path_shard(0, 4), "entries_by_path_0");
        assert_eq!(cf_name_for_path_shard(3, 4), "entries_by_path_3");
    }

    #[test]
    fn fresh_db_with_shards_persists_metadata_and_creates_cfs() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let p = dir.path().join("sharded.rocks");

        let h = RocksHandle::open_with_shards(&p, 4).unwrap();
        assert_eq!(h.shards(), 4);

        // Metadata persisted with the right values.
        let v = h.get_metadata(meta_keys::PATH_SHARDS).unwrap().unwrap();
        assert_eq!(v, "4");
        let hv = h
            .get_metadata(meta_keys::PATH_SHARDS_HASH_VERSION)
            .unwrap()
            .unwrap();
        assert_eq!(hv, PATH_SHARDS_HASH_VERSION.to_string());

        // All four shard CFs exist.
        for i in 0..4 {
            let _cf = h.cf_entries_by_path_shard(i);
        }

        // Reopening read-only auto-detects shards=4.
        drop(h);
        let ro = RocksHandle::open_readonly(&p).unwrap();
        assert_eq!(ro.shards(), 4);
    }

    #[test]
    fn legacy_single_cf_db_opens_with_shards_one() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let p = dir.path().join("legacy.rocks");

        // Open without specifying shards — single CF named entries_by_path.
        let h = RocksHandle::open(&p).unwrap();
        assert_eq!(h.shards(), 1);
        // No shard metadata written.
        assert!(h.get_metadata(meta_keys::PATH_SHARDS).unwrap().is_none());
        drop(h);

        let ro = RocksHandle::open_readonly(&p).unwrap();
        assert_eq!(ro.shards(), 1);
    }

    #[test]
    fn round_trip_through_multi_shard_db_yields_ordered_kway_merge() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let p = dir.path().join("merge.rocks");

        // Insert 1k synthetic entries across 8 shards. All entries
        // share the same inode-key space (collisions overwrite, which
        // mirrors hardlink semantics — fine for this test).
        let h = RocksHandle::open_with_shards(&p, 8).unwrap();
        let mut want_paths: Vec<String> = Vec::with_capacity(1000);
        for i in 0..1000u64 {
            let path = format!("/data/file-{:08}.bin", i);
            let entry = RocksEntry {
                name: format!("file-{:08}.bin", i),
                path: path.clone(),
                entry_type: 0,
                size: i,
                mtime: None,
                atime: None,
                ctime: None,
                mtime_sec: None,
                mtime_nsec: None,
                atime_sec: None,
                atime_nsec: None,
                ctime_sec: None,
                ctime_nsec: None,
                mode: Some(0o644),
                uid: Some(1000),
                gid: Some(1000),
                nlink: Some(1),
                inode: i + 1,
                depth: 2,
                extension: Some("bin".to_string()),
                blocks: 0,
                checksum: None,
                file_type: None,
            };
            h.put_entry(&entry).unwrap();
            want_paths.push(path);
        }
        h.db.flush().unwrap();
        drop(h);

        // Re-open RO and verify the merged iteration is in lexicographic
        // order and contains every path exactly once.
        let ro = RocksHandle::open_readonly(&p).unwrap();
        assert_eq!(ro.shards(), 8);

        let mut got_paths: Vec<String> = Vec::with_capacity(1000);
        let mut last: Option<Vec<u8>> = None;
        for res in ro.iter_by_path_kv() {
            let (k, e) = res.unwrap();
            if let Some(prev) = &last {
                assert!(prev.as_slice() <= k.as_slice(), "k-merge not ordered");
            }
            last = Some(k);
            got_paths.push(e.path);
        }
        assert_eq!(got_paths.len(), 1000);
        let mut want_sorted = want_paths.clone();
        want_sorted.sort();
        assert_eq!(got_paths, want_sorted);

        // Point lookups: every path resolves via its owning shard.
        for path in &want_paths {
            let entry = ro.get_by_path(path).unwrap();
            assert!(entry.is_some(), "missing path: {}", path);
            assert_eq!(entry.unwrap().path, *path);
        }
    }

    #[test]
    fn test_rocks_entry_serialization() {
        let entry = RocksEntry {
            name: "test.txt".to_string(),
            path: "/data/test.txt".to_string(),
            entry_type: 0,
            size: 1024,
            mtime: Some(1234567890),
            atime: None,
            ctime: Some(1234567890),
            mtime_sec: Some(1234567890),
            mtime_nsec: Some(0),
            atime_sec: None,
            atime_nsec: None,
            ctime_sec: Some(1234567890),
            ctime_nsec: Some(0),
            mode: Some(0o644),
            uid: Some(1000),
            gid: Some(1000),
            nlink: Some(1),
            inode: 123456,
            depth: 2,
            extension: Some("txt".to_string()),
            blocks: 8,
            checksum: Some("0123456789abcdef0123456789abcdef".to_string()),
            file_type: Some("text/plain".to_string()),
        };

        let bytes = entry.to_bytes().unwrap();
        let decoded = RocksEntry::from_bytes(&bytes).unwrap();

        assert_eq!(entry.name, decoded.name);
        assert_eq!(entry.path, decoded.path);
        assert_eq!(entry.inode, decoded.inode);
        assert_eq!(entry.extension, decoded.extension);
        assert_eq!(entry.blocks, decoded.blocks);
    }
}
