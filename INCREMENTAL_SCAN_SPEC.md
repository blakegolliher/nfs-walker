# Incremental Scan Implementation Specification

## Context

This is a high-performance NFS filesystem crawler written in Rust that uses direct libnfs protocol access (bypassing kernel NFS client). It currently:
- Crawls filesystems at ~110k files/sec using READDIRPLUS with work-stealing parallelism
- Stores all metadata in a SQLite database
- Uses multiple worker threads, each with their own NFS connection

**Goal**: Add incremental scanning that compares the current filesystem state against a baseline SQLite DB and outputs a NEW database containing only the changes.

---

## Requirements

### Change Detection Rules
1. **Added**: Path exists on filesystem but not in baseline DB
2. **Deleted**: Path exists in baseline DB but not on filesystem
3. **Modified**: Path exists in both, but `mtime` OR `size` differs
4. **Renamed**: Same `inode` found at a different path (use inode as unique identifier)

### Edge Cases
- **Renamed + Modified**: File moved AND content changed → report as `renamed` with new metadata
- **Hardlinks**: Multiple paths with same inode - handle gracefully (first seen path wins for rename detection)
- **Directory changes**: Directories can be added/deleted/renamed too (mtime changes when contents change)

---

## Database Schema

### Existing Schema (baseline DB) - `src/db/schema.rs`

The baseline uses the `entries` table:
```sql
CREATE TABLE entries (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    entry_type INTEGER NOT NULL,  -- 0=file, 1=dir, 2=symlink, etc.
    size INTEGER DEFAULT 0,
    mtime INTEGER,
    atime INTEGER,
    ctime INTEGER,
    mode INTEGER,
    uid INTEGER,
    gid INTEGER,
    nlink INTEGER,
    inode INTEGER,
    depth INTEGER NOT NULL
);

-- Indexes (already exist)
CREATE INDEX idx_entries_path ON entries(path);
CREATE INDEX idx_entries_mtime ON entries(mtime);
```

### New Schema for Changes DB

Add to `src/db/schema.rs`:

```rust
pub const CREATE_CHANGES_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS changes (
    id INTEGER PRIMARY KEY,
    change_type INTEGER NOT NULL,
    path TEXT NOT NULL,
    old_path TEXT,
    entry_type INTEGER,
    size INTEGER,
    mtime INTEGER,
    atime INTEGER,
    ctime INTEGER,
    mode INTEGER,
    uid INTEGER,
    gid INTEGER,
    nlink INTEGER,
    inode INTEGER,
    depth INTEGER
)
"#;

pub const CREATE_CHANGES_INDEXES: &str = r#"
CREATE INDEX IF NOT EXISTS idx_changes_type ON changes(change_type);
CREATE INDEX IF NOT EXISTS idx_changes_path ON changes(path);
CREATE INDEX IF NOT EXISTS idx_changes_inode ON changes(inode);
"#;

// Change types enum
pub const CHANGE_ADDED: i32 = 0;
pub const CHANGE_MODIFIED: i32 = 1;
pub const CHANGE_DELETED: i32 = 2;
pub const CHANGE_RENAMED: i32 = 3;
```

Add `change_info` table for metadata:
```sql
CREATE TABLE IF NOT EXISTS change_info (
    key TEXT PRIMARY KEY,
    value TEXT
);
-- Keys to store:
-- baseline_path: path to baseline DB
-- baseline_scan_time: when baseline was created
-- incremental_scan_time: when this scan ran
-- added_count, modified_count, deleted_count, renamed_count
-- source_url: NFS URL scanned
-- duration_secs: how long the incremental scan took
```

---

## CLI Changes

### File: `src/config.rs`

Add to `CliArgs` struct (around line 31):
```rust
/// Path to baseline database for incremental comparison
#[arg(long, value_name = "PATH")]
pub baseline: Option<PathBuf>,
```

Add to `WalkConfig` struct (around line 255):
```rust
pub baseline_path: Option<PathBuf>,
```

Add validation in `WalkConfig::from_args()`:
```rust
// Validate baseline exists and is a valid SQLite DB
if let Some(ref baseline) = args.baseline {
    if !baseline.exists() {
        return Err(ConfigError::Validation(
            format!("Baseline database not found: {}", baseline.display())
        ));
    }
    // Optionally: verify it's a valid nfs-walker DB by checking for entries table
}
```

---

## New Module: `src/walker/incremental.rs`

### Data Structures

```rust
use std::collections::HashMap;
use rusqlite::Connection;

/// Lightweight entry from baseline for comparison
#[derive(Clone)]
struct BaselineEntry {
    inode: u64,
    mtime: i64,
    size: u64,
    entry_type: i32,
}

/// Change to be written to output DB
pub struct Change {
    pub change_type: ChangeType,
    pub path: String,
    pub old_path: Option<String>,  // For renames
    pub entry: DbEntry,            // Current filesystem state (or baseline state for deletes)
}

#[derive(Clone, Copy, PartialEq)]
pub enum ChangeType {
    Added = 0,
    Modified = 1,
    Deleted = 2,
    Renamed = 3,
}

/// In-memory index of baseline DB
pub struct BaselineIndex {
    /// path -> baseline entry (for detecting modifications and tracking seen paths)
    path_index: HashMap<String, BaselineEntry>,
    /// inode -> path (for detecting renames)
    inode_to_path: HashMap<u64, String>,
}
```

### Loading Baseline Index

```rust
impl BaselineIndex {
    pub fn load(db_path: &Path) -> Result<Self, WalkerError> {
        let conn = Connection::open(db_path)?;

        let mut path_index = HashMap::new();
        let mut inode_to_path = HashMap::new();

        let mut stmt = conn.prepare(
            "SELECT path, inode, mtime, size, entry_type FROM entries"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,      // path
                row.get::<_, u64>(1)?,         // inode
                row.get::<_, i64>(2)?,         // mtime
                row.get::<_, u64>(3)?,         // size
                row.get::<_, i32>(4)?,         // entry_type
            ))
        })?;

        for row in rows {
            let (path, inode, mtime, size, entry_type) = row?;

            path_index.insert(path.clone(), BaselineEntry {
                inode,
                mtime,
                size,
                entry_type,
            });

            // For rename detection - map inode to path
            // Note: for hardlinks, last path wins (acceptable tradeoff)
            inode_to_path.insert(inode, path);
        }

        Ok(Self { path_index, inode_to_path })
    }

    /// Check an entry from current filesystem against baseline
    /// Returns Some(Change) if different, None if unchanged
    /// Also marks the path as "seen" by removing from path_index
    pub fn check_entry(&mut self, entry: &DbEntry) -> Option<Change> {
        let path = &entry.path;

        // Try to find by path first
        if let Some(baseline) = self.path_index.remove(path) {
            // Path exists in baseline - check if modified
            let dominated = entry.mtime.unwrap_or(0) != baseline.mtime
                         || entry.size != baseline.size as u64;

            if modified {
                return Some(Change {
                    change_type: ChangeType::Modified,
                    path: path.clone(),
                    old_path: None,
                    entry: entry.clone(),
                });
            }
            // Unchanged - return None
            return None;
        }

        // Path not in baseline - check if it's a rename (same inode, different path)
        if let Some(old_path) = self.inode_to_path.get(&entry.inode) {
            if old_path != path {
                // Found same inode at different path = rename
                // Remove old path from path_index so it's not reported as deleted
                let old_baseline = self.path_index.remove(old_path);

                // Check if also modified during rename
                let change_type = if let Some(baseline) = old_baseline {
                    // Could optionally check if content changed during rename
                    ChangeType::Renamed
                } else {
                    ChangeType::Renamed
                };

                return Some(Change {
                    change_type,
                    path: path.clone(),
                    old_path: Some(old_path.clone()),
                    entry: entry.clone(),
                });
            }
        }

        // Path not in baseline and inode not seen = new file
        Some(Change {
            change_type: ChangeType::Added,
            path: path.clone(),
            old_path: None,
            entry: entry.clone(),
        })
    }

    /// After walking is complete, remaining entries in path_index are deleted
    pub fn get_deleted_entries(&self) -> impl Iterator<Item = Change> + '_ {
        self.path_index.iter().map(|(path, baseline)| {
            Change {
                change_type: ChangeType::Deleted,
                path: path.clone(),
                old_path: None,
                entry: DbEntry {
                    parent_path: None,
                    name: path.rsplit('/').next().unwrap_or(path).to_string(),
                    path: path.clone(),
                    entry_type: EntryType::from_i32(baseline.entry_type),
                    size: baseline.size,
                    mtime: Some(baseline.mtime),
                    atime: None,
                    ctime: None,
                    mode: None,
                    uid: None,
                    gid: None,
                    nlink: None,
                    inode: baseline.inode,
                    depth: path.matches('/').count() as u32,
                },
            }
        })
    }
}
```

### Incremental Walker

The incremental walker should be similar to `SimpleWalker` but with these modifications:

```rust
pub struct IncrementalWalker {
    config: WalkConfig,
    baseline: Arc<Mutex<BaselineIndex>>,  // Shared across workers
}

impl IncrementalWalker {
    pub fn new(config: WalkConfig, baseline_path: &Path) -> Result<Self, WalkerError> {
        let baseline = BaselineIndex::load(baseline_path)?;
        Ok(Self {
            config,
            baseline: Arc::new(Mutex::new(baseline)),
        })
    }

    pub fn run(&self) -> Result<IncrementalStats, WalkerError> {
        // ... similar to SimpleWalker::run() but:
        // 1. Creates changes table instead of entries table
        // 2. Worker sends entries through baseline.check_entry() before sending to writer
        // 3. After walk completes, calls baseline.get_deleted_entries() and writes those
    }
}
```

### Key Differences from SimpleWalker

1. **Worker loop modification** (compare to `src/walker/simple.rs` lines 354-544):
   ```rust
   // Instead of sending all entries to writer:
   for entry in entries {
       // Lock baseline index and check entry
       let change = {
           let mut baseline = self.baseline.lock().unwrap();
           baseline.check_entry(&entry)
       };

       // Only send if there's a change
       if let Some(change) = change {
           batch.push(change);
       }
   }
   ```

2. **Writer loop modification** (compare to `src/walker/simple.rs` lines 547-625):
   ```rust
   // Insert into changes table instead of entries table
   let mut stmt = conn.prepare(
       "INSERT INTO changes (change_type, path, old_path, entry_type, size, mtime,
        atime, ctime, mode, uid, gid, nlink, inode, depth)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
   )?;

   for change in batch {
       stmt.execute(params![
           change.change_type as i32,
           change.path,
           change.old_path,
           change.entry.entry_type as i32,
           change.entry.size,
           // ... etc
       ])?;
   }
   ```

3. **Finalization** - after walk completes:
   ```rust
   // Get all deleted entries from baseline
   let baseline = self.baseline.lock().unwrap();
   let deleted: Vec<_> = baseline.get_deleted_entries().collect();
   drop(baseline);  // Release lock

   // Write deleted entries to changes DB
   for change in deleted {
       // ... insert with change_type = DELETED
   }
   ```

---

## Main Entry Point Changes

### File: `src/main.rs`

Around line 80, add dispatch logic:

```rust
let stats = if let Some(ref baseline_path) = config.baseline_path {
    // Incremental mode
    info!("Running incremental scan against baseline: {}", baseline_path.display());

    let walker = IncrementalWalker::new(config.clone(), baseline_path)?;

    if config.show_progress {
        walker.run_with_progress(|progress| {
            // ... same progress callback
        })?
    } else {
        walker.run()?
    }
} else {
    // Full scan mode (existing code)
    let walker = SimpleWalker::new(config.clone())?;
    // ...
};
```

---

## Module Exports

### File: `src/walker/mod.rs`

Add:
```rust
mod incremental;
pub use incremental::{IncrementalWalker, IncrementalStats, ChangeType};
```

---

## Statistics and Output

### IncrementalStats struct

```rust
pub struct IncrementalStats {
    pub added: u64,
    pub modified: u64,
    pub deleted: u64,
    pub renamed: u64,
    pub unchanged: u64,
    pub errors: u64,
    pub duration: Duration,
}
```

### Summary output in main.rs

```rust
if let Some(_) = config.baseline_path {
    println!("\nIncremental scan complete:");
    println!("  Added:     {:>12}", stats.added);
    println!("  Modified:  {:>12}", stats.modified);
    println!("  Deleted:   {:>12}", stats.deleted);
    println!("  Renamed:   {:>12}", stats.renamed);
    println!("  Unchanged: {:>12}", stats.unchanged);
    println!("  Errors:    {:>12}", stats.errors);
    println!("  Duration:  {:>12.2}s", stats.duration.as_secs_f64());
}
```

---

## Concurrency Considerations

### Baseline Index Locking

The baseline index needs to be accessed by all worker threads. Options:

**Option A: Mutex (simpler, some contention)**
```rust
baseline: Arc<Mutex<BaselineIndex>>
```
Each worker locks, checks entry, unlocks. Contention is brief since check_entry() is fast.

**Option B: DashMap (concurrent HashMap)**
```rust
// Using dashmap crate
path_index: Arc<DashMap<String, BaselineEntry>>
inode_to_path: Arc<DashMap<u64, String>>
```
Better for high contention, but adds dependency.

**Option C: Sharded by path prefix**
Partition the index by first character of path. Each worker handles paths in its shard. More complex but zero contention.

**Recommendation**: Start with Mutex (Option A). The critical section is small (~100ns), and with 8-16 workers the contention should be acceptable. Profile and optimize if needed.

---

## Memory Usage Estimates

For baseline index:
- Each entry: ~100 bytes (path string ~60 bytes avg + inode/mtime/size/entry_type + HashMap overhead)
- 100 million files: ~10 GB RAM
- 10 million files: ~1 GB RAM

For very large filesystems (>100M files), consider:
1. Memory-mapped baseline queries (slower but bounded memory)
2. Bloom filter for quick "definitely not in baseline" checks
3. LRU cache for hot paths

---

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_baseline_index_load() {
        // Create temp DB with known entries
        // Load baseline index
        // Verify path_index and inode_to_path populated correctly
    }

    #[test]
    fn test_detect_added() {
        // Entry not in baseline -> Added
    }

    #[test]
    fn test_detect_modified_mtime() {
        // Entry in baseline with different mtime -> Modified
    }

    #[test]
    fn test_detect_modified_size() {
        // Entry in baseline with different size -> Modified
    }

    #[test]
    fn test_detect_unchanged() {
        // Entry in baseline with same mtime and size -> None (unchanged)
    }

    #[test]
    fn test_detect_renamed() {
        // Same inode at different path -> Renamed
    }

    #[test]
    fn test_detect_deleted() {
        // After walk, remaining entries -> Deleted
    }
}
```

### Integration Tests

1. **Idempotent scan**: Run full scan, then incremental with same baseline → expect 0 changes
2. **Add file**: Touch new file, incremental → expect 1 added
3. **Modify file**: Change existing file, incremental → expect 1 modified
4. **Delete file**: Remove file, incremental → expect 1 deleted
5. **Rename file**: `mv file1 file2`, incremental → expect 1 renamed
6. **Performance**: Compare incremental vs full scan time on large directory

---

## Example Usage

```bash
# Initial full scan
nfs-walker nfs://server/export -o baseline.db -p

# Later, incremental scan
nfs-walker nfs://server/export -o changes.db --baseline baseline.db -p

# Inspect changes
sqlite3 changes.db "SELECT change_type, path, old_path FROM changes ORDER BY change_type"

# Count by type
sqlite3 changes.db "SELECT
    CASE change_type
        WHEN 0 THEN 'added'
        WHEN 1 THEN 'modified'
        WHEN 2 THEN 'deleted'
        WHEN 3 THEN 'renamed'
    END as type,
    COUNT(*)
FROM changes GROUP BY change_type"
```

---

## Future Enhancements (Out of Scope)

1. **Content hashing**: Detect modifications via checksum instead of mtime (expensive)
2. **Incremental update**: Instead of new DB, update baseline in-place with changelog
3. **Watch mode**: Use inotify/fsnotify to continuously track changes
4. **Delta compression**: Store only changed bytes for modified files
5. **Parallel baseline loading**: Split baseline DB load across threads
