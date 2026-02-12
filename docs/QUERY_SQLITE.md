# Querying SQLite Scans

After converting RocksDB to SQLite (or using `--sqlite` directly), you can run arbitrary SQL queries.

## Converting from RocksDB

```bash
nfs-walker convert scan.rocks scan.db --progress
```

## Basic Queries

```bash
sqlite3 scan.db
```

### Overview Statistics

```sql
SELECT
    COUNT(*) as total_entries,
    SUM(CASE WHEN entry_type = 0 THEN 1 ELSE 0 END) as files,
    SUM(CASE WHEN entry_type = 1 THEN 1 ELSE 0 END) as directories,
    SUM(CASE WHEN entry_type = 2 THEN 1 ELSE 0 END) as symlinks,
    ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as total_gb,
    ROUND(SUM(blocks) * 512.0 / 1024 / 1024 / 1024, 2) as allocated_gb,
    MAX(depth) as max_depth
FROM entries;
```

### Files by Extension

```sql
SELECT
    COALESCE(extension, '(none)') as ext,
    COUNT(*) as count,
    ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as size_gb,
    ROUND(SUM(blocks) * 512.0 / 1024 / 1024 / 1024, 2) as allocated_gb
FROM entries
WHERE entry_type = 0
GROUP BY extension
ORDER BY SUM(size) DESC
LIMIT 20;
```

### Largest Files

```sql
SELECT
    path,
    ROUND(size / 1024.0 / 1024 / 1024, 2) as size_gb,
    datetime(mtime, 'unixepoch', 'localtime') as modified
FROM entries
WHERE entry_type = 0
ORDER BY size DESC
LIMIT 20;
```

### Directories with Most Files

```sql
SELECT
    parent_path,
    COUNT(*) as file_count,
    ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as size_gb
FROM entries
WHERE entry_type = 0
GROUP BY parent_path
ORDER BY file_count DESC
LIMIT 20;
```

### Oldest Files

```sql
SELECT
    path,
    datetime(mtime, 'unixepoch', 'localtime') as modified,
    ROUND(size / 1024.0 / 1024, 2) as size_mb
FROM entries
WHERE entry_type = 0 AND mtime IS NOT NULL
ORDER BY mtime ASC
LIMIT 20;
```

### Files with Most Hard Links

```sql
SELECT
    path,
    nlink as links,
    ROUND(size / 1024.0 / 1024, 2) as size_mb
FROM entries
WHERE entry_type = 0 AND nlink > 1
ORDER BY nlink DESC
LIMIT 20;
```

### Usage by User ID

```sql
SELECT
    uid,
    SUM(CASE WHEN entry_type = 0 THEN 1 ELSE 0 END) as files,
    SUM(CASE WHEN entry_type = 1 THEN 1 ELSE 0 END) as dirs,
    ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as size_gb
FROM entries
GROUP BY uid
ORDER BY SUM(size) DESC
LIMIT 20;
```

### Usage by Group ID

```sql
SELECT
    gid,
    SUM(CASE WHEN entry_type = 0 THEN 1 ELSE 0 END) as files,
    SUM(CASE WHEN entry_type = 1 THEN 1 ELSE 0 END) as dirs,
    ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as size_gb
FROM entries
GROUP BY gid
ORDER BY SUM(size) DESC
LIMIT 20;
```

---

## Advanced Queries

### Space by Top-Level Directory

```sql
SELECT
    CASE
        WHEN depth = 0 THEN '/'
        ELSE '/' || SUBSTR(path, 2, INSTR(SUBSTR(path, 2) || '/', '/') - 1)
    END as top_dir,
    COUNT(*) as files,
    ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as size_gb
FROM entries
WHERE entry_type = 0
GROUP BY top_dir
ORDER BY SUM(size) DESC;
```

### Files Modified in Last N Days

```sql
SELECT
    path,
    ROUND(size / 1024.0 / 1024, 2) as size_mb,
    datetime(mtime, 'unixepoch', 'localtime') as modified
FROM entries
WHERE entry_type = 0
  AND mtime > strftime('%s', 'now', '-7 days')
ORDER BY mtime DESC
LIMIT 100;
```

### Files Not Accessed in Over a Year

```sql
SELECT
    path,
    ROUND(size / 1024.0 / 1024 / 1024, 2) as size_gb,
    datetime(atime, 'unixepoch', 'localtime') as last_accessed
FROM entries
WHERE entry_type = 0
  AND atime IS NOT NULL
  AND atime < strftime('%s', 'now', '-365 days')
ORDER BY size DESC
LIMIT 100;
```

### Disk Usage vs Logical Size (Sparse Files / Compression)

```sql
SELECT
    ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as logical_gb,
    ROUND(SUM(blocks) * 512.0 / 1024 / 1024 / 1024, 2) as allocated_gb,
    ROUND(100.0 * SUM(blocks) * 512.0 / NULLIF(SUM(size), 0), 1) as percent_allocated
FROM entries
WHERE entry_type = 0;
```

### Find Duplicate Files by Checksum

*Requires scan with `-c` flag. This finds true duplicates by content, not just name+size.*

```sql
-- Top duplicate groups by wasted space
SELECT
    checksum,
    COUNT(*) as copies,
    ROUND(size / 1024.0 / 1024, 2) as size_mb,
    ROUND(size * (COUNT(*) - 1) / 1024.0 / 1024 / 1024, 2) as wasted_gb,
    GROUP_CONCAT(path, CHAR(10)) as paths
FROM entries
WHERE entry_type = 0 AND checksum IS NOT NULL
GROUP BY checksum
HAVING COUNT(*) > 1
ORDER BY size * (COUNT(*) - 1) DESC
LIMIT 20;
```

```sql
-- Total wasted space from duplicates
SELECT
    COUNT(*) as duplicate_files,
    ROUND(SUM(wasted) / 1024.0 / 1024 / 1024, 2) as wasted_gb
FROM (
    SELECT size * (COUNT(*) - 1) as wasted
    FROM entries
    WHERE entry_type = 0 AND checksum IS NOT NULL
    GROUP BY checksum
    HAVING COUNT(*) > 1
);
```

### File Type Distribution

*Requires scan with `-t` flag.*

```sql
-- Files grouped by detected MIME type
SELECT
    COALESCE(file_type, '(unknown)') as mime_type,
    COUNT(*) as count,
    ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as size_gb
FROM entries
WHERE entry_type = 0
GROUP BY file_type
ORDER BY SUM(size) DESC
LIMIT 30;
```

```sql
-- Find all PDF files (by content, not extension)
SELECT path, ROUND(size / 1024.0 / 1024, 2) as size_mb
FROM entries
WHERE file_type = 'application/pdf'
ORDER BY size DESC
LIMIT 50;
```

```sql
-- Files where extension doesn't match detected type
SELECT path, extension, file_type, ROUND(size / 1024.0 / 1024, 2) as size_mb
FROM entries
WHERE entry_type = 0
  AND file_type IS NOT NULL
  AND extension IS NOT NULL
  AND (
    (extension IN ('jpg', 'jpeg') AND file_type != 'image/jpeg')
    OR (extension = 'png' AND file_type != 'image/png')
    OR (extension = 'pdf' AND file_type != 'application/pdf')
    OR (extension = 'mp4' AND file_type != 'video/mp4')
  )
LIMIT 50;
```

### Hard Link Groups

```sql
-- Files sharing the same inode (hard links)
SELECT
    inode,
    nlink,
    COUNT(*) as found_paths,
    ROUND(size / 1024.0 / 1024, 2) as size_mb,
    GROUP_CONCAT(path, CHAR(10)) as paths
FROM entries
WHERE entry_type = 0 AND nlink > 1
GROUP BY inode
HAVING COUNT(*) > 1
ORDER BY nlink DESC
LIMIT 20;
```

### Find Duplicate Files by Name + Size (no checksum needed)

```sql
SELECT
    name,
    size,
    COUNT(*) as copies,
    GROUP_CONCAT(path, CHAR(10)) as paths
FROM entries
WHERE entry_type = 0 AND size > 0
GROUP BY name, size
HAVING COUNT(*) > 1
ORDER BY size * COUNT(*) DESC
LIMIT 20;
```

### Directory Tree Size (Recursive)

```sql
WITH RECURSIVE dir_tree AS (
    SELECT path, size, 0 as is_dir
    FROM entries WHERE entry_type = 0
    UNION ALL
    SELECT parent_path, size, 1
    FROM dir_tree
    WHERE parent_path IS NOT NULL
)
SELECT
    path,
    COUNT(*) as file_count,
    ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as total_gb
FROM dir_tree
WHERE is_dir = 1
GROUP BY path
ORDER BY SUM(size) DESC
LIMIT 20;
```

### Permission Analysis

```sql
SELECT
    printf('%o', mode & 511) as permissions,
    COUNT(*) as count,
    ROUND(SUM(size) / 1024.0 / 1024 / 1024, 2) as size_gb
FROM entries
WHERE entry_type = 0 AND mode IS NOT NULL
GROUP BY permissions
ORDER BY count DESC
LIMIT 20;
```

### World-Writable Files

```sql
SELECT path, printf('%o', mode & 511) as permissions
FROM entries
WHERE entry_type = 0
  AND mode IS NOT NULL
  AND (mode & 2) = 2  -- other-write bit set
LIMIT 100;
```

---

## Exporting Data

### To CSV

```bash
# All files
sqlite3 -header -csv scan.db \
  "SELECT path, size, mtime FROM entries WHERE entry_type = 0" \
  > files.csv

# Large files only
sqlite3 -header -csv scan.db \
  "SELECT path, size/1024/1024/1024 as gb FROM entries
   WHERE entry_type = 0 AND size > 1073741824
   ORDER BY size DESC" \
  > large_files.csv

# Directory sizes
sqlite3 -header -csv scan.db \
  "SELECT parent_path, COUNT(*) as files, SUM(size)/1024/1024/1024 as gb
   FROM entries WHERE entry_type = 0
   GROUP BY parent_path ORDER BY SUM(size) DESC" \
  > dir_sizes.csv
```

### To JSON

```bash
sqlite3 -json scan.db \
  "SELECT path, size, mtime FROM entries LIMIT 100" \
  > sample.json
```

### To Plain Text

```bash
# Just paths
sqlite3 scan.db "SELECT path FROM entries WHERE size > 1073741824" > large_files.txt

# Tab-separated
sqlite3 -separator $'\t' scan.db \
  "SELECT path, size FROM entries WHERE entry_type = 0" \
  > files.tsv
```

---

## Schema Reference

```sql
CREATE TABLE entries (
    id INTEGER PRIMARY KEY,
    parent_path TEXT,            -- Parent directory path
    name TEXT NOT NULL,          -- Filename only
    path TEXT NOT NULL,          -- Full path
    entry_type INTEGER NOT NULL, -- 0=file, 1=dir, 2=symlink
    size INTEGER DEFAULT 0,      -- Logical size (bytes)
    mtime INTEGER,               -- Modification time (unix epoch)
    atime INTEGER,               -- Access time (unix epoch)
    ctime INTEGER,               -- Change time (unix epoch)
    mode INTEGER,                -- Permission bits
    uid INTEGER,                 -- Owner user ID
    gid INTEGER,                 -- Owner group ID
    nlink INTEGER,               -- Hard link count
    inode INTEGER,               -- Inode number
    depth INTEGER NOT NULL,      -- Depth from root (0 = root)
    extension TEXT,              -- File extension (lowercase, no dot)
    blocks INTEGER DEFAULT 0,    -- 512-byte blocks allocated
    checksum TEXT,               -- gxhash checksum (32-char hex, set with -c flag)
    file_type TEXT               -- MIME type (e.g. "application/pdf", set with -t flag)
);

CREATE TABLE walk_info (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Indexes
CREATE INDEX idx_entries_path ON entries(path);
CREATE INDEX idx_entries_parent ON entries(parent_path);
CREATE INDEX idx_entries_type ON entries(entry_type);
CREATE INDEX idx_entries_size ON entries(size) WHERE entry_type = 0;
CREATE INDEX idx_entries_depth ON entries(depth);
CREATE INDEX idx_entries_ext ON entries(extension) WHERE entry_type = 0;
CREATE INDEX idx_entries_inode ON entries(inode);
CREATE INDEX idx_entries_checksum ON entries(checksum) WHERE checksum IS NOT NULL;
CREATE INDEX idx_entries_file_type ON entries(file_type) WHERE file_type IS NOT NULL;
```

---

## Performance Tips

1. **Use indexes**: Queries on `path`, `parent_path`, `size`, `extension`, `inode`, `checksum`, and `file_type` are indexed
2. **Filter by entry_type first**: Most queries only need files (`entry_type = 0`)
3. **LIMIT early**: Add LIMIT to avoid scanning entire table
4. **Use EXPLAIN QUERY PLAN**: Check if your query uses indexes
   ```sql
   EXPLAIN QUERY PLAN SELECT * FROM entries WHERE size > 1000000000;
   ```
