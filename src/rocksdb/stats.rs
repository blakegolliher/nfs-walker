//! RocksDB statistics and queries
//!
//! Compute common filesystem statistics directly from RocksDB without conversion.

use crate::error::RocksError;
use crate::rocksdb::schema::RocksHandle;
use std::collections::HashMap;
use std::path::Path;

/// Statistics about files grouped by extension
#[derive(Debug, Clone, Default)]
pub struct ExtensionStats {
    pub extension: String,
    pub count: u64,
    pub total_bytes: u64,
    pub total_blocks: u64,
}

/// Overall database statistics
#[derive(Debug, Clone, Default)]
pub struct DbStats {
    pub total_entries: u64,
    pub total_files: u64,
    pub total_dirs: u64,
    pub total_symlinks: u64,
    pub total_bytes: u64,
    pub total_blocks: u64,
    pub max_depth: u32,
}

/// Compute statistics from a RocksDB database
pub fn compute_stats<P: AsRef<Path>>(path: P) -> Result<DbStats, RocksError> {
    let handle = RocksHandle::open_readonly(path).map_err(RocksError::Rocks)?;
    let mut stats = DbStats::default();

    for result in handle.iter_by_path() {
        let entry = result?;
        stats.total_entries += 1;

        match entry.entry_type {
            0 => {
                // File
                stats.total_files += 1;
                stats.total_bytes += entry.size;
                stats.total_blocks += entry.blocks;
            }
            1 => stats.total_dirs += 1,     // Directory
            2 => stats.total_symlinks += 1, // Symlink
            _ => {}
        }

        if entry.depth > stats.max_depth {
            stats.max_depth = entry.depth;
        }
    }

    Ok(stats)
}

/// Compute file statistics grouped by extension
pub fn stats_by_extension<P: AsRef<Path>>(
    path: P,
    top_n: usize,
) -> Result<Vec<ExtensionStats>, RocksError> {
    let handle = RocksHandle::open_readonly(path).map_err(RocksError::Rocks)?;
    let mut ext_map: HashMap<String, ExtensionStats> = HashMap::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        // Only count files
        if entry.entry_type != 0 {
            continue;
        }

        let ext = entry.extension.unwrap_or_default();
        let stats = ext_map.entry(ext.clone()).or_insert_with(|| ExtensionStats {
            extension: ext,
            count: 0,
            total_bytes: 0,
            total_blocks: 0,
        });

        stats.count += 1;
        stats.total_bytes += entry.size;
        stats.total_blocks += entry.blocks;
    }

    // Sort by total bytes descending
    let mut results: Vec<_> = ext_map.into_values().collect();
    results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
    results.truncate(top_n);

    Ok(results)
}

/// Find the largest files
pub fn largest_files<P: AsRef<Path>>(
    path: P,
    top_n: usize,
) -> Result<Vec<(String, u64)>, RocksError> {
    let handle = RocksHandle::open_readonly(path).map_err(RocksError::Rocks)?;
    let mut files: Vec<(String, u64)> = Vec::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        // Only count files
        if entry.entry_type != 0 {
            continue;
        }

        // Keep track of largest files
        if files.len() < top_n || entry.size > files.last().map(|f| f.1).unwrap_or(0) {
            files.push((entry.path.clone(), entry.size));
            files.sort_by(|a, b| b.1.cmp(&a.1));
            files.truncate(top_n);
        }
    }

    Ok(files)
}

/// Find directories with the most direct children
pub fn largest_directories<P: AsRef<Path>>(
    path: P,
    top_n: usize,
) -> Result<Vec<(String, u64)>, RocksError> {
    let handle = RocksHandle::open_readonly(path).map_err(RocksError::Rocks)?;
    let mut dir_counts: HashMap<String, u64> = HashMap::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        // Get parent directory
        if let Some(pos) = entry.path.rfind('/') {
            let parent = if pos == 0 {
                "/".to_string()
            } else {
                entry.path[..pos].to_string()
            };
            *dir_counts.entry(parent).or_insert(0) += 1;
        }
    }

    // Sort by count descending
    let mut results: Vec<_> = dir_counts.into_iter().collect();
    results.sort_by(|a, b| b.1.cmp(&a.1));
    results.truncate(top_n);

    Ok(results)
}

/// Find oldest files by mtime
pub fn oldest_files<P: AsRef<Path>>(
    path: P,
    top_n: usize,
) -> Result<Vec<(String, Option<i64>, u64)>, RocksError> {
    let handle = RocksHandle::open_readonly(path).map_err(RocksError::Rocks)?;
    let mut files: Vec<(String, Option<i64>, u64)> = Vec::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        // Only files
        if entry.entry_type != 0 {
            continue;
        }

        // Keep track of oldest files (smallest mtime)
        let dominated = files.len() >= top_n
            && entry.mtime >= files.last().and_then(|f| f.1);

        if !dominated {
            files.push((entry.path.clone(), entry.mtime, entry.size));
            files.sort_by(|a, b| a.1.cmp(&b.1));
            files.truncate(top_n);
        }
    }

    Ok(files)
}

/// Find files with the most hard links
pub fn most_hardlinks<P: AsRef<Path>>(
    path: P,
    top_n: usize,
) -> Result<Vec<(String, u64, u64)>, RocksError> {
    let handle = RocksHandle::open_readonly(path).map_err(RocksError::Rocks)?;
    let mut files: Vec<(String, u64, u64)> = Vec::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        // Only files
        if entry.entry_type != 0 {
            continue;
        }

        let nlink = entry.nlink.unwrap_or(1);

        // Keep track of files with most links
        if files.len() < top_n || nlink > files.last().map(|f| f.1).unwrap_or(0) {
            files.push((entry.path.clone(), nlink, entry.size));
            files.sort_by(|a, b| b.1.cmp(&a.1));
            files.truncate(top_n);
        }
    }

    Ok(files)
}

/// Statistics for a specific user
#[derive(Debug, Clone, Default)]
pub struct OwnerStats {
    pub id: u32,
    pub file_count: u64,
    pub dir_count: u64,
    pub total_bytes: u64,
}

/// Get file statistics by user ID
pub fn stats_by_uid<P: AsRef<Path>>(
    path: P,
    top_n: usize,
) -> Result<Vec<OwnerStats>, RocksError> {
    let handle = RocksHandle::open_readonly(path).map_err(RocksError::Rocks)?;
    let mut uid_map: HashMap<u32, OwnerStats> = HashMap::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        let uid = entry.uid.unwrap_or(0);
        let stats = uid_map.entry(uid).or_insert_with(|| OwnerStats {
            id: uid,
            ..Default::default()
        });

        match entry.entry_type {
            0 => {
                stats.file_count += 1;
                stats.total_bytes += entry.size;
            }
            1 => stats.dir_count += 1,
            _ => {}
        }
    }

    // Sort by total bytes descending
    let mut results: Vec<_> = uid_map.into_values().collect();
    results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
    results.truncate(top_n);

    Ok(results)
}

/// Get file statistics by group ID
pub fn stats_by_gid<P: AsRef<Path>>(
    path: P,
    top_n: usize,
) -> Result<Vec<OwnerStats>, RocksError> {
    let handle = RocksHandle::open_readonly(path).map_err(RocksError::Rocks)?;
    let mut gid_map: HashMap<u32, OwnerStats> = HashMap::new();

    for result in handle.iter_by_path() {
        let entry = result?;

        let gid = entry.gid.unwrap_or(0);
        let stats = gid_map.entry(gid).or_insert_with(|| OwnerStats {
            id: gid,
            ..Default::default()
        });

        match entry.entry_type {
            0 => {
                stats.file_count += 1;
                stats.total_bytes += entry.size;
            }
            1 => stats.dir_count += 1,
            _ => {}
        }
    }

    // Sort by total bytes descending
    let mut results: Vec<_> = gid_map.into_values().collect();
    results.sort_by(|a, b| b.total_bytes.cmp(&a.total_bytes));
    results.truncate(top_n);

    Ok(results)
}
