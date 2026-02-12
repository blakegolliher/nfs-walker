//! Static query catalog for the analytics server.
//!
//! Contains ~25 P0 queries across 9 categories, with parameterized SQL
//! templates and safe parameter binding.

use crate::error::ServerError;
use std::collections::HashMap;

/// Category of analytics query
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryCategory {
    CapacityVolume,
    OwnershipAccess,
    DirectoryStructure,
    FileSize,
    FileTypeExtension,
    TimeBased,
    StorageEfficiency,
    MigrationCompliance,
    ScanOperations,
}

impl QueryCategory {
    pub fn label(&self) -> &'static str {
        match self {
            Self::CapacityVolume => "Capacity & Volume",
            Self::OwnershipAccess => "Ownership & Access",
            Self::DirectoryStructure => "Directory Structure",
            Self::FileSize => "File Size",
            Self::FileTypeExtension => "File Type & Extension",
            Self::TimeBased => "Time-Based",
            Self::StorageEfficiency => "Storage Efficiency",
            Self::MigrationCompliance => "Migration & Compliance",
            Self::ScanOperations => "Scan Operations",
        }
    }
}

/// Type of a query parameter
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParamType {
    Integer,
    String,
}

/// Definition of a query parameter
#[derive(Debug, Clone, serde::Serialize)]
pub struct ParamDef {
    pub name: &'static str,
    pub description: &'static str,
    pub param_type: ParamType,
    pub default: Option<&'static str>,
    pub required: bool,
}

/// A query definition in the catalog
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryDef {
    pub id: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub category: QueryCategory,
    pub sql_template: &'static str,
    pub params: &'static [ParamDef],
}

// ─── Capacity & Volume ───────────────────────────────────────────

const Q001: QueryDef = QueryDef {
    id: "Q-001",
    name: "Total Entry Counts",
    description: "Count of all entries grouped by file type",
    category: QueryCategory::CapacityVolume,
    sql_template: "SELECT file_type, COUNT(*) as count, \
        SUM(size) as total_bytes, \
        SUM(allocated_blocks * 512) as allocated_bytes \
        FROM {table} GROUP BY file_type ORDER BY count DESC",
    params: &[],
};

const Q002: QueryDef = QueryDef {
    id: "Q-002",
    name: "Total Capacity Summary",
    description: "Overall storage capacity summary",
    category: QueryCategory::CapacityVolume,
    sql_template: "SELECT \
        COUNT(*) as total_entries, \
        COUNT(CASE WHEN file_type = 'file' THEN 1 END) as files, \
        COUNT(CASE WHEN file_type = 'directory' THEN 1 END) as directories, \
        COUNT(CASE WHEN file_type = 'symlink' THEN 1 END) as symlinks, \
        SUM(size) as total_bytes, \
        SUM(allocated_blocks * 512) as allocated_bytes \
        FROM {table}",
    params: &[],
};

const Q003: QueryDef = QueryDef {
    id: "Q-003",
    name: "Capacity by Depth",
    description: "Storage consumption grouped by directory depth",
    category: QueryCategory::CapacityVolume,
    sql_template: "SELECT depth, \
        COUNT(*) as entries, \
        COUNT(CASE WHEN file_type = 'file' THEN 1 END) as files, \
        SUM(size) as total_bytes \
        FROM {table} GROUP BY depth ORDER BY depth",
    params: &[],
};

const Q004: QueryDef = QueryDef {
    id: "Q-004",
    name: "Inodes by Type",
    description: "Inode count breakdown by file type",
    category: QueryCategory::CapacityVolume,
    sql_template: "SELECT file_type, COUNT(DISTINCT inode) as unique_inodes, COUNT(*) as entries \
        FROM {table} GROUP BY file_type ORDER BY unique_inodes DESC",
    params: &[],
};

const Q005: QueryDef = QueryDef {
    id: "Q-005",
    name: "Capacity by Top-Level Directory",
    description: "Storage usage per top-level directory",
    category: QueryCategory::CapacityVolume,
    sql_template: "SELECT \
        CASE WHEN depth >= 1 THEN split_part(path, '/', 2) ELSE '/' END as top_dir, \
        COUNT(*) as entries, \
        SUM(size) as total_bytes, \
        SUM(allocated_blocks * 512) as allocated_bytes \
        FROM {table} WHERE file_type = 'file' \
        GROUP BY top_dir ORDER BY total_bytes DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q006: QueryDef = QueryDef {
    id: "Q-006",
    name: "Hard Link Summary",
    description: "Files with multiple hard links (nlink > 1)",
    category: QueryCategory::CapacityVolume,
    sql_template: "SELECT nlink, COUNT(*) as count, SUM(size) as total_bytes \
        FROM {table} WHERE file_type = 'file' AND nlink > 1 \
        GROUP BY nlink ORDER BY nlink DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q007: QueryDef = QueryDef {
    id: "Q-007",
    name: "Duplicate Inode Detection",
    description: "Inodes appearing in multiple paths (hard links)",
    category: QueryCategory::CapacityVolume,
    sql_template: "SELECT inode, COUNT(*) as link_count, \
        MIN(path) as example_path, MAX(size) as size \
        FROM {table} WHERE file_type = 'file' \
        GROUP BY inode HAVING COUNT(*) > 1 \
        ORDER BY link_count DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q008: QueryDef = QueryDef {
    id: "Q-008",
    name: "Space Waste from Allocation",
    description: "Difference between logical size and allocated space",
    category: QueryCategory::CapacityVolume,
    sql_template: "SELECT \
        SUM(size) as logical_bytes, \
        SUM(allocated_blocks * 512) as allocated_bytes, \
        SUM(allocated_blocks * 512) - SUM(size) as waste_bytes, \
        CASE WHEN SUM(allocated_blocks * 512) > 0 \
            THEN ROUND(CAST(SUM(allocated_blocks * 512) - SUM(size) AS DOUBLE) / CAST(SUM(allocated_blocks * 512) AS DOUBLE) * 100, 2) \
            ELSE 0 END as waste_pct \
        FROM {table} WHERE file_type = 'file'",
    params: &[],
};

// ─── Ownership & Access ──────────────────────────────────────────

const Q010: QueryDef = QueryDef {
    id: "Q-010",
    name: "Usage by UID",
    description: "Storage consumption per user ID",
    category: QueryCategory::OwnershipAccess,
    sql_template: "SELECT uid, \
        COUNT(*) as entries, \
        COUNT(CASE WHEN file_type = 'file' THEN 1 END) as files, \
        SUM(size) as total_bytes \
        FROM {table} GROUP BY uid ORDER BY total_bytes DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q011: QueryDef = QueryDef {
    id: "Q-011",
    name: "Usage by GID",
    description: "Storage consumption per group ID",
    category: QueryCategory::OwnershipAccess,
    sql_template: "SELECT gid, \
        COUNT(*) as entries, \
        COUNT(CASE WHEN file_type = 'file' THEN 1 END) as files, \
        SUM(size) as total_bytes \
        FROM {table} GROUP BY gid ORDER BY total_bytes DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q012: QueryDef = QueryDef {
    id: "Q-012",
    name: "World-Writable Files",
    description: "Files with world-write permission bit set (permissions & 0o002)",
    category: QueryCategory::OwnershipAccess,
    sql_template: "SELECT path, size, uid, gid, permissions \
        FROM {table} WHERE file_type = 'file' AND permissions & 2 > 0 \
        ORDER BY size DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q013: QueryDef = QueryDef {
    id: "Q-013",
    name: "Files by UID and Extension",
    description: "Cross-tabulation of file count by UID and extension",
    category: QueryCategory::OwnershipAccess,
    sql_template: "SELECT uid, COALESCE(extension, '(none)') as ext, \
        COUNT(*) as count, SUM(size) as total_bytes \
        FROM {table} WHERE file_type = 'file' \
        GROUP BY uid, ext ORDER BY total_bytes DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q015: QueryDef = QueryDef {
    id: "Q-015",
    name: "Ownership Concentration",
    description: "How concentrated is ownership (top N UIDs by % of total)",
    category: QueryCategory::OwnershipAccess,
    sql_template: "WITH totals AS (SELECT SUM(size) as grand_total FROM {table} WHERE file_type = 'file'), \
        by_uid AS (SELECT uid, SUM(size) as uid_bytes FROM {table} WHERE file_type = 'file' GROUP BY uid) \
        SELECT by_uid.uid, by_uid.uid_bytes, \
        ROUND(CAST(by_uid.uid_bytes AS DOUBLE) / CAST(totals.grand_total AS DOUBLE) * 100, 2) as pct \
        FROM by_uid CROSS JOIN totals ORDER BY uid_bytes DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

// ─── Directory Structure ─────────────────────────────────────────

const Q020: QueryDef = QueryDef {
    id: "Q-020",
    name: "Widest Directories",
    description: "Directories with the most direct children",
    category: QueryCategory::DirectoryStructure,
    sql_template: "SELECT parent_path, COUNT(*) as child_count, \
        SUM(size) as total_bytes \
        FROM {table} GROUP BY parent_path \
        ORDER BY child_count DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q021: QueryDef = QueryDef {
    id: "Q-021",
    name: "Deepest Paths",
    description: "Entries at the greatest directory depth",
    category: QueryCategory::DirectoryStructure,
    sql_template: "SELECT path, depth, file_type, size \
        FROM {table} ORDER BY depth DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q022: QueryDef = QueryDef {
    id: "Q-022",
    name: "Empty Directories",
    description: "Directories that contain no children",
    category: QueryCategory::DirectoryStructure,
    sql_template: "SELECT d.path, d.depth \
        FROM {table} d \
        WHERE d.file_type = 'directory' \
        AND d.path NOT IN (SELECT DISTINCT parent_path FROM {table}) \
        ORDER BY d.depth DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q025: QueryDef = QueryDef {
    id: "Q-025",
    name: "Directory Depth Distribution",
    description: "Count of directories at each depth level",
    category: QueryCategory::DirectoryStructure,
    sql_template: "SELECT depth, COUNT(*) as dir_count \
        FROM {table} WHERE file_type = 'directory' \
        GROUP BY depth ORDER BY depth",
    params: &[],
};

const Q027: QueryDef = QueryDef {
    id: "Q-027",
    name: "Directory Fanout Distribution",
    description: "Distribution of directory child counts (histogram)",
    category: QueryCategory::DirectoryStructure,
    sql_template: "WITH fanout AS ( \
            SELECT parent_path, COUNT(*) as children \
            FROM {table} GROUP BY parent_path \
        ) \
        SELECT \
            CASE \
                WHEN children = 0 THEN '0' \
                WHEN children BETWEEN 1 AND 10 THEN '1-10' \
                WHEN children BETWEEN 11 AND 100 THEN '11-100' \
                WHEN children BETWEEN 101 AND 1000 THEN '101-1K' \
                WHEN children BETWEEN 1001 AND 10000 THEN '1K-10K' \
                WHEN children BETWEEN 10001 AND 100000 THEN '10K-100K' \
                ELSE '100K+' \
            END as bucket, \
            COUNT(*) as dir_count, \
            SUM(children) as total_children, \
            MIN(children) as bucket_min \
        FROM fanout GROUP BY bucket ORDER BY bucket_min ASC",
    params: &[],
};

// ─── File Size ───────────────────────────────────────────────────

const Q030: QueryDef = QueryDef {
    id: "Q-030",
    name: "File Size Histogram",
    description: "Distribution of files by size bucket",
    category: QueryCategory::FileSize,
    sql_template: "SELECT \
        CASE \
            WHEN size = 0 THEN '0 B' \
            WHEN size BETWEEN 1 AND 1023 THEN '1 B - 1 KiB' \
            WHEN size BETWEEN 1024 AND 1048575 THEN '1 KiB - 1 MiB' \
            WHEN size BETWEEN 1048576 AND 10485759 THEN '1 MiB - 10 MiB' \
            WHEN size BETWEEN 10485760 AND 104857599 THEN '10 MiB - 100 MiB' \
            WHEN size BETWEEN 104857600 AND 1073741823 THEN '100 MiB - 1 GiB' \
            WHEN size BETWEEN 1073741824 AND 10737418239 THEN '1 GiB - 10 GiB' \
            ELSE '10 GiB+' \
        END as size_bucket, \
        COUNT(*) as file_count, \
        SUM(size) as total_bytes, \
        MIN(size) as bucket_min \
        FROM {table} WHERE file_type = 'file' \
        GROUP BY size_bucket ORDER BY bucket_min ASC",
    params: &[],
};

const Q031: QueryDef = QueryDef {
    id: "Q-031",
    name: "Largest Files",
    description: "Top N largest files by size",
    category: QueryCategory::FileSize,
    sql_template: "SELECT path, size, uid, gid, \
        COALESCE(extension, '(none)') as ext \
        FROM {table} WHERE file_type = 'file' \
        ORDER BY size DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q032: QueryDef = QueryDef {
    id: "Q-032",
    name: "Zero-Byte Files",
    description: "Files with zero size",
    category: QueryCategory::FileSize,
    sql_template: "SELECT COUNT(*) as count, \
        COALESCE(extension, '(none)') as ext \
        FROM {table} WHERE file_type = 'file' AND size = 0 \
        GROUP BY ext ORDER BY count DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q033: QueryDef = QueryDef {
    id: "Q-033",
    name: "Size Percentiles",
    description: "File size percentiles (p50, p90, p95, p99)",
    category: QueryCategory::FileSize,
    sql_template: "SELECT \
        approx_percentile_cont(size, 0.5) as p50, \
        approx_percentile_cont(size, 0.9) as p90, \
        approx_percentile_cont(size, 0.95) as p95, \
        approx_percentile_cont(size, 0.99) as p99, \
        AVG(size) as mean, \
        MIN(size) as min, \
        MAX(size) as max \
        FROM {table} WHERE file_type = 'file'",
    params: &[],
};

const Q034: QueryDef = QueryDef {
    id: "Q-034",
    name: "Small Files Analysis",
    description: "Count of files smaller than threshold that waste allocation blocks",
    category: QueryCategory::FileSize,
    sql_template: "SELECT \
        COUNT(*) as small_files, \
        SUM(size) as logical_bytes, \
        SUM(allocated_blocks * 512) as allocated_bytes, \
        SUM(allocated_blocks * 512) - SUM(size) as wasted_bytes \
        FROM {table} WHERE file_type = 'file' AND size < {threshold}",
    params: &[PARAM_THRESHOLD],
};

const Q036: QueryDef = QueryDef {
    id: "Q-036",
    name: "Largest Files by Extension",
    description: "Top extensions by total size consumed",
    category: QueryCategory::FileSize,
    sql_template: "SELECT COALESCE(extension, '(none)') as ext, \
        COUNT(*) as file_count, \
        SUM(size) as total_bytes, \
        AVG(size) as avg_size \
        FROM {table} WHERE file_type = 'file' \
        GROUP BY ext ORDER BY total_bytes DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

// ─── File Type & Extension ───────────────────────────────────────

const Q040: QueryDef = QueryDef {
    id: "Q-040",
    name: "Count by Extension",
    description: "File count and size grouped by extension",
    category: QueryCategory::FileTypeExtension,
    sql_template: "SELECT COALESCE(extension, '(none)') as ext, \
        COUNT(*) as count, SUM(size) as total_bytes \
        FROM {table} WHERE file_type = 'file' \
        GROUP BY ext ORDER BY count DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q041: QueryDef = QueryDef {
    id: "Q-041",
    name: "Size by Extension",
    description: "Total storage by file extension",
    category: QueryCategory::FileTypeExtension,
    sql_template: "SELECT COALESCE(extension, '(none)') as ext, \
        COUNT(*) as count, SUM(size) as total_bytes, \
        AVG(size) as avg_size, MAX(size) as max_size \
        FROM {table} WHERE file_type = 'file' \
        GROUP BY ext ORDER BY total_bytes DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

const Q044: QueryDef = QueryDef {
    id: "Q-044",
    name: "Temp and Junk Files",
    description: "Files with temporary/junk extensions (.tmp, .bak, .swp, .log, etc.)",
    category: QueryCategory::FileTypeExtension,
    sql_template: "SELECT COALESCE(extension, '(none)') as ext, \
        COUNT(*) as count, SUM(size) as total_bytes \
        FROM {table} WHERE file_type = 'file' \
        AND extension IN ('tmp', 'bak', 'swp', 'swo', 'log', 'old', 'orig', 'temp', 'cache') \
        GROUP BY ext ORDER BY total_bytes DESC",
    params: &[],
};

// ─── Time-Based ──────────────────────────────────────────────────

const Q050: QueryDef = QueryDef {
    id: "Q-050",
    name: "Age Histogram (mtime)",
    description: "Files grouped by age bucket based on modification time",
    category: QueryCategory::TimeBased,
    sql_template: "SELECT \
        CASE \
            WHEN mtime_us IS NULL THEN 'unknown' \
            WHEN mtime_us >= ({now_us} - 86400000000) THEN 'last 24h' \
            WHEN mtime_us >= ({now_us} - 604800000000) THEN 'last 7d' \
            WHEN mtime_us >= ({now_us} - 2592000000000) THEN 'last 30d' \
            WHEN mtime_us >= ({now_us} - 7776000000000) THEN 'last 90d' \
            WHEN mtime_us >= ({now_us} - 31536000000000) THEN 'last 1y' \
            WHEN mtime_us >= ({now_us} - 94608000000000) THEN '1-3y' \
            WHEN mtime_us >= ({now_us} - 157680000000000) THEN '3-5y' \
            ELSE '5y+' \
        END as age_bucket, \
        COUNT(*) as file_count, \
        SUM(size) as total_bytes, \
        MIN(COALESCE(mtime_us, 0)) as bucket_min \
        FROM {table} WHERE file_type = 'file' \
        GROUP BY age_bucket ORDER BY bucket_min ASC",
    params: &[],
};

const Q051: QueryDef = QueryDef {
    id: "Q-051",
    name: "Cold Data (Untouched Files)",
    description: "Files not modified in over N days",
    category: QueryCategory::TimeBased,
    sql_template: "SELECT COUNT(*) as count, SUM(size) as total_bytes, \
        COALESCE(extension, '(none)') as ext \
        FROM {table} WHERE file_type = 'file' \
        AND mtime_us IS NOT NULL \
        AND mtime_us < ({now_us} - CAST({days} AS BIGINT) * 86400000000) \
        GROUP BY ext ORDER BY total_bytes DESC LIMIT {limit}",
    params: &[PARAM_DAYS, PARAM_LIMIT],
};

const Q052: QueryDef = QueryDef {
    id: "Q-052",
    name: "Hot Data (Recently Modified)",
    description: "Files modified in the last N days",
    category: QueryCategory::TimeBased,
    sql_template: "SELECT COUNT(*) as count, SUM(size) as total_bytes, \
        COALESCE(extension, '(none)') as ext \
        FROM {table} WHERE file_type = 'file' \
        AND mtime_us IS NOT NULL \
        AND mtime_us >= ({now_us} - CAST({days} AS BIGINT) * 86400000000) \
        GROUP BY ext ORDER BY total_bytes DESC LIMIT {limit}",
    params: &[PARAM_DAYS, PARAM_LIMIT],
};

const Q053: QueryDef = QueryDef {
    id: "Q-053",
    name: "Stale Data by Owner",
    description: "UIDs with most data not modified in over N days",
    category: QueryCategory::TimeBased,
    sql_template: "SELECT uid, COUNT(*) as stale_files, SUM(size) as stale_bytes \
        FROM {table} WHERE file_type = 'file' \
        AND mtime_us IS NOT NULL \
        AND mtime_us < ({now_us} - CAST({days} AS BIGINT) * 86400000000) \
        GROUP BY uid ORDER BY stale_bytes DESC LIMIT {limit}",
    params: &[PARAM_DAYS, PARAM_LIMIT],
};

const Q055: QueryDef = QueryDef {
    id: "Q-055",
    name: "Data Growth by Month",
    description: "Bytes created per month based on mtime",
    category: QueryCategory::TimeBased,
    sql_template: "SELECT \
        date_part('year', to_timestamp(mtime_us / 1000000)) as year, \
        date_part('month', to_timestamp(mtime_us / 1000000)) as month, \
        COUNT(*) as files_created, \
        SUM(size) as bytes_created \
        FROM {table} WHERE file_type = 'file' AND mtime_us IS NOT NULL \
        GROUP BY year, month ORDER BY year DESC, month DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

// ─── Storage Efficiency ──────────────────────────────────────────

const Q064: QueryDef = QueryDef {
    id: "Q-064",
    name: "Reclaimable Space Summary",
    description: "Summary of potentially reclaimable space (zero-byte, temp, old files)",
    category: QueryCategory::StorageEfficiency,
    sql_template: "SELECT \
        (SELECT COUNT(*) FROM {table} WHERE file_type = 'file' AND size = 0) as zero_byte_files, \
        (SELECT SUM(size) FROM {table} WHERE file_type = 'file' \
            AND extension IN ('tmp', 'bak', 'swp', 'log', 'old', 'orig', 'temp', 'cache')) as temp_file_bytes, \
        (SELECT COUNT(*) FROM {table} WHERE file_type = 'file' \
            AND extension IN ('tmp', 'bak', 'swp', 'log', 'old', 'orig', 'temp', 'cache')) as temp_file_count, \
        (SELECT SUM(size) FROM {table} WHERE file_type = 'file' \
            AND mtime_us IS NOT NULL AND mtime_us < ({now_us} - 94608000000000)) as data_3y_plus_bytes",
    params: &[],
};

// ─── Migration & Compliance ──────────────────────────────────────

const Q073: QueryDef = QueryDef {
    id: "Q-073",
    name: "Data Classification by Prefix",
    description: "Data volume grouped by top-level directory prefix",
    category: QueryCategory::MigrationCompliance,
    sql_template: "SELECT \
        CASE WHEN depth >= 1 THEN split_part(path, '/', 2) ELSE '/' END as prefix, \
        COUNT(*) as entries, \
        COUNT(CASE WHEN file_type = 'file' THEN 1 END) as files, \
        COUNT(CASE WHEN file_type = 'directory' THEN 1 END) as dirs, \
        SUM(size) as total_bytes \
        FROM {table} GROUP BY prefix ORDER BY total_bytes DESC LIMIT {limit}",
    params: &[PARAM_LIMIT],
};

// ─── Scan Operations ─────────────────────────────────────────────

const Q080: QueryDef = QueryDef {
    id: "Q-080",
    name: "Scan Metadata",
    description: "Basic metadata about the current scan",
    category: QueryCategory::ScanOperations,
    sql_template: "SELECT \
        MIN(scan_id) as scan_id, \
        COUNT(*) as total_entries, \
        COUNT(CASE WHEN file_type = 'file' THEN 1 END) as files, \
        COUNT(CASE WHEN file_type = 'directory' THEN 1 END) as directories, \
        SUM(size) as total_bytes, \
        MAX(depth) as max_depth \
        FROM {table}",
    params: &[],
};

const Q081: QueryDef = QueryDef {
    id: "Q-081",
    name: "Directory Fanout Stats",
    description: "Min/max/avg/median children per directory",
    category: QueryCategory::ScanOperations,
    sql_template: "WITH fanout AS ( \
            SELECT parent_path, COUNT(*) as children \
            FROM {table} GROUP BY parent_path \
        ) \
        SELECT \
            COUNT(*) as total_dirs, \
            MIN(children) as min_children, \
            MAX(children) as max_children, \
            AVG(children) as avg_children, \
            approx_percentile_cont(children, 0.5) as median_children \
        FROM fanout",
    params: &[],
};

// ─── Common parameter definitions ────────────────────────────────

const PARAM_LIMIT: ParamDef = ParamDef {
    name: "limit",
    description: "Maximum number of rows to return",
    param_type: ParamType::Integer,
    default: Some("50"),
    required: false,
};

const PARAM_DAYS: ParamDef = ParamDef {
    name: "days",
    description: "Number of days for time-based filtering",
    param_type: ParamType::Integer,
    default: Some("365"),
    required: false,
};

const PARAM_THRESHOLD: ParamDef = ParamDef {
    name: "threshold",
    description: "Size threshold in bytes",
    param_type: ParamType::Integer,
    default: Some("4096"),
    required: false,
};

// ─── Static catalog ──────────────────────────────────────────────

pub static QUERY_CATALOG: &[QueryDef] = &[
    // Capacity & Volume
    Q001, Q002, Q003, Q004, Q005, Q006, Q007, Q008,
    // Ownership & Access
    Q010, Q011, Q012, Q013, Q015,
    // Directory Structure
    Q020, Q021, Q022, Q025, Q027,
    // File Size
    Q030, Q031, Q032, Q033, Q034, Q036,
    // File Type & Extension
    Q040, Q041, Q044,
    // Time-Based
    Q050, Q051, Q052, Q053, Q055,
    // Storage Efficiency
    Q064,
    // Migration & Compliance
    Q073,
    // Scan Operations
    Q080, Q081,
];

/// Look up a query by ID
pub fn get_query(id: &str) -> Option<&'static QueryDef> {
    QUERY_CATALOG.iter().find(|q| q.id == id)
}

/// Bind parameters into a SQL template.
///
/// - `{table}` is always resolved from the internal table registry (never from user input)
/// - Integer params are parsed and validated
/// - String params are allow-listed (no arbitrary strings in SQL)
pub fn bind_params(
    query: &QueryDef,
    table: &str,
    user_params: &HashMap<String, String>,
) -> Result<String, ServerError> {
    let mut sql = query.sql_template.to_string();

    // Always bind {table} from internal registry
    sql = sql.replace("{table}", table);

    // Inject current timestamp for time-based queries
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;
    sql = sql.replace("{now_us}", &now_us.to_string());

    // Bind declared params
    for param in query.params {
        let placeholder = format!("{{{}}}", param.name);
        if !sql.contains(&placeholder) {
            continue;
        }

        let value = user_params
            .get(param.name)
            .cloned()
            .or_else(|| param.default.map(|d| d.to_string()));

        let value = match value {
            Some(v) => v,
            None if param.required => {
                return Err(ServerError::InvalidParameter {
                    name: param.name.to_string(),
                    reason: "required parameter not provided".to_string(),
                });
            }
            None => continue,
        };

        let safe_value = validate_param(param, &value)?;
        sql = sql.replace(&placeholder, &safe_value);
    }

    Ok(sql)
}

/// Validate and sanitize a parameter value
fn validate_param(param: &ParamDef, value: &str) -> Result<String, ServerError> {
    match param.param_type {
        ParamType::Integer => {
            let n: i64 = value.parse().map_err(|_| ServerError::InvalidParameter {
                name: param.name.to_string(),
                reason: format!("expected integer, got '{}'", value),
            })?;
            // Clamp limit-like params to reasonable bounds
            if param.name == "limit" && (n < 1 || n > 10000) {
                return Err(ServerError::InvalidParameter {
                    name: param.name.to_string(),
                    reason: "limit must be between 1 and 10000".to_string(),
                });
            }
            if param.name == "days" && (n < 1 || n > 36500) {
                return Err(ServerError::InvalidParameter {
                    name: param.name.to_string(),
                    reason: "days must be between 1 and 36500".to_string(),
                });
            }
            if param.name == "threshold" && n < 0 {
                return Err(ServerError::InvalidParameter {
                    name: param.name.to_string(),
                    reason: "threshold must be non-negative".to_string(),
                });
            }
            Ok(n.to_string())
        }
        ParamType::String => {
            // Only allow alphanumeric + underscore + hyphen + dot
            if !value.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.') {
                return Err(ServerError::InvalidParameter {
                    name: param.name.to_string(),
                    reason: "string contains disallowed characters".to_string(),
                });
            }
            Ok(value.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_count() {
        assert!(QUERY_CATALOG.len() >= 25, "Expected at least 25 P0 queries, got {}", QUERY_CATALOG.len());
    }

    #[test]
    fn test_unique_ids() {
        let mut ids: Vec<&str> = QUERY_CATALOG.iter().map(|q| q.id).collect();
        let original_len = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), original_len, "Duplicate query IDs found");
    }

    #[test]
    fn test_bind_params_basic() {
        let params = HashMap::new();
        let sql = bind_params(&Q001, "entries", &params).unwrap();
        assert!(sql.contains("FROM entries"));
        assert!(!sql.contains("{table}"));
    }

    #[test]
    fn test_bind_params_with_limit() {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "25".to_string());
        let sql = bind_params(&Q005, "entries", &params).unwrap();
        assert!(sql.contains("LIMIT 25"));
    }

    #[test]
    fn test_bind_params_default_limit() {
        let params = HashMap::new();
        let sql = bind_params(&Q005, "entries", &params).unwrap();
        assert!(sql.contains("LIMIT 50"));
    }

    #[test]
    fn test_bind_params_rejects_sql_injection() {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "1; DROP TABLE entries".to_string());
        assert!(bind_params(&Q005, "entries", &params).is_err());
    }

    #[test]
    fn test_bind_params_rejects_negative_limit() {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "-1".to_string());
        assert!(bind_params(&Q005, "entries", &params).is_err());
    }

    #[test]
    fn test_get_query() {
        assert!(get_query("Q-001").is_some());
        assert!(get_query("Q-999").is_none());
    }
}
