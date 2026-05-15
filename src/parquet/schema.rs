//! Canonical Arrow schema for Parquet export
//!
//! Single source of truth for the 24-column schema used in Parquet files.
//! Designed for efficient DataFusion queries with predicate pushdown.

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Build the canonical Arrow schema for filesystem entries.
///
/// 24 columns optimized for DataFusion analytics queries.
pub fn parquet_schema() -> Schema {
    Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("filename", DataType::Utf8, false),
        Field::new("extension", DataType::Utf8, true),
        Field::new("inode", DataType::UInt64, false),
        Field::new("file_type", DataType::Utf8, false),
        Field::new("size", DataType::UInt64, false),
        Field::new("allocated_blocks", DataType::UInt64, false),
        Field::new("nlink", DataType::UInt32, false),
        Field::new("uid", DataType::UInt32, false),
        Field::new("gid", DataType::UInt32, false),
        Field::new("permissions", DataType::UInt16, false),
        Field::new("mtime_us", DataType::Int64, true),
        Field::new("atime_us", DataType::Int64, true),
        Field::new("ctime_us", DataType::Int64, true),
        Field::new("mtime_sec", DataType::Int64, true),
        Field::new("mtime_nsec", DataType::Int32, true),
        Field::new("atime_sec", DataType::Int64, true),
        Field::new("atime_nsec", DataType::Int32, true),
        Field::new("ctime_sec", DataType::Int64, true),
        Field::new("ctime_nsec", DataType::Int32, true),
        Field::new("depth", DataType::UInt16, false),
        Field::new("parent_path", DataType::Utf8, false),
        // scan_id is dictionary-encoded so the per-shard writer doesn't
        // buffer ~9 MB of repeated UUID bytes per row group. DuckDB,
        // Spark, and pandas see this as a regular VARCHAR/string
        // column; Arrow-native readers get a DictionaryArray they can
        // operate on directly. See P1-4 in tasks/code-review-audit.md.
        Field::new(
            "scan_id",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("scan_timestamp_us", DataType::Int64, false),
    ])
}

/// Get the schema wrapped in an Arc (for Arrow writer APIs).
pub fn parquet_schema_ref() -> Arc<Schema> {
    Arc::new(parquet_schema())
}

/// Convert entry_type u8 to a human-readable string.
pub fn file_type_string(entry_type: u8) -> &'static str {
    match entry_type {
        0 => "file",
        1 => "directory",
        2 => "symlink",
        _ => "other",
    }
}

/// Convert seconds (epoch) to microseconds.
pub fn seconds_to_microseconds(secs: i64) -> i64 {
    secs.saturating_mul(1_000_000)
}

/// Extract the parent path from a full path.
///
/// Returns "/" for root-level entries, and the portion before the last '/' otherwise.
pub fn compute_parent_path(path: &str) -> &str {
    if path == "/" || !path.contains('/') {
        return "/";
    }
    match path.rfind('/') {
        Some(0) => "/",
        Some(pos) => &path[..pos],
        None => "/",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_has_24_fields() {
        let schema = parquet_schema();
        assert_eq!(schema.fields().len(), 24);
    }

    #[test]
    fn test_schema_field_names() {
        let schema = parquet_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "path",
                "filename",
                "extension",
                "inode",
                "file_type",
                "size",
                "allocated_blocks",
                "nlink",
                "uid",
                "gid",
                "permissions",
                "mtime_us",
                "atime_us",
                "ctime_us",
                "mtime_sec",
                "mtime_nsec",
                "atime_sec",
                "atime_nsec",
                "ctime_sec",
                "ctime_nsec",
                "depth",
                "parent_path",
                "scan_id",
                "scan_timestamp_us",
            ]
        );
    }

    #[test]
    fn test_schema_nullable_fields() {
        let schema = parquet_schema();
        let nullable: Vec<(&str, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.is_nullable()))
            .collect();
        // Only extension, mtime_us, atime_us, ctime_us should be nullable
        for (name, is_nullable) in &nullable {
            let expected = matches!(*name, "extension" | "mtime_us" | "atime_us" | "ctime_us" | "mtime_sec" | "mtime_nsec" | "atime_sec" | "atime_nsec" | "ctime_sec" | "ctime_nsec");
            assert_eq!(
                *is_nullable, expected,
                "Field '{}' nullable={}, expected={}",
                name, is_nullable, expected
            );
        }
    }

    #[test]
    fn test_file_type_string() {
        assert_eq!(file_type_string(0), "file");
        assert_eq!(file_type_string(1), "directory");
        assert_eq!(file_type_string(2), "symlink");
        assert_eq!(file_type_string(3), "other");
        assert_eq!(file_type_string(255), "other");
    }

    #[test]
    fn test_seconds_to_microseconds() {
        assert_eq!(seconds_to_microseconds(0), 0);
        assert_eq!(seconds_to_microseconds(1), 1_000_000);
        assert_eq!(seconds_to_microseconds(1_234_567_890), 1_234_567_890_000_000);
        // Saturating: no overflow
        assert_eq!(
            seconds_to_microseconds(i64::MAX),
            i64::MAX // saturates
        );
    }

    #[test]
    fn test_compute_parent_path() {
        assert_eq!(compute_parent_path("/"), "/");
        assert_eq!(compute_parent_path("/file.txt"), "/");
        assert_eq!(compute_parent_path("/dir/file.txt"), "/dir");
        assert_eq!(compute_parent_path("/a/b/c/d.txt"), "/a/b/c");
        assert_eq!(compute_parent_path("noSlash"), "/");
    }
}
