//! Canonical Arrow schema for Parquet export
//!
//! Single source of truth for the 28-column schema used in Parquet files.
//! Designed for efficient DataFusion queries with predicate pushdown.

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Build the canonical Arrow schema for filesystem entries.
///
/// The original 24 analytics columns remain stable. Additive columns preserve
/// authoritative POSIX path bytes and filesystem identity for migration
/// consumers.
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
        // operate on directly.
        Field::new(
            "scan_id",
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("scan_timestamp_us", DataType::Int64, false),
        Field::new("path_bytes", DataType::Binary, false),
        Field::new("filename_bytes", DataType::Binary, false),
        Field::new("parent_path_bytes", DataType::Binary, false),
        Field::new("fsid", DataType::UInt64, true),
    ])
}

/// Get the schema wrapped in an Arc (for Arrow writer APIs).
pub fn parquet_schema_ref() -> Arc<Schema> {
    Arc::new(parquet_schema())
}

/// Extract the parent path from a full path.
///
/// Returns "/" for root-level entries, and the portion before the last '/' otherwise.
pub fn compute_parent_path(path: &[u8]) -> &[u8] {
    if path == b"/" || !path.contains(&b'/') {
        return b"/";
    }
    match path.iter().rposition(|byte| *byte == b'/') {
        Some(0) => b"/",
        Some(pos) => &path[..pos],
        None => b"/",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_has_28_fields() {
        let schema = parquet_schema();
        assert_eq!(schema.fields().len(), 28);
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
                "path_bytes",
                "filename_bytes",
                "parent_path_bytes",
                "fsid",
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
        // Nullable: extension, filesystem identity, and every timestamp
        // column (the three legacy *_us columns and the six sec/nsec pairs).
        for (name, is_nullable) in &nullable {
            let expected = matches!(
                *name,
                "extension"
                    | "fsid"
                    | "mtime_us"
                    | "atime_us"
                    | "ctime_us"
                    | "mtime_sec"
                    | "mtime_nsec"
                    | "atime_sec"
                    | "atime_nsec"
                    | "ctime_sec"
                    | "ctime_nsec"
            );
            assert_eq!(
                *is_nullable, expected,
                "Field '{}' nullable={}, expected={}",
                name, is_nullable, expected
            );
        }
    }

    #[test]
    fn test_compute_parent_path() {
        assert_eq!(compute_parent_path(b"/"), b"/");
        assert_eq!(compute_parent_path(b"/file.txt"), b"/");
        assert_eq!(compute_parent_path(b"/dir/file.txt"), b"/dir");
        assert_eq!(compute_parent_path(b"/a/b/c/d.txt"), b"/a/b/c");
        assert_eq!(compute_parent_path(b"noSlash"), b"/");
        assert_eq!(compute_parent_path(b"/bad-\xff/name"), b"/bad-\xff");
    }
}
