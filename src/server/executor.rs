//! Query execution engine.
//!
//! Looks up queries from the catalog, binds parameters, executes via
//! DataFusion, and converts results to JSON.

use crate::error::ServerError;
use crate::server::catalog::{bind_params, get_query};
use crate::server::context::AnalyticsContext;
use arrow::json::LineDelimitedWriter;
use datafusion::arrow::array::RecordBatch;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Default per-query timeout in seconds (override with NFS_WALKER_SERVE_TIMEOUT_SECS)
const DEFAULT_TIMEOUT_SECS: u64 = 60;

/// Per-query timeout, from the environment or the default.
/// Parses leniently: anything non-numeric or zero falls back.
fn query_timeout() -> Duration {
    let secs = std::env::var("NFS_WALKER_SERVE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

/// Result of executing a query
#[derive(Debug, serde::Serialize)]
pub struct QueryResult {
    pub query_id: String,
    pub query_name: String,
    pub execution_ms: u64,
    pub row_count: usize,
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
}

/// Execute a query by ID with the given parameters
pub async fn execute_query(
    ctx: &AnalyticsContext,
    query_id: &str,
    params: &HashMap<String, String>,
    scan_id: Option<&str>,
) -> Result<QueryResult, ServerError> {
    let query =
        get_query(query_id).ok_or_else(|| ServerError::QueryNotFound(query_id.to_string()))?;

    let table = ctx.resolve_table(scan_id)?;
    let sql = bind_params(query, &table, params)?;

    let timeout = query_timeout();
    let start = Instant::now();
    let df = ctx.ctx.sql(&sql).await?;
    let batches = tokio::time::timeout(timeout, df.collect())
        .await
        .map_err(|_| {
            ServerError::Other(format!("Query timed out after {}s", timeout.as_secs()))
        })??;
    let execution_ms = start.elapsed().as_millis() as u64;

    let columns = if let Some(batch) = batches.first() {
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    } else {
        vec![]
    };

    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    let rows = batches_to_json(&batches)?;

    Ok(QueryResult {
        query_id: query.id.to_string(),
        query_name: query.name.to_string(),
        execution_ms,
        row_count,
        columns,
        rows,
    })
}

/// Convert Arrow RecordBatches to JSON rows using Arrow's JSON writer.
///
/// Note: JSON numbers become f64 in JavaScript, so u64/i64 values above
/// 2^53 (e.g. byte totals on very large filesystems) lose precision in the UI.
fn batches_to_json(batches: &[RecordBatch]) -> Result<Vec<serde_json::Value>, ServerError> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let mut buf = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buf);
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }

    let output = String::from_utf8_lossy(&buf);
    let rows: Vec<serde_json::Value> = output
        .lines()
        .filter(|line| !line.is_empty())
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    fn test_batches_to_json_empty() {
        let result = batches_to_json(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_batches_to_json_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let rows = batches_to_json(&[batch]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "alpha");
        assert_eq!(rows[0]["value"], 100);
        assert_eq!(rows[1]["name"], "beta");
        assert_eq!(rows[1]["value"], 200);
    }

    /// A tiny scan with hard links: inode 20 has both links in the tree,
    /// inode 30 two of its three, inode 40 one of its two. Directories
    /// have nlink > 1 too but are never hard-linked.
    fn hardlink_scan() -> AnalyticsContext {
        let rows: [(&str, &str, u64, u32, u64); 9] = [
            ("/a", "directory", 1, 3, 0),
            ("/a/b", "directory", 2, 2, 0),
            ("/a/f", "file", 10, 1, 100),
            ("/a/h", "file", 20, 2, 500),
            ("/a/b/h", "file", 20, 2, 500),
            ("/a/m", "file", 30, 3, 7),
            ("/a/b/m", "file", 30, 3, 7),
            ("/a/o", "file", 40, 2, 9),
            ("/a/s", "symlink", 50, 1, 4),
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("file_type", DataType::Utf8, false),
            Field::new("inode", DataType::UInt64, false),
            Field::new("nlink", DataType::UInt32, false),
            Field::new("size", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.3))),
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.4))),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("entries", Arc::new(table)).unwrap();
        AnalyticsContext {
            ctx,
            scans: HashMap::new(),
            data_dir: PathBuf::new(),
            latest_scan_id: Some("test".to_string()),
        }
    }

    #[tokio::test]
    async fn test_inodes_by_type_counts_each_hard_linked_inode_once() {
        let result = execute_query(&hardlink_scan(), "Q-004", &HashMap::new(), None)
            .await
            .unwrap();
        let counts: Vec<_> = result
            .rows
            .iter()
            .map(|r| {
                (
                    r["file_type"].as_str().unwrap(),
                    r["unique_inodes"].as_i64().unwrap(),
                    r["entries"].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            counts,
            vec![("file", 4, 6), ("directory", 2, 2), ("symlink", 1, 1)]
        );
    }

    #[tokio::test]
    async fn test_duplicate_inodes_finds_only_repeated_inodes() {
        let result = execute_query(&hardlink_scan(), "Q-007", &HashMap::new(), None)
            .await
            .unwrap();
        let mut dups: Vec<_> = result
            .rows
            .iter()
            .map(|r| {
                (
                    r["inode"].as_u64().unwrap(),
                    r["link_count"].as_i64().unwrap(),
                    r["example_path"].as_str().unwrap(),
                )
            })
            .collect();
        dups.sort();
        assert_eq!(dups, vec![(20, 2, "/a/b/h"), (30, 2, "/a/b/m")]);
    }
}
