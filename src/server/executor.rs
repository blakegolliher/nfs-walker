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
    let query = get_query(query_id)
        .ok_or_else(|| ServerError::QueryNotFound(query_id.to_string()))?;

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
        batch.schema().fields().iter().map(|f| f.name().clone()).collect()
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
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
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
        ).unwrap();

        let rows = batches_to_json(&[batch]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "alpha");
        assert_eq!(rows[0]["value"], 100);
        assert_eq!(rows[1]["name"], "beta");
        assert_eq!(rows[1]["value"], 200);
    }
}
