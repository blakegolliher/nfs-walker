//! DataFusion SessionContext setup for analytics queries.
//!
//! Discovers scan directories, registers Parquet files as tables,
//! and provides the query execution context.

use crate::error::ServerError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Default query memory limit in GiB (override with NFS_WALKER_SERVE_MEM_GB)
const DEFAULT_MEM_GB: f64 = 4.0;

/// Query memory limit in bytes, from the environment or the default.
/// Parses leniently: anything non-numeric or non-positive falls back.
fn memory_limit_bytes() -> usize {
    let gb = std::env::var("NFS_WALKER_SERVE_MEM_GB")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(DEFAULT_MEM_GB);
    (gb * 1024.0 * 1024.0 * 1024.0) as usize
}

/// Metadata about a discovered scan
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScanInfo {
    pub scan_id: String,
    pub scan_dir: PathBuf,
    pub table_name: String,
    pub source_url: Option<String>,
    pub total_entries: Option<u64>,
    pub scan_timestamp_us: Option<i64>,
    pub parquet_files: Vec<String>,
}

/// Analytics context holding the DataFusion session and scan registry
pub struct AnalyticsContext {
    pub ctx: SessionContext,
    pub scans: HashMap<String, ScanInfo>,
    pub data_dir: PathBuf,
    /// The most recent scan ID (used for the `entries` alias)
    pub latest_scan_id: Option<String>,
}

impl AnalyticsContext {
    /// Build a new AnalyticsContext by discovering scans in `data_dir`
    pub async fn build(data_dir: &Path) -> Result<Self, ServerError> {
        // Bound query memory so a runaway aggregation can't OOM the host
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit_bytes(), 1.0)
            .build_arc()?;
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime_env);
        let scans_dir = data_dir.join("scans");

        if !scans_dir.exists() {
            return Ok(Self {
                ctx,
                scans: HashMap::new(),
                data_dir: data_dir.to_path_buf(),
                latest_scan_id: None,
            });
        }

        let mut scans = HashMap::new();
        let mut latest_ts: Option<i64> = None;
        let mut latest_id: Option<String> = None;

        let dir_entries = std::fs::read_dir(&scans_dir)?;
        for dir_entry in dir_entries {
            let dir_entry = dir_entry?;
            let path = dir_entry.path();
            if !path.is_dir() {
                continue;
            }

            let metadata_path = path.join("metadata.json");
            if !metadata_path.exists() {
                continue;
            }

            let metadata_bytes = std::fs::read(&metadata_path)?;
            let metadata: serde_json::Value = serde_json::from_slice(&metadata_bytes)?;

            let scan_id = metadata["scan_id"]
                .as_str()
                .unwrap_or_default()
                .to_string();

            if scan_id.is_empty() {
                continue;
            }

            let table_name = format!("scan_{}", scan_id.replace('-', "_"));
            let source_url = metadata["source_url"].as_str().map(|s| s.to_string());
            let total_entries = metadata["total_entries"].as_u64();
            let scan_timestamp_us = metadata["scan_timestamp_us"].as_i64();
            let parquet_files: Vec<String> = metadata["parquet_files"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            // Register the scan directory as a Parquet table
            let scan_path = path.to_string_lossy().to_string();
            ctx.register_parquet(
                &table_name,
                &scan_path,
                ParquetReadOptions::default(),
            )
            .await?;

            // Track latest scan
            if let Some(ts) = scan_timestamp_us {
                if latest_ts.is_none() || ts > latest_ts.unwrap() {
                    latest_ts = Some(ts);
                    latest_id = Some(scan_id.clone());
                }
            }

            let info = ScanInfo {
                scan_id: scan_id.clone(),
                scan_dir: path.clone(),
                table_name,
                source_url,
                total_entries,
                scan_timestamp_us,
                parquet_files,
            };

            scans.insert(scan_id, info);
        }

        // If no scan carried scan_timestamp_us metadata, fall back to the
        // lexically-last scan so default_scan (and `entries`) still resolve
        if latest_id.is_none() && !scans.is_empty() {
            latest_id = scans.keys().max().cloned();
            eprintln!(
                "Warning: no scan has scan_timestamp_us metadata; using lexically-last scan {} as default",
                latest_id.as_deref().unwrap_or("?")
            );
        }

        // Register `entries` alias pointing to the most recent scan
        if let Some(ref latest) = latest_id {
            if let Some(info) = scans.get(latest) {
                let scan_path = info.scan_dir.to_string_lossy().to_string();
                ctx.register_parquet(
                    "entries",
                    &scan_path,
                    ParquetReadOptions::default(),
                )
                .await?;
            }
        }

        Ok(Self {
            ctx,
            scans,
            data_dir: data_dir.to_path_buf(),
            latest_scan_id: latest_id,
        })
    }

    /// Get the default table name (latest scan or "entries")
    pub fn default_table(&self) -> &str {
        "entries"
    }

    /// Resolve a table name — returns the actual registered table name
    pub fn resolve_table(&self, scan_id: Option<&str>) -> Result<String, ServerError> {
        match scan_id {
            Some(id) => {
                if let Some(info) = self.scans.get(id) {
                    Ok(info.table_name.clone())
                } else {
                    Err(ServerError::ScanNotFound(id.to_string()))
                }
            }
            None => {
                if self.latest_scan_id.is_some() {
                    Ok("entries".to_string())
                } else {
                    Err(ServerError::Other("No scans available".to_string()))
                }
            }
        }
    }
}

/// Thread-safe wrapper for AnalyticsContext
pub type SharedContext = Arc<RwLock<AnalyticsContext>>;
