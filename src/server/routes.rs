//! Axum HTTP routes for the analytics API.

use crate::error::ServerError;
use crate::server::catalog::QUERY_CATALOG;
use crate::server::context::{AnalyticsContext, SharedContext};
use crate::server::executor;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

/// Shared application state
pub struct AppState {
    pub context: SharedContext,
}

// ─── Route builder ───────────────────────────────────────────────

pub fn build_router(state: Arc<AppState>) -> Router {
    // API routes
    let api = Router::new()
        .route("/health", get(health))
        .route("/scans", get(list_scans))
        .route("/scans/:scan_id", get(get_scan))
        .route("/queries", get(list_queries))
        .route("/queries/:query_id/execute", post(execute_query))
        .route("/queries/batch", post(batch_execute));

    // SPA static file serving — falls back to index.html for client-side routing
    let dist_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("web/dist");
    let index_html = dist_dir.join("index.html");
    let spa = ServeDir::new(&dist_dir).fallback(ServeFile::new(index_html));

    Router::new()
        .nest("/api", api)
        .fallback_service(spa)
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// ─── Handlers ────────────────────────────────────────────────────

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "service": "nfs-walker-analytics",
    }))
}

async fn list_scans(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ServerError> {
    let ctx = state.context.read().await;
    let scans: Vec<_> = ctx.scans.values().collect();
    Ok(Json(serde_json::json!({
        "scans": scans,
        "count": scans.len(),
        "default_scan": ctx.latest_scan_id,
    })))
}

async fn get_scan(
    State(state): State<Arc<AppState>>,
    Path(scan_id): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let ctx = state.context.read().await;
    let info = ctx.scans.get(&scan_id)
        .ok_or_else(|| ServerError::ScanNotFound(scan_id.clone()))?;
    Ok(Json(serde_json::json!(info)))
}

async fn list_queries() -> impl IntoResponse {
    let queries: Vec<serde_json::Value> = QUERY_CATALOG
        .iter()
        .map(|q| {
            serde_json::json!({
                "id": q.id,
                "name": q.name,
                "description": q.description,
                "category": q.category,
                "params": q.params,
            })
        })
        .collect();

    Json(serde_json::json!({
        "queries": queries,
        "count": queries.len(),
    }))
}

/// Request body for executing a single query
#[derive(serde::Deserialize)]
struct ExecuteRequest {
    #[serde(default)]
    params: HashMap<String, String>,
    scan_id: Option<String>,
}

async fn execute_query(
    State(state): State<Arc<AppState>>,
    Path(query_id): Path<String>,
    Json(body): Json<ExecuteRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let ctx = state.context.read().await;
    let result = executor::execute_query(
        &ctx,
        &query_id,
        &body.params,
        body.scan_id.as_deref(),
    )
    .await?;
    Ok(Json(result))
}

/// Request body for batch execution
#[derive(serde::Deserialize)]
struct BatchRequest {
    queries: Vec<BatchQueryItem>,
    scan_id: Option<String>,
}

#[derive(serde::Deserialize)]
struct BatchQueryItem {
    query_id: String,
    #[serde(default)]
    params: HashMap<String, String>,
}

async fn batch_execute(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BatchRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let ctx = state.context.read().await;
    let mut results = Vec::new();

    for item in &body.queries {
        let result = executor::execute_query(
            &ctx,
            &item.query_id,
            &item.params,
            body.scan_id.as_deref(),
        )
        .await;

        match result {
            Ok(r) => results.push(serde_json::json!({
                "status": "ok",
                "result": r,
            })),
            Err(e) => results.push(serde_json::json!({
                "status": "error",
                "query_id": item.query_id,
                "error": e.to_string(),
            })),
        }
    }

    Ok(Json(serde_json::json!({
        "results": results,
        "count": results.len(),
    })))
}

// ─── Server startup ──────────────────────────────────────────────

/// Start the analytics server
pub async fn serve(
    data_dir: &path::Path,
    bind: &str,
    port: u16,
) -> Result<(), ServerError> {
    eprintln!("Loading scan data from {}...", data_dir.display());

    let analytics_ctx = AnalyticsContext::build(data_dir).await?;
    let scan_count = analytics_ctx.scans.len();

    if scan_count == 0 {
        eprintln!("Warning: No scans found in {}/scans/", data_dir.display());
        eprintln!("Run 'nfs-walker export-parquet' first to create scan data.");
    } else {
        eprintln!("Loaded {} scan(s)", scan_count);
        for info in analytics_ctx.scans.values() {
            eprintln!(
                "  - {} ({} entries, table: {})",
                info.scan_id,
                info.total_entries.unwrap_or(0),
                info.table_name
            );
        }
        if let Some(ref latest) = analytics_ctx.latest_scan_id {
            eprintln!("Default table 'entries' -> scan {}", latest);
        }
    }

    let state = Arc::new(AppState {
        context: Arc::new(RwLock::new(analytics_ctx)),
    });

    let router = build_router(state);
    let addr: SocketAddr = format!("{}:{}", bind, port)
        .parse()
        .map_err(|e| ServerError::Other(format!("Invalid bind address: {}", e)))?;

    eprintln!("Analytics server listening on http://{}", addr);
    eprintln!("Dashboard: http://{}/", addr);
    eprintln!("API endpoints:");
    eprintln!("  GET  /api/health");
    eprintln!("  GET  /api/scans");
    eprintln!("  GET  /api/scans/:scan_id");
    eprintln!("  GET  /api/queries");
    eprintln!("  POST /api/queries/:query_id/execute");
    eprintln!("  POST /api/queries/batch");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| ServerError::Io(e))?;

    eprintln!("\nServer shut down.");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    eprintln!("\nShutting down gracefully...");
}
