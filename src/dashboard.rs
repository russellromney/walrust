//! Prometheus metrics server for walrust

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Router};
use prometheus::{Encoder, GaugeVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Per-database status
#[derive(Debug, Clone, Serialize)]
pub struct DbStatus {
    pub name: String,
    pub path: String,
    pub last_sync_timestamp: i64,
    pub wal_size_bytes: u64,
    pub next_snapshot_timestamp: i64,
    pub error_count: u64,
    pub snapshot_count: u64,
    pub current_txid: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors_last_hour: Option<u64>,
}

/// Health status for the /health endpoint
#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub uptime_seconds: u64,
    pub databases: HashMap<String, DbHealthStatus>,
    pub s3: S3HealthStatus,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbHealthStatus {
    pub status: DbSyncStatus,
    pub last_sync: Option<String>,
    pub lag_seconds: i64,
    pub errors_last_hour: u64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DbSyncStatus {
    Syncing,
    Idle,
    Error,
}

#[derive(Debug, Clone, Serialize)]
pub struct S3HealthStatus {
    pub connected: bool,
    pub last_successful_upload: Option<String>,
}

/// Shared metrics state
pub struct MetricsState {
    pub start_time: Instant,
    pub databases: RwLock<HashMap<String, DbStatus>>,
    pub registry: Registry,
    pub last_sync: GaugeVec,
    pub wal_size: IntGaugeVec,
    pub next_snapshot: GaugeVec,
    pub error_count: IntCounterVec,
    pub snapshot_count: IntCounterVec,
    pub current_txid: IntGaugeVec,
    pub databases_total: prometheus::IntGauge,
    pub validation_success: IntCounterVec,
    pub validation_failure: IntCounterVec,
    pub last_validation_timestamp: GaugeVec,
    // Production hardening metrics (v0.2.0)
    pub checkpoint_total: IntCounterVec,
    pub checkpoint_duration: GaugeVec,
    pub retry_total: IntCounterVec,
    pub sync_latency_seconds: GaugeVec,
    // S3 health tracking
    pub last_successful_upload: RwLock<Option<chrono::DateTime<chrono::Utc>>>,
    pub s3_connected: RwLock<bool>,
}

impl MetricsState {
    pub fn new() -> Self {
        let registry = Registry::new();

        let last_sync = GaugeVec::new(
            Opts::new("walrust_last_sync_timestamp", "Unix timestamp of last sync"),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(last_sync.clone())).unwrap();

        let wal_size = IntGaugeVec::new(
            Opts::new("walrust_wal_size_bytes", "Current WAL file size in bytes"),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(wal_size.clone())).unwrap();

        let next_snapshot = GaugeVec::new(
            Opts::new(
                "walrust_next_snapshot_timestamp",
                "Estimated next snapshot time",
            ),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(next_snapshot.clone())).unwrap();

        let error_count = IntCounterVec::new(
            Opts::new("walrust_error_count_total", "Total sync/upload errors"),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(error_count.clone())).unwrap();

        let snapshot_count = IntCounterVec::new(
            Opts::new("walrust_snapshot_count_total", "Total snapshots taken"),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(snapshot_count.clone())).unwrap();

        let current_txid = IntGaugeVec::new(
            Opts::new("walrust_current_txid", "Current transaction ID"),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(current_txid.clone())).unwrap();

        let databases_total =
            prometheus::IntGauge::new("walrust_databases_total", "Number of watched databases")
                .unwrap();
        registry
            .register(Box::new(databases_total.clone()))
            .unwrap();

        let validation_success = IntCounterVec::new(
            Opts::new("walrust_validation_success_total", "Successful backup validations"),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(validation_success.clone())).unwrap();

        let validation_failure = IntCounterVec::new(
            Opts::new("walrust_validation_failure_total", "Failed backup validations"),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(validation_failure.clone())).unwrap();

        let last_validation_timestamp = GaugeVec::new(
            Opts::new(
                "walrust_last_validation_timestamp",
                "Unix timestamp of last validation",
            ),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(last_validation_timestamp.clone())).unwrap();

        // Production hardening metrics (v0.2.0)
        let checkpoint_total = IntCounterVec::new(
            Opts::new("walrust_checkpoint_total", "Total checkpoints triggered"),
            &["database", "type"],  // type: "passive" or "truncate"
        )
        .unwrap();
        registry.register(Box::new(checkpoint_total.clone())).unwrap();

        let checkpoint_duration = GaugeVec::new(
            Opts::new("walrust_checkpoint_duration_seconds", "Last checkpoint duration in seconds"),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(checkpoint_duration.clone())).unwrap();

        let retry_total = IntCounterVec::new(
            Opts::new("walrust_retry_total", "Total S3 operation retries"),
            &["database", "operation"],  // operation: "upload", "download", "list"
        )
        .unwrap();
        registry.register(Box::new(retry_total.clone())).unwrap();

        let sync_latency_seconds = GaugeVec::new(
            Opts::new("walrust_sync_latency_seconds", "Time from WAL change to S3 upload complete"),
            &["database"],
        )
        .unwrap();
        registry.register(Box::new(sync_latency_seconds.clone())).unwrap();

        Self {
            start_time: Instant::now(),
            databases: RwLock::new(HashMap::new()),
            registry,
            last_sync,
            wal_size,
            next_snapshot,
            error_count,
            snapshot_count,
            current_txid,
            databases_total,
            validation_success,
            validation_failure,
            last_validation_timestamp,
            checkpoint_total,
            checkpoint_duration,
            retry_total,
            sync_latency_seconds,
            last_successful_upload: RwLock::new(None),
            s3_connected: RwLock::new(true),
        }
    }

    pub async fn update_db(&self, status: DbStatus) {
        let name = status.name.clone();
        self.last_sync
            .with_label_values(&[&name])
            .set(status.last_sync_timestamp as f64);
        self.wal_size
            .with_label_values(&[&name])
            .set(status.wal_size_bytes as i64);
        self.next_snapshot
            .with_label_values(&[&name])
            .set(status.next_snapshot_timestamp as f64);
        self.current_txid
            .with_label_values(&[&name])
            .set(status.current_txid as i64);

        let mut dbs = self.databases.write().await;
        dbs.insert(name, status);
        self.databases_total.set(dbs.len() as i64);
    }

    pub fn record_error(&self, db_name: &str) {
        self.error_count.with_label_values(&[db_name]).inc();
    }

    pub fn record_snapshot(&self, db_name: &str) {
        self.snapshot_count.with_label_values(&[db_name]).inc();
    }

    pub fn record_validation_success(&self, db_name: &str) {
        self.validation_success.with_label_values(&[db_name]).inc();
        self.last_validation_timestamp
            .with_label_values(&[db_name])
            .set(chrono::Utc::now().timestamp() as f64);
    }

    pub fn record_validation_failure(&self, db_name: &str) {
        self.validation_failure.with_label_values(&[db_name]).inc();
        self.last_validation_timestamp
            .with_label_values(&[db_name])
            .set(chrono::Utc::now().timestamp() as f64);
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    // Production hardening metrics (v0.2.0)

    /// Record a checkpoint event
    pub fn record_checkpoint(&self, db_name: &str, checkpoint_type: &str, duration_secs: f64) {
        self.checkpoint_total
            .with_label_values(&[db_name, checkpoint_type])
            .inc();
        self.checkpoint_duration
            .with_label_values(&[db_name])
            .set(duration_secs);
    }

    /// Record an S3 retry
    pub fn record_retry(&self, db_name: &str, operation: &str) {
        self.retry_total
            .with_label_values(&[db_name, operation])
            .inc();
    }

    /// Record sync latency (time from WAL change to S3 upload complete)
    pub fn record_sync_latency(&self, db_name: &str, latency_secs: f64) {
        self.sync_latency_seconds
            .with_label_values(&[db_name])
            .set(latency_secs);
    }

    /// Record a successful S3 upload
    pub async fn record_successful_upload(&self) {
        let mut last_upload = self.last_successful_upload.write().await;
        *last_upload = Some(chrono::Utc::now());
        let mut connected = self.s3_connected.write().await;
        *connected = true;
    }

    /// Record an S3 connection failure
    pub async fn record_s3_connection_failure(&self) {
        let mut connected = self.s3_connected.write().await;
        *connected = false;
    }

    /// Get health status
    pub async fn get_health(&self) -> HealthResponse {
        let now = chrono::Utc::now().timestamp();
        let dbs = self.databases.read().await;
        let s3_connected = *self.s3_connected.read().await;
        let last_upload = self.last_successful_upload.read().await;

        let mut db_statuses = HashMap::new();
        let mut any_errors = false;
        let mut all_stale = !dbs.is_empty();

        for (name, status) in dbs.iter() {
            let lag = now - status.last_sync_timestamp;
            let is_stale = lag > 300; // 5 minutes without sync = stale
            let has_recent_errors = status.errors_last_hour.unwrap_or(0) > 0;

            if !is_stale {
                all_stale = false;
            }
            if has_recent_errors {
                any_errors = true;
            }

            let db_status = if has_recent_errors {
                DbSyncStatus::Error
            } else if is_stale {
                DbSyncStatus::Idle
            } else {
                DbSyncStatus::Syncing
            };

            db_statuses.insert(
                name.clone(),
                DbHealthStatus {
                    status: db_status,
                    last_sync: if status.last_sync_timestamp > 0 {
                        Some(
                            chrono::DateTime::from_timestamp(status.last_sync_timestamp, 0)
                                .map(|dt| dt.to_rfc3339())
                                .unwrap_or_else(|| "unknown".to_string()),
                        )
                    } else {
                        None
                    },
                    lag_seconds: lag,
                    errors_last_hour: status.errors_last_hour.unwrap_or(0),
                },
            );
        }

        // Determine overall health
        let overall_status = if !s3_connected || (any_errors && all_stale) {
            HealthStatus::Unhealthy
        } else if any_errors || all_stale {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        HealthResponse {
            status: overall_status,
            uptime_seconds: self.uptime_seconds(),
            databases: db_statuses,
            s3: S3HealthStatus {
                connected: s3_connected,
                last_successful_upload: last_upload.map(|dt| dt.to_rfc3339()),
            },
        }
    }
}

impl Default for MetricsState {
    fn default() -> Self {
        Self::new()
    }
}

/// Start the metrics server (localhost only, graceful on port conflict)
pub async fn start_server(port: u16, state: Arc<MetricsState>) {
    let app = Router::new()
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .with_state(state);

    // Bind to localhost only (127.0.0.1)
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            tracing::info!(
                "Metrics available at http://127.0.0.1:{}/metrics, health at /health",
                port
            );
            if let Err(e) = axum::serve(listener, app).await {
                tracing::warn!("Metrics server error: {}", e);
            }
        }
        Err(e) => {
            tracing::warn!(
                "Could not start metrics server on port {} ({}), continuing without metrics",
                port,
                e
            );
        }
    }
}

async fn metrics(State(state): State<Arc<MetricsState>>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = state.registry.gather();
    let mut buffer = Vec::new();

    if encoder.encode(&metric_families, &mut buffer).is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to encode metrics",
        )
            .into_response();
    }

    let uptime_line = format!(
        "\n# HELP walrust_uptime_seconds Process uptime in seconds\n# TYPE walrust_uptime_seconds gauge\nwalrust_uptime_seconds {}\n",
        state.uptime_seconds()
    );
    buffer.extend_from_slice(uptime_line.as_bytes());

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        buffer,
    )
        .into_response()
}

/// Health check endpoint for load balancers and monitoring
async fn health(State(state): State<Arc<MetricsState>>) -> impl IntoResponse {
    let health = state.get_health().await;

    let status_code = match health.status {
        HealthStatus::Healthy => StatusCode::OK,
        HealthStatus::Degraded => StatusCode::OK, // Still operational, just degraded
        HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    (
        status_code,
        [("content-type", "application/json")],
        serde_json::to_string_pretty(&health).unwrap_or_else(|_| r#"{"status":"unhealthy"}"#.to_string()),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_metrics_state() {
        let state = MetricsState::new();

        state
            .update_db(DbStatus {
                name: "test".to_string(),
                path: "/tmp/test.db".to_string(),
                last_sync_timestamp: 1700000000,
                wal_size_bytes: 4096,
                next_snapshot_timestamp: 1700003600,
                error_count: 0,
                snapshot_count: 5,
                current_txid: 42,
                last_error: None,
                errors_last_hour: None,
            })
            .await;

        let dbs = state.databases.read().await;
        assert_eq!(dbs.len(), 1);
        assert!(dbs.contains_key("test"));
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let state = Arc::new(MetricsState::new());

        // Add a database
        state
            .update_db(DbStatus {
                name: "testdb".to_string(),
                path: "/tmp/testdb.db".to_string(),
                last_sync_timestamp: 1700000000,
                wal_size_bytes: 8192,
                next_snapshot_timestamp: 1700003600,
                error_count: 0,
                snapshot_count: 3,
                current_txid: 100,
                last_error: None,
                errors_last_hour: None,
            })
            .await;

        // Record some events
        state.record_snapshot("testdb");
        state.record_error("testdb");

        let app = Router::new()
            .route("/metrics", get(metrics))
            .with_state(state);

        let response = app
            .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        // Verify metrics are present
        assert!(body_str.contains("walrust_last_sync_timestamp"));
        assert!(body_str.contains("walrust_wal_size_bytes"));
        assert!(body_str.contains("walrust_snapshot_count_total"));
        assert!(body_str.contains("walrust_error_count_total"));
        assert!(body_str.contains("walrust_uptime_seconds"));
        assert!(body_str.contains("walrust_databases_total"));
        assert!(body_str.contains("testdb"));
    }

    #[tokio::test]
    async fn test_metrics_counters() {
        let state = MetricsState::new();

        state.record_snapshot("db1");
        state.record_snapshot("db1");
        state.record_error("db1");

        // Counters should accumulate
        let encoder = prometheus::TextEncoder::new();
        let families = state.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&families, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();

        assert!(output.contains("walrust_snapshot_count_total{database=\"db1\"} 2"));
        assert!(output.contains("walrust_error_count_total{database=\"db1\"} 1"));
    }

    #[tokio::test]
    async fn test_multiple_databases() {
        let state = MetricsState::new();

        state
            .update_db(DbStatus {
                name: "db1".to_string(),
                path: "/data/db1.db".to_string(),
                last_sync_timestamp: 1000,
                wal_size_bytes: 1024,
                next_snapshot_timestamp: 2000,
                error_count: 0,
                snapshot_count: 0,
                current_txid: 10,
                last_error: None,
                errors_last_hour: None,
            })
            .await;

        state
            .update_db(DbStatus {
                name: "db2".to_string(),
                path: "/data/db2.db".to_string(),
                last_sync_timestamp: 1500,
                wal_size_bytes: 2048,
                next_snapshot_timestamp: 2500,
                error_count: 0,
                snapshot_count: 0,
                current_txid: 20,
                last_error: None,
                errors_last_hour: None,
            })
            .await;

        let dbs = state.databases.read().await;
        assert_eq!(dbs.len(), 2);
        assert_eq!(state.databases_total.get(), 2);
    }

    #[tokio::test]
    async fn test_health_endpoint_healthy() {
        let state = Arc::new(MetricsState::new());

        // Add a database with recent sync
        let now = chrono::Utc::now().timestamp();
        state
            .update_db(DbStatus {
                name: "testdb".to_string(),
                path: "/tmp/testdb.db".to_string(),
                last_sync_timestamp: now,
                wal_size_bytes: 4096,
                next_snapshot_timestamp: now + 3600,
                error_count: 0,
                snapshot_count: 1,
                current_txid: 10,
                last_error: None,
                errors_last_hour: Some(0),
            })
            .await;

        // Mark S3 as connected
        state.record_successful_upload().await;

        let health = state.get_health().await;
        assert_eq!(health.status, HealthStatus::Healthy);
        assert!(health.s3.connected);
        assert!(health.databases.contains_key("testdb"));
        assert_eq!(health.databases["testdb"].status, DbSyncStatus::Syncing);
    }

    #[tokio::test]
    async fn test_health_endpoint_degraded() {
        let state = Arc::new(MetricsState::new());

        // Add a database with recent errors
        let now = chrono::Utc::now().timestamp();
        state
            .update_db(DbStatus {
                name: "testdb".to_string(),
                path: "/tmp/testdb.db".to_string(),
                last_sync_timestamp: now,
                wal_size_bytes: 4096,
                next_snapshot_timestamp: now + 3600,
                error_count: 5,
                snapshot_count: 1,
                current_txid: 10,
                last_error: Some("Upload failed".to_string()),
                errors_last_hour: Some(3),
            })
            .await;

        let health = state.get_health().await;
        assert_eq!(health.status, HealthStatus::Degraded);
        assert_eq!(health.databases["testdb"].status, DbSyncStatus::Error);
        assert_eq!(health.databases["testdb"].errors_last_hour, 3);
    }

    #[tokio::test]
    async fn test_health_endpoint_unhealthy() {
        let state = Arc::new(MetricsState::new());

        // Mark S3 as disconnected
        state.record_s3_connection_failure().await;

        let health = state.get_health().await;
        assert_eq!(health.status, HealthStatus::Unhealthy);
        assert!(!health.s3.connected);
    }

    #[tokio::test]
    async fn test_production_metrics() {
        let state = MetricsState::new();

        // Test checkpoint metrics
        state.record_checkpoint("db1", "passive", 0.5);
        state.record_checkpoint("db1", "truncate", 1.2);

        // Test retry metrics
        state.record_retry("db1", "upload");
        state.record_retry("db1", "upload");

        // Test sync latency
        state.record_sync_latency("db1", 0.25);

        // Verify metrics are recorded
        let encoder = prometheus::TextEncoder::new();
        let families = state.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&families, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();

        assert!(output.contains("walrust_checkpoint_total"));
        assert!(output.contains("walrust_checkpoint_duration_seconds"));
        assert!(output.contains("walrust_retry_total"));
        assert!(output.contains("walrust_sync_latency_seconds"));
    }
}
