use anyhow::Result;
use chrono::Utc;
use hadb_storage_s3::S3Storage;
use std::sync::Arc;

use crate::dashboard::{DbStatus, MetricsState};
use crate::retry::{classify_error, ErrorKind, RetryPolicy};
use crate::webhook::WebhookSender;
use walrust_core::legacy_wal_sync::{
    apply_sync_output_to_watched_state, sync_watched_db_once_to_cache, WatchedDbState,
};

use super::types::{DbState, DbTaskState, SyncInput, SyncOutput};

// ============================================================================
// Concurrent WAL sync operations (immutable, for parallel execution)
// ============================================================================

/// Sync WAL changes concurrently (immutable version)
/// Returns SyncOutput with changes to apply, or None if no changes
pub(crate) async fn sync_wal_concurrent(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    input: SyncInput,
) -> Result<SyncOutput> {
    let storage = S3Storage::new(client.clone(), bucket.to_string());
    walrust_core::legacy_wal_sync::sync_wal_to_storage(&storage, prefix, input).await
}

/// Sync WAL concurrently with retry and webhook notifications
pub(crate) async fn sync_wal_concurrent_with_retry(
    client: Arc<aws_sdk_s3::Client>,
    bucket: String,
    prefix: String,
    input: SyncInput,
    retry_policy: RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
) -> Result<SyncOutput> {
    let db_name = input.name.clone();
    let mut attempts = 0u32;

    loop {
        attempts += 1;
        match sync_wal_concurrent(&client, &bucket, &prefix, input.clone()).await {
            Ok(output) => return Ok(output),
            Err(e) => {
                let error_kind = classify_error(&e);
                let is_retryable = matches!(error_kind, ErrorKind::Transient | ErrorKind::Unknown);

                if error_kind == ErrorKind::AuthError {
                    tracing::error!("{}: Authentication error during WAL sync: {}", db_name, e);
                    webhook_sender
                        .notify_auth_failure(&db_name, &e.to_string())
                        .await;
                    return Err(e);
                }

                if !is_retryable || attempts > retry_policy.config().max_retries + 1 {
                    tracing::error!(
                        "{}: WAL sync failed after {} attempts: {}",
                        db_name,
                        attempts,
                        e
                    );
                    webhook_sender
                        .notify_upload_failed(&db_name, &e.to_string(), attempts)
                        .await;
                    return Err(e);
                }

                let delay = retry_policy.calculate_delay(attempts - 1);
                tracing::warn!(
                    "{}: WAL sync attempt {}/{} failed, retrying in {:?}: {}",
                    db_name,
                    attempts,
                    retry_policy.config().max_retries + 1,
                    delay,
                    e
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Startup (re-)anchor for a watched stream, with the same bounded transient
/// retry + webhook notification the sync/snapshot paths use.
///
/// Delegates the decision (fresh base vs resume re-anchor snapshot) to
/// [`walrust_core::legacy_wal_sync::anchor_stream_on_startup`] so the production
/// `--independent-tasks` startup and the regression suites exercise the exact
/// same re-anchor logic. See that function's docs for why a resume MUST publish
/// a snapshot (the restart chain seam).
pub(crate) async fn anchor_stream_on_startup_with_retry(
    client: Arc<aws_sdk_s3::Client>,
    bucket: String,
    prefix: String,
    state: &mut DbState,
    retry_policy: RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
) -> Result<()> {
    let db_name = state.name.clone();
    let mut watched = WatchedDbState::from(&*state);
    let mut attempts = 0u32;

    loop {
        attempts += 1;
        let storage = S3Storage::new((*client).clone(), bucket.clone());
        match walrust_core::legacy_wal_sync::anchor_stream_on_startup(
            &storage,
            &prefix,
            &mut watched,
        )
        .await
        {
            Ok(_) => {
                state.apply_watched_state(&watched);
                return Ok(());
            }
            Err(e) => {
                let error_kind = classify_error(&e);
                let is_retryable = matches!(error_kind, ErrorKind::Transient | ErrorKind::Unknown);

                if error_kind == ErrorKind::AuthError {
                    tracing::error!(
                        "{}: Authentication error during startup anchor: {}",
                        db_name,
                        e
                    );
                    webhook_sender
                        .notify_auth_failure(&db_name, &e.to_string())
                        .await;
                    return Err(e);
                }

                if !is_retryable || attempts > retry_policy.config().max_retries + 1 {
                    tracing::error!(
                        "{}: Startup anchor failed after {} attempts: {}",
                        db_name,
                        attempts,
                        e
                    );
                    webhook_sender
                        .notify_upload_failed(&db_name, &e.to_string(), attempts)
                        .await;
                    return Err(e);
                }

                let delay = retry_policy.calculate_delay(attempts - 1);
                tracing::warn!(
                    "{}: Startup anchor attempt {}/{} failed, retrying in {:?}: {}",
                    db_name,
                    attempts,
                    retry_policy.config().max_retries + 1,
                    delay,
                    e
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

// ============================================================================
// Independent per-DB task for concurrent sync
// ============================================================================

/// Perform a single sync operation for a DB task
///
/// When cache_state is Some, encodes LTX to disk cache and notifies uploader.
/// When cache_state is None, uploads directly to S3 with retry logic.
pub(crate) async fn do_sync(
    state: &mut DbTaskState,
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    retry_policy: &RetryPolicy,
    webhook_sender: &Arc<WebhookSender>,
    metrics_state: &Arc<MetricsState>,
    cache_state: Option<&super::types::CacheState>,
) -> Result<u64> {
    let mut watched_state = WatchedDbState::from(&state.db_state);

    let result = if let Some(cache) = cache_state {
        sync_watched_db_once_to_cache(
            &mut watched_state,
            &cache.cache,
            &cache.shadow,
            &cache.upload_tx,
        )
        .await?
    } else {
        let input = SyncInput::from(&watched_state);
        let result = sync_wal_concurrent_with_retry(
            Arc::new(client.clone()),
            bucket.to_string(),
            prefix.to_string(),
            input,
            retry_policy.clone(),
            Arc::clone(webhook_sender),
        )
        .await?;
        apply_sync_output_to_watched_state(&mut watched_state, &result);
        result
    };

    state.db_state.apply_watched_state(&watched_state);
    if result.checkpoint_detected {
        // A WAL rollover in the direct/independent sync path is a durability-
        // relevant event (the WAL was reset out from under the incremental
        // cursor). Emit it loudly (error log + webhook) even though the safety
        // path already re-anchored, so operators can see the re-anchor happened
        // (A3/A4). Shadow mode handles generation rolls routinely and does not
        // route through here.
        let event = format!(
            "{}: WAL rollover/checkpoint detected; backup safety path re-anchored before continuing",
            state.db_state.name
        );
        tracing::error!("{}", event);
        webhook_sender
            .notify_checkpoint_detected(&state.db_state.name, &event)
            .await;
    }

    if result.frame_count > 0 {
        // Update trigger state
        state.trigger_state.frames_since_snapshot += result.frame_count;
        state.trigger_state.last_wal_activity = Some(std::time::Instant::now());

        // Update metrics
        let wal_size = std::fs::metadata(&state.db_state.wal_path)
            .map(|m| m.len())
            .unwrap_or(0);
        metrics_state
            .update_db(DbStatus {
                name: state.db_state.name.clone(),
                path: state.db_state.db_path.display().to_string(),
                last_sync_timestamp: chrono::Utc::now().timestamp(),
                wal_size_bytes: wal_size,
                next_snapshot_timestamp: 0, // TODO
                error_count: 0,
                snapshot_count: 0,
                current_txid: state.db_state.current_txid,
                last_error: None,
                errors_last_hour: None,
            })
            .await;
    }

    Ok(result.frame_count)
}

// ============================================================================
// Retry-wrapped S3 operations for production use
// ============================================================================

/// Take snapshot with retry and webhook notifications
pub(crate) async fn take_snapshot_with_retry(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    state: &mut DbState,
    retry_policy: &RetryPolicy,
    webhook_sender: &Arc<WebhookSender>,
) -> Result<()> {
    let db_name = state.name.clone();
    let mut attempts = 0u32;

    // Try the snapshot operation with retries
    loop {
        attempts += 1;
        match take_snapshot(client, bucket, prefix, state).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let error_kind = classify_error(&e);
                let is_retryable = matches!(error_kind, ErrorKind::Transient | ErrorKind::Unknown);

                // Handle auth errors immediately
                if error_kind == ErrorKind::AuthError {
                    tracing::error!("{}: Authentication error during snapshot: {}", db_name, e);
                    webhook_sender
                        .notify_auth_failure(&db_name, &e.to_string())
                        .await;
                    return Err(e);
                }

                // If not retryable or exhausted retries, fail
                if !is_retryable || attempts > retry_policy.config().max_retries + 1 {
                    tracing::error!(
                        "{}: Snapshot failed after {} attempts: {}",
                        db_name,
                        attempts,
                        e
                    );
                    webhook_sender
                        .notify_upload_failed(&db_name, &e.to_string(), attempts)
                        .await;
                    return Err(e);
                }

                // Calculate backoff and retry
                let delay = retry_policy.calculate_delay(attempts - 1);
                tracing::warn!(
                    "{}: Snapshot attempt {}/{} failed, retrying in {:?}: {}",
                    db_name,
                    attempts,
                    retry_policy.config().max_retries + 1,
                    delay,
                    e
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Take a full database snapshot as LTX
pub(crate) async fn take_snapshot(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    state: &mut DbState,
) -> Result<()> {
    let timestamp = Utc::now();
    let storage = S3Storage::new(client.clone(), bucket.to_string());
    let output = walrust_core::legacy_wal_sync::take_snapshot_to_storage(
        &storage,
        prefix,
        SyncInput::from(&*state),
    )
    .await?;

    // Update state. The snapshot folded all WAL frames into the base, so the
    // WAL cursor must be reset, not left pointing into the pre-checkpoint WAL
    // (F11). A PASSIVE checkpoint may restart the WAL with a new salt; re-read
    // the header so the next incremental read re-seeds the salt/checksum chain
    // and reads from offset 0 of the new generation. The snapshot's db_checksum
    // becomes the explicit hand-off base for the first incremental.
    state.current_txid = output.new_current_txid;
    state.last_snapshot = Some(timestamp);
    state.db_checksum = output.new_db_checksum;
    state.wal_offset = output.new_wal_offset;
    state.wal_generation = output.new_wal_generation;
    state.wal_salt = output.new_wal_salt;
    state.wal_checksum_chain = output.new_wal_checksum_chain;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WebhookConfig;
    use crate::retry::{RetryConfig, RetryPolicy};
    use rusqlite::Connection;
    use serde_json::Value;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn capture_one_webhook() -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());

        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 1024];

            loop {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0, "webhook connection closed before request body");
                buffer.extend_from_slice(&chunk[..n]);

                if let Some(header_end) = find_header_end(&buffer) {
                    let headers = String::from_utf8_lossy(&buffer[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("Content-Length:")
                                .or_else(|| line.strip_prefix("content-length:"))
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    let body_start = header_end + 4;
                    if buffer.len() >= body_start + content_length {
                        let body = String::from_utf8(
                            buffer[body_start..body_start + content_length].to_vec(),
                        )
                        .unwrap();
                        stream
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                            .await
                            .unwrap();
                        return body;
                    }
                }
            }
        });

        (url, handle)
    }

    fn find_header_end(bytes: &[u8]) -> Option<usize> {
        bytes.windows(4).position(|window| window == b"\r\n\r\n")
    }

    #[tokio::test]
    async fn test_sync_wal_concurrent_rejects_database_out_of_wal_mode() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("delete-mode.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=DELETE;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (value) VALUES ('base');
            ",
        )
        .unwrap();
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_ne!(mode.to_lowercase(), "wal");
        drop(conn);

        let input = SyncInput {
            db_path: db_path.clone(),
            name: "delete-mode".to_string(),
            wal_path: db_path.with_extension("db-wal"),
            wal_offset: 0,
            wal_generation: 0,
            current_txid: 1,
            db_checksum: Some(0),
            wal_salt: None,
            wal_checksum_chain: None,
        };
        let client = crate::s3::create_client(None).await.unwrap();

        let err = match sync_wal_concurrent(&client, "unused", "", input).await {
            Ok(_) => panic!("sync must fail closed when SQLite is not in WAL mode"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(msg.contains("journal_mode"), "{msg}");
        assert!(msg.contains("WAL"), "{msg}");
    }

    #[tokio::test]
    async fn test_sync_wal_retry_notifies_webhook_when_database_leaves_wal_mode() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("delete-mode-webhook.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=DELETE;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (value) VALUES ('base');
            ",
        )
        .unwrap();
        drop(conn);

        let input = SyncInput {
            db_path: db_path.clone(),
            name: "delete-mode-webhook".to_string(),
            wal_path: db_path.with_extension("db-wal"),
            wal_offset: 0,
            wal_generation: 0,
            current_txid: 1,
            db_checksum: Some(0),
            wal_salt: None,
            wal_checksum_chain: None,
        };
        let client = Arc::new(crate::s3::create_client(None).await.unwrap());
        let (url, webhook_body) = capture_one_webhook().await;
        let webhook_sender = Arc::new(WebhookSender::new(vec![WebhookConfig {
            url,
            events: vec!["upload_failed".to_string()],
            secret: None,
        }]));
        let retry_policy = RetryPolicy::new(RetryConfig {
            max_retries: 0,
            base_delay_ms: 1,
            max_delay_ms: 1,
            circuit_breaker_enabled: false,
            circuit_breaker_threshold: 10,
            circuit_breaker_cooldown_ms: 1,
        });

        let err = match sync_wal_concurrent_with_retry(
            client,
            "unused".to_string(),
            "".to_string(),
            input,
            retry_policy,
            webhook_sender,
        )
        .await
        {
            Ok(_) => panic!("retry wrapper must fail closed when SQLite is not in WAL mode"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("journal_mode"));

        let body = tokio::time::timeout(std::time::Duration::from_secs(2), webhook_body)
            .await
            .unwrap()
            .unwrap();
        let payload: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(payload["event"], "upload_failed");
        assert_eq!(payload["database"], "delete-mode-webhook");
        assert!(
            payload["error"].as_str().unwrap().contains("journal_mode"),
            "{payload:?}"
        );
    }

    #[tokio::test]
    async fn test_checkpoint_detected_webhook_event_is_sent_for_rollover() {
        // D4: rollover events must use the dedicated checkpoint_detected webhook
        // variant, not piggyback on upload_failed.
        let (url, webhook_body) = capture_one_webhook().await;
        let webhook_sender = Arc::new(WebhookSender::new(vec![WebhookConfig {
            url,
            events: vec!["checkpoint_detected".to_string()],
            secret: None,
        }]));

        webhook_sender
            .notify_checkpoint_detected("rollover-db", "checkpoint/rollover detected")
            .await;

        let body = tokio::time::timeout(std::time::Duration::from_secs(2), webhook_body)
            .await
            .unwrap()
            .unwrap();
        let payload: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(payload["event"], "checkpoint_detected");
        assert_eq!(payload["database"], "rollover-db");
        assert!(
            payload["error"]
                .as_str()
                .unwrap()
                .contains("checkpoint/rollover detected"),
            "{payload:?}"
        );
    }
}
