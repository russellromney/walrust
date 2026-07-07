use anyhow::Result;
use chrono::Utc;
use hadb_storage_s3::S3Storage;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::cache::LocalCache;
use crate::dashboard::{DbStatus, MetricsState};
use crate::ltx;
use crate::retry::{classify_error, ErrorKind, RetryPolicy};
use crate::s3;
use crate::shadow::ShadowWal;
use crate::uploader::UploadMessage;
use crate::wal;
use crate::webhook::WebhookSender;

use super::manifest::{build_ltx_key, discover_state_from_s3};
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
    let input = SyncInput::from(&state.db_state);

    let result = if let Some(cache) = cache_state {
        // Cache-enabled path: shadow WAL → encode → cache → notify uploader
        sync_wal_to_cache(&input, &cache.cache, &cache.shadow, &cache.upload_tx).await?
    } else {
        // Direct S3 upload path (current behavior)
        sync_wal_concurrent_with_retry(
            Arc::new(client.clone()),
            bucket.to_string(),
            prefix.to_string(),
            input,
            retry_policy.clone(),
            Arc::clone(webhook_sender),
        )
        .await?
    };

    // Track the WAL salt and running checksum chain even on a no-op sync so a
    // later in-place WAL reset (new salt) is detected and the chain seed stays
    // current for the next incremental read.
    state.db_state.wal_salt = result.new_wal_salt;
    state.db_state.wal_checksum_chain = result.new_wal_checksum_chain;
    if result.checkpoint_detected {
        let event = format!(
            "{}: WAL rollover/checkpoint detected; backup safety path handled it before continuing",
            state.db_state.name
        );
        tracing::warn!("{}", event);
        webhook_sender
            .notify_upload_failed(&state.db_state.name, &event, 1)
            .await;
        state.db_state.wal_generation = result.new_wal_generation;
        state.db_state.wal_offset = result.new_wal_offset;
    }

    if result.frame_count > 0 {
        // Update state
        state.db_state.wal_offset = result.new_wal_offset;
        state.db_state.current_txid = result.new_current_txid;
        state.db_state.db_checksum = result.new_db_checksum;
        if result.checkpoint_detected {
            state.db_state.wal_generation = result.new_wal_generation;
        }

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

/// Sync WAL to local cache via shadow WAL (checkpoint-safe)
///
/// This function uses the Litestream-style shadow WAL architecture:
/// 1. Shadow WAL holds checkpoint blocker (prevents SQLite from truncating WAL)
/// 2. Frames are copied from live WAL to shadow (now safe from checkpoint)
/// 3. Frames are encoded to LTX format
/// 4. LTX is written to local disk cache (atomic write)
/// 5. TXID notification sent to uploader task
///
/// The shadow WAL ensures no frames are lost to checkpoints. The uploader task
/// runs independently and handles S3 uploads with retry. This provides both
/// checkpoint safety and crash recovery.
pub(crate) async fn sync_wal_to_cache(
    input: &SyncInput,
    cache: &Arc<LocalCache>,
    shadow: &Arc<tokio::sync::Mutex<ShadowWal>>,
    upload_tx: &mpsc::Sender<UploadMessage>,
) -> Result<SyncOutput> {
    use crate::ltx::Checksum;

    // Special case: Initial sync (current_txid == 0) should create a snapshot
    if input.current_txid == 0 {
        tracing::debug!(
            "{}: Initial sync - creating snapshot from database file",
            input.name
        );

        let page_size = {
            let shadow_guard = shadow.lock().await;
            shadow_guard.page_size()
        };

        let db_path_for_encode = input.db_path.clone();
        let db_name_for_error = input.name.clone();
        let new_txid = 1u64;

        let (ltx_buffer, db_checksum_new) = tokio::task::spawn_blocking(move || {
            ltx::encode_sqlite_snapshot_to_vec(&db_path_for_encode, page_size, new_txid).map_err(
                |e| {
                    anyhow::anyhow!(
                        "{}: Initial snapshot encode failed: {}",
                        db_name_for_error,
                        e
                    )
                },
            )
        })
        .await??;

        let ltx_size = ltx_buffer.len();

        // Write to cache instead of S3. This is the full-DB snapshot base, so
        // mark it as such — the cleanup floor must never evict the restore base
        // or its incremental chain (F8).
        cache.write_snapshot_ltx(new_txid, &ltx_buffer)?;

        // Notify uploader
        if let Err(e) = upload_tx.send(UploadMessage::Upload(new_txid)).await {
            tracing::warn!(
                "{}: Failed to notify uploader for TXID {}: {}",
                input.name,
                new_txid,
                e
            );
        }

        tracing::info!(
            "{}: Created initial snapshot LTX ({} bytes, TXID 1) -> cache",
            input.name,
            ltx_size
        );

        return Ok(SyncOutput {
            db_path: input.db_path.clone(),
            frame_count: 1,
            new_wal_offset: 0,
            new_current_txid: new_txid,
            new_db_checksum: Some(db_checksum_new.into_inner()),
            checkpoint_detected: false,
            new_wal_generation: input.wal_generation,
            new_wal_salt: None,
            new_wal_checksum_chain: None,
        });
    }

    // Normal incremental sync path using shadow WAL
    // The shadow WAL:
    // 1. Holds a checkpoint blocker connection (prevents auto-checkpoint)
    // 2. Copies frames from live WAL to shadow segment files
    // 3. Detects checkpoints via WAL salt changes
    // 4. Returns frames that are now safely stored in shadow
    let mut shadow_guard = shadow.lock().await;
    let page_size = shadow_guard.page_size();

    // Copy frames from live WAL to shadow (checkpoint-safe)
    let (frames, new_offset) = shadow_guard.copy_frames(input.wal_offset).await?;

    // Track if checkpoint was detected (shadow increments generation on checkpoint)
    let shadow_gen = shadow_guard.generation();
    let checkpoint_detected = shadow_gen > input.wal_generation;
    let wal_generation = shadow_gen;

    // Chain continues through checkpoints — no need to recompute from file
    let db_checksum = input.db_checksum;

    drop(shadow_guard); // Release lock before CPU-bound encoding

    if frames.is_empty() {
        return Ok(SyncOutput {
            db_path: input.db_path.clone(),
            frame_count: 0,
            new_wal_offset: new_offset,
            new_current_txid: input.current_txid,
            new_db_checksum: db_checksum,
            checkpoint_detected,
            new_wal_generation: wal_generation,
            new_wal_salt: input.wal_salt,
            new_wal_checksum_chain: input.wal_checksum_chain,
        });
    }

    // Deduplicate pages and extract final committed db size in one pass (move, not clone)
    let frame_count = frames.len();
    let mut final_db_size = 0u32;
    let mut page_map: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
    for frame in frames {
        if frame.db_size > 0 {
            final_db_size = frame.db_size;
        }
        page_map.insert(frame.page_number, frame.data);
    }

    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();

    // Get pre_apply_checksum from state or compute from db
    let pre_checksum = match db_checksum {
        Some(cs) => Checksum::new(cs),
        None => {
            tracing::debug!(
                "{}: Computing checksum from database (no cached value)",
                input.name
            );
            ltx::compute_checksum_from_file(&input.db_path)?
        }
    };

    // Increment TXID for this incremental
    let min_txid = input.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if final_db_size > 0 {
        final_db_size
    } else {
        let db_size = std::fs::metadata(&input.db_path)?.len();
        (db_size / page_size as u64) as u32
    };

    // Chained page checksum: O(changed pages), no disk read
    let expected_post = ltx::chain_checksum(pre_checksum, &pages);

    // Encode as incremental LTX
    let unique_pages = pages.len();
    let estimated_size = unique_pages
        .saturating_mul(page_size as usize)
        .saturating_mul(2);
    let db_name_for_error = input.name.clone();
    let page_nums: Vec<u32> = pages.iter().map(|(n, _)| *n).collect();

    let (ltx_buffer, post_checksum) = tokio::task::spawn_blocking(move || {
        let mut ltx_buffer = Vec::with_capacity(estimated_size);
        let post_checksum = ltx::encode_wal_changes(
            &mut ltx_buffer,
            &pages,
            page_size,
            min_txid,
            max_txid,
            commit_page,
            Some(pre_checksum),
            expected_post,
        )
        .map_err(|e| {
            anyhow::anyhow!(
                "{}: LTX encode failed (pages={:?}, page_size={}, txid={}-{}, commit={}): {}",
                db_name_for_error,
                page_nums,
                page_size,
                min_txid,
                max_txid,
                commit_page,
                e
            )
        })?;
        Ok::<_, anyhow::Error>((ltx_buffer, post_checksum))
    })
    .await??;

    let ltx_size = ltx_buffer.len();

    // Write to cache - use max_txid as the file identifier
    cache.write_ltx(max_txid, &ltx_buffer)?;

    // Notify uploader
    if let Err(e) = upload_tx.send(UploadMessage::Upload(max_txid)).await {
        tracing::warn!(
            "{}: Failed to notify uploader for TXID {}: {}",
            input.name,
            max_txid,
            e
        );
    }

    tracing::info!(
        "{}: Synced {} WAL frames to cache ({} bytes, {} unique pages, TXID {}-{})",
        input.name,
        frame_count,
        ltx_size,
        unique_pages,
        min_txid,
        max_txid
    );

    Ok(SyncOutput {
        db_path: input.db_path.clone(),
        frame_count: frame_count as u64,
        new_wal_offset: new_offset,
        new_current_txid: max_txid,
        new_db_checksum: Some(post_checksum.into_inner()),
        checkpoint_detected,
        new_wal_generation: wal_generation,
        new_wal_salt: input.wal_salt,
        new_wal_checksum_chain: input.wal_checksum_chain,
    })
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

    // CRITICAL: Checkpoint WAL to ensure all committed data is in the main database file.
    // Without this, we could snapshot stale data if another connection holds WAL frames.
    // Use PASSIVE to avoid blocking writers (we'll get whatever is safely checkpointable).
    checkpoint_wal(&state.db_path).await?;

    // Get page size from database header
    let page_size = get_page_size(&state.db_path).await?;

    // Increment TXID for this snapshot
    let new_txid = state.current_txid + 1;

    // Discover current generation from S3 and create new one
    let (_, current_gen, _) = discover_state_from_s3(client, bucket, prefix, &state.name).await?;
    let snapshot_gen = current_gen + 1;

    // Snapshots go to generation 1+ (litestream format)
    let ltx_key = build_ltx_key(prefix, &state.name, snapshot_gen, 1, new_txid);

    let (ltx_buffer, db_checksum) =
        ltx::encode_sqlite_snapshot_to_vec(&state.db_path, page_size, new_txid)?;

    let ltx_size = ltx_buffer.len() as u64;

    // Upload LTX file
    s3::upload_bytes(client, bucket, &ltx_key, ltx_buffer).await?;

    tracing::info!(
        "{}: LTX snapshot uploaded (gen {}, TXID 1-{}, {} bytes, checksum {:#x}) -> {}",
        state.name,
        snapshot_gen,
        new_txid,
        ltx_size,
        db_checksum.into_inner(),
        ltx_key
    );

    // Update state. The snapshot folded all WAL frames into the base, so the
    // WAL cursor must be reset, not left pointing into the pre-checkpoint WAL
    // (F11). A PASSIVE checkpoint may restart the WAL with a new salt; re-read
    // the header so the next incremental read re-seeds the salt/checksum chain
    // and reads from offset 0 of the new generation. The snapshot's db_checksum
    // becomes the explicit hand-off base for the first incremental.
    state.current_txid = new_txid;
    state.last_snapshot = Some(timestamp);
    state.db_checksum = Some(db_checksum.into_inner());
    state.wal_offset = 0;
    state.wal_generation += 1;
    state.wal_salt = wal::read_header(&state.wal_path)
        .await
        .ok()
        .flatten()
        .map(|h| h.salt());
    // Force the next read to re-seed the running checksum from the (new) header.
    state.wal_checksum_chain = None;

    Ok(())
}

pub(crate) async fn checkpoint_wal(db_path: &Path) -> Result<()> {
    let db_path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

/// Get SQLite database page size from header
pub(crate) async fn get_page_size(db_path: &Path) -> Result<u32> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(db_path).await?;
    let mut header = [0u8; 100];
    file.read_exact(&mut header).await?;

    // Page size is at offset 16-17, big-endian
    let page_size = u16::from_be_bytes([header[16], header[17]]) as u32;

    // Page size of 1 means 65536
    let page_size = if page_size == 1 { 65536 } else { page_size };

    Ok(page_size)
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
}
