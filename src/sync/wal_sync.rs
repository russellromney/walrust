use anyhow::Result;
use chrono::Utc;
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

use super::manifest::{build_ltx_key, discover_state_from_s3, save_state, GENERATION_LIVE};
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
    use litepages::Checksum;

    // Special case: Initial sync (current_txid == 0) should ALWAYS create a snapshot from DB file
    // This handles the case where WAL file exists but is empty (0 bytes)
    if input.current_txid == 0 {
        tracing::debug!("{}: Initial sync - creating snapshot from database file", input.name);

        // Get page size from WAL header if available, otherwise use default
        let page_size = match wal::read_header(&input.wal_path).await? {
            Some(h) => h.page_size,
            None => 4096, // SQLite default page size
        };

        let db_path_for_encode = input.db_path.clone();
        let db_size = std::fs::metadata(&input.db_path)?.len() as usize;
        let estimated_size = db_size.saturating_mul(2);
        let db_name_for_error = input.name.clone();
        let new_txid = 1u64; // Initial snapshot is TXID 1

        let (ltx_buffer, db_checksum_new) = tokio::task::spawn_blocking(move || {
            let mut ltx_buffer = Vec::with_capacity(estimated_size);
            ltx::encode_snapshot(&mut ltx_buffer, &db_path_for_encode, page_size, new_txid)
                .map_err(|e| anyhow::anyhow!("{}: Initial snapshot encode failed: {}", db_name_for_error, e))?;
            let db_checksum = ltx::compute_checksum_from_file(&db_path_for_encode)?;
            Ok::<_, anyhow::Error>((ltx_buffer, db_checksum))
        }).await??;

        let ltx_size = ltx_buffer.len() as u64;
        // Snapshots go to generation 1+ (litestream format)
        let ltx_key = build_ltx_key(prefix, &input.name, 1, 1, new_txid);

        // Upload snapshot LTX file
        s3::upload_bytes(client, bucket, &ltx_key, ltx_buffer).await?;

        tracing::info!(
            "{}: Created initial snapshot LTX ({} bytes, TXID 1-{}) -> {}",
            input.name,
            ltx_size,
            new_txid,
            ltx_key
        );

        return Ok(SyncOutput {
            db_path: input.db_path,
            frame_count: 1, // Snapshot represents 1 "frame"
            new_wal_offset: 0,
            new_current_txid: new_txid,
            new_db_checksum: Some(db_checksum_new.into_inner()),
            checkpoint_detected: false,
            new_wal_generation: input.wal_generation,
        });
    }

    // Normal incremental sync path
    let header = match wal::read_header(&input.wal_path).await? {
        Some(h) => h,
        None => {
            // No WAL file - return no-op output
            return Ok(SyncOutput {
                db_path: input.db_path,
                frame_count: 0,
                new_wal_offset: input.wal_offset,
                new_current_txid: input.current_txid,
                new_db_checksum: input.db_checksum,
                checkpoint_detected: false,
                new_wal_generation: input.wal_generation,
            });
        }
    };

    // Track state changes locally
    let mut wal_offset = input.wal_offset;
    let mut wal_generation = input.wal_generation;
    let mut db_checksum = input.db_checksum;
    let mut checkpoint_detected = false;

    // Check if WAL was reset (checkpoint happened)
    let current_size = wal::get_wal_size(&input.wal_path).await?;
    if current_size < wal_offset {
        // WAL was truncated, start fresh and recompute checksum
        tracing::info!("{}: WAL checkpoint detected, resetting offset", input.name);
        wal_offset = 0;
        wal_generation += 1;
        checkpoint_detected = true;

        // Recompute checksum from current database state after checkpoint
        match ltx::compute_checksum_from_file(&input.db_path) {
            Ok(cs) => {
                db_checksum = Some(cs.into_inner());
                tracing::debug!("{}: Recomputed checksum after checkpoint: {:#x}", input.name, cs.into_inner());
            }
            Err(e) => {
                tracing::warn!("{}: Could not recompute checksum: {}", input.name, e);
            }
        }
    }

    // Read WAL frames as parsed pages
    let (frames, new_offset, max_db_size) =
        wal::read_frames_as_pages(&input.wal_path, header.page_size, wal_offset).await?;

    if frames.is_empty() {
        return Ok(SyncOutput {
            db_path: input.db_path,
            frame_count: 0,
            new_wal_offset: wal_offset,
            new_current_txid: input.current_txid,
            new_db_checksum: db_checksum,
            checkpoint_detected,
            new_wal_generation: wal_generation,
        });
    }

    // Deduplicate pages: keep only the latest version of each page
    let mut page_map: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
    for frame in &frames {
        page_map.insert(frame.page_number, frame.data.clone());
    }

    let frame_count = frames.len();
    let page_size = header.page_size;

    // At this point, current_txid > 0 (initial sync handled earlier)
    // Incremental sync
    // Convert to format expected by encode_wal_changes
    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();

    // Get pre_apply_checksum from state or compute from db
    let pre_checksum = match db_checksum {
        Some(cs) => Checksum::new(cs),
        None => {
            tracing::debug!("{}: Computing checksum from database (no cached value)", input.name);
            ltx::compute_checksum_from_file(&input.db_path)?
        }
    };

    // Increment TXID for this incremental
    let min_txid = input.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if max_db_size > 0 { max_db_size } else {
        let db_size = std::fs::metadata(&input.db_path)?.len();
        (db_size / page_size as u64) as u32
    };

    // Encode as incremental LTX (CPU-bound, run in blocking thread pool)
    // Pre-allocate buffer: estimate 2x pages * page_size for compression headroom
    let unique_pages = pages.len();
    let estimated_size = unique_pages.saturating_mul(page_size as usize).saturating_mul(2);
    let db_name_for_error = input.name.clone();
    let page_nums: Vec<u32> = pages.iter().map(|(n, _)| *n).collect();
    let db_path_for_checksum = input.db_path.clone();
    let (ltx_buffer, post_checksum) = tokio::task::spawn_blocking(move || {
        // Compute expected post_checksum by simulating changes against current DB
        let expected_post = ltx::compute_expected_post_checksum(&db_path_for_checksum, page_size, &pages)?;

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
        ).map_err(|e| anyhow::anyhow!("{}: LTX encode failed (pages={:?}, page_size={}, txid={}-{}, commit={}): {}",
            db_name_for_error, page_nums, page_size, min_txid, max_txid, commit_page, e))?;
        Ok::<_, anyhow::Error>((ltx_buffer, post_checksum))
    }).await??;

    let ltx_size = ltx_buffer.len() as u64;

    // Incrementals go to generation 0 (live folder, litestream format)
    let ltx_key = build_ltx_key(prefix, &input.name, GENERATION_LIVE, min_txid, max_txid);

    // Upload incremental LTX file
    s3::upload_bytes(client, bucket, &ltx_key, ltx_buffer).await?;

    tracing::info!(
        "{}: Synced {} WAL frames as incremental LTX ({} bytes, {} unique pages, TXID {}-{}) -> {}",
        input.name,
        frame_count,
        ltx_size,
        unique_pages,
        min_txid,
        max_txid,
        ltx_key
    );

    Ok(SyncOutput {
        db_path: input.db_path,
        frame_count: frame_count as u64,
        new_wal_offset: new_offset,
        new_current_txid: max_txid,
        new_db_checksum: Some(post_checksum.into_inner()),
        checkpoint_detected,
        new_wal_generation: wal_generation,
    })
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
                    webhook_sender.notify_auth_failure(&db_name, &e.to_string()).await;
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
                        .notify_sync_failed(&db_name, &e.to_string(), attempts)
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
        sync_wal_to_cache(
            &input,
            &cache.cache,
            &cache.shadow,
            &cache.upload_tx,
        ).await?
    } else {
        // Direct S3 upload path (current behavior)
        sync_wal_concurrent_with_retry(
            Arc::new(client.clone()),
            bucket.to_string(),
            prefix.to_string(),
            input,
            retry_policy.clone(),
            Arc::clone(webhook_sender),
        ).await?
    };

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
        metrics_state.update_db(DbStatus {
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
        }).await;
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
    use litepages::Checksum;

    // Special case: Initial sync (current_txid == 0) should create a snapshot
    if input.current_txid == 0 {
        tracing::debug!("{}: Initial sync - creating snapshot from database file", input.name);

        let page_size = {
            let shadow_guard = shadow.lock().await;
            shadow_guard.page_size()
        };

        let db_path_for_encode = input.db_path.clone();
        let db_size = std::fs::metadata(&input.db_path)?.len() as usize;
        let estimated_size = db_size.saturating_mul(2);
        let db_name_for_error = input.name.clone();
        let new_txid = 1u64;

        let (ltx_buffer, db_checksum_new) = tokio::task::spawn_blocking(move || {
            let mut ltx_buffer = Vec::with_capacity(estimated_size);
            ltx::encode_snapshot(&mut ltx_buffer, &db_path_for_encode, page_size, new_txid)
                .map_err(|e| anyhow::anyhow!("{}: Initial snapshot encode failed: {}", db_name_for_error, e))?;
            let db_checksum = ltx::compute_checksum_from_file(&db_path_for_encode)?;
            Ok::<_, anyhow::Error>((ltx_buffer, db_checksum))
        }).await??;

        let ltx_size = ltx_buffer.len();

        // Write to cache instead of S3
        cache.write_ltx(new_txid, &ltx_buffer)?;

        // Notify uploader
        if let Err(e) = upload_tx.send(UploadMessage::Upload(new_txid)).await {
            tracing::warn!("{}: Failed to notify uploader for TXID {}: {}", input.name, new_txid, e);
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

    // Recompute checksum if checkpoint occurred
    let db_checksum = if checkpoint_detected {
        match ltx::compute_checksum_from_file(&input.db_path) {
            Ok(cs) => {
                tracing::debug!("{}: Recomputed checksum after checkpoint: {:#x}", input.name, cs.into_inner());
                Some(cs.into_inner())
            }
            Err(e) => {
                tracing::warn!("{}: Could not recompute checksum: {}", input.name, e);
                input.db_checksum
            }
        }
    } else {
        input.db_checksum
    };

    drop(shadow_guard); // Release lock before CPU-bound encoding

    // Get max_db_size from last commit frame (or 0 if none)
    let max_db_size = frames.iter().filter(|f| f.db_size > 0).map(|f| f.db_size).max().unwrap_or(0);

    if frames.is_empty() {
        return Ok(SyncOutput {
            db_path: input.db_path.clone(),
            frame_count: 0,
            new_wal_offset: new_offset,
            new_current_txid: input.current_txid,
            new_db_checksum: db_checksum,
            checkpoint_detected,
            new_wal_generation: wal_generation,
        });
    }

    // Deduplicate pages: keep only the latest version of each page
    let mut page_map: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
    for frame in &frames {
        page_map.insert(frame.page_number, frame.data.clone());
    }

    let frame_count = frames.len();
    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();

    // Get pre_apply_checksum from state or compute from db
    let pre_checksum = match db_checksum {
        Some(cs) => Checksum::new(cs),
        None => {
            tracing::debug!("{}: Computing checksum from database (no cached value)", input.name);
            ltx::compute_checksum_from_file(&input.db_path)?
        }
    };

    // Increment TXID for this incremental
    let min_txid = input.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if max_db_size > 0 { max_db_size } else {
        let db_size = std::fs::metadata(&input.db_path)?.len();
        (db_size / page_size as u64) as u32
    };

    // Encode as incremental LTX
    let unique_pages = pages.len();
    let estimated_size = unique_pages.saturating_mul(page_size as usize).saturating_mul(2);
    let db_name_for_error = input.name.clone();
    let page_nums: Vec<u32> = pages.iter().map(|(n, _)| *n).collect();
    let db_path_for_checksum = input.db_path.clone();

    let (ltx_buffer, post_checksum) = tokio::task::spawn_blocking(move || {
        // Compute expected post_checksum by simulating changes against current DB
        let expected_post = ltx::compute_expected_post_checksum(&db_path_for_checksum, page_size, &pages)?;

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
        ).map_err(|e| anyhow::anyhow!("{}: LTX encode failed (pages={:?}, page_size={}, txid={}-{}, commit={}): {}",
            db_name_for_error, page_nums, page_size, min_txid, max_txid, commit_page, e))?;
        Ok::<_, anyhow::Error>((ltx_buffer, post_checksum))
    }).await??;

    let ltx_size = ltx_buffer.len();

    // Write to cache - use max_txid as the file identifier
    cache.write_ltx(max_txid, &ltx_buffer)?;

    // Notify uploader
    if let Err(e) = upload_tx.send(UploadMessage::Upload(max_txid)).await {
        tracing::warn!("{}: Failed to notify uploader for TXID {}: {}", input.name, max_txid, e);
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
    })
}

// ============================================================================
// Retry-wrapped S3 operations for production use
// ============================================================================

/// Sync WAL changes with retry and webhook notifications
pub(crate) async fn sync_wal_with_retry(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    state: &mut DbState,
    retry_policy: &RetryPolicy,
    webhook_sender: &Arc<WebhookSender>,
) -> Result<u64> {
    let db_name = state.name.clone();
    let mut _last_error: Option<anyhow::Error> = None;
    let mut attempts = 0u32;

    // Try the sync operation with retries
    loop {
        attempts += 1;
        match sync_wal(client, bucket, prefix, state).await {
            Ok(frames) => return Ok(frames),
            Err(e) => {
                let error_kind = classify_error(&e);
                let is_retryable = matches!(error_kind, ErrorKind::Transient | ErrorKind::Unknown);

                // Handle auth errors immediately
                if error_kind == ErrorKind::AuthError {
                    tracing::error!("{}: Authentication error during WAL sync: {}", db_name, e);
                    webhook_sender.notify_auth_failure(&db_name, &e.to_string()).await;
                    return Err(e);
                }

                // If not retryable or exhausted retries, fail
                if !is_retryable || attempts > retry_policy.config().max_retries + 1 {
                    tracing::error!(
                        "{}: WAL sync failed after {} attempts: {}",
                        db_name,
                        attempts,
                        e
                    );
                    webhook_sender
                        .notify_sync_failed(&db_name, &e.to_string(), attempts)
                        .await;
                    return Err(e);
                }

                // Calculate backoff and retry
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
                _last_error = Some(e);
            }
        }
    }
}

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
                    webhook_sender.notify_auth_failure(&db_name, &e.to_string()).await;
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
                        .notify_sync_failed(&db_name, &e.to_string(), attempts)
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

/// Sync WAL changes to S3 as incremental LTX files
///
/// WAL frames are parsed, deduplicated (keeping latest version of each page),
/// encoded as LTX with checksum chaining, and uploaded to S3.
/// This provides:
/// - Unified LTX format for both snapshots and incrementals
/// - Built-in compression (LZ4)
/// - Checksum chain for integrity verification
/// - Litestream-compatible file format
pub(crate) async fn sync_wal(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    state: &mut DbState,
) -> Result<u64> {
    use litepages::Checksum;

    let header = match wal::read_header(&state.wal_path).await? {
        Some(h) => h,
        None => return Ok(0), // No WAL file
    };

    // Check if WAL was reset (checkpoint happened)
    let current_size = wal::get_wal_size(&state.wal_path).await?;
    if current_size < state.wal_offset {
        // WAL was truncated, start fresh and recompute checksum
        tracing::info!("{}: WAL checkpoint detected, resetting offset", state.name);
        state.wal_offset = 0;
        state.wal_generation += 1;

        // Recompute checksum from current database state after checkpoint
        match ltx::compute_checksum_from_file(&state.db_path) {
            Ok(cs) => {
                state.db_checksum = Some(cs.into_inner());
                tracing::debug!("{}: Recomputed checksum after checkpoint: {:#x}", state.name, cs.into_inner());
            }
            Err(e) => {
                tracing::warn!("{}: Could not recompute checksum: {}", state.name, e);
            }
        }
    }

    // Read WAL frames as parsed pages
    let (frames, new_offset, max_db_size) =
        wal::read_frames_as_pages(&state.wal_path, header.page_size, state.wal_offset).await?;

    if frames.is_empty() {
        return Ok(0);
    }

    // Deduplicate pages: keep only the latest version of each page
    // WAL can have multiple writes to the same page; we want the final state
    let mut page_map: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
    for frame in &frames {
        page_map.insert(frame.page_number, frame.data.clone());
    }

    // Convert to format expected by encode_wal_changes
    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();
    let frame_count = frames.len();

    // Get pre_apply_checksum from state or compute from db
    let pre_checksum = match state.db_checksum {
        Some(cs) => Checksum::new(cs),
        None => {
            // Fallback: compute from database
            tracing::debug!("{}: Computing checksum from database (no cached value)", state.name);
            ltx::compute_checksum_from_file(&state.db_path)?
        }
    };

    // Increment TXID for this incremental
    let min_txid = state.current_txid + 1;
    let max_txid = min_txid + pages.len() as u64 - 1;
    let commit_page = if max_db_size > 0 { max_db_size } else {
        // Estimate from database file size
        let db_size = std::fs::metadata(&state.db_path)?.len();
        (db_size / header.page_size as u64) as u32
    };

    // Encode as incremental LTX (CPU-bound, run in blocking thread pool)
    // Pre-allocate buffer: estimate 2x pages * page_size for compression headroom
    let estimated_size = pages.len().saturating_mul(header.page_size as usize).saturating_mul(2);
    let page_size = header.page_size;
    let db_name = state.name.clone();
    let db_path_for_checksum = state.db_path.clone();
    let (ltx_buffer, post_checksum) = tokio::task::spawn_blocking(move || {
        // Compute expected post_checksum by simulating changes against current DB
        let expected_post = ltx::compute_expected_post_checksum(&db_path_for_checksum, page_size, &pages)?;

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
        )?;
        Ok::<_, anyhow::Error>((ltx_buffer, post_checksum))
    }).await??;

    let ltx_size = ltx_buffer.len() as u64;

    // Incrementals go to generation 0 (live folder, litestream format)
    let ltx_key = build_ltx_key(prefix, &db_name, GENERATION_LIVE, min_txid, max_txid);

    // Upload incremental LTX file
    s3::upload_bytes(client, bucket, &ltx_key, ltx_buffer).await?;

    tracing::info!(
        "{}: Synced {} WAL frames as incremental LTX ({} bytes, TXID {}-{}) -> {}",
        state.name,
        frame_count,
        ltx_size,
        min_txid,
        max_txid,
        ltx_key
    );

    // Update state
    state.wal_offset = new_offset;
    state.current_txid = max_txid;
    state.db_checksum = Some(post_checksum.into_inner());

    // Save legacy state for backwards compat
    save_state(client, bucket, prefix, state).await?;

    Ok(frame_count as u64)
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

    // Encode database as LTX
    // Pre-allocate buffer: estimate 2x db size for compression headroom
    let db_size = std::fs::metadata(&state.db_path)?.len() as usize;
    let estimated_size = db_size.saturating_mul(2);
    let mut ltx_buffer = Vec::with_capacity(estimated_size);
    ltx::encode_snapshot(&mut ltx_buffer, &state.db_path, page_size, new_txid)?;

    let ltx_size = ltx_buffer.len() as u64;

    // Upload LTX file
    s3::upload_bytes(client, bucket, &ltx_key, ltx_buffer).await?;

    // Compute checksum from database for future incremental LTX
    let db_checksum = ltx::compute_checksum_from_file(&state.db_path)?;

    tracing::info!(
        "{}: LTX snapshot uploaded (gen {}, TXID 1-{}, {} bytes, checksum {:#x}) -> {}",
        state.name,
        snapshot_gen,
        new_txid,
        ltx_size,
        db_checksum.into_inner(),
        ltx_key
    );

    // Update state
    state.current_txid = new_txid;
    state.last_snapshot = Some(timestamp);
    state.db_checksum = Some(db_checksum.into_inner());

    Ok(())
}

/// Checkpoint WAL to ensure all committed data is in the main database file.
/// Uses PASSIVE mode to avoid blocking active writers - this checkpoints whatever
/// frames are safe to checkpoint without waiting for readers/writers.
pub(crate) async fn checkpoint_wal(db_path: &Path) -> Result<()> {
    let db_path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?;
        // PASSIVE: checkpoint frames that can be checkpointed without blocking
        // This won't block if there are active readers, but ensures we get committed data
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
