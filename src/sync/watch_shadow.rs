use anyhow::{anyhow, Result};
use futures::future::join_all;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::mpsc;

use crate::cache::LocalCache;
use crate::config::{CacheConfig, ResolvedDbConfig, SyncConfig, WebhookConfig};
use crate::dashboard::{self, DbStatus, MetricsState};
use crate::ltx;
use crate::retention::RetentionPolicy;
use crate::retry::{RetryConfig, RetryPolicy};
use crate::s3::{self, create_client, parse_bucket};
use crate::shadow::ShadowWal;
use crate::storage::StorageBackend;
use hadb_storage_s3::S3Storage;
use crate::uploader::{spawn_uploader, UploadMessage, Uploader};
use crate::webhook::WebhookSender;

use super::shadow::{run_compaction, sync_shadow_concurrent_with_retry, sync_shadow_to_cache_with_retry};
use super::types::{DbState, Manifest, ShadowDbState, ShadowSyncInput, TriggerState};
use super::verify::validate_backup_integrity;
use super::wal_sync::take_snapshot_with_retry;

/// Shadow WAL mode decouples S3 uploads from SQLite's active WAL file:
/// - Holds a read transaction to prevent SQLite auto-checkpoint
/// - Copies WAL frames to shadow directory on notification
/// - Uploads from shadow directory (not active WAL)
/// - Manually triggers checkpoints when ready
pub async fn watch_with_shadow(
    databases: Vec<ResolvedDbConfig>,
    bucket: &str,
    endpoint: Option<&str>,
    global_sync: SyncConfig,
    compact_policy: Option<RetentionPolicy>,
    metrics_port: u16,
    no_metrics: bool,
    retry_config: RetryConfig,
    webhooks: Vec<WebhookConfig>,
    cache_config: CacheConfig,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = Arc::new(create_client(endpoint).await?);

    // Set up retry policy and webhook sender
    let retry_policy = RetryPolicy::new(retry_config.clone());
    let webhook_sender = Arc::new(WebhookSender::new(webhooks));

    if retry_config.max_retries > 0 {
        tracing::info!(
            "Retry enabled: {} attempts, {}ms base delay, {}ms max delay",
            retry_config.max_retries,
            retry_config.base_delay_ms,
            retry_config.max_delay_ms
        );
    }
    if !webhook_sender.is_empty() {
        tracing::info!("Webhooks enabled for failure notifications");
    }

    // Set up metrics server (unless disabled)
    let metrics_state = Arc::new(MetricsState::new());
    if !no_metrics {
        let state_clone = Arc::clone(&metrics_state);
        tokio::spawn(async move {
            dashboard::start_server(metrics_port, state_clone).await;
        });
    }

    // Initialize cache + uploader per database (if cache enabled)
    let mut cache_states: HashMap<PathBuf, (Arc<LocalCache>, mpsc::Sender<UploadMessage>)> = HashMap::new();
    let mut uploader_handles: Vec<(PathBuf, tokio::task::JoinHandle<()>)> = Vec::new();
    if cache_config.enabled {
        tracing::info!(
            "Shadow mode cache enabled (concurrency={}, retention={}, max_size={})",
            cache_config.uploader_concurrency,
            cache_config.retention,
            cache_config.max_size,
        );
    }

    // Initialize shadow state for each database
    let mut db_states: HashMap<PathBuf, ShadowDbState> = HashMap::new();
    let mut trigger_states: HashMap<PathBuf, TriggerState> = HashMap::new();
    let mut sync_configs: HashMap<PathBuf, SyncConfig> = HashMap::new();

    for db_config in &databases {
        let db_path = &db_config.path;
        if !db_path.exists() {
            return Err(anyhow!("Database not found: {}", db_path.display()));
        }

        let name = db_config.prefix.clone();
        let wal_path = db_path.with_extension("db-wal");

        // Check for existing state in S3 (manifest.json)
        let manifest_key = format!("{}{}/manifest.json", prefix, name);
        let (current_txid, manifest_checksum) =
            match s3::download_bytes(&client, &bucket_name, &manifest_key).await {
                Ok(data) => {
                    let manifest: Manifest = serde_json::from_slice(&data).unwrap_or_default();
                    (manifest.current_txid, manifest.last_checksum)
                }
                Err(_) => (0, None),
            };

        // Get initial checksum: from manifest if available, otherwise compute from db
        let db_checksum = match manifest_checksum {
            Some(cs) => {
                tracing::debug!("{}: Using checksum from manifest: {:#x}", name, cs);
                Some(cs)
            }
            None => match ltx::compute_checksum_from_file(db_path) {
                Ok(cs) => {
                    tracing::debug!(
                        "{}: Computed initial checksum: {:#x}",
                        name,
                        cs.into_inner()
                    );
                    Some(cs.into_inner())
                }
                Err(e) => {
                    tracing::warn!("{}: Could not compute initial checksum: {}", name, e);
                    None
                }
            },
        };

        // Create shadow WAL manager (this holds the checkpoint blocker)
        let shadow = match ShadowWal::new(db_path).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("{}: Failed to create shadow WAL: {}", name, e);
                return Err(e);
            }
        };

        tracing::info!(
            "Shadow WAL: Watching {} as '{}' (TXID: {}, generation: {}, shadow dir: {})",
            db_path.display(),
            name,
            current_txid,
            shadow.generation(),
            shadow.shadow_dir().display()
        );

        // Initialize cache + uploader for this database (if cache enabled)
        if cache_config.enabled {
            let cache = Arc::new(LocalCache::new(db_path)?);
            let storage: Arc<dyn StorageBackend> = Arc::new(
                S3Storage::new((*client).clone(), bucket_name.clone())
            );
            let s3_prefix = format!("{}{}", prefix, name);
            let uploader = Arc::new(Uploader::new(
                name.clone(),
                Arc::clone(&cache),
                storage,
                s3_prefix,
                Arc::new(retry_policy.clone()),
                Arc::clone(&webhook_sender),
                cache_config.uploader_concurrency,
            ));
            let (upload_tx, handle) = spawn_uploader(uploader);
            cache_states.insert(db_path.clone(), (cache, upload_tx));
            uploader_handles.push((db_path.clone(), handle));
        }

        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name,
                db_path: db_path.clone(),
                wal_path,
                current_txid,
                last_snapshot: None,
                db_checksum,
                shadow,
                shadow_sync_offset: 0,
                wal_copy_offset: 0,
            },
        );

        trigger_states.insert(db_path.clone(), TriggerState::default());
        sync_configs.insert(db_path.clone(), db_config.sync.clone());

        // Update dashboard with initial state
        let wal_size = std::fs::metadata(&db_path.with_extension("db-wal"))
            .map(|m| m.len())
            .unwrap_or(0);
        metrics_state
            .update_db(DbStatus {
                name: db_config.prefix.clone(),
                path: db_path.display().to_string(),
                last_sync_timestamp: 0,
                wal_size_bytes: wal_size,
                next_snapshot_timestamp: chrono::Utc::now().timestamp()
                    + global_sync.snapshot_interval as i64,
                error_count: 0,
                snapshot_count: 0,
                current_txid,
                last_error: None,
                errors_last_hour: None,
            })
            .await;
    }

    // Initial copy of any existing WAL data to shadow
    for (_db_path, state) in db_states.iter_mut() {
        if state.wal_path.exists() {
            match state.shadow.copy_frames(state.wal_copy_offset).await {
                Ok((frames, new_offset)) => {
                    if !frames.is_empty() {
                        tracing::debug!(
                            "{}: Initial shadow copy: {} frames",
                            state.name,
                            frames.len()
                        );
                        state.wal_copy_offset = new_offset;
                    }
                }
                Err(e) => {
                    tracing::error!("{}: Initial shadow copy failed: {}", state.name, e);
                }
            }
        }
    }

    // Take initial snapshots if on_startup is enabled
    for (db_path, state) in db_states.iter_mut() {
        let sync_config = sync_configs.get(db_path).unwrap_or(&global_sync);
        if sync_config.on_startup {
            // Convert to DbState temporarily for snapshot
            let mut db_state = DbState {
                name: state.name.clone(),
                db_path: state.db_path.clone(),
                wal_path: state.wal_path.clone(),
                wal_offset: 0,
                wal_generation: state.shadow.generation(),
                current_txid: state.current_txid,
                last_snapshot: state.last_snapshot,
                db_checksum: state.db_checksum,
            };
            if let Err(e) = take_snapshot_with_retry(
                &client,
                &bucket_name,
                &prefix,
                &mut db_state,
                &retry_policy,
                &webhook_sender,
            )
            .await
            {
                tracing::error!("{}: Initial snapshot failed: {}", state.name, e);
            } else {
                state.current_txid = db_state.current_txid;
                state.last_snapshot = db_state.last_snapshot;
                state.db_checksum = db_state.db_checksum;

                if let Some(trigger) = trigger_states.get_mut(db_path) {
                    trigger.frames_since_snapshot = 0;
                    trigger.first_change_time = None;
                }
            }
        }
    }

    // Set up periodic timers
    let snapshot_interval = Duration::from_secs(global_sync.snapshot_interval);
    let mut snapshot_timer = tokio::time::interval(snapshot_interval);

    let wal_sync_interval = Duration::from_secs(global_sync.wal_sync_interval);
    let mut wal_sync_timer = tokio::time::interval(wal_sync_interval);
    wal_sync_timer.tick().await;

    let compact_interval_duration = if global_sync.compact_interval > 0 {
        Duration::from_secs(global_sync.compact_interval)
    } else {
        Duration::from_secs(u64::MAX)
    };
    let mut compact_timer = tokio::time::interval(compact_interval_duration);
    compact_timer.tick().await;

    // Shadow mode: checkpoint is manual via shadow.checkpoint()
    let checkpoint_interval_duration = if global_sync.checkpoint_interval > 0 {
        Duration::from_secs(global_sync.checkpoint_interval)
    } else {
        Duration::from_secs(u64::MAX)
    };
    let mut checkpoint_timer = tokio::time::interval(checkpoint_interval_duration);
    checkpoint_timer.tick().await;

    let trigger_interval_duration = Duration::from_secs(global_sync.wal_sync_interval);
    let mut trigger_timer = tokio::time::interval(trigger_interval_duration);

    let validation_interval_duration = if global_sync.validation_interval > 0 {
        Duration::from_secs(global_sync.validation_interval)
    } else {
        Duration::from_secs(u64::MAX)
    };
    let mut validation_timer = tokio::time::interval(validation_interval_duration);
    validation_timer.tick().await;

    // Cache cleanup timer (every 5 minutes when cache is enabled)
    let cache_enabled = !cache_states.is_empty();
    let mut cache_cleanup_timer = tokio::time::interval(Duration::from_secs(300));
    cache_cleanup_timer.tick().await; // Skip first immediate tick

    // Parse cache retention for cleanup
    let cache_retention = if cache_config.enabled {
        crate::config::parse_duration_string(&cache_config.retention).ok()
    } else {
        None
    };

    tracing::info!(
        "walrust shadow mode running (snapshot: {}s, WAL sync: {}s, checkpoint: {}s)",
        global_sync.snapshot_interval,
        global_sync.wal_sync_interval,
        global_sync.checkpoint_interval
    );

    // Set up shutdown signal
    let shutdown_signal = async {
        #[cfg(unix)]
        {
            use signal::unix::{signal, SignalKind};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to set up SIGTERM handler");
            let mut sigint =
                signal(SignalKind::interrupt()).expect("Failed to set up SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => "SIGTERM",
                _ = sigint.recv() => "SIGINT",
            }
        }
        #[cfg(not(unix))]
        {
            signal::ctrl_c()
                .await
                .expect("Failed to set up Ctrl+C handler");
            "Ctrl+C"
        }
    };
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            // Handle shutdown signals
            signal_name = &mut shutdown_signal => {
                tracing::info!("Received {}, initiating graceful shutdown...", signal_name);
                break;
            }

            // Poll and sync at wal_sync_interval
            _ = wal_sync_timer.tick() => {
                // Copy any new WAL frames to shadow for all databases
                for state in db_states.values_mut() {
                    if state.wal_path.exists() {
                        match state.shadow.copy_frames(state.wal_copy_offset).await {
                            Ok((frames, new_offset)) => {
                                if !frames.is_empty() {
                                    tracing::debug!(
                                        "{}: Copied {} frames to shadow (offset {} -> {})",
                                        state.name,
                                        frames.len(),
                                        state.wal_copy_offset,
                                        new_offset
                                    );
                                    state.wal_copy_offset = new_offset;
                                }
                            }
                            Err(e) => {
                                tracing::error!("{}: Shadow copy failed: {}", state.name, e);
                            }
                        }
                    }
                }

                // Phase 1: Collect inputs for ALL databases with shadow segments
                let sync_inputs: Vec<ShadowSyncInput> = db_states
                    .values()
                    .map(|state| ShadowSyncInput {
                        db_path: state.db_path.clone(),
                        name: state.name.clone(),
                        current_txid: state.current_txid,
                        db_checksum: state.db_checksum,
                        generation: state.shadow.generation(),
                        shadow_sync_offset: state.shadow_sync_offset,
                        page_size: state.shadow.page_size(),
                        shadow_dir: state.shadow.shadow_dir().to_path_buf(),
                    })
                    .collect();

                if sync_inputs.is_empty() {
                    continue;
                }

                // Phase 2: Run all syncs concurrently (cache path or direct S3)
                let sync_futures: Vec<_> = sync_inputs
                    .into_iter()
                    .map(|input| {
                        let policy = retry_policy.clone();
                        let webhooks = Arc::clone(&webhook_sender);

                        if let Some((cache, upload_tx)) = cache_states.get(&input.db_path) {
                            // Cache path: write to disk cache, uploader handles S3
                            let cache = Arc::clone(cache);
                            let upload_tx = upload_tx.clone();
                            Box::pin(sync_shadow_to_cache_with_retry(
                                cache, upload_tx, input, policy, webhooks,
                            )) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<_>> + Send>>
                        } else {
                            // Direct S3 path (no cache)
                            let client = Arc::clone(&client);
                            let bucket = bucket_name.clone();
                            let pfx = prefix.clone();
                            Box::pin(sync_shadow_concurrent_with_retry(
                                client, bucket, pfx, input, policy, webhooks,
                            ))
                        }
                    })
                    .collect();

                let results = join_all(sync_futures).await;

                // Phase 3: Apply results sequentially
                for result in results {
                    match result {
                        Ok(output) if output.frame_count > 0 => {
                            if let Some(state) = db_states.get_mut(&output.db_path) {
                                state.shadow_sync_offset = output.new_shadow_sync_offset;
                                state.current_txid = output.new_current_txid;
                                state.db_checksum = output.new_db_checksum;

                                // Update dashboard
                                let shadow_size = walkdir::WalkDir::new(state.shadow.shadow_dir())
                                    .into_iter()
                                    .filter_map(|e| e.ok())
                                    .filter_map(|e| e.metadata().ok())
                                    .map(|m| m.len())
                                    .sum::<u64>();

                                metrics_state.update_db(DbStatus {
                                    name: state.name.clone(),
                                    path: state.db_path.display().to_string(),
                                    last_sync_timestamp: chrono::Utc::now().timestamp(),
                                    wal_size_bytes: shadow_size,
                                    next_snapshot_timestamp: state.last_snapshot.map(|t| t.timestamp() + global_sync.snapshot_interval as i64).unwrap_or(0),
                                    error_count: 0,
                                    snapshot_count: 0,
                                    current_txid: state.current_txid,
                                    last_error: None,
                                    errors_last_hour: None,
                                }).await;

                                // Update trigger state
                                if let Some(trigger) = trigger_states.get_mut(&output.db_path) {
                                    trigger.frames_since_snapshot += output.frame_count;
                                    trigger.last_wal_activity = Some(std::time::Instant::now());
                                    if trigger.first_change_time.is_none() {
                                        trigger.first_change_time = Some(std::time::Instant::now());
                                    }

                                    // Check max_changes trigger
                                    let sync_config = sync_configs.get(&output.db_path).unwrap_or(&global_sync);
                                    if sync_config.max_changes > 0
                                        && trigger.frames_since_snapshot >= sync_config.max_changes
                                    {
                                        tracing::info!(
                                            "{}: max_changes trigger ({} frames)",
                                            state.name,
                                            trigger.frames_since_snapshot
                                        );
                                        // Trigger snapshot
                                        let mut db_state = DbState {
                                            name: state.name.clone(),
                                            db_path: state.db_path.clone(),
                                            wal_path: state.wal_path.clone(),
                                            wal_offset: 0,
                                            wal_generation: state.shadow.generation(),
                                            current_txid: state.current_txid,
                                            last_snapshot: state.last_snapshot,
                                            db_checksum: state.db_checksum,
                                        };
                                        if let Err(e) = take_snapshot_with_retry(&client, &bucket_name, &prefix, &mut db_state, &retry_policy, &webhook_sender).await {
                                            tracing::error!("Failed to snapshot {}: {}", state.name, e);
                                            metrics_state.record_error(&state.name);
                                        } else {
                                            state.current_txid = db_state.current_txid;
                                            state.last_snapshot = db_state.last_snapshot;
                                            state.db_checksum = db_state.db_checksum;
                                            metrics_state.record_snapshot(&state.name);
                                            trigger.frames_since_snapshot = 0;
                                            trigger.first_change_time = None;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(_) => {} // No frames synced
                        Err(e) => {
                            tracing::error!("Shadow sync failed: {}", e);
                        }
                    }
                }
            }

            // Trigger timer for max_interval and on_idle checks
            _ = trigger_timer.tick() => {
                let now = std::time::Instant::now();

                for (db_path, trigger) in trigger_states.iter_mut() {
                    let sync_config = sync_configs.get(db_path).unwrap_or(&global_sync);

                    if trigger.frames_since_snapshot == 0 {
                        continue;
                    }

                    let state = match db_states.get_mut(db_path) {
                        Some(s) => s,
                        None => continue,
                    };

                    let mut should_snapshot = false;
                    let mut reason = "";

                    // Check max_interval
                    if sync_config.max_interval > 0 {
                        if let Some(first_change) = trigger.first_change_time {
                            if now.duration_since(first_change).as_secs() >= sync_config.max_interval {
                                should_snapshot = true;
                                reason = "max_interval";
                            }
                        }
                    }

                    // Check on_idle
                    if !should_snapshot && sync_config.on_idle > 0 {
                        if let Some(last_activity) = trigger.last_wal_activity {
                            if now.duration_since(last_activity).as_secs() >= sync_config.on_idle {
                                should_snapshot = true;
                                reason = "on_idle";
                            }
                        }
                    }

                    if should_snapshot {
                        tracing::info!(
                            "{}: {} trigger ({} frames)",
                            state.name,
                            reason,
                            trigger.frames_since_snapshot
                        );

                        let mut db_state = DbState {
                            name: state.name.clone(),
                            db_path: state.db_path.clone(),
                            wal_path: state.wal_path.clone(),
                            wal_offset: 0,
                            wal_generation: state.shadow.generation(),
                            current_txid: state.current_txid,
                            last_snapshot: state.last_snapshot,
                            db_checksum: state.db_checksum,
                        };

                        if let Err(e) = take_snapshot_with_retry(&client, &bucket_name, &prefix, &mut db_state, &retry_policy, &webhook_sender).await {
                            tracing::error!("Failed to snapshot {}: {}", state.name, e);
                            metrics_state.record_error(&state.name);
                        } else {
                            state.current_txid = db_state.current_txid;
                            state.last_snapshot = db_state.last_snapshot;
                            state.db_checksum = db_state.db_checksum;
                            metrics_state.record_snapshot(&state.name);
                            trigger.frames_since_snapshot = 0;
                            trigger.first_change_time = None;
                        }
                    }
                }
            }

            // Periodic snapshot timer
            _ = snapshot_timer.tick() => {
                for (db_path, state) in db_states.iter_mut() {
                    let mut db_state = DbState {
                        name: state.name.clone(),
                        db_path: state.db_path.clone(),
                        wal_path: state.wal_path.clone(),
                        wal_offset: 0,
                        wal_generation: state.shadow.generation(),
                        current_txid: state.current_txid,
                        last_snapshot: state.last_snapshot,
                        db_checksum: state.db_checksum,
                    };

                    if let Err(e) = take_snapshot_with_retry(&client, &bucket_name, &prefix, &mut db_state, &retry_policy, &webhook_sender).await {
                        tracing::error!("Failed to snapshot {}: {}", state.name, e);
                        metrics_state.record_error(&state.name);
                    } else {
                        state.current_txid = db_state.current_txid;
                        state.last_snapshot = db_state.last_snapshot;
                        state.db_checksum = db_state.db_checksum;
                        metrics_state.record_snapshot(&state.name);

                        if let Some(trigger) = trigger_states.get_mut(db_path) {
                            trigger.frames_since_snapshot = 0;
                            trigger.first_change_time = None;
                        }
                    }
                }

                // Run compaction after snapshots if enabled
                if global_sync.compact_after_snapshot {
                    if let Some(ref policy) = compact_policy {
                        for state in db_states.values() {
                            if let Err(e) = run_compaction(&client, &bucket_name, &prefix, &state.name, policy).await {
                                tracing::error!("Failed to compact {}: {}", state.name, e);
                            }
                        }
                    }
                }
            }

            // Compaction timer
            _ = compact_timer.tick(), if global_sync.compact_interval > 0 => {
                if let Some(ref policy) = compact_policy {
                    for state in db_states.values() {
                        if let Err(e) = run_compaction(&client, &bucket_name, &prefix, &state.name, policy).await {
                            tracing::error!("Failed to compact {}: {}", state.name, e);
                        }
                    }
                }
            }

            // Checkpoint timer - use shadow.checkpoint() for manual control
            _ = checkpoint_timer.tick(), if global_sync.checkpoint_interval > 0 => {
                for (_db_path, state) in db_states.iter_mut() {
                    // Check if shadow has accumulated enough data
                    let segments = match state.shadow.list_segments(state.shadow.generation()).await {
                        Ok(s) => s,
                        Err(_) => continue,
                    };

                    let total_segment_size: u64 = segments.iter().map(|s| s.size).sum();
                    let page_size = state.shadow.page_size() as u64;
                    let frame_size = 24 + page_size; // header + page
                    let estimated_frames = if frame_size > 0 { total_segment_size / frame_size } else { 0 };

                    let sync_config = sync_configs.get(&state.db_path).unwrap_or(&global_sync);

                    if estimated_frames >= sync_config.min_checkpoint_page_count {
                        tracing::info!(
                            "{}: Running shadow checkpoint (~{} frames)",
                            state.name,
                            estimated_frames
                        );

                        // First, ensure all shadow data is uploaded
                        // Then trigger checkpoint via shadow
                        if let Err(e) = state.shadow.checkpoint().await {
                            tracing::error!("{}: Shadow checkpoint failed: {}", state.name, e);
                        } else {
                            tracing::debug!("{}: Shadow checkpoint completed", state.name);
                            // Clean up old generation segments
                            let current_gen = state.shadow.generation();
                            if current_gen > 0 {
                                if let Err(e) = state.shadow.cleanup_segments(current_gen).await {
                                    tracing::warn!("{}: Shadow cleanup failed: {}", state.name, e);
                                }
                            }
                        }
                    } else {
                        tracing::debug!(
                            "{}: Skipping checkpoint (only ~{} frames, need {})",
                            state.name,
                            estimated_frames,
                            sync_config.min_checkpoint_page_count
                        );
                    }
                }
            }

            // Cache cleanup timer
            _ = cache_cleanup_timer.tick(), if cache_enabled => {
                if let Some(ref retention) = cache_retention {
                    for (db_path, (cache, _)) in cache_states.iter() {
                        let name = db_states.get(db_path).map(|s| s.name.as_str()).unwrap_or("unknown");
                        match cache.cleanup(*retention, cache_config.max_size) {
                            Ok(stats) => {
                                if stats.deleted_count > 0 {
                                    tracing::info!(
                                        "{}: Cache cleanup: deleted {} files ({:.2} MB), {:.2} MB remaining",
                                        name,
                                        stats.deleted_count,
                                        stats.deleted_bytes as f64 / (1024.0 * 1024.0),
                                        stats.remaining_bytes as f64 / (1024.0 * 1024.0)
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::error!("{}: Cache cleanup failed: {}", name, e);
                            }
                        }
                    }
                }
            }

            // Validation timer
            _ = validation_timer.tick(), if global_sync.validation_interval > 0 => {
                for (_db_path, state) in db_states.iter() {
                    let db_name = &state.name;

                    tracing::debug!("{}: Running periodic backup validation", db_name);

                    match validate_backup_integrity(&client, &bucket_name, &prefix, db_name).await {
                        Ok(result) => {
                            if result.is_valid {
                                tracing::info!(
                                    "{}: Validation passed ({} files, {:.2} MB)",
                                    db_name,
                                    result.verified_count,
                                    result.verified_size_bytes as f64 / (1024.0 * 1024.0)
                                );
                                metrics_state.record_validation_success(db_name);
                            } else {
                                tracing::error!(
                                    "{}: Validation failed with {} issues",
                                    db_name,
                                    result.issues.len()
                                );
                                for issue in &result.issues {
                                    tracing::error!("  {}: {}", issue.filename, issue.issue);
                                }
                                metrics_state.record_validation_failure(db_name);
                            }
                        }
                        Err(e) => {
                            tracing::error!("{}: Validation error: {}", db_name, e);
                            metrics_state.record_validation_failure(db_name);
                        }
                    }
                }
            }
        }
    }

    // Graceful shutdown - sync remaining shadow data
    tracing::info!("Shadow mode shutdown: syncing remaining data...");

    for (_db_path, state) in db_states.iter_mut() {
        // Copy any remaining WAL frames
        if let Ok((frames, _)) = state.shadow.copy_frames(state.wal_copy_offset).await {
            if !frames.is_empty() {
                tracing::debug!("{}: Final shadow copy: {} frames", state.name, frames.len());
            }
        }
    }

    // Shutdown uploaders (drain in-flight uploads)
    for (db_path, (_, upload_tx)) in cache_states.iter() {
        let name = db_states.get(db_path).map(|s| s.name.as_str()).unwrap_or("unknown");
        tracing::debug!("{}: Sending shutdown to uploader", name);
        if let Err(e) = upload_tx.send(UploadMessage::Shutdown).await {
            tracing::error!("{}: Failed to send shutdown to uploader: {}", name, e);
        }
    }

    // Wait for uploaders to finish draining (with timeout)
    let drain_timeout = Duration::from_secs(10);
    for (db_path, handle) in uploader_handles {
        let name = db_states.get(&db_path).map(|s| s.name.as_str()).unwrap_or("unknown");
        match tokio::time::timeout(drain_timeout, handle).await {
            Ok(Ok(())) => tracing::debug!("{}: Uploader drained successfully", name),
            Ok(Err(e)) => tracing::error!("{}: Uploader task panicked: {}", name, e),
            Err(_) => tracing::warn!("{}: Uploader drain timed out after {:?}", name, drain_timeout),
        }
    }

    tracing::info!("walrust shadow mode shutdown complete");
    Ok(())
}
