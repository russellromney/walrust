use anyhow::{anyhow, Result};
use futures::future::join_all;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;

use crate::cache::LocalCache;
use crate::config::{parse_duration_string, CacheConfig, ResolvedDbConfig, SyncConfig, WebhookConfig};
use crate::retention::RetentionPolicy;
use crate::dashboard::{self, DbStatus, MetricsState};
use crate::ltx;
use crate::retry::{RetryConfig, RetryPolicy};
use crate::s3::{self, create_client, parse_bucket};
use crate::shadow::ShadowWal;
use crate::storage::{S3Backend, StorageBackend};
use crate::uploader::{spawn_uploader, UploadMessage, Uploader};
use crate::webhook::WebhookSender;

use super::compact::{get_wal_page_count, run_checkpoint, CheckpointMode};
use super::manifest::discover_state_from_s3;
use super::restore::validate_backup_integrity;
use super::shadow::{run_compaction, sync_shadow_concurrent_with_retry};
use super::types::{CacheState, DbState, DbTaskState, Manifest, ShadowDbState, ShadowSyncInput, SyncInput, TriggerState};
use super::wal_sync::{do_sync, sync_wal, sync_wal_concurrent_with_retry, sync_wal_with_retry, take_snapshot, take_snapshot_with_retry};

pub async fn watch(
    databases: Vec<PathBuf>,
    bucket: &str,
    snapshot_interval: u64,
    endpoint: Option<&str>,
    compact_after_snapshot: bool,
    compact_interval: u64,
    compact_policy: Option<RetentionPolicy>,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = Arc::new(create_client(endpoint).await?);

    // Initialize state for each database
    let mut db_states: HashMap<PathBuf, DbState> = HashMap::new();

    for db_path in &databases {
        if !db_path.exists() {
            return Err(anyhow!("Database not found: {}", db_path.display()));
        }

        let name = db_path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("Invalid database path: {}", db_path.display()))?
            .to_string();

        let wal_path = db_path.with_extension("db-wal");

        // Discover state from S3 file listings (litestream format - no manifest)
        let (current_txid, _max_gen, _) =
            discover_state_from_s3(&client, &bucket_name, &prefix, &name).await?;

        // WAL offset and generation are local state - start fresh
        // (we always take a snapshot on startup to ensure consistency)
        let wal_offset = 0u64;
        let wal_generation = 0u64;

        // Compute checksum from database file
        let db_checksum = match ltx::compute_checksum_from_file(db_path) {
            Ok(cs) => {
                tracing::debug!("{}: Computed initial checksum: {:#x}", name, cs.into_inner());
                Some(cs.into_inner())
            }
            Err(e) => {
                tracing::warn!("{}: Could not compute initial checksum: {}", name, e);
                None
            }
        };

        tracing::info!(
            "Watching {} (WAL offset: {}, generation: {}, TXID: {}, checksum: {})",
            db_path.display(),
            wal_offset,
            wal_generation,
            current_txid,
            db_checksum.map(|c| format!("{:#x}", c)).unwrap_or_else(|| "none".to_string())
        );

        db_states.insert(
            db_path.clone(),
            DbState {
                name,
                db_path: db_path.clone(),
                wal_path,
                wal_offset,
                wal_generation,
                current_txid,
                last_snapshot: None,
                db_checksum,
            },
        );
    }

    // Initial sync of any existing WAL data
    for state in db_states.values_mut() {
        if state.wal_path.exists() {
            let _ = sync_wal(&client, &bucket_name, &prefix, state).await?;
        }
    }

    // Take initial snapshots
    for state in db_states.values_mut() {
        take_snapshot(&client, &bucket_name, &prefix, state).await?;
    }

    let snapshot_interval = Duration::from_secs(snapshot_interval);
    let mut snapshot_timer = tokio::time::interval(snapshot_interval);

    // Set up WAL sync timer (poll every 1 second by default)
    let mut wal_sync_timer = tokio::time::interval(Duration::from_secs(1));
    wal_sync_timer.tick().await; // Skip first tick

    // Set up compaction timer (only if compact_interval > 0)
    let compact_interval_duration = if compact_interval > 0 {
        Duration::from_secs(compact_interval)
    } else {
        Duration::from_secs(u64::MAX) // Effectively disabled
    };
    let mut compact_timer = tokio::time::interval(compact_interval_duration);
    // Skip the first immediate tick
    compact_timer.tick().await;

    if compact_after_snapshot {
        tracing::info!(
            "walrust running (snapshot interval: {}s, compact after snapshot: enabled)",
            snapshot_interval.as_secs()
        );
    } else if compact_interval > 0 {
        tracing::info!(
            "walrust running (snapshot interval: {}s, compact interval: {}s)",
            snapshot_interval.as_secs(),
            compact_interval
        );
    } else {
        tracing::info!("walrust running (snapshot interval: {}s)", snapshot_interval.as_secs());
    }

    loop {
        tokio::select! {
            // Poll and sync WAL changes
            _ = wal_sync_timer.tick() => {
                for state in db_states.values_mut() {
                    if state.wal_path.exists() {
                        match sync_wal(&client, &bucket_name, &prefix, state).await {
                            Ok(_frame_count) => {}
                            Err(e) => tracing::error!("Failed to sync WAL for {}: {}", state.name, e),
                        }
                    }
                }
            }

            // Snapshot timer
            _ = snapshot_timer.tick() => {
                for state in db_states.values_mut() {
                    if let Err(e) = take_snapshot(&client, &bucket_name, &prefix, state).await {
                        tracing::error!("Failed to snapshot {}: {}", state.name, e);
                    }
                }

                // Run compaction after snapshots if enabled
                if compact_after_snapshot {
                    if let Some(ref policy) = compact_policy {
                        for state in db_states.values() {
                            if let Err(e) = run_compaction(&client, &bucket_name, &prefix, &state.name, policy).await {
                                tracing::error!("Failed to compact {}: {}", state.name, e);
                            }
                        }
                    }
                }
            }

            // Compaction timer (if enabled)
            _ = compact_timer.tick(), if compact_interval > 0 => {
                if let Some(ref policy) = compact_policy {
                    for state in db_states.values() {
                        if let Err(e) = run_compaction(&client, &bucket_name, &prefix, &state.name, policy).await {
                            tracing::error!("Failed to compact {}: {}", state.name, e);
                        }
                    }
                }
            }
        }
    }
}
pub async fn watch_with_config(
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

    // Note: Cache mode is only supported with independent tasks (watch_with_independent_tasks)
    // This legacy shared-state mode doesn't support cache - log warning if enabled
    if cache_config.enabled {
        tracing::warn!("Cache mode is only supported with independent tasks architecture. Using direct S3 upload.");
    }

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

    // Initialize state for each database
    let mut db_states: HashMap<PathBuf, DbState> = HashMap::new();
    let mut trigger_states: HashMap<PathBuf, TriggerState> = HashMap::new();
    let mut sync_configs: HashMap<PathBuf, SyncConfig> = HashMap::new();

    for db_config in &databases {
        let db_path = &db_config.path;
        if !db_path.exists() {
            return Err(anyhow!("Database not found: {}", db_path.display()));
        }

        let name = db_config.prefix.clone();
        let wal_path = db_path.with_extension("db-wal");

        // Discover state from S3 file listings (litestream format - no manifest)
        let (current_txid, _max_gen, _) =
            discover_state_from_s3(&client, &bucket_name, &prefix, &name).await?;

        // WAL offset and generation are local state - start fresh
        // (we always take a snapshot on startup to ensure consistency)
        let wal_offset = 0u64;
        let wal_generation = 0u64;

        // Compute checksum from database file
        let db_checksum = match ltx::compute_checksum_from_file(db_path) {
            Ok(cs) => {
                tracing::debug!("{}: Computed initial checksum: {:#x}", name, cs.into_inner());
                Some(cs.into_inner())
            }
            Err(e) => {
                tracing::warn!("{}: Could not compute initial checksum: {}", name, e);
                None
            }
        };

        tracing::info!(
            "Watching {} as '{}' (WAL offset: {}, generation: {}, TXID: {}, checksum: {})",
            db_path.display(),
            name,
            wal_offset,
            wal_generation,
            current_txid,
            db_checksum.map(|c| format!("{:#x}", c)).unwrap_or_else(|| "none".to_string())
        );

        db_states.insert(
            db_path.clone(),
            DbState {
                name,
                db_path: db_path.clone(),
                wal_path,
                wal_offset,
                wal_generation,
                current_txid,
                last_snapshot: None,
                db_checksum,
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

    // Take initial snapshots FIRST if on_startup is enabled.
    // This establishes the base state before any incremental WAL syncs.
    // The snapshot checkpoints WAL to main db, ensuring all committed data is captured.
    for (db_path, state) in db_states.iter_mut() {
        let sync_config = sync_configs.get(db_path).unwrap_or(&global_sync);
        if sync_config.on_startup {
            take_snapshot_with_retry(&client, &bucket_name, &prefix, state, &retry_policy, &webhook_sender).await?;
            if let Some(trigger) = trigger_states.get_mut(db_path) {
                trigger.frames_since_snapshot = 0;
                trigger.first_change_time = None;
            }

            // Run compaction after initial snapshot if enabled
            if sync_config.compact_after_snapshot {
                if let Some(ref policy) = compact_policy {
                    if let Err(e) =
                        run_compaction(&client, &bucket_name, &prefix, &state.name, policy).await
                    {
                        tracing::error!("Failed to compact {}: {}", state.name, e);
                    }
                }
            }
        }
    }

    // Sync any WAL data that accumulated since the snapshot (typically empty after checkpoint)
    for (db_path, state) in db_states.iter_mut() {
        if state.wal_path.exists() {
            match sync_wal_with_retry(&client, &bucket_name, &prefix, state, &retry_policy, &webhook_sender).await {
                Ok(frame_count) if frame_count > 0 => {
                    if let Some(trigger) = trigger_states.get_mut(db_path) {
                        trigger.frames_since_snapshot += frame_count;
                        trigger.last_wal_activity = Some(std::time::Instant::now());
                        if trigger.first_change_time.is_none() {
                            trigger.first_change_time = Some(std::time::Instant::now());
                        }
                    }
                }
                Ok(_) => {} // No frames to sync
                Err(e) => {
                    // WAL sync failure after snapshot is non-fatal - log and continue
                    tracing::warn!("{}: Initial WAL sync failed (will retry on next change): {}", state.name, e);
                }
            }
        }
    }

    // Set up periodic snapshot timer based on global config
    let snapshot_interval = Duration::from_secs(global_sync.snapshot_interval);
    let mut snapshot_timer = tokio::time::interval(snapshot_interval);

    // Set up WAL sync timer (batches WAL changes instead of syncing immediately)
    let wal_sync_interval = Duration::from_secs(global_sync.wal_sync_interval);
    let mut wal_sync_timer = tokio::time::interval(wal_sync_interval);
    wal_sync_timer.tick().await; // Skip first tick

    // Set up compaction timer
    let compact_interval_duration = if global_sync.compact_interval > 0 {
        Duration::from_secs(global_sync.compact_interval)
    } else {
        Duration::from_secs(u64::MAX)
    };
    let mut compact_timer = tokio::time::interval(compact_interval_duration);
    compact_timer.tick().await; // Skip first tick

    // Set up checkpoint timer (PASSIVE mode, non-blocking)
    let checkpoint_interval_duration = if global_sync.checkpoint_interval > 0 {
        Duration::from_secs(global_sync.checkpoint_interval)
    } else {
        Duration::from_secs(u64::MAX) // Disabled
    };
    let mut checkpoint_timer = tokio::time::interval(checkpoint_interval_duration);
    checkpoint_timer.tick().await; // Skip first tick

    // Set up trigger check interval (uses wal_sync_interval for polling)
    let trigger_interval_duration = Duration::from_secs(global_sync.wal_sync_interval);
    let mut trigger_timer = tokio::time::interval(trigger_interval_duration);

    // Set up validation timer (periodic backup integrity check)
    let validation_interval_duration = if global_sync.validation_interval > 0 {
        Duration::from_secs(global_sync.validation_interval)
    } else {
        Duration::from_secs(u64::MAX) // Disabled
    };
    let mut validation_timer = tokio::time::interval(validation_interval_duration);
    validation_timer.tick().await; // Skip first tick

    // Log startup info with sync trigger settings
    let triggers_enabled = global_sync.max_changes > 0
        || global_sync.max_interval > 0
        || global_sync.on_idle > 0;

    let validation_info = if global_sync.validation_interval > 0 {
        format!(", validation: {}s", global_sync.validation_interval)
    } else {
        String::new()
    };

    if triggers_enabled {
        tracing::info!(
            "walrust running (snapshot: {}s, WAL sync: {}s, checkpoint: {}s, max_changes: {}, max_interval: {}s, on_idle: {}s{})",
            global_sync.snapshot_interval,
            global_sync.wal_sync_interval,
            global_sync.checkpoint_interval,
            global_sync.max_changes,
            global_sync.max_interval,
            global_sync.on_idle,
            validation_info
        );
    } else {
        tracing::info!(
            "walrust running (snapshot: {}s, WAL sync: {}s, checkpoint: {}s{})",
            global_sync.snapshot_interval,
            global_sync.wal_sync_interval,
            global_sync.checkpoint_interval,
            validation_info
        );
    }

    // Set up shutdown signal future (SIGTERM or SIGINT on Unix, Ctrl+C on other platforms)
    let shutdown_signal = async {
        #[cfg(unix)]
        {
            use signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set up SIGTERM handler");
            let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set up SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => "SIGTERM",
                _ = sigint.recv() => "SIGINT",
            }
        }
        #[cfg(not(unix))]
        {
            signal::ctrl_c().await.expect("Failed to set up Ctrl+C handler");
            "Ctrl+C"
        }
    };
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            // Handle shutdown signals for graceful shutdown
            signal_name = &mut shutdown_signal => {
                tracing::info!("Received {}, initiating graceful shutdown...", signal_name);
                break;
            }

            // Poll and sync WAL changes at wal_sync_interval
            _ = wal_sync_timer.tick() => {
                // Collect inputs for all databases that have WAL files
                let sync_inputs: Vec<SyncInput> = db_states
                    .values()
                    .filter(|state| state.wal_path.exists())
                    .map(SyncInput::from)
                    .collect();

                if sync_inputs.is_empty() {
                    continue;
                }

                // Phase 2: Run all syncs concurrently
                let sync_futures: Vec<_> = sync_inputs
                    .into_iter()
                    .map(|input| {
                        let client = Arc::clone(&client);
                        let bucket = bucket_name.clone();
                        let pfx = prefix.clone();
                        let policy = retry_policy.clone();
                        let webhooks = Arc::clone(&webhook_sender);
                        sync_wal_concurrent_with_retry(client, bucket, pfx, input, policy, webhooks)
                    })
                    .collect();

                let results = join_all(sync_futures).await;

                // Phase 3: Apply results sequentially (state updates)
                for result in results {
                    match result {
                        Ok(output) if output.frame_count > 0 => {
                            // Apply state changes
                            if let Some(state) = db_states.get_mut(&output.db_path) {
                                state.wal_offset = output.new_wal_offset;
                                state.current_txid = output.new_current_txid;
                                state.db_checksum = output.new_db_checksum;
                                if output.checkpoint_detected {
                                    state.wal_generation = output.new_wal_generation;
                                }

                                // Update dashboard
                                let wal_size = std::fs::metadata(&state.wal_path).map(|m| m.len()).unwrap_or(0);
                                metrics_state.update_db(DbStatus {
                                    name: state.name.clone(),
                                    path: state.db_path.display().to_string(),
                                    last_sync_timestamp: chrono::Utc::now().timestamp(),
                                    wal_size_bytes: wal_size,
                                    next_snapshot_timestamp: state.last_snapshot.map(|t| t.timestamp() + global_sync.snapshot_interval as i64).unwrap_or(0),
                                    error_count: 0,
                                    snapshot_count: 0,
                                    current_txid: state.current_txid,
                                    last_error: None,
                                    errors_last_hour: None,
                                }).await;

                                // Check for emergency truncate threshold
                                let sync_config = sync_configs.get(&output.db_path).unwrap_or(&global_sync);
                                if sync_config.wal_truncate_threshold_pages > 0 {
                                    if let Ok(wal_pages) = get_wal_page_count(&state.wal_path).await {
                                        if wal_pages >= sync_config.wal_truncate_threshold_pages {
                                            tracing::warn!(
                                                "{}: WAL size ({} pages) exceeded emergency threshold ({} pages) - triggering TRUNCATE checkpoint",
                                                state.name,
                                                wal_pages,
                                                sync_config.wal_truncate_threshold_pages
                                            );
                                            if let Err(e) = run_checkpoint(&state.db_path, CheckpointMode::Truncate).await {
                                                tracing::error!("{}: Emergency TRUNCATE checkpoint failed: {}", state.name, e);
                                            } else {
                                                tracing::info!("{}: Emergency TRUNCATE checkpoint completed", state.name);
                                            }
                                        }
                                    }
                                }

                                // Update trigger state and check max_changes
                                if let Some(trigger) = trigger_states.get_mut(&output.db_path) {
                                    trigger.frames_since_snapshot += output.frame_count;
                                    trigger.last_wal_activity = Some(std::time::Instant::now());
                                    if trigger.first_change_time.is_none() {
                                        trigger.first_change_time = Some(std::time::Instant::now());
                                    }

                                    if sync_config.max_changes > 0
                                        && trigger.frames_since_snapshot >= sync_config.max_changes
                                    {
                                        tracing::info!(
                                            "{}: max_changes trigger ({} frames)",
                                            state.name,
                                            trigger.frames_since_snapshot
                                        );
                                        if let Err(e) = take_snapshot_with_retry(&client, &bucket_name, &prefix, state, &retry_policy, &webhook_sender).await {
                                            tracing::error!("Failed to snapshot {}: {}", state.name, e);
                                            metrics_state.record_error(&state.name);
                                        } else {
                                            metrics_state.record_snapshot(&state.name);
                                            trigger.frames_since_snapshot = 0;
                                            trigger.first_change_time = None;

                                            if sync_config.compact_after_snapshot {
                                                if let Some(ref policy) = compact_policy {
                                                    let _ = run_compaction(&client, &bucket_name, &prefix, &state.name, policy).await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(_) => {} // No frames synced
                        Err(e) => {
                            tracing::error!("Failed to sync WAL: {}", e);
                            // Error already recorded in concurrent_with_retry via webhook
                        }
                    }
                }
            }

            // Check sync triggers
            _ = trigger_timer.tick() => {
                let now = std::time::Instant::now();

                for (db_path, trigger) in trigger_states.iter_mut() {
                    let sync_config = sync_configs.get(db_path).unwrap_or(&global_sync);

                    // Skip if no pending changes
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
                            let elapsed = now.duration_since(first_change);
                            if elapsed.as_secs() >= sync_config.max_interval {
                                should_snapshot = true;
                                reason = "max_interval";
                            }
                        }
                    }

                    // Check on_idle
                    if !should_snapshot && sync_config.on_idle > 0 {
                        if let Some(last_activity) = trigger.last_wal_activity {
                            let idle_duration = now.duration_since(last_activity);
                            if idle_duration.as_secs() >= sync_config.on_idle {
                                should_snapshot = true;
                                reason = "on_idle";
                            }
                        }
                    }

                    if should_snapshot {
                        tracing::info!(
                            "{}: {} trigger ({} pending frames)",
                            state.name,
                            reason,
                            trigger.frames_since_snapshot
                        );

                        if let Err(e) = take_snapshot_with_retry(&client, &bucket_name, &prefix, state, &retry_policy, &webhook_sender).await {
                            tracing::error!("Failed to snapshot {}: {}", state.name, e);
                            metrics_state.record_error(&state.name);
                        } else {
                            metrics_state.record_snapshot(&state.name);
                            trigger.frames_since_snapshot = 0;
                            trigger.first_change_time = None;
                            trigger.last_wal_activity = None;

                            if sync_config.compact_after_snapshot {
                                if let Some(ref policy) = compact_policy {
                                    let _ = run_compaction(&client, &bucket_name, &prefix, &state.name, policy).await;
                                }
                            }
                        }
                    }
                }
            }

            // Periodic snapshot timer
            _ = snapshot_timer.tick() => {
                for (db_path, state) in db_states.iter_mut() {
                    if let Err(e) = take_snapshot_with_retry(&client, &bucket_name, &prefix, state, &retry_policy, &webhook_sender).await {
                        tracing::error!("Failed to snapshot {}: {}", state.name, e);
                        metrics_state.record_error(&state.name);
                    } else {
                        metrics_state.record_snapshot(&state.name);
                        // Reset trigger state after scheduled snapshot
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

            // Compaction timer (if enabled)
            _ = compact_timer.tick(), if global_sync.compact_interval > 0 => {
                if let Some(ref policy) = compact_policy {
                    for state in db_states.values() {
                        if let Err(e) = run_compaction(&client, &bucket_name, &prefix, &state.name, policy).await {
                            tracing::error!("Failed to compact {}: {}", state.name, e);
                        }
                    }
                }
            }

            // Periodic PASSIVE checkpoint
            _ = checkpoint_timer.tick(), if global_sync.checkpoint_interval > 0 => {
                for (db_path, state) in db_states.iter_mut() {
                    let sync_config = sync_configs.get(db_path).unwrap_or(&global_sync);

                    // Check if WAL has enough pages to warrant checkpoint
                    if let Ok(wal_pages) = get_wal_page_count(&state.wal_path).await {
                        if wal_pages >= sync_config.min_checkpoint_page_count {
                            tracing::info!(
                                "{}: Running PASSIVE checkpoint ({} pages)",
                                state.name,
                                wal_pages
                            );

                            if let Err(e) = run_checkpoint(&state.db_path, CheckpointMode::Passive).await {
                                tracing::error!("{}: PASSIVE checkpoint failed: {}", state.name, e);
                            } else {
                                tracing::debug!("{}: PASSIVE checkpoint completed", state.name);
                            }
                        } else {
                            tracing::debug!(
                                "{}: Skipping checkpoint (only {} pages, need {})",
                                state.name,
                                wal_pages,
                                sync_config.min_checkpoint_page_count
                            );
                        }
                    }
                }
            }

            // Periodic backup validation
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

    // Graceful shutdown: final sync for all databases with pending changes (5s timeout)
    tracing::info!("Completing final syncs before shutdown...");

    let shutdown_start = std::time::Instant::now();
    let shutdown_timeout = Duration::from_secs(5);
    let mut synced_count = 0;
    let mut failed_count = 0;

    let db_paths: Vec<_> = db_states.keys().cloned().collect();
    for db_path in db_paths {
        if shutdown_start.elapsed() >= shutdown_timeout {
            tracing::warn!("Shutdown timeout reached");
            break;
        }

        if let Some(state) = db_states.get_mut(&db_path) {
            if !state.wal_path.exists() {
                continue;
            }
            let remaining = shutdown_timeout.saturating_sub(shutdown_start.elapsed());
            match tokio::time::timeout(
                remaining,
                sync_wal_with_retry(&client, &bucket_name, &prefix, state, &retry_policy, &webhook_sender)
            ).await {
                Ok(Ok(frame_count)) if frame_count > 0 => {
                    tracing::debug!("{}: Final sync completed ({} frames)", state.name, frame_count);
                    synced_count += 1;
                }
                Ok(Ok(_)) => {} // No frames to sync
                Ok(Err(e)) => {
                    tracing::error!("{}: Final sync failed: {}", state.name, e);
                    failed_count += 1;
                }
                Err(_) => {
                    tracing::warn!("{}: Final sync timed out", state.name);
                    failed_count += 1;
                }
            }
        }
    }

    if synced_count > 0 || failed_count > 0 {
        tracing::info!(
            "Shutdown sync complete: {} succeeded, {} failed",
            synced_count,
            failed_count
        );
    }

    tracing::info!("walrust shutdown complete");
    Ok(())
}

// ============================================================================
// Independent per-DB tasks mode - maximum concurrency
// ============================================================================

/// Watch databases using independent per-DB tasks for maximum concurrency
///
/// Each database gets its own task that independently:
/// - Watches its WAL file for changes
/// - Debounces rapid writes (100ms)
/// - Syncs at max_interval even under continuous writes
/// - Uses spawn_blocking for CPU-bound encoding
///
/// This architecture allows 250+ databases to sync concurrently,
/// with CPU-bound encoding distributed across the thread pool.
pub async fn watch_with_independent_tasks(
    databases: Vec<ResolvedDbConfig>,
    bucket: &str,
    endpoint: Option<&str>,
    global_sync: SyncConfig,
    _compact_policy: Option<RetentionPolicy>,
    metrics_port: u16,
    no_metrics: bool,
    retry_config: RetryConfig,
    webhooks: Vec<WebhookConfig>,
    cache_config: CacheConfig,
) -> Result<()> {
    use tokio::sync::broadcast;

    // Parse cache retention duration
    let cache_retention = if cache_config.enabled {
        match parse_duration_string(&cache_config.retention) {
            Ok(d) => {
                tracing::info!(
                    "Cache enabled: retention={}, max_size={}MB",
                    cache_config.retention,
                    cache_config.max_size / 1024 / 1024
                );
                Some(d)
            }
            Err(e) => {
                return Err(anyhow!("Invalid cache retention '{}': {}", cache_config.retention, e));
            }
        }
    } else {
        None
    };

    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = Arc::new(create_client(endpoint).await?);

    // Set up retry policy and webhook sender
    let retry_policy = RetryPolicy::new(retry_config.clone());
    let webhook_sender = Arc::new(WebhookSender::new(webhooks));

    // Set up metrics server (unless disabled)
    let metrics_state = Arc::new(MetricsState::new());
    if !no_metrics {
        let state_clone = Arc::clone(&metrics_state);
        tokio::spawn(async move {
            dashboard::start_server(metrics_port, state_clone).await;
        });
    }

    // Shutdown broadcast channel
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Initialize and spawn independent task for each database
    let mut task_handles = Vec::new();

    for db_config in databases {
        let db_path = &db_config.path;
        if !db_path.exists() {
            return Err(anyhow!("Database not found: {}", db_path.display()));
        }

        let name = db_config.prefix.clone();
        let wal_path = db_path.with_extension("db-wal");

        // Discover state from S3 file listings (litestream format - no manifest)
        let (current_txid, _max_gen, _) =
            discover_state_from_s3(&client, &bucket_name, &prefix, &name).await?;

        // WAL offset and generation are local state - start fresh
        let wal_offset = 0u64;
        let wal_generation = 0u64;

        // Compute checksum from database file
        let db_checksum = match ltx::compute_checksum_from_file(db_path) {
            Ok(cs) => Some(cs.into_inner()),
            Err(_) => None,
        };

        tracing::info!(
            "Spawning independent task for {} (TXID: {}, checksum: {})",
            name,
            current_txid,
            db_checksum.map(|c| format!("{:#x}", c)).unwrap_or_else(|| "none".to_string())
        );

        // Initial sync of any existing WAL data (before starting event loop)
        // This ensures we don't miss frames that exist when walrust starts
        tracing::debug!("{}: Checking for existing WAL at {:?}", name, wal_path);
        let wal_exists = wal_path.exists();
        tracing::debug!("{}: WAL exists = {}", name, wal_exists);

        let (wal_offset, wal_generation, current_txid, db_checksum) = if wal_exists {
            tracing::debug!("{}: Starting initial sync (offset={}, gen={}, txid={})", name, wal_offset, wal_generation, current_txid);
            let input = SyncInput {
                db_path: db_path.clone(),
                name: name.clone(),
                wal_path: wal_path.clone(),
                wal_offset,
                wal_generation,
                current_txid,
                db_checksum,
            };
            match sync_wal_concurrent_with_retry(
                Arc::clone(&client),
                bucket_name.clone(),
                prefix.clone(),
                input,
                retry_policy.clone(),
                Arc::clone(&webhook_sender),
            ).await {
                Ok(result) => {
                    tracing::debug!("{}: Initial sync returned: frame_count={}, new_offset={}, new_txid={}",
                        name, result.frame_count, result.new_wal_offset, result.new_current_txid);
                    if result.frame_count > 0 {
                        tracing::info!("{}: Initial sync captured {} frames", name, result.frame_count);
                    } else {
                        tracing::debug!("{}: Initial sync returned 0 frames", name);
                    }
                    (
                        result.new_wal_offset,
                        result.new_wal_generation,
                        result.new_current_txid,
                        result.new_db_checksum,
                    )
                }
                Err(e) => {
                    tracing::warn!("{}: Initial sync failed (will retry on changes): {}", name, e);
                    (wal_offset, wal_generation, current_txid, db_checksum)
                }
            }
        } else {
            tracing::debug!("{}: No WAL file found, skipping initial sync", name);
            (wal_offset, wal_generation, current_txid, db_checksum)
        };

        // Create task state with potentially updated values from initial sync
        let task_state = DbTaskState {
            db_state: DbState {
                name: name.clone(),
                db_path: db_path.clone(),
                wal_path: wal_path.clone(),
                wal_offset,
                wal_generation,
                current_txid,
                last_snapshot: None,
                db_checksum,
            },
            trigger_state: TriggerState::default(),
            sync_config: db_config.sync.clone(),
        };

        // Initialize cache and uploader if cache is enabled
        let cache_state = if let Some(ref retention) = cache_retention {
            // Determine cache directory path
            let cache_dir = if let Some(ref custom_path) = cache_config.path {
                PathBuf::from(custom_path).join(&name)
            } else {
                // Default: .{db_name}-walrust/ next to database file
                let parent = db_path.parent().unwrap_or(Path::new("."));
                parent.join(format!(".{}-walrust", name))
            };

            // Create cache directory if it doesn't exist
            if !cache_dir.exists() {
                std::fs::create_dir_all(&cache_dir)
                    .map_err(|e| anyhow!("Failed to create cache directory {}: {}", cache_dir.display(), e))?;
            }

            // Create LocalCache
            let cache = Arc::new(LocalCache::new(&cache_dir)?);
            tracing::debug!("{}: LocalCache initialized at {}", name, cache_dir.display());

            // Create ShadowWal for checkpoint-safe frame copying
            let shadow = ShadowWal::new(db_path).await
                .map_err(|e| anyhow!("{}: Failed to create shadow WAL: {}", name, e))?;
            let shadow = Arc::new(tokio::sync::Mutex::new(shadow));
            tracing::debug!("{}: ShadowWal initialized (checkpoint blocker active)", name);

            // Resume pending uploads count
            let pending_count = cache.pending_uploads().len();
            if pending_count > 0 {
                tracing::info!("{}: Found {} pending uploads to resume", name, pending_count);
            }

            // Create S3Backend for uploader
            // Note: AWS SDK Client is Clone (cheap Arc internally)
            let storage: Arc<dyn StorageBackend> = Arc::new(
                S3Backend::new((*client).clone(), bucket_name.clone())
            );

            // Create Uploader
            let s3_prefix = format!("{}/{}", prefix, name);
            let uploader = Arc::new(Uploader::new(
                name.clone(),
                Arc::clone(&cache),
                storage,
                s3_prefix,
                Arc::new(retry_policy.clone()),
                Arc::clone(&webhook_sender),
            ));

            // Spawn uploader task and get channel
            let upload_tx = spawn_uploader(uploader);

            Some(CacheState {
                cache,
                shadow,
                upload_tx,
                retention_duration: *retention,
                max_cache_size: cache_config.max_size,
            })
        } else {
            None
        };

        // Spawn independent task
        let client = Arc::clone(&client);
        let bucket = bucket_name.clone();
        let pfx = prefix.clone();
        let policy = retry_policy.clone();
        let webhooks = Arc::clone(&webhook_sender);
        let metrics = Arc::clone(&metrics_state);
        let shutdown_rx = shutdown_tx.subscribe();

        let handle = tokio::spawn(async move {
            if let Err(e) = run_db_task(
                task_state,
                client,
                bucket,
                pfx,
                policy,
                webhooks,
                metrics,
                shutdown_rx,
                cache_state,
            ).await {
                tracing::error!("{}: Task failed: {}", name, e);
            }
        });

        task_handles.push(handle);
    }

    tracing::info!(
        "walrust running with {} independent tasks (debounce: 100ms, max_interval: {}s)",
        task_handles.len(),
        global_sync.wal_sync_interval
    );

    // Wait for shutdown signal
    let shutdown_signal = async {
        #[cfg(unix)]
        {
            use signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set up SIGTERM handler");
            let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set up SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => "SIGTERM",
                _ = sigint.recv() => "SIGINT",
            }
        }
        #[cfg(not(unix))]
        {
            signal::ctrl_c().await.expect("Failed to set up Ctrl+C handler");
            "Ctrl+C"
        }
    };

    let signal_name = shutdown_signal.await;
    tracing::info!("Received {}, initiating graceful shutdown...", signal_name);

    // Signal all tasks to shutdown
    let _ = shutdown_tx.send(());

    // Wait for all tasks to complete (with timeout)
    let shutdown_timeout = Duration::from_secs(10);
    match tokio::time::timeout(shutdown_timeout, async {
        for handle in task_handles {
            let _ = handle.await;
        }
    }).await {
        Ok(_) => tracing::info!("All tasks shut down gracefully"),
        Err(_) => tracing::warn!("Shutdown timeout - some tasks may not have completed"),
    }

    tracing::info!("walrust shutdown complete");
    Ok(())
}

// ============================================================================
// Shadow WAL mode - decouples S3 uploads from SQLite WAL
// ============================================================================

/// State for a watched database in shadow mode
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

                // Phase 2: Run all syncs concurrently
                let sync_futures: Vec<_> = sync_inputs
                    .into_iter()
                    .map(|input| {
                        let client = Arc::clone(&client);
                        let bucket = bucket_name.clone();
                        let pfx = prefix.clone();
                        let policy = retry_policy.clone();
                        let webhooks = Arc::clone(&webhook_sender);
                        sync_shadow_concurrent_with_retry(client, bucket, pfx, input, policy, webhooks)
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

    tracing::info!("walrust shadow mode shutdown complete");
    Ok(())
}

/// Sync shadow WAL to S3 concurrently
async fn run_db_task(
    mut state: DbTaskState,
    client: Arc<aws_sdk_s3::Client>,
    bucket: String,
    prefix: String,
    retry_policy: RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
    metrics_state: Arc<MetricsState>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    cache_state: Option<CacheState>,
) -> Result<()> {
    let db_name = state.db_state.name.clone();
    let wal_path = state.db_state.wal_path.clone();

    // Poll interval: check WAL size and sync every N seconds
    let poll_interval = Duration::from_secs(state.sync_config.wal_sync_interval);
    let mut poll_timer = tokio::time::interval(poll_interval);
    poll_timer.tick().await; // Skip first immediate tick

    // Track last synced WAL size to detect changes
    let mut last_synced_wal_size: u64 = std::fs::metadata(&wal_path)
        .map(|m| m.len())
        .unwrap_or(0);

    tracing::debug!(
        "{}: Task started, polling every {}s (WAL: {})",
        db_name,
        poll_interval.as_secs(),
        wal_path.display()
    );

    loop {
        tokio::select! {
            // Shutdown signal
            _ = shutdown_rx.recv() => {
                // Final sync before shutdown
                let current_wal_size = std::fs::metadata(&wal_path)
                    .map(|m| m.len())
                    .unwrap_or(0);
                if current_wal_size > last_synced_wal_size {
                    let _ = do_sync(&mut state, &client, &bucket, &prefix, &retry_policy, &webhook_sender, &metrics_state, cache_state.as_ref()).await;
                }
                // Signal uploader to shutdown if cache is enabled
                if let Some(ref cache) = cache_state {
                    let _ = cache.upload_tx.send(UploadMessage::Shutdown).await;
                }
                break;
            }

            // Poll timer - check WAL size and sync if changed
            _ = poll_timer.tick() => {
                let current_wal_size = std::fs::metadata(&wal_path)
                    .map(|m| m.len())
                    .unwrap_or(0);

                // Only sync if WAL has grown
                if current_wal_size > last_synced_wal_size {
                    match do_sync(&mut state, &client, &bucket, &prefix, &retry_policy, &webhook_sender, &metrics_state, cache_state.as_ref()).await {
                        Ok(frame_count) => {
                            if frame_count > 0 {
                                tracing::debug!("{}: Synced {} frames", db_name, frame_count);
                            }
                            last_synced_wal_size = current_wal_size;
                        }
                        Err(e) => {
                            tracing::error!("{}: Sync failed: {}", db_name, e);
                            // Don't update last_synced_wal_size on failure - will retry next tick
                        }
                    }
                }
            }
        }
    }

    tracing::debug!("{}: Task exiting", db_name);
    Ok(())
}
