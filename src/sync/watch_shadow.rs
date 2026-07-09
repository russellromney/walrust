use anyhow::{anyhow, Context, Result};
use futures::future::join_all;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::mpsc;

use crate::cache::LocalCache;
use crate::config::{CacheConfig, ResolvedDbConfig, SyncConfig, WebhookConfig};
use crate::dashboard::{self, DbStatus, MetricsState};
use crate::errors::WalrustError;
use crate::ltx;
use crate::retention::RetentionPolicy;
use crate::retry::{RetryConfig, RetryPolicy};
use crate::s3::{self, create_client, parse_bucket};
use crate::shadow::ShadowWal;
use crate::uploader::{spawn_uploader, UploadMessage, Uploader, UploaderStats};
use crate::webhook::WebhookSender;
use hadb_storage::StorageBackend;
use hadb_storage_s3::S3Storage;
use walrust_core::legacy_shadow_watch::{
    apply_shadow_sync_result_to_state, apply_shadow_sync_results_strict, load_shadow_progress,
    save_shadow_watch_progress as save_shadow_progress, shadow_sync_input,
    wait_for_cache_checkpoint_durability,
};

use super::shadow::{
    run_compaction, sync_shadow_concurrent_with_retry, sync_shadow_to_cache_with_retry,
};
use super::types::{DbState, Manifest, ShadowDbState, ShadowSyncInput, TriggerState};
use super::verify::validate_backup_integrity;
use super::wal_sync::take_snapshot_with_retry;

type ShadowSyncFuture =
    Pin<Box<dyn Future<Output = Result<super::types::ShadowSyncOutput>> + Send>>;

const CHECKPOINT_UPLOAD_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct DirectShadowSyncTarget {
    client: Arc<aws_sdk_s3::Client>,
    bucket_name: String,
    prefix: String,
}

async fn run_shadow_syncs(
    db_states: &HashMap<PathBuf, ShadowDbState>,
    cache_states: &HashMap<PathBuf, (Arc<LocalCache>, mpsc::Sender<UploadMessage>)>,
    direct_target: Option<DirectShadowSyncTarget>,
    retry_policy: &RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
) -> Vec<Result<super::types::ShadowSyncOutput>> {
    let sync_inputs: Vec<ShadowSyncInput> = db_states.values().map(shadow_sync_input).collect();

    let sync_futures: Vec<ShadowSyncFuture> = sync_inputs
        .into_iter()
        .map(|input| {
            let policy = retry_policy.clone();
            let webhooks = Arc::clone(&webhook_sender);

            if let Some((cache, upload_tx)) = cache_states.get(&input.db_path) {
                let cache = Arc::clone(cache);
                let upload_tx = upload_tx.clone();
                Box::pin(sync_shadow_to_cache_with_retry(
                    cache, upload_tx, input, policy, webhooks,
                )) as ShadowSyncFuture
            } else if let Some(target) = direct_target.clone() {
                Box::pin(sync_shadow_concurrent_with_retry(
                    Arc::clone(&target.client),
                    target.bucket_name,
                    target.prefix,
                    input,
                    policy,
                    webhooks,
                )) as ShadowSyncFuture
            } else {
                let name = input.name;
                Box::pin(async move {
                    Err(anyhow!(
                        "{}: no cache uploader or direct upload target configured",
                        name
                    ))
                }) as ShadowSyncFuture
            }
        })
        .collect();

    join_all(sync_futures).await
}

async fn copy_final_shadow_frames(db_states: &mut HashMap<PathBuf, ShadowDbState>) -> Result<()> {
    for state in db_states.values_mut() {
        if !state.wal_path.exists() {
            continue;
        }

        let (frames, new_offset) = state
            .shadow
            .copy_frames(state.wal_copy_offset)
            .await
            .with_context(|| format!("{}: final shadow copy failed", state.name))?;

        if !frames.is_empty() {
            tracing::debug!("{}: Final shadow copy: {} frames", state.name, frames.len());
            state.wal_copy_offset = new_offset;
        }
    }

    Ok(())
}

async fn checkpoint_shadow_after_durable_sync(
    state: &mut ShadowDbState,
    cache_state: Option<&(Arc<LocalCache>, mpsc::Sender<UploadMessage>)>,
    direct_target: Option<DirectShadowSyncTarget>,
    retry_policy: &RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
    drain_timeout: Duration,
) -> Result<()> {
    let (frames, new_offset) = state
        .shadow
        .copy_frames(state.wal_copy_offset)
        .await
        .with_context(|| format!("{}: shadow copy before checkpoint failed", state.name))?;
    if !frames.is_empty() {
        tracing::debug!(
            "{}: checkpoint copied {} frames to shadow (offset {} -> {})",
            state.name,
            frames.len(),
            state.wal_copy_offset,
            new_offset
        );
        state.wal_copy_offset = new_offset;
    }

    let input = shadow_sync_input(state);
    let output = if let Some((cache, upload_tx)) = cache_state {
        sync_shadow_to_cache_with_retry(
            Arc::clone(cache),
            upload_tx.clone(),
            input,
            retry_policy.clone(),
            Arc::clone(&webhook_sender),
        )
        .await?
    } else if let Some(target) = direct_target {
        sync_shadow_concurrent_with_retry(
            Arc::clone(&target.client),
            target.bucket_name,
            target.prefix,
            input,
            retry_policy.clone(),
            Arc::clone(&webhook_sender),
        )
        .await?
    } else {
        anyhow::bail!(
            "{}: no cache uploader or direct upload target configured for checkpoint drain",
            state.name
        );
    };
    if let Some((cache, _)) = cache_state {
        wait_for_cache_checkpoint_durability(
            cache,
            &state.name,
            output.new_current_txid,
            drain_timeout,
        )
        .await?;
    }

    apply_shadow_sync_result_to_state(state, &output).await?;

    state
        .shadow
        .checkpoint()
        .await
        .with_context(|| format!("{}: shadow checkpoint failed", state.name))?;

    let cleanup_before_gen = state.shadow_sync_generation;
    if cleanup_before_gen > 0 {
        state
            .shadow
            .cleanup_segments(cleanup_before_gen)
            .await
            .with_context(|| format!("{}: shadow cleanup failed", state.name))?;
    }

    Ok(())
}

async fn shutdown_shadow_uploaders(
    cache_states: &HashMap<PathBuf, (Arc<LocalCache>, mpsc::Sender<UploadMessage>)>,
    db_states: &HashMap<PathBuf, ShadowDbState>,
    uploader_handles: Vec<(PathBuf, tokio::task::JoinHandle<Result<UploaderStats>>)>,
) -> Result<()> {
    for (db_path, (_, upload_tx)) in cache_states.iter() {
        let name = db_states
            .get(db_path)
            .map(|s| s.name.as_str())
            .unwrap_or("unknown");
        tracing::debug!("{}: Sending shutdown to uploader", name);
        upload_tx
            .send(UploadMessage::Shutdown)
            .await
            .map_err(|e| anyhow!("{}: failed to send shutdown to uploader: {}", name, e))?;
    }

    let drain_timeout = Duration::from_secs(10);
    let mut first_error = None;

    for (db_path, handle) in uploader_handles {
        let name = db_states
            .get(&db_path)
            .map(|s| s.name.as_str())
            .unwrap_or("unknown");
        match tokio::time::timeout(drain_timeout, handle).await {
            Ok(Ok(Ok(stats))) => {
                tracing::debug!("{}: Uploader drained successfully: {:?}", name, stats);
            }
            Ok(Ok(Err(e))) => {
                let e = e.context(format!("{}: uploader drain failed", name));
                tracing::error!("{}", e);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Ok(Err(e)) => {
                let e = anyhow!("{}: uploader task panicked: {}", name, e);
                tracing::error!("{}", e);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(_) => {
                let e = anyhow!(
                    "{}: uploader drain timed out after {:?}",
                    name,
                    drain_timeout
                );
                tracing::error!("{}", e);
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }

    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Perform the initial shadow copy on startup and detect databases whose live
/// WAL salt changed while walrust was down.
///
/// Returns the set of database paths that should be snapshotted eagerly (D3).
async fn initial_shadow_copy(
    db_states: &mut HashMap<PathBuf, ShadowDbState>,
) -> Result<HashSet<PathBuf>> {
    let mut eager_snapshot: HashSet<PathBuf> = HashSet::new();

    for (_db_path, state) in db_states.iter_mut() {
        if !state.wal_path.exists() {
            continue;
        }

        let generation_before = state.shadow.generation();
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

                if state.shadow.generation() > generation_before {
                    tracing::info!(
                        "{}: Downtime checkpoint detected (WAL salt mismatch); scheduling eager snapshot",
                        state.name
                    );
                    eager_snapshot.insert(state.db_path.clone());
                }
            }
            Err(e) => {
                tracing::error!("{}: Initial shadow copy failed: {}", state.name, e);
                return Err(e)
                    .with_context(|| format!("{}: initial shadow copy failed", state.name));
            }
        }
    }

    Ok(eager_snapshot)
}

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
    let client = Arc::new(
        create_client(endpoint)
            .await
            .map_err(|e| WalrustError::s3(e.to_string()))?,
    );

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
    let mut cache_states: HashMap<PathBuf, (Arc<LocalCache>, mpsc::Sender<UploadMessage>)> =
        HashMap::new();
    let mut uploader_handles: Vec<(PathBuf, tokio::task::JoinHandle<Result<UploaderStats>>)> =
        Vec::new();
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
            return Err(WalrustError::database(format!(
                "Database not found: {}",
                db_path.display()
            ))
            .into());
        }

        let name = db_config.prefix.clone();
        let wal_path = db_path.with_extension("db-wal");

        // Check for existing state in S3 (manifest.json)
        let manifest_key = format!("{}{}/manifest.json", prefix, name);
        let (mut current_txid, manifest_checksum) =
            match s3::download_bytes(&client, &bucket_name, &manifest_key).await {
                Ok(data) => {
                    let manifest: Manifest = serde_json::from_slice(&data).unwrap_or_default();
                    (manifest.current_txid, manifest.last_checksum)
                }
                Err(_) => (0, None),
            };

        // Get initial checksum: from manifest if available, otherwise compute from db
        let mut db_checksum = match manifest_checksum {
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
        let mut last_snapshot = None;
        let mut shadow_sync_generation = 0;
        let mut shadow_sync_offset = 0;

        // Create shadow WAL manager (this holds the checkpoint blocker)
        let mut shadow = match ShadowWal::new(db_path).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("{}: Failed to create shadow WAL: {}", name, e);
                return Err(e);
            }
        };

        let mut restored_wal_copy_offset = 0u64;
        if let Some(progress) = load_shadow_progress(&shadow, &name)? {
            tracing::info!(
                "{}: restored durable shadow progress (TXID: {}, generation: {}, offset: {}, wal_copy_offset: {})",
                name,
                progress.current_txid,
                progress.shadow_sync_generation,
                progress.shadow_sync_offset,
                progress.wal_copy_offset
            );
            current_txid = progress.current_txid;
            last_snapshot = progress.last_snapshot;
            db_checksum = progress.db_checksum;
            shadow_sync_generation = progress.shadow_sync_generation;
            shadow_sync_offset = progress.shadow_sync_offset;
            // B4 restart-window: resume the live-WAL read cursor from the
            // persisted offset, seeding the salt + running checksum chain so
            // the first post-restart read validates per-frame (and does not
            // re-append the whole live WAL from offset 0).
            restored_wal_copy_offset = progress.wal_copy_offset;
            shadow.restore_read_cursor(progress.wal_salt, progress.wal_checksum_chain);
        }

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
            let storage: Arc<dyn StorageBackend> =
                Arc::new(S3Storage::new((*client).clone(), bucket_name.clone()));
            let uploader = Arc::new(Uploader::new(
                name.clone(),
                Arc::clone(&cache),
                storage,
                prefix.clone(),
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
                last_snapshot,
                db_checksum,
                shadow,
                shadow_sync_generation,
                shadow_sync_offset,
                wal_copy_offset: restored_wal_copy_offset,
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

    // Initial copy of any existing WAL data to shadow. If the live WAL salt
    // changed while we were down, `copy_frames` bumps the shadow generation;
    // those databases need an eager snapshot instead of waiting for the periodic
    // timer (D3).
    let eager_snapshot_paths = initial_shadow_copy(&mut db_states).await?;

    // Take initial snapshots if on_startup is enabled, skipping any DB that is
    // already scheduled for an eager snapshot below.
    for (db_path, state) in db_states.iter_mut() {
        let sync_config = sync_configs.get(db_path).unwrap_or(&global_sync);
        if sync_config.on_startup && !eager_snapshot_paths.contains(db_path) {
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
                wal_salt: None,
                wal_checksum_chain: None,
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
                state.shadow_sync_generation = state.shadow.generation();
                state.shadow_sync_offset = state.shadow.segment_offset();
                state.wal_copy_offset = 0;
                save_shadow_progress(state)?;

                if let Some(trigger) = trigger_states.get_mut(db_path) {
                    trigger.frames_since_snapshot = 0;
                    trigger.first_change_time = None;
                }
            }
        }
    }

    // D3: eager snapshot for any database whose live WAL salt changed while we
    // were down. This runs even when on_startup is disabled, so we do not wait
    // for the periodic snapshot timer after a downtime checkpoint.
    for db_path in &eager_snapshot_paths {
        let state = db_states
            .get_mut(db_path)
            .expect("eager snapshot path must exist in db_states");
        let mut db_state = DbState {
            name: state.name.clone(),
            db_path: state.db_path.clone(),
            wal_path: state.wal_path.clone(),
            wal_offset: 0,
            wal_generation: state.shadow.generation(),
            current_txid: state.current_txid,
            last_snapshot: state.last_snapshot,
            db_checksum: state.db_checksum,
            wal_salt: None,
            wal_checksum_chain: None,
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
            tracing::error!(
                "{}: Eager snapshot after downtime checkpoint failed: {}",
                state.name,
                e
            );
            metrics_state.record_error(&state.name);
        } else {
            state.current_txid = db_state.current_txid;
            state.last_snapshot = db_state.last_snapshot;
            state.db_checksum = db_state.db_checksum;
            state.shadow_sync_generation = state.shadow.generation();
            state.shadow_sync_offset = state.shadow.segment_offset();
            state.wal_copy_offset = 0;
            save_shadow_progress(state)?;
            metrics_state.record_snapshot(&state.name);

            if let Some(trigger) = trigger_states.get_mut(db_path) {
                trigger.frames_since_snapshot = 0;
                trigger.first_change_time = None;
            }
        }
    }

    // Set up periodic timers
    let snapshot_interval = Duration::from_secs(global_sync.snapshot_interval);
    let mut snapshot_timer = tokio::time::interval(snapshot_interval);

    let wal_sync_interval = Duration::from_secs(global_sync.wal_sync_interval);
    let mut wal_sync_timer = tokio::time::interval(wal_sync_interval);
    wal_sync_timer.tick().await;

    let disabled_timer_duration = Duration::from_secs(86400 * 365);
    let compact_interval_duration = if global_sync.compact_interval > 0 {
        Duration::from_secs(global_sync.compact_interval)
    } else {
        disabled_timer_duration
    };
    let mut compact_timer = tokio::time::interval(compact_interval_duration);
    compact_timer.tick().await;

    // Shadow mode: checkpoint is manual via shadow.checkpoint()
    let checkpoint_interval_duration = if global_sync.checkpoint_interval > 0 {
        Duration::from_secs(global_sync.checkpoint_interval)
    } else {
        disabled_timer_duration
    };
    let mut checkpoint_timer = tokio::time::interval(checkpoint_interval_duration);
    checkpoint_timer.tick().await;

    let trigger_interval_duration = Duration::from_secs(global_sync.wal_sync_interval);
    let mut trigger_timer = tokio::time::interval(trigger_interval_duration);

    let validation_interval_duration = if global_sync.validation_interval > 0 {
        Duration::from_secs(global_sync.validation_interval)
    } else {
        disabled_timer_duration
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
                            webhook_sender
                                .notify_upload_failed(&state.name, &e.to_string(), 1)
                                .await;
                            return Err(e.context(format!("{}: shadow copy failed", state.name)));
                        }
                    }
                }

                let results = run_shadow_syncs(
                    &db_states,
                    &cache_states,
                    Some(DirectShadowSyncTarget {
                        client: Arc::clone(&client),
                        bucket_name: bucket_name.clone(),
                        prefix: prefix.clone(),
                    }),
                    &retry_policy,
                    Arc::clone(&webhook_sender),
                ).await;

                // Phase 3: Apply results sequentially
                for result in results {
                    match result {
                        Ok(output) => {
                            let frame_count = output.frame_count;

                            if let Some(state) = db_states.get_mut(&output.db_path) {
                                apply_shadow_sync_result_to_state(state, &output).await?;

                                if frame_count == 0 {
                                    continue;
                                }

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
                                    trigger.frames_since_snapshot += frame_count;
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
                                            wal_salt: None,
                                            wal_checksum_chain: None,
                                        };
                                        if let Err(e) = take_snapshot_with_retry(&client, &bucket_name, &prefix, &mut db_state, &retry_policy, &webhook_sender).await {
                                            tracing::error!("Failed to snapshot {}: {}", state.name, e);
                                            metrics_state.record_error(&state.name);
                                        } else {
                                            state.current_txid = db_state.current_txid;
                                            state.last_snapshot = db_state.last_snapshot;
                                            state.db_checksum = db_state.db_checksum;
                                            save_shadow_progress(state)?;
                                            metrics_state.record_snapshot(&state.name);
                                            trigger.frames_since_snapshot = 0;
                                            trigger.first_change_time = None;
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Shadow sync failed: {}", e);
                            return Err(e).context("shadow sync failed");
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
                            wal_salt: None,
                            wal_checksum_chain: None,
                        };

                        if let Err(e) = take_snapshot_with_retry(&client, &bucket_name, &prefix, &mut db_state, &retry_policy, &webhook_sender).await {
                            tracing::error!("Failed to snapshot {}: {}", state.name, e);
                            metrics_state.record_error(&state.name);
                        } else {
                            state.current_txid = db_state.current_txid;
                            state.last_snapshot = db_state.last_snapshot;
                            state.db_checksum = db_state.db_checksum;
                            save_shadow_progress(state)?;
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
                        wal_salt: None,
                        wal_checksum_chain: None,
                    };

                    if let Err(e) = take_snapshot_with_retry(&client, &bucket_name, &prefix, &mut db_state, &retry_policy, &webhook_sender).await {
                        tracing::error!("Failed to snapshot {}: {}", state.name, e);
                        metrics_state.record_error(&state.name);
                    } else {
                        state.current_txid = db_state.current_txid;
                        state.last_snapshot = db_state.last_snapshot;
                        state.db_checksum = db_state.db_checksum;
                        save_shadow_progress(state)?;
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

            // Checkpoint timer - copy, sync, verify durability, then checkpoint.
            _ = checkpoint_timer.tick(), if global_sync.checkpoint_interval > 0 => {
                for (db_path, state) in db_states.iter_mut() {
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

                        if let Err(e) = checkpoint_shadow_after_durable_sync(
                            state,
                            cache_states.get(db_path),
                            Some(DirectShadowSyncTarget {
                                client: Arc::clone(&client),
                                bucket_name: bucket_name.clone(),
                                prefix: prefix.clone(),
                            }),
                            &retry_policy,
                            Arc::clone(&webhook_sender),
                            CHECKPOINT_UPLOAD_DRAIN_TIMEOUT,
                        ).await {
                            tracing::error!("{}: Shadow checkpoint failed: {}", state.name, e);
                            webhook_sender
                                .notify_upload_failed(&state.name, &e.to_string(), 1)
                                .await;
                            return Err(e.context(format!(
                                "{}: shadow checkpoint failed",
                                state.name
                            )));
                        }

                        tracing::debug!("{}: Shadow checkpoint completed", state.name);
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

    copy_final_shadow_frames(&mut db_states).await?;
    let final_results = run_shadow_syncs(
        &db_states,
        &cache_states,
        Some(DirectShadowSyncTarget {
            client: Arc::clone(&client),
            bucket_name: bucket_name.clone(),
            prefix: prefix.clone(),
        }),
        &retry_policy,
        Arc::clone(&webhook_sender),
    )
    .await;
    apply_shadow_sync_results_strict(&mut db_states, final_results).await?;

    shutdown_shadow_uploaders(&cache_states, &db_states, uploader_handles).await?;

    tracing::info!("walrust shadow mode shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shadow::format_segment_name;
    use rusqlite::Connection;
    use std::io::{Read, Write};
    use tempfile::TempDir;

    fn create_real_wal_db() -> (TempDir, PathBuf, Connection) {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("shutdown-shadow.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO items (value) VALUES ('alpha'), ('beta'), ('gamma');
            ",
        )
        .unwrap();

        assert!(
            db_path.with_extension("db-wal").exists(),
            "test must exercise a live SQLite WAL"
        );

        (temp, db_path, conn)
    }

    fn write_shadow_segment(
        shadow_dir: &std::path::Path,
        generation: u64,
        page_size: usize,
        page_data: &[u8],
    ) {
        std::fs::create_dir_all(shadow_dir).unwrap();
        let path = shadow_dir.join(format_segment_name(generation, 0));
        let mut file = std::fs::File::create(path).unwrap();
        let mut header = [0u8; 24];
        header[0..4].copy_from_slice(&1u32.to_be_bytes());
        header[4..8].copy_from_slice(&1u32.to_be_bytes());
        file.write_all(&header).unwrap();
        file.write_all(&page_data[..page_size]).unwrap();
    }

    #[tokio::test]
    async fn test_shadow_shutdown_syncs_final_real_wal_frames_to_cache() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow = ShadowWal::new(&db_path).await.unwrap();
        let wal_path = db_path.with_extension("db-wal");

        let mut db_states = HashMap::new();
        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name: "shutdown_shadow".to_string(),
                db_path: db_path.clone(),
                wal_path,
                current_txid: 0,
                last_snapshot: None,
                db_checksum: None,
                shadow,
                shadow_sync_generation: 0,
                shadow_sync_offset: 0,
                wal_copy_offset: 0,
            },
        );

        copy_final_shadow_frames(&mut db_states).await.unwrap();
        assert!(
            db_states.get(&db_path).unwrap().wal_copy_offset > 0,
            "final shutdown copy must consume real WAL frames"
        );

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, mut upload_rx) = mpsc::channel(10);
        let mut cache_states = HashMap::new();
        cache_states.insert(db_path.clone(), (Arc::clone(&cache), upload_tx));

        let results = run_shadow_syncs(
            &db_states,
            &cache_states,
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
        )
        .await;
        apply_shadow_sync_results_strict(&mut db_states, results)
            .await
            .unwrap();

        let pending = cache.pending_uploads();
        assert_eq!(
            pending.len(),
            1,
            "final shutdown sync must queue an LTX upload"
        );

        let ltx = cache.read_ltx(pending[0]).unwrap();
        crate::ltx::verify_ltx(std::io::Cursor::new(ltx)).unwrap();

        let msg = upload_rx.try_recv().unwrap();
        assert!(
            matches!(msg, UploadMessage::Upload(txid) if txid == pending[0]),
            "cache sync must notify uploader for queued LTX"
        );
    }

    #[tokio::test]
    async fn test_shadow_sync_persists_restart_progress_after_durable_cache_write() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow = ShadowWal::new(&db_path).await.unwrap();
        let progress_path = shadow.shadow_dir().join("progress.json");
        let wal_path = db_path.with_extension("db-wal");

        let mut db_states = HashMap::new();
        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name: "restart_progress".to_string(),
                db_path: db_path.clone(),
                wal_path,
                current_txid: 0,
                last_snapshot: None,
                db_checksum: None,
                shadow,
                shadow_sync_generation: 0,
                shadow_sync_offset: 0,
                wal_copy_offset: 0,
            },
        );

        copy_final_shadow_frames(&mut db_states).await.unwrap();

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, _upload_rx) = mpsc::channel(10);
        let mut cache_states = HashMap::new();
        cache_states.insert(db_path.clone(), (Arc::clone(&cache), upload_tx));

        let results = run_shadow_syncs(
            &db_states,
            &cache_states,
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
        )
        .await;
        apply_shadow_sync_results_strict(&mut db_states, results)
            .await
            .unwrap();

        assert!(
            progress_path.exists(),
            "shadow sync must persist a restart progress record after durable cache write"
        );
        let state = db_states.get(&db_path).unwrap();
        let reloaded = load_shadow_progress(&state.shadow, &state.name)
            .unwrap()
            .expect("progress record must reload");
        assert_eq!(reloaded.current_txid, state.current_txid);
        assert_eq!(
            reloaded.shadow_sync_generation,
            state.shadow_sync_generation
        );
        assert_eq!(reloaded.shadow_sync_offset, state.shadow_sync_offset);
    }

    #[tokio::test]
    async fn test_shadow_sync_cursor_resets_offset_when_advancing_generation() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow_dir = ShadowWal::shadow_dir_for(&db_path);
        let page_size = 4096usize;
        let mut page = vec![0u8; page_size];
        std::fs::File::open(&db_path)
            .unwrap()
            .read_exact(&mut page)
            .unwrap();

        write_shadow_segment(&shadow_dir, 0, page_size, &page);
        write_shadow_segment(&shadow_dir, 1, page_size, &page);

        let shadow = ShadowWal::new(&db_path).await.unwrap();
        assert_eq!(
            shadow.generation(),
            1,
            "test setup must start with a newer live shadow generation"
        );
        let frame_size = 24 + page_size as u64;
        let wal_path = db_path.with_extension("db-wal");
        let mut db_states = HashMap::new();
        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name: "generation_cursor".to_string(),
                db_path: db_path.clone(),
                wal_path,
                current_txid: 0,
                last_snapshot: None,
                db_checksum: None,
                shadow,
                shadow_sync_generation: 0,
                shadow_sync_offset: frame_size,
                wal_copy_offset: 0,
            },
        );

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, _upload_rx) = mpsc::channel(10);
        let mut cache_states = HashMap::new();
        cache_states.insert(db_path.clone(), (Arc::clone(&cache), upload_tx));

        let drained_old_generation = run_shadow_syncs(
            &db_states,
            &cache_states,
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
        )
        .await;
        apply_shadow_sync_results_strict(&mut db_states, drained_old_generation)
            .await
            .unwrap();
        let state = db_states.get(&db_path).unwrap();
        assert_eq!(state.shadow_sync_generation, 1);
        assert_eq!(
            state.shadow_sync_offset, 0,
            "offset must reset when advancing to the new shadow generation"
        );
        assert!(
            cache.pending_uploads().is_empty(),
            "draining the old generation at EOF should not upload anything"
        );

        let synced_new_generation = run_shadow_syncs(
            &db_states,
            &cache_states,
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
        )
        .await;
        apply_shadow_sync_results_strict(&mut db_states, synced_new_generation)
            .await
            .unwrap();
        assert_eq!(
            cache.pending_uploads().len(),
            1,
            "new generation frames must be read from offset 0, not skipped by the prior generation offset"
        );
    }

    #[tokio::test]
    async fn test_shadow_checkpoint_copies_syncs_and_waits_for_cache_upload() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow = ShadowWal::new(&db_path).await.unwrap();
        let wal_path = db_path.with_extension("db-wal");
        let mut state = ShadowDbState {
            name: "checkpoint_shadow".to_string(),
            db_path: db_path.clone(),
            wal_path,
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, mut upload_rx) = mpsc::channel(10);
        let ack_cache = Arc::clone(&cache);
        let ack_handle = tokio::spawn(async move {
            match upload_rx.recv().await {
                Some(UploadMessage::Upload(txid)) => ack_cache.mark_uploaded(txid).unwrap(),
                other => panic!("expected upload notification, got {other:?}"),
            }
        });
        let cache_state = (Arc::clone(&cache), upload_tx);

        checkpoint_shadow_after_durable_sync(
            &mut state,
            Some(&cache_state),
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        ack_handle.await.unwrap();

        assert!(
            state.wal_copy_offset > 0,
            "checkpoint path must copy real active WAL frames before checkpointing"
        );
        assert!(
            state.shadow_sync_offset > 0,
            "checkpoint path must sync copied shadow frames before checkpointing"
        );
        assert!(
            cache.pending_uploads().is_empty(),
            "checkpoint must wait until cache uploads are confirmed durable"
        );
        assert!(
            cache.last_uploaded_txid() >= state.current_txid,
            "confirmed uploaded LTX must cover the checkpointed shadow state"
        );
    }

    #[tokio::test]
    async fn test_shadow_checkpoint_refuses_pending_cache_upload() {
        let (_temp, db_path, _conn) = create_real_wal_db();
        let shadow = ShadowWal::new(&db_path).await.unwrap();
        let wal_path = db_path.with_extension("db-wal");
        let mut state = ShadowDbState {
            name: "checkpoint_shadow_pending".to_string(),
            db_path: db_path.clone(),
            wal_path,
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
            shadow,
            shadow_sync_generation: 0,
            shadow_sync_offset: 0,
            wal_copy_offset: 0,
        };

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let (upload_tx, _upload_rx) = mpsc::channel(10);
        let cache_state = (Arc::clone(&cache), upload_tx);

        let err = checkpoint_shadow_after_durable_sync(
            &mut state,
            Some(&cache_state),
            None,
            &RetryPolicy::new(RetryConfig::default()),
            Arc::new(WebhookSender::new(vec![])),
            Duration::from_millis(100),
        )
        .await
        .expect_err("checkpoint must fail closed while cache uploads are pending")
        .to_string();

        assert!(
            err.contains("durable upload confirmation timed out"),
            "expected durability timeout, got {err}"
        );
        assert!(
            !cache.pending_uploads().is_empty(),
            "test must leave an unconfirmed cache upload pending"
        );
    }

    #[tokio::test]
    async fn test_initial_shadow_copy_detects_downtime_checkpoint() {
        // D3: if the live WAL salt changed while walrust was down, the initial
        // copy must flag the database for an eager snapshot.
        let (_temp, db_path, conn) = create_real_wal_db();

        // First "process": copy WAL frames and remember the salt/offset.
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        let (frames, offset) = shadow.copy_frames(0).await.unwrap();
        assert!(!frames.is_empty(), "pre-restart copy must read frames");
        let saved_salt = shadow.wal_read_salt();
        let saved_chain = shadow.wal_read_chain();
        drop(shadow);

        // External checkpoint while walrust is down resets the WAL and changes salt.
        let _: (i64, i64, i64) = conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        conn.execute("INSERT INTO items (value) VALUES ('post-ckpt')", [])
            .unwrap();

        // Second "process": restart with the persisted cursor.
        let restarted = ShadowWal::new(&db_path).await.unwrap();
        let mut shadow = restarted;
        shadow.restore_read_cursor(saved_salt, saved_chain);

        let mut db_states = HashMap::new();
        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name: "downtime-ckpt".to_string(),
                db_path: db_path.clone(),
                wal_path: db_path.with_extension("db-wal"),
                current_txid: 0,
                last_snapshot: None,
                db_checksum: None,
                shadow,
                shadow_sync_generation: 0,
                shadow_sync_offset: 0,
                wal_copy_offset: offset,
            },
        );

        let eager = initial_shadow_copy(&mut db_states).await.unwrap();
        assert!(
            eager.contains(&db_path),
            "initial copy must flag a downtime-checkpointed DB for eager snapshot"
        );
        assert!(
            db_states[&db_path].shadow.generation() > 0,
            "shadow generation must advance after salt mismatch"
        );
    }

    #[tokio::test]
    async fn test_initial_shadow_copy_no_eager_snapshot_without_downtime_checkpoint() {
        // D3 (negative): if the live WAL salt did NOT change while walrust was
        // down (no external checkpoint), the initial copy must NOT flag the
        // database for an eager snapshot. Eager snapshots fire ONLY on mismatch.
        let (_temp, db_path, conn) = create_real_wal_db();

        // First "process": copy WAL frames and remember the salt/offset.
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        let (frames, offset) = shadow.copy_frames(0).await.unwrap();
        assert!(!frames.is_empty(), "pre-restart copy must read frames");
        let saved_salt = shadow.wal_read_salt();
        let saved_chain = shadow.wal_read_chain();
        drop(shadow);

        // Normal writes while walrust is "down" — appended to the SAME WAL, no
        // checkpoint, so the salt is unchanged.
        conn.execute("INSERT INTO items (value) VALUES ('delta')", [])
            .unwrap();

        // Second "process": restart with the persisted cursor.
        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        shadow.restore_read_cursor(saved_salt, saved_chain);
        let generation_before = shadow.generation();

        let mut db_states = HashMap::new();
        db_states.insert(
            db_path.clone(),
            ShadowDbState {
                name: "no-downtime-ckpt".to_string(),
                db_path: db_path.clone(),
                wal_path: db_path.with_extension("db-wal"),
                current_txid: 0,
                last_snapshot: None,
                db_checksum: None,
                shadow,
                shadow_sync_generation: 0,
                shadow_sync_offset: 0,
                wal_copy_offset: offset,
            },
        );

        let eager = initial_shadow_copy(&mut db_states).await.unwrap();
        assert!(
            !eager.contains(&db_path),
            "no salt mismatch means no eager snapshot must be scheduled"
        );
        assert_eq!(
            db_states[&db_path].shadow.generation(),
            generation_before,
            "shadow generation must not advance without a downtime checkpoint"
        );
    }
}
