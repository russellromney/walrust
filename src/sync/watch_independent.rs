use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;

use crate::cache::LocalCache;
use crate::config::{
    parse_duration_string, CacheConfig, ResolvedDbConfig, SyncConfig, WebhookConfig,
};
use crate::dashboard::{self, MetricsState};
use crate::ltx;
use crate::retention::RetentionPolicy;
use crate::retry::{RetryConfig, RetryPolicy};
use crate::s3::{create_client, parse_bucket};
use crate::shadow::ShadowWal;
use crate::uploader::{spawn_uploader, UploadMessage, Uploader};
use crate::webhook::WebhookSender;
use hadb_storage::StorageBackend;
use hadb_storage_s3::S3Storage;

use super::manifest::discover_state_from_s3;
use super::types::{CacheState, DbState, DbTaskState, SyncInput, TriggerState};
use super::verify::validate_backup_integrity;
use super::wal_sync::{do_sync, sync_wal_concurrent_with_retry};

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
                return Err(anyhow!(
                    "Invalid cache retention '{}': {}",
                    cache_config.retention,
                    e
                ));
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
            db_checksum
                .map(|c| format!("{:#x}", c))
                .unwrap_or_else(|| "none".to_string())
        );

        // Initial sync of any existing WAL data (before starting event loop)
        // This ensures we don't miss frames that exist when walrust starts
        tracing::debug!("{}: Checking for existing WAL at {:?}", name, wal_path);
        let wal_exists = wal_path.exists();
        tracing::debug!("{}: WAL exists = {}", name, wal_exists);

        let (wal_offset, wal_generation, current_txid, db_checksum) = if wal_exists {
            tracing::debug!(
                "{}: Starting initial sync (offset={}, gen={}, txid={})",
                name,
                wal_offset,
                wal_generation,
                current_txid
            );
            let input = SyncInput {
                db_path: db_path.clone(),
                name: name.clone(),
                wal_path: wal_path.clone(),
                wal_offset,
                wal_generation,
                current_txid,
                db_checksum,
                wal_salt: None,
                wal_checksum_chain: None,
            };
            match sync_wal_concurrent_with_retry(
                Arc::clone(&client),
                bucket_name.clone(),
                prefix.clone(),
                input,
                retry_policy.clone(),
                Arc::clone(&webhook_sender),
            )
            .await
            {
                Ok(result) => {
                    tracing::debug!(
                        "{}: Initial sync returned: frame_count={}, new_offset={}, new_txid={}",
                        name,
                        result.frame_count,
                        result.new_wal_offset,
                        result.new_current_txid
                    );
                    if result.frame_count > 0 {
                        tracing::info!(
                            "{}: Initial sync captured {} frames",
                            name,
                            result.frame_count
                        );
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
                    tracing::warn!(
                        "{}: Initial sync failed (will retry on changes): {}",
                        name,
                        e
                    );
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
                wal_salt: None,
                wal_checksum_chain: None,
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
                std::fs::create_dir_all(&cache_dir).map_err(|e| {
                    anyhow!(
                        "Failed to create cache directory {}: {}",
                        cache_dir.display(),
                        e
                    )
                })?;
            }

            // Create LocalCache
            let cache = Arc::new(LocalCache::new(&cache_dir)?);
            tracing::debug!(
                "{}: LocalCache initialized at {}",
                name,
                cache_dir.display()
            );

            // Create ShadowWal for checkpoint-safe frame copying
            let shadow = ShadowWal::new(db_path)
                .await
                .map_err(|e| anyhow!("{}: Failed to create shadow WAL: {}", name, e))?;
            let shadow = Arc::new(tokio::sync::Mutex::new(shadow));
            tracing::debug!(
                "{}: ShadowWal initialized (checkpoint blocker active)",
                name
            );

            // Resume pending uploads count
            let pending_count = cache.pending_uploads().len();
            if pending_count > 0 {
                tracing::info!(
                    "{}: Found {} pending uploads to resume",
                    name,
                    pending_count
                );
            }

            // Create storage backend for the uploader.
            // AWS SDK Client is Clone (cheap Arc internally).
            let storage: Arc<dyn StorageBackend> =
                Arc::new(S3Storage::new((*client).clone(), bucket_name.clone()));

            // Create Uploader
            let s3_prefix = format!("{}/{}", prefix, name);
            let uploader = Arc::new(Uploader::new(
                name.clone(),
                Arc::clone(&cache),
                storage,
                s3_prefix,
                Arc::new(retry_policy.clone()),
                Arc::clone(&webhook_sender),
                cache_config.uploader_concurrency,
            ));

            let (upload_tx, uploader_handle) = spawn_uploader(uploader);

            Some(CacheState {
                cache,
                shadow,
                upload_tx,
                upload_handle: Some(uploader_handle),
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
            let result = run_db_task(
                task_state,
                client,
                bucket,
                pfx,
                policy,
                webhooks,
                metrics,
                shutdown_rx,
                cache_state,
            )
            .await;

            if let Err(e) = &result {
                tracing::error!("{}: Task failed: {}", name, e);
            }

            result
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

    let signal_name = shutdown_signal.await;
    tracing::info!("Received {}, initiating graceful shutdown...", signal_name);

    // Signal all tasks to shutdown
    let _ = shutdown_tx.send(());

    // Wait for all tasks to complete (with timeout)
    let shutdown_timeout = Duration::from_secs(10);
    match tokio::time::timeout(shutdown_timeout, async {
        let mut first_error = None;
        for handle in task_handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!("Database task shutdown failed: {}", e);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(e) => {
                    tracing::error!("Database task panicked during shutdown: {}", e);
                    if first_error.is_none() {
                        first_error = Some(anyhow!("database task panicked during shutdown: {e}"));
                    }
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    })
    .await
    {
        Ok(Ok(())) => tracing::info!("All tasks shut down gracefully"),
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(anyhow!(
                "shutdown timeout - some tasks may not have completed"
            ))
        }
    }

    tracing::info!("walrust shutdown complete");
    Ok(())
}

async fn run_db_task(
    mut state: DbTaskState,
    client: Arc<aws_sdk_s3::Client>,
    bucket: String,
    prefix: String,
    retry_policy: RetryPolicy,
    webhook_sender: Arc<WebhookSender>,
    metrics_state: Arc<MetricsState>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    mut cache_state: Option<CacheState>,
) -> Result<()> {
    let db_name = state.db_state.name.clone();
    let wal_path = state.db_state.wal_path.clone();
    let validation_interval = state.sync_config.validation_interval;

    // Poll interval: check WAL size and sync every N seconds
    let poll_interval = Duration::from_secs(state.sync_config.wal_sync_interval);
    let mut poll_timer = tokio::time::interval(poll_interval);
    poll_timer.tick().await; // Skip first immediate tick

    // Validation timer (disabled when interval is 0)
    let validation_duration = if validation_interval > 0 {
        Duration::from_secs(validation_interval)
    } else {
        Duration::from_secs(86400 * 365) // effectively never
    };
    let mut validation_timer = tokio::time::interval(validation_duration);
    validation_timer.tick().await; // Skip first immediate tick

    // Cache cleanup timer (every 5 minutes when cache is enabled)
    let cache_enabled = cache_state.is_some();
    let mut cleanup_timer = tokio::time::interval(Duration::from_secs(300));
    cleanup_timer.tick().await; // Skip first immediate tick

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
                do_sync(&mut state, &client, &bucket, &prefix, &retry_policy, &webhook_sender, &metrics_state, cache_state.as_ref())
                    .await
                    .with_context(|| format!("{}: final sync before shutdown failed", db_name))?;
                // Signal uploader to shutdown if cache is enabled
                if let Some(mut cache) = cache_state.take() {
                    cache.upload_tx
                        .send(UploadMessage::Shutdown)
                        .await
                        .map_err(|e| anyhow!("{}: failed to send uploader shutdown: {}", db_name, e))?;

                    if let Some(handle) = cache.upload_handle.take() {
                        let stats = tokio::time::timeout(Duration::from_secs(10), handle)
                            .await
                            .map_err(|_| anyhow!("{}: uploader drain timed out", db_name))?
                            .map_err(|e| anyhow!("{}: uploader task panicked: {}", db_name, e))?
                            .with_context(|| format!("{}: uploader drain failed", db_name))?;
                        tracing::debug!("{}: Uploader drained successfully: {:?}", db_name, stats);
                    }
                }
                break;
            }

            // Poll timer - check WAL size and sync if changed
            _ = poll_timer.tick() => {
                match do_sync(&mut state, &client, &bucket, &prefix, &retry_policy, &webhook_sender, &metrics_state, cache_state.as_ref()).await {
                    Ok(frame_count) => {
                        if frame_count > 0 {
                            tracing::debug!("{}: Synced {} frames", db_name, frame_count);
                        }
                    }
                    Err(e) => {
                        let error_msg = e.to_string();
                        tracing::error!("{}: Sync failed: {}", db_name, error_msg);
                        webhook_sender
                            .notify_upload_failed(&db_name, &error_msg, 1)
                            .await;
                        return Err(anyhow!("{}: sync failed: {}", db_name, error_msg));
                    }
                }
            }

            // Cache cleanup timer
            _ = cleanup_timer.tick(), if cache_enabled => {
                if let Some(ref cache) = cache_state {
                    match cache.cache.cleanup(cache.retention_duration, cache.max_cache_size) {
                        Ok(stats) => {
                            if stats.deleted_count > 0 {
                                tracing::info!(
                                    "{}: Cache cleanup: deleted {} files ({:.2} MB), {} remaining",
                                    db_name,
                                    stats.deleted_count,
                                    stats.deleted_bytes as f64 / (1024.0 * 1024.0),
                                    stats.remaining_bytes as f64 / (1024.0 * 1024.0)
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!("{}: Cache cleanup failed: {}", db_name, e);
                        }
                    }
                }
            }

            // Periodic validation timer
            _ = validation_timer.tick(), if validation_interval > 0 => {
                tracing::debug!("{}: Running periodic backup validation", db_name);

                match validate_backup_integrity(&client, &bucket, &prefix, &db_name).await {
                    Ok(result) => {
                        if result.is_valid {
                            tracing::info!(
                                "{}: Validation passed ({} files, {:.2} MB)",
                                db_name,
                                result.verified_count,
                                result.verified_size_bytes as f64 / (1024.0 * 1024.0)
                            );
                            metrics_state.record_validation_success(&db_name);
                        } else {
                            tracing::error!(
                                "{}: Validation failed with {} issues",
                                db_name,
                                result.issues.len()
                            );
                            for issue in &result.issues {
                                tracing::error!("  {}: {}", issue.filename, issue.issue);
                            }
                            metrics_state.record_validation_failure(&db_name);
                        }
                    }
                    Err(e) => {
                        tracing::error!("{}: Validation error: {}", db_name, e);
                        metrics_state.record_validation_failure(&db_name);
                    }
                }
            }
        }
    }

    tracing::debug!("{}: Task exiting", db_name);
    Ok(())
}
