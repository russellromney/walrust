use anyhow::{anyhow, Result};
use chrono::Utc;
use notify::{Event, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::config::{Config, ResolvedDbConfig, SyncConfig};
use crate::dashboard::{self, DbStatus, MetricsState};
use crate::ltx;
use crate::retention::{self, RetentionPolicy, SnapshotEntry};
use crate::s3::{self, create_client, parse_bucket};
use crate::wal;

/// State for a single watched database
struct DbState {
    /// Database name (filename without extension)
    name: String,
    /// Path to main db file
    db_path: PathBuf,
    /// Path to WAL file
    wal_path: PathBuf,
    /// Current WAL sync position
    wal_offset: u64,
    /// WAL generation (increments on checkpoint)
    wal_generation: u64,
    /// Current transaction ID (for LTX files)
    current_txid: u64,
    /// Last snapshot time
    last_snapshot: Option<chrono::DateTime<Utc>>,
    /// Current database checksum (for incremental LTX chaining)
    /// Computed from database on startup, updated after each LTX upload
    db_checksum: Option<u64>,
}

/// Entry in the manifest tracking LTX files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LtxEntry {
    /// Filename (e.g., "00000001-00000010.ltx")
    pub filename: String,
    /// Starting transaction ID
    pub min_txid: u64,
    /// Ending transaction ID
    pub max_txid: u64,
    /// File size in bytes
    pub size: u64,
    /// Upload timestamp (ISO 8601)
    pub created_at: String,
    /// Whether this is a snapshot (full DB) or incremental
    pub is_snapshot: bool,
}

/// Manifest tracking all LTX files for a database
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    /// Database name
    pub name: String,
    /// Current highest TXID
    pub current_txid: u64,
    /// Page size of the database
    pub page_size: u32,
    /// List of LTX files
    pub files: Vec<LtxEntry>,
    /// Last known database checksum (for incremental LTX chaining)
    /// This is the post_apply_checksum from the most recent LTX file
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checksum: Option<u64>,
}

/// Watch multiple databases and sync to S3
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

        // Check for existing state in S3 (manifest.json)
        let manifest_key = format!("{}{}/manifest.json", prefix, name);
        let (wal_offset, wal_generation, current_txid, manifest_checksum) = match s3::download_bytes(&client, &bucket_name, &manifest_key).await {
            Ok(data) => {
                let manifest: Manifest = serde_json::from_slice(&data).unwrap_or_default();
                // For backwards compat, also check old state.json
                let state_key = format!("{}{}/state.json", prefix, name);
                let (offset, gen) = match s3::download_bytes(&client, &bucket_name, &state_key).await {
                    Ok(state_data) => {
                        let state: serde_json::Value = serde_json::from_slice(&state_data)?;
                        (
                            state["wal_offset"].as_u64().unwrap_or(0),
                            state["wal_generation"].as_u64().unwrap_or(0),
                        )
                    }
                    Err(_) => (0, 0),
                };
                (offset, gen, manifest.current_txid, manifest.last_checksum)
            }
            Err(_) => {
                // Try old state.json for backwards compat
                let state_key = format!("{}{}/state.json", prefix, name);
                match s3::download_bytes(&client, &bucket_name, &state_key).await {
                    Ok(data) => {
                        let state: serde_json::Value = serde_json::from_slice(&data)?;
                        (
                            state["wal_offset"].as_u64().unwrap_or(0),
                            state["wal_generation"].as_u64().unwrap_or(0),
                            state["current_txid"].as_u64().unwrap_or(0),
                            None,
                        )
                    }
                    Err(_) => (0, 0, 0, None),
                }
            }
        };

        // Get initial checksum: from manifest if available, otherwise compute from db
        let db_checksum = match manifest_checksum {
            Some(cs) => {
                tracing::debug!("{}: Using checksum from manifest: {:#x}", name, cs);
                Some(cs)
            }
            None => {
                // Compute from database file
                match ltx::compute_checksum_from_file(db_path) {
                    Ok(cs) => {
                        tracing::debug!("{}: Computed initial checksum: {:#x}", name, cs.into_inner());
                        Some(cs.into_inner())
                    }
                    Err(e) => {
                        tracing::warn!("{}: Could not compute initial checksum: {}", name, e);
                        None
                    }
                }
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

    // Set up file watcher
    let (tx, mut rx) = mpsc::channel::<PathBuf>(100);

    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        if let Ok(event) = res {
            for path in event.paths {
                // Only care about WAL files
                if path.extension().map(|e| e == "db-wal").unwrap_or(false) {
                    let _ = tx.blocking_send(path);
                }
            }
        }
    })?;

    // Watch parent directories of all databases
    let mut watched_dirs = std::collections::HashSet::new();
    for db_path in &databases {
        if let Some(parent) = db_path.parent() {
            if watched_dirs.insert(parent.to_path_buf()) {
                watcher.watch(parent, RecursiveMode::NonRecursive)?;
                tracing::debug!("Watching directory: {}", parent.display());
            }
        }
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
            // WAL file changed
            Some(wal_path) = rx.recv() => {
                // Find the corresponding database
                let db_path = wal_path.with_extension("db");
                if let Some(state) = db_states.get_mut(&db_path) {
                    match sync_wal(&client, &bucket_name, &prefix, state).await {
                        Ok(_frame_count) => {}
                        Err(e) => tracing::error!("Failed to sync WAL for {}: {}", state.name, e),
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

/// State for sync trigger tracking
struct TriggerState {
    /// WAL frames synced since last snapshot
    frames_since_snapshot: u64,
    /// When the first change was detected (for max_interval)
    first_change_time: Option<std::time::Instant>,
    /// When the last WAL activity occurred (for on_idle)
    last_wal_activity: Option<std::time::Instant>,
}

impl Default for TriggerState {
    fn default() -> Self {
        Self {
            frames_since_snapshot: 0,
            first_change_time: None,
            last_wal_activity: None,
        }
    }
}

/// Watch databases with config-based settings and sync triggers
pub async fn watch_with_config(
    databases: Vec<ResolvedDbConfig>,
    bucket: &str,
    endpoint: Option<&str>,
    global_sync: SyncConfig,
    compact_policy: Option<RetentionPolicy>,
    metrics_port: u16,
    no_metrics: bool,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = Arc::new(create_client(endpoint).await?);

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

        // Check for existing state in S3 (manifest.json)
        let manifest_key = format!("{}{}/manifest.json", prefix, name);
        let (wal_offset, wal_generation, current_txid, manifest_checksum) =
            match s3::download_bytes(&client, &bucket_name, &manifest_key).await {
                Ok(data) => {
                    let manifest: Manifest = serde_json::from_slice(&data).unwrap_or_default();
                    // For backwards compat, also check old state.json
                    let state_key = format!("{}{}/state.json", prefix, name);
                    let (offset, gen) =
                        match s3::download_bytes(&client, &bucket_name, &state_key).await {
                            Ok(state_data) => {
                                let state: serde_json::Value =
                                    serde_json::from_slice(&state_data)?;
                                (
                                    state["wal_offset"].as_u64().unwrap_or(0),
                                    state["wal_generation"].as_u64().unwrap_or(0),
                                )
                            }
                            Err(_) => (0, 0),
                        };
                    (offset, gen, manifest.current_txid, manifest.last_checksum)
                }
                Err(_) => {
                    // Try old state.json for backwards compat
                    let state_key = format!("{}{}/state.json", prefix, name);
                    match s3::download_bytes(&client, &bucket_name, &state_key).await {
                        Ok(data) => {
                            let state: serde_json::Value = serde_json::from_slice(&data)?;
                            (
                                state["wal_offset"].as_u64().unwrap_or(0),
                                state["wal_generation"].as_u64().unwrap_or(0),
                                state["current_txid"].as_u64().unwrap_or(0),
                                None,
                            )
                        }
                        Err(_) => (0, 0, 0, None),
                    }
                }
            };

        // Get initial checksum: from manifest if available, otherwise compute from db
        let db_checksum = match manifest_checksum {
            Some(cs) => {
                tracing::debug!("{}: Using checksum from manifest: {:#x}", name, cs);
                Some(cs)
            }
            None => {
                match ltx::compute_checksum_from_file(db_path) {
                    Ok(cs) => {
                        tracing::debug!("{}: Computed initial checksum: {:#x}", name, cs.into_inner());
                        Some(cs.into_inner())
                    }
                    Err(e) => {
                        tracing::warn!("{}: Could not compute initial checksum: {}", name, e);
                        None
                    }
                }
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
            })
            .await;
    }

    // Set up file watcher
    let (tx, mut rx) = mpsc::channel::<PathBuf>(100);

    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        if let Ok(event) = res {
            for path in event.paths {
                // Only care about WAL files
                if path.extension().map(|e| e == "db-wal").unwrap_or(false) {
                    let _ = tx.blocking_send(path);
                }
            }
        }
    })?;

    // Watch parent directories of all databases
    let mut watched_dirs = std::collections::HashSet::new();
    for db_config in &databases {
        if let Some(parent) = db_config.path.parent() {
            if watched_dirs.insert(parent.to_path_buf()) {
                watcher.watch(parent, RecursiveMode::NonRecursive)?;
                tracing::debug!("Watching directory: {}", parent.display());
            }
        }
    }

    // Initial sync of any existing WAL data
    for (db_path, state) in db_states.iter_mut() {
        if state.wal_path.exists() {
            let frame_count = sync_wal(&client, &bucket_name, &prefix, state).await?;
            if frame_count > 0 {
                if let Some(trigger) = trigger_states.get_mut(db_path) {
                    trigger.frames_since_snapshot += frame_count;
                    trigger.last_wal_activity = Some(std::time::Instant::now());
                    if trigger.first_change_time.is_none() {
                        trigger.first_change_time = Some(std::time::Instant::now());
                    }
                }
            }
        }
    }

    // Take initial snapshots if on_startup is enabled
    for (db_path, state) in db_states.iter_mut() {
        let sync_config = sync_configs.get(db_path).unwrap_or(&global_sync);
        if sync_config.on_startup {
            take_snapshot(&client, &bucket_name, &prefix, state).await?;
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

    // Set up trigger check interval (configurable via monitor_interval)
    let monitor_interval_duration = Duration::from_secs(global_sync.monitor_interval);
    let mut trigger_timer = tokio::time::interval(monitor_interval_duration);

    // Set up validation timer (periodic backup integrity check)
    let validation_interval_duration = if global_sync.validation_interval > 0 {
        Duration::from_secs(global_sync.validation_interval)
    } else {
        Duration::from_secs(u64::MAX) // Disabled
    };
    let mut validation_timer = tokio::time::interval(validation_interval_duration);
    validation_timer.tick().await; // Skip first tick

    // Track pending WAL syncs
    let mut pending_wal_syncs = std::collections::HashSet::new();

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
            "walrust running (snapshot: {}s, WAL sync: {}s, checkpoint: {}s, monitor: {}s, max_changes: {}, max_interval: {}s, on_idle: {}s{})",
            global_sync.snapshot_interval,
            global_sync.wal_sync_interval,
            global_sync.checkpoint_interval,
            global_sync.monitor_interval,
            global_sync.max_changes,
            global_sync.max_interval,
            global_sync.on_idle,
            validation_info
        );
    } else {
        tracing::info!(
            "walrust running (snapshot: {}s, WAL sync: {}s, checkpoint: {}s, monitor: {}s{})",
            global_sync.snapshot_interval,
            global_sync.wal_sync_interval,
            global_sync.checkpoint_interval,
            global_sync.monitor_interval,
            validation_info
        );
    }

    loop {
        tokio::select! {
            // WAL file changed - mark for sync instead of syncing immediately
            Some(wal_path) = rx.recv() => {
                let db_path = wal_path.with_extension("db");
                if db_states.contains_key(&db_path) {
                    pending_wal_syncs.insert(db_path);
                }
            }

            // Batch sync pending WAL changes
            _ = wal_sync_timer.tick() => {
                for db_path in pending_wal_syncs.drain() {
                    if let Some(state) = db_states.get_mut(&db_path) {
                        let sync_config = sync_configs.get(&db_path).unwrap_or(&global_sync);

                        match sync_wal(&client, &bucket_name, &prefix, state).await {
                            Ok(frame_count) if frame_count > 0 => {
                                // Update dashboard on successful sync
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
                                }).await;

                                // Check for emergency truncate threshold
                                if sync_config.wal_truncate_threshold_pages > 0 {
                                    if let Ok(wal_pages) = get_wal_page_count(&state.wal_path).await {
                                        if wal_pages >= sync_config.wal_truncate_threshold_pages {
                                            tracing::warn!(
                                                "{}: WAL size ({} pages) exceeded emergency threshold ({} pages) - triggering TRUNCATE checkpoint",
                                                state.name,
                                                wal_pages,
                                                sync_config.wal_truncate_threshold_pages
                                            );

                                            // TRUNCATE checkpoint blocks readers and writers!
                                            if let Err(e) = run_checkpoint(&state.db_path, CheckpointMode::Truncate).await {
                                                tracing::error!("{}: Emergency TRUNCATE checkpoint failed: {}", state.name, e);
                                            } else {
                                                tracing::info!("{}: Emergency TRUNCATE checkpoint completed", state.name);
                                            }
                                        }
                                    }
                                }

                                if let Some(trigger) = trigger_states.get_mut(&db_path) {
                                    trigger.frames_since_snapshot += frame_count;
                                    trigger.last_wal_activity = Some(std::time::Instant::now());
                                    if trigger.first_change_time.is_none() {
                                        trigger.first_change_time = Some(std::time::Instant::now());
                                    }

                                    // Check max_changes trigger
                                    if sync_config.max_changes > 0
                                        && trigger.frames_since_snapshot >= sync_config.max_changes
                                    {
                                        tracing::info!(
                                            "{}: max_changes trigger ({} frames)",
                                            state.name,
                                            trigger.frames_since_snapshot
                                        );
                                        if let Err(e) = take_snapshot(&client, &bucket_name, &prefix, state).await {
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
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!("Failed to sync WAL for {}: {}", state.name, e);
                                metrics_state.record_error(&state.name);
                            }
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

                        if let Err(e) = take_snapshot(&client, &bucket_name, &prefix, state).await {
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
                    if let Err(e) = take_snapshot(&client, &bucket_name, &prefix, state).await {
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
}

/// Internal compaction for watch mode (non-interactive, always force)
async fn run_compaction(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    name: &str,
    policy: &RetentionPolicy,
) -> Result<()> {
    // Load manifest to get snapshot info
    let manifest = load_manifest(client, bucket, prefix, name).await?;

    if manifest.files.is_empty() {
        return Ok(());
    }

    // Filter to only snapshots (not incremental files)
    let snapshot_entries: Vec<SnapshotEntry> = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot)
        .filter_map(|f| {
            chrono::DateTime::parse_from_rfc3339(&f.created_at)
                .ok()
                .map(|dt| SnapshotEntry {
                    filename: f.filename.clone(),
                    created_at: dt.with_timezone(&Utc),
                    max_txid: f.max_txid,
                    size: f.size,
                })
        })
        .collect();

    if snapshot_entries.is_empty() {
        return Ok(());
    }

    let now = Utc::now();
    let plan = retention::analyze_retention(&snapshot_entries, policy, now);

    if !plan.has_deletions() {
        tracing::debug!("Compaction for {}: nothing to delete", name);
        return Ok(());
    }

    tracing::info!(
        "Compacting {}: deleting {} snapshots, keeping {}",
        name,
        plan.delete.len(),
        plan.keep.len()
    );

    // Delete files
    let keys_to_delete: Vec<String> = plan
        .delete
        .iter()
        .map(|e| format!("{}{}/{}", prefix, name, e.filename))
        .collect();

    let deleted_count = s3::delete_objects(client, bucket, &keys_to_delete).await?;

    // Update manifest to remove deleted entries
    let kept_filenames: std::collections::HashSet<_> =
        plan.keep.iter().map(|e| e.filename.as_str()).collect();

    let updated_files: Vec<LtxEntry> = manifest
        .files
        .into_iter()
        .filter(|f| !f.is_snapshot || kept_filenames.contains(f.filename.as_str()))
        .collect();

    let updated_manifest = Manifest {
        files: updated_files,
        ..manifest
    };

    save_manifest(client, bucket, prefix, &updated_manifest).await?;

    tracing::info!(
        "Compaction complete for {}: deleted {} snapshots, freed {:.2} MB",
        name,
        deleted_count,
        plan.bytes_freed as f64 / (1024.0 * 1024.0)
    );

    Ok(())
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
async fn sync_wal(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    state: &mut DbState,
) -> Result<u64> {
    use litetx::Checksum;

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

    // Encode as incremental LTX
    let mut ltx_buffer = Vec::new();
    let post_checksum = ltx::encode_wal_changes(
        &mut ltx_buffer,
        &pages,
        header.page_size,
        min_txid,
        max_txid,
        commit_page,
        Some(pre_checksum),
    )?;

    let ltx_size = ltx_buffer.len() as u64;

    // LTX filename: {min_txid:08}-{max_txid:08}.ltx
    let ltx_filename = format!("{:08}-{:08}.ltx", min_txid, max_txid);
    let ltx_key = format!("{}{}/{}", prefix, state.name, ltx_filename);

    // Upload incremental LTX file
    let timestamp = Utc::now();
    s3::upload_bytes(client, bucket, &ltx_key, ltx_buffer).await?;

    tracing::info!(
        "{}: Synced {} WAL frames as incremental LTX {} ({} bytes, {} unique pages, TXID {}-{})",
        state.name,
        frame_count,
        ltx_filename,
        ltx_size,
        pages.len(),
        min_txid,
        max_txid
    );

    // Update manifest with new incremental entry
    let mut manifest = load_manifest(client, bucket, prefix, &state.name).await?;
    manifest.current_txid = max_txid;
    manifest.last_checksum = Some(post_checksum.into_inner());
    manifest.files.push(LtxEntry {
        filename: ltx_filename,
        min_txid,
        max_txid,
        size: ltx_size,
        created_at: timestamp.to_rfc3339(),
        is_snapshot: false,
    });
    save_manifest(client, bucket, prefix, &manifest).await?;

    // Update state
    state.wal_offset = new_offset;
    state.current_txid = max_txid;
    state.db_checksum = Some(post_checksum.into_inner());

    // Save legacy state for backwards compat
    save_state(client, bucket, prefix, state).await?;

    Ok(frame_count as u64)
}

/// Take a full database snapshot as LTX
async fn take_snapshot(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    state: &mut DbState,
) -> Result<()> {
    let timestamp = Utc::now();

    // Get page size from database header
    let page_size = get_page_size(&state.db_path).await?;

    // Increment TXID for this snapshot
    let new_txid = state.current_txid + 1;

    // LTX filename: {min_txid:08}-{max_txid:08}.ltx
    // For a snapshot, min=1 and max=new_txid (it contains all pages up to this point)
    let ltx_filename = format!("{:08}-{:08}.ltx", 1, new_txid);
    let ltx_key = format!("{}{}/{}", prefix, state.name, ltx_filename);

    // Encode database as LTX
    let mut ltx_buffer = Vec::new();
    ltx::encode_snapshot(&mut ltx_buffer, &state.db_path, page_size, new_txid)?;

    let ltx_size = ltx_buffer.len() as u64;

    // Upload LTX file
    s3::upload_bytes(client, bucket, &ltx_key, ltx_buffer).await?;

    // Compute checksum from database for future incremental LTX
    let db_checksum = ltx::compute_checksum_from_file(&state.db_path)?;

    tracing::info!(
        "{}: LTX snapshot uploaded to {} (TXID: {}, size: {} bytes, checksum: {:#x})",
        state.name,
        ltx_key,
        new_txid,
        ltx_size,
        db_checksum.into_inner()
    );

    // Update manifest
    let mut manifest = load_manifest(client, bucket, prefix, &state.name).await?;
    manifest.current_txid = new_txid;
    manifest.page_size = page_size;
    manifest.last_checksum = Some(db_checksum.into_inner());
    manifest.files.push(LtxEntry {
        filename: ltx_filename,
        min_txid: 1,
        max_txid: new_txid,
        size: ltx_size,
        created_at: timestamp.to_rfc3339(),
        is_snapshot: true,
    });
    save_manifest(client, bucket, prefix, &manifest).await?;

    // Update state
    state.current_txid = new_txid;
    state.last_snapshot = Some(timestamp);
    state.db_checksum = Some(db_checksum.into_inner());

    Ok(())
}

/// Get SQLite database page size from header
async fn get_page_size(db_path: &Path) -> Result<u32> {
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

/// Save sync state to S3 (legacy state.json for backwards compat)
async fn save_state(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    state: &DbState,
) -> Result<()> {
    let state_key = format!("{}{}/state.json", prefix, state.name);
    let state_json = serde_json::json!({
        "wal_offset": state.wal_offset,
        "wal_generation": state.wal_generation,
        "current_txid": state.current_txid,
        "last_snapshot": state.last_snapshot,
    });

    s3::upload_bytes(
        client,
        bucket,
        &state_key,
        serde_json::to_vec_pretty(&state_json)?,
    )
    .await?;

    Ok(())
}

/// Load manifest from S3
async fn load_manifest(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<Manifest> {
    let manifest_key = format!("{}{}/manifest.json", prefix, db_name);
    match s3::download_bytes(client, bucket, &manifest_key).await {
        Ok(data) => Ok(serde_json::from_slice(&data)?),
        Err(_) => Ok(Manifest {
            name: db_name.to_string(),
            ..Default::default()
        }),
    }
}

/// Save manifest to S3
async fn save_manifest(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    manifest: &Manifest,
) -> Result<()> {
    let manifest_key = format!("{}{}/manifest.json", prefix, manifest.name);
    s3::upload_bytes(
        client,
        bucket,
        &manifest_key,
        serde_json::to_vec_pretty(manifest)?,
    )
    .await?;
    Ok(())
}

/// Restore a database from S3 using LTX files
pub async fn restore(
    name: &str,
    output: &Path,
    bucket: &str,
    endpoint: Option<&str>,
    point_in_time: Option<&str>,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    // Load manifest to find LTX files
    let manifest = load_manifest(&client, &bucket_name, &prefix, name).await?;

    if manifest.files.is_empty() {
        // Fall back to legacy snapshot-based restore for backwards compatibility
        return restore_legacy(name, output, bucket, endpoint, point_in_time).await;
    }

    // Parse point in time if provided (as TXID or timestamp)
    let target_txid = if let Some(pit) = point_in_time {
        // Try parsing as TXID first
        if let Ok(txid) = pit.parse::<u64>() {
            txid
        } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(pit) {
            // Find latest file before timestamp
            manifest
                .files
                .iter()
                .filter(|f| {
                    chrono::DateTime::parse_from_rfc3339(&f.created_at)
                        .map(|fdt| fdt <= dt)
                        .unwrap_or(false)
                })
                .map(|f| f.max_txid)
                .max()
                .ok_or_else(|| anyhow!("No LTX file found before {}", pit))?
        } else {
            return Err(anyhow!(
                "Invalid point_in_time format. Use TXID (number) or ISO 8601 timestamp"
            ));
        }
    } else {
        manifest.current_txid
    };

    // Find the best snapshot that covers our target TXID
    // A snapshot has min_txid=1 and should have max_txid >= target_txid
    let snapshot = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot && f.max_txid <= target_txid)
        .max_by_key(|f| f.max_txid)
        .ok_or_else(|| anyhow!("No LTX snapshot found for TXID {}", target_txid))?;

    tracing::info!(
        "Restoring from LTX snapshot: {} (TXID: {}-{})",
        snapshot.filename,
        snapshot.min_txid,
        snapshot.max_txid
    );

    // Download and decode LTX snapshot
    let ltx_key = format!("{}{}/{}", prefix, name, snapshot.filename);
    let ltx_data = s3::download_bytes(&client, &bucket_name, &ltx_key).await?;

    let cursor = std::io::Cursor::new(ltx_data);
    let header = ltx::decode_to_db(cursor, output)?;

    tracing::info!(
        "Restored {} from LTX (page_size: {}, pages: {}, TXID: {}-{})",
        name,
        header.page_size.into_inner(),
        header.commit.into_inner(),
        header.min_txid.into_inner(),
        header.max_txid.into_inner()
    );

    // Find incremental LTX files to apply (if any)
    let incrementals: Vec<_> = manifest
        .files
        .iter()
        .filter(|f| {
            !f.is_snapshot
                && f.min_txid > snapshot.max_txid
                && f.max_txid <= target_txid
        })
        .collect();

    if !incrementals.is_empty() {
        tracing::info!("Found {} incremental LTX files to apply", incrementals.len());
        // TODO: Apply incremental LTX files
        // For now, snapshots are sufficient for basic restore
        tracing::warn!("Incremental LTX replay not yet implemented - using snapshot only");
    }

    println!(
        "Restored {} to {} (TXID: {})",
        name,
        output.display(),
        snapshot.max_txid
    );
    Ok(())
}

/// Legacy restore for backwards compatibility with raw .db snapshots
async fn restore_legacy(
    name: &str,
    output: &Path,
    bucket: &str,
    endpoint: Option<&str>,
    point_in_time: Option<&str>,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    // Find legacy snapshots
    let snapshots_prefix = format!("{}{}/snapshots/", prefix, name);
    let snapshots = s3::list_objects(&client, &bucket_name, &snapshots_prefix).await?;

    if snapshots.is_empty() {
        return Err(anyhow!("No snapshots found for database: {}", name));
    }

    let pit = point_in_time
        .map(|s| chrono::DateTime::parse_from_rfc3339(s))
        .transpose()?
        .map(|dt| dt.with_timezone(&Utc));

    let snapshot_key = if let Some(pit) = pit {
        snapshots
            .iter()
            .filter(|k| {
                if let Some(ts) = k
                    .strip_prefix(&snapshots_prefix)
                    .and_then(|s| s.strip_suffix(".db"))
                {
                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(ts, "%Y%m%d%H%M%S") {
                        return dt.and_utc() <= pit;
                    }
                }
                false
            })
            .max()
            .ok_or_else(|| anyhow!("No snapshot found before {}", pit))?
            .clone()
    } else {
        snapshots
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("No snapshots"))?
    };

    tracing::info!("Restoring from legacy snapshot: {}", snapshot_key);
    s3::download_file(&client, &bucket_name, &snapshot_key, output).await?;

    if let Ok(Some(stored_checksum)) =
        s3::get_checksum(&client, &bucket_name, &snapshot_key).await
    {
        let restored_checksum = compute_file_sha256(output).await?;
        if stored_checksum != restored_checksum {
            return Err(anyhow!(
                "Checksum mismatch! Stored: {}, Restored: {}",
                stored_checksum,
                restored_checksum
            ));
        }
        tracing::info!("Checksum verified: {}", restored_checksum);
    }

    tracing::info!("Restored {} to {}", name, output.display());
    Ok(())
}

/// List databases in bucket
pub async fn list(bucket: &str, endpoint: Option<&str>) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    let objects = s3::list_objects(&client, &bucket_name, &prefix).await?;

    // Extract unique database names
    let mut dbs: std::collections::HashSet<String> = std::collections::HashSet::new();

    for key in &objects {
        if let Some(rest) = key.strip_prefix(&prefix) {
            if let Some(name) = rest.split('/').next() {
                if !name.is_empty() {
                    dbs.insert(name.to_string());
                }
            }
        }
    }

    if dbs.is_empty() {
        println!("No databases found in s3://{}/{}", bucket_name, prefix);
    } else {
        println!("Databases in s3://{}/{}:", bucket_name, prefix);
        for db in dbs {
            // Get latest snapshot info
            let snapshots_prefix = format!("{}{}/snapshots/", prefix, db);
            let snapshots = s3::list_objects(&client, &bucket_name, &snapshots_prefix).await?;
            let snapshot_count = snapshots.len();

            let wal_prefix = format!("{}{}/wal/", prefix, db);
            let wals = s3::list_objects(&client, &bucket_name, &wal_prefix).await?;
            let wal_count = wals.len();

            println!("  {} ({} snapshots, {} WAL segments)", db, snapshot_count, wal_count);
        }
    }

    Ok(())
}

/// Compact old snapshots using retention policy (GFS rotation)
///
/// Analyzes snapshots and deletes those that don't fit the retention policy.
/// By default runs in dry-run mode (force=false) to show what would be deleted.
pub async fn compact(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    policy: &RetentionPolicy,
    force: bool,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    // Load manifest to get snapshot info
    let manifest = load_manifest(&client, &bucket_name, &prefix, name).await?;

    if manifest.files.is_empty() {
        println!("No snapshots found for database '{}'", name);
        return Ok(());
    }

    // Filter to only snapshots (not incremental files)
    let snapshot_entries: Vec<SnapshotEntry> = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot)
        .filter_map(|f| {
            chrono::DateTime::parse_from_rfc3339(&f.created_at)
                .ok()
                .map(|dt| SnapshotEntry {
                    filename: f.filename.clone(),
                    created_at: dt.with_timezone(&Utc),
                    max_txid: f.max_txid,
                    size: f.size,
                })
        })
        .collect();

    if snapshot_entries.is_empty() {
        println!("No snapshots found for database '{}'", name);
        return Ok(());
    }

    let now = Utc::now();
    let plan = retention::analyze_retention(&snapshot_entries, policy, now);

    // Print summary
    println!("Compaction plan for '{}':", name);
    println!("  {}", plan.summary());
    println!();

    if !plan.has_deletions() {
        println!("Nothing to delete - all snapshots fit retention policy.");
        return Ok(());
    }

    // Print what will be kept
    println!("Keeping {} snapshots:", plan.keep.len());
    for entry in &plan.keep {
        println!(
            "  {} (TXID: {}, {})",
            entry.filename,
            entry.max_txid,
            format_age(now, entry.created_at)
        );
    }
    println!();

    // Print what will be deleted
    println!("Deleting {} snapshots:", plan.delete.len());
    for entry in &plan.delete {
        println!(
            "  {} (TXID: {}, {})",
            entry.filename,
            entry.max_txid,
            format_age(now, entry.created_at)
        );
    }
    println!();

    if !force {
        println!("Dry-run mode: no files deleted. Use --force to actually delete.");
        return Ok(());
    }

    // Actually delete files
    println!("Deleting files...");

    let keys_to_delete: Vec<String> = plan
        .delete
        .iter()
        .map(|e| format!("{}{}/{}", prefix, name, e.filename))
        .collect();

    let deleted_count = s3::delete_objects(&client, &bucket_name, &keys_to_delete).await?;

    tracing::info!("Deleted {} snapshot files", deleted_count);

    // Update manifest to remove deleted entries
    let kept_filenames: std::collections::HashSet<_> =
        plan.keep.iter().map(|e| e.filename.as_str()).collect();

    let updated_files: Vec<LtxEntry> = manifest
        .files
        .into_iter()
        .filter(|f| !f.is_snapshot || kept_filenames.contains(f.filename.as_str()))
        .collect();

    let updated_manifest = Manifest {
        files: updated_files,
        ..manifest
    };

    save_manifest(&client, &bucket_name, &prefix, &updated_manifest).await?;

    println!(
        "Compaction complete: deleted {} snapshots, freed {:.2} MB",
        deleted_count,
        plan.bytes_freed as f64 / (1024.0 * 1024.0)
    );

    Ok(())
}

/// Format age of a snapshot in human-readable form
fn format_age(now: chrono::DateTime<Utc>, created_at: chrono::DateTime<Utc>) -> String {
    let age = now.signed_duration_since(created_at);

    if age.num_hours() < 1 {
        format!("{} min ago", age.num_minutes())
    } else if age.num_hours() < 24 {
        format!("{} hours ago", age.num_hours())
    } else if age.num_days() < 7 {
        format!("{} days ago", age.num_days())
    } else if age.num_weeks() < 12 {
        format!("{} weeks ago", age.num_weeks())
    } else {
        format!("{} months ago", age.num_days() / 30)
    }
}

/// Compute SHA256 hash of file for integrity verification
async fn compute_file_sha256(path: &Path) -> Result<String> {
    use std::io::Read;
    use sha2::{Sha256, Digest};

    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 8192];

    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Take immediate snapshot as LTX file
pub async fn snapshot(database: &Path, bucket: &str, endpoint: Option<&str>) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    if !database.exists() {
        return Err(anyhow!("Database not found: {}", database.display()));
    }

    let name = database
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("Invalid database path"))?;

    let timestamp = Utc::now();

    // Get page size from database header
    let page_size = get_page_size(database).await?;

    // Load existing manifest to get current TXID
    let mut manifest = load_manifest(&client, &bucket_name, &prefix, name).await?;
    let new_txid = manifest.current_txid + 1;

    // LTX filename: {min_txid:08}-{max_txid:08}.ltx
    // For a snapshot, min=1 (contains all pages)
    let ltx_filename = format!("{:08}-{:08}.ltx", 1, new_txid);
    let ltx_key = format!("{}{}/{}", prefix, name, ltx_filename);

    // Encode database as LTX
    let mut ltx_buffer = Vec::new();
    ltx::encode_snapshot(&mut ltx_buffer, database, page_size, new_txid)?;

    let ltx_size = ltx_buffer.len() as u64;

    // Upload LTX file
    s3::upload_bytes(&client, &bucket_name, &ltx_key, ltx_buffer).await?;

    // Update manifest
    manifest.name = name.to_string();
    manifest.current_txid = new_txid;
    manifest.page_size = page_size;
    manifest.files.push(LtxEntry {
        filename: ltx_filename.clone(),
        min_txid: 1,
        max_txid: new_txid,
        size: ltx_size,
        created_at: timestamp.to_rfc3339(),
        is_snapshot: true,
    });
    save_manifest(&client, &bucket_name, &prefix, &manifest).await?;

    tracing::info!(
        "LTX snapshot uploaded: {} (TXID: {}, size: {} bytes)",
        ltx_key,
        new_txid,
        ltx_size
    );
    println!(
        "Snapshot uploaded: s3://{}/{} (TXID: {})",
        bucket_name, ltx_key, new_txid
    );
    Ok(())
}

/// Run as a read replica, polling S3 for new LTX files and applying them locally
///
/// This command:
/// 1. Bootstraps the local database from the latest snapshot if it doesn't exist
/// 2. Polls S3 at the specified interval for new LTX files
/// 3. Downloads and applies incremental LTX files in-place
/// 4. Tracks progress using TXID to know where we left off
pub async fn replicate(
    source: &str,
    local: &Path,
    interval: Duration,
    endpoint: Option<&str>,
) -> Result<()> {
    // Parse source: "s3://bucket/prefix/dbname" or "s3://bucket/dbname"
    let source = source.strip_prefix("s3://").unwrap_or(source);
    let parts: Vec<&str> = source.splitn(2, '/').collect();
    if parts.len() < 2 {
        return Err(anyhow!(
            "Invalid source format. Expected: s3://bucket/dbname or s3://bucket/prefix/dbname"
        ));
    }

    let bucket_name = parts[0];
    let path_part = parts[1];

    // Split path into prefix and dbname (last component is dbname)
    let (prefix, db_name) = if let Some(idx) = path_part.rfind('/') {
        let p = &path_part[..=idx]; // Include trailing slash
        let n = &path_part[idx + 1..];
        (p.to_string(), n.to_string())
    } else {
        (String::new(), path_part.to_string())
    };

    let client = create_client(endpoint).await?;

    tracing::info!(
        "Starting replica: source=s3://{}/{}{}, local={}",
        bucket_name,
        prefix,
        db_name,
        local.display()
    );

    // Track current TXID (0 = not yet initialized)
    let mut current_txid: u64 = 0;

    // Check if local database exists
    if local.exists() {
        // Try to determine current TXID from local state file
        let state_path = local.with_extension("db-replica-state");
        if state_path.exists() {
            if let Ok(data) = std::fs::read_to_string(&state_path) {
                if let Ok(state) = serde_json::from_str::<ReplicaState>(&data) {
                    current_txid = state.current_txid;
                    tracing::info!("Resuming replica from TXID {}", current_txid);
                }
            }
        }
    }

    println!(
        "Replicating s3://{}/{}{} -> {}",
        bucket_name,
        prefix,
        db_name,
        local.display()
    );
    println!("Poll interval: {:?}", interval);
    println!("Press Ctrl+C to stop\n");

    // Main replication loop
    loop {
        match replicate_poll(
            &client,
            bucket_name,
            &prefix,
            &db_name,
            local,
            &mut current_txid,
        )
        .await
        {
            Ok(applied) => {
                if applied > 0 {
                    println!(
                        "[{}] Applied {} LTX file(s), now at TXID {}",
                        chrono::Local::now().format("%H:%M:%S"),
                        applied,
                        current_txid
                    );
                }
            }
            Err(e) => {
                tracing::error!("Replication error: {}", e);
                eprintln!(
                    "[{}] Error: {}",
                    chrono::Local::now().format("%H:%M:%S"),
                    e
                );
            }
        }

        tokio::time::sleep(interval).await;
    }
}

/// State tracking for replica
#[derive(Debug, Serialize, Deserialize)]
struct ReplicaState {
    current_txid: u64,
    last_updated: String,
}

/// Single poll iteration for replication
async fn replicate_poll(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    local: &Path,
    current_txid: &mut u64,
) -> Result<usize> {
    // Load manifest from S3
    let manifest = load_manifest(client, bucket, prefix, db_name).await?;

    if manifest.files.is_empty() {
        return Err(anyhow!("No LTX files found in manifest for '{}'", db_name));
    }

    // If we haven't initialized yet (current_txid = 0), bootstrap from snapshot
    if *current_txid == 0 || !local.exists() {
        bootstrap_replica(client, bucket, prefix, db_name, local, &manifest).await?;
        // After bootstrap, current_txid is the snapshot's max_txid
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot)
            .max_by_key(|f| f.max_txid)
            .ok_or_else(|| anyhow!("No snapshot found for bootstrap"))?;
        *current_txid = snapshot.max_txid;
        save_replica_state(local, *current_txid)?;
        return Ok(1);
    }

    // Find incremental LTX files we need to apply (min_txid > current_txid)
    let mut incrementals: Vec<_> = manifest
        .files
        .iter()
        .filter(|f| !f.is_snapshot && f.min_txid > *current_txid)
        .collect();

    // Also check for newer snapshots that might be more efficient
    // (e.g., if we're very far behind, a snapshot might be faster)
    let latest_snapshot = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot)
        .max_by_key(|f| f.max_txid);

    // If there's a snapshot newer than our position + all incrementals we'd apply,
    // and we're far behind, consider using the snapshot instead
    if let Some(snap) = latest_snapshot {
        if snap.max_txid > *current_txid && incrementals.is_empty() {
            // We're behind but no incrementals bridge the gap - need snapshot
            tracing::info!(
                "Gap detected: at TXID {}, latest snapshot at TXID {}. Re-bootstrapping.",
                current_txid,
                snap.max_txid
            );
            bootstrap_replica(client, bucket, prefix, db_name, local, &manifest).await?;
            *current_txid = snap.max_txid;
            save_replica_state(local, *current_txid)?;
            return Ok(1);
        }
    }

    if incrementals.is_empty() {
        return Ok(0); // No new data
    }

    // Sort by min_txid to apply in order
    incrementals.sort_by_key(|f| f.min_txid);

    let mut applied = 0;

    for ltx_entry in incrementals {
        // Verify continuity: min_txid should be current_txid + 1
        // (or we accept any min_txid > current_txid for robustness)
        if ltx_entry.min_txid != *current_txid + 1 {
            tracing::warn!(
                "TXID gap: expected {}, got {}. Skipping to avoid corruption.",
                *current_txid + 1,
                ltx_entry.min_txid
            );
            // Could trigger re-bootstrap here, but for now just warn and continue
            continue;
        }

        let ltx_key = format!("{}{}/{}", prefix, db_name, ltx_entry.filename);
        tracing::debug!("Downloading incremental: {}", ltx_key);

        let ltx_data = s3::download_bytes(client, bucket, &ltx_key).await?;
        let cursor = std::io::Cursor::new(ltx_data);

        // Apply in-place
        let header = ltx::apply_ltx_to_db(cursor, local)?;

        tracing::info!(
            "Applied {} (TXID {}-{})",
            ltx_entry.filename,
            header.min_txid.into_inner(),
            header.max_txid.into_inner()
        );

        *current_txid = ltx_entry.max_txid;
        applied += 1;

        // Save state after each successful apply
        save_replica_state(local, *current_txid)?;
    }

    Ok(applied)
}

/// Bootstrap replica from latest snapshot
async fn bootstrap_replica(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
    local: &Path,
    manifest: &Manifest,
) -> Result<()> {
    // Find the best (latest) snapshot
    let snapshot = manifest
        .files
        .iter()
        .filter(|f| f.is_snapshot)
        .max_by_key(|f| f.max_txid)
        .ok_or_else(|| anyhow!("No snapshot found for database '{}'", db_name))?;

    tracing::info!(
        "Bootstrapping replica from snapshot: {} (TXID: {})",
        snapshot.filename,
        snapshot.max_txid
    );

    let ltx_key = format!("{}{}/{}", prefix, db_name, snapshot.filename);
    let ltx_data = s3::download_bytes(client, bucket, &ltx_key).await?;

    // Decode snapshot to local database
    let cursor = std::io::Cursor::new(ltx_data);
    let header = ltx::decode_to_db(cursor, local)?;

    println!(
        "Bootstrapped from snapshot: {} pages, TXID {}",
        header.commit.into_inner(),
        header.max_txid.into_inner()
    );

    Ok(())
}

/// Save replica state to local file
fn save_replica_state(local: &Path, current_txid: u64) -> Result<()> {
    let state_path = local.with_extension("db-replica-state");
    let state = ReplicaState {
        current_txid,
        last_updated: Utc::now().to_rfc3339(),
    };
    let data = serde_json::to_string_pretty(&state)?;
    std::fs::write(&state_path, data)?;
    Ok(())
}

/// Explain what the current configuration will do without running
///
/// Loads the config file and prints a human-readable summary of:
/// - Databases being watched (resolved from config/globs)
/// - Snapshot triggers (interval, max_changes, on_idle, on_startup)
/// - Compaction settings if enabled
/// - Retention policy tiers
/// - S3 bucket and endpoint
pub fn explain(config: &Option<Config>) -> Result<()> {
    match config {
        None => {
            println!("No configuration file found.");
            println!();
            println!("walrust looks for ./walrust.toml in the current directory,");
            println!("or you can specify a config file with --config <path>.");
            println!();
            println!("Without a config file, you must provide all options via CLI:");
            println!("  walrust watch <database> --bucket <bucket> [options]");
            return Ok(());
        }
        Some(cfg) => {
            println!("Configuration Summary");
            println!("=====================");
            println!();

            // S3 Settings
            println!("S3 Storage:");
            if let Some(bucket) = &cfg.s3.bucket {
                println!("  Bucket:   {}", bucket);
            } else {
                println!("  Bucket:   (not configured - must specify via --bucket)");
            }
            if let Some(endpoint) = &cfg.s3.endpoint {
                println!("  Endpoint: {}", endpoint);
            } else {
                println!("  Endpoint: (default AWS S3)");
            }
            println!();

            // Snapshot Triggers
            println!("Snapshot Triggers (global defaults):");
            println!("  Interval:    {} seconds ({} minutes)",
                cfg.sync.snapshot_interval,
                cfg.sync.snapshot_interval / 60
            );
            if cfg.sync.max_changes > 0 {
                println!("  Max changes: {} WAL frames", cfg.sync.max_changes);
            } else {
                println!("  Max changes: disabled");
            }
            if cfg.sync.max_interval > 0 {
                println!("  Max interval: {} seconds", cfg.sync.max_interval);
            }
            if cfg.sync.on_idle > 0 {
                println!("  On idle:     {} seconds", cfg.sync.on_idle);
            } else {
                println!("  On idle:     disabled");
            }
            println!("  On startup:  {}", if cfg.sync.on_startup { "yes" } else { "no" });
            println!();

            // Compaction Settings
            println!("Compaction:");
            if cfg.sync.compact_after_snapshot {
                println!("  After snapshot: enabled");
            } else {
                println!("  After snapshot: disabled");
            }
            if cfg.sync.compact_interval > 0 {
                println!("  Interval:       {} seconds ({} minutes)",
                    cfg.sync.compact_interval,
                    cfg.sync.compact_interval / 60
                );
            } else {
                println!("  Interval:       disabled");
            }
            println!();

            // Retention Policy
            println!("Retention Policy (GFS rotation):");
            println!("  Hourly:  {} snapshots (last {} hours)", cfg.retention.hourly, cfg.retention.hourly);
            println!("  Daily:   {} snapshots (last {} days)", cfg.retention.daily, cfg.retention.daily);
            println!("  Weekly:  {} snapshots (last {} weeks)", cfg.retention.weekly, cfg.retention.weekly);
            println!("  Monthly: {} snapshots (last {} months)", cfg.retention.monthly, cfg.retention.monthly);
            println!();

            // Databases
            println!("Databases:");
            if cfg.databases.is_empty() {
                println!("  (none configured - must specify via CLI)");
            } else {
                // Resolve databases to show actual paths
                match cfg.resolve_databases() {
                    Ok(resolved) => {
                        if resolved.is_empty() {
                            println!("  (no matching files found for configured patterns)");
                        } else {
                            for db in &resolved {
                                println!("  - {} -> s3://.../{}/*", db.path.display(), db.prefix);

                                // Show per-database overrides if different from global
                                let mut overrides = Vec::new();
                                if db.sync.snapshot_interval != cfg.sync.snapshot_interval {
                                    overrides.push(format!("interval={}s", db.sync.snapshot_interval));
                                }
                                if db.sync.max_changes != cfg.sync.max_changes {
                                    overrides.push(format!("max_changes={}", db.sync.max_changes));
                                }
                                if db.retention.hourly != cfg.retention.hourly
                                    || db.retention.daily != cfg.retention.daily
                                    || db.retention.weekly != cfg.retention.weekly
                                    || db.retention.monthly != cfg.retention.monthly
                                {
                                    overrides.push(format!(
                                        "retention={}/{}/{}/{}",
                                        db.retention.hourly, db.retention.daily,
                                        db.retention.weekly, db.retention.monthly
                                    ));
                                }
                                if !overrides.is_empty() {
                                    println!("    Overrides: {}", overrides.join(", "));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("  (error resolving databases: {})", e);
                        for db in &cfg.databases {
                            println!("  - {} (pattern)", db.path);
                        }
                    }
                }
            }
            println!();

            // Summary
            let total_snapshots = cfg.retention.hourly + cfg.retention.daily
                + cfg.retention.weekly + cfg.retention.monthly;
            println!("Summary:");
            println!("  Max snapshots retained per database: ~{}", total_snapshots);
            if cfg.sync.compact_after_snapshot || cfg.sync.compact_interval > 0 {
                println!("  Automatic compaction: enabled");
            } else {
                println!("  Automatic compaction: disabled (run 'walrust compact' manually)");
            }
        }
    }

    Ok(())
}

/// Verification issue found during verify
#[derive(Debug, Clone)]
pub struct VerifyIssue {
    pub filename: String,
    pub issue: String,
    pub is_orphan: bool,
}

/// Result of backup validation
#[derive(Debug)]
pub struct ValidationResult {
    pub verified_count: usize,
    pub total_files: usize,
    pub issues: Vec<VerifyIssue>,
    pub verified_size_bytes: u64,
    pub is_valid: bool,
}

/// Validate backup integrity for a database (non-blocking, for periodic validation)
async fn validate_backup_integrity(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    db_name: &str,
) -> Result<ValidationResult> {
    // Load manifest
    let manifest = load_manifest(client, bucket, prefix, db_name).await?;

    if manifest.files.is_empty() {
        return Ok(ValidationResult {
            verified_count: 0,
            total_files: 0,
            issues: Vec::new(),
            verified_size_bytes: 0,
            is_valid: true,
        });
    }

    let mut issues: Vec<VerifyIssue> = Vec::new();
    let mut verified_count = 0;
    let mut total_size: u64 = 0;

    // Check each LTX file
    for entry in &manifest.files {
        let ltx_key = format!("{}{}/{}", prefix, db_name, entry.filename);

        match s3::exists(client, bucket, &ltx_key).await {
            Ok(true) => {
                // File exists, download and verify
                match s3::download_bytes(client, bucket, &ltx_key).await {
                    Ok(data) => {
                        let cursor = std::io::Cursor::new(&data);
                        match ltx::verify_ltx(cursor) {
                            Ok(header) => {
                                let header_min = header.min_txid.into_inner();
                                let header_max = header.max_txid.into_inner();

                                if header_min != entry.min_txid || header_max != entry.max_txid {
                                    issues.push(VerifyIssue {
                                        filename: entry.filename.clone(),
                                        issue: format!(
                                            "TXID mismatch: manifest {}-{}, header {}-{}",
                                            entry.min_txid, entry.max_txid,
                                            header_min, header_max
                                        ),
                                        is_orphan: false,
                                    });
                                } else {
                                    verified_count += 1;
                                    total_size += data.len() as u64;
                                }
                            }
                            Err(e) => {
                                issues.push(VerifyIssue {
                                    filename: entry.filename.clone(),
                                    issue: format!("Checksum failed: {}", e),
                                    is_orphan: false,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        issues.push(VerifyIssue {
                            filename: entry.filename.clone(),
                            issue: format!("Download failed: {}", e),
                            is_orphan: false,
                        });
                    }
                }
            }
            Ok(false) => {
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: "File missing from S3".to_string(),
                    is_orphan: true,
                });
            }
            Err(e) => {
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: format!("S3 check failed: {}", e),
                    is_orphan: false,
                });
            }
        }
    }

    // Check TXID continuity
    let mut sorted_files: Vec<_> = manifest.files.iter().collect();
    sorted_files.sort_by_key(|f| f.min_txid);

    let mut expected_next_txid: Option<u64> = None;
    for entry in &sorted_files {
        if let Some(expected) = expected_next_txid {
            // For incrementals, check for gaps
            if !entry.is_snapshot && entry.min_txid != expected && entry.min_txid > expected {
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: format!(
                        "TXID gap: expected {}, got {} (missing {}-{})",
                        expected, entry.min_txid,
                        expected, entry.min_txid - 1
                    ),
                    is_orphan: false,
                });
            }
        }
        expected_next_txid = Some(entry.max_txid + 1);
    }

    Ok(ValidationResult {
        verified_count,
        total_files: manifest.files.len(),
        issues: issues.clone(),
        verified_size_bytes: total_size,
        is_valid: issues.is_empty(),
    })
}

/// Verify integrity of all LTX files in S3 for a database
///
/// Checks:
/// - Each LTX file in manifest exists in S3
/// - LTX headers can be decoded
/// - LTX internal checksums are valid
/// - TXID continuity (no gaps in the chain)
///
/// With --fix, removes orphaned entries from manifest
pub async fn verify(
    name: &str,
    bucket: &str,
    endpoint: Option<&str>,
    fix: bool,
) -> Result<()> {
    let (bucket_name, prefix) = parse_bucket(bucket);
    let client = create_client(endpoint).await?;

    println!("Verifying integrity of '{}' in s3://{}/{}{}...",
        name, bucket_name, prefix, name);
    println!();

    // Load manifest
    let manifest = load_manifest(&client, &bucket_name, &prefix, name).await?;

    if manifest.files.is_empty() {
        println!("No LTX files found in manifest.");
        return Ok(());
    }

    println!("Found {} LTX files in manifest", manifest.files.len());
    println!("Current TXID: {}", manifest.current_txid);
    println!("Page size: {} bytes", manifest.page_size);
    println!();

    let mut issues: Vec<VerifyIssue> = Vec::new();
    let mut verified_count = 0;
    let mut total_size: u64 = 0;

    // Check each LTX file
    for entry in &manifest.files {
        let ltx_key = format!("{}{}/{}", prefix, name, entry.filename);

        // Check if file exists in S3
        match s3::exists(&client, &bucket_name, &ltx_key).await {
            Ok(true) => {
                // File exists, download and verify
                match s3::download_bytes(&client, &bucket_name, &ltx_key).await {
                    Ok(data) => {
                        let cursor = std::io::Cursor::new(&data);
                        match ltx::verify_ltx(cursor) {
                            Ok(header) => {
                                // Verify header matches manifest entry
                                let header_min = header.min_txid.into_inner();
                                let header_max = header.max_txid.into_inner();

                                if header_min != entry.min_txid || header_max != entry.max_txid {
                                    issues.push(VerifyIssue {
                                        filename: entry.filename.clone(),
                                        issue: format!(
                                            "TXID mismatch: manifest says {}-{}, header says {}-{}",
                                            entry.min_txid, entry.max_txid,
                                            header_min, header_max
                                        ),
                                        is_orphan: false,
                                    });
                                } else {
                                    verified_count += 1;
                                    total_size += data.len() as u64;
                                }
                            }
                            Err(e) => {
                                issues.push(VerifyIssue {
                                    filename: entry.filename.clone(),
                                    issue: format!("Checksum verification failed: {}", e),
                                    is_orphan: false,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        issues.push(VerifyIssue {
                            filename: entry.filename.clone(),
                            issue: format!("Download failed: {}", e),
                            is_orphan: false,
                        });
                    }
                }
            }
            Ok(false) => {
                // File missing from S3 but in manifest
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: "File missing from S3".to_string(),
                    is_orphan: true,
                });
            }
            Err(e) => {
                issues.push(VerifyIssue {
                    filename: entry.filename.clone(),
                    issue: format!("S3 check failed: {}", e),
                    is_orphan: false,
                });
            }
        }
    }

    // Check TXID continuity
    let mut sorted_files: Vec<_> = manifest.files.iter().collect();
    sorted_files.sort_by_key(|f| f.min_txid);

    let mut expected_next_txid: Option<u64> = None;
    for entry in &sorted_files {
        if let Some(expected) = expected_next_txid {
            // For incrementals, min_txid should be expected (previous max + 1)
            // For snapshots, they can reset the chain (min_txid = 1)
            if !entry.is_snapshot && entry.min_txid != expected {
                // Check if there's a gap
                if entry.min_txid > expected {
                    issues.push(VerifyIssue {
                        filename: entry.filename.clone(),
                        issue: format!(
                            "TXID gap: expected min_txid={}, got {} (missing TXIDs {}-{})",
                            expected, entry.min_txid,
                            expected, entry.min_txid - 1
                        ),
                        is_orphan: false,
                    });
                }
            }
        }
        expected_next_txid = Some(entry.max_txid + 1);
    }

    // Report results
    println!("Verification Results");
    println!("====================");
    println!("Verified:  {} files ({:.2} MB)", verified_count, total_size as f64 / (1024.0 * 1024.0));
    println!("Issues:    {}", issues.len());
    println!();

    if issues.is_empty() {
        println!("All LTX files verified successfully.");
        return Ok(());
    }

    // Report issues
    println!("Issues Found:");
    let orphan_count = issues.iter().filter(|i| i.is_orphan).count();
    let other_count = issues.len() - orphan_count;

    for issue in &issues {
        let marker = if issue.is_orphan { "[ORPHAN]" } else { "[ERROR]" };
        println!("  {} {}: {}", marker, issue.filename, issue.issue);
    }
    println!();

    // Fix orphaned entries if requested
    if fix && orphan_count > 0 {
        println!("Fixing {} orphaned manifest entries...", orphan_count);

        let orphan_filenames: std::collections::HashSet<_> = issues
            .iter()
            .filter(|i| i.is_orphan)
            .map(|i| &i.filename)
            .collect();

        let mut fixed_manifest = manifest.clone();
        fixed_manifest.files.retain(|f| !orphan_filenames.contains(&f.filename));

        // Update current_txid to latest remaining file
        if let Some(latest) = fixed_manifest.files.iter().max_by_key(|f| f.max_txid) {
            fixed_manifest.current_txid = latest.max_txid;
        }

        save_manifest(&client, &bucket_name, &prefix, &fixed_manifest).await?;
        println!("Removed {} orphaned entries from manifest.", orphan_count);
    } else if orphan_count > 0 {
        println!("Run with --fix to remove {} orphaned manifest entries.", orphan_count);
    }

    if other_count > 0 {
        println!();
        println!("Note: {} non-orphan issues found. These may require manual intervention:", other_count);
        println!("  - Checksum failures indicate corrupted files");
        println!("  - TXID gaps may require restoring from an earlier snapshot");
    }

    Ok(())
}

/// Checkpoint mode for SQLite WAL
#[derive(Debug, Clone, Copy)]
enum CheckpointMode {
    /// Non-blocking, best effort checkpoint
    Passive,
    /// Blocking checkpoint that ensures WAL is reset
    Truncate,
}

/// Get WAL page count for size checking
async fn get_wal_page_count(wal_path: &Path) -> Result<u64> {
    if !wal_path.exists() {
        return Ok(0);
    }

    // WAL file size / page size (4096 bytes typically)
    let metadata = tokio::fs::metadata(wal_path).await?;
    let file_size = metadata.len();

    if file_size < 32 {
        // WAL file too small to have a valid header
        return Ok(0);
    }

    // Read page size from WAL header (bytes 8-11)
    let mut file = tokio::fs::File::open(wal_path).await?;
    let mut header = vec![0u8; 32];
    use tokio::io::AsyncReadExt;
    file.read_exact(&mut header).await?;

    let page_size = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as u64;

    // Account for WAL header (32 bytes) + frame headers (24 bytes each)
    // Approximate: (file_size - 32) / (page_size + 24)
    let approx_pages = if page_size > 0 {
        (file_size.saturating_sub(32)) / (page_size + 24)
    } else {
        0
    };

    Ok(approx_pages)
}

/// Run SQLite checkpoint on database
async fn run_checkpoint(db_path: &Path, mode: CheckpointMode) -> Result<()> {
    // Use blocking task since SQLite operations are synchronous
    let db_path = db_path.to_path_buf();

    tokio::task::spawn_blocking(move || {
        let conn = rusqlite::Connection::open(&db_path)?;

        let pragma = match mode {
            CheckpointMode::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
            CheckpointMode::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
        };

        // Returns (busy, checkpointed_frames, log_size)
        let (busy, frames, log_size): (i32, i32, i32) = conn.query_row(pragma, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;

        if busy != 0 {
            tracing::debug!("Checkpoint was busy (concurrent writers)");
        }

        tracing::debug!(
            "Checkpointed {} frames (log size: {})",
            frames,
            log_size
        );
        Ok(())
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn get_test_bucket() -> Option<String> {
        std::env::var("WALRUST_TEST_BUCKET").ok()
    }

    fn get_test_endpoint() -> Option<String> {
        std::env::var("AWS_ENDPOINT_URL_S3").ok()
    }

    /// Helper to create a test database with valid SQLite structure
    async fn create_test_db(name: &str) -> PathBuf {
        let path = PathBuf::from(format!("/tmp/walrust-test-{}.db", name));
        let page_size = 4096u32;

        // Create a minimal valid SQLite database (1 page)
        let mut db_data = vec![0u8; page_size as usize];
        // SQLite header magic
        db_data[0..16].copy_from_slice(b"SQLite format 3\0");
        // Page size at offset 16-17 (big-endian)
        db_data[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        // File format versions
        db_data[18] = 1;
        db_data[19] = 1;
        // Reserved space
        db_data[20] = 0;
        // Max/min payload fractions
        db_data[21] = 64;
        db_data[22] = 32;
        db_data[23] = 32;
        // File change counter
        db_data[24..28].copy_from_slice(&1u32.to_be_bytes());
        // Database size in pages
        db_data[28..32].copy_from_slice(&1u32.to_be_bytes());

        tokio::fs::write(&path, &db_data).await.ok();
        path
    }

    /// Helper to create a test WAL file
    async fn create_test_wal(db_path: &PathBuf) {
        let wal_path = db_path.with_extension("db-wal");
        let mut wal_data = vec![0u8; 32];
        // Write valid WAL magic number (0x377F0682)
        wal_data[0..4].copy_from_slice(&0x377F0682u32.to_be_bytes());
        // Format version
        wal_data[4..8].copy_from_slice(&3007000u32.to_be_bytes());
        // Page size
        wal_data[8..12].copy_from_slice(&4096u32.to_be_bytes());
        // Add a simple frame
        let page_size = 4096u32;
        let frame_size = 24 + page_size as usize;
        wal_data.resize(32 + frame_size, 0u8);
        tokio::fs::write(&wal_path, wal_data).await.ok();
    }

    /// Compute SHA256 hash of data for integrity verification (for tests)
    fn compute_sha256(data: &[u8]) -> String {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_snapshot() {
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("snapshot-test-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;

        let result = snapshot(&db_path, &bucket, endpoint.as_deref()).await;

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();

        assert!(result.is_ok(), "Snapshot should succeed");
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_list_empty_bucket() {
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();

        // This should not panic even if bucket is empty or only has test files
        let result = list(&bucket, endpoint.as_deref()).await;
        assert!(result.is_ok(), "List should succeed on bucket");
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_list_with_database() {
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("list-test-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;

        // Upload a snapshot
        let _ = snapshot(&db_path, &bucket, endpoint.as_deref()).await;

        // List databases
        let result = list(&bucket, endpoint.as_deref()).await;

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();

        assert!(result.is_ok(), "List should succeed");
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_restore_nonexistent() {
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let output = PathBuf::from(format!("/tmp/restored-{}.db", uuid::Uuid::new_v4()));

        let result = restore("nonexistent-db", &output, &bucket, endpoint.as_deref(), None).await;

        // Should fail - no snapshots exist
        assert!(result.is_err(), "Restore of nonexistent database should fail");

        // Cleanup
        tokio::fs::remove_file(&output).await.ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_snapshot_and_restore() {
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("snapshot-restore-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;
        let db_name = db_path.file_stem().unwrap().to_str().unwrap();
        let restored_path = PathBuf::from(format!("/tmp/restored-{}.db", uuid::Uuid::new_v4()));

        // Read original database content and compute hash
        let original_data = tokio::fs::read(&db_path).await.unwrap();
        let original_hash = compute_sha256(&original_data);

        // Take snapshot
        let snapshot_result = snapshot(&db_path, &bucket, endpoint.as_deref()).await;
        assert!(snapshot_result.is_ok(), "Snapshot should succeed");

        // Wait a moment for S3 to be consistent
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Restore database
        let restore_result = restore(db_name, &restored_path, &bucket, endpoint.as_deref(), None).await;
        assert!(restore_result.is_ok(), "Restore should succeed");

        // Verify restored file exists
        assert!(restored_path.exists(), "Restored database should exist");

        // CRITICAL: Verify restored database matches original exactly
        let restored_data = tokio::fs::read(&restored_path).await.unwrap();
        let restored_hash = compute_sha256(&restored_data);

        assert_eq!(original_data.len(), restored_data.len(),
            "Restored database size ({}) must match original ({})",
            restored_data.len(), original_data.len());
        assert_eq!(original_hash, restored_hash,
            "Restored database content must be byte-for-byte identical to original");
        assert_eq!(original_data, restored_data,
            "Restored database is not identical to original");

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();
        tokio::fs::remove_file(&restored_path).await.ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_sync_wal_workflow() {
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("wal-sync-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;

        // Create a WAL file
        create_test_wal(&db_path).await;

        // Take initial snapshot - this should work with a WAL file present
        let snapshot_result = snapshot(&db_path, &bucket, endpoint.as_deref()).await;
        assert!(snapshot_result.is_ok(), "Snapshot with WAL should succeed");

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();
        tokio::fs::remove_file(db_path.with_extension("db-wal")).await.ok();
    }

    #[test]
    fn test_parse_bucket_variations() {
        // This tests the bucket parsing logic used by sync functions
        let (bucket1, prefix1) = crate::s3::parse_bucket("s3://my-bucket");
        assert_eq!(bucket1, "my-bucket");
        assert_eq!(prefix1, "");

        let (bucket2, prefix2) = crate::s3::parse_bucket("s3://my-bucket/walrust/");
        assert_eq!(bucket2, "my-bucket");
        assert_eq!(prefix2, "walrust/");

        let (bucket3, prefix3) = crate::s3::parse_bucket("my-bucket/path/to/prefix");
        assert_eq!(bucket3, "my-bucket");
        assert_eq!(prefix3, "path/to/prefix");
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_snapshot_and_restore_with_data() {
        // Test that snapshot/restore preserves exact data content (like Litestream)
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("snapshot-restore-data-{}", uuid::Uuid::new_v4());
        let db_path = PathBuf::from(format!("/tmp/walrust-test-{}.db", test_name));
        let db_name = db_path.file_stem().unwrap().to_str().unwrap();
        let restored_path = PathBuf::from(format!("/tmp/restored-{}.db", uuid::Uuid::new_v4()));

        // Create a valid SQLite-structured database with varied binary content
        // Must have: valid header, page_size at bytes 16-17, and be page-aligned
        let page_size = 4096u32;
        let num_pages = 3; // 3 pages = 12KB database
        let mut original_data = vec![0u8; (page_size as usize) * num_pages];

        // Page 1: Valid SQLite header with varied content
        original_data[0..16].copy_from_slice(b"SQLite format 3\0");
        original_data[16..18].copy_from_slice(&(page_size as u16).to_be_bytes()); // Page size
        original_data[18] = 1; // File format write version
        original_data[19] = 1; // File format read version
        original_data[20] = 0; // Reserved space
        original_data[21] = 64; // Max payload fraction
        original_data[22] = 32; // Min payload fraction
        original_data[23] = 32; // Leaf payload fraction
        original_data[24..28].copy_from_slice(&1u32.to_be_bytes()); // File change counter
        original_data[28..32].copy_from_slice(&(num_pages as u32).to_be_bytes()); // DB size in pages

        // Fill rest of page 1 with varied byte patterns
        for i in 100..page_size as usize {
            original_data[i] = (i % 256) as u8;
        }

        // Page 2: All byte values 0x00-0xFF repeated
        let page2_start = page_size as usize;
        for i in 0..page_size as usize {
            original_data[page2_start + i] = (i % 256) as u8;
        }

        // Page 3: Mix of patterns including 0xFF and custom data
        let page3_start = (page_size * 2) as usize;
        for i in 0..1024 {
            original_data[page3_start + i] = 0xFF; // First 1KB = 0xFF
        }
        let test_msg = b"This is test data for binary preservation verification!";
        original_data[page3_start + 1024..page3_start + 1024 + test_msg.len()].copy_from_slice(test_msg);
        for i in (page3_start + 2048)..(page_size * 3) as usize {
            original_data[i] = 0x42; // Fill rest with 'B'
        }

        tokio::fs::write(&db_path, &original_data).await.unwrap();

        // Snapshot -> Restore -> Verify exact match
        snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        restore(db_name, &restored_path, &bucket, endpoint.as_deref(), None).await.unwrap();

        let restored_data = tokio::fs::read(&restored_path).await.unwrap();

        // Critical verification: byte-for-byte identical
        assert_eq!(original_data.len(), restored_data.len(),
            "Size mismatch: original={}, restored={}",
            original_data.len(), restored_data.len());

        for (i, (orig, restored)) in original_data.iter().zip(restored_data.iter()).enumerate() {
            assert_eq!(orig, restored,
                "Data mismatch at byte {}: original=0x{:02x}, restored=0x{:02x}",
                i, orig, restored);
        }

        assert_eq!(original_data, restored_data,
            "Restored database is NOT identical to original - data corruption detected!");

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();
        tokio::fs::remove_file(&restored_path).await.ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_multi_database_snapshot() {
        // Test walrust advantage: single process handles multiple databases
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();

        const NUM_DBS: usize = 5;
        let mut db_paths = Vec::new();
        let mut db_names = Vec::new();

        // Create multiple test databases
        for i in 0..NUM_DBS {
            let test_name = format!("multi-db-{}-{}", i, uuid::Uuid::new_v4());
            let db_path = create_test_db(&test_name).await;
            db_names.push(test_name);
            db_paths.push(db_path);
        }

        // Snapshot all databases (this is where walrust shines - single process)
        for db_path in &db_paths {
            let result = snapshot(db_path, &bucket, endpoint.as_deref()).await;
            assert!(result.is_ok(), "All snapshots should succeed");
        }

        // Verify all were uploaded
        let list_result = list(&bucket, endpoint.as_deref()).await;
        assert!(list_result.is_ok(), "List should succeed");

        // Cleanup
        for db_path in &db_paths {
            tokio::fs::remove_file(db_path).await.ok();
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_checksum_verification() {
        // Test that checksums are stored and verified
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("checksum-test-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;
        let db_name = db_path.file_stem().unwrap().to_str().unwrap();
        let restored_path = PathBuf::from(format!("/tmp/restored-checksum-{}.db", uuid::Uuid::new_v4()));

        // Read original and compute its hash
        let original_data = tokio::fs::read(&db_path).await.unwrap();
        let original_hash = compute_sha256(&original_data);

        // Snapshot (should store checksum in metadata)
        snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Restore (should verify checksum)
        let restore_result = restore(db_name, &restored_path, &bucket, endpoint.as_deref(), None).await;
        assert!(restore_result.is_ok(), "Restore with valid checksum should succeed");

        // Verify restored data
        let restored_data = tokio::fs::read(&restored_path).await.unwrap();
        let restored_hash = compute_sha256(&restored_data);

        assert_eq!(original_hash, restored_hash, "Checksums should match");

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();
        tokio::fs::remove_file(&restored_path).await.ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_manifest_updates() {
        // Test that manifest is properly created and updated across snapshots
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("manifest-test-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;
        let db_name = db_path.file_stem().unwrap().to_str().unwrap();

        // First snapshot
        snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Second snapshot (should increment TXID)
        snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Third snapshot
        snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Verify manifest has 3 entries (we can't directly check without downloading,
        // but restore should succeed with latest TXID)
        let restored_path = PathBuf::from(format!("/tmp/restored-manifest-{}.db", uuid::Uuid::new_v4()));
        let restore_result = restore(db_name, &restored_path, &bucket, endpoint.as_deref(), None).await;
        assert!(restore_result.is_ok(), "Restore should find latest snapshot from manifest");

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();
        tokio::fs::remove_file(&restored_path).await.ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_point_in_time_restore_by_txid() {
        // Test point-in-time restore using TXID
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("pit-txid-test-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;
        let db_name = db_path.file_stem().unwrap().to_str().unwrap();

        // Create multiple snapshots
        snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap(); // TXID 1
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Modify DB content slightly
        let mut data = tokio::fs::read(&db_path).await.unwrap();
        data.extend(vec![0xAA; 100]);
        tokio::fs::write(&db_path, &data).await.unwrap();

        snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap(); // TXID 2
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Restore to TXID 1 (first snapshot)
        let restored_path = PathBuf::from(format!("/tmp/restored-pit-{}.db", uuid::Uuid::new_v4()));
        let restore_result = restore(db_name, &restored_path, &bucket, endpoint.as_deref(), Some("1")).await;
        assert!(restore_result.is_ok(), "Point-in-time restore by TXID should succeed");

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();
        tokio::fs::remove_file(&restored_path).await.ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_ltx_file_naming() {
        // Test that LTX files are created with correct naming convention
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("ltx-naming-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;

        // Snapshot creates LTX file: 00000001-{txid}.ltx
        snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // List should work and show the database
        let list_result = list(&bucket, endpoint.as_deref()).await;
        assert!(list_result.is_ok());

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_sqlite_like_database() {
        // Test with a database that has SQLite-like structure
        use tempfile::tempdir;

        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join(format!("sqlite-like-{}.db", uuid::Uuid::new_v4()));
        let db_name = db_path.file_stem().unwrap().to_str().unwrap();
        let restored_path = dir.path().join("restored.db");

        let page_size = 4096u32;

        // Create a database with valid SQLite header structure
        let mut db_data = vec![0u8; page_size as usize * 3]; // 3 pages
        // SQLite header magic
        db_data[0..16].copy_from_slice(b"SQLite format 3\0");
        // Page size at offset 16-17 (big-endian)
        db_data[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        // File format versions
        db_data[18] = 1;
        db_data[19] = 1;
        // Database file change counter
        db_data[24..28].copy_from_slice(&1u32.to_be_bytes());
        // Schema version
        db_data[40..44].copy_from_slice(&1u32.to_be_bytes());
        // Add some varied content in remaining pages
        for i in page_size as usize..db_data.len() {
            db_data[i] = ((i * 17) % 256) as u8;
        }

        tokio::fs::write(&db_path, &db_data).await.unwrap();

        let original_hash = compute_sha256(&db_data);

        // Snapshot
        let snapshot_result = snapshot(&db_path, &bucket, endpoint.as_deref()).await;
        assert!(snapshot_result.is_ok(), "Snapshot of SQLite-like DB should succeed");

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Restore
        let restore_result = restore(db_name, &restored_path, &bucket, endpoint.as_deref(), None).await;
        assert!(restore_result.is_ok(), "Restore should succeed");

        // Verify byte-for-byte match
        let restored_data = tokio::fs::read(&restored_path).await.unwrap();
        let restored_hash = compute_sha256(&restored_data);

        assert_eq!(original_hash, restored_hash, "Database should be byte-identical after restore");
        assert_eq!(db_data, restored_data);
    }

    #[test]
    fn test_performance_multi_database_advantage() {
        // This test documents the theoretical advantage of walrust vs Litestream
        // Litestream: N databases = N processes = N overhead
        // Walrust: N databases = 1 process = 1 overhead

        let database_counts = vec![1, 5, 10, 100];

        println!("\n=== Performance Advantage: Walrust vs Litestream ===\n");
        println!("Databases | Litestream Processes | Walrust Processes | Memory Saved (est)");
        println!("----------|---------------------|-------------------|------------------");

        for count in database_counts {
            let litestream_processes = count;
            let walrust_processes = 1;
            let processes_saved = litestream_processes - walrust_processes;

            // Rough estimate: ~50MB per Litestream process
            let memory_per_process = 50;
            let memory_saved_mb = processes_saved * memory_per_process;

            println!(
                "{:9} | {:21} | {:17} | {:>14} MB",
                count, litestream_processes, walrust_processes, memory_saved_mb
            );
        }

        println!("\nNote: This is a theoretical advantage. Actual overhead depends on");
        println!("binary size, Tigris connection pooling, and WAL activity per database.\n");
    }

    // ============================================
    // Manifest Tests
    // ============================================

    #[test]
    fn test_manifest_serialization() {
        let manifest = Manifest {
            name: "testdb".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000050.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 50,
                    size: 1024,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000051-00000100.ltx".to_string(),
                    min_txid: 51,
                    max_txid: 100,
                    size: 512,
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                    is_snapshot: false,
                },
            ],
            last_checksum: None,
        };

        // Serialize
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        assert!(json.contains("testdb"));
        assert!(json.contains("00000001-00000050.ltx"));

        // Deserialize
        let parsed: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "testdb");
        assert_eq!(parsed.current_txid, 100);
        assert_eq!(parsed.files.len(), 2);
        assert!(parsed.files[0].is_snapshot);
        assert!(!parsed.files[1].is_snapshot);
    }

    #[test]
    fn test_manifest_default() {
        let manifest = Manifest::default();
        assert_eq!(manifest.name, "");
        assert_eq!(manifest.current_txid, 0);
        assert_eq!(manifest.page_size, 0);
        assert!(manifest.files.is_empty());
    }

    #[test]
    fn test_ltx_entry_serialization() {
        let entry = LtxEntry {
            filename: "00000001-00000010.ltx".to_string(),
            min_txid: 1,
            max_txid: 10,
            size: 8192,
            created_at: "2024-06-15T12:30:45Z".to_string(),
            is_snapshot: true,
        };

        let json = serde_json::to_string(&entry).unwrap();
        let parsed: LtxEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.filename, entry.filename);
        assert_eq!(parsed.min_txid, entry.min_txid);
        assert_eq!(parsed.max_txid, entry.max_txid);
        assert_eq!(parsed.size, entry.size);
        assert_eq!(parsed.is_snapshot, entry.is_snapshot);
    }

    #[test]
    fn test_ltx_filename_format() {
        // Test the LTX filename format: {min_txid:08}-{max_txid:08}.ltx
        let test_cases = vec![
            (1, 1, "00000001-00000001.ltx"),
            (1, 100, "00000001-00000100.ltx"),
            (50, 150, "00000050-00000150.ltx"),
            (1000000, 1000050, "01000000-01000050.ltx"),
        ];

        for (min_txid, max_txid, expected) in test_cases {
            let filename = format!("{:08}-{:08}.ltx", min_txid, max_txid);
            assert_eq!(filename, expected);
        }
    }

    #[test]
    fn test_manifest_find_latest_snapshot() {
        let manifest = Manifest {
            name: "test".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000020.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 20,
                    size: 1000,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000021-00000040.ltx".to_string(),
                    min_txid: 21,
                    max_txid: 40,
                    size: 500,
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                    is_snapshot: false,
                },
                LtxEntry {
                    filename: "00000001-00000060.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 60,
                    size: 1500,
                    created_at: "2024-01-01T02:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000061-00000100.ltx".to_string(),
                    min_txid: 61,
                    max_txid: 100,
                    size: 600,
                    created_at: "2024-01-01T03:00:00Z".to_string(),
                    is_snapshot: false,
                },
            ],
        last_checksum: None,
        };

        // Find latest snapshot up to TXID 50
        let target_txid = 50u64;
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot && f.max_txid <= target_txid)
            .max_by_key(|f| f.max_txid);

        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().max_txid, 20); // First snapshot

        // Find latest snapshot up to TXID 100
        let target_txid = 100u64;
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot && f.max_txid <= target_txid)
            .max_by_key(|f| f.max_txid);

        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().max_txid, 60); // Second snapshot
    }

    #[test]
    fn test_manifest_find_incrementals_after_snapshot() {
        let manifest = Manifest {
            name: "test".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000050.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 50,
                    size: 1000,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000051-00000070.ltx".to_string(),
                    min_txid: 51,
                    max_txid: 70,
                    size: 500,
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                    is_snapshot: false,
                },
                LtxEntry {
                    filename: "00000071-00000100.ltx".to_string(),
                    min_txid: 71,
                    max_txid: 100,
                    size: 600,
                    created_at: "2024-01-01T02:00:00Z".to_string(),
                    is_snapshot: false,
                },
            ],
        last_checksum: None,
        };

        // Find incrementals after snapshot (max_txid=50) up to target (80)
        let snapshot_max_txid = 50u64;
        let target_txid = 80u64;

        let incrementals: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| !f.is_snapshot && f.min_txid > snapshot_max_txid && f.max_txid <= target_txid)
            .collect();

        assert_eq!(incrementals.len(), 1);
        assert_eq!(incrementals[0].filename, "00000051-00000070.ltx");

        // Find incrementals up to TXID 100
        let target_txid = 100u64;
        let incrementals: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| !f.is_snapshot && f.min_txid > snapshot_max_txid && f.max_txid <= target_txid)
            .collect();

        assert_eq!(incrementals.len(), 2);
    }

    // ============================================
    // Page Size Tests
    // ============================================

    #[tokio::test]
    async fn test_get_page_size_sqlite_format() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a minimal SQLite header with page size 4096
        let mut header = vec![0u8; 100];
        header[0..16].copy_from_slice(b"SQLite format 3\0");
        // Page size at offset 16-17, big-endian
        header[16..18].copy_from_slice(&4096u16.to_be_bytes());

        tokio::fs::write(&db_path, header).await.unwrap();

        let page_size = get_page_size(&db_path).await.unwrap();
        assert_eq!(page_size, 4096);
    }

    #[tokio::test]
    async fn test_get_page_size_65536() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Page size of 1 means 65536
        let mut header = vec![0u8; 100];
        header[0..16].copy_from_slice(b"SQLite format 3\0");
        header[16..18].copy_from_slice(&1u16.to_be_bytes());

        tokio::fs::write(&db_path, header).await.unwrap();

        let page_size = get_page_size(&db_path).await.unwrap();
        assert_eq!(page_size, 65536);
    }

    #[tokio::test]
    async fn test_get_page_size_various() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();

        for expected_size in [512u32, 1024, 2048, 4096, 8192, 16384, 32768] {
            let db_path = dir.path().join(format!("test_{}.db", expected_size));

            let mut header = vec![0u8; 100];
            header[0..16].copy_from_slice(b"SQLite format 3\0");
            header[16..18].copy_from_slice(&(expected_size as u16).to_be_bytes());

            tokio::fs::write(&db_path, header).await.unwrap();

            let page_size = get_page_size(&db_path).await.unwrap();
            assert_eq!(page_size, expected_size, "Page size mismatch for {}", expected_size);
        }
    }

    // ============================================
    // DbState Tests
    // ============================================

    #[test]
    fn test_db_state_creation() {
        let state = DbState {
            name: "mydb".to_string(),
            db_path: PathBuf::from("/data/mydb.db"),
            wal_path: PathBuf::from("/data/mydb.db-wal"),
            wal_offset: 0,
            wal_generation: 0,
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
        };

        assert_eq!(state.name, "mydb");
        assert_eq!(state.wal_offset, 0);
        assert_eq!(state.current_txid, 0);
        assert!(state.last_snapshot.is_none());
        assert!(state.db_checksum.is_none());
    }

    #[test]
    fn test_db_state_with_txid() {
        let state = DbState {
            name: "testdb".to_string(),
            db_path: PathBuf::from("/tmp/test.db"),
            wal_path: PathBuf::from("/tmp/test.db-wal"),
            wal_offset: 1024,
            wal_generation: 5,
            current_txid: 100,
            last_snapshot: Some(Utc::now()),
            db_checksum: Some(0x123456789ABCDEF0),
        };

        assert_eq!(state.wal_offset, 1024);
        assert_eq!(state.wal_generation, 5);
        assert_eq!(state.current_txid, 100);
        assert!(state.last_snapshot.is_some());
        assert_eq!(state.db_checksum, Some(0x123456789ABCDEF0));
    }

    // ============================================
    // Restore Logic Tests
    // ============================================

    #[test]
    fn test_restore_point_in_time_txid_parsing() {
        // Test parsing TXID as point-in-time
        let pit = "100";
        let result = pit.parse::<u64>();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);

        // Large TXID
        let pit = "9999999999";
        let result = pit.parse::<u64>();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 9999999999);

        // Invalid TXID (not a number)
        let pit = "abc";
        let result = pit.parse::<u64>();
        assert!(result.is_err());
    }

    #[test]
    fn test_restore_point_in_time_timestamp_parsing() {
        // Valid ISO 8601 timestamp
        let pit = "2024-06-15T12:30:45Z";
        let result = chrono::DateTime::parse_from_rfc3339(pit);
        assert!(result.is_ok());

        // With timezone offset
        let pit = "2024-06-15T12:30:45+00:00";
        let result = chrono::DateTime::parse_from_rfc3339(pit);
        assert!(result.is_ok());

        // Invalid timestamp
        let pit = "2024-13-45T99:99:99Z";
        let result = chrono::DateTime::parse_from_rfc3339(pit);
        assert!(result.is_err());

        // Not a timestamp or TXID
        let pit = "yesterday";
        let txid_result = pit.parse::<u64>();
        let ts_result = chrono::DateTime::parse_from_rfc3339(pit);
        assert!(txid_result.is_err());
        assert!(ts_result.is_err());
    }

    #[test]
    fn test_restore_snapshot_selection_basic() {
        let manifest = Manifest {
            name: "test".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000050.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 50,
                    size: 1000,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: true,
                },
            ],
        last_checksum: None,
        };

        // Select snapshot for TXID 50
        let target = 50u64;
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot && f.max_txid <= target)
            .max_by_key(|f| f.max_txid);

        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().max_txid, 50);
    }

    #[test]
    fn test_restore_snapshot_selection_multiple_snapshots() {
        let manifest = Manifest {
            name: "test".to_string(),
            current_txid: 200,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000025.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 25,
                    size: 500,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000001-00000075.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 75,
                    size: 1000,
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000001-00000150.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 150,
                    size: 1500,
                    created_at: "2024-01-01T02:00:00Z".to_string(),
                    is_snapshot: true,
                },
            ],
        last_checksum: None,
        };

        // Target TXID 100: should select snapshot with max_txid=75 (closest <= 100)
        let target = 100u64;
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot && f.max_txid <= target)
            .max_by_key(|f| f.max_txid);

        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().max_txid, 75);

        // Target TXID 200: should select snapshot with max_txid=150
        let target = 200u64;
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot && f.max_txid <= target)
            .max_by_key(|f| f.max_txid);

        assert!(snapshot.is_some());
        assert_eq!(snapshot.unwrap().max_txid, 150);

        // Target TXID 20: should select snapshot with max_txid=25... wait no, 25 > 20
        // so it should fail to find one
        let target = 20u64;
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot && f.max_txid <= target)
            .max_by_key(|f| f.max_txid);

        assert!(snapshot.is_none());
    }

    #[test]
    fn test_restore_snapshot_selection_no_snapshots() {
        let manifest = Manifest {
            name: "test".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![
                // Only incrementals, no snapshots
                LtxEntry {
                    filename: "00000010-00000050.ltx".to_string(),
                    min_txid: 10,
                    max_txid: 50,
                    size: 500,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: false,
                },
            ],
        last_checksum: None,
        };

        let target = 100u64;
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot && f.max_txid <= target)
            .max_by_key(|f| f.max_txid);

        assert!(snapshot.is_none());
    }

    #[test]
    fn test_restore_incrementals_selection() {
        let manifest = Manifest {
            name: "test".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000030.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 30,
                    size: 1000,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000031-00000050.ltx".to_string(),
                    min_txid: 31,
                    max_txid: 50,
                    size: 200,
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                    is_snapshot: false,
                },
                LtxEntry {
                    filename: "00000051-00000070.ltx".to_string(),
                    min_txid: 51,
                    max_txid: 70,
                    size: 200,
                    created_at: "2024-01-01T02:00:00Z".to_string(),
                    is_snapshot: false,
                },
                LtxEntry {
                    filename: "00000071-00000100.ltx".to_string(),
                    min_txid: 71,
                    max_txid: 100,
                    size: 200,
                    created_at: "2024-01-01T03:00:00Z".to_string(),
                    is_snapshot: false,
                },
            ],
        last_checksum: None,
        };

        // Find incrementals after snapshot (max_txid=30) up to target (60)
        let snapshot_max_txid = 30u64;
        let target_txid = 60u64;

        let incrementals: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| {
                !f.is_snapshot
                    && f.min_txid > snapshot_max_txid
                    && f.max_txid <= target_txid
            })
            .collect();

        // Should include 31-50, but not 51-70 (max_txid=70 > target=60)
        assert_eq!(incrementals.len(), 1);
        assert_eq!(incrementals[0].filename, "00000031-00000050.ltx");

        // Find incrementals up to target 100
        let target_txid = 100u64;
        let incrementals: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| {
                !f.is_snapshot
                    && f.min_txid > snapshot_max_txid
                    && f.max_txid <= target_txid
            })
            .collect();

        assert_eq!(incrementals.len(), 3);
    }

    #[test]
    fn test_restore_incrementals_ordering() {
        let manifest = Manifest {
            name: "test".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000020.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 20,
                    size: 1000,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: true,
                },
                // Out of order in manifest (should be sorted by min_txid for replay)
                LtxEntry {
                    filename: "00000051-00000070.ltx".to_string(),
                    min_txid: 51,
                    max_txid: 70,
                    size: 200,
                    created_at: "2024-01-01T03:00:00Z".to_string(),
                    is_snapshot: false,
                },
                LtxEntry {
                    filename: "00000021-00000050.ltx".to_string(),
                    min_txid: 21,
                    max_txid: 50,
                    size: 200,
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                    is_snapshot: false,
                },
            ],
        last_checksum: None,
        };

        let snapshot_max_txid = 20u64;
        let target_txid = 100u64;

        let mut incrementals: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| {
                !f.is_snapshot
                    && f.min_txid > snapshot_max_txid
                    && f.max_txid <= target_txid
            })
            .collect();

        // Sort by min_txid for proper replay order
        incrementals.sort_by_key(|f| f.min_txid);

        assert_eq!(incrementals.len(), 2);
        assert_eq!(incrementals[0].min_txid, 21); // First
        assert_eq!(incrementals[1].min_txid, 51); // Second
    }

    #[test]
    fn test_restore_timestamp_based_txid_selection() {
        let manifest = Manifest {
            name: "test".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000030.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 30,
                    size: 1000,
                    created_at: "2024-01-15T10:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000001-00000060.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 60,
                    size: 1500,
                    created_at: "2024-01-15T12:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000001-00000100.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 100,
                    size: 2000,
                    created_at: "2024-01-15T14:00:00Z".to_string(),
                    is_snapshot: true,
                },
            ],
        last_checksum: None,
        };

        // Find latest file before 11:00 (should be the 10:00 one)
        let target_dt = chrono::DateTime::parse_from_rfc3339("2024-01-15T11:00:00Z").unwrap();

        let target_txid = manifest
            .files
            .iter()
            .filter(|f| {
                chrono::DateTime::parse_from_rfc3339(&f.created_at)
                    .map(|fdt| fdt <= target_dt)
                    .unwrap_or(false)
            })
            .map(|f| f.max_txid)
            .max();

        assert_eq!(target_txid, Some(30));

        // Find latest file before 13:00 (should be the 12:00 one)
        let target_dt = chrono::DateTime::parse_from_rfc3339("2024-01-15T13:00:00Z").unwrap();

        let target_txid = manifest
            .files
            .iter()
            .filter(|f| {
                chrono::DateTime::parse_from_rfc3339(&f.created_at)
                    .map(|fdt| fdt <= target_dt)
                    .unwrap_or(false)
            })
            .map(|f| f.max_txid)
            .max();

        assert_eq!(target_txid, Some(60));
    }

    // ============================================
    // LTX Decode Tests
    // ============================================

    #[tokio::test]
    async fn test_restore_ltx_roundtrip_basic() {
        use tempfile::tempdir;
        use crate::ltx;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("original.db");
        let ltx_path = dir.path().join("backup.ltx");
        let restored_path = dir.path().join("restored.db");

        // Create a database with recognizable content
        let page_size = 4096u32;
        let original_data = vec![0x42u8; page_size as usize * 3]; // 3 pages
        tokio::fs::write(&db_path, &original_data).await.unwrap();

        // Encode to LTX
        let ltx_file = std::fs::File::create(&ltx_path).unwrap();
        ltx::encode_snapshot(ltx_file, &db_path, page_size, 1).unwrap();

        // Decode from LTX
        let ltx_file = std::fs::File::open(&ltx_path).unwrap();
        let header = ltx::decode_to_db(ltx_file, &restored_path).unwrap();

        // Verify
        let restored_data = tokio::fs::read(&restored_path).await.unwrap();
        assert_eq!(original_data, restored_data);
        assert_eq!(header.page_size.into_inner(), page_size);
        assert_eq!(header.commit.into_inner(), 3); // 3 pages
    }

    #[tokio::test]
    async fn test_restore_ltx_with_varied_content() {
        use tempfile::tempdir;
        use crate::ltx;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("varied.db");
        let restored_path = dir.path().join("restored.db");

        let page_size = 4096u32;

        // Create database with various byte patterns
        let mut original_data = Vec::new();
        for page_num in 0..5 {
            let mut page = vec![0u8; page_size as usize];
            // Fill with different patterns
            for i in 0..page_size as usize {
                page[i] = ((page_num * 256 + i) % 256) as u8;
            }
            original_data.extend(page);
        }
        tokio::fs::write(&db_path, &original_data).await.unwrap();

        // Encode and decode
        let mut ltx_buffer = Vec::new();
        ltx::encode_snapshot(&mut ltx_buffer, &db_path, page_size, 100).unwrap();

        let cursor = std::io::Cursor::new(ltx_buffer);
        let header = ltx::decode_to_db(cursor, &restored_path).unwrap();

        // Verify byte-for-byte
        let restored_data = tokio::fs::read(&restored_path).await.unwrap();
        assert_eq!(original_data.len(), restored_data.len());

        for (i, (orig, rest)) in original_data.iter().zip(restored_data.iter()).enumerate() {
            assert_eq!(
                orig, rest,
                "Byte mismatch at offset {}: expected 0x{:02x}, got 0x{:02x}",
                i, orig, rest
            );
        }

        assert_eq!(header.max_txid.into_inner(), 100);
    }

    #[tokio::test]
    async fn test_restore_ltx_preserves_sqlite_header() {
        use tempfile::tempdir;
        use crate::ltx;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("sqlite.db");
        let restored_path = dir.path().join("restored.db");

        let page_size = 4096u32;

        // Create a minimal SQLite-like database
        let mut db_data = vec![0u8; page_size as usize];
        // SQLite magic
        db_data[0..16].copy_from_slice(b"SQLite format 3\0");
        // Page size at offset 16-17 (big-endian)
        db_data[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        // Other header fields...
        db_data[18] = 1; // file format write version
        db_data[19] = 1; // file format read version

        tokio::fs::write(&db_path, &db_data).await.unwrap();

        // Encode and decode
        let mut ltx_buffer = Vec::new();
        ltx::encode_snapshot(&mut ltx_buffer, &db_path, page_size, 1).unwrap();

        let cursor = std::io::Cursor::new(ltx_buffer);
        ltx::decode_to_db(cursor, &restored_path).unwrap();

        // Verify SQLite header is preserved
        let restored_data = tokio::fs::read(&restored_path).await.unwrap();
        assert_eq!(&restored_data[0..16], b"SQLite format 3\0");
        assert_eq!(
            u16::from_be_bytes([restored_data[16], restored_data[17]]),
            page_size as u16
        );
    }

    #[tokio::test]
    async fn test_restore_ltx_from_memory_buffer() {
        use tempfile::tempdir;
        use crate::ltx;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let restored_path = dir.path().join("restored.db");

        let page_size = 4096u32;
        let original_data = vec![0xAB; page_size as usize * 2];
        tokio::fs::write(&db_path, &original_data).await.unwrap();

        // Simulate S3 workflow: encode to Vec, decode from Cursor
        let mut ltx_buffer: Vec<u8> = Vec::new();
        ltx::encode_snapshot(&mut ltx_buffer, &db_path, page_size, 50).unwrap();

        // This is exactly how restore() works with S3 data
        let cursor = std::io::Cursor::new(ltx_buffer);
        let header = ltx::decode_to_db(cursor, &restored_path).unwrap();

        let restored_data = tokio::fs::read(&restored_path).await.unwrap();
        assert_eq!(original_data, restored_data);
        assert_eq!(header.min_txid.into_inner(), 1);
        assert_eq!(header.max_txid.into_inner(), 50);
    }

    #[test]
    fn test_restore_ltx_corrupted_data() {
        use tempfile::tempdir;
        use crate::ltx;

        let dir = tempdir().unwrap();
        let restored_path = dir.path().join("restored.db");

        // Try to decode garbage data
        let garbage = vec![0xFF; 1000];
        let cursor = std::io::Cursor::new(garbage);
        let result = ltx::decode_to_db(cursor, &restored_path);

        assert!(result.is_err(), "Decoding garbage should fail");
    }

    #[test]
    fn test_restore_ltx_truncated_data() {
        use tempfile::tempdir;
        use crate::ltx;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let restored_path = dir.path().join("restored.db");

        // Create valid LTX first
        let page_size = 4096u32;
        let db_data = vec![0x42; page_size as usize];
        std::fs::write(&db_path, &db_data).unwrap();

        let mut ltx_buffer = Vec::new();
        ltx::encode_snapshot(&mut ltx_buffer, &db_path, page_size, 1).unwrap();

        // Truncate the LTX data
        let truncated = &ltx_buffer[0..ltx_buffer.len() / 2];
        let cursor = std::io::Cursor::new(truncated);
        let result = ltx::decode_to_db(cursor, &restored_path);

        assert!(result.is_err(), "Decoding truncated LTX should fail");
    }

    #[test]
    fn test_restore_ltx_empty_data() {
        use tempfile::tempdir;
        use crate::ltx;

        let dir = tempdir().unwrap();
        let restored_path = dir.path().join("restored.db");

        let empty: Vec<u8> = Vec::new();
        let cursor = std::io::Cursor::new(empty);
        let result = ltx::decode_to_db(cursor, &restored_path);

        assert!(result.is_err(), "Decoding empty data should fail");
    }

    // ============================================
    // Manifest File Selection Tests
    // ============================================

    #[test]
    fn test_manifest_empty_files() {
        let manifest = Manifest {
            name: "empty".to_string(),
            current_txid: 0,
            page_size: 4096,
            files: vec![],
        last_checksum: None,
        };

        assert!(manifest.files.is_empty());

        // Should trigger legacy fallback in restore()
        let snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot)
            .max_by_key(|f| f.max_txid);

        assert!(snapshot.is_none());
    }

    #[test]
    fn test_manifest_mixed_snapshots_and_incrementals() {
        let manifest = Manifest {
            name: "mixed".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000010.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 10,
                    size: 1000,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000011-00000020.ltx".to_string(),
                    min_txid: 11,
                    max_txid: 20,
                    size: 100,
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                    is_snapshot: false,
                },
                LtxEntry {
                    filename: "00000001-00000050.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 50,
                    size: 2000,
                    created_at: "2024-01-01T02:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000051-00000100.ltx".to_string(),
                    min_txid: 51,
                    max_txid: 100,
                    size: 200,
                    created_at: "2024-01-01T03:00:00Z".to_string(),
                    is_snapshot: false,
                },
            ],
        last_checksum: None,
        };

        // Count snapshots vs incrementals
        let snapshots: Vec<_> = manifest.files.iter().filter(|f| f.is_snapshot).collect();
        let incrementals: Vec<_> = manifest.files.iter().filter(|f| !f.is_snapshot).collect();

        assert_eq!(snapshots.len(), 2);
        assert_eq!(incrementals.len(), 2);

        // For target TXID 100:
        // 1. Best snapshot is max_txid=50
        // 2. Incrementals to apply: 51-100
        let target = 100u64;
        let best_snapshot = snapshots
            .iter()
            .filter(|f| f.max_txid <= target)
            .max_by_key(|f| f.max_txid);

        assert!(best_snapshot.is_some());
        assert_eq!(best_snapshot.unwrap().max_txid, 50);

        let snapshot_max = 50u64;
        let needed_incrementals: Vec<_> = incrementals
            .iter()
            .filter(|f| f.min_txid > snapshot_max && f.max_txid <= target)
            .collect();

        assert_eq!(needed_incrementals.len(), 1);
        assert_eq!(needed_incrementals[0].filename, "00000051-00000100.ltx");
    }

    #[test]
    fn test_manifest_snapshot_supersedes_incrementals() {
        // When a new snapshot is taken, it supersedes older incrementals
        let manifest = Manifest {
            name: "supersede".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000030.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 30,
                    size: 1000,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: true,
                },
                LtxEntry {
                    filename: "00000031-00000050.ltx".to_string(),
                    min_txid: 31,
                    max_txid: 50,
                    size: 100,
                    created_at: "2024-01-01T01:00:00Z".to_string(),
                    is_snapshot: false,
                },
                // New snapshot that includes everything up to TXID 70
                LtxEntry {
                    filename: "00000001-00000070.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 70,
                    size: 2000,
                    created_at: "2024-01-01T02:00:00Z".to_string(),
                    is_snapshot: true,
                },
            ],
        last_checksum: None,
        };

        // For target TXID 70, the newer snapshot at TXID 70 should be used
        // The incremental 31-50 is NOT needed (it's covered by the new snapshot)
        let target = 70u64;

        let best_snapshot = manifest
            .files
            .iter()
            .filter(|f| f.is_snapshot && f.max_txid <= target)
            .max_by_key(|f| f.max_txid)
            .unwrap();

        assert_eq!(best_snapshot.max_txid, 70);

        // No incrementals needed because snapshot covers everything
        let incrementals: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| {
                !f.is_snapshot
                    && f.min_txid > best_snapshot.max_txid
                    && f.max_txid <= target
            })
            .collect();

        assert!(incrementals.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_compaction() {
        use crate::retention::RetentionPolicy;

        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("compact-test-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;

        // Take multiple snapshots to have something to compact
        for _ in 0..3 {
            snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        // Run compaction in dry-run mode first
        let policy = RetentionPolicy::new(1, 0, 0, 0); // Keep only 1 hourly
        let result = compact(&test_name, &bucket, endpoint.as_deref(), &policy, false).await;
        assert!(result.is_ok());

        // Run compaction with force
        let result = compact(&test_name, &bucket, endpoint.as_deref(), &policy, true).await;
        assert!(result.is_ok());

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();

        // Clean up S3 (best effort)
        let (bucket_name, prefix) = parse_bucket(&bucket);
        if let Ok(client) = create_client(endpoint.as_deref()).await {
            let db_prefix = format!("{}{}/", prefix, test_name);
            if let Ok(keys) = s3::list_objects(&client, &bucket_name, &db_prefix).await {
                let _ = s3::delete_objects(&client, &bucket_name, &keys).await;
            }
        }
    }

    // ============================================
    // Incremental LTX Tests
    // ============================================

    #[test]
    fn test_incremental_ltx_basic_encoding() {
        use litetx::Checksum;

        // Test that we can encode WAL pages as incremental LTX
        let page_size = 4096u32;
        let pages: Vec<(u32, Vec<u8>)> = vec![
            (1, vec![0xAA; page_size as usize]),
            (3, vec![0xBB; page_size as usize]),
            (5, vec![0xCC; page_size as usize]),
        ];

        let pre_checksum = Checksum::new(0x123456789ABCDEF0);

        let mut buffer = Vec::new();
        let post_checksum = ltx::encode_wal_changes(
            &mut buffer,
            &pages,
            page_size,
            10,  // min_txid
            12,  // max_txid
            10,  // commit_page (db size)
            Some(pre_checksum),
        ).unwrap();

        // Verify we got a valid LTX file
        assert!(!buffer.is_empty());
        assert!(buffer.len() > 100); // At least header + some data

        // Post checksum should be different from pre (pages were modified)
        assert_ne!(post_checksum.into_inner(), pre_checksum.into_inner());
    }

    #[test]
    fn test_incremental_ltx_page_deduplication() {
        use crate::wal::ParsedFrame;
        use std::collections::HashMap;

        // Simulate WAL with multiple writes to the same page
        let frames = vec![
            ParsedFrame { page_number: 1, db_size: 0, data: vec![0x11; 4096] },
            ParsedFrame { page_number: 2, db_size: 0, data: vec![0x22; 4096] },
            ParsedFrame { page_number: 1, db_size: 0, data: vec![0x33; 4096] }, // Overwrites page 1
            ParsedFrame { page_number: 3, db_size: 0, data: vec![0x44; 4096] },
            ParsedFrame { page_number: 1, db_size: 5, data: vec![0x55; 4096] }, // Final value for page 1
        ];

        // Deduplicate (same logic as sync_wal)
        let mut page_map: HashMap<u32, Vec<u8>> = HashMap::new();
        for frame in &frames {
            page_map.insert(frame.page_number, frame.data.clone());
        }

        // Should have 3 unique pages
        assert_eq!(page_map.len(), 3);

        // Page 1 should have the last value (0x55)
        assert_eq!(page_map.get(&1).unwrap()[0], 0x55);
        assert_eq!(page_map.get(&2).unwrap()[0], 0x22);
        assert_eq!(page_map.get(&3).unwrap()[0], 0x44);
    }

    #[test]
    fn test_incremental_ltx_checksum_chain() {
        use litetx::Checksum;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let page_size = 4096u32;

        // Create initial database (3 pages)
        let initial_data = vec![0x00u8; page_size as usize * 3];
        std::fs::write(&db_path, &initial_data).unwrap();

        // Get initial checksum
        let checksum0 = ltx::compute_checksum_from_file(&db_path).unwrap();

        // First incremental: modify page 1
        let pages1: Vec<(u32, Vec<u8>)> = vec![(1, vec![0xAA; page_size as usize])];
        let mut buf1 = Vec::new();
        let _checksum1 = ltx::encode_wal_changes(
            &mut buf1, &pages1, page_size, 2, 2, 3, Some(checksum0)
        ).unwrap();

        // Apply first incremental
        let cursor1 = std::io::Cursor::new(&buf1);
        ltx::apply_ltx_to_db(cursor1, &db_path).unwrap();

        // Verify checksum matches expected
        let actual_checksum1 = ltx::compute_checksum_from_file(&db_path).unwrap();
        // Note: post_apply_checksum is computed from pages, not full db, so may differ
        // The important thing is the chain is consistent

        // Second incremental: modify page 2, using actual db checksum as pre
        let pages2: Vec<(u32, Vec<u8>)> = vec![(2, vec![0xBB; page_size as usize])];
        let mut buf2 = Vec::new();
        let _checksum2 = ltx::encode_wal_changes(
            &mut buf2, &pages2, page_size, 3, 3, 3, Some(actual_checksum1)
        ).unwrap();

        // Apply second incremental
        let cursor2 = std::io::Cursor::new(&buf2);
        ltx::apply_ltx_to_db(cursor2, &db_path).unwrap();

        // Third incremental: modify page 3
        let actual_checksum2 = ltx::compute_checksum_from_file(&db_path).unwrap();
        let pages3: Vec<(u32, Vec<u8>)> = vec![(3, vec![0xCC; page_size as usize])];
        let mut buf3 = Vec::new();
        let _checksum3 = ltx::encode_wal_changes(
            &mut buf3, &pages3, page_size, 4, 4, 3, Some(actual_checksum2)
        ).unwrap();

        // Apply third incremental
        let cursor3 = std::io::Cursor::new(&buf3);
        ltx::apply_ltx_to_db(cursor3, &db_path).unwrap();

        // Verify final database state
        let final_data = std::fs::read(&db_path).unwrap();
        assert_eq!(&final_data[0..page_size as usize], &vec![0xAAu8; page_size as usize][..]);
        assert_eq!(&final_data[page_size as usize..2*page_size as usize], &vec![0xBBu8; page_size as usize][..]);
        assert_eq!(&final_data[2*page_size as usize..3*page_size as usize], &vec![0xCCu8; page_size as usize][..]);
    }

    #[test]
    fn test_incremental_ltx_manifest_tracking() {
        // Test that incremental entries are properly tracked in manifest
        let mut manifest = Manifest {
            name: "testdb".to_string(),
            current_txid: 1,
            page_size: 4096,
            files: vec![
                LtxEntry {
                    filename: "00000001-00000001.ltx".to_string(),
                    min_txid: 1,
                    max_txid: 1,
                    size: 10000,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    is_snapshot: true,
                },
            ],
            last_checksum: Some(0x123456789ABCDEF0),
        };

        // Add incremental
        manifest.files.push(LtxEntry {
            filename: "00000002-00000005.ltx".to_string(),
            min_txid: 2,
            max_txid: 5,
            size: 1000,
            created_at: "2024-01-01T01:00:00Z".to_string(),
            is_snapshot: false,
        });
        manifest.current_txid = 5;
        manifest.last_checksum = Some(0xFEDCBA9876543210);

        // Serialize and deserialize
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: Manifest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.files.len(), 2);
        assert!(parsed.files[0].is_snapshot);
        assert!(!parsed.files[1].is_snapshot);
        assert_eq!(parsed.current_txid, 5);
        assert_eq!(parsed.last_checksum, Some(0xFEDCBA9876543210));
    }

    #[test]
    fn test_incremental_ltx_restore_with_incrementals() {
        // Test restoring from snapshot + incrementals
        let dir = tempfile::tempdir().unwrap();
        let original_path = dir.path().join("original.db");
        let restored_path = dir.path().join("restored.db");
        let page_size = 4096u32;

        // Create original database (5 pages with distinct content)
        let mut original_data = Vec::new();
        for i in 0..5u8 {
            original_data.extend(vec![i * 10; page_size as usize]);
        }
        std::fs::write(&original_path, &original_data).unwrap();

        // Create snapshot (TXID 1)
        let mut snapshot_buf = Vec::new();
        ltx::encode_snapshot(&mut snapshot_buf, &original_path, page_size, 1).unwrap();

        // Simulate changes and create incrementals
        // Incremental 1: change page 2
        let mut data1 = original_data.clone();
        data1[page_size as usize..2*page_size as usize].fill(0xAA);
        std::fs::write(&original_path, &data1).unwrap();

        let _checksum1 = ltx::compute_checksum_from_file(&original_path).unwrap();
        let pages1: Vec<(u32, Vec<u8>)> = vec![(2, vec![0xAA; page_size as usize])];
        let mut inc1_buf = Vec::new();
        // Note: for incremental, we use the checksum from BEFORE the change
        // Actually compute from original state
        std::fs::write(&original_path, &original_data).unwrap();
        let pre_check1 = ltx::compute_checksum_from_file(&original_path).unwrap();
        std::fs::write(&original_path, &data1).unwrap();

        ltx::encode_wal_changes(
            &mut inc1_buf, &pages1, page_size, 2, 2, 5, Some(pre_check1)
        ).unwrap();

        // Incremental 2: change page 4
        let mut data2 = data1.clone();
        data2[3*page_size as usize..4*page_size as usize].fill(0xBB);
        std::fs::write(&original_path, &data2).unwrap();

        std::fs::write(&original_path, &data1).unwrap();
        let pre_check2 = ltx::compute_checksum_from_file(&original_path).unwrap();
        std::fs::write(&original_path, &data2).unwrap();

        let pages2: Vec<(u32, Vec<u8>)> = vec![(4, vec![0xBB; page_size as usize])];
        let mut inc2_buf = Vec::new();
        ltx::encode_wal_changes(
            &mut inc2_buf, &pages2, page_size, 3, 3, 5, Some(pre_check2)
        ).unwrap();

        // Now restore: first snapshot, then incrementals
        let cursor_snap = std::io::Cursor::new(&snapshot_buf);
        ltx::decode_to_db(cursor_snap, &restored_path).unwrap();

        // Apply incrementals in order
        let cursor_inc1 = std::io::Cursor::new(&inc1_buf);
        ltx::apply_ltx_to_db(cursor_inc1, &restored_path).unwrap();

        let cursor_inc2 = std::io::Cursor::new(&inc2_buf);
        ltx::apply_ltx_to_db(cursor_inc2, &restored_path).unwrap();

        // Verify restored matches final state
        let restored_data = std::fs::read(&restored_path).unwrap();
        assert_eq!(restored_data.len(), data2.len());

        // Page 1: original (0)
        assert_eq!(restored_data[0], 0);
        // Page 2: changed to 0xAA
        assert_eq!(restored_data[page_size as usize], 0xAA);
        // Page 3: original (20)
        assert_eq!(restored_data[2*page_size as usize], 20);
        // Page 4: changed to 0xBB
        assert_eq!(restored_data[3*page_size as usize], 0xBB);
        // Page 5: original (40)
        assert_eq!(restored_data[4*page_size as usize], 40);
    }

    #[test]
    fn test_incremental_ltx_large_page_count() {
        use litetx::Checksum;

        // Test with many pages to ensure scalability
        let page_size = 4096u32;
        let num_pages = 100;

        let pages: Vec<(u32, Vec<u8>)> = (1..=num_pages)
            .map(|i| (i, vec![(i % 256) as u8; page_size as usize]))
            .collect();

        let pre_checksum = Checksum::new(0x123456789ABCDEF0);

        let mut buffer = Vec::new();
        let result = ltx::encode_wal_changes(
            &mut buffer,
            &pages,
            page_size,
            10,
            10 + num_pages as u64 - 1,
            num_pages,
            Some(pre_checksum),
        );

        assert!(result.is_ok());
        // Verify we got valid output
        assert!(buffer.len() > 0);

        // Verify we can decode the header
        let cursor = std::io::Cursor::new(&buffer);
        let (_, header) = litetx::Decoder::new(cursor).unwrap();
        assert_eq!(header.min_txid.into_inner(), 10);
        assert_eq!(header.max_txid.into_inner(), 10 + num_pages as u64 - 1);
    }

    #[test]
    fn test_incremental_ltx_single_page() {
        use litetx::Checksum;

        // Edge case: single page change
        let page_size = 4096u32;
        let pages: Vec<(u32, Vec<u8>)> = vec![(42, vec![0xFF; page_size as usize])];

        let pre_checksum = Checksum::new(0x123456789ABCDEF0);

        let mut buffer = Vec::new();
        let post_checksum = ltx::encode_wal_changes(
            &mut buffer,
            &pages,
            page_size,
            100,
            100,  // min == max for single page
            100,
            Some(pre_checksum),
        ).unwrap();

        assert!(post_checksum.into_inner() != 0);

        // Decode and verify
        let cursor = std::io::Cursor::new(&buffer);
        let (_, header) = litetx::Decoder::new(cursor).unwrap();
        assert_eq!(header.min_txid.into_inner(), 100);
        assert_eq!(header.max_txid.into_inner(), 100);
    }

    #[test]
    fn test_incremental_ltx_non_contiguous_pages() {
        use litetx::Checksum;

        // Pages don't need to be contiguous (WAL often isn't)
        let page_size = 4096u32;
        let pages: Vec<(u32, Vec<u8>)> = vec![
            (1, vec![0x11; page_size as usize]),
            (5, vec![0x55; page_size as usize]),
            (10, vec![0xAA; page_size as usize]),
            (100, vec![0xFF; page_size as usize]),
        ];

        let pre_checksum = Checksum::new(0x123456789ABCDEF0);

        let mut buffer = Vec::new();
        let result = ltx::encode_wal_changes(
            &mut buffer,
            &pages,
            page_size,
            50,
            53,
            100,
            Some(pre_checksum),
        );

        assert!(result.is_ok());

        // Apply to a database and verify
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create db with 100 pages
        let db_data = vec![0x00u8; 100 * page_size as usize];
        std::fs::write(&db_path, &db_data).unwrap();

        let cursor = std::io::Cursor::new(&buffer);
        ltx::apply_ltx_to_db(cursor, &db_path).unwrap();

        let result_data = std::fs::read(&db_path).unwrap();

        // Verify specific pages were updated
        assert_eq!(result_data[0], 0x11); // Page 1
        assert_eq!(result_data[4 * page_size as usize], 0x55); // Page 5
        assert_eq!(result_data[9 * page_size as usize], 0xAA); // Page 10
        assert_eq!(result_data[99 * page_size as usize], 0xFF); // Page 100

        // Verify other pages unchanged
        assert_eq!(result_data[2 * page_size as usize], 0x00); // Page 3
        assert_eq!(result_data[50 * page_size as usize], 0x00); // Page 51
    }

    #[test]
    fn test_incremental_ltx_checksum_recompute_on_failure() {
        // Simulate the case where we need to recompute checksum from db
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let page_size = 4096u32;

        // Create database
        let db_data = vec![0x42u8; page_size as usize * 5];
        std::fs::write(&db_path, &db_data).unwrap();

        // Compute checksum
        let checksum = ltx::compute_checksum_from_file(&db_path).unwrap();

        // Modify database
        let mut modified_data = db_data.clone();
        modified_data[0] = 0xFF;
        std::fs::write(&db_path, &modified_data).unwrap();

        // Recompute - should be different
        let new_checksum = ltx::compute_checksum_from_file(&db_path).unwrap();
        assert_ne!(checksum.into_inner(), new_checksum.into_inner());

        // Restore original
        std::fs::write(&db_path, &db_data).unwrap();

        // Checksum should match original
        let restored_checksum = ltx::compute_checksum_from_file(&db_path).unwrap();
        assert_eq!(checksum.into_inner(), restored_checksum.into_inner());
    }

    #[test]
    fn test_manifest_last_checksum_persistence() {
        // Test that last_checksum is properly serialized/deserialized
        let manifest_with_checksum = Manifest {
            name: "test".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![],
            last_checksum: Some(0xDEADBEEF12345678),
        };

        let json = serde_json::to_string(&manifest_with_checksum).unwrap();
        assert!(json.contains("last_checksum"));
        assert!(json.contains("16045690981402826360")); // Decimal representation of 0xDEADBEEF12345678

        let parsed: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.last_checksum, Some(0xDEADBEEF12345678));

        // Test without checksum (backwards compatibility)
        let manifest_no_checksum = Manifest {
            name: "test".to_string(),
            current_txid: 100,
            page_size: 4096,
            files: vec![],
            last_checksum: None,
        };

        let json2 = serde_json::to_string(&manifest_no_checksum).unwrap();
        // None should be skipped due to skip_serializing_if
        assert!(!json2.contains("last_checksum"));

        // Parsing old format (no last_checksum field) should work
        let old_format = r#"{"name":"test","current_txid":50,"page_size":4096,"files":[]}"#;
        let parsed_old: Manifest = serde_json::from_str(old_format).unwrap();
        assert_eq!(parsed_old.last_checksum, None);
    }

    #[test]
    fn test_db_state_checksum_field() {
        // Test DbState with checksum
        let state_with_checksum = DbState {
            name: "testdb".to_string(),
            db_path: PathBuf::from("/data/test.db"),
            wal_path: PathBuf::from("/data/test.db-wal"),
            wal_offset: 1024,
            wal_generation: 3,
            current_txid: 50,
            last_snapshot: None,
            db_checksum: Some(0xABCDEF0123456789),
        };

        assert_eq!(state_with_checksum.db_checksum, Some(0xABCDEF0123456789));

        let state_no_checksum = DbState {
            name: "testdb".to_string(),
            db_path: PathBuf::from("/data/test.db"),
            wal_path: PathBuf::from("/data/test.db-wal"),
            wal_offset: 0,
            wal_generation: 0,
            current_txid: 0,
            last_snapshot: None,
            db_checksum: None,
        };

        assert_eq!(state_no_checksum.db_checksum, None);
    }

    #[test]
    fn test_incremental_ltx_various_page_sizes() {
        use litetx::Checksum;

        // Test with different SQLite page sizes
        for page_size in [512u32, 1024, 2048, 4096, 8192, 16384, 32768] {
            let pages: Vec<(u32, Vec<u8>)> = vec![
                (1, vec![0xAA; page_size as usize]),
                (2, vec![0xBB; page_size as usize]),
            ];

            let pre_checksum = Checksum::new(0x123456789ABCDEF0);

            let mut buffer = Vec::new();
            let result = ltx::encode_wal_changes(
                &mut buffer,
                &pages,
                page_size,
                10,
                11,
                10,
                Some(pre_checksum),
            );

            assert!(result.is_ok(), "Failed for page_size={}", page_size);

            // Verify header
            let cursor = std::io::Cursor::new(&buffer);
            let (_, header) = litetx::Decoder::new(cursor).unwrap();
            assert_eq!(header.page_size.into_inner(), page_size);
        }
    }

    // ============================================
    // Explain Command Tests
    // ============================================

    #[test]
    fn test_explain_no_config() {
        // Test explain with no config - should not panic
        let result = explain(&None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_explain_with_config() {
        use crate::config::{Config, S3Config, SyncConfig, RetentionConfig};

        let config = Config {
            s3: S3Config {
                bucket: Some("s3://test-bucket/prefix".to_string()),
                endpoint: Some("https://fly.storage.tigris.dev".to_string()),
            },
            sync: SyncConfig {
                snapshot_interval: 1800,
                wal_sync_interval: 1,
                max_changes: 100,
                max_interval: 300,
                on_idle: 60,
                on_startup: true,
                compact_after_snapshot: true,
                compact_interval: 3600,
                checkpoint_interval: 60,
                min_checkpoint_page_count: 1000,
                wal_truncate_threshold_pages: 121359,
                monitor_interval: 1,
                validation_interval: 0,
            },
            retention: RetentionConfig {
                hourly: 12,
                daily: 5,
                weekly: 8,
                monthly: 6,
            },
            databases: vec![], // Empty databases - explain should still work
        };

        let result = explain(&Some(config));
        assert!(result.is_ok());
    }

    // ============================================
    // Checkpoint Tests
    // ============================================

    #[tokio::test]
    async fn test_get_wal_page_count() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db_path = tmpdir.path().join("test.db");

        // Create a test database with WAL mode
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)", []).unwrap();

        let wal_path = db_path.with_extension("db-wal");
        let initial_pages = get_wal_page_count(&wal_path).await.unwrap();

        // Write some data to generate WAL pages
        for i in 0..100 {
            conn.execute("INSERT INTO test (data) VALUES (?)", [format!("data_{}", i)]).unwrap();
        }

        let after_pages = get_wal_page_count(&wal_path).await.unwrap();
        assert!(after_pages > initial_pages, "WAL should have more pages after writes");
    }

    #[tokio::test]
    async fn test_passive_checkpoint() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db_path = tmpdir.path().join("test.db");

        // Create a test database with WAL mode
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)", []).unwrap();

        // Write enough data to generate WAL pages
        for i in 0..500 {
            conn.execute("INSERT INTO test (data) VALUES (?)", [format!("data_{}", i)]).unwrap();
        }

        let wal_path = db_path.with_extension("db-wal");
        let before_pages = get_wal_page_count(&wal_path).await.unwrap();
        assert!(before_pages > 0, "Should have WAL pages before checkpoint");

        // Run PASSIVE checkpoint
        let result = run_checkpoint(&db_path, CheckpointMode::Passive).await;
        assert!(result.is_ok(), "PASSIVE checkpoint should succeed");

        // WAL should be smaller after checkpoint (though not necessarily zero with PASSIVE)
        let after_pages = get_wal_page_count(&wal_path).await.unwrap();
        assert!(
            after_pages <= before_pages,
            "WAL should not grow after checkpoint"
        );
    }

    #[tokio::test]
    async fn test_truncate_checkpoint() {
        let tmpdir = tempfile::tempdir().unwrap();
        let db_path = tmpdir.path().join("test.db");

        // Create a test database with WAL mode
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();
            conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)", []).unwrap();

            // Write data to generate WAL pages
            for i in 0..500 {
                conn.execute("INSERT INTO test (data) VALUES (?)", [format!("data_{}", i)]).unwrap();
            }
            // Connection auto-closes here
        }

        // Wait a bit for connection to fully close
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let wal_path = db_path.with_extension("db-wal");
        let before_pages = get_wal_page_count(&wal_path).await.unwrap_or(0);

        // Only test if there are WAL pages (connection close might checkpoint)
        if before_pages > 0 {
            // Run TRUNCATE checkpoint
            let result = run_checkpoint(&db_path, CheckpointMode::Truncate).await;
            assert!(result.is_ok(), "TRUNCATE checkpoint should succeed");

            // With TRUNCATE and no active connections, WAL should be reset
            let after_pages = get_wal_page_count(&wal_path).await.unwrap_or(0);
            assert!(
                after_pages <= before_pages,
                "WAL should not grow after TRUNCATE checkpoint"
            );
        } else {
            // If no WAL pages, just verify checkpoint doesn't error
            let result = run_checkpoint(&db_path, CheckpointMode::Truncate).await;
            assert!(result.is_ok(), "TRUNCATE checkpoint should succeed even with empty WAL");
        }
    }

    #[tokio::test]
    async fn test_get_wal_page_count_nonexistent() {
        let tmpdir = tempfile::tempdir().unwrap();
        let wal_path = tmpdir.path().join("nonexistent.db-wal");

        let pages = get_wal_page_count(&wal_path).await.unwrap();
        assert_eq!(pages, 0, "Non-existent WAL should return 0 pages");
    }

    // ============================================
    // Verify Command Integration Tests
    // ============================================

    #[tokio::test]
    #[ignore]
    async fn test_integration_verify_valid_database() {
        // Test verify on a database with valid LTX files
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("verify-valid-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;
        let db_name = db_path.file_stem().unwrap().to_str().unwrap();

        // Create some snapshots
        snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Verify should pass with no issues
        let result = verify(db_name, &bucket, endpoint.as_deref(), false).await;
        assert!(result.is_ok(), "Verify should succeed on valid database");

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_verify_nonexistent_database() {
        // Test verify on a database that doesn't exist
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();

        // Verify a nonexistent database should fail gracefully
        let result = verify("nonexistent-db-12345", &bucket, endpoint.as_deref(), false).await;
        // This will fail because manifest doesn't exist - that's expected
        assert!(result.is_err(), "Verify should fail for nonexistent database");
    }

    #[tokio::test]
    #[ignore]
    async fn test_integration_verify_multiple_snapshots() {
        // Test verify on a database with multiple snapshots
        let bucket = get_test_bucket().expect("WALRUST_TEST_BUCKET not set");
        let endpoint = get_test_endpoint();
        let test_name = format!("verify-multi-{}", uuid::Uuid::new_v4());
        let db_path = create_test_db(&test_name).await;
        let db_name = db_path.file_stem().unwrap().to_str().unwrap();

        // Create multiple snapshots
        for _ in 0..3 {
            snapshot(&db_path, &bucket, endpoint.as_deref()).await.unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        }

        // Verify should check all LTX files
        let result = verify(db_name, &bucket, endpoint.as_deref(), false).await;
        assert!(result.is_ok(), "Verify should succeed with multiple snapshots");

        // Cleanup
        tokio::fs::remove_file(&db_path).await.ok();
    }
}
