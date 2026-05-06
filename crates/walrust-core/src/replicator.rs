//! Multi-database WAL replicator.
//!
//! High-level API for replicating multiple SQLite databases to S3.
//! The caller adds/removes databases dynamically; the Replicator handles
//! sync scheduling, snapshots, and retry internally.
//!
//! Works both embedded (inside another process) and standalone (as a sidecar).
//!
//! ```ignore
//! let replicator = Replicator::new(storage, config);
//! replicator.add("tenant-1", Path::new("/data/tenant-1.db")).await?;
//! replicator.add("tenant-2", Path::new("/data/tenant-2.db")).await?;
//! // ... background loop syncs both every tick ...
//! replicator.remove("tenant-1").await;  // final sync, then drop
//! ```

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::RwLock;
use tokio::task::JoinSet;

use crate::sync::{self, ReplicationConfig, SyncState};
use hadb_storage::StorageBackend;

/// Per-database replication state.
struct DbState {
    state: SyncState,
    prefix: String,
}

/// Multi-database WAL replicator.
///
/// Owns a shared S3 backend and a set of databases being replicated.
/// Spawns a background task that syncs all registered databases every
/// `config.sync_interval` and takes snapshots every `config.snapshot_interval`.
///
/// **Caller responsibilities:**
/// - Set `PRAGMA wal_autocheckpoint=0` on all databases before adding them.
/// - Call `remove()` before checkpointing/closing a database.
pub struct Replicator {
    storage: Arc<dyn StorageBackend>,
    config: ReplicationConfig,
    /// S3 prefix for all databases (e.g., "wal/" or "ha-test/").
    /// Each database is stored under `{prefix}{db_name}/`.
    prefix: String,
    databases: Arc<RwLock<HashMap<String, Arc<AsyncMutex<DbState>>>>>,
}

impl Replicator {
    /// Storage backend reference.
    pub fn storage(&self) -> &Arc<dyn StorageBackend> {
        &self.storage
    }

    /// S3 key prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Create a new Replicator and start its background sync loop.
    ///
    /// `prefix` is the S3 key prefix for all databases (e.g., "wal/" or "ha-test/").
    /// Each database added via `add()` is stored under `{prefix}{db_name}/`.
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        prefix: &str,
        config: ReplicationConfig,
    ) -> Arc<Self> {
        Self::try_new(storage, prefix, config).expect("invalid walrust ReplicationConfig")
    }

    /// Fallible constructor for callers that want configuration errors instead of panic.
    pub fn try_new(
        storage: Arc<dyn StorageBackend>,
        prefix: &str,
        config: ReplicationConfig,
    ) -> Result<Arc<Self>> {
        config.validate()?;

        let replicator = Arc::new(Self {
            storage,
            config,
            prefix: prefix.to_string(),
            databases: Arc::new(RwLock::new(HashMap::new())),
        });

        tracing::info!(
            "Replicator started (sync={}ms, snapshot={}s)",
            replicator.config.sync_interval.as_millis(),
            replicator.config.snapshot_interval.as_secs(),
        );

        let r = replicator.clone();
        tokio::spawn(async move { r.run_loop().await });

        Ok(replicator)
    }

    /// Add a database to replication.
    ///
    /// In walrust-owned mode, takes an initial snapshot (blocks until uploaded).
    /// In external-base-state mode, registers without uploading a snapshot.
    ///
    /// Returns error if initialization fails — the database is NOT added in that case.
    pub async fn add(&self, name: &str, db_path: &Path) -> Result<()> {
        self.add_with_wal_path(name, db_path, &db_path.with_extension("db-wal"))
            .await
    }

    /// Add a database to replication with an explicit WAL path.
    ///
    /// Use this when the checkpointed base file and live WAL file do not share
    /// SQLite's normal `<db>.db` / `<db>.db-wal` layout.
    pub async fn add_with_wal_path(
        &self,
        name: &str,
        db_path: &Path,
        wal_path: &Path,
    ) -> Result<()> {
        if self.config.snapshot_ownership.is_external() {
            return self
                .add_without_snapshot_with_wal_path(name, db_path, wal_path)
                .await;
        }

        let prefix = self.prefix.clone();

        // Build state and take initial snapshot OUTSIDE the map lock
        let mut state = SyncState::new_with_paths(db_path.to_path_buf(), wal_path.to_path_buf())?;
        state.name = name.to_string();

        if db_path.exists() {
            state.init_checksum()?;
            let base_change_counter = sync::change_counter_from_file(db_path).unwrap_or(0);
            state.current_seq = base_change_counter;
            state.current_txid = base_change_counter;
        }

        sync::take_snapshot_with_retry(
            self.storage.as_ref(),
            &prefix,
            &mut state,
            &self.config.retry_policy,
        )
        .await?;

        let db_state = Arc::new(AsyncMutex::new(DbState { state, prefix }));

        self.databases
            .write()
            .await
            .insert(name.to_string(), db_state);

        tracing::info!("Replicator: added '{}' ({})", name, db_path.display());
        Ok(())
    }

    /// Register a database without taking a snapshot.
    /// Use after `restore()` when the database already has the latest state
    /// from S3. Avoids uploading a redundant snapshot that could race with
    /// other nodes' changesets.
    pub async fn add_without_snapshot(&self, name: &str, db_path: &Path) -> Result<()> {
        self.add_without_snapshot_with_wal_path(name, db_path, &db_path.with_extension("db-wal"))
            .await
    }

    /// Register a database without taking a snapshot, with an explicit WAL path.
    pub async fn add_without_snapshot_with_wal_path(
        &self,
        name: &str,
        db_path: &Path,
        wal_path: &Path,
    ) -> Result<()> {
        let prefix = self.prefix.clone();

        let mut state = SyncState::new_with_paths(db_path.to_path_buf(), wal_path.to_path_buf())?;
        state.name = name.to_string();

        if db_path.exists() {
            state.init_checksum()?;
            let base_change_counter = sync::change_counter_from_file(db_path).unwrap_or(0);
            state.current_seq = base_change_counter;
            state.current_txid = base_change_counter;
        }

        // Load existing state from storage to get the correct current_seq.
        // This ensures flush() starts at the right seq (after any existing changesets).
        let state_key = format!("{}{}/state.json", prefix, name);
        if let Ok(Some(data)) = self.storage.get(&state_key).await {
            if let Ok(saved) = serde_json::from_slice::<serde_json::Value>(&data) {
                if let Some(seq) = saved.get("current_seq").and_then(|v| v.as_u64()) {
                    state.current_seq = seq;
                }
                if let Some(gen) = saved.get("wal_generation").and_then(|v| v.as_u64()) {
                    state.wal_generation = gen;
                }
                if let Some(txid) = saved.get("current_txid").and_then(|v| v.as_u64()) {
                    state.current_txid = txid;
                }
                if let Some(checksum) = saved.get("db_checksum").and_then(|v| v.as_u64()) {
                    state.db_checksum = Some(checksum);
                }
                tracing::info!(
                    "Replicator: loaded state for '{}': seq={}, gen={}, txid={}, checksum={:?}",
                    name,
                    state.current_seq,
                    state.wal_generation,
                    state.current_txid,
                    state.db_checksum,
                );
            }
        }

        let db_state = Arc::new(AsyncMutex::new(DbState { state, prefix }));

        self.databases
            .write()
            .await
            .insert(name.to_string(), db_state);

        tracing::info!(
            "Replicator: added '{}' without snapshot ({})",
            name,
            db_path.display()
        );
        Ok(())
    }

    /// Remove a database from replication.
    ///
    /// Does a final sync before removing — blocks until the sync completes
    /// (or fails). The caller should checkpoint/close the database AFTER this returns.
    pub async fn remove(&self, name: &str) {
        let entry = self.databases.write().await.remove(name);

        if let Some(db_state) = entry {
            let mut s = db_state.lock().await;
            let prefix = s.prefix.clone();
            let sync_result = if self.config.snapshot_ownership.is_external() {
                sync::sync_wal_after_external_base(self.storage.as_ref(), &prefix, &mut s.state)
                    .await
            } else {
                sync::sync_wal(self.storage.as_ref(), &prefix, &mut s.state).await
            };
            match sync_result {
                Ok(frames) if frames > 0 => {
                    tracing::info!(
                        "Replicator: final sync for '{}' captured {} frames",
                        name,
                        frames
                    );
                }
                Err(e) => {
                    tracing::error!("Replicator: final sync for '{}' failed: {}", name, e);
                }
                _ => {}
            }
            tracing::info!("Replicator: removed '{}'", name);
        }
    }

    /// Restore a database from S3.
    ///
    /// Returns `Ok(Some(txid))` if data was found and restored,
    /// `Ok(None)` if no WAL data exists for this name.
    pub async fn restore(&self, name: &str, output_path: &Path) -> Result<Option<u64>> {
        let prefix = self.prefix.clone();

        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let seq = match sync::restore(self.storage.as_ref(), &prefix, name, output_path, None).await
        {
            Ok(seq) => seq,
            Err(e) if e.to_string().contains("No snapshot found") => return Ok(None),
            Err(e) => return Err(e),
        };

        tracing::info!(
            "Replicator: restored '{}' to seq {} ({})",
            name,
            seq,
            output_path.display()
        );
        Ok(Some(seq))
    }

    /// Flush pending WAL frames for a specific database to S3.
    ///
    /// Blocks until the upload completes. Returns the number of frames
    /// flushed (0 if nothing pending).
    ///
    /// This is the same code path that the background `sync_all()` loop uses,
    /// but triggered on demand for a single named database.
    pub async fn flush(&self, name: &str) -> Result<u64> {
        let databases = self.databases.read().await;
        let db_state = databases
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Database '{}' not registered", name))?
            .clone();
        drop(databases); // Release read lock before acquiring db mutex

        let mut state = db_state.lock().await;
        let prefix = state.prefix.clone();
        let frame_count = if self.config.snapshot_ownership.is_external() {
            sync::sync_wal_after_external_base(self.storage.as_ref(), &prefix, &mut state.state)
                .await?
        } else {
            sync::sync_wal(self.storage.as_ref(), &prefix, &mut state.state).await?
        };

        Ok(frame_count)
    }

    /// Number of databases currently being replicated.
    pub async fn database_count(&self) -> usize {
        self.databases.read().await.len()
    }

    /// Check if a specific database is being replicated.
    pub async fn contains(&self, name: &str) -> bool {
        self.databases.read().await.contains_key(name)
    }

    /// Get the current sequence number for a database.
    /// Returns None if the database is not registered.
    pub async fn current_seq(&self, name: &str) -> Option<u64> {
        let databases = self.databases.read().await;
        let db_state = databases.get(name)?.clone();
        drop(databases);
        let state = db_state.lock().await;
        Some(state.state.current_seq)
    }

    // ========================================================================
    // Background loop
    // ========================================================================

    async fn run_loop(&self) {
        let mut sync_timer = tokio::time::interval(self.config.sync_interval);

        if !self.config.autonomous_snapshots {
            // WAL-only mode: sync WAL, never take autonomous snapshots.
            // Used both for lease-coordinated snapshot ownership and for
            // external-base-state mode where another layer owns checkpoints.
            loop {
                sync_timer.tick().await;
                self.sync_all().await;
            }
        } else {
            // Standalone mode: sync WAL + periodic snapshots.
            let mut snapshot_timer = tokio::time::interval(self.config.snapshot_interval);
            // Skip first snapshot tick -- databases take snapshots on add()
            snapshot_timer.tick().await;

            loop {
                tokio::select! {
                    _ = sync_timer.tick() => self.sync_all().await,
                    _ = snapshot_timer.tick() => self.snapshot_all().await,
                }
            }
        }
    }

    /// Sync all registered databases in parallel.
    async fn sync_all(&self) {
        let entries: Vec<(String, Arc<AsyncMutex<DbState>>)> = {
            let dbs = self.databases.read().await;
            if dbs.is_empty() {
                return;
            }
            dbs.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        let mut set = JoinSet::new();
        let external_base_state = self.config.snapshot_ownership.is_external();
        for (name, db_state) in entries {
            let storage = self.storage.clone();
            set.spawn(async move {
                let mut s = db_state.lock().await;
                let prefix = s.prefix.clone();
                let sync_result = if external_base_state {
                    sync::sync_wal_after_external_base(storage.as_ref(), &prefix, &mut s.state)
                        .await
                } else {
                    sync::sync_wal(storage.as_ref(), &prefix, &mut s.state).await
                };
                match sync_result {
                    Ok(frames) if frames > 0 => {
                        tracing::debug!(
                            "Replicator: synced '{}' ({} frames, seq {})",
                            name,
                            frames,
                            s.state.current_seq
                        );
                    }
                    Err(e) => {
                        tracing::error!("Replicator: sync failed for '{}': {}", name, e);
                    }
                    _ => {}
                }
            });
        }

        while set.join_next().await.is_some() {}
    }

    /// Take snapshots for all registered databases (sequential — snapshots are heavy).
    async fn snapshot_all(&self) {
        let entries: Vec<(String, Arc<AsyncMutex<DbState>>)> = {
            let dbs = self.databases.read().await;
            dbs.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        for (name, db_state) in entries {
            let mut s = db_state.lock().await;
            let prefix = s.prefix.clone();
            match sync::take_snapshot_with_retry(
                self.storage.as_ref(),
                &prefix,
                &mut s.state,
                &self.config.retry_policy,
            )
            .await
            {
                Ok(()) => {
                    tracing::info!(
                        "Replicator: snapshot for '{}' (seq {})",
                        name,
                        s.state.current_seq
                    );
                }
                Err(e) => {
                    tracing::error!("Replicator: snapshot failed for '{}': {}", name, e);
                }
            }
        }
    }
}
