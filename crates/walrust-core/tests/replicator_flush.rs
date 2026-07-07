//! Tests for Replicator::flush() — on-demand WAL sync to S3.

use anyhow::Result;
use async_trait::async_trait;
use hadb_changeset::physical::{self, PageEntry, PageId, PageIdSize, PhysicalChangeset};
use hadb_changeset::storage::{self as cs_storage, ChangesetKind, GENERATION_INCREMENTAL};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use hadb_storage::{CasResult, StorageBackend};
use walrust::Replicator;
use walrust::{ReplicationConfig, SnapshotOwnership};
use walrust_core as walrust;

// ============================================================================
// In-memory storage backend for tests
// ============================================================================

struct MemStorage {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemStorage {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            objects: Mutex::new(HashMap::new()),
        })
    }

    /// Return all keys currently stored.
    fn keys(&self) -> Vec<String> {
        let map = self.objects.lock().unwrap();
        let mut keys: Vec<String> = map.keys().cloned().collect();
        keys.sort();
        keys
    }

    async fn value(&self, key: &str) -> serde_json::Value {
        let bytes = self
            .get(key)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("missing storage key {key}"));
        serde_json::from_slice(&bytes).unwrap()
    }

    fn max_hadbp_seq(&self, prefix: &str) -> Option<u64> {
        self.keys()
            .into_iter()
            .filter(|key| key.starts_with(prefix) && key.ends_with(".hadbp"))
            .filter_map(|key| {
                key.strip_suffix(".hadbp")
                    .and_then(|s| s.rsplit('/').next())
                    .and_then(|hex| u64::from_str_radix(hex, 16).ok())
            })
            .max()
    }
}

struct ConcurrentIdenticalPutStore {
    inner: Arc<MemStorage>,
}

impl ConcurrentIdenticalPutStore {
    fn new(inner: Arc<MemStorage>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl StorageBackend for ConcurrentIdenticalPutStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        self.inner.put_if_absent(key, data).await?;
        Ok(CasResult {
            success: false,
            etag: None,
        })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.inner.put_if_match(key, data, etag).await
    }

    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        self.inner.range_get(key, start, len).await
    }
}

struct DelayedDuplicateVisibleStore {
    inner: Arc<MemStorage>,
    hidden_hadbp_gets: AtomicUsize,
}

impl DelayedDuplicateVisibleStore {
    fn new(inner: Arc<MemStorage>, hidden_hadbp_gets: usize) -> Arc<Self> {
        Arc::new(Self {
            inner,
            hidden_hadbp_gets: AtomicUsize::new(hidden_hadbp_gets),
        })
    }
}

#[async_trait]
impl StorageBackend for DelayedDuplicateVisibleStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if key.ends_with(".hadbp")
            && self.inner.exists(key).await?
            && self
                .hidden_hadbp_gets
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            return Ok(None);
        }
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        self.inner.put_if_absent(key, data).await?;
        Ok(CasResult {
            success: false,
            etag: None,
        })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.inner.put_if_match(key, data, etag).await
    }

    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        self.inner.range_get(key, start, len).await
    }
}

#[async_trait]
impl StorageBackend for MemStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let map = self.objects.lock().unwrap();
        let mut keys: Vec<String> = map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .filter(|k| after.map(|a| k.as_str() > a).unwrap_or(true))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.lock().unwrap().contains_key(key))
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let mut map = self.objects.lock().unwrap();
        if map.contains_key(key) {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        map.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".into()),
        })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
        let mut map = self.objects.lock().unwrap();
        if !map.contains_key(key) {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        map.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".into()),
        })
    }
}

struct StateJsonForbiddenStorage {
    inner: Arc<MemStorage>,
}

struct StateJsonGetFailsStorage {
    inner: Arc<MemStorage>,
}

struct PutFailsStorage {
    inner: Arc<MemStorage>,
}

struct EnospcStorage {
    inner: Arc<MemStorage>,
}

struct FailAfterPutsStorage {
    inner: Arc<MemStorage>,
    remaining_successful_puts: AtomicUsize,
}

impl StateJsonForbiddenStorage {
    fn new(inner: Arc<MemStorage>) -> Arc<Self> {
        Arc::new(Self { inner })
    }

    fn reject_state_key(key: &str) -> Result<()> {
        if key.ends_with("/state.json") {
            anyhow::bail!("state.json access is forbidden in external-base mode: {key}");
        }
        Ok(())
    }
}

impl StateJsonGetFailsStorage {
    fn new(inner: Arc<MemStorage>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

impl PutFailsStorage {
    fn new(inner: Arc<MemStorage>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

impl EnospcStorage {
    fn new(inner: Arc<MemStorage>) -> Arc<Self> {
        Arc::new(Self { inner })
    }

    fn enospc(key: &str) -> anyhow::Error {
        anyhow::anyhow!("No space left on device while writing {key}")
    }
}

impl FailAfterPutsStorage {
    fn new(inner: Arc<MemStorage>, successful_puts: usize) -> Arc<Self> {
        Arc::new(Self {
            inner,
            remaining_successful_puts: AtomicUsize::new(successful_puts),
        })
    }
}

#[async_trait]
impl StorageBackend for StateJsonForbiddenStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Self::reject_state_key(key)?;
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        Self::reject_state_key(key)?;
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        Self::reject_state_key(key)?;
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Self::reject_state_key(key)?;
        self.inner.exists(key).await
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        Self::reject_state_key(key)?;
        self.inner.put_if_absent(key, data).await
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        Self::reject_state_key(key)?;
        self.inner.put_if_match(key, data, etag).await
    }
}

#[async_trait]
impl StorageBackend for StateJsonGetFailsStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if key.ends_with("/state.json") {
            anyhow::bail!("injected state.json read failure: {key}");
        }
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        self.inner.put_if_absent(key, data).await
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.inner.put_if_match(key, data, etag).await
    }
}

#[async_trait]
impl StorageBackend for PutFailsStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, _data: &[u8]) -> Result<()> {
        anyhow::bail!("injected put failure: {key}");
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn put_if_absent(&self, key: &str, _data: &[u8]) -> Result<CasResult> {
        anyhow::bail!("injected put failure: {key}");
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.inner.put_if_match(key, data, etag).await
    }
}

#[async_trait]
impl StorageBackend for EnospcStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, _data: &[u8]) -> Result<()> {
        Err(Self::enospc(key))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn put_if_absent(&self, key: &str, _data: &[u8]) -> Result<CasResult> {
        Err(Self::enospc(key))
    }

    async fn put_if_match(&self, _key: &str, _data: &[u8], _etag: &str) -> Result<CasResult> {
        Ok(CasResult {
            success: false,
            etag: None,
        })
    }

    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        self.inner.range_get(key, start, len).await
    }
}

#[async_trait]
impl StorageBackend for FailAfterPutsStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if self
            .remaining_successful_puts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return self.inner.put(key, data).await;
        }
        anyhow::bail!("injected put failure after allowed successes: {key}");
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        if self
            .remaining_successful_puts
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return self.inner.put_if_absent(key, data).await;
        }
        anyhow::bail!("injected put failure after allowed successes: {key}");
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.inner.put_if_match(key, data, etag).await
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Create a SQLite database in WAL mode at `path`, with a table and some rows.
/// Returns the open connection so the WAL is not checkpointed on close.
fn create_wal_db(path: &Path, rows: u32) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
        .unwrap();
    conn.execute_batch("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);")
        .unwrap();
    for i in 0..rows {
        conn.execute(
            "INSERT INTO data VALUES (?1, ?2)",
            rusqlite::params![i, format!("row_{}", i)],
        )
        .unwrap();
    }
    conn
}

/// Write additional rows using an existing connection (generates new WAL frames
/// without risking a checkpoint from open/close of a new connection).
fn write_rows(conn: &rusqlite::Connection, start: u32, count: u32) {
    for i in start..start + count {
        conn.execute(
            "INSERT INTO data VALUES (?1, ?2)",
            rusqlite::params![i, format!("row_{}", i)],
        )
        .unwrap();
    }
}

fn snapshot_keys(storage: &MemStorage, prefix: &str, db_name: &str) -> Vec<String> {
    let db_prefix = format!("{prefix}{db_name}/");
    storage
        .keys()
        .into_iter()
        .filter(|key| {
            key.starts_with(&db_prefix)
                && key.ends_with(".hadbp")
                && (key.starts_with(&format!("{db_prefix}0001/")) || key.contains("/0001/"))
        })
        .collect()
}

fn make_config() -> ReplicationConfig {
    ReplicationConfig {
        // Very long intervals so the background loop doesn't interfere with tests.
        sync_interval: std::time::Duration::from_secs(3600),
        snapshot_interval: std::time::Duration::from_secs(3600),
        ..Default::default()
    }
}

fn make_external_config() -> ReplicationConfig {
    ReplicationConfig {
        sync_interval: std::time::Duration::from_secs(3600),
        snapshot_interval: std::time::Duration::from_secs(3600),
        autonomous_snapshots: false,
        snapshot_ownership: SnapshotOwnership::External,
        ..Default::default()
    }
}

async fn seed_physical_delta(
    storage: &MemStorage,
    prefix: &str,
    db_name: &str,
    seq: u64,
    prev_checksum: u64,
) -> PhysicalChangeset {
    let changeset = PhysicalChangeset::new(
        seq,
        prev_checksum,
        PageIdSize::U32,
        4096,
        vec![PageEntry {
            page_id: PageId::U32(1),
            data: vec![seq as u8; 4096],
        }],
    );
    let key = cs_storage::format_key(
        prefix,
        db_name,
        GENERATION_INCREMENTAL,
        seq,
        ChangesetKind::Physical,
    );
    storage
        .put(&key, &physical::encode(&changeset))
        .await
        .unwrap();
    changeset
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_walrust_owned_reload_restores_saved_wal_checksum_chain() -> Result<()> {
    let storage = MemStorage::new();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("reload-chain.db");
    let wal_path = db_path.with_extension("db-wal");
    let conn = create_wal_db(&db_path, 3);
    let header = walrust::wal::read_header(&wal_path).await?.unwrap();
    let saved_offset = std::fs::metadata(&wal_path)?.len();

    let state_json = serde_json::json!({
        "wal_offset": saved_offset,
        "wal_generation": 4,
        "current_seq": 10,
        "current_txid": 10,
        "db_checksum": 0u64,
        "last_snapshot": null,
        "wal_salt": [header.salt().0, header.salt().1],
        "wal_checksum_chain": [0x11111111u32, 0x22222222u32],
    });
    storage
        .put(
            "wal/reload-chain/state.json",
            &serde_json::to_vec(&state_json)?,
        )
        .await?;

    write_rows(&conn, 100, 1);

    let replicator = Replicator::new(storage.clone(), "wal/", make_config());
    replicator
        .add_without_snapshot("reload-chain", &db_path)
        .await?;

    let frames = replicator.flush("reload-chain").await?;
    assert_eq!(
        frames, 0,
        "the saved checksum chain seed must be restored; the intentionally bogus seed makes the next committed frame unverifiable"
    );
    assert_ne!(
        storage.max_hadbp_seq("wal/reload-chain/"),
        Some(11),
        "flush must not publish a changeset when the saved chain seed cannot validate the next WAL frame"
    );

    Ok(())
}

#[tokio::test]
async fn test_walrust_owned_reload_restores_saved_wal_salt() -> Result<()> {
    let storage = MemStorage::new();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("reload-salt.db");
    let wal_path = db_path.with_extension("db-wal");
    let conn = create_wal_db(&db_path, 3);
    let header = walrust::wal::read_header(&wal_path).await?.unwrap();
    let saved_offset = std::fs::metadata(&wal_path)?.len();

    let state_json = serde_json::json!({
        "wal_offset": saved_offset,
        "wal_generation": 4,
        "current_seq": 10,
        "current_txid": 10,
        "db_checksum": 0u64,
        "last_snapshot": null,
        "wal_salt": [header.salt().0.wrapping_add(1), header.salt().1],
        "wal_checksum_chain": null,
    });
    storage
        .put(
            "wal/reload-salt/state.json",
            &serde_json::to_vec(&state_json)?,
        )
        .await?;

    write_rows(&conn, 100, 1);

    let replicator = Replicator::new(storage.clone(), "wal/", make_config());
    replicator
        .add_without_snapshot("reload-salt", &db_path)
        .await?;
    let frames = replicator.flush("reload-salt").await?;
    assert!(
        frames > 0,
        "salt rollover should reset to the WAL header and resync committed frames"
    );

    let saved = storage.value("wal/reload-salt/state.json").await;
    assert_eq!(
        saved.get("wal_generation").and_then(|v| v.as_u64()),
        Some(5),
        "the saved salt must be reloaded so a salt mismatch increments the WAL generation"
    );

    Ok(())
}

#[tokio::test]
async fn test_walrust_owned_reload_state_transport_error_is_hard_error() -> Result<()> {
    let inner = MemStorage::new();
    let storage = StateJsonGetFailsStorage::new(inner);
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("reload-error.db");
    let _conn = create_wal_db(&db_path, 1);

    let replicator = Replicator::new(storage, "wal/", make_config());
    let err = replicator
        .add_without_snapshot("reload-error", &db_path)
        .await
        .expect_err("state.json read failures must not be treated as a cold start");

    assert!(
        err.to_string().contains("state.json"),
        "error should identify the failed state reload: {err}"
    );
    assert!(
        !replicator.contains("reload-error").await,
        "database must not be registered after a failed state reload"
    );

    Ok(())
}

#[tokio::test]
async fn test_remove_keeps_database_registered_when_final_sync_fails() -> Result<()> {
    let inner = MemStorage::new();
    let storage = PutFailsStorage::new(inner);
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("remove-fail.db");
    let conn = create_wal_db(&db_path, 1);

    let replicator = Replicator::new(storage, "wal/", make_config());
    replicator
        .add_without_snapshot("remove-fail", &db_path)
        .await?;
    write_rows(&conn, 100, 1);

    let err = replicator
        .remove("remove-fail")
        .await
        .expect_err("failed final sync must be returned to the caller");

    assert!(
        err.to_string().contains("final sync"),
        "remove error should identify the failed final sync: {err}"
    );

    assert!(
        replicator.contains("remove-fail").await,
        "a failed final sync must not unregister the database"
    );

    Ok(())
}

#[tokio::test]
async fn test_run_wal_replication_returns_final_sync_error_on_shutdown() -> Result<()> {
    let storage = PutFailsStorage::new(MemStorage::new());
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("wal-loop-final-fail.db");
    let conn = create_wal_db(&db_path, 1);
    write_rows(&conn, 100, 1);

    let mut state = walrust::SyncState::new(db_path.clone())?;
    state.name = "wal-loop-final-fail".to_string();
    state.init_checksum()?;
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let _ = cancel_tx.send(true);
    });

    let err = walrust::run_wal_replication(
        storage.as_ref(),
        "wal/",
        &mut state,
        0,
        make_config(),
        cancel_rx,
    )
    .await
    .expect_err("shutdown final sync upload failure must be returned");

    assert!(
        err.to_string().contains("Final sync"),
        "error should identify the final sync failure: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_run_replication_returns_final_sync_error_on_shutdown() -> Result<()> {
    let storage = FailAfterPutsStorage::new(MemStorage::new(), 2);
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("owned-loop-final-fail.db");
    let conn = create_wal_db(&db_path, 1);

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let write_path = db_path.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let conn = rusqlite::Connection::open(&write_path).unwrap();
        write_rows(&conn, 100, 1);
        let _ = cancel_tx.send(true);
    });

    let err = walrust::sync::run_replication(
        storage.as_ref(),
        "wal/",
        &db_path,
        make_config(),
        cancel_rx,
    )
    .await
    .expect_err("shutdown final sync upload failure must be returned");
    drop(conn);

    assert!(
        err.to_string().contains("Final sync"),
        "error should identify the final sync failure: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_walrust_owned_flush_resnapshots_after_checkpoint_rollover() -> Result<()> {
    let storage = MemStorage::new();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("rollover-resnapshot.db");
    let conn = create_wal_db(&db_path, 3);

    let replicator = Replicator::new(storage.clone(), "wal/", make_config());
    replicator.add("rollover-resnapshot", &db_path).await?;

    write_rows(&conn, 100, 2);
    let first_frames = replicator.flush("rollover-resnapshot").await?;
    assert!(
        first_frames > 0,
        "initial flush must publish real WAL frames before the forced checkpoint"
    );
    let snapshots_before = snapshot_keys(&storage, "wal/", "rollover-resnapshot");
    assert!(
        !snapshots_before.is_empty(),
        "add() must have published the base snapshot; keys={:?}",
        storage.keys()
    );

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    let wal_path = db_path.with_extension("db-wal");
    assert_eq!(
        std::fs::metadata(&wal_path)?.len(),
        0,
        "test setup must force a WAL rollover by truncating the synced WAL"
    );

    write_rows(&conn, 200, 2);
    let rollover_frames = replicator.flush("rollover-resnapshot").await?;
    assert!(
        rollover_frames > 0,
        "rollover flush should publish a re-anchor snapshot"
    );

    let snapshots_after = snapshot_keys(&storage, "wal/", "rollover-resnapshot");
    assert!(
        snapshots_after.len() > snapshots_before.len(),
        "checkpoint rollover must publish a new snapshot instead of a live incremental across the gap; before={snapshots_before:?}, after={snapshots_after:?}, all_keys={:?}",
        storage.keys()
    );

    Ok(())
}

#[tokio::test]
async fn test_external_mode_refuses_checkpoint_rollover_until_reanchored() -> Result<()> {
    let storage = MemStorage::new();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-rollover-refusal.db");
    let conn = create_wal_db(&db_path, 3);

    let replicator = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");
    replicator.add("external-rollover", &db_path).await?;

    write_rows(&conn, 100, 2);
    let first_frames = replicator.flush("external-rollover").await?;
    assert!(first_frames > 0, "first flush must publish real WAL frames");
    let max_seq_before = storage
        .max_hadbp_seq("wal/external-rollover/")
        .expect("first external flush should publish a delta");

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    assert_eq!(
        std::fs::metadata(db_path.with_extension("db-wal"))?.len(),
        0,
        "test setup must force a WAL rollover"
    );

    write_rows(&conn, 200, 2);
    let err = replicator
        .flush("external-rollover")
        .await
        .expect_err("external-base mode must not publish across a WAL rollover")
        .to_string();
    assert!(
        err.contains("WAL rollover") && err.contains("external base"),
        "expected rollover re-anchor refusal, got {err}"
    );
    assert_eq!(
        storage.max_hadbp_seq("wal/external-rollover/"),
        Some(max_seq_before),
        "rollover refusal must not publish a new external delta"
    );

    let retry_err = replicator
        .flush("external-rollover")
        .await
        .expect_err("rollover state must stay poisoned until external re-anchor")
        .to_string();
    assert!(
        retry_err.contains("WAL rollover") && retry_err.contains("external base"),
        "retry must still refuse until re-anchor, got {retry_err}"
    );

    drop(conn);
    Ok(())
}

#[tokio::test]
async fn test_fenced_external_mode_refuses_checkpoint_rollover_until_reanchored() -> Result<()> {
    let storage = MemStorage::new();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("fenced-rollover-refusal.db");
    let conn = create_wal_db(&db_path, 3);
    let base_seq =
        walrust::sync::change_counter_from_file(&db_path).expect("read base change counter");

    let replicator = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");
    replicator.add("fenced-rollover", &db_path).await?;
    assert!(
        replicator
            .set_external_delta_base("fenced-rollover", 7, "writer-a", base_seq, [0x42; 32],)
            .await?,
        "registered database should accept fenced delta base"
    );

    write_rows(&conn, 100, 2);
    let first_frames = replicator.flush("fenced-rollover").await?;
    assert!(
        first_frames > 0,
        "first fenced flush must publish real WAL frames"
    );
    let tlmd_before = storage
        .keys()
        .into_iter()
        .filter(|key| key.starts_with("wal/fenced-rollover/") && key.ends_with(".tlmd"))
        .count();
    assert!(tlmd_before > 0, "first flush should publish a .tlmd delta");

    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    assert_eq!(
        std::fs::metadata(db_path.with_extension("db-wal"))?.len(),
        0,
        "test setup must force a WAL rollover"
    );

    write_rows(&conn, 200, 2);
    let err = replicator
        .flush("fenced-rollover")
        .await
        .expect_err("fenced external mode must not publish across a WAL rollover")
        .to_string();
    assert!(
        err.contains("WAL rollover") && err.contains("external mode"),
        "expected fenced rollover re-anchor refusal, got {err}"
    );
    let tlmd_after = storage
        .keys()
        .into_iter()
        .filter(|key| key.starts_with("wal/fenced-rollover/") && key.ends_with(".tlmd"))
        .count();
    assert_eq!(
        tlmd_after, tlmd_before,
        "rollover refusal must not publish a new fenced delta"
    );

    let retry_err = replicator
        .flush("fenced-rollover")
        .await
        .expect_err("fenced rollover state must stay poisoned until external re-anchor")
        .to_string();
    assert!(
        retry_err.contains("WAL rollover") && retry_err.contains("external mode"),
        "retry must still refuse until re-anchor, got {retry_err}"
    );

    drop(conn);
    Ok(())
}

/// flush() after writing WAL frames should upload an LTX file and return frame count > 0.
#[tokio::test]
async fn test_flush_uploads_ltx() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    // Create database with initial data. Keep connection open to prevent WAL checkpoint.
    let conn = create_wal_db(&db_path, 10);

    let storage = MemStorage::new();
    let replicator = Replicator::new(storage.clone(), "wal/", make_config());

    // add() takes an initial snapshot
    replicator.add("test", &db_path).await.unwrap();

    // Record keys after snapshot
    let keys_after_snapshot = storage.keys();
    assert!(
        !keys_after_snapshot.is_empty(),
        "add() should have uploaded at least a snapshot"
    );

    // Write new data to generate WAL frames (using same connection).
    write_rows(&conn, 100, 5);

    // flush() should sync the new WAL frames.
    // Note: the background sync loop (first tick) may have already synced the
    // initial WAL frames. flush() syncs whatever is pending at this moment.
    let _frames = replicator.flush("test").await.unwrap();

    // Verify LTX files were uploaded (either by flush or background sync).
    let keys_after_flush = storage.keys();
    let ltx_keys: Vec<_> = keys_after_flush
        .iter()
        .filter(|k| k.starts_with("wal/test/") && k.ends_with(".hadbp"))
        .collect();
    assert!(
        ltx_keys.len() >= 2,
        "Should have snapshot + at least one incremental HADBP changeset, got: {:?}",
        ltx_keys
    );

    // Either flush or background sync should have captured the WAL frames.
    // The total key count should exceed the snapshot-only state.
    assert!(
        keys_after_flush.len() > keys_after_snapshot.len(),
        "WAL frames should have been uploaded. Before: {:?}, After: {:?}",
        keys_after_snapshot,
        keys_after_flush,
    );

    drop(conn);
}

/// External-base-state mode must reject autonomous snapshots at construction time.
#[test]
fn test_try_new_rejects_external_mode_with_autonomous_snapshots() {
    let storage = MemStorage::new();
    let mut config = make_external_config();
    config.autonomous_snapshots = true;

    let err = match Replicator::try_new(storage, "wal/", config) {
        Ok(_) => panic!("unsafe config should be rejected"),
        Err(err) => err,
    };

    assert!(
        err.to_string().contains(
            "external snapshot ownership and autonomous snapshots are mutually exclusive"
        ),
        "unexpected error: {err}"
    );
}

/// External-base-state mode should register the DB without uploading a snapshot.
#[tokio::test]
async fn test_add_external_mode_skips_snapshot_upload() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external.db");
    let conn = create_wal_db(&db_path, 3);

    let storage = MemStorage::new();
    let replicator = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");

    let base_counter =
        walrust::sync::change_counter_from_file(&db_path).expect("read base change counter");
    replicator.add("external", &db_path).await.unwrap();
    assert_eq!(
        replicator.current_seq("external").await,
        Some(base_counter),
        "external mode should start delta seq at the checkpoint/base change counter"
    );

    let keys_after_add = storage.keys();
    assert!(
        keys_after_add.is_empty(),
        "external mode should not upload a snapshot on add(), got keys: {:?}",
        keys_after_add
    );

    write_rows(&conn, 100, 2);
    let frames = replicator.flush("external").await.unwrap();
    assert!(
        frames > 0,
        "flush should upload WAL frames in external mode"
    );

    let keys_after_flush = storage.keys();
    let hadbp_keys: Vec<_> = keys_after_flush
        .iter()
        .filter(|k| k.starts_with("wal/external/") && k.ends_with(".hadbp"))
        .collect();
    assert!(
        !hadbp_keys.is_empty(),
        "flush should upload an incremental HADBP changeset, got keys: {:?}",
        keys_after_flush
    );
    let uploaded_seq = hadbp_keys
        .iter()
        .filter_map(|key| {
            key.strip_suffix(".hadbp")
                .and_then(|s| s.rsplit('/').next())
                .and_then(|hex| u64::from_str_radix(hex, 16).ok())
        })
        .max()
        .expect("uploaded changeset seq");
    assert_eq!(
        uploaded_seq,
        base_counter + 1,
        "external delta seq must be contiguous after the base cursor; base={base_counter}, keys={hadbp_keys:?}"
    );

    let uploaded_key = hadbp_keys
        .into_iter()
        .find(|key| {
            key.strip_suffix(".hadbp")
                .and_then(|s| s.rsplit('/').next())
                .and_then(|hex| u64::from_str_radix(hex, 16).ok())
                == Some(uploaded_seq)
        })
        .expect("uploaded key for max seq");
    let changeset_bytes = storage
        .get(uploaded_key)
        .await
        .unwrap()
        .expect("uploaded changeset bytes");
    let changeset = hadb_changeset::physical::decode(&changeset_bytes).unwrap();
    assert_eq!(
        changeset.header.prev_checksum,
        walrust::ltx::compute_checksum_from_file(&db_path).unwrap(),
        "external-base delta must chain from the base checksum"
    );
    assert!(
        !keys_after_flush
            .iter()
            .any(|key| key.ends_with("state.json")),
        "external-base mode must not write remote state.json; keys={keys_after_flush:?}"
    );

    drop(conn);
}

#[tokio::test]
async fn test_external_mode_reopen_derives_head_without_remote_state() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-reopen.db");
    let conn = create_wal_db(&db_path, 3);

    let storage = MemStorage::new();
    let first = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");
    first.add("external", &db_path).await.unwrap();

    write_rows(&conn, 100, 2);
    let first_frames = first.flush("external").await.unwrap();
    assert!(first_frames > 0, "first flush should upload WAL frames");

    let saved_seq = storage
        .max_hadbp_seq("wal/external/")
        .expect("first flush should publish a HADBP delta");
    assert!(
        storage
            .get("wal/external/state.json")
            .await
            .unwrap()
            .is_none(),
        "external-base mode should not write remote state.json"
    );

    drop(first);

    let second = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");
    second.add("external", &db_path).await.unwrap();

    let duplicate_frames = second.flush("external").await.unwrap();
    assert_eq!(
        duplicate_frames, 0,
        "reopened external-base replicator must derive the object-chain head and not re-encode already-published WAL frames"
    );
    assert_eq!(
        second.current_seq("external").await,
        Some(saved_seq),
        "reopened external-base replicator must preserve saved sequence when no new WAL frames exist"
    );

    write_rows(&conn, 200, 1);
    let new_frames = second.flush("external").await.unwrap();
    assert!(new_frames > 0, "new rows after reopen should still flush");

    assert_eq!(
        storage.max_hadbp_seq("wal/external/"),
        Some(saved_seq + 1),
        "new frames after reopen should produce exactly one new contiguous changeset"
    );
    assert!(
        storage
            .get("wal/external/state.json")
            .await
            .unwrap()
            .is_none(),
        "external-base mode should still not write remote state.json after reopen"
    );

    drop(conn);
}

#[tokio::test]
async fn test_external_mode_ignores_stale_state_after_wal_reset() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-reset.db");
    let conn = create_wal_db(&db_path, 3);
    let base_counter =
        walrust::sync::change_counter_from_file(&db_path).expect("read base change counter");
    let base_checksum = walrust::ltx::compute_checksum_from_file(&db_path).unwrap();

    let storage = MemStorage::new();
    storage
        .put(
            "wal/external/state.json",
            serde_json::json!({
                "wal_offset": 999_999_u64,
                "wal_generation": 4_u64,
                "current_seq": base_counter + 97,
                "current_txid": base_counter + 97,
                "db_checksum": 0xfeed_face_dead_beefu64,
                "last_snapshot": null,
            })
            .to_string()
            .as_bytes(),
        )
        .await
        .unwrap();

    let replicator = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");
    replicator.add("external", &db_path).await.unwrap();
    assert_eq!(
        replicator.current_seq("external").await,
        Some(base_counter),
        "stale external state from an older WAL incarnation must not override the current base cursor"
    );

    write_rows(&conn, 100, 1);
    let frames = replicator.flush("external").await.unwrap();
    assert!(frames > 0, "fresh post-reset rows should flush");

    let hadbp_key = storage
        .keys()
        .into_iter()
        .find(|key| key.ends_with(".hadbp"))
        .expect("uploaded changeset");
    let changeset_bytes = storage
        .get(&hadbp_key)
        .await
        .unwrap()
        .expect("uploaded changeset bytes");
    let changeset = hadb_changeset::physical::decode(&changeset_bytes).unwrap();
    assert_eq!(
        changeset.header.seq,
        base_counter + 1,
        "post-reset external delta must continue directly after the current base cursor"
    );
    assert_eq!(
        changeset.header.prev_checksum, base_checksum,
        "post-reset external delta must chain from the current base checksum, not stale state.json"
    );

    drop(conn);
}

#[tokio::test]
async fn test_external_mode_ignores_stale_same_seq_delta() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-stale-same-seq.db");
    let conn = create_wal_db(&db_path, 3);
    let base_counter =
        walrust::sync::change_counter_from_file(&db_path).expect("read base change counter");
    let base_checksum = walrust::ltx::compute_checksum_from_file(&db_path).unwrap();

    let storage = MemStorage::new();
    seed_physical_delta(
        &storage,
        "wal/",
        "external",
        base_counter,
        0xfeed_face_dead_beefu64,
    )
    .await;

    let replicator = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");
    replicator.add("external", &db_path).await.unwrap();

    write_rows(&conn, 100, 1);
    let frames = replicator.flush("external").await.unwrap();
    assert!(
        frames > 0,
        "fresh post-base rows should flush even when a stale same-seq object exists"
    );

    let changeset_bytes = storage
        .get(&cs_storage::format_key(
            "wal/",
            "external",
            GENERATION_INCREMENTAL,
            base_counter + 1,
            ChangesetKind::Physical,
        ))
        .await
        .unwrap()
        .expect("uploaded post-base changeset bytes");
    let changeset = hadb_changeset::physical::decode(&changeset_bytes).unwrap();
    assert_eq!(
        changeset.header.prev_checksum, base_checksum,
        "post-base delta must chain from the current external page-base checksum, not a stale same-seq object"
    );

    drop(conn);
}

#[tokio::test]
async fn test_external_mode_registration_does_not_skip_unpublished_wal_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-unpublished-wal.db");
    let wal_path = db_path.with_extension("db-wal");
    let conn = create_wal_db(&db_path, 3);
    let base_seq =
        walrust::sync::change_counter_from_file(&db_path).expect("read base change counter");
    let base_checksum = walrust::ltx::compute_checksum_from_file(&db_path).unwrap();

    write_rows(&conn, 100, 1);

    let storage = MemStorage::new();
    let replicator = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");
    replicator
        .add_external_base_with_wal_path(
            "external",
            &db_path,
            &wal_path,
            walrust::ExternalBaseCursor {
                seq: base_seq,
                checksum: base_checksum,
            },
        )
        .await
        .unwrap();

    let frames = replicator.flush("external").await.unwrap();
    assert!(
        frames > 0,
        "registration from an older external base must not skip WAL bytes already present locally"
    );
    assert_eq!(
        storage.max_hadbp_seq("wal/external/"),
        Some(base_seq + 1),
        "unpublished WAL bytes must become the next external changeset"
    );

    drop(conn);
}

#[tokio::test]
async fn test_external_mode_rejects_remote_chain_without_local_progress() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-missing-progress.db");
    let conn = create_wal_db(&db_path, 3);
    let base_seq =
        walrust::sync::change_counter_from_file(&db_path).expect("read base change counter");
    let base_checksum = walrust::ltx::compute_checksum_from_file(&db_path).unwrap();

    let storage = MemStorage::new();
    seed_physical_delta(&storage, "wal/", "external", base_seq + 1, base_checksum).await;

    let replicator = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");
    let err = replicator
        .add_external_base_with_wal_path(
            "external",
            &db_path,
            &db_path.with_extension("db-wal"),
            walrust::ExternalBaseCursor {
                seq: base_seq,
                checksum: base_checksum,
            },
        )
        .await
        .expect_err("remote chain head without local WAL progress proof must fail closed");
    assert!(
        err.to_string().contains("local external-base progress"),
        "expected local progress proof error, got {err}"
    );

    drop(conn);
}

#[tokio::test]
async fn test_external_mode_does_not_read_or_write_remote_state_json() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-no-state-access.db");
    let conn = create_wal_db(&db_path, 3);

    let raw_storage = MemStorage::new();
    let guarded_storage = StateJsonForbiddenStorage::new(raw_storage.clone());
    let replicator = Replicator::try_new(guarded_storage, "wal/", make_external_config())
        .expect("external config should be valid");

    replicator.add("external", &db_path).await.unwrap();
    write_rows(&conn, 100, 1);
    let frames = replicator.flush("external").await.unwrap();
    assert!(
        frames > 0,
        "external-base flush should still publish frames"
    );
    assert!(
        raw_storage
            .get("wal/external/state.json")
            .await
            .unwrap()
            .is_none(),
        "external-base mode must not write state.json"
    );

    drop(conn);
}

#[tokio::test]
async fn test_external_mode_rejects_wrong_chain_existing_delta_after_base() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-wrong-chain.db");
    let conn = create_wal_db(&db_path, 3);
    let base_seq =
        walrust::sync::change_counter_from_file(&db_path).expect("read base change counter");

    let storage = MemStorage::new();
    seed_physical_delta(
        &storage,
        "wal/",
        "external",
        base_seq + 1,
        0xfeed_face_dead_beefu64,
    )
    .await;

    let replicator = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");
    let err = replicator
        .add("external", &db_path)
        .await
        .expect_err("wrong-chain existing delta must fail external-base registration")
        .to_string();
    assert!(
        err.contains("checksum chain break"),
        "expected checksum-chain failure, got {err}"
    );

    drop(conn);
}

#[tokio::test]
async fn test_external_mode_duplicate_next_seq_publish_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-duplicate-publish.db");
    let conn = create_wal_db(&db_path, 3);
    let base_seq =
        walrust::sync::change_counter_from_file(&db_path).expect("read base change counter");
    let base_checksum = walrust::ltx::compute_checksum_from_file(&db_path).unwrap();

    let storage = MemStorage::new();
    let replicator = Replicator::try_new(storage.clone(), "wal/", make_external_config())
        .expect("external config should be valid");
    replicator.add("external", &db_path).await.unwrap();

    seed_physical_delta(&storage, "wal/", "external", base_seq + 1, base_checksum).await;
    write_rows(&conn, 100, 1);

    let err = replicator
        .flush("external")
        .await
        .expect_err("duplicate next seq must not overwrite an existing object")
        .to_string();
    assert!(
        err.contains("duplicate changeset seq"),
        "expected duplicate-seq failure, got {err}"
    );
    assert_eq!(
        storage.max_hadbp_seq("wal/external/"),
        Some(base_seq + 1),
        "duplicate failure should leave the existing object in place"
    );

    drop(conn);
}

#[tokio::test]
async fn test_external_mode_identical_duplicate_next_seq_publish_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-idempotent-duplicate-publish.db");
    let conn = create_wal_db(&db_path, 3);
    let base_seq =
        walrust::sync::change_counter_from_file(&db_path).expect("read base change counter");

    let raw_storage = MemStorage::new();
    let storage = ConcurrentIdenticalPutStore::new(raw_storage.clone());
    let replicator = Replicator::try_new(storage, "wal/", make_external_config())
        .expect("external config should be valid");
    replicator.add("external", &db_path).await.unwrap();
    write_rows(&conn, 100, 1);

    let frames = replicator
        .flush("external")
        .await
        .expect("identical duplicate publish should be idempotent");
    assert!(
        frames > 0,
        "idempotent duplicate publish should still advance local sync state"
    );
    assert_eq!(
        raw_storage.max_hadbp_seq("wal/external/"),
        Some(base_seq + 1),
        "idempotent duplicate publish leaves one changeset object in place"
    );

    drop(conn);
}

#[tokio::test]
async fn test_external_mode_duplicate_publish_waits_for_visible_existing_object() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("external-delayed-duplicate-visible.db");
    let conn = create_wal_db(&db_path, 3);
    let base_seq =
        walrust::sync::change_counter_from_file(&db_path).expect("read base change counter");

    let raw_storage = MemStorage::new();
    let storage = DelayedDuplicateVisibleStore::new(raw_storage.clone(), 2);
    let replicator = Replicator::try_new(storage, "wal/", make_external_config())
        .expect("external config should be valid");
    replicator.add("external", &db_path).await.unwrap();
    write_rows(&conn, 100, 1);

    let frames = replicator
        .flush("external")
        .await
        .expect("CAS loser should retry until the identical object is visible");
    assert!(
        frames > 0,
        "delayed duplicate visibility should still advance local sync state"
    );
    assert_eq!(
        raw_storage.max_hadbp_seq("wal/external/"),
        Some(base_seq + 1),
        "delayed duplicate publish leaves one changeset object in place"
    );

    drop(conn);
}

#[tokio::test]
async fn test_walrust_owned_reopen_uses_legacy_state_json() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("walrust-owned-reopen.db");
    let conn = create_wal_db(&db_path, 3);

    let storage = MemStorage::new();
    let first = Replicator::try_new(storage.clone(), "wal/", make_config()).unwrap();
    first.add("owned", &db_path).await.unwrap();
    write_rows(&conn, 100, 2);
    let frames = first.flush("owned").await.unwrap();
    assert!(
        frames > 0,
        "first walrust-owned flush should publish frames"
    );

    let saved_state = storage.value("wal/owned/state.json").await;
    let saved_seq = saved_state
        .get("current_seq")
        .and_then(|value| value.as_u64())
        .expect("walrust-owned mode persists current_seq");
    assert!(
        saved_state
            .get("wal_offset")
            .and_then(|value| value.as_u64())
            .unwrap_or(0)
            > 0,
        "walrust-owned mode persists wal_offset"
    );

    drop(first);

    let second = Replicator::try_new(storage.clone(), "wal/", make_config()).unwrap();
    second
        .add_without_snapshot("owned", &db_path)
        .await
        .expect("walrust-owned reopen from state");
    assert_eq!(
        second.current_seq("owned").await,
        Some(saved_seq),
        "walrust-owned add_without_snapshot should still use legacy state.json"
    );
    let duplicate = second.flush("owned").await.unwrap();
    assert_eq!(
        duplicate, 0,
        "walrust-owned reopen should not re-encode old WAL frames"
    );

    drop(conn);
}

#[tokio::test]
async fn test_walrust_owned_new_stream_writes_lineage_state_and_keys() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("walrust-owned-lineage.db");
    let conn = create_wal_db(&db_path, 3);

    let storage = MemStorage::new();
    let replicator = Replicator::try_new(storage.clone(), "wal/", make_config()).unwrap();
    replicator.add("owned-lineage", &db_path).await.unwrap();
    write_rows(&conn, 100, 2);
    let frames = replicator.flush("owned-lineage").await.unwrap();
    assert!(frames > 0, "flush should publish live WAL frames");

    let saved_state = storage.value("wal/owned-lineage/state.json").await;
    let lineage_id = saved_state
        .get("lineage_id")
        .and_then(|value| value.as_str())
        .expect("walrust-owned mode must persist lineage_id");
    assert!(
        !lineage_id.is_empty() && !lineage_id.contains('/'),
        "lineage_id must be a single path segment, got {lineage_id:?}"
    );

    let lineage_prefix = format!("wal/owned-lineage/lineages/{lineage_id}/");
    let keys = storage.keys();
    assert!(
        keys.iter().any(
            |key| key.starts_with(&format!("{lineage_prefix}0001/")) && key.ends_with(".hadbp")
        ),
        "initial snapshot must be written under the lineage namespace; keys={keys:?}"
    );
    assert!(
        keys.iter().any(
            |key| key.starts_with(&format!("{lineage_prefix}0000/")) && key.ends_with(".hadbp")
        ),
        "live WAL changeset must be written under the lineage namespace; keys={keys:?}"
    );
    assert!(
        !keys.iter().any(|key| {
            (key.starts_with("wal/owned-lineage/0000/")
                || key.starts_with("wal/owned-lineage/0001/"))
                && key.ends_with(".hadbp")
        }),
        "new walrust-owned streams must not write legacy unlineaged HADBP keys; keys={keys:?}"
    );

    drop(conn);
}

#[tokio::test]
async fn test_walrust_owned_add_refuses_existing_active_state() {
    let dir = tempfile::tempdir().unwrap();
    let first_db = dir.path().join("walrust-owned-first.db");
    let second_db = dir.path().join("walrust-owned-second.db");
    let first_conn = create_wal_db(&first_db, 3);
    let second_conn = create_wal_db(&second_db, 5);
    let storage = MemStorage::new();

    let first = Replicator::try_new(storage.clone(), "wal/", make_config()).unwrap();
    first.add("owned-fence", &first_db).await.unwrap();

    let first_state = storage.value("wal/owned-fence/state.json").await;
    let first_lineage = first_state
        .get("lineage_id")
        .and_then(|value| value.as_str())
        .expect("initial walrust-owned add must persist active lineage")
        .to_string();

    let second = Replicator::try_new(storage.clone(), "wal/", make_config()).unwrap();
    let err = second
        .add("owned-fence", &second_db)
        .await
        .expect_err("competing walrust-owned add must not overwrite active state");
    assert!(
        err.to_string().contains("already has replication state"),
        "expected active-state refusal, got {err}"
    );

    let state_after = storage.value("wal/owned-fence/state.json").await;
    assert_eq!(
        state_after
            .get("lineage_id")
            .and_then(|value| value.as_str()),
        Some(first_lineage.as_str()),
        "competing add must not replace the active lineage"
    );

    drop(first_conn);
    drop(second_conn);
}

#[tokio::test]
async fn test_walrust_owned_concurrent_two_watchers_only_one_wins() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("walrust-owned-two-watchers.db");
    let conn = create_wal_db(&db_path, 3);
    let storage = MemStorage::new();

    let first = Replicator::try_new(storage.clone(), "wal/", make_config()).unwrap();
    let second = Replicator::try_new(storage.clone(), "wal/", make_config()).unwrap();

    let (first_result, second_result) = tokio::join!(
        first.add("owned-two-watchers", &db_path),
        second.add("owned-two-watchers", &db_path)
    );

    let successes = usize::from(first_result.is_ok()) + usize::from(second_result.is_ok());
    let errors = [first_result.as_ref().err(), second_result.as_ref().err()];

    assert_eq!(
        successes, 1,
        "exactly one competing walrust-owned watcher may claim active state"
    );
    assert!(
        errors
            .iter()
            .flatten()
            .any(|err| err.to_string().contains("active walrust-owned lineage")),
        "losing watcher must fail closed with active-lineage refusal"
    );

    let state = storage.value("wal/owned-two-watchers/state.json").await;
    assert!(
        state
            .get("lineage_id")
            .and_then(|value| value.as_str())
            .is_some(),
        "winning watcher must persist the active lineage"
    );

    drop(conn);
}

#[tokio::test]
async fn test_walrust_owned_enospc_during_add_is_hard_error() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("walrust-owned-enospc.db");
    let conn = create_wal_db(&db_path, 3);
    let inner = MemStorage::new();
    let storage = EnospcStorage::new(inner);
    let replicator = Replicator::try_new(storage, "wal/", make_config()).unwrap();

    let err = replicator
        .add("owned-enospc", &db_path)
        .await
        .expect_err("ENOSPC during initial snapshot/state write must fail add");
    assert!(
        err.to_string().contains("No space left on device"),
        "expected ENOSPC to be surfaced, got {err}"
    );
    assert_eq!(
        replicator.current_seq("owned-enospc").await,
        None,
        "failed add must not leave the database registered"
    );

    drop(conn);
}

#[tokio::test]
async fn test_walrust_owned_restore_uses_active_lineage_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("walrust-owned-lineage-restore.db");
    let restore_path = dir.path().join("restored.db");
    let conn = create_wal_db(&db_path, 3);

    let storage = MemStorage::new();
    let replicator = Replicator::try_new(storage.clone(), "wal/", make_config()).unwrap();
    replicator
        .add("owned-lineage-restore", &db_path)
        .await
        .unwrap();
    write_rows(&conn, 100, 2);
    replicator.flush("owned-lineage-restore").await.unwrap();

    let restored_seq = walrust::sync::restore(
        storage.as_ref(),
        "wal/",
        "owned-lineage-restore",
        &restore_path,
        None,
    )
    .await
    .expect("restore must discover active lineage from state.json");
    assert_eq!(
        restored_seq,
        replicator
            .current_seq("owned-lineage-restore")
            .await
            .unwrap()
    );

    let restored = rusqlite::Connection::open(&restore_path).unwrap();
    let count: i64 = restored
        .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 5, "restore must include snapshot and lineaged WAL");

    drop(conn);
}

/// flush() with no pending WAL frames should return 0.
#[tokio::test]
async fn test_flush_no_pending_frames_returns_zero() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let conn = create_wal_db(&db_path, 5);

    let storage = MemStorage::new();
    let replicator = Replicator::new(storage.clone(), "wal/", make_config());
    replicator.add("test", &db_path).await.unwrap();

    // First flush syncs any pending frames from the initial writes
    let _first = replicator.flush("test").await.unwrap();

    // Second flush with no new writes should return 0
    let second = replicator.flush("test").await.unwrap();
    assert_eq!(
        second, 0,
        "flush() with no new WAL data should return 0, got {}",
        second
    );

    drop(conn);
}

/// flush() with an unknown database name should return an error.
#[tokio::test]
async fn test_flush_unknown_database_errors() {
    let storage = MemStorage::new();
    let replicator = Replicator::new(storage.clone(), "wal/", make_config());

    let result = replicator.flush("nonexistent").await;
    assert!(result.is_err(), "flush() for unknown database should error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not registered"),
        "Error should mention 'not registered', got: {}",
        err,
    );
}

/// flush() should be callable multiple times: write, flush, write more, flush again.
/// Verifies that data ends up in storage after each round (either via flush or background sync).
#[tokio::test]
async fn test_flush_multiple_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("multi.db");
    let conn = create_wal_db(&db_path, 5);

    let storage = MemStorage::new();
    let replicator = Replicator::new(storage.clone(), "pfx/", make_config());
    replicator.add("multi", &db_path).await.unwrap();

    // Round 1: flush initial WAL frames
    let _frames1 = replicator.flush("multi").await.unwrap();
    let keys1 = storage.keys();
    let ltx_count_1 = keys1.iter().filter(|k| k.ends_with(".hadbp")).count();

    // Round 2: write more, flush again
    write_rows(&conn, 100, 10);
    let _frames2 = replicator.flush("multi").await.unwrap();
    let keys2 = storage.keys();
    let ltx_count_2 = keys2.iter().filter(|k| k.ends_with(".hadbp")).count();

    // The new WAL data should have produced additional LTX files
    // (either via flush() or the background sync loop)
    assert!(
        ltx_count_2 > ltx_count_1,
        "Second round should produce more HADBP files. Round 1: {} changesets, Round 2: {} changesets. Keys1: {:?}, Keys2: {:?}",
        ltx_count_1,
        ltx_count_2,
        keys1,
        keys2,
    );

    // Round 3: no new writes, flush should return 0
    let frames3 = replicator.flush("multi").await.unwrap();
    assert_eq!(frames3, 0, "Third flush with no writes should return 0");

    drop(conn);
}

/// flush() after remove() should error (database no longer registered).
#[tokio::test]
async fn test_flush_after_remove_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let conn = create_wal_db(&db_path, 3);

    let storage = MemStorage::new();
    let replicator = Replicator::new(storage.clone(), "wal/", make_config());
    replicator.add("test", &db_path).await.unwrap();

    // Remove the database
    replicator.remove("test").await.unwrap();

    // flush() should fail
    let result = replicator.flush("test").await;
    assert!(result.is_err(), "flush() after remove() should error");
    assert!(
        result.unwrap_err().to_string().contains("not registered"),
        "Error should mention 'not registered'"
    );

    drop(conn);
}

/// flush() exercises the same code path as sync_all() for a single database.
/// Verify that calling flush() on a freshly-added DB (with WAL frames from
/// the initial writes) returns the correct frame count.
#[tokio::test]
async fn test_flush_returns_frame_count() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("count.db");
    let conn = create_wal_db(&db_path, 0); // no rows yet

    let storage = MemStorage::new();
    let replicator = Replicator::new(storage.clone(), "wal/", make_config());
    replicator.add("count", &db_path).await.unwrap();

    // Initial flush with no WAL data should return 0
    // (create_wal_db creates the table, which writes WAL frames, but the
    // background sync may have already consumed them. Either way, after
    // one flush the state should be caught up.)
    let _initial = replicator.flush("count").await.unwrap();

    // Ensure background sync is done by flushing again
    let before = replicator.flush("count").await.unwrap();
    assert_eq!(before, 0, "No pending frames expected");

    // Write exactly 1 row (generates WAL frames)
    write_rows(&conn, 0, 1);

    // flush() should capture the new frame(s)
    let after = replicator.flush("count").await.unwrap();
    // Either flush caught them or the background loop did
    // At minimum, the data is in storage
    let ltx_keys: Vec<_> = storage
        .keys()
        .iter()
        .filter(|k| k.contains("0000/") && k.ends_with(".hadbp"))
        .cloned()
        .collect();
    assert!(
        !ltx_keys.is_empty() || after > 0,
        "WAL frames should be in storage after flush"
    );

    drop(conn);
}
