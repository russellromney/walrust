use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use rusqlite::Connection;
use walrust_core::sync::{
    restore, resume_owned_after_restore, save_state, sync_wal_with_retry, take_snapshot_with_retry,
    SyncState,
};
use walrust_core::{RetryConfig, RetryPolicy};

#[derive(Default)]
struct MemStorage {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

struct FailStatePut {
    inner: Arc<MemStorage>,
    fail_state_put: AtomicBool,
    fail_preflight_get: AtomicBool,
}

impl FailStatePut {
    fn new(inner: Arc<MemStorage>) -> Self {
        Self {
            inner,
            fail_state_put: AtomicBool::new(true),
            fail_preflight_get: AtomicBool::new(false),
        }
    }

    fn preflight_get(inner: Arc<MemStorage>) -> Self {
        Self {
            inner,
            fail_state_put: AtomicBool::new(false),
            fail_preflight_get: AtomicBool::new(true),
        }
    }
}

impl MemStorage {
    fn keys(&self) -> Vec<String> {
        let mut keys: Vec<_> = self.objects.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }

    fn bytes(&self, key: &str) -> Vec<u8> {
        self.objects.lock().unwrap()[key].clone()
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
        let mut keys: Vec<_> = self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .filter(|key| after.map(|cursor| key.as_str() > cursor).unwrap_or(true))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.lock().unwrap().contains_key(key))
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key) {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        objects.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".to_string()),
        })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
        let mut objects = self.objects.lock().unwrap();
        if !objects.contains_key(key) {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        objects.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".to_string()),
        })
    }
}

#[async_trait]
impl StorageBackend for FailStatePut {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if key.ends_with("0000000000000003.hadbp")
            && self.fail_preflight_get.swap(false, Ordering::SeqCst)
        {
            anyhow::bail!("injected resume preflight read failure");
        }
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        if key.ends_with("/state.json") && self.fail_state_put.load(Ordering::SeqCst) {
            anyhow::bail!("injected state.json persistence failure");
        }
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

fn no_retry() -> RetryPolicy {
    RetryPolicy::new(RetryConfig {
        max_retries: 0,
        circuit_breaker_enabled: false,
        ..Default::default()
    })
}

fn create_db(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA wal_autocheckpoint=0;
        CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE blobs (id INTEGER PRIMARY KEY, value BLOB NOT NULL);
        INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three');
        INSERT INTO blobs VALUES (1, zeroblob(131072));
        ",
    )
    .unwrap();
    conn
}

fn item_rows(path: &Path) -> Vec<(i64, String)> {
    let conn = Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare("SELECT id, value FROM items ORDER BY id")
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn assert_integrity(path: &Path) {
    let conn = Connection::open(path).unwrap();
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
}

async fn seed_owned_history(
    storage: &MemStorage,
    path: &Path,
    name: &str,
    lineaged: bool,
) -> (Connection, SyncState) {
    let writer = create_db(path);
    let retry = no_retry();
    let mut state = SyncState::new(path.to_path_buf()).unwrap();
    state.name = name.to_string();
    if lineaged {
        state.ensure_lineage_id();
    }
    take_snapshot_with_retry(storage, "wal/", &mut state, &retry)
        .await
        .unwrap();
    save_state(storage, "wal/", &state).await.unwrap();

    writer
        .execute_batch(
            "
            UPDATE items SET value = 'two-before' WHERE id = 2;
            DELETE FROM items WHERE id = 3;
            INSERT INTO items VALUES (4, 'four-before');
            UPDATE blobs SET value = randomblob(196608) WHERE id = 1;
            ",
        )
        .unwrap();
    assert!(
        sync_wal_with_retry(storage, "wal/", &mut state, &retry)
            .await
            .unwrap()
            > 0
    );
    (writer, state)
}

#[tokio::test]
async fn public_owned_resume_round_trips_lineaged_mixed_workload() {
    let storage = Arc::new(MemStorage::default());
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("app.db");
    let (source_writer, source_state) =
        seed_owned_history(&storage, &source_path, "app", true).await;

    let restore_dir = tempfile::tempdir().unwrap();
    let restore_path = restore_dir.path().join("app.db");
    let restored = restore(storage.clone(), "wal/", "app", &restore_path, None)
        .await
        .unwrap();
    assert_eq!(restored.seq(), 2);
    drop(source_writer);
    drop(source_state);

    let retry = no_retry();
    let mut resumed = SyncState::new(restore_path.clone()).unwrap();
    resumed.name = "app".to_string();
    resume_owned_after_restore(&*storage, "wal/", &mut resumed, &restored, &retry)
        .await
        .unwrap();
    assert_eq!(resumed.current_seq, 3);

    let writer = Connection::open(&restore_path).unwrap();
    writer
        .execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA wal_autocheckpoint=0;
            ALTER TABLE items ADD COLUMN note TEXT;
            UPDATE items SET value = 'two-after', note = 'migrated' WHERE id = 2;
            INSERT INTO items (id, value, note) VALUES (5, 'five-after', 'new');
            INSERT INTO blobs VALUES (2, randomblob(262144));
            ",
        )
        .unwrap();
    assert!(
        sync_wal_with_retry(&*storage, "wal/", &mut resumed, &retry)
            .await
            .unwrap()
            > 0
    );

    let verify_dir = tempfile::tempdir().unwrap();
    let verify_path = verify_dir.path().join("app.db");
    let final_restore = restore(storage.clone(), "wal/", "app", &verify_path, None)
        .await
        .unwrap();
    assert_eq!(final_restore.seq(), 4);
    assert_integrity(&verify_path);
    assert_eq!(item_rows(&verify_path), item_rows(&restore_path));

    let verify = Connection::open(&verify_path).unwrap();
    let blob_bytes: i64 = verify
        .query_row("SELECT SUM(length(value)) FROM blobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(blob_bytes, 196608 + 262144);
    let migrated: String = verify
        .query_row("SELECT note FROM items WHERE id = 2", [], |row| row.get(0))
        .unwrap();
    assert_eq!(migrated, "migrated");

    let state_json: serde_json::Value =
        serde_json::from_slice(&storage.get("wal/app/state.json").await.unwrap().unwrap()).unwrap();
    let lineage = state_json["lineage_id"].as_str().unwrap();
    assert!(storage.keys().iter().any(|key| {
        key.starts_with(&format!("wal/app/lineages/{lineage}/0001/"))
            && key.ends_with("0000000000000003.hadbp")
    }));
}

#[tokio::test]
async fn public_owned_resume_refuses_concurrent_writer_without_overwrite() {
    let storage = Arc::new(MemStorage::default());
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("split.db");
    let (source_writer, mut source_state) =
        seed_owned_history(&storage, &source_path, "split", false).await;

    let restore_dir = tempfile::tempdir().unwrap();
    let restore_path = restore_dir.path().join("split.db");
    let restored = restore(storage.clone(), "wal/", "split", &restore_path, None)
        .await
        .unwrap();

    source_writer
        .execute("INSERT INTO items VALUES (6, 'winner')", [])
        .unwrap();
    sync_wal_with_retry(&*storage, "wal/", &mut source_state, &no_retry())
        .await
        .unwrap();
    let winner_key = "wal/split/0000/0000000000000003.hadbp";
    let winner_bytes = storage.bytes(winner_key);

    let loser = Connection::open(&restore_path).unwrap();
    loser
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
             INSERT INTO items VALUES (99, 'loser');",
        )
        .unwrap();
    let mut resumed = SyncState::new(restore_path.clone()).unwrap();
    resumed.name = "split".to_string();
    let err = resume_owned_after_restore(&*storage, "wal/", &mut resumed, &restored, &no_retry())
        .await
        .expect_err("a live writer at restored seq + 1 must win the CAS");
    assert!(err.to_string().contains("equivocation"), "{err}");
    assert_eq!(
        resumed.current_seq, 0,
        "failed adoption restores fresh state"
    );
    assert_eq!(
        storage.bytes(winner_key),
        winner_bytes,
        "winner object changed"
    );

    let verify_dir = tempfile::tempdir().unwrap();
    let verify_path = verify_dir.path().join("split.db");
    restore(storage.clone(), "wal/", "split", &verify_path, None)
        .await
        .unwrap();
    let rows = item_rows(&verify_path);
    assert!(rows.iter().any(|row| row == &(6, "winner".to_string())));
    assert!(!rows.iter().any(|row| row.0 == 99));
    assert_integrity(&verify_path);
}

#[tokio::test]
async fn public_owned_resume_rejects_pitr_without_storage_writes() {
    let storage = Arc::new(MemStorage::default());
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("pitr.db");
    let (_writer, _state) = seed_owned_history(&storage, &source_path, "pitr", false).await;

    let restore_dir = tempfile::tempdir().unwrap();
    let restore_path = restore_dir.path().join("pitr.db");
    let restored = restore(storage.clone(), "wal/", "pitr", &restore_path, Some("1"))
        .await
        .unwrap();
    let keys_before = storage.keys();
    let mut resumed = SyncState::new(restore_path).unwrap();
    resumed.name = "pitr".to_string();
    let err = resume_owned_after_restore(&*storage, "wal/", &mut resumed, &restored, &no_retry())
        .await
        .expect_err("PITR history must not replace latest history");
    assert!(err.to_string().contains("point-in-time"), "{err}");
    assert_eq!(storage.keys(), keys_before);
    assert_eq!(resumed.current_seq, 0);
}

#[tokio::test]
async fn public_owned_resume_state_failure_is_loud_and_snapshot_is_recoverable() {
    let storage = Arc::new(MemStorage::default());
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("state-fail.db");
    let (_writer, _state) = seed_owned_history(&storage, &source_path, "state-fail", false).await;

    let restore_dir = tempfile::tempdir().unwrap();
    let restore_path = restore_dir.path().join("state-fail.db");
    let restored = restore(storage.clone(), "wal/", "state-fail", &restore_path, None)
        .await
        .unwrap();
    let failing = FailStatePut::new(storage.clone());
    let mut resumed = SyncState::new(restore_path).unwrap();
    resumed.name = "state-fail".to_string();

    let err = resume_owned_after_restore(&failing, "wal/", &mut resumed, &restored, &no_retry())
        .await
        .expect_err("state persistence failure must be returned");
    assert!(err.to_string().contains("state.json"), "{err}");
    assert_eq!(
        resumed.current_seq, 3,
        "published snapshot cursor must not roll back in memory"
    );
    assert!(storage
        .keys()
        .contains(&"wal/state-fail/0001/0000000000000003.hadbp".to_string()));

    let saved: serde_json::Value = serde_json::from_slice(
        &storage
            .get("wal/state-fail/state.json")
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        saved["current_seq"], 2,
        "failed save leaves old cursor intact"
    );

    let verify_dir = tempfile::tempdir().unwrap();
    let verify_path = verify_dir.path().join("state-fail.db");
    let recovered = restore(storage, "wal/", "state-fail", &verify_path, None)
        .await
        .expect("object discovery must recover the published snapshot");
    assert_eq!(recovered.seq(), 3);
    assert_integrity(&verify_path);
}

#[tokio::test]
async fn public_owned_resume_preflight_failure_leaves_state_retryable() {
    let storage = Arc::new(MemStorage::default());
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("preflight.db");
    let (_writer, _state) = seed_owned_history(&storage, &source_path, "preflight", false).await;
    let restore_dir = tempfile::tempdir().unwrap();
    let restore_path = restore_dir.path().join("preflight.db");
    let restored = restore(storage.clone(), "wal/", "preflight", &restore_path, None)
        .await
        .unwrap();
    let failing = FailStatePut::preflight_get(storage.clone());
    let mut resumed = SyncState::new(restore_path).unwrap();
    resumed.name = "preflight".to_string();

    let err = resume_owned_after_restore(&failing, "wal/", &mut resumed, &restored, &no_retry())
        .await
        .expect_err("preflight storage failure must propagate");
    assert!(err.to_string().contains("preflight read failure"), "{err}");
    assert_eq!(resumed.current_seq, 0);
    assert_eq!(resumed.current_txid, 0);
    assert_eq!(resumed.wal_offset, 0);
    assert!(resumed.lineage_id.is_none());
    assert!(resumed.db_checksum.is_none());

    resume_owned_after_restore(&*storage, "wal/", &mut resumed, &restored, &no_retry())
        .await
        .expect("same fresh state must be retryable after transient preflight failure");
    assert_eq!(resumed.current_seq, 3);
}
