use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use walrust_core::legacy_ltx;
use walrust_core::legacy_manifest::{build_ltx_key, GENERATION_LIVE};
use walrust_core::wal;

#[derive(Clone, Default)]
struct MemStorage {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
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
        let mut keys: Vec<String> = self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .filter(|key| after.map(|marker| key.as_str() > marker).unwrap_or(true))
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
            etag: Some("mem".into()),
        })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
        self.put(key, data).await?;
        Ok(CasResult {
            success: true,
            etag: Some("mem".into()),
        })
    }
}

fn marker_value(path: &Path) -> String {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row("SELECT value FROM marker", [], |row| row.get(0))
        .unwrap()
}

#[tokio::test]
async fn legacy_restore_is_owned_by_core_and_replays_real_wal_incremental() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("source.db");
    let restore_path = dir.path().join("restored.db");
    let storage = MemStorage::default();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "
        PRAGMA page_size=4096;
        PRAGMA journal_mode=WAL;
        PRAGMA wal_autocheckpoint=0;
        CREATE TABLE marker (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO marker (id, value) VALUES (1, 'snapshot');
        PRAGMA wal_checkpoint(TRUNCATE);
        ",
    )
    .unwrap();

    let (snapshot, snapshot_checksum) =
        legacy_ltx::encode_sqlite_snapshot_to_vec(&db_path, 4096, 1).unwrap();
    storage
        .put(&build_ltx_key("backups", "app", 1, 1, 1), &snapshot)
        .await
        .unwrap();

    conn.execute("UPDATE marker SET value = 'incremental' WHERE id = 1", [])
        .unwrap();

    let wal_path = db_path.with_extension("db-wal");
    let (page_map, frame_count, _offset, final_db_size, _commit_count, _chain) =
        wal::read_frames_as_page_map_checked(&wal_path, 4096, 0, None)
            .await
            .unwrap();
    assert!(frame_count > 0, "fixture must produce committed WAL frames");
    let pages: Vec<(u32, Vec<u8>)> = page_map.into_iter().collect();
    let post_checksum = legacy_ltx::chain_checksum(snapshot_checksum, &pages);
    let mut incremental = Vec::new();
    legacy_ltx::encode_wal_changes(
        &mut incremental,
        &pages,
        4096,
        2,
        2,
        final_db_size,
        Some(snapshot_checksum),
        post_checksum,
    )
    .unwrap();
    storage
        .put(
            &build_ltx_key("backups", "app", GENERATION_LIVE, 2, 2),
            &incremental,
        )
        .await
        .unwrap();

    let restored_txid = walrust_core::legacy_restore::restore_legacy_ltx(
        &storage,
        "backups",
        "app",
        &restore_path,
        None,
    )
    .await
    .unwrap();

    assert_eq!(restored_txid, 2);
    assert_eq!(marker_value(&restore_path), "incremental");
}

#[tokio::test]
async fn e4_far_future_point_in_time_is_a_hard_error_naming_the_max() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("source.db");
    let restore_path = dir.path().join("restored.db");
    let storage = MemStorage::default();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "
        PRAGMA page_size=4096;
        PRAGMA journal_mode=WAL;
        PRAGMA wal_autocheckpoint=0;
        CREATE TABLE marker (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO marker (id, value) VALUES (1, 'snapshot');
        PRAGMA wal_checkpoint(TRUNCATE);
        ",
    )
    .unwrap();
    let (snapshot, _checksum) =
        legacy_ltx::encode_sqlite_snapshot_to_vec(&db_path, 4096, 1).unwrap();
    storage
        .put(&build_ltx_key("backups", "app", 1, 1, 1), &snapshot)
        .await
        .unwrap();

    // Newest available TXID is 1. A far-future point-in-time must be a hard
    // error naming the max available — not a silent latest-DB restore.
    let err = walrust_core::legacy_restore::restore_legacy_ltx(
        &storage,
        "backups",
        "app",
        &restore_path,
        Some(999_999),
    )
    .await
    .expect_err("far-future point-in-time must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("beyond the newest available TXID 1"),
        "error must name the max available TXID, got: {msg}"
    );
    assert!(
        !restore_path.exists(),
        "no database should be written for an unreachable point-in-time"
    );

    // A point-in-time at exactly the newest available TXID still succeeds.
    let txid = walrust_core::legacy_restore::restore_legacy_ltx(
        &storage,
        "backups",
        "app",
        &restore_path,
        Some(1),
    )
    .await
    .expect("point-in-time at the max available TXID must succeed");
    assert_eq!(txid, 1);
}
