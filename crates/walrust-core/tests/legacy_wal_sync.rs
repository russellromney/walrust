use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use walrust_core::legacy_cache::LocalCache;
use walrust_core::legacy_manifest::build_ltx_key;
use walrust_core::legacy_uploader::UploadMessage;
use walrust_core::legacy_wal_sync::{
    snapshot_database_to_storage, sync_wal_to_cache, sync_wal_to_storage,
    sync_watched_db_once_to_cache, take_snapshot_to_storage, SyncInput, WatchedDbState,
};
use walrust_core::shadow::ShadowWal;

#[derive(Default)]
struct MemoryStorage {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

#[async_trait]
impl StorageBackend for MemoryStorage {
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

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key) {
            Ok(CasResult {
                success: false,
                etag: None,
            })
        } else {
            objects.insert(key.to_string(), data.to_vec());
            Ok(CasResult {
                success: true,
                etag: Some("mem".into()),
            })
        }
    }

    async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".into()),
        })
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str, _after: Option<&str>) -> Result<Vec<String>> {
        let mut keys = self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        Ok(keys)
    }
}

#[tokio::test]
async fn legacy_wal_sync_initial_snapshot_is_owned_by_core() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("source.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE marker (id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO marker (value) VALUES ('initial-snapshot');",
        )?;
    }

    let storage = MemoryStorage::default();
    let output = sync_wal_to_storage(
        &storage,
        "backups",
        SyncInput {
            db_path: db_path.clone(),
            name: "app".to_string(),
            wal_path: db_path.with_extension("db-wal"),
            wal_offset: 0,
            wal_generation: 0,
            current_txid: 0,
            db_checksum: None,
            wal_salt: None,
            wal_checksum_chain: None,
        },
    )
    .await?;

    let key = build_ltx_key("backups", "app", 1, 1, 1);
    assert!(storage.get(&key).await?.is_some());
    assert_eq!(output.new_current_txid, 1);
    assert_eq!(output.frame_count, 1);
    Ok(())
}

#[tokio::test]
async fn legacy_wal_sync_cache_initial_snapshot_is_owned_by_core() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("cache-source.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE marker (id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO marker (value) VALUES ('cache-initial-snapshot');",
        )?;
    }

    let cache = Arc::new(LocalCache::new(&db_path)?);
    let shadow = Arc::new(tokio::sync::Mutex::new(ShadowWal::new(&db_path).await?));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<UploadMessage>(4);

    let output = sync_wal_to_cache(
        &SyncInput {
            db_path: db_path.clone(),
            name: "app".to_string(),
            wal_path: db_path.with_extension("db-wal"),
            wal_offset: 0,
            wal_generation: 0,
            current_txid: 0,
            db_checksum: None,
            wal_salt: None,
            wal_checksum_chain: None,
        },
        &cache,
        &shadow,
        &tx,
    )
    .await?;

    assert_eq!(output.new_current_txid, 1);
    assert_eq!(output.frame_count, 1);
    assert_eq!(cache.pending_uploads(), vec![1]);
    assert!(matches!(rx.try_recv()?, UploadMessage::Upload(1)));
    Ok(())
}

#[tokio::test]
async fn legacy_watch_sync_once_state_machine_is_owned_by_core() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("watch-source.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE marker (id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO marker (value) VALUES ('watch-sync-once');",
        )?;
    }

    let cache = Arc::new(LocalCache::new(&db_path)?);
    let shadow = Arc::new(tokio::sync::Mutex::new(ShadowWal::new(&db_path).await?));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<UploadMessage>(4);
    let mut state = WatchedDbState {
        db_path: db_path.clone(),
        name: "app".to_string(),
        wal_path: db_path.with_extension("db-wal"),
        wal_offset: 0,
        wal_generation: 0,
        current_txid: 0,
        db_checksum: None,
        wal_salt: None,
        wal_checksum_chain: None,
    };

    let output = sync_watched_db_once_to_cache(&mut state, &cache, &shadow, &tx).await?;

    assert_eq!(output.frame_count, 1);
    assert_eq!(state.wal_offset, output.new_wal_offset);
    assert_eq!(state.current_txid, output.new_current_txid);
    assert_eq!(state.db_checksum, output.new_db_checksum);
    assert_eq!(state.wal_salt, output.new_wal_salt);
    assert_eq!(state.wal_checksum_chain, output.new_wal_checksum_chain);
    assert_eq!(cache.pending_uploads(), vec![1]);
    assert!(matches!(rx.try_recv()?, UploadMessage::Upload(1)));
    Ok(())
}

#[tokio::test]
async fn legacy_wal_sync_periodic_snapshot_is_owned_by_core() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("periodic-source.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE marker (id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO marker (value) VALUES ('periodic-snapshot');",
        )?;
    }

    let storage = MemoryStorage::default();
    storage
        .put(&build_ltx_key("backups", "app", 1, 1, 2), b"existing")
        .await?;

    let output = take_snapshot_to_storage(
        &storage,
        "backups",
        SyncInput {
            db_path: db_path.clone(),
            name: "app".to_string(),
            wal_path: db_path.with_extension("db-wal"),
            wal_offset: 8192,
            wal_generation: 3,
            current_txid: 2,
            db_checksum: None,
            wal_salt: None,
            wal_checksum_chain: Some((1, 2)),
        },
    )
    .await?;

    let key = build_ltx_key("backups", "app", 2, 1, 3);
    assert!(storage.get(&key).await?.is_some());
    assert_eq!(output.new_current_txid, 3);
    assert_eq!(output.new_wal_offset, 0);
    assert_eq!(output.new_wal_generation, 4);
    assert!(output.new_db_checksum.is_some());
    assert!(output.new_wal_checksum_chain.is_none());
    Ok(())
}

#[tokio::test]
async fn legacy_manual_snapshot_is_owned_by_core() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("manual-source.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE marker (id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO marker (value) VALUES ('manual-snapshot');",
        )?;
    }

    let storage = MemoryStorage::default();
    storage
        .put(
            &build_ltx_key("backups", "manual-source", 1, 1, 4),
            b"existing",
        )
        .await?;

    let output =
        snapshot_database_to_storage(&storage, "backups", "manual-source", &db_path).await?;

    let key = build_ltx_key("backups", "manual-source", 2, 1, 5);
    assert!(storage.get(&key).await?.is_some());
    assert_eq!(output.key, key);
    assert_eq!(output.generation, 2);
    assert_eq!(output.min_txid, 1);
    assert_eq!(output.max_txid, 5);
    assert!(output.size_bytes > 0);
    Ok(())
}

#[tokio::test]
async fn legacy_manual_snapshot_folds_wal_resident_rows() -> Result<()> {
    // B10 regression guard: a snapshot must include WAL-resident commits in the
    // base image rather than encode a stale main-DB file. The fold here is done by
    // the stable `VACUUM INTO` copy inside `encode_sqlite_snapshot_to_vec`, which
    // captures a consistent (main DB + WAL) view; the PASSIVE checkpoint in
    // snapshot_database_to_storage is belt-and-suspenders (redundant given VACUUM
    // INTO). Keep rows in an uncheckpointed WAL, snapshot, decode the LTX, and
    // assert every row is present — i.e. the snapshot's TXID coverage is honest and
    // does not claim commits the image lacks. (The manual `walrust snapshot`
    // command additionally runs a completeness-checked TRUNCATE fold before this —
    // see sync::compact::snapshot — so it fails closed if a watcher pins the WAL.)
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("fold-source.db");
    {
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        // Prevent auto-checkpoint so the rows stay resident in the WAL, not the
        // main DB file — the exact condition a PASSIVE checkpoint could miss.
        conn.pragma_update(None, "wal_autocheckpoint", 0)?;
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT);")?;
        for id in 1..=25 {
            conn.execute(
                "INSERT INTO t (id, value) VALUES (?1, ?2)",
                rusqlite::params![id, format!("row-{id}")],
            )?;
        }
        // Row data now lives in the -wal file; the main DB file is still empty of
        // these rows. Do NOT checkpoint here.
    }

    let storage = MemoryStorage::default();
    let output = snapshot_database_to_storage(&storage, "backups", "fold-source", &db_path).await?;

    let bytes = storage
        .get(&output.key)
        .await?
        .expect("snapshot object should exist");

    let restored = dir.path().join("restored.db");
    walrust_core::legacy_ltx::decode_to_db(std::io::Cursor::new(bytes), &restored)?;

    let conn = rusqlite::Connection::open(&restored)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))?;
    assert_eq!(
        count, 25,
        "all WAL-resident rows must be folded into snapshot"
    );
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    assert_eq!(integrity, "ok");
    Ok(())
}
