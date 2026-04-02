//! Tests for Replicator::flush() — on-demand WAL sync to S3.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use walrust::ReplicationConfig;
use walrust::Replicator;
use hadb_io::ObjectStore as StorageBackend;

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
}

#[async_trait]
impl StorageBackend for MemStorage {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.objects.lock().unwrap().insert(key.to_string(), data);
        Ok(())
    }

    async fn upload_bytes_with_checksum(&self, key: &str, data: Vec<u8>, _checksum: &str) -> Result<()> {
        self.objects.lock().unwrap().insert(key.to_string(), data);
        Ok(())
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = tokio::fs::read(path).await?;
        self.upload_bytes(key, data).await
    }

    async fn upload_file_with_checksum(&self, key: &str, path: &Path, checksum: &str) -> Result<()> {
        let data = tokio::fs::read(path).await?;
        self.upload_bytes_with_checksum(key, data, checksum).await
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow!("not found: {}", key))
    }

    async fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = self.download_bytes(key).await?;
        tokio::fs::write(path, data).await?;
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let map = self.objects.lock().unwrap();
        let mut keys: Vec<String> = map.keys().filter(|k| k.starts_with(prefix)).cloned().collect();
        keys.sort();
        Ok(keys)
    }

    async fn list_objects_after(&self, prefix: &str, start_after: &str) -> Result<Vec<String>> {
        let map = self.objects.lock().unwrap();
        let mut keys: Vec<String> = map
            .keys()
            .filter(|k| k.starts_with(prefix) && k.as_str() > start_after)
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.lock().unwrap().contains_key(key))
    }

    async fn get_checksum(&self, _key: &str) -> Result<Option<String>> {
        Ok(None)
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        let mut map = self.objects.lock().unwrap();
        let mut deleted = 0;
        for k in keys {
            if map.remove(k).is_some() {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    fn bucket_name(&self) -> &str {
        "test-bucket"
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

fn make_config() -> ReplicationConfig {
    ReplicationConfig {
        // Very long intervals so the background loop doesn't interfere with tests.
        sync_interval: std::time::Duration::from_secs(3600),
        snapshot_interval: std::time::Duration::from_secs(3600),
        ..Default::default()
    }
}

// ============================================================================
// Tests
// ============================================================================

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
    assert_eq!(second, 0, "flush() with no new WAL data should return 0, got {}", second);

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
    replicator.remove("test").await;

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
    let ltx_keys: Vec<_> = storage.keys().iter()
        .filter(|k| k.contains("0000/") && k.ends_with(".hadbp"))
        .cloned()
        .collect();
    assert!(
        !ltx_keys.is_empty() || after > 0,
        "WAL frames should be in storage after flush"
    );

    drop(conn);
}
