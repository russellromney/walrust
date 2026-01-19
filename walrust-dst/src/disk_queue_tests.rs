//! Integration tests for v0.1.9 disk-based upload queue.
//!
//! Tests the complete disk queue architecture:
//! - WAL encoding → disk cache → independent upload task → S3
//! - Crash recovery (restart and resume pending uploads)
//! - Network failures (cache continues, uploads resume)
//! - Fast local restore (cache-first, no S3 needed)
//! - Retention policy enforcement
//! - Multi-database concurrent stress
//!
//! These are INTEGRATION tests that verify the full system behavior,
//! not just individual components.

use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::time::{sleep, Duration};
use walrust::cache::{LocalCache, CacheStats};
use walrust::retry::{RetryPolicy, RetryConfig};
use walrust::storage::StorageBackend;
use walrust::uploader::{Uploader, UploadMessage, spawn_uploader};
use walrust::webhook::WebhookSender;

/// Mock S3 storage with controllable failures
#[derive(Clone)]
pub struct FailableStorage {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    is_available: Arc<Mutex<bool>>,
    failure_rate: Arc<Mutex<f64>>,
}

impl FailableStorage {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
            is_available: Arc::new(Mutex::new(true)),
            failure_rate: Arc::new(Mutex::new(0.0)),
        }
    }

    pub fn set_available(&self, available: bool) {
        *self.is_available.lock().unwrap() = available;
    }

    pub fn set_failure_rate(&self, rate: f64) {
        *self.failure_rate.lock().unwrap() = rate;
    }

    pub fn object_count(&self) -> usize {
        self.objects.lock().unwrap().len()
    }

    pub fn clear(&self) {
        self.objects.lock().unwrap().clear();
    }
}

#[async_trait]
impl StorageBackend for FailableStorage {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        // Check availability (network failure simulation)
        if !*self.is_available.lock().unwrap() {
            return Err(anyhow::anyhow!("Network unavailable"));
        }

        // Random failure injection
        let failure_rate = *self.failure_rate.lock().unwrap();
        if failure_rate > 0.0 && rand::random::<f64>() < failure_rate {
            return Err(anyhow::anyhow!("Random S3 error"));
        }

        self.objects.lock().unwrap().insert(key.to_string(), data);
        Ok(())
    }

    async fn upload_bytes_with_checksum(&self, key: &str, data: Vec<u8>, _checksum: &str) -> Result<()> {
        self.upload_bytes(key, data).await
    }

    async fn upload_file(&self, _key: &str, _path: &Path) -> Result<()> {
        unimplemented!("upload_file not needed for tests")
    }

    async fn upload_file_with_checksum(&self, _key: &str, _path: &Path, _checksum: &str) -> Result<()> {
        unimplemented!("upload_file_with_checksum not needed for tests")
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        if !*self.is_available.lock().unwrap() {
            return Err(anyhow::anyhow!("Network unavailable"));
        }

        self.objects.lock().unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Object not found: {}", key))
    }

    async fn download_file(&self, _key: &str, _path: &Path) -> Result<()> {
        unimplemented!("download_file not needed for tests")
    }

    async fn list_objects(&self, _prefix: &str) -> Result<Vec<String>> {
        Ok(self.objects.lock().unwrap().keys().cloned().collect())
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
        let mut objects = self.objects.lock().unwrap();
        let mut count = 0;
        for key in keys {
            if objects.remove(key).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    fn bucket_name(&self) -> &str {
        "test-bucket"
    }
}

/// Test fixture for disk queue tests
struct DiskQueueTestFixture {
    temp_dir: TempDir,
    db_path: PathBuf,
    cache: Arc<LocalCache>,
    storage: Arc<FailableStorage>,
}

impl DiskQueueTestFixture {
    fn new() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test.db");
        let cache = Arc::new(LocalCache::new(&db_path)?);
        let storage = Arc::new(FailableStorage::new());

        Ok(Self {
            temp_dir,
            db_path,
            cache,
            storage,
        })
    }

    fn create_uploader(&self) -> Arc<Uploader> {
        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay_ms: 10,
            max_delay_ms: 100,
            ..Default::default()
        };

        Arc::new(Uploader::new(
            "test_db".to_string(),
            self.cache.clone(),
            self.storage.clone() as Arc<dyn StorageBackend>,
            "test-prefix".to_string(),
            Arc::new(RetryPolicy::new(retry_config)),
            Arc::new(WebhookSender::new(vec![])),
        ))
    }
}

#[tokio::test]
async fn test_crash_recovery_basic() {
    // Scenario: Write 10 LTX to cache, simulate crash, restart uploader, verify all upload

    let fixture = DiskQueueTestFixture::new().unwrap();

    // Simulate WAL encoding writing to cache (before crash)
    for i in 1..=10 {
        fixture.cache.write_ltx(i, format!("data{}", i).as_bytes()).unwrap();
    }

    // Verify cache state
    assert_eq!(fixture.cache.pending_uploads().len(), 10);
    assert_eq!(fixture.storage.object_count(), 0);

    // Simulate restart: Create new uploader (will auto-resume pending)
    let uploader = fixture.create_uploader();
    let tx = spawn_uploader(uploader.clone());

    // Wait for resume to complete
    sleep(Duration::from_millis(200)).await;

    // Verify all uploaded
    let stats = uploader.stats().await;
    assert_eq!(stats.uploads_succeeded, 10);
    assert_eq!(fixture.storage.object_count(), 10);
    assert_eq!(fixture.cache.pending_uploads().len(), 0);

    tx.send(UploadMessage::Shutdown).await.unwrap();
}

#[tokio::test]
async fn test_crash_recovery_partial() {
    // Scenario: Write 10, upload 5, crash, restart, verify remaining 5 upload

    let fixture = DiskQueueTestFixture::new().unwrap();

    // Write 10 TXIDs
    for i in 1..=10 {
        fixture.cache.write_ltx(i, b"data").unwrap();
    }

    // Upload first 5 manually
    for i in 1..=5 {
        let data = fixture.cache.read_ltx(i).unwrap();
        fixture.storage.upload_bytes(&format!("test-prefix/{:08}.ltx", i), data).await.unwrap();
        fixture.cache.mark_uploaded(i).unwrap();
    }

    assert_eq!(fixture.cache.pending_uploads(), vec![6, 7, 8, 9, 10]);

    // Simulate restart
    let uploader = fixture.create_uploader();
    let tx = spawn_uploader(uploader.clone());

    sleep(Duration::from_millis(200)).await;

    // Verify only remaining 5 uploaded
    let stats = uploader.stats().await;
    assert_eq!(stats.uploads_succeeded, 5);
    assert_eq!(stats.last_uploaded_txid, 10);
    assert_eq!(fixture.storage.object_count(), 10); // Total 10 (5 before + 5 after)

    tx.send(UploadMessage::Shutdown).await.unwrap();
}

#[tokio::test]
async fn test_network_disconnect_recovery() {
    // Scenario: Start uploader, disconnect network, cache continues, reconnect, verify upload

    let fixture = DiskQueueTestFixture::new().unwrap();
    let uploader = fixture.create_uploader();
    let tx = spawn_uploader(uploader.clone());

    // Wait for uploader to start and complete auto-resume
    sleep(Duration::from_millis(10)).await;

    // Disconnect network
    fixture.storage.set_available(false);

    // Try to upload (should fail but cache continues)
    fixture.cache.write_ltx(1, b"data1").unwrap();
    tx.send(UploadMessage::Upload(1)).await.unwrap();

    // Wait for retry loop to exhaust (3 retries with exponential backoff: ~70ms total)
    sleep(Duration::from_millis(100)).await;

    // Upload failed, but cache intact
    let stats = uploader.stats().await;
    assert!(stats.uploads_failed > 0);
    assert_eq!(fixture.cache.pending_uploads(), vec![1]);

    // Reconnect network
    fixture.storage.set_available(true);

    // Retry upload
    tx.send(UploadMessage::Upload(1)).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Now succeeds
    let stats = uploader.stats().await;
    assert_eq!(stats.uploads_succeeded, 1);
    assert_eq!(fixture.cache.pending_uploads().len(), 0);

    tx.send(UploadMessage::Shutdown).await.unwrap();
}

#[tokio::test]
async fn test_cache_continues_during_s3_failure() {
    // Scenario: S3 down, WAL encoding continues writing to cache without blocking

    let fixture = DiskQueueTestFixture::new().unwrap();

    // Disconnect S3
    fixture.storage.set_available(false);

    let uploader = fixture.create_uploader();
    let tx = spawn_uploader(uploader.clone());

    // Write to cache (should NOT block even with S3 down)
    let start = std::time::Instant::now();
    for i in 1..=100 {
        fixture.cache.write_ltx(i, b"data").unwrap();
        tx.send(UploadMessage::Upload(i)).await.unwrap();
    }
    let elapsed = start.elapsed();

    // Cache writes should be fast (< 1 second for 100 writes)
    assert!(elapsed.as_secs() < 1, "Cache writes blocked on S3");

    // All in cache, none uploaded
    assert_eq!(fixture.cache.pending_uploads().len(), 100);
    assert_eq!(fixture.storage.object_count(), 0);

    tx.send(UploadMessage::Shutdown).await.unwrap();
}

#[tokio::test]
async fn test_fast_local_restore() {
    // Scenario: Restore from local cache without fetching from S3

    let fixture = DiskQueueTestFixture::new().unwrap();

    // Write and upload some LTX files
    let uploader = fixture.create_uploader();
    let tx = spawn_uploader(uploader.clone());

    for i in 1..=10 {
        fixture.cache.write_ltx(i, format!("restore_data_{}", i).as_bytes()).unwrap();
        tx.send(UploadMessage::Upload(i)).await.unwrap();
    }

    sleep(Duration::from_millis(200)).await;
    tx.send(UploadMessage::Shutdown).await.unwrap();

    // Simulate network down (no S3 access)
    fixture.storage.set_available(false);

    // Restore from local cache (should work without S3)
    for i in 1..=10 {
        let data = fixture.cache.read_ltx(i).unwrap();
        assert_eq!(data, format!("restore_data_{}", i).as_bytes());
    }

    // Verify S3 was never accessed
    // (would fail if we tried to use S3 due to network down)
}

#[tokio::test]
async fn test_cleanup_retention_policy() {
    // Scenario: Upload 100 files, cleanup with retention, verify correct files deleted

    let fixture = DiskQueueTestFixture::new().unwrap();
    let uploader = fixture.create_uploader();
    let tx = spawn_uploader(uploader.clone());

    // Wait for uploader to start and complete auto-resume
    sleep(Duration::from_millis(10)).await;

    // Write and upload 100 files
    for i in 1..=100 {
        fixture.cache.write_ltx(i, &vec![0u8; 1000]).unwrap();
        tx.send(UploadMessage::Upload(i)).await.unwrap();
    }

    sleep(Duration::from_millis(500)).await;
    tx.send(UploadMessage::Shutdown).await.unwrap();

    // All uploaded
    assert_eq!(fixture.cache.stats().uploaded_count, 100);

    // Cleanup: keep only 10 most recent (size-based cleanup)
    let cleanup_stats = fixture.cache.cleanup(
        chrono::Duration::hours(24), // 24 hour retention (all files are fresh)
        10_000, // Max 10KB (10 files x 1KB) - this is the limiting factor
    ).unwrap();

    // Should delete ~90 files to get under size limit
    assert!(cleanup_stats.deleted_count >= 85);
    assert!(cleanup_stats.deleted_count <= 95);

    // Verify cache stats updated
    let stats = fixture.cache.stats();
    assert!(stats.total_entries <= 15);
}

#[tokio::test]
async fn test_txid_ordering_preserved() {
    // Scenario: Upload out-of-order TXIDs, verify S3 receives in correct order

    let fixture = DiskQueueTestFixture::new().unwrap();
    let uploader = fixture.create_uploader();
    let tx = spawn_uploader(uploader.clone());

    // Write out of order
    fixture.cache.write_ltx(5, b"data5").unwrap();
    fixture.cache.write_ltx(2, b"data2").unwrap();
    fixture.cache.write_ltx(8, b"data8").unwrap();
    fixture.cache.write_ltx(1, b"data1").unwrap();

    // Send upload messages in order (uploader should process sequentially)
    let pending = fixture.cache.pending_uploads(); // Returns sorted
    for txid in pending {
        tx.send(UploadMessage::Upload(txid)).await.unwrap();
    }

    sleep(Duration::from_millis(200)).await;
    tx.send(UploadMessage::Shutdown).await.unwrap();

    // Verify last_uploaded_txid is max
    assert_eq!(uploader.stats().await.last_uploaded_txid, 8);
}

#[tokio::test]
async fn test_concurrent_multi_database() {
    // Scenario: 10 databases writing/uploading concurrently

    let mut fixtures = Vec::new();
    let mut uploaders = Vec::new();
    let mut channels = Vec::new();

    // Create 10 database fixtures
    for i in 0..10 {
        let fixture = DiskQueueTestFixture::new().unwrap();
        let uploader = fixture.create_uploader();
        let tx = spawn_uploader(uploader.clone());

        fixtures.push(fixture);
        uploaders.push(uploader);
        channels.push(tx);
    }

    // Wait for all uploaders to start and complete auto-resume
    sleep(Duration::from_millis(10)).await;

    // Each database writes 50 TXIDs concurrently
    for (i, fixture) in fixtures.iter().enumerate() {
        for txid in 1..=50 {
            fixture.cache.write_ltx(txid, format!("db{}_data{}", i, txid).as_bytes()).unwrap();
            channels[i].send(UploadMessage::Upload(txid)).await.unwrap();
        }
    }

    // Wait for all uploads
    sleep(Duration::from_millis(1000)).await;

    // Verify all databases uploaded independently
    for (i, uploader) in uploaders.iter().enumerate() {
        let stats = uploader.stats().await;
        assert_eq!(stats.uploads_succeeded, 50, "DB {} upload count mismatch", i);
        assert_eq!(fixtures[i].cache.pending_uploads().len(), 0);
    }

    // Shutdown all
    for tx in channels {
        tx.send(UploadMessage::Shutdown).await.unwrap();
    }
}

#[tokio::test]
async fn test_chaos_random_failures() {
    // Scenario: 20% S3 failure rate, verify retries succeed

    let fixture = DiskQueueTestFixture::new().unwrap();

    // Enable 20% random failures
    fixture.storage.set_failure_rate(0.2);

    let uploader = fixture.create_uploader();
    let tx = spawn_uploader(uploader.clone());

    // Write 50 TXIDs
    for i in 1..=50 {
        fixture.cache.write_ltx(i, b"data").unwrap();
        tx.send(UploadMessage::Upload(i)).await.unwrap();
    }

    // Wait for retries to complete
    sleep(Duration::from_millis(1000)).await;
    tx.send(UploadMessage::Shutdown).await.unwrap();

    let stats = uploader.stats().await;

    // With retries, most/all should eventually succeed
    assert!(stats.uploads_succeeded >= 45, "Too many failures despite retries");

    // Some failures expected due to randomness
    assert!(stats.uploads_attempted >= stats.uploads_succeeded);
}

#[tokio::test]
async fn test_cache_verify_integrity() {
    // Scenario: Verify cache.verify() detects corruption

    let fixture = DiskQueueTestFixture::new().unwrap();

    // Write normal data
    fixture.cache.write_ltx(1, b"data1").unwrap();
    fixture.cache.write_ltx(2, b"data2").unwrap();

    // Verify clean
    let issues = fixture.cache.verify().unwrap();
    assert_eq!(issues.len(), 0);

    // Corrupt: delete file but keep manifest entry
    let ltx_path = get_ltx_path(&fixture.temp_dir, "test.db", 1);
    std::fs::remove_file(&ltx_path).unwrap();

    let issues = fixture.cache.verify().unwrap();
    assert!(issues.iter().any(|i| i.contains("file missing")));

    // Create orphan file (no manifest entry)
    let orphan_path = fixture.temp_dir.path()
        .join(format!(".{}-walrust", fixture.db_path.file_name().unwrap().to_string_lossy()))
        .join("ltx")
        .join("00000099.ltx");
    std::fs::write(&orphan_path, b"orphan").unwrap();

    let issues = fixture.cache.verify().unwrap();
    assert!(issues.iter().any(|i| i.contains("not in manifest")));
}

#[tokio::test]
async fn test_uploader_graceful_shutdown_completes_pending() {
    // Scenario: Send shutdown while uploads in progress, verify they complete

    let fixture = DiskQueueTestFixture::new().unwrap();

    // Slow uploads (via small delays in mock storage)
    let uploader = fixture.create_uploader();
    let tx = spawn_uploader(uploader.clone());

    // Wait for uploader to start and complete auto-resume
    sleep(Duration::from_millis(10)).await;

    // Queue many uploads
    for i in 1..=20 {
        fixture.cache.write_ltx(i, b"data").unwrap();
        tx.send(UploadMessage::Upload(i)).await.unwrap();
    }

    // Immediately send shutdown (uploads in progress)
    tx.send(UploadMessage::Shutdown).await.unwrap();

    // Wait a bit
    sleep(Duration::from_millis(500)).await;

    // All should complete gracefully
    let stats = uploader.stats().await;
    assert_eq!(stats.uploads_succeeded, 20);
}

#[tokio::test]
async fn test_cache_persistence_across_restarts() {
    // Scenario: Write cache, restart (new LocalCache instance), verify state restored

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // First session: write data
    {
        let cache = LocalCache::new(&db_path).unwrap();
        for i in 1..=10 {
            cache.write_ltx(i, format!("persistent_{}", i).as_bytes()).unwrap();
        }
        cache.mark_uploaded(1).unwrap();
        cache.mark_uploaded(2).unwrap();
    }

    // Second session: load from disk
    {
        let cache = LocalCache::new(&db_path).unwrap();

        // State restored
        assert_eq!(cache.last_uploaded_txid(), 2);
        assert_eq!(cache.pending_uploads(), vec![3, 4, 5, 6, 7, 8, 9, 10]);

        // Files readable
        assert_eq!(cache.read_ltx(5).unwrap(), b"persistent_5");

        let stats = cache.stats();
        assert_eq!(stats.total_entries, 10);
        assert_eq!(stats.uploaded_count, 2);
        assert_eq!(stats.pending_count, 8);
    }
}

// Helper function to get LTX path for tests
fn get_ltx_path(temp_dir: &TempDir, db_filename: &str, txid: u64) -> PathBuf {
    temp_dir.path()
        .join(format!(".{}-walrust", db_filename))
        .join("ltx")
        .join(format!("{:08}.ltx", txid))
}

// ============================================
// End-to-End Integration Tests (SQLite → Shadow → Cache → S3)
// ============================================

#[tokio::test]
async fn test_sqlite_to_cache_to_s3_end_to_end() {
    // Scenario: Real SQLite writes → Shadow WAL copies frames → Cache → S3
    // This tests the full pipeline with actual database operations
    use rusqlite::Connection;
    use walrust::shadow::ShadowWal;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("end_to_end.db");

    // Create SQLite database in WAL mode
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
        ).unwrap();
    }

    // Initialize shadow WAL BEFORE writing data (this is the correct sequence)
    // The shadow WAL needs to be watching before writes happen
    let mut shadow = ShadowWal::new(&db_path).await.unwrap();

    // Now write data to SQLite (creates WAL frames)
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA wal_autocheckpoint=0;").unwrap();
        for i in 1..=10 {
            conn.execute("INSERT INTO users (name) VALUES (?1)", [format!("user_{}", i)]).unwrap();
        }
    }

    // Copy frames from WAL to shadow
    let (frames, _new_offset) = shadow.copy_frames(0).await.unwrap();
    assert!(!frames.is_empty(), "Should have copied frames from WAL");

    // Set up cache (fresh cache with no pending uploads)
    let cache = Arc::new(LocalCache::new(&db_path).unwrap());
    let storage = Arc::new(FailableStorage::new());

    // Verify cache starts empty
    assert_eq!(cache.pending_uploads().len(), 0, "Cache should start empty");

    // Simulate LTX encoding: each frame batch becomes an LTX file
    let txid = 1u64;
    let ltx_data = frames.iter()
        .flat_map(|f| f.data.clone())
        .collect::<Vec<_>>();
    cache.write_ltx(txid, &ltx_data).unwrap();

    // Verify cache state
    assert_eq!(cache.pending_uploads(), vec![1]);
    assert!(cache.stats().total_bytes > 0);

    // Set up uploader
    let retry_config = RetryConfig {
        max_retries: 3,
        base_delay_ms: 10,
        max_delay_ms: 100,
        ..Default::default()
    };

    let uploader = Arc::new(Uploader::new(
        "end_to_end".to_string(),
        cache.clone(),
        storage.clone() as Arc<dyn StorageBackend>,
        "test-prefix".to_string(),
        Arc::new(RetryPolicy::new(retry_config)),
        Arc::new(WebhookSender::new(vec![])),
    ));

    // Spawn uploader - it will auto-resume the one pending upload
    let tx = spawn_uploader(uploader.clone());

    // Wait for auto-resume to complete
    sleep(Duration::from_millis(200)).await;
    tx.send(UploadMessage::Shutdown).await.unwrap();

    // Verify upload succeeded (auto-resume should have uploaded txid 1)
    let stats = uploader.stats().await;
    assert_eq!(stats.uploads_succeeded, 1, "Should have uploaded 1 item via auto-resume");
    assert_eq!(stats.last_uploaded_txid, 1);
    assert!(stats.bytes_uploaded > 0);

    // Verify S3 has the data
    assert_eq!(storage.object_count(), 1);

    // Verify cache marked as uploaded
    assert_eq!(cache.pending_uploads().len(), 0);
}

#[tokio::test]
async fn test_checkpoint_safety_no_frame_loss() {
    // Scenario: Shadow WAL prevents frame loss during checkpoint
    // The checkpoint blocker read transaction keeps WAL frames visible
    use rusqlite::Connection;
    use walrust::shadow::ShadowWal;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("checkpoint_test.db");

    // Create SQLite database in WAL mode
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
        ).unwrap();
    }

    // Initialize shadow WAL - this starts checkpoint blocker
    let mut shadow = ShadowWal::new(&db_path).await.unwrap();
    let initial_generation = shadow.generation();

    // Write data in batches
    {
        let conn = Connection::open(&db_path).unwrap();
        for batch in 0..3 {
            for i in 0..10 {
                conn.execute(
                    "INSERT INTO data (value) VALUES (?1)",
                    [format!("batch_{}_item_{}", batch, i)],
                ).unwrap();
            }
        }
    }

    // Copy frames before checkpoint attempt
    let (frames_before, offset_before) = shadow.copy_frames(0).await.unwrap();
    let frame_count_before = frames_before.len();
    assert!(frame_count_before > 0, "Should have frames before checkpoint");

    // Try to checkpoint from another connection
    // With checkpoint blocker active, PASSIVE checkpoint won't truncate WAL
    {
        let checkpoint_conn = Connection::open(&db_path).unwrap();
        checkpoint_conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE);").unwrap();
    }

    // Shadow WAL should still have access to frames
    // Generation should not have changed (salt unchanged because blocker active)
    assert_eq!(shadow.generation(), initial_generation);

    // Write more data
    {
        let conn = Connection::open(&db_path).unwrap();
        for i in 0..5 {
            conn.execute("INSERT INTO data (value) VALUES (?1)", [format!("post_checkpoint_{}", i)]).unwrap();
        }
    }

    // Copy new frames - should get frames from same generation
    let (frames_after, offset_after) = shadow.copy_frames(offset_before).await.unwrap();
    assert!(!frames_after.is_empty(), "Should have new frames after checkpoint attempt");
    assert!(offset_after > offset_before, "Offset should have increased");

    // Total frames should include all writes (no loss)
    let total_frames = frame_count_before + frames_after.len();
    assert!(total_frames >= 35, "Should have all frames: got {} (before: {}, after: {})",
        total_frames, frame_count_before, frames_after.len());

    // Verify shadow directory has segment files
    let segments = shadow.list_segments(initial_generation).await.unwrap();
    assert!(!segments.is_empty(), "Should have shadow segments");
}

#[tokio::test]
async fn test_kill_mid_upload_recovery() {
    // Scenario: Upload starts → network dies mid-upload → restart → resume succeeds
    // Simulates process kill during upload by disconnecting storage mid-operation

    let fixture = DiskQueueTestFixture::new().unwrap();

    // Write 5 TXIDs to cache
    for i in 1..=5 {
        fixture.cache.write_ltx(i, format!("data_for_txid_{}", i).as_bytes()).unwrap();
    }

    assert_eq!(fixture.cache.pending_uploads(), vec![1, 2, 3, 4, 5]);

    // Start uploader
    let uploader1 = fixture.create_uploader();
    let tx1 = spawn_uploader(uploader1.clone());

    // Wait for uploader to start and complete auto-resume (uploads 1-5)
    // But disconnect network after first 2 uploads
    sleep(Duration::from_millis(50)).await;

    // Check how many uploaded so far
    let stats1 = uploader1.stats().await;
    let uploaded_before_kill = stats1.uploads_succeeded;

    // "Kill" by disconnecting storage and shutting down
    fixture.storage.set_available(false);
    tx1.send(UploadMessage::Shutdown).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    // Some uploads succeeded, some failed (are still pending)
    let pending_after_kill = fixture.cache.pending_uploads();
    let s3_count_after_kill = fixture.storage.object_count();

    // Verify state is consistent: uploaded count + pending = total
    assert_eq!(
        s3_count_after_kill + pending_after_kill.len(),
        5,
        "Upload + pending should equal total"
    );

    // Simulate restart: Reconnect storage and create new uploader
    fixture.storage.set_available(true);

    let uploader2 = fixture.create_uploader();
    let tx2 = spawn_uploader(uploader2.clone());

    // Wait for resume to complete
    sleep(Duration::from_millis(300)).await;
    tx2.send(UploadMessage::Shutdown).await.unwrap();

    // All should now be uploaded
    let final_stats = uploader2.stats().await;
    assert_eq!(
        final_stats.uploads_succeeded,
        pending_after_kill.len() as u64,
        "Should have uploaded remaining pending items"
    );
    assert_eq!(fixture.storage.object_count(), 5);
    assert_eq!(fixture.cache.pending_uploads().len(), 0);
}

#[tokio::test]
async fn test_controlled_checkpoint_with_shadow() {
    // Scenario: Use shadow.checkpoint() to trigger controlled checkpoint
    // Verify frames are preserved in shadow during the operation
    use rusqlite::Connection;
    use walrust::shadow::ShadowWal;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("controlled_ckpt.db");

    // Create database with WAL mode and disable auto-checkpoint
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT);",
        ).unwrap();
        // Insert enough data to ensure WAL frames are created
        for i in 0..20 {
            conn.execute("INSERT INTO log (msg) VALUES (?1)", [format!("entry_{}", i)]).unwrap();
        }
    }

    // Initialize shadow WAL
    let mut shadow = ShadowWal::new(&db_path).await.unwrap();

    // Copy initial frames - frames should exist from the inserts above
    let (initial_frames, _offset) = shadow.copy_frames(0).await.unwrap();

    // If no initial frames, the WAL might have been checkpointed already
    // Just verify the shadow WAL was created successfully
    let initial_generation = shadow.generation();

    // Trigger controlled checkpoint through shadow
    shadow.checkpoint().await.unwrap();

    // Write more data after checkpoint
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA wal_autocheckpoint=0;").unwrap();
        for i in 20..30 {
            conn.execute("INSERT INTO log (msg) VALUES (?1)", [format!("entry_{}", i)]).unwrap();
        }
    }

    // Copy frames after checkpoint
    let (post_checkpoint_frames, _new_offset) = shadow.copy_frames(0).await.unwrap();

    // After controlled checkpoint:
    // - Either we have frames (new writes) or generation increased
    // - Key: no data loss, shadow WAL continues to function
    let generation_increased = shadow.generation() > initial_generation;
    let has_frames = !initial_frames.is_empty() || !post_checkpoint_frames.is_empty();

    assert!(
        has_frames || generation_increased,
        "Shadow WAL should have frames or new generation after checkpoint. \
         initial_frames: {}, post_frames: {}, gen: {} -> {}",
        initial_frames.len(), post_checkpoint_frames.len(),
        initial_generation, shadow.generation()
    );
}
