//! Independent S3 uploader task.
//!
//! Decouples WAL encoding from S3 uploads:
//! - WAL monitor writes LTX to disk cache → sends TXID to channel
//! - Uploader task reads LTX from cache → uploads to S3
//! - Sequential TXID processing preserves ordering
//! - Crash recovery: pending uploads resume on restart
//!
//! Architecture:
//! ```text
//! WAL Monitor Task          Uploader Task
//!       |                        |
//!   encode_wal()                 |
//!       |                        |
//!   cache.write_ltx()            |
//!       |                        |
//!   tx.send(txid) -------->  rx.recv(txid)
//!       |                        |
//!   continue                 cache.read_ltx()
//!                                |
//!                            upload_to_s3()
//!                                |
//!                          cache.mark_uploaded()
//! ```

use crate::cache::LocalCache;
use crate::retry::RetryPolicy;
use crate::storage::StorageBackend;
use crate::webhook::WebhookSender;
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::mpsc;
// Duration imported via tokio::time::sleep
use tracing::{error, info, warn};

/// Upload notification message
#[derive(Debug, Clone)]
pub enum UploadMessage {
    /// Upload a specific TXID
    Upload(u64),
    /// Graceful shutdown (complete pending uploads, then exit)
    Shutdown,
}

/// Statistics for uploader task
#[derive(Debug, Clone, Default)]
pub struct UploaderStats {
    /// Total uploads attempted
    pub uploads_attempted: u64,
    /// Successful uploads
    pub uploads_succeeded: u64,
    /// Failed uploads (after retries exhausted)
    pub uploads_failed: u64,
    /// Total bytes uploaded
    pub bytes_uploaded: u64,
    /// Last uploaded TXID
    pub last_uploaded_txid: u64,
}

/// Independent S3 uploader task
pub struct Uploader {
    /// Database name (for logging/metrics)
    db_name: String,
    /// Local cache
    cache: Arc<LocalCache>,
    /// Storage backend (S3)
    storage: Arc<dyn StorageBackend>,
    /// S3 key prefix
    prefix: String,
    /// Retry policy
    retry_policy: Arc<RetryPolicy>,
    /// Webhook sender
    webhook_sender: Arc<WebhookSender>,
    /// Upload statistics
    stats: Arc<tokio::sync::Mutex<UploaderStats>>,
}

impl Uploader {
    /// Create a new uploader task
    pub fn new(
        db_name: String,
        cache: Arc<LocalCache>,
        storage: Arc<dyn StorageBackend>,
        prefix: String,
        retry_policy: Arc<RetryPolicy>,
        webhook_sender: Arc<WebhookSender>,
    ) -> Self {
        Self {
            db_name,
            cache,
            storage,
            prefix,
            retry_policy,
            webhook_sender,
            stats: Arc::new(tokio::sync::Mutex::new(UploaderStats::default())),
        }
    }

    /// Run uploader task (blocking, runs until Shutdown message)
    ///
    /// Processes upload messages from channel:
    /// - Upload(txid): Read from cache, upload to S3, mark uploaded
    /// - Shutdown: Complete pending uploads, then exit
    pub async fn run(
        &self,
        mut rx: mpsc::Receiver<UploadMessage>,
    ) -> Result<UploaderStats> {
        info!("[{}] Uploader task started", self.db_name);

        // Resume pending uploads on startup
        self.resume_pending_uploads().await?;

        // Process messages
        loop {
            match rx.recv().await {
                Some(UploadMessage::Upload(txid)) => {
                    if let Err(e) = self.upload_txid(txid).await {
                        error!("[{}] Upload failed for TXID {}: {}", self.db_name, txid, e);
                        // Webhook notification sent inside upload_txid
                    }
                }
                Some(UploadMessage::Shutdown) => {
                    info!("[{}] Uploader received shutdown signal", self.db_name);
                    break;
                }
                None => {
                    warn!("[{}] Upload channel closed, shutting down", self.db_name);
                    break;
                }
            }
        }

        let stats = self.stats.lock().await.clone();
        info!("[{}] Uploader task stopped. Stats: {:?}", self.db_name, stats);

        Ok(stats)
    }

    /// Resume pending uploads from cache on startup
    async fn resume_pending_uploads(&self) -> Result<()> {
        let pending = self.cache.pending_uploads();

        if pending.is_empty() {
            info!("[{}] No pending uploads to resume", self.db_name);
            return Ok(());
        }

        info!("[{}] Resuming {} pending uploads", self.db_name, pending.len());

        for txid in pending {
            if let Err(e) = self.upload_txid(txid).await {
                error!("[{}] Failed to resume upload for TXID {}: {}", self.db_name, txid, e);
                // Continue with other pending uploads
            }
        }

        Ok(())
    }

    /// Upload a single TXID to S3 with retry logic
    async fn upload_txid(&self, txid: u64) -> Result<()> {
        let mut stats = self.stats.lock().await;
        stats.uploads_attempted += 1;
        drop(stats); // Release lock before I/O

        // Read LTX from cache
        let data = self.cache.read_ltx(txid)
            .with_context(|| format!("Failed to read TXID {} from cache", txid))?;

        // Upload with retry loop
        let key = format!("{}/{:08}.ltx", self.prefix, txid);
        let mut attempts = 0u32;
        let max_retries = self.retry_policy.config().max_retries;

        loop {
            attempts += 1;

            match self.storage.upload_bytes(&key, data.clone()).await {
                Ok(_) => {
                    // Upload succeeded!
                    self.cache.mark_uploaded(txid)
                        .context("Failed to mark TXID as uploaded")?;

                    // Update stats
                    let mut stats = self.stats.lock().await;
                    stats.uploads_succeeded += 1;
                    stats.bytes_uploaded += data.len() as u64;
                    stats.last_uploaded_txid = txid;

                    info!("[{}] Uploaded TXID {} ({} bytes)", self.db_name, txid, data.len());

                    return Ok(());
                }
                Err(e) => {
                    let error_kind = crate::retry::classify_error(&e);
                    let is_retryable = matches!(
                        error_kind,
                        crate::retry::ErrorKind::Transient | crate::retry::ErrorKind::Unknown
                    );

                    // Auth errors fail immediately
                    if error_kind == crate::retry::ErrorKind::AuthError {
                        error!("[{}] Auth error uploading TXID {}: {}", self.db_name, txid, e);
                        self.webhook_sender.notify_auth_failure(&self.db_name, &e.to_string()).await;

                        let mut stats = self.stats.lock().await;
                        stats.uploads_failed += 1;

                        return Err(e).context(format!("Auth error uploading TXID {}", txid));
                    }

                    // If not retryable or exhausted retries, fail
                    if !is_retryable || attempts > max_retries + 1 {
                        error!(
                            "[{}] Upload failed for TXID {} after {} attempts: {}",
                            self.db_name, txid, attempts, e
                        );
                        self.webhook_sender
                            .notify_sync_failed(&self.db_name, &e.to_string(), attempts)
                            .await;

                        let mut stats = self.stats.lock().await;
                        stats.uploads_failed += 1;

                        return Err(e).context(format!("Failed to upload TXID {} after retries", txid));
                    }

                    // Retry with exponential backoff
                    let delay = self.retry_policy.calculate_delay(attempts - 1);
                    warn!(
                        "[{}] Upload failed for TXID {}, attempt {}/{}, retrying in {:?}: {}",
                        self.db_name, txid, attempts, max_retries + 1, delay, e
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Get current upload statistics
    pub async fn stats(&self) -> UploaderStats {
        self.stats.lock().await.clone()
    }
}

/// Spawn uploader task and return channel sender
pub fn spawn_uploader(
    uploader: Arc<Uploader>,
) -> mpsc::Sender<UploadMessage> {
    let (tx, rx) = mpsc::channel(1000); // Buffer 1000 upload notifications

    tokio::spawn(async move {
        if let Err(e) = uploader.run(rx).await {
            error!("Uploader task failed: {}", e);
        }
    });

    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::LocalCache;
    use crate::retry::RetryConfig;
    use crate::storage::StorageBackend;
    use crate::webhook::WebhookSender;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Mutex;
    use tempfile::TempDir;
    use tokio::time::{timeout, Duration};

    /// Mock storage backend for testing
    struct MockStorage {
        objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        fail_count: Arc<Mutex<usize>>,
        max_failures: usize,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                objects: Arc::new(Mutex::new(HashMap::new())),
                fail_count: Arc::new(Mutex::new(0)),
                max_failures: 0,
            }
        }

        fn with_failures(max_failures: usize) -> Self {
            Self {
                objects: Arc::new(Mutex::new(HashMap::new())),
                fail_count: Arc::new(Mutex::new(0)),
                max_failures,
            }
        }

        fn get_object(&self, key: &str) -> Option<Vec<u8>> {
            self.objects.lock().unwrap().get(key).cloned()
        }

        fn object_count(&self) -> usize {
            self.objects.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl StorageBackend for MockStorage {
        async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
            let mut fail_count = self.fail_count.lock().unwrap();

            if *fail_count < self.max_failures {
                *fail_count += 1;
                return Err(anyhow::anyhow!("Simulated S3 failure {}/{}", fail_count, self.max_failures));
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
            "mock-bucket"
        }
    }

    fn setup_uploader() -> (Arc<Uploader>, Arc<LocalCache>, Arc<MockStorage>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let storage = Arc::new(MockStorage::new());

        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay_ms: 10,
            max_delay_ms: 100,
            ..Default::default()
        };

        let uploader = Arc::new(Uploader::new(
            "test_db".to_string(),
            cache.clone(),
            storage.clone() as Arc<dyn StorageBackend>,
            "test-prefix".to_string(),
            Arc::new(RetryPolicy::new(retry_config)),
            Arc::new(WebhookSender::new(vec![])),
        ));

        (uploader, cache, storage, temp_dir)
    }

    #[tokio::test]
    async fn test_uploader_basic_upload() {
        let (uploader, cache, storage, _temp) = setup_uploader();

        // Create channel
        let (tx, rx) = mpsc::channel(10);

        // Spawn uploader task
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move {
            uploader_clone.run(rx).await
        });

        // Wait for uploader to start and complete resume
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Write LTX to cache AFTER spawning (so resume doesn't upload it)
        cache.write_ltx(1, b"test data").unwrap();

        // Send upload message
        tx.send(UploadMessage::Upload(1)).await.unwrap();

        // Send shutdown
        tx.send(UploadMessage::Shutdown).await.unwrap();

        // Wait for completion
        let stats = task.await.unwrap().unwrap();

        // Verify upload succeeded
        assert_eq!(stats.uploads_attempted, 1);
        assert_eq!(stats.uploads_succeeded, 1);
        assert_eq!(stats.uploads_failed, 0);
        assert_eq!(stats.last_uploaded_txid, 1);

        // Verify S3 has the object
        assert_eq!(storage.object_count(), 1);
        let data = storage.get_object("test-prefix/00000001.ltx").unwrap();
        assert_eq!(data, b"test data");

        // Verify cache marked as uploaded
        assert_eq!(cache.pending_uploads().len(), 0);
        assert_eq!(cache.last_uploaded_txid(), 1);
    }

    #[tokio::test]
    async fn test_uploader_sequential_processing() {
        let (uploader, cache, storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10);

        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move {
            uploader_clone.run(rx).await
        });

        // Wait for uploader to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Write multiple TXIDs AFTER spawning
        for i in 1..=5 {
            cache.write_ltx(i, format!("data{}", i).as_bytes()).unwrap();
        }

        // Send upload messages
        for i in 1..=5 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();

        // All should succeed
        assert_eq!(stats.uploads_succeeded, 5);
        assert_eq!(stats.last_uploaded_txid, 5);

        // All objects in S3
        assert_eq!(storage.object_count(), 5);

        // Cache should be empty of pending
        assert_eq!(cache.pending_uploads().len(), 0);
    }

    #[tokio::test]
    async fn test_uploader_resume_pending() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let storage = Arc::new(MockStorage::new());

        // Simulate interrupted upload (written to cache but not uploaded)
        cache.write_ltx(1, b"data1").unwrap();
        cache.write_ltx(2, b"data2").unwrap();
        cache.write_ltx(3, b"data3").unwrap();

        // Create uploader
        let retry_config = RetryConfig {
            max_retries: 3,
            base_delay_ms: 10,
            max_delay_ms: 100,
            ..Default::default()
        };

        let uploader = Arc::new(Uploader::new(
            "test_db".to_string(),
            cache.clone(),
            storage.clone() as Arc<dyn StorageBackend>,
            "test-prefix".to_string(),
            Arc::new(RetryPolicy::new(retry_config)),
            Arc::new(WebhookSender::new(vec![])),
        ));

        let (tx, rx) = mpsc::channel(10);

        // Spawn uploader (should auto-resume pending)
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move {
            uploader_clone.run(rx).await
        });

        // Give it time to resume
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Shutdown
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();

        // Should have uploaded all 3 pending
        assert_eq!(stats.uploads_succeeded, 3);
        assert_eq!(storage.object_count(), 3);
        assert_eq!(cache.pending_uploads().len(), 0);
    }

    #[tokio::test]
    async fn test_uploader_retry_on_failure() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());

        // Storage that fails 2 times then succeeds
        let storage = Arc::new(MockStorage::with_failures(2));

        let retry_config = RetryConfig {
            max_retries: 5,
            base_delay_ms: 10,
            max_delay_ms: 100,
            ..Default::default()
        };

        let uploader = Arc::new(Uploader::new(
            "test_db".to_string(),
            cache.clone(),
            storage.clone() as Arc<dyn StorageBackend>,
            "test-prefix".to_string(),
            Arc::new(RetryPolicy::new(retry_config)),
            Arc::new(WebhookSender::new(vec![])),
        ));

        let (tx, rx) = mpsc::channel(10);

        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move {
            uploader_clone.run(rx).await
        });

        // Wait for uploader to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Write and send upload message
        cache.write_ltx(1, b"data").unwrap();
        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();

        // Should eventually succeed after retries
        assert_eq!(stats.uploads_succeeded, 1);
        assert_eq!(stats.uploads_failed, 0);
    }

    #[tokio::test]
    async fn test_uploader_channel_buffer() {
        let (uploader, cache, _storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10); // Small buffer

        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move {
            uploader_clone.run(rx).await
        });

        // Wait for uploader to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Write many TXIDs AFTER spawning
        for i in 1..=100 {
            cache.write_ltx(i, b"data").unwrap();
        }

        // Send many messages (should not block due to buffering)
        for i in 1..=100 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();

        assert_eq!(stats.uploads_succeeded, 100);
    }

    #[tokio::test]
    async fn test_uploader_graceful_shutdown() {
        let (uploader, cache, storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10);

        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move {
            uploader_clone.run(rx).await
        });

        // Wait for uploader to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Write AFTER spawning
        cache.write_ltx(1, b"data1").unwrap();
        cache.write_ltx(2, b"data2").unwrap();

        // Send uploads and immediate shutdown
        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tx.send(UploadMessage::Upload(2)).await.unwrap();
        tx.send(UploadMessage::Shutdown).await.unwrap();

        // Should complete pending uploads before shutdown
        let result = timeout(Duration::from_secs(5), task).await;
        assert!(result.is_ok(), "Uploader should shutdown gracefully");

        let stats = result.unwrap().unwrap().unwrap();

        // Both should be uploaded despite immediate shutdown
        assert_eq!(stats.uploads_succeeded, 2);
    }

    #[tokio::test]
    async fn test_uploader_stats_tracking() {
        let (uploader, cache, _storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10);

        let uploader_clone = uploader.clone();
        tokio::spawn(async move {
            uploader_clone.run(rx).await
        });

        // Wait for uploader to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Write AFTER spawning
        cache.write_ltx(1, &vec![0u8; 100]).unwrap();
        cache.write_ltx(2, &vec![0u8; 200]).unwrap();

        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stats = uploader.stats().await;
        assert_eq!(stats.uploads_attempted, 1);
        assert_eq!(stats.bytes_uploaded, 100);

        tx.send(UploadMessage::Upload(2)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let stats = uploader.stats().await;
        assert_eq!(stats.uploads_attempted, 2);
        assert_eq!(stats.bytes_uploaded, 300);
        assert_eq!(stats.last_uploaded_txid, 2);

        tx.send(UploadMessage::Shutdown).await.unwrap();
    }

    #[tokio::test]
    async fn test_spawn_uploader_helper() {
        let (uploader, cache, storage, _temp) = setup_uploader();

        cache.write_ltx(1, b"test").unwrap();

        // Use helper function
        let tx = spawn_uploader(uploader.clone());

        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify upload succeeded
        assert_eq!(storage.object_count(), 1);

        tx.send(UploadMessage::Shutdown).await.unwrap();
    }
}
