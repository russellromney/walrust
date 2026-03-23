//! Concurrent S3 uploader task.
//!
//! Decouples WAL encoding from S3 uploads:
//! - WAL monitor writes LTX to disk cache → sends TXID to channel
//! - Uploader task reads LTX from cache → uploads to S3 (up to N concurrently)
//! - Crash recovery: pending uploads resume on restart
//!
//! Architecture:
//! ```text
//! WAL Monitor Task          Uploader Task (JoinSet, max N concurrent)
//!       |                        |
//!   encode_wal()                 |
//!       |                        |
//!   cache.write_ltx()            |
//!       |                        |
//!   tx.send(txid) -------->  rx.recv(txid)
//!       |                        |
//!   continue                 spawn upload task ──┐
//!                            spawn upload task ──┤ (up to max_concurrent)
//!                            spawn upload task ──┘
//!                                |
//!                            reap completed:
//!                              cache.read_ltx()
//!                              upload_to_s3()
//!                              cache.mark_uploaded()
//! ```

use crate::cache::LocalCache;
use crate::retry::RetryPolicy;
use crate::storage::StorageBackend;
use crate::webhook::WebhookSender;
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
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

/// Shared context for upload tasks spawned into JoinSet.
/// Cloned into each spawned task to avoid &self lifetime issues.
#[derive(Clone)]
struct UploadTaskContext {
    db_name: String,
    cache: Arc<LocalCache>,
    storage: Arc<dyn StorageBackend>,
    prefix: String,
    retry_policy: Arc<RetryPolicy>,
    webhook_sender: Arc<WebhookSender>,
    stats: Arc<tokio::sync::Mutex<UploaderStats>>,
}

/// Result of a single upload task
struct UploadResult {
    _txid: u64,
}

impl UploadTaskContext {
    /// Upload a single TXID to S3 with retry logic
    async fn upload_txid(&self, txid: u64) -> Result<UploadResult> {
        {
            let mut stats = self.stats.lock().await;
            stats.uploads_attempted += 1;
        }

        // Read LTX from cache
        let data = self.cache.read_ltx(txid)
            .with_context(|| format!("Failed to read TXID {} from cache", txid))?;

        let data_len = data.len() as u64;

        // Upload with retry loop
        let key = format!("{}/{:08}.ltx", self.prefix, txid);
        let mut attempts = 0u32;
        let max_retries = self.retry_policy.config().max_retries;

        loop {
            attempts += 1;

            match self.storage.upload_bytes(&key, data.clone()).await {
                Ok(_) => {
                    self.cache.mark_uploaded(txid)
                        .context("Failed to mark TXID as uploaded")?;

                    let mut stats = self.stats.lock().await;
                    stats.uploads_succeeded += 1;
                    stats.bytes_uploaded += data_len;
                    if txid > stats.last_uploaded_txid {
                        stats.last_uploaded_txid = txid;
                    }

                    info!("[{}] Uploaded TXID {} ({} bytes)", self.db_name, txid, data_len);

                    return Ok(UploadResult { _txid: txid });
                }
                Err(e) => {
                    let error_kind = crate::retry::classify_error(&e);
                    let is_retryable = matches!(
                        error_kind,
                        crate::retry::ErrorKind::Transient | crate::retry::ErrorKind::Unknown
                    );

                    if error_kind == crate::retry::ErrorKind::AuthError {
                        error!("[{}] Auth error uploading TXID {}: {}", self.db_name, txid, e);
                        self.webhook_sender.notify_auth_failure(&self.db_name, &e.to_string()).await;

                        let mut stats = self.stats.lock().await;
                        stats.uploads_failed += 1;

                        return Err(e).context(format!("Auth error uploading TXID {}", txid));
                    }

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
}

/// Concurrent S3 uploader task
pub struct Uploader {
    ctx: UploadTaskContext,
    /// Max concurrent upload tasks
    max_concurrent: usize,
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
        max_concurrent: usize,
    ) -> Self {
        Self {
            ctx: UploadTaskContext {
                db_name,
                cache,
                storage,
                prefix,
                retry_policy,
                webhook_sender,
                stats: Arc::new(tokio::sync::Mutex::new(UploaderStats::default())),
            },
            max_concurrent: max_concurrent.max(1), // At least 1
        }
    }

    /// Run uploader task (blocking, runs until Shutdown message or channel close)
    ///
    /// Processes upload messages from channel with up to max_concurrent
    /// uploads in flight simultaneously via JoinSet.
    pub async fn run(
        &self,
        mut rx: mpsc::Receiver<UploadMessage>,
    ) -> Result<UploaderStats> {
        info!("[{}] Uploader task started (max_concurrent={})", self.ctx.db_name, self.max_concurrent);

        let mut in_flight: JoinSet<Result<UploadResult>> = JoinSet::new();

        // Resume pending uploads on startup
        let pending = self.ctx.cache.pending_uploads();
        if !pending.is_empty() {
            info!("[{}] Resuming {} pending uploads", self.ctx.db_name, pending.len());
            for txid in pending {
                // Wait for a slot if at capacity
                while in_flight.len() >= self.max_concurrent {
                    if let Some(result) = in_flight.join_next().await {
                        Self::handle_join_result(&self.ctx.db_name, result);
                    }
                }
                let ctx = self.ctx.clone();
                in_flight.spawn(async move { ctx.upload_txid(txid).await });
            }
        } else {
            info!("[{}] No pending uploads to resume", self.ctx.db_name);
        }

        // Process messages
        loop {
            tokio::select! {
                // Accept new uploads when under concurrency limit
                msg = rx.recv(), if in_flight.len() < self.max_concurrent => {
                    match msg {
                        Some(UploadMessage::Upload(txid)) => {
                            let ctx = self.ctx.clone();
                            in_flight.spawn(async move { ctx.upload_txid(txid).await });
                        }
                        Some(UploadMessage::Shutdown) => {
                            info!("[{}] Uploader received shutdown signal, draining {} in-flight", self.ctx.db_name, in_flight.len());
                            while let Some(result) = in_flight.join_next().await {
                                Self::handle_join_result(&self.ctx.db_name, result);
                            }
                            break;
                        }
                        None => {
                            info!("[{}] Upload channel closed, draining {} in-flight", self.ctx.db_name, in_flight.len());
                            while let Some(result) = in_flight.join_next().await {
                                Self::handle_join_result(&self.ctx.db_name, result);
                            }
                            break;
                        }
                    }
                }
                // Reap completed uploads when at capacity (or any time one finishes)
                Some(result) = in_flight.join_next() => {
                    Self::handle_join_result(&self.ctx.db_name, result);
                }
            }
        }

        let stats = self.ctx.stats.lock().await.clone();
        info!("[{}] Uploader task stopped. Stats: {:?}", self.ctx.db_name, stats);

        Ok(stats)
    }

    /// Handle a JoinSet result
    fn handle_join_result(db_name: &str, result: Result<Result<UploadResult>, tokio::task::JoinError>) {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                // Stats already updated inside upload_txid
                error!("[{}] Upload task failed: {}", db_name, e);
            }
            Err(e) => {
                error!("[{}] Upload task panicked: {}", db_name, e);
            }
        }
    }

    /// Get current upload statistics
    pub async fn stats(&self) -> UploaderStats {
        self.ctx.stats.lock().await.clone()
    }
}

/// Spawn uploader task and return channel sender + join handle.
///
/// The JoinHandle allows callers to await completion after sending Shutdown,
/// ensuring in-flight uploads finish before the runtime exits.
pub fn spawn_uploader(
    uploader: Arc<Uploader>,
) -> (mpsc::Sender<UploadMessage>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(1000); // Buffer 1000 upload notifications

    let handle = tokio::spawn(async move {
        if let Err(e) = uploader.run(rx).await {
            error!("Uploader task failed: {}", e);
        }
    });

    (tx, handle)
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tempfile::TempDir;
    use tokio::time::{timeout, Duration};

    /// Mock storage backend for testing
    struct MockStorage {
        objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
        fail_count: Arc<Mutex<usize>>,
        max_failures: usize,
        /// Optional upload delay for concurrency testing
        upload_delay: Option<Duration>,
        /// Track peak concurrent uploads
        active_uploads: Arc<AtomicUsize>,
        peak_concurrent: Arc<AtomicUsize>,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                objects: Arc::new(Mutex::new(HashMap::new())),
                fail_count: Arc::new(Mutex::new(0)),
                max_failures: 0,
                upload_delay: None,
                active_uploads: Arc::new(AtomicUsize::new(0)),
                peak_concurrent: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_failures(max_failures: usize) -> Self {
            Self {
                max_failures,
                ..Self::new()
            }
        }

        fn with_delay(delay: Duration) -> Self {
            Self {
                upload_delay: Some(delay),
                ..Self::new()
            }
        }

        fn get_object(&self, key: &str) -> Option<Vec<u8>> {
            self.objects.lock().unwrap().get(key).cloned()
        }

        fn object_count(&self) -> usize {
            self.objects.lock().unwrap().len()
        }

        fn peak_concurrent(&self) -> usize {
            self.peak_concurrent.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl StorageBackend for MockStorage {
        async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
            // Track concurrency
            let active = self.active_uploads.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_concurrent.fetch_max(active, Ordering::SeqCst);

            // Simulate delay if configured
            if let Some(delay) = self.upload_delay {
                tokio::time::sleep(delay).await;
            }

            let result = {
                let mut fail_count = self.fail_count.lock().unwrap();
                if *fail_count < self.max_failures {
                    *fail_count += 1;
                    Err(anyhow::anyhow!("Simulated S3 failure {}/{}", fail_count, self.max_failures))
                } else {
                    self.objects.lock().unwrap().insert(key.to_string(), data);
                    Ok(())
                }
            };

            self.active_uploads.fetch_sub(1, Ordering::SeqCst);
            result
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

    fn make_retry_config() -> RetryConfig {
        RetryConfig {
            max_retries: 3,
            base_delay_ms: 10,
            max_delay_ms: 100,
            ..Default::default()
        }
    }

    fn setup_uploader() -> (Arc<Uploader>, Arc<LocalCache>, Arc<MockStorage>, TempDir) {
        setup_uploader_with_concurrency(4)
    }

    fn setup_uploader_with_concurrency(max_concurrent: usize) -> (Arc<Uploader>, Arc<LocalCache>, Arc<MockStorage>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let storage = Arc::new(MockStorage::new());

        let uploader = Arc::new(Uploader::new(
            "test_db".to_string(),
            cache.clone(),
            storage.clone() as Arc<dyn StorageBackend>,
            "test-prefix".to_string(),
            Arc::new(RetryPolicy::new(make_retry_config())),
            Arc::new(WebhookSender::new(vec![])),
            max_concurrent,
        ));

        (uploader, cache, storage, temp_dir)
    }

    fn setup_uploader_with_storage(storage: Arc<MockStorage>, max_concurrent: usize) -> (Arc<Uploader>, Arc<LocalCache>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let cache = Arc::new(LocalCache::new(&db_path).unwrap());

        let uploader = Arc::new(Uploader::new(
            "test_db".to_string(),
            cache.clone(),
            storage as Arc<dyn StorageBackend>,
            "test-prefix".to_string(),
            Arc::new(RetryPolicy::new(make_retry_config())),
            Arc::new(WebhookSender::new(vec![])),
            max_concurrent,
        ));

        (uploader, cache, temp_dir)
    }

    // ============================================
    // Basic upload tests (ported from sequential)
    // ============================================

    #[tokio::test]
    async fn test_uploader_basic_upload() {
        let (uploader, cache, storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        cache.write_ltx(1, b"test data").unwrap();
        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();

        assert_eq!(stats.uploads_attempted, 1);
        assert_eq!(stats.uploads_succeeded, 1);
        assert_eq!(stats.uploads_failed, 0);
        assert_eq!(stats.last_uploaded_txid, 1);

        assert_eq!(storage.object_count(), 1);
        let data = storage.get_object("test-prefix/00000001.ltx").unwrap();
        assert_eq!(data, b"test data");

        assert_eq!(cache.pending_uploads().len(), 0);
        assert_eq!(cache.last_uploaded_txid(), 1);
    }

    #[tokio::test]
    async fn test_uploader_multiple_uploads() {
        let (uploader, cache, storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        for i in 1..=5 {
            cache.write_ltx(i, format!("data{}", i).as_bytes()).unwrap();
        }
        for i in 1..=5 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();

        assert_eq!(stats.uploads_succeeded, 5);
        assert_eq!(stats.last_uploaded_txid, 5);
        assert_eq!(storage.object_count(), 5);
        assert_eq!(cache.pending_uploads().len(), 0);
    }

    #[tokio::test]
    async fn test_uploader_resume_pending() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let cache = Arc::new(LocalCache::new(&db_path).unwrap());
        let storage = Arc::new(MockStorage::new());

        // Simulate interrupted upload
        cache.write_ltx(1, b"data1").unwrap();
        cache.write_ltx(2, b"data2").unwrap();
        cache.write_ltx(3, b"data3").unwrap();

        let uploader = Arc::new(Uploader::new(
            "test_db".to_string(),
            cache.clone(),
            storage.clone() as Arc<dyn StorageBackend>,
            "test-prefix".to_string(),
            Arc::new(RetryPolicy::new(make_retry_config())),
            Arc::new(WebhookSender::new(vec![])),
            4,
        ));

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();

        assert_eq!(stats.uploads_succeeded, 3);
        assert_eq!(storage.object_count(), 3);
        assert_eq!(cache.pending_uploads().len(), 0);
    }

    #[tokio::test]
    async fn test_uploader_retry_on_failure() {
        let storage = Arc::new(MockStorage::with_failures(2));
        let (uploader, cache, _temp) = setup_uploader_with_storage(storage.clone(), 4);

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        cache.write_ltx(1, b"data").unwrap();
        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();

        assert_eq!(stats.uploads_succeeded, 1);
        assert_eq!(stats.uploads_failed, 0);
    }

    #[tokio::test]
    async fn test_uploader_channel_buffer() {
        let (uploader, cache, _storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        for i in 1..=100 {
            cache.write_ltx(i, b"data").unwrap();
        }
        for i in 1..=100 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.uploads_succeeded, 100);
    }

    #[tokio::test]
    async fn test_uploader_graceful_shutdown() {
        let (uploader, cache, _storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        cache.write_ltx(1, b"data1").unwrap();
        cache.write_ltx(2, b"data2").unwrap();

        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tx.send(UploadMessage::Upload(2)).await.unwrap();
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let result = timeout(Duration::from_secs(5), task).await;
        assert!(result.is_ok(), "Uploader should shutdown gracefully");

        let stats = result.unwrap().unwrap().unwrap();
        assert_eq!(stats.uploads_succeeded, 2);
    }

    #[tokio::test]
    async fn test_uploader_stats_tracking() {
        let (uploader, cache, _storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

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

        let (tx, handle) = spawn_uploader(uploader.clone());

        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(storage.object_count(), 1);

        tx.send(UploadMessage::Shutdown).await.unwrap();
        handle.await.unwrap(); // Verify handle completes
    }

    // ============================================
    // Concurrent upload tests
    // ============================================

    #[tokio::test]
    async fn test_concurrent_uploads() {
        // Send 10 uploads with max_concurrent=3, verify all complete
        let storage = Arc::new(MockStorage::with_delay(Duration::from_millis(50)));
        let (uploader, cache, _temp) = setup_uploader_with_storage(storage.clone(), 3);

        let (tx, rx) = mpsc::channel(20);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        for i in 1..=10 {
            cache.write_ltx(i, format!("data{}", i).as_bytes()).unwrap();
        }
        for i in 1..=10 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = timeout(Duration::from_secs(5), task).await
            .expect("should not timeout")
            .unwrap()
            .unwrap();

        assert_eq!(stats.uploads_succeeded, 10);
        assert_eq!(storage.object_count(), 10);
        assert_eq!(cache.pending_uploads().len(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_respects_limit() {
        // With slow uploads and max_concurrent=2, peak should never exceed 2
        let storage = Arc::new(MockStorage::with_delay(Duration::from_millis(100)));
        let (uploader, cache, _temp) = setup_uploader_with_storage(storage.clone(), 2);

        let (tx, rx) = mpsc::channel(20);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        for i in 1..=6 {
            cache.write_ltx(i, b"data").unwrap();
        }
        for i in 1..=6 {
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = timeout(Duration::from_secs(5), task).await
            .expect("should not timeout")
            .unwrap()
            .unwrap();

        assert_eq!(stats.uploads_succeeded, 6);
        assert!(storage.peak_concurrent() <= 2,
            "peak concurrent {} should not exceed max_concurrent 2", storage.peak_concurrent());
    }

    #[tokio::test]
    async fn test_concurrent_resume_pending() {
        // 5 pending on startup, all resume concurrently
        let storage = Arc::new(MockStorage::with_delay(Duration::from_millis(50)));
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let cache = Arc::new(LocalCache::new(&db_path).unwrap());

        for i in 1..=5 {
            cache.write_ltx(i, format!("data{}", i).as_bytes()).unwrap();
        }

        let uploader = Arc::new(Uploader::new(
            "test_db".to_string(),
            cache.clone(),
            storage.clone() as Arc<dyn StorageBackend>,
            "test-prefix".to_string(),
            Arc::new(RetryPolicy::new(make_retry_config())),
            Arc::new(WebhookSender::new(vec![])),
            3,
        ));

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();

        assert_eq!(stats.uploads_succeeded, 5);
        assert_eq!(cache.pending_uploads().len(), 0);
        // Should have used concurrency (peak > 1)
        assert!(storage.peak_concurrent() > 1,
            "resume should use concurrency, got peak {}", storage.peak_concurrent());
        assert!(storage.peak_concurrent() <= 3,
            "resume should respect limit, got peak {}", storage.peak_concurrent());
    }

    #[tokio::test]
    async fn test_concurrent_shutdown_drains() {
        // In-flight uploads complete before shutdown returns
        let storage = Arc::new(MockStorage::with_delay(Duration::from_millis(200)));
        let (uploader, cache, _temp) = setup_uploader_with_storage(storage.clone(), 4);

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        for i in 1..=4 {
            cache.write_ltx(i, b"data").unwrap();
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }

        // Brief delay to let uploads start, then shutdown
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = timeout(Duration::from_secs(5), task).await
            .expect("shutdown should drain in-flight")
            .unwrap()
            .unwrap();

        // All 4 should complete even though shutdown was sent mid-flight
        assert_eq!(stats.uploads_succeeded, 4);
        assert_eq!(storage.object_count(), 4);
    }

    #[tokio::test]
    async fn test_concurrent_failure_doesnt_block() {
        // One failing upload doesn't prevent others from completing.
        // Storage fails the first call, then succeeds for all subsequent.
        // With concurrent uploads, one task retries while others proceed.
        let storage = Arc::new(MockStorage::with_failures(1));
        let (uploader, cache, _temp) = setup_uploader_with_storage(storage.clone(), 4);

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        for i in 1..=5 {
            cache.write_ltx(i, b"data").unwrap();
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = timeout(Duration::from_secs(5), task).await
            .expect("should not timeout")
            .unwrap()
            .unwrap();

        // All 5 should succeed (first retries after 1 failure, rest succeed immediately)
        assert_eq!(stats.uploads_succeeded, 5);
        assert_eq!(stats.uploads_failed, 0);
    }

    #[tokio::test]
    async fn test_concurrent_is_faster_than_sequential() {
        // With slow uploads, concurrent should be measurably faster
        let storage = Arc::new(MockStorage::with_delay(Duration::from_millis(100)));
        let (uploader, cache, _temp) = setup_uploader_with_storage(storage.clone(), 4);

        let (tx, rx) = mpsc::channel(20);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let n = 8;
        for i in 1..=n {
            cache.write_ltx(i, b"data").unwrap();
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let start = tokio::time::Instant::now();
        let stats = task.await.unwrap().unwrap();
        let elapsed = start.elapsed();

        assert_eq!(stats.uploads_succeeded, n as u64);

        // Sequential: 8 * 100ms = 800ms minimum
        // Concurrent(4): 2 batches * 100ms = ~200ms
        // Be generous: just check it's faster than sequential
        assert!(elapsed < Duration::from_millis(600),
            "concurrent should be faster than sequential, took {:?}", elapsed);
    }

    // ============================================
    // Edge cases
    // ============================================

    #[tokio::test]
    async fn test_max_concurrent_clamped_to_1() {
        // max_concurrent=0 should be clamped to 1
        let (uploader, cache, storage, _temp) = setup_uploader_with_concurrency(0);

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        cache.write_ltx(1, b"data").unwrap();
        tx.send(UploadMessage::Upload(1)).await.unwrap();
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.uploads_succeeded, 1);
        assert_eq!(storage.object_count(), 1);
    }

    #[tokio::test]
    async fn test_channel_close_drains_inflight() {
        // Dropping the sender (channel close) should drain in-flight tasks
        let storage = Arc::new(MockStorage::with_delay(Duration::from_millis(100)));
        let (uploader, cache, _temp) = setup_uploader_with_storage(storage.clone(), 4);

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        for i in 1..=3 {
            cache.write_ltx(i, b"data").unwrap();
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }

        // Drop sender instead of sending Shutdown
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(tx);

        let stats = timeout(Duration::from_secs(5), task).await
            .expect("should drain on channel close")
            .unwrap()
            .unwrap();

        assert_eq!(stats.uploads_succeeded, 3);
    }

    #[tokio::test]
    async fn test_last_uploaded_txid_tracks_highest() {
        // With concurrent uploads completing out of order, last_uploaded_txid
        // should track the highest seen, not the last to complete
        let (uploader, cache, _storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Write in reverse order
        for i in (1..=5).rev() {
            cache.write_ltx(i, b"data").unwrap();
            tx.send(UploadMessage::Upload(i)).await.unwrap();
        }
        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.uploads_succeeded, 5);
        assert_eq!(stats.last_uploaded_txid, 5);
    }

    #[tokio::test]
    async fn test_empty_run_no_pending_no_messages() {
        // Uploader with no pending and immediate shutdown
        let (uploader, _cache, _storage, _temp) = setup_uploader();

        let (tx, rx) = mpsc::channel(10);
        let uploader_clone = uploader.clone();
        let task = tokio::spawn(async move { uploader_clone.run(rx).await });

        tx.send(UploadMessage::Shutdown).await.unwrap();

        let stats = timeout(Duration::from_secs(2), task).await
            .expect("should shutdown immediately")
            .unwrap()
            .unwrap();

        assert_eq!(stats.uploads_attempted, 0);
        assert_eq!(stats.uploads_succeeded, 0);
    }
}
