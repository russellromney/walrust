//! Mock storage backend for deterministic simulation testing
//!
//! Provides an in-memory implementation of StorageBackend with:
//! - Fault injection (random errors, slow writes, corruption)
//! - Deterministic behavior (seeded RNG for reproducible tests)
//! - Operation logging for test verification

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rand::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use hadb_storage::StorageBackend;

/// Fault types that can be injected into storage operations
#[derive(Debug, Clone)]
pub enum StorageFault {
    /// Random internal server errors
    /// `rate` is probability 0.0-1.0 of failure per operation
    RandomError { rate: f64 },

    /// Artificial latency on operations
    Latency { delay_ms: u64 },

    /// Upload stops after `at_bytes` bytes (simulates network interruption)
    PartialWrite { at_bytes: usize },

    /// Silent data corruption with given probability
    SilentCorruption { rate: f64 },

    /// Object temporarily unavailable after write (eventual consistency)
    EventualConsistency { delay_ms: u64 },
}

/// Configuration for the mock storage
#[derive(Debug, Clone)]
pub struct MockStorageConfig {
    /// Faults to inject
    pub faults: Vec<StorageFault>,
    /// Seed for deterministic RNG
    pub seed: u64,
    /// Bucket name for logging
    pub bucket: String,
}

impl Default for MockStorageConfig {
    fn default() -> Self {
        Self {
            faults: Vec::new(),
            seed: 0,
            bucket: "test-bucket".to_string(),
        }
    }
}

impl MockStorageConfig {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            ..Default::default()
        }
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn with_fault(mut self, fault: StorageFault) -> Self {
        self.faults.push(fault);
        self
    }
}

/// Record of a storage operation for test verification
#[derive(Debug, Clone)]
pub struct OperationRecord {
    pub operation: String,
    pub key: String,
    pub success: bool,
    pub fault_triggered: Option<String>,
    pub data_size: Option<usize>,
}

/// Stored object with metadata
#[derive(Debug, Clone)]
struct StoredObject {
    data: Vec<u8>,
    checksum: Option<String>,
    created_at: std::time::Instant,
    /// Deterministic eventual-consistency gate: the global operation count at
    /// or after which this object becomes visible to reads/lists. `0` means
    /// immediately visible (no EC fault configured). Derived from the seeded
    /// RNG at write time so visibility is reproducible, not wall-clock.
    visible_after_ops: u64,
}

/// Mock storage backend for testing
pub struct MockStorageBackend {
    /// In-memory storage
    storage: Arc<Mutex<HashMap<String, StoredObject>>>,
    /// Configuration
    config: MockStorageConfig,
    /// Deterministic RNG
    rng: Arc<Mutex<StdRng>>,
    /// Operation log
    operations: Arc<Mutex<Vec<OperationRecord>>>,
    /// Monotonic operation counter for deterministic eventual consistency.
    op_counter: Arc<Mutex<u64>>,
}

impl MockStorageBackend {
    /// Create a new mock storage backend
    pub fn new(config: MockStorageConfig) -> Self {
        let rng = StdRng::seed_from_u64(config.seed);
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            config,
            rng: Arc::new(Mutex::new(rng)),
            operations: Arc::new(Mutex::new(Vec::new())),
            op_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Advance and return the global operation counter.
    fn next_op(&self) -> u64 {
        let mut c = self.op_counter.lock().unwrap();
        *c += 1;
        *c
    }

    /// Current operation counter without advancing it.
    fn current_op(&self) -> u64 {
        *self.op_counter.lock().unwrap()
    }

    /// Compute the operation count at which a freshly written object becomes
    /// visible, from the Ec config gated on the seeded RNG. Returns `0` (always
    /// visible) when no EC fault is configured. The `delay_ms` is reinterpreted
    /// deterministically as a small bounded number of operations.
    fn eventual_consistency_visible_after(&self) -> u64 {
        for fault in &self.config.faults {
            if let StorageFault::EventualConsistency { delay_ms } = fault {
                // Map the configured delay onto a 1..=N op window, picked from
                // the seeded RNG so the staleness is reproducible.
                let max_lag = ((*delay_ms / 10).max(1)).min(16);
                let mut rng = self.rng.lock().unwrap();
                let lag = rng.gen_range(1..=max_lag);
                return self.current_op() + lag;
            }
        }
        0
    }

    /// Get all operation records
    pub fn get_operations(&self) -> Vec<OperationRecord> {
        self.operations.lock().unwrap().clone()
    }

    /// Get error count
    pub fn error_count(&self) -> usize {
        self.operations
            .lock()
            .unwrap()
            .iter()
            .filter(|op| !op.success)
            .count()
    }

    /// Clear all data and operations
    pub fn reset(&self) {
        self.storage.lock().unwrap().clear();
        self.operations.lock().unwrap().clear();
    }

    /// Record an operation
    fn record(&self, operation: &str, key: &str, success: bool, fault: Option<&str>, size: Option<usize>) {
        self.operations.lock().unwrap().push(OperationRecord {
            operation: operation.to_string(),
            key: key.to_string(),
            success,
            fault_triggered: fault.map(|s| s.to_string()),
            data_size: size,
        });
    }

    /// Check if we should inject a random error
    fn should_inject_error(&self) -> bool {
        for fault in &self.config.faults {
            if let StorageFault::RandomError { rate } = fault {
                let mut rng = self.rng.lock().unwrap();
                if rng.gen::<f64>() < *rate {
                    return true;
                }
            }
        }
        false
    }

    /// Get latency delay if configured
    fn get_latency(&self) -> Option<Duration> {
        for fault in &self.config.faults {
            if let StorageFault::Latency { delay_ms } = fault {
                return Some(Duration::from_millis(*delay_ms));
            }
        }
        None
    }

    /// Get partial write limit if configured
    fn get_partial_write_limit(&self) -> Option<usize> {
        for fault in &self.config.faults {
            if let StorageFault::PartialWrite { at_bytes } = fault {
                return Some(*at_bytes);
            }
        }
        None
    }

    /// Apply corruption if configured
    fn maybe_corrupt(&self, data: &mut [u8]) -> bool {
        for fault in &self.config.faults {
            if let StorageFault::SilentCorruption { rate } = fault {
                let mut rng = self.rng.lock().unwrap();
                if rng.gen::<f64>() < *rate && !data.is_empty() {
                    let byte_idx = rng.gen_range(0..data.len());
                    let bit_idx = rng.gen_range(0..8);
                    data[byte_idx] ^= 1 << bit_idx;
                    return true;
                }
            }
        }
        false
    }

    /// Check if object is visible (deterministic eventual consistency).
    ///
    /// Visibility is gated on the global operation counter, not wall-clock
    /// time: an object written with `visible_after_ops = N` is invisible to
    /// reads/lists until the counter reaches `N`. This makes list-after-write
    /// and read-after-write staleness reproducible under a fixed seed.
    fn is_visible(&self, obj: &StoredObject) -> bool {
        obj.visible_after_ops == 0 || self.current_op() >= obj.visible_after_ops
    }
}

#[async_trait]
impl StorageBackend for MockStorageBackend {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        // Check for random error
        if self.should_inject_error() {
            self.record("upload_bytes", key, false, Some("RandomError"), Some(data.len()));
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }

        // Apply latency
        if let Some(delay) = self.get_latency() {
            tokio::time::sleep(delay).await;
        }

        // Check partial write: a real interrupted PUT leaves the truncated
        // prefix durable (the object exists but is short / corrupt), then
        // surfaces the error. Storing nothing made this fault untestable —
        // downstream readers never saw the torn object.
        if let Some(limit) = self.get_partial_write_limit() {
            if data.len() > limit {
                let truncated: Vec<u8> = data[..limit].to_vec();
                let stored_len = truncated.len();
                self.storage.lock().unwrap().insert(
                    key.to_string(),
                    StoredObject {
                        data: truncated,
                        checksum: None,
                        created_at: std::time::Instant::now(),
                        visible_after_ops: self.eventual_consistency_visible_after(),
                    },
                );
                self.record("upload_bytes", key, false, Some("PartialWrite"), Some(stored_len));
                return Err(anyhow!(
                    "Storage error: Upload interrupted after {} bytes (partial object persisted)",
                    limit
                ));
            }
        }

        // Apply corruption
        let mut data = data;
        let corrupted = self.maybe_corrupt(&mut data);
        if corrupted {
            self.record("upload_bytes", key, true, Some("SilentCorruption"), Some(data.len()));
        } else {
            self.record("upload_bytes", key, true, None, Some(data.len()));
        }

        // Store
        let visible_after = self.eventual_consistency_visible_after();
        self.storage.lock().unwrap().insert(
            key.to_string(),
            StoredObject {
                data,
                checksum: None,
                created_at: std::time::Instant::now(),
                visible_after_ops: visible_after,
            },
        );

        Ok(())
    }

    async fn upload_bytes_with_checksum(
        &self,
        key: &str,
        data: Vec<u8>,
        checksum: &str,
    ) -> Result<()> {
        if self.should_inject_error() {
            self.record("upload_bytes_with_checksum", key, false, Some("RandomError"), Some(data.len()));
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }

        if let Some(delay) = self.get_latency() {
            tokio::time::sleep(delay).await;
        }

        if let Some(limit) = self.get_partial_write_limit() {
            if data.len() > limit {
                let truncated: Vec<u8> = data[..limit].to_vec();
                let stored_len = truncated.len();
                self.storage.lock().unwrap().insert(
                    key.to_string(),
                    StoredObject {
                        data: truncated,
                        checksum: Some(checksum.to_string()),
                        created_at: std::time::Instant::now(),
                        visible_after_ops: self.eventual_consistency_visible_after(),
                    },
                );
                self.record("upload_bytes_with_checksum", key, false, Some("PartialWrite"), Some(stored_len));
                return Err(anyhow!("Storage error: Upload interrupted (partial object persisted)"));
            }
        }

        let mut data = data;
        let corrupted = self.maybe_corrupt(&mut data);
        self.record(
            "upload_bytes_with_checksum",
            key,
            true,
            if corrupted { Some("SilentCorruption") } else { None },
            Some(data.len()),
        );

        let visible_after = self.eventual_consistency_visible_after();
        self.storage.lock().unwrap().insert(
            key.to_string(),
            StoredObject {
                data,
                checksum: Some(checksum.to_string()),
                created_at: std::time::Instant::now(),
                visible_after_ops: visible_after,
            },
        );

        Ok(())
    }

    async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = tokio::fs::read(path).await?;
        self.upload_bytes(key, data).await
    }

    async fn upload_file_with_checksum(
        &self,
        key: &str,
        path: &Path,
        checksum: &str,
    ) -> Result<()> {
        let data = tokio::fs::read(path).await?;
        self.upload_bytes_with_checksum(key, data, checksum).await
    }

    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>> {
        // Advance the deterministic clock first so eventual-consistency
        // visibility progresses with each access.
        self.next_op();
        if self.should_inject_error() {
            self.record("download_bytes", key, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }

        let storage = self.storage.lock().unwrap();
        match storage.get(key) {
            Some(obj) => {
                if !self.is_visible(obj) {
                    drop(storage);
                    self.record("download_bytes", key, false, Some("EventualConsistency"), None);
                    return Err(anyhow!("Storage error: Object not found (eventual consistency)"));
                }
                self.record("download_bytes", key, true, None, Some(obj.data.len()));
                Ok(obj.data.clone())
            }
            None => {
                self.record("download_bytes", key, false, None, None);
                Err(anyhow!("Storage error: Object not found: {}", key))
            }
        }
    }

    async fn download_file(&self, key: &str, path: &Path) -> Result<()> {
        let data = self.download_bytes(key).await?;
        tokio::fs::write(path, data).await?;
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        self.next_op();
        if self.should_inject_error() {
            self.record("list_objects", prefix, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }

        // list-after-write staleness: an object not yet visible (per the
        // deterministic EC gate) does NOT appear in the listing, exactly as a
        // freshly-PUT object can be missing from an eventually-consistent LIST.
        let storage = self.storage.lock().unwrap();
        let mut keys: Vec<String> = storage
            .iter()
            .filter(|(k, obj)| k.starts_with(prefix) && self.is_visible(obj))
            .map(|(k, _)| k.clone())
            .collect();
        keys.sort();

        self.record("list_objects", prefix, true, None, None);
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.next_op();
        if self.should_inject_error() {
            self.record("exists", key, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }

        let storage = self.storage.lock().unwrap();
        let exists = storage.get(key).map(|obj| self.is_visible(obj)).unwrap_or(false);

        self.record("exists", key, true, None, None);
        Ok(exists)
    }

    async fn get_checksum(&self, key: &str) -> Result<Option<String>> {
        if self.should_inject_error() {
            self.record("get_checksum", key, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }

        let storage = self.storage.lock().unwrap();
        let checksum = storage.get(key).and_then(|obj| obj.checksum.clone());

        self.record("get_checksum", key, true, None, None);
        Ok(checksum)
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        if self.should_inject_error() {
            self.record("delete_object", key, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }

        self.storage.lock().unwrap().remove(key);
        self.record("delete_object", key, true, None, None);
        Ok(())
    }

    async fn delete_objects(&self, keys: &[String]) -> Result<usize> {
        if self.should_inject_error() {
            self.record("delete_objects", &keys.join(","), false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }

        let mut storage = self.storage.lock().unwrap();
        let mut deleted = 0;
        for key in keys {
            if storage.remove(key).is_some() {
                deleted += 1;
            }
        }

        self.record("delete_objects", &format!("{} keys", keys.len()), true, None, None);
        Ok(deleted)
    }

    fn bucket_name(&self) -> &str {
        &self.config.bucket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_upload_download() {
        let storage = MockStorageBackend::new(MockStorageConfig::default());

        storage.upload_bytes("key", vec![1, 2, 3]).await.unwrap();
        let data = storage.download_bytes("key").await.unwrap();

        assert_eq!(data, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_random_error_injection() {
        let config = MockStorageConfig::default()
            .with_seed(42)
            .with_fault(StorageFault::RandomError { rate: 1.0 });
        let storage = MockStorageBackend::new(config);

        let result = storage.upload_bytes("key", vec![1, 2, 3]).await;
        assert!(result.is_err());
        assert_eq!(storage.error_count(), 1);
    }

    #[tokio::test]
    async fn test_partial_write_persists_truncated_prefix() {
        let config = MockStorageConfig::default()
            .with_fault(StorageFault::PartialWrite { at_bytes: 10 });
        let storage = MockStorageBackend::new(config);

        // Small upload should succeed and round-trip whole.
        storage.upload_bytes("small", vec![1, 2, 3]).await.unwrap();
        assert_eq!(storage.download_bytes("small").await.unwrap(), vec![1, 2, 3]);

        // Large upload fails BUT leaves the truncated prefix durable, so a
        // reader observes a torn object — the fault is now actually exercisable.
        let result = storage.upload_bytes("large", vec![7u8; 100]).await;
        assert!(result.is_err(), "partial write must surface an error");
        let torn = storage
            .download_bytes("large")
            .await
            .expect("partial object must be persisted, not absent");
        assert_eq!(torn.len(), 10, "only the truncated prefix persists");
        assert!(torn.iter().all(|&b| b == 7));
    }

    #[tokio::test]
    async fn test_eventual_consistency_is_deterministic_not_wallclock() {
        // A freshly written object is invisible to read/list until enough
        // operations elapse, and the pattern is reproducible under a seed.
        let mk = || {
            MockStorageBackend::new(
                MockStorageConfig::default()
                    .with_seed(7)
                    .with_fault(StorageFault::EventualConsistency { delay_ms: 50 }),
            )
        };

        let run = |s: MockStorageBackend| async move {
            s.upload_bytes("k", vec![1, 2, 3]).await.unwrap();
            // Drive reads until it becomes visible; record how many ops it took.
            let mut ops = 0;
            loop {
                ops += 1;
                if let Ok(d) = s.download_bytes("k").await {
                    assert_eq!(d, vec![1, 2, 3]);
                    break ops;
                }
                if ops > 100 {
                    panic!("object never became visible");
                }
            }
        };

        let a = run(mk()).await;
        let b = run(mk()).await;
        assert_eq!(a, b, "EC visibility must be reproducible under a fixed seed");
        assert!(a > 1, "object must be stale for at least one read");
    }

    #[tokio::test]
    async fn test_list_after_write_staleness() {
        let storage = MockStorageBackend::new(
            MockStorageConfig::default()
                .with_seed(3)
                .with_fault(StorageFault::EventualConsistency { delay_ms: 100 }),
        );
        storage.upload_bytes("p/fresh", vec![1]).await.unwrap();
        // Immediately after write the object may be missing from LIST.
        let first = storage.list_objects("p/").await.unwrap();
        // Drive the clock forward; eventually it appears.
        let mut appeared = first.contains(&"p/fresh".to_string());
        for _ in 0..50 {
            if storage.list_objects("p/").await.unwrap().contains(&"p/fresh".to_string()) {
                appeared = true;
                break;
            }
        }
        assert!(appeared, "object must eventually appear in LIST");
    }

    #[tokio::test]
    async fn test_silent_corruption() {
        let config = MockStorageConfig::default()
            .with_seed(42)
            .with_fault(StorageFault::SilentCorruption { rate: 1.0 });
        let storage = MockStorageBackend::new(config);

        let original = vec![0u8; 100];
        storage.upload_bytes("key", original.clone()).await.unwrap();
        let downloaded = storage.download_bytes("key").await.unwrap();

        assert_ne!(original, downloaded);
    }

    #[tokio::test]
    async fn test_list_objects() {
        let storage = MockStorageBackend::new(MockStorageConfig::default());

        storage.upload_bytes("prefix/a", vec![1]).await.unwrap();
        storage.upload_bytes("prefix/b", vec![2]).await.unwrap();
        storage.upload_bytes("other/c", vec![3]).await.unwrap();

        let keys = storage.list_objects("prefix/").await.unwrap();
        assert_eq!(keys, vec!["prefix/a", "prefix/b"]);
    }

    #[tokio::test]
    async fn test_deterministic_errors() {
        // Same seed should produce same error pattern
        let config1 = MockStorageConfig::default()
            .with_seed(12345)
            .with_fault(StorageFault::RandomError { rate: 0.5 });
        let config2 = MockStorageConfig::default()
            .with_seed(12345)
            .with_fault(StorageFault::RandomError { rate: 0.5 });

        let storage1 = MockStorageBackend::new(config1);
        let storage2 = MockStorageBackend::new(config2);

        let mut results1 = Vec::new();
        let mut results2 = Vec::new();

        for i in 0..10 {
            results1.push(storage1.upload_bytes(&format!("k{}", i), vec![]).await.is_ok());
            results2.push(storage2.upload_bytes(&format!("k{}", i), vec![]).await.is_ok());
        }

        assert_eq!(results1, results2);
    }

    #[tokio::test]
    async fn test_operation_logging() {
        let storage = MockStorageBackend::new(MockStorageConfig::default());

        storage.upload_bytes("key1", vec![1]).await.unwrap();
        storage.download_bytes("key1").await.unwrap();
        let _ = storage.download_bytes("nonexistent").await;

        let ops = storage.get_operations();
        assert_eq!(ops.len(), 3);
        assert!(ops[0].success);
        assert!(ops[1].success);
        assert!(!ops[2].success);
    }
}
