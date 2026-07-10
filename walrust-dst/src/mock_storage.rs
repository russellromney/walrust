//! Mock storage backend for deterministic simulation testing.
//!
//! In-memory implementation of the current `hadb_storage::StorageBackend`
//! trait (get/put/delete/list/exists + the CAS primitives put_if_absent /
//! put_if_match) with:
//! - Fault injection (random errors, latency, partial writes, silent
//!   corruption, eventual consistency)
//! - Deterministic behavior (seeded RNG for reproducible runs)
//! - Operation logging for test verification
//!
//! The fault model is honest: a partial write persists the truncated prefix
//! then errors (a real torn object a reader can observe), corruption flips a
//! real bit in stored bytes, and eventual consistency is gated on a seeded
//! operation counter rather than wall-clock time so staleness is reproducible.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use rand::prelude::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Fault types that can be injected into storage operations.
#[derive(Debug, Clone)]
pub enum StorageFault {
    /// Random internal server errors. `rate` is probability 0.0-1.0 per op.
    RandomError { rate: f64 },

    /// Artificial latency on operations.
    Latency { delay_ms: u64 },

    /// A PUT stops after `at_bytes` bytes (network interruption). The truncated
    /// prefix is persisted, then the call errors — a torn object a reader sees.
    PartialWrite { at_bytes: usize },

    /// Silent data corruption with the given per-PUT probability.
    SilentCorruption { rate: f64 },

    /// Object temporarily unavailable after write (eventual consistency),
    /// expressed as a deterministic op-count lag derived from `delay_ms`.
    EventualConsistency { delay_ms: u64 },
}

/// Configuration for the mock storage.
#[derive(Debug, Clone)]
pub struct MockStorageConfig {
    /// Faults to inject.
    pub faults: Vec<StorageFault>,
    /// Seed for deterministic RNG.
    pub seed: u64,
    /// Bucket name for logging.
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

/// Record of a storage operation for test verification.
#[derive(Debug, Clone)]
pub struct OperationRecord {
    pub operation: String,
    pub key: String,
    pub success: bool,
    pub fault_triggered: Option<String>,
    pub data_size: Option<usize>,
}

/// Stored object with metadata.
#[derive(Debug, Clone)]
struct StoredObject {
    data: Vec<u8>,
    etag: u64,
    /// Deterministic eventual-consistency gate: the global operation count at
    /// or after which this object becomes visible to reads/lists. `0` means
    /// immediately visible. Derived from the seeded RNG at write time so
    /// visibility is reproducible, not wall-clock.
    visible_after_ops: u64,
}

/// Mock storage backend for testing.
///
/// Every field is an `Arc<Mutex<..>>` (or a cheap `Clone` config), so `Clone`
/// yields a second handle onto the *same* underlying store, RNG, and operation
/// log — used by the state-machine harness to hand a compaction `RangeLayout` a
/// `StorageBackend` view of the exact bucket the legacy engine writes to.
#[derive(Clone)]
pub struct MockStorageBackend {
    /// In-memory storage.
    storage: Arc<Mutex<HashMap<String, StoredObject>>>,
    /// Configuration.
    config: MockStorageConfig,
    /// Deterministic RNG.
    rng: Arc<Mutex<StdRng>>,
    /// Operation log.
    operations: Arc<Mutex<Vec<OperationRecord>>>,
    /// Monotonic operation counter for deterministic eventual consistency.
    op_counter: Arc<Mutex<u64>>,
    /// Monotonic etag source for CAS.
    etag_counter: Arc<Mutex<u64>>,
}

impl MockStorageBackend {
    /// Create a new mock storage backend.
    pub fn new(config: MockStorageConfig) -> Self {
        let rng = StdRng::seed_from_u64(config.seed);
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            config,
            rng: Arc::new(Mutex::new(rng)),
            operations: Arc::new(Mutex::new(Vec::new())),
            op_counter: Arc::new(Mutex::new(0)),
            etag_counter: Arc::new(Mutex::new(0)),
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

    fn next_etag(&self) -> u64 {
        let mut c = self.etag_counter.lock().unwrap();
        *c += 1;
        *c
    }

    /// Compute the operation count at which a freshly written object becomes
    /// visible, from the EC config gated on the seeded RNG. Returns `0` (always
    /// visible) when no EC fault is configured.
    fn eventual_consistency_visible_after(&self) -> u64 {
        for fault in &self.config.faults {
            if let StorageFault::EventualConsistency { delay_ms } = fault {
                // Minimum lag is 2 ops: the PUT consumes one op and the first
                // read-after-write consumes another, so a lag of 2 guarantees
                // that first read observes the object as not-yet-visible — i.e.
                // genuine, reproducible read-after-write staleness rather than a
                // window that closes before anyone can observe it.
                let max_lag = ((*delay_ms / 10).max(2)).min(16);
                let mut rng = self.rng.lock().unwrap();
                let lag = rng.gen_range(2..=max_lag);
                return self.current_op() + lag;
            }
        }
        0
    }

    /// Get all operation records.
    pub fn get_operations(&self) -> Vec<OperationRecord> {
        self.operations.lock().unwrap().clone()
    }

    /// Count of failed operations recorded so far.
    pub fn error_count(&self) -> usize {
        self.operations
            .lock()
            .unwrap()
            .iter()
            .filter(|op| !op.success)
            .count()
    }

    /// Number of GET-style reads recorded so far.
    pub fn get_operations_count(&self) -> usize {
        self.operations
            .lock()
            .unwrap()
            .iter()
            .filter(|op| op.operation == "get")
            .count()
    }

    /// Clear all data and operation history.
    pub fn reset(&self) {
        self.storage.lock().unwrap().clear();
        self.operations.lock().unwrap().clear();
    }

    /// Record an operation.
    fn record(
        &self,
        operation: &str,
        key: &str,
        success: bool,
        fault: Option<&str>,
        size: Option<usize>,
    ) {
        self.operations.lock().unwrap().push(OperationRecord {
            operation: operation.to_string(),
            key: key.to_string(),
            success,
            fault_triggered: fault.map(|s| s.to_string()),
            data_size: size,
        });
    }

    /// Decide whether to inject a random error this op.
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

    /// Latency delay if configured.
    fn get_latency(&self) -> Option<Duration> {
        for fault in &self.config.faults {
            if let StorageFault::Latency { delay_ms } = fault {
                return Some(Duration::from_millis(*delay_ms));
            }
        }
        None
    }

    /// Partial-write byte limit if configured.
    fn get_partial_write_limit(&self) -> Option<usize> {
        for fault in &self.config.faults {
            if let StorageFault::PartialWrite { at_bytes } = fault {
                return Some(*at_bytes);
            }
        }
        None
    }

    /// Apply corruption to a buffer if configured; returns whether it flipped.
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

    /// Visibility under the deterministic eventual-consistency gate.
    fn is_visible(&self, obj: &StoredObject) -> bool {
        obj.visible_after_ops == 0 || self.current_op() >= obj.visible_after_ops
    }

    /// Shared write path for `put`/`put_if_*` after the precondition passes.
    /// Applies latency, partial-write, and corruption faults, then stores.
    /// Returns the new etag on success, or an error (after persisting the torn
    /// prefix) on a partial write.
    async fn write_object(&self, op: &str, key: &str, data: &[u8]) -> Result<u64> {
        if let Some(delay) = self.get_latency() {
            tokio::time::sleep(delay).await;
        }

        let visible_after = self.eventual_consistency_visible_after();
        let etag = self.next_etag();

        // Partial write: persist the truncated prefix, then error.
        if let Some(limit) = self.get_partial_write_limit() {
            if data.len() > limit {
                let truncated = data[..limit].to_vec();
                let stored_len = truncated.len();
                self.storage.lock().unwrap().insert(
                    key.to_string(),
                    StoredObject {
                        data: truncated,
                        etag,
                        visible_after_ops: visible_after,
                    },
                );
                self.record(op, key, false, Some("PartialWrite"), Some(stored_len));
                return Err(anyhow!(
                    "Storage error: Upload interrupted after {} bytes (partial object persisted)",
                    limit
                ));
            }
        }

        let mut bytes = data.to_vec();
        let corrupted = self.maybe_corrupt(&mut bytes);
        let len = bytes.len();
        self.storage.lock().unwrap().insert(
            key.to_string(),
            StoredObject {
                data: bytes,
                etag,
                visible_after_ops: visible_after,
            },
        );
        self.record(
            op,
            key,
            true,
            if corrupted {
                Some("SilentCorruption")
            } else {
                None
            },
            Some(len),
        );
        Ok(etag)
    }
}

#[async_trait]
impl StorageBackend for MockStorageBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // Advance the deterministic clock first so EC visibility progresses.
        self.next_op();
        if self.should_inject_error() {
            self.record("get", key, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }

        let storage = self.storage.lock().unwrap();
        match storage.get(key) {
            Some(obj) if self.is_visible(obj) => {
                let len = obj.data.len();
                let data = obj.data.clone();
                drop(storage);
                self.record("get", key, true, None, Some(len));
                Ok(Some(data))
            }
            Some(_) => {
                drop(storage);
                // Not yet visible under eventual consistency: behaves as absent.
                self.record("get", key, false, Some("EventualConsistency"), None);
                Ok(None)
            }
            None => {
                drop(storage);
                self.record("get", key, true, None, None);
                Ok(None)
            }
        }
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.next_op();
        if self.should_inject_error() {
            self.record("put", key, false, Some("RandomError"), Some(data.len()));
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }
        self.write_object("put", key, data).await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.next_op();
        if self.should_inject_error() {
            self.record("delete", key, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }
        self.storage.lock().unwrap().remove(key);
        self.record("delete", key, true, None, None);
        Ok(())
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.next_op();
        if self.should_inject_error() {
            self.record("list", prefix, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }

        // list-after-write staleness: an object not yet visible under the EC
        // gate does NOT appear, exactly as a fresh PUT can be missing from LIST.
        let storage = self.storage.lock().unwrap();
        let mut keys: Vec<String> = storage
            .iter()
            .filter(|(k, obj)| k.starts_with(prefix) && self.is_visible(obj))
            .map(|(k, _)| k.clone())
            .filter(|k| after.map(|a| k.as_str() > a).unwrap_or(true))
            .collect();
        keys.sort();
        drop(storage);

        self.record("list", prefix, true, None, None);
        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.next_op();
        if self.should_inject_error() {
            self.record("exists", key, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }
        let storage = self.storage.lock().unwrap();
        let exists = storage
            .get(key)
            .map(|obj| self.is_visible(obj))
            .unwrap_or(false);
        drop(storage);
        self.record("exists", key, true, None, None);
        Ok(exists)
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        self.next_op();
        if self.should_inject_error() {
            self.record("put_if_absent", key, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }
        {
            let storage = self.storage.lock().unwrap();
            if storage.contains_key(key) {
                drop(storage);
                self.record("put_if_absent", key, true, None, None);
                return Ok(CasResult {
                    success: false,
                    etag: None,
                });
            }
        }
        let etag = self.write_object("put_if_absent", key, data).await?;
        Ok(CasResult {
            success: true,
            etag: Some(etag.to_string()),
        })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.next_op();
        if self.should_inject_error() {
            self.record("put_if_match", key, false, Some("RandomError"), None);
            return Err(anyhow!("Storage error: Service unavailable (injected)"));
        }
        {
            let storage = self.storage.lock().unwrap();
            match storage.get(key) {
                Some(obj) if obj.etag.to_string() == etag => {}
                _ => {
                    drop(storage);
                    self.record("put_if_match", key, true, None, None);
                    return Ok(CasResult {
                        success: false,
                        etag: None,
                    });
                }
            }
        }
        let new_etag = self.write_object("put_if_match", key, data).await?;
        Ok(CasResult {
            success: true,
            etag: Some(new_etag.to_string()),
        })
    }

    fn backend_name(&self) -> &str {
        &self.config.bucket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_put_get() {
        let storage = MockStorageBackend::new(MockStorageConfig::default());
        storage.put("key", &[1, 2, 3]).await.unwrap();
        let data = storage.get("key").await.unwrap();
        assert_eq!(data, Some(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn test_get_missing_is_none() {
        let storage = MockStorageBackend::new(MockStorageConfig::default());
        assert_eq!(storage.get("nope").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_random_error_injection() {
        let config = MockStorageConfig::default()
            .with_seed(42)
            .with_fault(StorageFault::RandomError { rate: 1.0 });
        let storage = MockStorageBackend::new(config);

        let result = storage.put("key", &[1, 2, 3]).await;
        assert!(result.is_err());
        assert_eq!(storage.error_count(), 1);
    }

    #[tokio::test]
    async fn test_partial_write_persists_truncated_prefix() {
        let config =
            MockStorageConfig::default().with_fault(StorageFault::PartialWrite { at_bytes: 10 });
        let storage = MockStorageBackend::new(config);

        // Small upload round-trips whole.
        storage.put("small", &[1, 2, 3]).await.unwrap();
        assert_eq!(storage.get("small").await.unwrap(), Some(vec![1, 2, 3]));

        // Large upload errors BUT leaves the truncated prefix durable.
        let result = storage.put("large", &[7u8; 100]).await;
        assert!(result.is_err(), "partial write must surface an error");
        let torn = storage
            .get("large")
            .await
            .unwrap()
            .expect("partial object must be persisted, not absent");
        assert_eq!(torn.len(), 10, "only the truncated prefix persists");
        assert!(torn.iter().all(|&b| b == 7));
    }

    #[tokio::test]
    async fn test_eventual_consistency_is_deterministic_not_wallclock() {
        let mk = || {
            MockStorageBackend::new(
                MockStorageConfig::default()
                    .with_seed(7)
                    .with_fault(StorageFault::EventualConsistency { delay_ms: 50 }),
            )
        };

        let run = |s: MockStorageBackend| async move {
            s.put("k", &[1, 2, 3]).await.unwrap();
            let mut ops = 0;
            loop {
                ops += 1;
                if let Some(d) = s.get("k").await.unwrap() {
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
        assert_eq!(
            a, b,
            "EC visibility must be reproducible under a fixed seed"
        );
        assert!(a > 1, "object must be stale for at least one read");
    }

    #[tokio::test]
    async fn test_list_after_write_staleness() {
        let storage = MockStorageBackend::new(
            MockStorageConfig::default()
                .with_seed(3)
                .with_fault(StorageFault::EventualConsistency { delay_ms: 100 }),
        );
        storage.put("p/fresh", &[1]).await.unwrap();
        let first = storage.list("p/", None).await.unwrap();
        let mut appeared = first.contains(&"p/fresh".to_string());
        for _ in 0..50 {
            if storage
                .list("p/", None)
                .await
                .unwrap()
                .contains(&"p/fresh".to_string())
            {
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
        storage.put("key", &original).await.unwrap();
        let downloaded = storage.get("key").await.unwrap().unwrap();
        assert_ne!(original, downloaded);
    }

    #[tokio::test]
    async fn test_list_prefix_and_after() {
        let storage = MockStorageBackend::new(MockStorageConfig::default());
        storage.put("prefix/a", &[1]).await.unwrap();
        storage.put("prefix/b", &[2]).await.unwrap();
        storage.put("other/c", &[3]).await.unwrap();

        let keys = storage.list("prefix/", None).await.unwrap();
        assert_eq!(keys, vec!["prefix/a", "prefix/b"]);

        let after = storage.list("prefix/", Some("prefix/a")).await.unwrap();
        assert_eq!(after, vec!["prefix/b"]);
    }

    #[tokio::test]
    async fn test_cas_put_if_absent() {
        let storage = MockStorageBackend::new(MockStorageConfig::default());
        let first = storage.put_if_absent("k", &[1]).await.unwrap();
        assert!(first.success);
        let second = storage.put_if_absent("k", &[2]).await.unwrap();
        assert!(!second.success, "second put_if_absent must fail");
        assert_eq!(storage.get("k").await.unwrap(), Some(vec![1]));
    }

    #[tokio::test]
    async fn test_cas_put_if_match() {
        let storage = MockStorageBackend::new(MockStorageConfig::default());
        let created = storage.put_if_absent("k", &[1]).await.unwrap();
        let etag = created.etag.unwrap();

        let bad = storage.put_if_match("k", &[2], "999999").await.unwrap();
        assert!(!bad.success, "stale etag must fail");

        let good = storage.put_if_match("k", &[3], &etag).await.unwrap();
        assert!(good.success, "matching etag must succeed");
        assert_eq!(storage.get("k").await.unwrap(), Some(vec![3]));
    }

    #[tokio::test]
    async fn test_deterministic_errors() {
        let mk = || {
            MockStorageBackend::new(
                MockStorageConfig::default()
                    .with_seed(12345)
                    .with_fault(StorageFault::RandomError { rate: 0.5 }),
            )
        };
        let s1 = mk();
        let s2 = mk();

        let mut r1 = Vec::new();
        let mut r2 = Vec::new();
        for i in 0..10 {
            r1.push(s1.put(&format!("k{}", i), &[]).await.is_ok());
            r2.push(s2.put(&format!("k{}", i), &[]).await.is_ok());
        }
        assert_eq!(r1, r2);
    }

    #[tokio::test]
    async fn test_operation_logging() {
        let storage = MockStorageBackend::new(MockStorageConfig::default());
        storage.put("key1", &[1]).await.unwrap();
        storage.get("key1").await.unwrap();
        let _ = storage.get("nonexistent").await;

        let ops = storage.get_operations();
        assert_eq!(ops.len(), 3);
        assert!(ops[0].success);
        assert!(ops[1].success);
        assert_eq!(storage.get_operations_count(), 2);
    }
}
