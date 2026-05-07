//! Regression tests for `Replicator` Drop semantics.
//!
//! Background: `Replicator::try_new` used to `Arc::clone` the new
//! `Arc<Self>` and move it into the background sync task. That gave the
//! task a strong reference, so when the caller's last `Arc` was dropped
//! the strong refcount stayed at 1 and the task — plus its
//! `Arc<dyn StorageBackend>` connection pool — leaked forever.
//!
//! Any caller that builds a Replicator inside an init-retry loop hits
//! this hard: each successful `try_new` followed by a downstream check
//! that fails leaks one Replicator and one S3 client connection pool.
//! A few dozen retries can exhaust the process file-descriptor limit.
//!
//! These tests pin the contract: dropping the Replicator must release
//! the storage backend it owns, and create/drop cycles must not
//! accumulate background tasks.

use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hadb_storage::{CasResult, StorageBackend};
use walrust::Replicator;
use walrust::{ReplicationConfig, SnapshotOwnership};

// ============================================================================
// Minimal in-memory storage backend (mirrors tests/replicator_flush.rs but
// kept self-contained so this file is independently understandable).
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

fn config_for_test() -> ReplicationConfig {
    // Short sync interval so the task gets a chance to upgrade `Weak<Self>`
    // and observe its release in the test's wall-clock budget.
    ReplicationConfig {
        sync_interval: Duration::from_millis(20),
        snapshot_interval: Duration::from_secs(3600),
        autonomous_snapshots: false,
        snapshot_ownership: SnapshotOwnership::External,
        ..Default::default()
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Single-shot: build a Replicator, drop it, verify the storage backend
/// is released. Before the fix, the spawned task held a strong
/// `Arc<Replicator>`, which kept `storage` alive forever — strong count
/// would never return to 1.
#[tokio::test]
async fn drop_releases_storage_backend() {
    let storage = MemStorage::new();
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();

    let replicator = Replicator::try_new(storage_dyn, "test/", config_for_test())
        .expect("construct replicator");
    // Sanity: the replicator owns one strong ref to storage on top of ours.
    assert_eq!(
        Arc::strong_count(&storage),
        2,
        "expected exactly the test's ref + the replicator's ref while alive"
    );

    drop(replicator);

    // Give the runtime a brief window to actually run Drop's `abort()`
    // through to completion of any in-flight task work. Drop is sync and
    // runs the abort immediately, but the aborted task's own destructor
    // (releasing its captured Arcs) only runs when the runtime polls it.
    // 100ms is generous against a 20ms sync interval.
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        Arc::strong_count(&storage),
        1,
        "storage must be released after Replicator is dropped; \
         strong_count > 1 means a background task still holds it"
    );
}

/// Init-retry stress: simulate the kind of caller loop that originally
/// surfaced the leak — build and drop the Replicator many times in a
/// tight loop, with no chance to acquire databases or run a real sync.
/// Storage strong count must return to 1; accumulating references
/// would exhaust file descriptors in any production caller running
/// this shape of retry.
#[tokio::test]
async fn create_drop_cycles_do_not_accumulate_storage_refs() {
    let storage = MemStorage::new();

    for _ in 0..50 {
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let r = Replicator::try_new(storage_dyn, "test/", config_for_test())
            .expect("construct replicator");
        // Immediate drop (no `add()`, no `flush()` — the failed-init
        // path where the constructor succeeded but a downstream check
        // bailed and the partial object was dropped).
        drop(r);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        Arc::strong_count(&storage),
        1,
        "after 50 create/drop cycles, storage strong count must be \
         exactly 1 (just the test's ref); higher count means the \
         leak has regressed"
    );
}

/// Drop happens while the background task is mid-tick. The Replicator
/// is built with a 20ms sync interval, then dropped at 60ms — well into
/// the steady-state of the loop, where `Weak::upgrade` has been
/// happening. Storage must still be released promptly.
#[tokio::test]
async fn drop_during_active_sync_loop_is_clean() {
    let storage = MemStorage::new();
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let replicator = Replicator::try_new(storage_dyn, "test/", config_for_test())
        .expect("construct replicator");

    // Let the loop tick a few times so we're past first-iteration
    // initialisation.
    tokio::time::sleep(Duration::from_millis(60)).await;

    drop(replicator);
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        Arc::strong_count(&storage),
        1,
        "drop during active loop must still release storage"
    );
}
