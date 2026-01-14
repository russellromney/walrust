# Walrust Battle Testing: Deterministic Simulation Testing

> "If you built a plane in a wind tunnel with zero induced turbulence effects, would you then fly that plane? Because that's how people are building the distributed systems you use today."
> — [sled simulation guide](https://sled.rs/simulation.html)

This document outlines how to make walrust **Jepsen-proof** before production use.

---

## The Problem

Traditional testing catches ~5% of real bugs in backup/replication systems. Real production has:
- Network delays and partitions during S3 uploads
- Crashes mid-WAL-sync with partial writes
- S3 eventual consistency (object appears, disappears, reappears)
- SQLite checkpoint happening mid-backup
- Clock drift between snapshot intervals
- Concurrent writes while backup is reading WAL

**The goal**: Find data loss bugs in milliseconds on a laptop that would take months to surface in production.

---

## Critical Properties for Walrust

### 1. **Durability** (No Data Loss)
```
Property: Every committed SQLite transaction must be recoverable from S3
Test: Write N transactions, crash at random point, verify all N can be restored
```

### 2. **Point-in-Time Restore Correctness**
```
Property: Restoring to timestamp T gives exact database state at T
Test: Generate writes with timestamps, restore to random T, verify state matches
```

### 3. **WAL Sync Batching Correctness**
```
Property: Batching WAL syncs (new wal_sync_interval) never loses frames
Test: 100 writes/sec with 1s batching, verify all 100 frames uploaded
```

### 4. **Snapshot Consistency**
```
Property: Snapshots are atomic (no partial state)
Test: Concurrent writes during snapshot, verify snapshot is valid SQLite DB
```

### 5. **GFS Retention Correctness**
```
Property: Compaction never deletes data needed for recovery
Test: Create hourly/daily/weekly backups, compact, verify restore still works
```

### 6. **Litestream Compatibility**
```
Property: LTX files are valid and interchangeable with Litestream
Test: Walrust backs up, Litestream restores (and vice versa)
```

---

## Architecture for Walrust DST

### Current Issues

```
┌─────────────────────────────────────────────┐
│              walrust watch                   │
│  Uses: tokio, notify, std::time, aws-sdk    │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│            Real Filesystem                   │
│    (notify watching .db-wal files)          │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│               Real S3                        │
│        (network I/O, eventual consistency)   │
└─────────────────────────────────────────────┘
```

**Problem**: Can't control:
- File watcher event timing
- S3 network latency/failures
- SQLite WAL checkpoint timing
- Clock for interval-based syncs

### Target Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Test Harness (DST)                          │
│  • Seeded RNG for workload generation                          │
│  • Simulated clock (control wal_sync_interval, snapshots)      │
│  • Simulated filesystem (control WAL events)                   │
│  • Simulated S3 (inject faults: 500 errors, eventual consistency)│
└─────────────────────────────┬───────────────────────────────────┘
                              │ Commands
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    WalrustEngine (Pure Logic)                   │
│  • Processes WAL frames, decides when to snapshot              │
│  • No direct I/O, time, or randomness                          │
│  • All external dependencies injected via traits               │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────┴───────────────────────────────────┐
│                     Storage Backend Traits                      │
├─────────────────────────────┬───────────────────────────────────┤
│   SimulatedS3               │       RealS3                      │
│   • In-memory buckets       │       • aws-sdk-s3                │
│   • Fault injection         │       • Production use            │
│   • Deterministic           │                                   │
│   • Eventual consistency    │                                   │
├─────────────────────────────┼───────────────────────────────────┤
│   SimulatedFS               │       RealFS                      │
│   • In-memory WAL files     │       • notify + std::fs          │
│   • Control event timing    │       • Production use            │
│   • Deterministic           │                                   │
└─────────────────────────────┴───────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: Property-Based Testing (2-3 days)

**Goal**: Catch logic bugs without major refactoring.

**Create `walrust-dst/` crate:**

```toml
# walrust-dst/Cargo.toml
[package]
name = "walrust-dst"
version = "0.1.0"
edition = "2021"

[dependencies]
walrust = { path = ".." }
proptest = "1.4"
rusqlite = "0.32"
anyhow = "1"
rand = "0.8"
clap = { version = "4", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

**Create property tests:**

```rust
// walrust-dst/src/properties.rs
use proptest::prelude::*;

/// Property: WAL batching doesn't lose frames
///
/// Given: N writes within wal_sync_interval
/// When: Batch sync happens
/// Then: All N frames are in S3
proptest! {
    #[test]
    fn wal_batching_no_loss(
        write_count in 1..200usize,
        wal_sync_interval in 1..5u64
    ) {
        // Create temp DB
        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.path().join("test.db");

        // Set up walrust with batching
        let _watcher = start_walrust_watch(
            &db_path,
            "s3://test-bucket",
            wal_sync_interval
        );

        // Write N transactions
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute("PRAGMA journal_mode=WAL", [])?;
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)", [])?;

        for i in 0..write_count {
            conn.execute("INSERT INTO test (val) VALUES (?)", [format!("val_{}", i)])?;
        }

        // Wait for sync
        std::thread::sleep(Duration::from_secs(wal_sync_interval + 1));

        // Verify all frames in S3
        let frames = count_s3_frames("s3://test-bucket", "test");
        prop_assert!(frames >= write_count,
            "Lost frames: expected {} got {}", write_count, frames);
    }
}

/// Property: Point-in-time restore is exact
///
/// Given: Database with timestamped writes
/// When: Restore to arbitrary timestamp T
/// Then: Database state exactly matches state at T
proptest! {
    #[test]
    fn pitr_exact(
        writes in prop::collection::vec(("[a-z]+", 0..1000i64), 10..100)
    ) {
        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.path().join("test.db");

        // Write with timestamps
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute("PRAGMA journal_mode=WAL", [])?;
        conn.execute("CREATE TABLE events (ts INTEGER, data TEXT)", [])?;

        for (data, ts) in &writes {
            conn.execute("INSERT INTO events VALUES (?, ?)", [ts, data])?;
        }

        // Backup
        walrust::backup(&db_path, "s3://bucket")?;

        // Pick random restore point
        let restore_ts = writes[writes.len() / 2].1;

        // Restore to that point
        let restore_path = tmpdir.path().join("restored.db");
        walrust::restore("s3://bucket/test", &restore_path, Some(restore_ts))?;

        // Verify exact state
        let restored = rusqlite::Connection::open(&restore_path)?;
        let count: i64 = restored.query_row(
            "SELECT COUNT(*) FROM events WHERE ts <= ?",
            [restore_ts],
            |r| r.get(0)
        )?;

        let expected = writes.iter().filter(|(_, ts)| *ts <= restore_ts).count();
        prop_assert_eq!(count as usize, expected);
    }
}

/// Property: GFS compaction preserves recoverability
///
/// Given: Hourly backups compacted to daily/weekly
/// When: Restore from any compacted generation
/// Then: Data is complete and correct
proptest! {
    #[test]
    fn gfs_compaction_safe(
        hourly_count in 24..48usize,
        restore_hour in 0..24usize
    ) {
        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.path().join("test.db");

        // Generate hourly backups
        for hour in 0..hourly_count {
            // Write data for this hour
            write_hourly_data(&db_path, hour)?;
            walrust::snapshot(&db_path, "s3://bucket")?;
        }

        // Run GFS compaction
        walrust::compact("s3://bucket/test", RetentionPolicy {
            hourly: 24,
            daily: 7,
            weekly: 4,
            monthly: 12,
        })?;

        // Verify can restore from any surviving generation
        let restore_path = tmpdir.path().join("restored.db");
        let hour_ts = calculate_timestamp(restore_hour);

        let result = walrust::restore("s3://bucket/test", &restore_path, Some(hour_ts));
        prop_assert!(result.is_ok(), "Cannot restore after compaction: {:?}", result);
    }
}

/// Property: Concurrent writes during snapshot don't corrupt
///
/// Given: Active writes happening during snapshot
/// When: Snapshot completes
/// Then: Snapshot is valid SQLite DB (not corrupted)
proptest! {
    #[test]
    fn snapshot_concurrent_writes(
        concurrent_writes in 10..100usize
    ) {
        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.path().join("test.db");

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute("PRAGMA journal_mode=WAL", [])?;
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)", [])?;

        // Start snapshot in background
        let db_path_clone = db_path.clone();
        let snapshot_handle = std::thread::spawn(move || {
            walrust::snapshot(&db_path_clone, "s3://bucket")
        });

        // Concurrent writes
        for i in 0..concurrent_writes {
            conn.execute("INSERT INTO test VALUES (?)", [i])?;
            std::thread::sleep(Duration::from_millis(10));
        }

        // Wait for snapshot
        snapshot_handle.join().unwrap()?;

        // Download and verify snapshot is valid
        let snapshot_path = tmpdir.path().join("snapshot.db");
        download_latest_snapshot("s3://bucket/test", &snapshot_path)?;

        // Should be valid SQLite DB
        let snap_conn = rusqlite::Connection::open(&snapshot_path)?;
        let integrity: String = snap_conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        prop_assert_eq!(integrity, "ok");
    }
}
```

**Run with:**

```bash
# Quick check
cargo test --package walrust-dst

# Thorough (10K cases per property)
PROPTEST_CASES=10000 cargo test --package walrust-dst

# Reproduce failure
PROPTEST_SEED=0x1234567890abcdef cargo test --package walrust-dst
```

### Phase 2: Fault Injection (3-5 days)

**Create S3 fault injector:**

```rust
// walrust-dst/src/fault_s3.rs
use anyhow::Result;
use rand::Rng;

pub enum S3Fault {
    /// Random 500 Internal Server Error
    InternalError { rate: f64 },

    /// Slow upload (simulates network congestion)
    SlowWrite { delay_ms: u64 },

    /// Eventual consistency (object appears, then disappears)
    EventualConsistency { delay_ms: u64 },

    /// Partial write (upload stops mid-stream)
    PartialWrite { at_bytes: usize },

    /// Silent corruption (upload succeeds but data is wrong)
    SilentCorruption { rate: f64 },
}

pub struct FaultInjectingS3<S> {
    inner: S,
    faults: Vec<S3Fault>,
    rng: RefCell<StdRng>,
}

impl<S: S3Backend> S3Backend for FaultInjectingS3<S> {
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<()> {
        for fault in &self.faults {
            match fault {
                S3Fault::InternalError { rate } => {
                    if self.rng.borrow_mut().gen::<f64>() < *rate {
                        return Err(anyhow!("500 Internal Server Error"));
                    }
                }
                S3Fault::SlowWrite { delay_ms } => {
                    tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
                }
                S3Fault::PartialWrite { at_bytes } => {
                    if data.len() > *at_bytes {
                        // Truncate data mid-write
                        return self.inner.put_object(key, data[..*at_bytes].to_vec()).await;
                    }
                }
                S3Fault::SilentCorruption { rate } => {
                    if self.rng.borrow_mut().gen::<f64>() < *rate {
                        // Corrupt one byte
                        let mut corrupted = data.clone();
                        corrupted[0] ^= 0xFF;
                        return self.inner.put_object(key, corrupted).await;
                    }
                }
                _ => {}
            }
        }

        self.inner.put_object(key, data).await
    }
}
```

**Chaos test scenarios:**

```rust
// walrust-dst/src/chaos.rs

#[test]
fn chaos_s3_500_errors() {
    let s3 = FaultInjectingS3::new(InMemoryS3::new(), vec![
        S3Fault::InternalError { rate: 0.1 }, // 10% failure rate
    ]);

    // Should retry and eventually succeed
    let result = run_backup_with_s3(s3, 100);
    assert!(result.is_ok(), "Backup should succeed despite transient S3 errors");
}

#[test]
fn chaos_crash_mid_snapshot() {
    // Simulate process crash during snapshot
    let result = std::panic::catch_unwind(|| {
        let db = create_test_db();

        // Start snapshot, crash halfway
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(50));
            std::process::abort(); // Simulate SIGKILL
        });

        walrust::snapshot(&db, "s3://bucket").unwrap();
    });

    // Recovery: should be able to continue from partial state
    // Verify no corruption in S3
}

#[test]
fn chaos_eventual_consistency() {
    let s3 = FaultInjectingS3::new(InMemoryS3::new(), vec![
        S3Fault::EventualConsistency { delay_ms: 5000 },
    ]);

    // Upload object
    s3.put_object("test.ltx", vec![1, 2, 3]).await?;

    // Immediately try to read - should handle "not found" gracefully
    let result = s3.get_object("test.ltx").await;

    // Wait for consistency
    tokio::time::sleep(Duration::from_secs(6)).await;
    let result = s3.get_object("test.ltx").await?;
    assert_eq!(result, vec![1, 2, 3]);
}
```

### Phase 3: CLI Tool (2 days)

**Create `walrust-dst` binary:**

```rust
// walrust-dst/src/main.rs
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "walrust-dst")]
#[command(about = "Deterministic Simulation Testing for Walrust")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Quick sanity check
    Smoke,

    /// Property-based tests
    Properties {
        #[arg(long, default_value = "1000")]
        seeds: u64,
    },

    /// Fault injection tests
    Chaos {
        #[arg(long, default_value = "s3_errors,crashes,eventual_consistency")]
        faults: String,
    },

    /// Stress test at scale
    Stress {
        #[arg(long, default_value = "100")]
        databases: usize,

        #[arg(long, default_value = "1000")]
        writes_per_sec: usize,
    },

    /// Long-running soak test
    Soak {
        #[arg(long, default_value = "24h")]
        duration: String,
    },

    /// Reproduce failure from seed
    Replay {
        #[arg(long)]
        seed: u64,
    },
}
```

### Phase 4: Continuous Testing (1 day)

**GitHub Actions:**

```yaml
# .github/workflows/battle-test.yml
name: Battle Test

on:
  push:
    branches: [main]
  pull_request:
  schedule:
    - cron: '0 4 * * *'  # 4 AM UTC daily

jobs:
  smoke:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable

      - name: Smoke tests
        run: cargo run -p walrust-dst -- smoke

  properties:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable

      - name: Property tests (quick)
        run: cargo test -p walrust-dst

      - name: Property tests (extended)
        if: github.event_name == 'schedule'
        run: PROPTEST_CASES=10000 cargo test -p walrust-dst

  chaos:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable

      - name: Chaos tests
        run: cargo run -p walrust-dst -- chaos

  soak:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable

      - name: 1-hour soak test
        run: cargo run -p walrust-dst -- soak --duration 1h
```

---

## Test Scenarios for Walrust

### Critical Scenarios

| Scenario | What | Expected Behavior |
|----------|------|-------------------|
| **Crash during WAL sync** | SIGKILL mid-upload | No partial LTX files in S3 |
| **S3 500 errors** | Transient failures | Retry succeeds, no data loss |
| **WAL checkpoint race** | SQLite resets WAL while reading | Detect and retry |
| **Eventual consistency** | Object appears then disappears | Handle gracefully |
| **Clock skew** | System clock jumps backward | Snapshot intervals still work |
| **Concurrent snapshots** | Two snapshots triggered simultaneously | Only one runs |
| **Restore corruption** | Downloaded LTX is corrupted | Detect via checksum |

### Scale Scenarios

| Scenario | Parameters | Success Criteria |
|----------|------------|------------------|
| Many DBs | 1000 databases | All sync reliably |
| High write rate | 1000 writes/sec/db | No backlog, all frames uploaded |
| Large WAL | 1GB WAL file | Memory bounded |
| Long restore | Restore 1TB from S3 | Completes without OOM |

---

## Success Criteria Before v1.0

Before releasing as production-ready:

- [ ] **10K+ seeds pass** all property tests
- [ ] **Zero data loss** in chaos tests (crashes, S3 faults)
- [ ] **Litestream compatibility** verified (restore Litestream backups)
- [ ] **100+ database scale** tested without issues
- [ ] **1000 writes/sec/db** sustained (with batching)
- [ ] **24h soak test** passes with no memory leaks
- [ ] **CI runs nightly** for 2+ weeks with zero failures

---

## Quick Start

```bash
# Create DST crate
cargo new walrust-dst --name walrust-dst
cd walrust-dst

# Add dependencies
cargo add walrust --path=..
cargo add proptest rusqlite anyhow rand clap serde serde_json

# Run smoke tests
cargo run -- smoke

# Run property tests
cargo run -- properties --seeds 1000

# Run chaos tests
cargo run -- chaos

# Reproduce a failure
cargo run -- replay --seed 8675309
```

---

## Chaos Test Roadmap

### Current State (v0.1.5+)

**Architecture: StorageBackend Trait Approach**

Instead of MadSim, we use a simpler trait-based approach:

1. Created `StorageBackend` trait in `src/storage.rs`
2. Created `S3Backend` implementation for production
3. Created `MockStorageBackend` in walrust-dst with fault injection
4. Added `walrust::testable` module exposing sync functions that accept `&dyn StorageBackend`

This is simpler than MadSim and allows direct testing of walrust's actual code paths.

**Implemented:**
- ✅ Property tests (7 properties, 100+ cases each)
- ✅ Smoke tests (SQLite WAL, LTX roundtrip, verification)
- ✅ Corruption detection test (tests `ltx::verify_ltx()`)
- ✅ `StorageBackend` trait abstraction
- ✅ `MockStorageBackend` with fault injection (RandomError, Latency, PartialWrite, SilentCorruption, EventualConsistency)
- ✅ `walrust::testable` module with `sync_wal`, `take_snapshot`, `restore` using `&dyn StorageBackend`
- ✅ Real chaos tests calling actual walrust sync functions with MockStorageBackend

**Chaos Tests Now Working:**
- ✅ `chaos_silent_corruption` - Tests LTX checksum verification (>90% detection rate)
- ✅ `test_snapshot_with_mock_storage` - Baseline test with no faults (100% success)
- ✅ `chaos_s3_errors` - Documents walrust's lack of retry logic (expected to fail)
- ✅ `chaos_eventual_consistency` - Observational test of EC behavior

**Next Steps:**

- ❌ **Retry Logic** - Add exponential backoff with jitter to S3 operations
  - Exponential backoff: 100ms → 200ms → 400ms → ... capped at 30s
  - Jitter to avoid thundering herd
  - Max retries (default: 5)
  - Error classification:
    - Retry: 500/502/503/504, timeouts, network errors
    - Fail immediately: 400 (bug), 401/403 (auth), 404 (context-dependent)
  - Circuit breaker after N consecutive failures across all operations

- ❌ **Failure Webhooks** - Alert on persistent failures
  - POST to configurable URL on: auth failures, repeated S3 errors, corruption detected
  - Payload: `{ "event": "sync_failed", "database": "...", "error": "...", "attempts": N }`
  - Optional: Slack/Discord/PagerDuty integrations
  - Config: `webhooks: [{ url: "https://...", events: ["auth_failure", "sync_failed"] }]`

- ❌ Make `chaos_s3_errors` pass after adding retry logic
- ❌ Add crash recovery tests
- ❌ Stress testing with high error rates

### How It Works

The key insight: We don't need MadSim. By introducing a `StorageBackend` trait,
we can test walrust's actual code with a mock storage backend.

```rust
// walrust/src/storage.rs - The trait
#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()>;
    async fn download_bytes(&self, key: &str) -> Result<Vec<u8>>;
    // ... other methods
}

// walrust/src/sync.rs - Testable functions
pub mod testable {
    pub async fn sync_wal(
        storage: &dyn StorageBackend,  // Accept trait object
        prefix: &str,
        state: &mut SyncState,
    ) -> Result<u64> {
        // ... actual walrust sync logic
    }
}
```

```rust
// walrust-dst/src/mock_storage.rs - Mock with fault injection
pub struct MockStorageBackend { ... }

impl StorageBackend for MockStorageBackend {
    async fn upload_bytes(&self, key: &str, data: Vec<u8>) -> Result<()> {
        // Check for random error injection
        if self.should_inject_error() {
            return Err(anyhow!("Service unavailable (injected)"));
        }
        // ... store in memory
    }
}
```

```rust
// walrust-dst/src/chaos.rs - Real chaos tests
pub async fn chaos_s3_errors(seed: u64, error_rate: f64, iterations: u32) -> Result<ChaosTestResult> {
    let storage = MockStorageBackend::new(
        MockStorageConfig::new("test-bucket")
            .with_seed(seed)
            .with_fault(StorageFault::RandomError { rate: error_rate })
    );

    let mut state = SyncState::new(db_path)?;

    // This calls ACTUAL walrust code with fault injection!
    let result = testable::take_snapshot(&storage, "", &mut state).await;
    // ...
}
```

### Fixing Walrust Based on Failures

When `chaos_s3_errors` fails (as expected), add retry logic:

```rust
// src/s3.rs or src/storage.rs
pub async fn upload_bytes_with_retry<S: StorageBackend>(
    storage: &S,
    key: &str,
    data: Vec<u8>,
    max_retries: u32,
) -> Result<()> {
    let mut attempts = 0;
    loop {
        match storage.upload_bytes(key, data.clone()).await {
            Ok(()) => return Ok(()),
            Err(e) if is_retryable(&e) && attempts < max_retries => {
                attempts += 1;
                tokio::time::sleep(backoff(attempts)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Test Results

Running `cargo test` in walrust-dst shows all 22 tests passing:

```
test chaos::tests::test_chaos_silent_corruption ... ok
test chaos::tests::test_snapshot_baseline ... ok
test chaos::tests::test_chaos_s3_errors_documents_current_behavior ... ok
test chaos::tests::test_eventual_consistency ... ok
test properties::tests::test_prop_ltx_roundtrip ... ok
test properties::tests::test_prop_durability ... ok
// ... 22 total
```

The `chaos_s3_errors` test currently documents that walrust lacks retry logic.
After adding retry logic, this test should pass with high success rates even
under fault injection.

---

## References

- [sled simulation guide](https://sled.rs/simulation.html)
- [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md)
- [MadSim](https://github.com/madsim-rs/madsim) (not needed with trait approach)
- [Jepsen](https://jepsen.io)
- [Redlite DST implementation](../redlite/redlite-dst/)
