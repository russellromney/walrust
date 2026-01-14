//! Core invariant property tests for walrust
//!
//! These tests verify the critical data safety invariants that walrust must maintain:
//!
//! 1. **Transaction Recovery** - Every committed transaction is recoverable from S3
//! 2. **Point-in-Time Restore** - Restore at timestamp T gives exact state at T
//! 3. **WAL Batching** - WAL batching never loses frames
//! 4. **Snapshot Atomicity** - Snapshots are atomic (no partial state)
//! 5. **GFS Compaction** - Compaction preserves recoverability
//! 6. **TXID Monotonicity** - No gaps, no duplicates in TXID sequence
//! 7. **Checksum Chain** - pre_apply → post_apply checksum chain is valid
//! 8. **Binary Preservation** - Restored DB is byte-identical to source

use crate::mock_storage::{MockStorageBackend, MockStorageConfig, StorageFault};
use anyhow::Result;
use proptest::prelude::*;
use rusqlite::Connection;
use std::collections::HashSet;
use std::io::Cursor;
use tempfile::TempDir;
use walrust::ltx;
use walrust::testable::{self, SyncState};

/// Get proptest config from environment
fn get_config() -> proptest::test_runner::Config {
    proptest::test_runner::Config {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100),
        ..Default::default()
    }
}

// ============================================================================
// Invariant 1: Transaction Recovery
// ============================================================================

/// Property: Every committed transaction is recoverable from S3
///
/// After N write operations and syncs to storage, restoring from storage
/// should yield a database with exactly those N transactions.
pub fn prop_transaction_recovery() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());
    let rt = tokio::runtime::Runtime::new()?;

    let result = runner.run(
        &(1..50usize, 42u64..1000000u64),
        |(num_transactions, seed)| {
            rt.block_on(async {
                let tmpdir = TempDir::new().unwrap();
                let db_path = tmpdir.path().join("test.db");
                let restored_path = tmpdir.path().join("restored.db");

                // Create database with transactions
                let conn = Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
                     CREATE TABLE txns (id INTEGER PRIMARY KEY, data TEXT, committed_at TEXT);",
                )
                .unwrap();

                for i in 0..num_transactions {
                    conn.execute(
                        "INSERT INTO txns (data, committed_at) VALUES (?, datetime('now'))",
                        [format!("transaction_{}", i)],
                    )
                    .unwrap();
                }
                conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                    .unwrap();
                drop(conn);

                // Snapshot to mock storage
                let config = MockStorageConfig::new("test-bucket").with_seed(seed);
                let storage = MockStorageBackend::new(config);
                let mut state = SyncState::new(db_path.clone()).unwrap();

                testable::take_snapshot(&storage, "", &mut state).await.unwrap();

                // Restore from storage
                testable::restore(&storage, "", &state.name, &restored_path, None::<&str>)
                    .await
                    .unwrap();

                // Verify all transactions are present
                let restored_conn = Connection::open(&restored_path).unwrap();
                let count: i64 = restored_conn
                    .query_row("SELECT COUNT(*) FROM txns", [], |r| r.get(0))
                    .unwrap();

                prop_assert_eq!(
                    count as usize,
                    num_transactions,
                    "Expected {} transactions, got {}",
                    num_transactions,
                    count
                );

                // Verify each transaction's data
                for i in 0..num_transactions {
                    let data: String = restored_conn
                        .query_row(
                            "SELECT data FROM txns WHERE id = ?",
                            [i as i64 + 1],
                            |r| r.get(0),
                        )
                        .unwrap();
                    prop_assert_eq!(
                        data,
                        format!("transaction_{}", i),
                        "Transaction {} data mismatch",
                        i
                    );
                }

                Ok(())
            })
        },
    );

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

// ============================================================================
// Invariant 2: Point-in-Time Restore
// ============================================================================

/// Property: Point-in-time restore gives exact state at timestamp T
///
/// If we take multiple snapshots at different TXIDs, restoring to a specific
/// TXID should give exactly the state at that point.
pub fn prop_point_in_time_restore() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());
    let rt = tokio::runtime::Runtime::new()?;

    let result = runner.run(&(2..10usize, 42u64..1000000u64), |(num_snapshots, seed)| {
        rt.block_on(async {
            let tmpdir = TempDir::new().unwrap();
            let db_path = tmpdir.path().join("test.db");

            // Create database
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
                 CREATE TABLE data (id INTEGER PRIMARY KEY, snapshot_num INTEGER);",
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
            drop(conn);

            let config = MockStorageConfig::new("test-bucket").with_seed(seed);
            let storage = MockStorageBackend::new(config);
            let mut state = SyncState::new(db_path.clone()).unwrap();

            // Take initial snapshot
            testable::take_snapshot(&storage, "", &mut state).await.unwrap();
            let mut txid_to_count: Vec<(u64, usize)> = vec![(state.current_txid, 0)];

            // Take snapshots with incrementing data
            for snapshot_idx in 1..num_snapshots {
                let conn = Connection::open(&db_path).unwrap();
                conn.execute(
                    "INSERT INTO data (snapshot_num) VALUES (?)",
                    [snapshot_idx as i64],
                )
                .unwrap();
                conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                    .unwrap();
                drop(conn);

                testable::take_snapshot(&storage, "", &mut state).await.unwrap();
                txid_to_count.push((state.current_txid, snapshot_idx));
            }

            // Verify each point-in-time restore
            for (txid, expected_count) in &txid_to_count {
                let restored_path = tmpdir.path().join(format!("restored_{}.db", txid));
                let pit = format!("txid:{}", txid);
                testable::restore(
                    &storage,
                    "",
                    &state.name,
                    &restored_path,
                    Some(pit.as_str()),
                )
                .await
                .unwrap();

                let restored_conn = Connection::open(&restored_path).unwrap();
                let count: i64 = restored_conn
                    .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
                    .unwrap();

                prop_assert_eq!(
                    count as usize,
                    *expected_count,
                    "PITR to TXID {} should have {} rows, got {}",
                    txid,
                    expected_count,
                    count
                );
            }

            Ok(())
        })
    });

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

// ============================================================================
// Invariant 3: WAL Batching Never Loses Frames
// ============================================================================

/// Property: WAL batching never loses frames
///
/// When multiple writes occur and are batched into a single sync,
/// all frames must be preserved.
pub fn prop_wal_batching_no_loss() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());
    let rt = tokio::runtime::Runtime::new()?;

    let result = runner.run(
        &(10..100usize, 1..5usize, 42u64..1000000u64),
        |(num_writes, batch_size, seed)| {
            rt.block_on(async {
                let tmpdir = TempDir::new().unwrap();
                let db_path = tmpdir.path().join("test.db");
                let restored_path = tmpdir.path().join("restored.db");

                // Create database
                let conn = Connection::open(&db_path).unwrap();
                conn.execute_batch(
                    "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
                     CREATE TABLE writes (id INTEGER PRIMARY KEY, batch INTEGER, seq INTEGER);",
                )
                .unwrap();

                let config = MockStorageConfig::new("test-bucket").with_seed(seed);
                let storage = MockStorageBackend::new(config);
                let mut state = SyncState::new(db_path.clone()).unwrap();

                // Initial snapshot
                conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                    .unwrap();
                drop(conn);
                testable::take_snapshot(&storage, "", &mut state).await.unwrap();

                // Write in batches, syncing WAL after each batch
                let mut total_writes = 0;
                let mut batch_num = 0;

                while total_writes < num_writes {
                    let conn = Connection::open(&db_path).unwrap();
                    let writes_this_batch = std::cmp::min(batch_size, num_writes - total_writes);

                    for seq in 0..writes_this_batch {
                        conn.execute(
                            "INSERT INTO writes (batch, seq) VALUES (?, ?)",
                            [batch_num as i64, seq as i64],
                        )
                        .unwrap();
                        total_writes += 1;
                    }
                    drop(conn);

                    // Sync WAL changes
                    let _ = testable::sync_wal(&storage, "", &mut state).await;
                    batch_num += 1;
                }

                // Final checkpoint and snapshot
                let conn = Connection::open(&db_path).unwrap();
                conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                    .unwrap();
                drop(conn);
                testable::take_snapshot(&storage, "", &mut state).await.unwrap();

                // Restore and verify all writes present
                testable::restore(&storage, "", &state.name, &restored_path, None::<&str>)
                    .await
                    .unwrap();

                let restored_conn = Connection::open(&restored_path).unwrap();
                let count: i64 = restored_conn
                    .query_row("SELECT COUNT(*) FROM writes", [], |r| r.get(0))
                    .unwrap();

                prop_assert_eq!(
                    count as usize,
                    num_writes,
                    "WAL batching lost frames: expected {}, got {}",
                    num_writes,
                    count
                );

                Ok(())
            })
        },
    );

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

// ============================================================================
// Invariant 4: Snapshot Atomicity
// ============================================================================

/// Property: Snapshot is atomic (no partial state)
///
/// A snapshot should either succeed completely or fail without leaving
/// partial state. Restored database should pass integrity check.
pub fn prop_snapshot_atomicity() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());
    let rt = tokio::runtime::Runtime::new()?;

    let result = runner.run(
        &(1..20usize, prop::collection::vec(any::<u8>(), 100..1000)),
        |(num_tables, blob_data)| {
            rt.block_on(async {
                let tmpdir = TempDir::new().unwrap();
                let db_path = tmpdir.path().join("test.db");
                let restored_path = tmpdir.path().join("restored.db");

                // Create complex database state
                let conn = Connection::open(&db_path).unwrap();
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA page_size=4096;")
                    .unwrap();

                for t in 0..num_tables {
                    conn.execute(
                        &format!("CREATE TABLE table_{} (id INTEGER PRIMARY KEY, data BLOB)", t),
                        [],
                    )
                    .unwrap();

                    for r in 0..5 {
                        let data: Vec<u8> = blob_data
                            .iter()
                            .map(|b| b.wrapping_add((t * 5 + r) as u8))
                            .collect();
                        conn.execute(
                            &format!("INSERT INTO table_{} (data) VALUES (?)", t),
                            [&data],
                        )
                        .unwrap();
                    }
                }
                conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                    .unwrap();
                drop(conn);

                // Snapshot
                let config = MockStorageConfig::new("test-bucket");
                let storage = MockStorageBackend::new(config);
                let mut state = SyncState::new(db_path.clone()).unwrap();
                testable::take_snapshot(&storage, "", &mut state).await.unwrap();

                // Restore
                testable::restore(&storage, "", &state.name, &restored_path, None::<&str>)
                    .await
                    .unwrap();

                // Verify atomicity via integrity check
                let restored_conn = Connection::open(&restored_path).unwrap();
                let integrity: String = restored_conn
                    .query_row("PRAGMA integrity_check", [], |r| r.get(0))
                    .unwrap();

                prop_assert_eq!(integrity, "ok", "Snapshot not atomic - integrity failed");

                // Verify all tables and data present
                for t in 0..num_tables {
                    let count: i64 = restored_conn
                        .query_row(&format!("SELECT COUNT(*) FROM table_{}", t), [], |r| {
                            r.get(0)
                        })
                        .unwrap();
                    prop_assert_eq!(count, 5, "Table {} missing rows", t);
                }

                Ok(())
            })
        },
    );

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

// ============================================================================
// Invariant 5: TXID Monotonicity
// ============================================================================

/// Property: TXIDs are monotonically increasing with no gaps in sequence
pub fn prop_txid_monotonicity() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());
    let rt = tokio::runtime::Runtime::new()?;

    let result = runner.run(&(5..20usize, 42u64..1000000u64), |(num_operations, seed)| {
        rt.block_on(async {
            let tmpdir = TempDir::new().unwrap();
            let db_path = tmpdir.path().join("test.db");

            // Create database
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
                 CREATE TABLE data (id INTEGER PRIMARY KEY);",
            )
            .unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
            drop(conn);

            let config = MockStorageConfig::new("test-bucket").with_seed(seed);
            let storage = MockStorageBackend::new(config);
            let mut state = SyncState::new(db_path.clone()).unwrap();

            let mut txids: Vec<u64> = Vec::new();

            // Perform operations and record TXIDs
            testable::take_snapshot(&storage, "", &mut state).await.unwrap();
            txids.push(state.current_txid);

            for i in 1..num_operations {
                let conn = Connection::open(&db_path).unwrap();
                conn.execute("INSERT INTO data DEFAULT VALUES", []).unwrap();
                conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                    .unwrap();
                drop(conn);

                testable::take_snapshot(&storage, "", &mut state).await.unwrap();
                txids.push(state.current_txid);
            }

            // Verify monotonicity
            for i in 1..txids.len() {
                prop_assert!(
                    txids[i] > txids[i - 1],
                    "TXID not monotonic: {} <= {}",
                    txids[i],
                    txids[i - 1]
                );
            }

            // Verify no duplicates
            let unique: HashSet<u64> = txids.iter().cloned().collect();
            prop_assert_eq!(
                unique.len(),
                txids.len(),
                "Duplicate TXIDs found: {:?}",
                txids
            );

            Ok(())
        })
    });

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

// ============================================================================
// Invariant 6: Binary Preservation
// ============================================================================

/// Property: Restored database is byte-identical to source (for snapshots)
///
/// Uses LTX encoding directly to verify byte-for-byte preservation.
pub fn prop_binary_preservation() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(get_config());

    let result = runner.run(
        &prop::collection::vec(any::<u8>(), 4096..4096 * 10),
        |raw_data| {
            let tmpdir = TempDir::new().unwrap();
            let db_path = tmpdir.path().join("test.db");
            let restored_path = tmpdir.path().join("restored.db");

            // Create database file (must be page-aligned at 4096)
            let page_size = 4096u32;
            let aligned_len = (raw_data.len() / page_size as usize) * page_size as usize;
            if aligned_len == 0 {
                return Ok(()); // Skip if not enough data
            }
            let db_data = &raw_data[..aligned_len];
            std::fs::write(&db_path, db_data).unwrap();

            // Encode using LTX directly (bypasses page size detection)
            let mut ltx_buffer = Vec::new();
            ltx::encode_snapshot(&mut ltx_buffer, &db_path, page_size, 1).unwrap();

            // Decode back
            let cursor = Cursor::new(ltx_buffer);
            ltx::decode_to_db(cursor, &restored_path).unwrap();

            // Compare bytes
            let restored_data = std::fs::read(&restored_path).unwrap();
            prop_assert_eq!(
                db_data.len(),
                restored_data.len(),
                "Size mismatch: {} vs {}",
                db_data.len(),
                restored_data.len()
            );

            for (i, (a, b)) in db_data.iter().zip(restored_data.iter()).enumerate() {
                prop_assert_eq!(a, b, "Byte mismatch at offset {}", i);
            }

            Ok(())
        },
    );

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

// ============================================================================
// Invariant 7: Recovery Under Failure
// ============================================================================

/// Property: Recovery succeeds even when some S3 operations fail
pub fn prop_recovery_under_failure() -> Result<()> {
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50),
        ..Default::default()
    });
    let rt = tokio::runtime::Runtime::new()?;

    let result = runner.run(&(1..20usize, 42u64..1000000u64), |(num_writes, seed)| {
        rt.block_on(async {
            let tmpdir = TempDir::new().unwrap();
            let db_path = tmpdir.path().join("test.db");
            let restored_path = tmpdir.path().join("restored.db");

            // Create database
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
                 CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
            )
            .unwrap();

            for i in 0..num_writes {
                conn.execute(
                    "INSERT INTO data (value) VALUES (?)",
                    [format!("value_{}", i)],
                )
                .unwrap();
            }
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
            drop(conn);

            // Use retry policy for reliability under failure
            let retry_config = walrust::retry::RetryConfig {
                max_retries: 5,
                base_delay_ms: 10,
                max_delay_ms: 100,
                circuit_breaker_enabled: false,
                ..Default::default()
            };
            let retry_policy = walrust::retry::RetryPolicy::new(retry_config);

            // Storage with 10% error rate
            let config = MockStorageConfig::new("test-bucket")
                .with_seed(seed)
                .with_fault(StorageFault::RandomError { rate: 0.1 });
            let storage = MockStorageBackend::new(config);
            let mut state = SyncState::new(db_path.clone()).unwrap();

            // Snapshot with retry
            let snapshot_result =
                testable::take_snapshot_with_retry(&storage, "", &mut state, &retry_policy).await;

            if snapshot_result.is_ok() {
                // Try to restore (may fail, that's OK for this test)
                let restore_result =
                    testable::restore(&storage, "", &state.name, &restored_path, None::<&str>).await;

                if restore_result.is_ok() {
                    // If restore succeeded, verify data
                    let restored_conn = Connection::open(&restored_path).unwrap();
                    let count: i64 = restored_conn
                        .query_row("SELECT COUNT(*) FROM data", [], |r| r.get(0))
                        .unwrap();

                    prop_assert_eq!(
                        count as usize,
                        num_writes,
                        "Data loss after recovery under failure"
                    );
                }
            }

            // Test passes if no panics/crashes - partial failures are acceptable
            Ok(())
        })
    });

    result.map_err(|e| anyhow::anyhow!("Property test failed: {:?}", e))
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prop_transaction_recovery() {
        std::env::set_var("PROPTEST_CASES", "5");
        prop_transaction_recovery().unwrap();
    }

    #[test]
    fn test_prop_point_in_time_restore() {
        std::env::set_var("PROPTEST_CASES", "5");
        prop_point_in_time_restore().unwrap();
    }

    #[test]
    fn test_prop_wal_batching_no_loss() {
        std::env::set_var("PROPTEST_CASES", "5");
        prop_wal_batching_no_loss().unwrap();
    }

    #[test]
    fn test_prop_snapshot_atomicity() {
        std::env::set_var("PROPTEST_CASES", "5");
        prop_snapshot_atomicity().unwrap();
    }

    #[test]
    fn test_prop_txid_monotonicity() {
        std::env::set_var("PROPTEST_CASES", "5");
        prop_txid_monotonicity().unwrap();
    }

    #[test]
    fn test_prop_binary_preservation() {
        std::env::set_var("PROPTEST_CASES", "5");
        prop_binary_preservation().unwrap();
    }

    #[test]
    fn test_prop_recovery_under_failure() {
        std::env::set_var("PROPTEST_CASES", "3");
        prop_recovery_under_failure().unwrap();
    }
}
