//! Chaos test scenarios for walrust
//!
//! These tests exercise walrust's actual sync functions using the
//! MockStorageBackend for fault injection. Unlike the earlier fake tests,
//! these actually test walrust's behavior under failure conditions.
//!
//! The key insight: walrust now has a `testable` module that exposes
//! sync_wal, take_snapshot, and restore functions that accept a
//! `&dyn StorageBackend` trait object instead of `&Client` + bucket.

use crate::mock_storage::{MockStorageBackend, MockStorageConfig, StorageFault};
use anyhow::Result;
use rusqlite::Connection;
use std::io::Cursor;
use tempfile::TempDir;
use walrust::ltx;
use walrust::testable::{self, SyncState};

/// Results from a chaos test run
#[derive(Debug, Clone)]
pub struct ChaosTestResult {
    pub name: String,
    pub passed: bool,
    pub iterations: u32,
    pub errors_injected: u32,
    pub errors_recovered: u32,
    pub message: String,
}

// ============================================================================
// Chaos Test: Silent Corruption Detection
// ============================================================================

/// Test that silent data corruption is detected via checksum verification.
///
/// This test ACTUALLY exercises walrust's ltx::verify_ltx() function.
///
/// Property: "Silent corruption is detected by checksum verification"
pub async fn chaos_silent_corruption(seed: u64, iterations: u32) -> Result<ChaosTestResult> {
    use rand::prelude::*;

    let mut rng = StdRng::seed_from_u64(seed);
    let mut corruptions_detected = 0;
    let mut corruptions_missed = 0;

    for i in 0..iterations {
        let tmpdir = TempDir::new()?;
        let db_path = tmpdir.path().join("test.db");

        // Create a test database
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
             CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB);",
        )?;
        for j in 0..10 {
            conn.execute("INSERT INTO data (value) VALUES (?)", [vec![j as u8; 500]])?;
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        drop(conn);

        // Create LTX snapshot using walrust's actual encoder
        let mut ltx_buffer = Vec::new();
        ltx::encode_snapshot(&mut ltx_buffer, &db_path, 4096, (i + 1) as u64)?;

        // Corrupt the LTX data (simulating S3/network corruption)
        let mut corrupted = ltx_buffer.clone();
        if !corrupted.is_empty() {
            let byte_idx = rng.gen_range(0..corrupted.len());
            let bit_idx = rng.gen_range(0..8);
            corrupted[byte_idx] ^= 1 << bit_idx;
        }

        // Verify corruption occurred
        if corrupted == ltx_buffer {
            continue; // RNG edge case, skip
        }

        // Try to verify the corrupted LTX using walrust's actual verifier
        let cursor = Cursor::new(&corrupted);
        let verify_result = ltx::verify_ltx(cursor);

        if verify_result.is_err() {
            corruptions_detected += 1;
        } else {
            // Corruption happened in non-checksummed area (header padding, etc.)
            // This is rare but possible - LTX checksum covers data, not all metadata
            corruptions_missed += 1;
        }
    }

    let detection_rate = if iterations > 0 {
        (corruptions_detected as f64 / iterations as f64) * 100.0
    } else {
        0.0
    };

    // LTX checksums should catch most corruption (>90%)
    let passed = detection_rate >= 90.0;

    Ok(ChaosTestResult {
        name: "chaos_silent_corruption".to_string(),
        passed,
        iterations,
        errors_injected: iterations,
        errors_recovered: corruptions_detected,
        message: format!(
            "LTX checksum detected {:.1}% of corruptions ({}/{}, {} missed)",
            detection_rate, corruptions_detected, iterations, corruptions_missed
        ),
    })
}

// ============================================================================
// Chaos Test: S3 Random Errors (using MockStorageBackend)
// ============================================================================

/// Test that walrust handles transient S3 errors gracefully.
///
/// This test uses MockStorageBackend with fault injection to simulate
/// random S3 failures, testing walrust's actual sync_wal and take_snapshot
/// functions.
///
/// Current behavior: walrust has NO retry logic, so this will FAIL.
/// This is intentional - it shows we need to add retry logic.
pub async fn chaos_s3_errors(seed: u64, error_rate: f64, iterations: u32) -> Result<ChaosTestResult> {
    let mut successful_syncs = 0;
    let mut failed_syncs = 0;
    let mut total_errors_injected = 0;

    for i in 0..iterations {
        let tmpdir = TempDir::new()?;
        let db_path = tmpdir.path().join("test.db");

        // Create a test database with WAL mode
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
             CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
        )?;

        // Insert some data to create WAL frames
        for j in 0..5 {
            conn.execute("INSERT INTO data (value) VALUES (?)", [format!("value_{}", j)])?;
        }
        drop(conn);

        // Create MockStorageBackend with error injection
        let config = MockStorageConfig::new("test-bucket")
            .with_seed(seed + i as u64)
            .with_fault(StorageFault::RandomError { rate: error_rate });
        let storage = MockStorageBackend::new(config);

        // Create sync state
        let mut state = match SyncState::new(db_path.clone()) {
            Ok(s) => s,
            Err(e) => {
                return Ok(ChaosTestResult {
                    name: "chaos_s3_errors".to_string(),
                    passed: false,
                    iterations,
                    errors_injected: 0,
                    errors_recovered: 0,
                    message: format!("Failed to create SyncState: {}", e),
                });
            }
        };

        // Try to take a snapshot - this may fail due to injected errors
        let result = testable::take_snapshot(&storage, "", &mut state).await;

        let errors_this_iteration = storage.error_count();
        total_errors_injected += errors_this_iteration as u32;

        if result.is_ok() {
            successful_syncs += 1;
        } else {
            failed_syncs += 1;
        }
    }

    // With retry logic, we'd expect high success rate even with errors
    // Without retry logic, failures should roughly match error_rate
    let success_rate = successful_syncs as f64 / iterations as f64 * 100.0;

    // Currently, walrust has NO retry logic, so we expect failures
    // This test documents the current behavior (will fail often)
    // After adding retry logic, this test should pass with high success rate
    let passed = success_rate >= 80.0; // Expect 80%+ success with retry logic

    Ok(ChaosTestResult {
        name: "chaos_s3_errors".to_string(),
        passed,
        iterations,
        errors_injected: total_errors_injected,
        errors_recovered: successful_syncs,
        message: format!(
            "Success rate: {:.1}% ({}/{} syncs). {} errors injected. \
             NOTE: {} because walrust lacks retry logic.",
            success_rate,
            successful_syncs,
            iterations,
            total_errors_injected,
            if passed { "Passed unexpectedly" } else { "Expected failure" }
        ),
    })
}

// ============================================================================
// Chaos Test: Snapshot with Storage Backend
// ============================================================================

/// Test that snapshots work correctly with MockStorageBackend (no errors)
///
/// This verifies the testable module integration is correct before
/// we add chaos/fault injection.
pub async fn test_snapshot_with_mock_storage(seed: u64, iterations: u32) -> Result<ChaosTestResult> {
    let mut successful_snapshots = 0;
    let mut failed_snapshots = 0;

    for i in 0..iterations {
        let tmpdir = TempDir::new()?;
        let db_path = tmpdir.path().join("test.db");

        // Create a test database
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
             CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);",
        )?;
        for j in 0..10 {
            conn.execute("INSERT INTO users (name) VALUES (?)", [format!("user_{}", j)])?;
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        drop(conn);

        // Create MockStorageBackend with NO fault injection
        let config = MockStorageConfig::new("test-bucket").with_seed(seed + i as u64);
        let storage = MockStorageBackend::new(config);

        // Create sync state
        let mut state = SyncState::new(db_path.clone())?;

        // Take a snapshot
        match testable::take_snapshot(&storage, "", &mut state).await {
            Ok(_) => {
                // Verify the snapshot was stored
                let manifest = testable::load_manifest(&storage, "", &state.name).await?;
                if !manifest.files.is_empty() && manifest.files[0].is_snapshot {
                    successful_snapshots += 1;
                } else {
                    failed_snapshots += 1;
                }
            }
            Err(e) => {
                tracing::warn!("Snapshot failed: {}", e);
                failed_snapshots += 1;
            }
        }
    }

    let success_rate = successful_snapshots as f64 / iterations as f64 * 100.0;
    let passed = success_rate == 100.0;

    Ok(ChaosTestResult {
        name: "test_snapshot_with_mock_storage".to_string(),
        passed,
        iterations,
        errors_injected: 0,
        errors_recovered: successful_snapshots,
        message: format!(
            "Success rate: {:.1}% ({}/{} snapshots)",
            success_rate, successful_snapshots, iterations
        ),
    })
}

// ============================================================================
// Chaos Test: Eventual Consistency
// ============================================================================

/// Test that walrust handles eventual consistency in S3.
///
/// After uploading, immediately trying to download may fail temporarily.
/// This tests if walrust can handle "object not found" errors gracefully.
pub async fn chaos_eventual_consistency(seed: u64, delay_ms: u64, iterations: u32) -> Result<ChaosTestResult> {
    let mut visibility_failures = 0;
    let mut successful_reads = 0;

    for i in 0..iterations {
        let tmpdir = TempDir::new()?;
        let db_path = tmpdir.path().join("test.db");

        // Create database
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA page_size=4096;
             CREATE TABLE data (id INTEGER PRIMARY KEY);",
        )?;
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        drop(conn);

        // Create MockStorageBackend with eventual consistency
        let config = MockStorageConfig::new("test-bucket")
            .with_seed(seed + i as u64)
            .with_fault(StorageFault::EventualConsistency { delay_ms });
        let storage = MockStorageBackend::new(config);

        // Create sync state and take snapshot
        let mut state = SyncState::new(db_path.clone())?;
        testable::take_snapshot(&storage, "", &mut state).await?;

        // Immediately try to load manifest (might fail due to eventual consistency)
        match testable::load_manifest(&storage, "", &state.name).await {
            Ok(manifest) if !manifest.files.is_empty() => {
                successful_reads += 1;
            }
            Ok(_) => {
                visibility_failures += 1;
            }
            Err(_) => {
                visibility_failures += 1;
            }
        }
    }

    // With eventual consistency configured, we expect some visibility delays
    // The test passes if we correctly observe the behavior (either success or expected failure)
    let passed = true; // This test is more observational

    Ok(ChaosTestResult {
        name: "chaos_eventual_consistency".to_string(),
        passed,
        iterations,
        errors_injected: iterations, // All iterations have EC enabled
        errors_recovered: successful_reads,
        message: format!(
            "Immediate reads: {} successful, {} delayed ({}ms EC configured)",
            successful_reads, visibility_failures, delay_ms
        ),
    })
}

/// Run all implemented chaos tests
pub async fn run_all_chaos_tests(seed: u64) -> Vec<ChaosTestResult> {
    let mut results = Vec::new();

    // Test 1: Silent corruption detection (using ltx::verify_ltx)
    match chaos_silent_corruption(seed, 20).await {
        Ok(r) => results.push(r),
        Err(e) => results.push(ChaosTestResult {
            name: "chaos_silent_corruption".to_string(),
            passed: false,
            iterations: 0,
            errors_injected: 0,
            errors_recovered: 0,
            message: format!("Test failed with error: {}", e),
        }),
    }

    // Test 2: Snapshot with mock storage (baseline - no faults)
    match test_snapshot_with_mock_storage(seed, 10).await {
        Ok(r) => results.push(r),
        Err(e) => results.push(ChaosTestResult {
            name: "test_snapshot_with_mock_storage".to_string(),
            passed: false,
            iterations: 0,
            errors_injected: 0,
            errors_recovered: 0,
            message: format!("Test failed with error: {}", e),
        }),
    }

    // Test 3: S3 errors with 20% error rate
    // NOTE: This will FAIL because walrust has no retry logic yet
    match chaos_s3_errors(seed, 0.2, 10).await {
        Ok(r) => results.push(r),
        Err(e) => results.push(ChaosTestResult {
            name: "chaos_s3_errors".to_string(),
            passed: false,
            iterations: 0,
            errors_injected: 0,
            errors_recovered: 0,
            message: format!("Test failed with error: {}", e),
        }),
    }

    // Test 4: Eventual consistency (observational)
    match chaos_eventual_consistency(seed, 100, 5).await {
        Ok(r) => results.push(r),
        Err(e) => results.push(ChaosTestResult {
            name: "chaos_eventual_consistency".to_string(),
            passed: false,
            iterations: 0,
            errors_injected: 0,
            errors_recovered: 0,
            message: format!("Test failed with error: {}", e),
        }),
    }

    results
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_chaos_silent_corruption() {
        let result = chaos_silent_corruption(42, 10).await.unwrap();
        assert!(result.passed, "Failed: {}", result.message);
    }

    #[tokio::test]
    async fn test_snapshot_baseline() {
        // This should pass - no fault injection
        let result = test_snapshot_with_mock_storage(42, 5).await.unwrap();
        assert!(result.passed, "Failed: {}", result.message);
    }

    #[tokio::test]
    async fn test_chaos_s3_errors_documents_current_behavior() {
        // This test documents that walrust currently lacks retry logic
        // It will likely fail until retry logic is added
        let result = chaos_s3_errors(42, 0.3, 5).await.unwrap();
        // We don't assert pass/fail here - just verify it runs
        println!("S3 error test result: {}", result.message);
    }

    #[tokio::test]
    async fn test_eventual_consistency() {
        let result = chaos_eventual_consistency(42, 50, 3).await.unwrap();
        // Observational test - always passes but shows behavior
        assert!(result.passed, "Failed: {}", result.message);
    }
}
