// Comprehensive tests for `walrust verify` command
//
// Tests cover: positive cases, negative cases, edge cases, and integration tests
// Requires S3/Tigris credentials. The workspace `make test` target injects
// them via Soup.
use anyhow::Result;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

/// Helper to create a test database file
fn create_test_db(path: &str) -> Result<()> {
    let conn = rusqlite::Connection::open(path)?;
    conn.execute("CREATE TABLE test(id INTEGER PRIMARY KEY, data TEXT)", [])?;
    conn.execute("INSERT INTO test VALUES (1, 'data1')", [])?;
    conn.execute("INSERT INTO test VALUES (2, 'data2')", [])?;
    Ok(())
}

/// Helper to get test bucket and endpoint
fn test_bucket_config() -> (String, String) {
    let bucket = std::env::var("WALRUST_TEST_BUCKET")
        .unwrap_or_else(|_| "walrust-test-rr-2026/verify-test".to_string());
    let endpoint = std::env::var("AWS_ENDPOINT_URL_S3")
        .unwrap_or_else(|_| "https://fly.storage.tigris.dev".to_string());
    (bucket, endpoint)
}

fn unique_db_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

// ============================================================================
// INTEGRATION TESTS - Require S3 credentials
// ============================================================================

#[test]
fn test_verify_valid_backup() -> Result<()> {
    let (bucket, endpoint) = test_bucket_config();
    let tempdir = TempDir::new()?;
    let db_path = tempdir.path().join("verify-test.db");

    // Create test database
    create_test_db(db_path.to_str().unwrap())?;

    // Take snapshot
    let snapshot_output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("snapshot")
        .arg(db_path.to_str().unwrap())
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    assert!(snapshot_output.status.success(), "Snapshot should succeed");

    // Verify the backup
    let verify_output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("verify")
        .arg("verify-test")
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    let stdout = String::from_utf8_lossy(&verify_output.stdout);

    // Should succeed with exit code 0
    assert!(
        verify_output.status.success(),
        "Verify should succeed: {}",
        stdout
    );

    // Check output format
    assert!(
        stdout.contains("Verifying backup:"),
        "Output should have header"
    );
    assert!(
        stdout.contains("Snapshot: Found generation"),
        "Output should confirm snapshot exists"
    );
    assert!(
        stdout.contains("All checks passed"),
        "Output should show success"
    );
    assert!(
        stdout.contains("Exit code: 0"),
        "Output should show exit code 0"
    );

    Ok(())
}

#[test]
fn test_verify_with_incrementals() -> Result<()> {
    let (bucket, endpoint) = test_bucket_config();
    let tempdir = TempDir::new()?;
    let db_path = tempdir.path().join("verify-incremental.db");

    // Create and populate database
    create_test_db(db_path.to_str().unwrap())?;

    // Take initial snapshot
    Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("snapshot")
        .arg(db_path.to_str().unwrap())
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    // Add more data to create incrementals
    let conn = rusqlite::Connection::open(&db_path)?;
    for i in 3..10 {
        conn.execute(
            "INSERT INTO test VALUES (?, ?)",
            rusqlite::params![i, format!("data{}", i)],
        )?;
    }
    drop(conn);

    // Start watch mode briefly to create incrementals
    let mut watch_cmd = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("watch")
        .arg(db_path.to_str().unwrap())
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .arg("--snapshot-interval")
        .arg("999999")
        .arg("--wal-sync-interval")
        .arg("1")
        .spawn()?;

    std::thread::sleep(std::time::Duration::from_secs(3));
    watch_cmd.kill()?;

    // Verify the backup
    let verify_output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("verify")
        .arg("verify-incremental")
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    let stdout = String::from_utf8_lossy(&verify_output.stdout);

    assert!(
        verify_output.status.success(),
        "Verify with incrementals should succeed: {}",
        stdout
    );
    assert!(stdout.contains("OK"), "Should have OK for verified files");
    assert!(stdout.contains("files"), "Should show file count");

    Ok(())
}

#[test]
fn test_verify_no_backup_found() -> Result<()> {
    let (bucket, endpoint) = test_bucket_config();
    let db_name = unique_db_name("nonexistent-database");

    // Try to verify non-existent database
    let verify_output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("verify")
        .arg(&db_name)
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    let stdout = String::from_utf8_lossy(&verify_output.stdout);
    let stderr = String::from_utf8_lossy(&verify_output.stderr);

    // A verifier cannot certify an empty/missing backup as success.
    assert!(
        !verify_output.status.success(),
        "verify must fail closed when no LTX files exist"
    );
    assert!(
        stdout.contains("No LTX files found") || stderr.contains("No LTX files found"),
        "Should report no files found"
    );

    Ok(())
}

#[test]
fn test_verify_exit_codes() -> Result<()> {
    let (bucket, endpoint) = test_bucket_config();
    let tempdir = TempDir::new()?;
    let db_path = tempdir.path().join("exit-code-test.db");

    create_test_db(db_path.to_str().unwrap())?;

    // Create valid backup
    Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("snapshot")
        .arg(db_path.to_str().unwrap())
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    // Verify should return exit code 0 for valid backup
    let output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("verify")
        .arg("exit-code-test")
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "Exit code should be 0 for valid backup: {}",
        stdout
    );
    assert!(
        stdout.contains("Exit code: 0"),
        "Output should explicitly state exit code 0"
    );

    Ok(())
}

#[test]
fn test_verify_continuity_check() -> Result<()> {
    let (bucket, endpoint) = test_bucket_config();
    let tempdir = TempDir::new()?;
    let db_path = tempdir.path().join("continuity-test.db");

    create_test_db(db_path.to_str().unwrap())?;

    // Create snapshot
    Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("snapshot")
        .arg(db_path.to_str().unwrap())
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    // Verify should check TXID continuity
    let output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("verify")
        .arg("continuity-test")
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should report on continuity
    assert!(
        stdout.contains("Continuity:"),
        "Should check TXID continuity"
    );
    assert!(
        stdout.contains("No gaps detected")
            || stdout.contains("Gaps detected")
            || stdout.contains("Snapshot only"),
        "Should report continuity status"
    );

    Ok(())
}

// ============================================================================
// UNIT TESTS - Can run without S3
// ============================================================================

#[test]
fn test_verify_requires_database_name() {
    // Verify command requires a database name argument
    let output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("verify")
        .arg("-b")
        .arg("test-bucket")
        .output()
        .expect("Failed to execute command");

    // Should fail without database name
    assert!(
        !output.status.success(),
        "Should fail without database name"
    );
}

#[test]
fn test_verify_requires_bucket() {
    // Verify command requires a bucket argument
    let output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("verify")
        .arg("test-db")
        .output()
        .expect("Failed to execute command");

    // Should fail without bucket
    assert!(!output.status.success(), "Should fail without bucket");
}

#[test]
fn test_verify_help_output() {
    let output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("verify")
        .arg("--help")
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "Help should succeed");
    assert!(stdout.contains("Verify"), "Help should mention verify");
    assert!(
        stdout.contains("integrity"),
        "Help should mention integrity"
    );
    assert!(
        stdout.contains("--bucket"),
        "Help should show --bucket flag"
    );
}

// ============================================================================
// REGRESSION TESTS - Specific bugs
// ============================================================================

#[test]
fn test_verify_doesnt_double_count_file_sizes() -> Result<()> {
    // Regression test for the bug where total_size was counted twice
    let (bucket, endpoint) = test_bucket_config();
    let tempdir = TempDir::new()?;
    let db_path = tempdir.path().join("size-test.db");

    create_test_db(db_path.to_str().unwrap())?;

    // Create large database to make size counting visible
    let conn = rusqlite::Connection::open(&db_path)?;
    for i in 0..1000 {
        conn.execute(
            "INSERT INTO test VALUES (?, ?)",
            rusqlite::params![100 + i, format!("large_data_string_{}", "x".repeat(100))],
        )?;
    }
    drop(conn);

    Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("snapshot")
        .arg(db_path.to_str().unwrap())
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    let output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("verify")
        .arg("size-test")
        .arg("-b")
        .arg(&bucket)
        .arg("--endpoint")
        .arg(&endpoint)
        .output()?;

    // Should succeed and not panic with overflow
    assert!(
        output.status.success(),
        "Should handle large files without overflow"
    );

    Ok(())
}
