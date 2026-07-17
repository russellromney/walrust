// Comprehensive tests for `walrust verify` command
//
// Tests cover: positive cases, negative cases, edge cases, and integration tests
// Requires S3/Tigris credentials. The workspace `make test` target injects
// them via Soup.
use anyhow::Result;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

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

/// S3-backed tests run only when S3 credentials/an endpoint are configured.
/// CI provisions MinIO and sets AWS_* env; local dev injects Tigris creds via
/// Soup. On a clean machine with no S3 configured these tests skip so that a
/// plain `cargo test --workspace` stays green (Phase 0.5).
fn s3_test_enabled() -> bool {
    std::env::var("AWS_ENDPOINT_URL_S3").is_ok()
        || std::env::var("AWS_ENDPOINT_URL").is_ok()
        || std::env::var("AWS_ACCESS_KEY_ID").is_ok()
}

#[test]
fn test_verify_no_backup_found() -> Result<()> {
    if !s3_test_enabled() {
        eprintln!("SKIP test_verify_no_backup_found: no S3 endpoint/credentials configured");
        return Ok(());
    }
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
        "verify must fail closed when no native recovery stream exists"
    );
    assert_eq!(
        verify_output.status.code(),
        Some(5),
        "missing backups are integrity failures, not generic errors; stdout={stdout}; stderr={stderr}"
    );
    assert!(
        stdout.contains("no contiguous native-v1 HADBP recovery stream")
            || stderr.contains("no contiguous native-v1 HADBP recovery stream"),
        "Should report no contiguous native stream"
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
