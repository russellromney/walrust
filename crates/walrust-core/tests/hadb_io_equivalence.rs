//! Re-export validation tests: verify walrust-core's public API re-exports
//! from hadb-io work correctly after the migration.
//!
//! These tests ensure downstream consumers (walrust CLI, haqlite) can use
//! walrust-core's re-exported types without importing hadb-io directly.

use anyhow::anyhow;

// ============================================================================
// Re-export: Retry types accessible via walrust::
// ============================================================================

#[test]
fn test_reexport_retry_config() {
    let config = walrust::RetryConfig::default();
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.base_delay_ms, 100);
    assert_eq!(config.max_delay_ms, 30_000);
    assert!(config.circuit_breaker_enabled);
}

#[test]
fn test_reexport_retry_policy() {
    let policy = walrust::RetryPolicy::default_policy();
    assert!(policy.circuit_breaker().is_some());
    let delay = policy.calculate_delay(0);
    assert!(delay <= std::time::Duration::from_millis(100));
}

#[test]
fn test_reexport_classify_error() {
    assert_eq!(
        format!("{:?}", walrust::classify_error(&anyhow!("500 Internal Server Error"))),
        "Transient"
    );
    assert_eq!(
        format!("{:?}", walrust::classify_error(&anyhow!("401 Unauthorized"))),
        "AuthError"
    );
    assert_eq!(
        format!("{:?}", walrust::classify_error(&anyhow!("404 Not Found"))),
        "NotFound"
    );
}

#[test]
fn test_reexport_is_retryable() {
    assert!(walrust::is_retryable(&anyhow!("500 Internal Server Error")));
    assert!(!walrust::is_retryable(&anyhow!("401 Unauthorized")));
}

#[test]
fn test_reexport_circuit_breaker() {
    let cb = walrust::CircuitBreaker::new(3, 100);
    assert_eq!(format!("{:?}", cb.state()), "Closed");
    assert!(cb.should_allow());
    assert_eq!(cb.consecutive_failures(), 0);

    cb.record_failure();
    cb.record_failure();
    cb.record_failure();
    assert_eq!(format!("{:?}", cb.state()), "Open");
    assert!(!cb.should_allow());
}

// ============================================================================
// Re-export: S3 module accessible via walrust::s3::
// ============================================================================

#[test]
fn test_reexport_s3_parse_bucket() {
    let (bucket, prefix) = walrust::s3::parse_bucket("s3://my-bucket/backups");
    assert_eq!(bucket, "my-bucket");
    assert_eq!(prefix, "backups/");
}

#[test]
fn test_reexport_s3_parse_bucket_simple() {
    let (bucket, prefix) = walrust::s3::parse_bucket("my-bucket");
    assert_eq!(bucket, "my-bucket");
    assert_eq!(prefix, "");
}

// ============================================================================
// Re-export: StorageBackend alias works
// ============================================================================

/// Compile-time check that walrust::StorageBackend is usable as a trait object.
#[allow(dead_code)]
fn _assert_storage_backend_usable(_: &dyn walrust::StorageBackend) {}

/// Compile-time check that the canonical S3 impl implements the trait
/// walrust re-exports. Before Phase Anvil f walrust bundled its own
/// `S3Backend`; now consumers wire in `hadb_storage_s3::S3Storage`.
#[allow(dead_code)]
fn _assert_s3_storage_usable<T: walrust::StorageBackend>() {}

#[allow(dead_code)]
fn _check_s3_storage_type() {
    _assert_s3_storage_usable::<hadb_storage_s3::S3Storage>();
}

// ============================================================================
// Re-export: RetryPolicy execute works end-to-end
// ============================================================================

#[tokio::test]
async fn test_reexport_retry_execute_success() {
    let policy = walrust::RetryPolicy::default_policy();
    let result: anyhow::Result<i32> = policy.execute(|| async { Ok(42) }).await;
    assert_eq!(result.unwrap(), 42);
}

#[tokio::test]
async fn test_reexport_retry_execute_auth_no_retry() {
    let policy = walrust::RetryPolicy::new(walrust::RetryConfig {
        max_retries: 5,
        base_delay_ms: 1,
        ..Default::default()
    });

    let attempts = std::sync::atomic::AtomicU32::new(0);
    let result: anyhow::Result<i32> = policy
        .execute(|| {
            attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async { Err(anyhow!("401 Unauthorized")) }
        })
        .await;

    assert!(result.is_err());
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[tokio::test]
async fn test_reexport_retry_execute_with_context() {
    let policy = walrust::RetryPolicy::new(walrust::RetryConfig {
        max_retries: 0,
        circuit_breaker_enabled: false,
        ..Default::default()
    });

    let result: anyhow::Result<i32> = policy
        .execute_with_context("upload segment", || async {
            Err(anyhow!("Service unavailable (injected)"))
        })
        .await;

    let err = result.unwrap_err().to_string();
    assert!(err.contains("upload segment"));
    assert!(err.contains("Service unavailable"));
}
