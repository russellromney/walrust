//! Equivalence tests: verify hadb-io behaves identically to walrust-core's
//! retry, s3, and storage modules before migrating.
//!
//! These tests compare the behavior of walrust-core's local implementations
//! against hadb-io's implementations to ensure the migration is safe.

use anyhow::anyhow;

// ============================================================================
// Retry: Error Classification Equivalence
// ============================================================================

/// Test that classify_error produces identical results for all known error patterns.
#[test]
fn test_classify_error_equivalence() {
    let test_cases = vec![
        // Transient errors
        "500 Internal Server Error",
        "502 Bad Gateway",
        "503 Service Unavailable",
        "504 Gateway Timeout",
        "internal server error happened",
        "bad gateway response",
        "service unavailable right now",
        "gateway timeout occurred",
        // Network errors
        "Connection timeout",
        "connection timed out",
        "connection reset by peer",
        "network unreachable",
        "socket error occurred",
        "reset by remote",
        "broken pipe detected",
        "unexpected eof",
        "temporarily unavailable",
        // Throttling
        "throttling exception",
        "SlowDown: reduce your request rate",
        "request rate exceeded",
        // Injected test errors
        "Service unavailable (injected)",
        "Storage error: Service unavailable (injected)",
        // Client errors
        "400 Bad Request",
        "bad request format",
        // Auth errors
        "401 Unauthorized",
        "403 Forbidden",
        "Access Denied",
        "unauthorized access",
        "forbidden resource",
        "invalid credentials provided",
        "expired token error",
        // Not found
        "404 Not Found",
        "not found in bucket",
        "No such key",
        // Unknown
        "some random error",
        "weird stuff happened",
    ];

    for case in &test_cases {
        let error = anyhow!("{}", case);
        let walrust_kind = walrust::retry::classify_error(&error);
        let hadb_kind = hadb_io::classify_error(&error);

        // Map to comparable values (both use same enum variant names)
        let walrust_str = format!("{:?}", walrust_kind);
        let hadb_str = format!("{:?}", hadb_kind);

        assert_eq!(
            walrust_str, hadb_str,
            "classify_error mismatch for '{}': walrust={}, hadb-io={}",
            case, walrust_str, hadb_str
        );
    }
}

/// Test is_retryable produces identical results.
#[test]
fn test_is_retryable_equivalence() {
    let test_cases = vec![
        "500 Internal Server Error",
        "Connection timeout",
        "Service unavailable (injected)",
        "some unknown error",
        "401 Unauthorized",
        "403 Forbidden",
        "400 Bad Request",
        "404 Not Found",
        "No such key",
        "throttling exception",
        "broken pipe",
    ];

    for case in &test_cases {
        let error = anyhow!("{}", case);
        let walrust_result = walrust::retry::is_retryable(&error);
        let hadb_result = hadb_io::is_retryable(&error);

        assert_eq!(
            walrust_result, hadb_result,
            "is_retryable mismatch for '{}': walrust={}, hadb-io={}",
            case, walrust_result, hadb_result
        );
    }
}

// ============================================================================
// Retry: RetryConfig Defaults Equivalence
// ============================================================================

#[test]
fn test_retry_config_defaults_equivalence() {
    let walrust_config = walrust::RetryConfig::default();
    let hadb_config = hadb_io::RetryConfig::default();

    assert_eq!(walrust_config.max_retries, hadb_config.max_retries);
    assert_eq!(walrust_config.base_delay_ms, hadb_config.base_delay_ms);
    assert_eq!(walrust_config.max_delay_ms, hadb_config.max_delay_ms);
    assert_eq!(
        walrust_config.circuit_breaker_enabled,
        hadb_config.circuit_breaker_enabled
    );
    assert_eq!(
        walrust_config.circuit_breaker_threshold,
        hadb_config.circuit_breaker_threshold
    );
    assert_eq!(
        walrust_config.circuit_breaker_cooldown_ms,
        hadb_config.circuit_breaker_cooldown_ms
    );
}

/// Verify serde deserialization produces equivalent configs.
#[test]
fn test_retry_config_serde_equivalence() {
    let json_cases = vec![
        r#"{}"#,
        r#"{"max_retries": 3}"#,
        r#"{"max_retries": 10, "base_delay_ms": 200, "max_delay_ms": 60000}"#,
        r#"{"circuit_breaker_enabled": false}"#,
        r#"{"circuit_breaker_threshold": 5, "circuit_breaker_cooldown_ms": 30000}"#,
    ];

    for json in &json_cases {
        let walrust_config: walrust::RetryConfig = serde_json::from_str(json).unwrap();
        let hadb_config: hadb_io::RetryConfig = serde_json::from_str(json).unwrap();

        assert_eq!(
            walrust_config.max_retries, hadb_config.max_retries,
            "max_retries mismatch for json: {}",
            json
        );
        assert_eq!(
            walrust_config.base_delay_ms, hadb_config.base_delay_ms,
            "base_delay_ms mismatch for json: {}",
            json
        );
        assert_eq!(
            walrust_config.max_delay_ms, hadb_config.max_delay_ms,
            "max_delay_ms mismatch for json: {}",
            json
        );
        assert_eq!(
            walrust_config.circuit_breaker_enabled, hadb_config.circuit_breaker_enabled,
            "circuit_breaker_enabled mismatch for json: {}",
            json
        );
        assert_eq!(
            walrust_config.circuit_breaker_threshold, hadb_config.circuit_breaker_threshold,
            "circuit_breaker_threshold mismatch for json: {}",
            json
        );
        assert_eq!(
            walrust_config.circuit_breaker_cooldown_ms, hadb_config.circuit_breaker_cooldown_ms,
            "circuit_breaker_cooldown_ms mismatch for json: {}",
            json
        );
    }
}

// ============================================================================
// Retry: CircuitBreaker Equivalence
// ============================================================================

#[test]
fn test_circuit_breaker_state_transitions_equivalence() {
    let walrust_cb = walrust::retry::CircuitBreaker::new(3, 100);
    let hadb_cb = hadb_io::CircuitBreaker::new(3, 100);

    // Initial state
    assert_eq!(
        format!("{:?}", walrust_cb.state()),
        format!("{:?}", hadb_cb.state()),
        "Initial state mismatch"
    );
    assert_eq!(walrust_cb.should_allow(), hadb_cb.should_allow());

    // After 2 failures — still closed
    walrust_cb.record_failure();
    walrust_cb.record_failure();
    hadb_cb.record_failure();
    hadb_cb.record_failure();

    assert_eq!(
        format!("{:?}", walrust_cb.state()),
        format!("{:?}", hadb_cb.state()),
        "After 2 failures state mismatch"
    );
    assert_eq!(walrust_cb.should_allow(), hadb_cb.should_allow());

    // 3rd failure — opens
    walrust_cb.record_failure();
    hadb_cb.record_failure();

    assert_eq!(
        format!("{:?}", walrust_cb.state()),
        format!("{:?}", hadb_cb.state()),
        "After 3 failures state mismatch"
    );
    assert_eq!(walrust_cb.should_allow(), hadb_cb.should_allow());
    assert!(!walrust_cb.should_allow());

    // Wait for cooldown
    std::thread::sleep(std::time::Duration::from_millis(150));

    assert_eq!(
        format!("{:?}", walrust_cb.state()),
        format!("{:?}", hadb_cb.state()),
        "After cooldown state mismatch"
    );
    assert_eq!(walrust_cb.should_allow(), hadb_cb.should_allow());
    assert!(walrust_cb.should_allow()); // HalfOpen

    // Success resets
    walrust_cb.record_success();
    hadb_cb.record_success();

    assert_eq!(
        format!("{:?}", walrust_cb.state()),
        format!("{:?}", hadb_cb.state()),
        "After success state mismatch"
    );
    assert_eq!(walrust_cb.should_allow(), hadb_cb.should_allow());
    assert!(walrust_cb.should_allow()); // Closed
}

// ============================================================================
// Retry: RetryPolicy Execute Equivalence
// ============================================================================

#[tokio::test]
async fn test_retry_policy_success_equivalence() {
    let walrust_policy = walrust::RetryPolicy::default_policy();
    let hadb_policy = hadb_io::RetryPolicy::default_policy();

    let walrust_result: anyhow::Result<i32> = walrust_policy.execute(|| async { Ok(42) }).await;
    let hadb_result: anyhow::Result<i32> = hadb_policy.execute(|| async { Ok(42) }).await;

    assert_eq!(walrust_result.unwrap(), hadb_result.unwrap());
}

#[tokio::test]
async fn test_retry_policy_auth_error_no_retry_equivalence() {
    let walrust_policy = walrust::RetryPolicy::new(walrust::RetryConfig {
        max_retries: 5,
        base_delay_ms: 10,
        ..Default::default()
    });
    let hadb_policy = hadb_io::RetryPolicy::new(hadb_io::RetryConfig {
        max_retries: 5,
        base_delay_ms: 10,
        ..Default::default()
    });

    let walrust_attempts = std::sync::atomic::AtomicU32::new(0);
    let hadb_attempts = std::sync::atomic::AtomicU32::new(0);

    let walrust_result: anyhow::Result<i32> = walrust_policy
        .execute(|| {
            walrust_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async { Err(anyhow!("401 Unauthorized")) }
        })
        .await;

    let hadb_result: anyhow::Result<i32> = hadb_policy
        .execute(|| {
            hadb_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async { Err(anyhow!("401 Unauthorized")) }
        })
        .await;

    assert!(walrust_result.is_err());
    assert!(hadb_result.is_err());
    assert_eq!(
        walrust_attempts.load(std::sync::atomic::Ordering::Relaxed),
        hadb_attempts.load(std::sync::atomic::Ordering::Relaxed),
        "Attempt count mismatch for auth error"
    );
    assert_eq!(
        walrust_attempts.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "Auth errors should not be retried"
    );
}

#[tokio::test]
async fn test_retry_policy_exhausted_equivalence() {
    let walrust_policy = walrust::RetryPolicy::new(walrust::RetryConfig {
        max_retries: 2,
        base_delay_ms: 1, // fast for tests
        circuit_breaker_enabled: false,
        ..Default::default()
    });
    let hadb_policy = hadb_io::RetryPolicy::new(hadb_io::RetryConfig {
        max_retries: 2,
        base_delay_ms: 1,
        circuit_breaker_enabled: false,
        ..Default::default()
    });

    let walrust_attempts = std::sync::atomic::AtomicU32::new(0);
    let hadb_attempts = std::sync::atomic::AtomicU32::new(0);

    let walrust_result: anyhow::Result<i32> = walrust_policy
        .execute(|| {
            walrust_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async { Err(anyhow!("Service unavailable (injected)")) }
        })
        .await;

    let hadb_result: anyhow::Result<i32> = hadb_policy
        .execute(|| {
            hadb_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async { Err(anyhow!("Service unavailable (injected)")) }
        })
        .await;

    assert!(walrust_result.is_err());
    assert!(hadb_result.is_err());

    let w_count = walrust_attempts.load(std::sync::atomic::Ordering::Relaxed);
    let h_count = hadb_attempts.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(w_count, h_count, "Attempt count mismatch: walrust={}, hadb-io={}", w_count, h_count);
    assert_eq!(w_count, 3, "Should be initial + 2 retries = 3");
}

#[tokio::test]
async fn test_retry_policy_transient_then_success_equivalence() {
    let walrust_policy = walrust::RetryPolicy::new(walrust::RetryConfig {
        max_retries: 3,
        base_delay_ms: 1,
        ..Default::default()
    });
    let hadb_policy = hadb_io::RetryPolicy::new(hadb_io::RetryConfig {
        max_retries: 3,
        base_delay_ms: 1,
        ..Default::default()
    });

    let walrust_attempts = std::sync::atomic::AtomicU32::new(0);
    let hadb_attempts = std::sync::atomic::AtomicU32::new(0);

    let walrust_result: anyhow::Result<i32> = walrust_policy
        .execute(|| {
            let attempt =
                walrust_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                if attempt < 2 {
                    Err(anyhow!("Service unavailable (injected)"))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

    let hadb_result: anyhow::Result<i32> = hadb_policy
        .execute(|| {
            let attempt =
                hadb_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            async move {
                if attempt < 2 {
                    Err(anyhow!("Service unavailable (injected)"))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

    assert_eq!(walrust_result.unwrap(), hadb_result.unwrap());
    assert_eq!(
        walrust_attempts.load(std::sync::atomic::Ordering::Relaxed),
        hadb_attempts.load(std::sync::atomic::Ordering::Relaxed),
    );
}

#[tokio::test]
async fn test_retry_policy_circuit_breaker_blocks_equivalence() {
    let walrust_policy = walrust::RetryPolicy::new(walrust::RetryConfig {
        max_retries: 1,
        base_delay_ms: 1,
        circuit_breaker_enabled: true,
        circuit_breaker_threshold: 2,
        circuit_breaker_cooldown_ms: 60_000,
        ..Default::default()
    });
    let hadb_policy = hadb_io::RetryPolicy::new(hadb_io::RetryConfig {
        max_retries: 1,
        base_delay_ms: 1,
        circuit_breaker_enabled: true,
        circuit_breaker_threshold: 2,
        circuit_breaker_cooldown_ms: 60_000,
        ..Default::default()
    });

    // Trip both circuit breakers
    let walrust_cb = walrust_policy.circuit_breaker().unwrap();
    walrust_cb.record_failure();
    walrust_cb.record_failure();

    let hadb_cb = hadb_policy.circuit_breaker().unwrap();
    hadb_cb.record_failure();
    hadb_cb.record_failure();

    let walrust_result: anyhow::Result<i32> = walrust_policy.execute(|| async { Ok(42) }).await;
    let hadb_result: anyhow::Result<i32> = hadb_policy.execute(|| async { Ok(42) }).await;

    assert!(walrust_result.is_err());
    assert!(hadb_result.is_err());

    // Both should mention circuit breaker
    assert!(walrust_result.unwrap_err().to_string().contains("Circuit breaker"));
    assert!(hadb_result.unwrap_err().to_string().contains("Circuit breaker"));
}

#[tokio::test]
async fn test_execute_with_context_equivalence() {
    let walrust_policy = walrust::RetryPolicy::new(walrust::RetryConfig {
        max_retries: 0,
        circuit_breaker_enabled: false,
        ..Default::default()
    });
    let hadb_policy = hadb_io::RetryPolicy::new(hadb_io::RetryConfig {
        max_retries: 0,
        circuit_breaker_enabled: false,
        ..Default::default()
    });

    let walrust_result: anyhow::Result<i32> = walrust_policy
        .execute_with_context("upload segment", || async {
            Err(anyhow!("Service unavailable (injected)"))
        })
        .await;

    let hadb_result: anyhow::Result<i32> = hadb_policy
        .execute_with_context("upload segment", || async {
            Err(anyhow!("Service unavailable (injected)"))
        })
        .await;

    assert!(walrust_result.is_err());
    assert!(hadb_result.is_err());

    let w_err = walrust_result.unwrap_err().to_string();
    let h_err = hadb_result.unwrap_err().to_string();
    assert!(w_err.contains("upload segment"), "walrust missing context: {}", w_err);
    assert!(h_err.contains("upload segment"), "hadb-io missing context: {}", h_err);
    assert!(w_err.contains("Service unavailable"), "walrust missing error: {}", w_err);
    assert!(h_err.contains("Service unavailable"), "hadb-io missing error: {}", h_err);
}

// ============================================================================
// S3: parse_bucket Equivalence
// ============================================================================

#[test]
fn test_parse_bucket_equivalence() {
    let test_cases = vec![
        "my-bucket",
        "my-bucket/some/prefix",
        "s3://my-bucket/backups",
        "s3://my-bucket",
        "my-bucket/",
        "s3://my-bucket/prefix/",
        "s3://bucket/a/b/c",
        "bucket-name",
    ];

    for case in &test_cases {
        let (walrust_bucket, walrust_prefix) = walrust::s3::parse_bucket(case);
        let (hadb_bucket, hadb_prefix) = hadb_io::s3::parse_bucket(case);

        assert_eq!(
            walrust_bucket, hadb_bucket,
            "Bucket mismatch for '{}': walrust='{}', hadb-io='{}'",
            case, walrust_bucket, hadb_bucket
        );
        assert_eq!(
            walrust_prefix, hadb_prefix,
            "Prefix mismatch for '{}': walrust='{}', hadb-io='{}'",
            case, walrust_prefix, hadb_prefix
        );
    }
}

// ============================================================================
// Storage: Trait Method Equivalence (compile-time)
// ============================================================================

/// Compile-time verification that hadb_io::ObjectStore has the same methods
/// as walrust::StorageBackend. If this compiles, the trait surfaces match.
#[allow(dead_code)]
async fn _verify_trait_surface_walrust(backend: &dyn walrust::StorageBackend) {
    let _ = backend.upload_bytes("key", vec![]).await;
    let _ = backend.upload_bytes_with_checksum("key", vec![], "sha").await;
    let _ = backend.upload_file("key", std::path::Path::new("/tmp/f")).await;
    let _ = backend.upload_file_with_checksum("key", std::path::Path::new("/tmp/f"), "sha").await;
    let _ = backend.download_bytes("key").await;
    let _ = backend.download_file("key", std::path::Path::new("/tmp/f")).await;
    let _ = backend.list_objects("prefix").await;
    let _ = backend.list_objects_after("prefix", "after").await;
    let _ = backend.exists("key").await;
    let _ = backend.get_checksum("key").await;
    let _ = backend.delete_object("key").await;
    let _ = backend.delete_objects(&["k".to_string()]).await;
    let _ = backend.bucket_name();
}

#[allow(dead_code)]
async fn _verify_trait_surface_hadb_io(store: &dyn hadb_io::ObjectStore) {
    let _ = store.upload_bytes("key", vec![]).await;
    let _ = store.upload_bytes_with_checksum("key", vec![], "sha").await;
    let _ = store.upload_file("key", std::path::Path::new("/tmp/f")).await;
    let _ = store.upload_file_with_checksum("key", std::path::Path::new("/tmp/f"), "sha").await;
    let _ = store.download_bytes("key").await;
    let _ = store.download_file("key", std::path::Path::new("/tmp/f")).await;
    let _ = store.list_objects("prefix").await;
    let _ = store.list_objects_after("prefix", "after").await;
    let _ = store.exists("key").await;
    let _ = store.get_checksum("key").await;
    let _ = store.delete_object("key").await;
    let _ = store.delete_objects(&["k".to_string()]).await;
    let _ = store.bucket_name();
}

// ============================================================================
// Backoff Calculation: Statistical Equivalence
// ============================================================================

/// Verify both policies produce delays within the same bounds.
/// We can't test exact values (jittered) but we can verify bounds match.
#[test]
fn test_backoff_bounds_equivalence() {
    let walrust_policy = walrust::RetryPolicy::new(walrust::RetryConfig {
        base_delay_ms: 100,
        max_delay_ms: 30_000,
        ..Default::default()
    });
    let hadb_policy = hadb_io::RetryPolicy::new(hadb_io::RetryConfig {
        base_delay_ms: 100,
        max_delay_ms: 30_000,
        ..Default::default()
    });

    // Attempt 0: max = min(30000, 100 * 2^0) = 100ms
    for _ in 0..50 {
        let w = walrust_policy.calculate_delay(0);
        let h = hadb_policy.calculate_delay(0);
        assert!(w <= std::time::Duration::from_millis(100));
        assert!(h <= std::time::Duration::from_millis(100));
    }

    // Attempt 1: max = min(30000, 100 * 2^1) = 200ms
    for _ in 0..50 {
        let w = walrust_policy.calculate_delay(1);
        let h = hadb_policy.calculate_delay(1);
        assert!(w <= std::time::Duration::from_millis(200));
        assert!(h <= std::time::Duration::from_millis(200));
    }

    // Attempt 5: max = min(30000, 100 * 2^5) = 3200ms
    for _ in 0..50 {
        let w = walrust_policy.calculate_delay(5);
        let h = hadb_policy.calculate_delay(5);
        assert!(w <= std::time::Duration::from_millis(3200));
        assert!(h <= std::time::Duration::from_millis(3200));
    }

    // Attempt 20: capped at 30000ms
    for _ in 0..50 {
        let w = walrust_policy.calculate_delay(20);
        let h = hadb_policy.calculate_delay(20);
        assert!(w <= std::time::Duration::from_millis(30_000));
        assert!(h <= std::time::Duration::from_millis(30_000));
    }
}

// ============================================================================
// hadb-io extras: verify new features don't break compatibility
// ============================================================================

/// hadb-io adds "dispatch failure" classification (from graphstream).
/// walrust-core classifies this as Unknown. Verify hadb-io classifies as Transient.
/// This is an ADDITIVE improvement, not a behavioral change for existing patterns.
#[test]
fn test_hadb_io_dispatch_failure_is_additive() {
    let error = anyhow!("dispatch failure");
    let walrust_kind = format!("{:?}", walrust::retry::classify_error(&error));
    let hadb_kind = format!("{:?}", hadb_io::classify_error(&error));

    // walrust classifies as Unknown, hadb-io as Transient
    // Both are retryable (Unknown is retried with caution)
    assert_eq!(walrust_kind, "Unknown");
    assert_eq!(hadb_kind, "Transient");

    // Critically, both are retryable
    assert!(walrust::retry::is_retryable(&error));
    assert!(hadb_io::is_retryable(&error));
}

/// hadb-io's OnCircuitOpen callback is additive — default CircuitBreaker
/// (without callback) behaves identically to walrust-core's.
#[test]
fn test_hadb_io_on_circuit_open_is_additive() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let called = Arc::new(AtomicU32::new(0));
    let called_clone = called.clone();

    let cb_with_callback =
        hadb_io::CircuitBreaker::with_on_open(2, 60_000, Arc::new(move |failures| {
            called_clone.store(failures, Ordering::Relaxed);
        }));

    let cb_without = hadb_io::CircuitBreaker::new(2, 60_000);
    let walrust_cb = walrust::retry::CircuitBreaker::new(2, 60_000);

    // All three should behave the same for state transitions
    cb_with_callback.record_failure();
    cb_without.record_failure();
    walrust_cb.record_failure();

    assert_eq!(called.load(Ordering::Relaxed), 0); // Not yet at threshold

    cb_with_callback.record_failure();
    cb_without.record_failure();
    walrust_cb.record_failure();

    assert_eq!(called.load(Ordering::Relaxed), 2); // Callback fired

    // All three should be in Open state
    assert!(!cb_with_callback.should_allow());
    assert!(!cb_without.should_allow());
    assert!(!walrust_cb.should_allow());
}
