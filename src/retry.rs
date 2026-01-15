//! Retry logic with exponential backoff for walrust storage operations
//!
//! Implements retry policies for transient S3/storage errors with:
//! - Exponential backoff with full jitter
//! - Error classification (retryable vs non-retryable)
//! - Circuit breaker pattern
//!
//! Based on AWS best practices:
//! https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

use anyhow::{anyhow, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for retry behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (default: 5)
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Initial backoff delay in milliseconds (default: 100)
    #[serde(default = "default_base_delay_ms")]
    pub base_delay_ms: u64,

    /// Maximum backoff delay in milliseconds (default: 30000 = 30s)
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,

    /// Enable circuit breaker (default: true)
    #[serde(default = "default_circuit_breaker_enabled")]
    pub circuit_breaker_enabled: bool,

    /// Number of consecutive failures before circuit opens (default: 10)
    #[serde(default = "default_circuit_breaker_threshold")]
    pub circuit_breaker_threshold: u32,

    /// Time to wait before attempting half-open state (milliseconds, default: 60000 = 1min)
    #[serde(default = "default_circuit_breaker_cooldown_ms")]
    pub circuit_breaker_cooldown_ms: u64,
}

fn default_max_retries() -> u32 {
    5
}
fn default_base_delay_ms() -> u64 {
    100
}
fn default_max_delay_ms() -> u64 {
    30_000
}
fn default_circuit_breaker_enabled() -> bool {
    true
}
fn default_circuit_breaker_threshold() -> u32 {
    10
}
fn default_circuit_breaker_cooldown_ms() -> u64 {
    60_000
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            base_delay_ms: default_base_delay_ms(),
            max_delay_ms: default_max_delay_ms(),
            circuit_breaker_enabled: default_circuit_breaker_enabled(),
            circuit_breaker_threshold: default_circuit_breaker_threshold(),
            circuit_breaker_cooldown_ms: default_circuit_breaker_cooldown_ms(),
        }
    }
}

/// Error classification for retry decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Transient error - should retry (500, 502, 503, 504, timeouts, network)
    Transient,
    /// Client error - don't retry, it's a bug (400)
    ClientError,
    /// Authentication error - don't retry without user intervention (401, 403)
    AuthError,
    /// Not found - context dependent (404)
    NotFound,
    /// Unknown error - may retry with caution
    Unknown,
}

/// Classify an error to determine retry behavior
pub fn classify_error(error: &anyhow::Error) -> ErrorKind {
    let error_str = error.to_string().to_lowercase();

    // Check for specific HTTP status codes in error message
    if error_str.contains("500")
        || error_str.contains("502")
        || error_str.contains("503")
        || error_str.contains("504")
        || error_str.contains("internal server error")
        || error_str.contains("bad gateway")
        || error_str.contains("service unavailable")
        || error_str.contains("gateway timeout")
    {
        return ErrorKind::Transient;
    }

    // Network and timeout errors
    if error_str.contains("timeout")
        || error_str.contains("timed out")
        || error_str.contains("connection")
        || error_str.contains("network")
        || error_str.contains("socket")
        || error_str.contains("reset")
        || error_str.contains("broken pipe")
        || error_str.contains("eof")
        || error_str.contains("temporarily unavailable")
    {
        return ErrorKind::Transient;
    }

    // AWS SDK specific transient errors
    if error_str.contains("throttl")
        || error_str.contains("slowdown")
        || error_str.contains("reduce your request rate")
        || error_str.contains("request rate exceeded")
    {
        return ErrorKind::Transient;
    }

    // Injected test errors (from MockStorageBackend)
    if error_str.contains("service unavailable (injected)") {
        return ErrorKind::Transient;
    }

    // Client errors - don't retry
    if error_str.contains("400") || error_str.contains("bad request") {
        return ErrorKind::ClientError;
    }

    // Auth errors - don't retry
    if error_str.contains("401")
        || error_str.contains("403")
        || error_str.contains("unauthorized")
        || error_str.contains("forbidden")
        || error_str.contains("access denied")
        || error_str.contains("invalid credentials")
        || error_str.contains("expired token")
    {
        return ErrorKind::AuthError;
    }

    // Not found
    if error_str.contains("404") || error_str.contains("not found") || error_str.contains("no such key") {
        return ErrorKind::NotFound;
    }

    ErrorKind::Unknown
}

/// Check if an error is retryable
pub fn is_retryable(error: &anyhow::Error) -> bool {
    matches!(classify_error(error), ErrorKind::Transient | ErrorKind::Unknown)
}

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation
    Closed,
    /// Failing - rejecting requests
    Open,
    /// Testing if service recovered
    HalfOpen,
}

/// Circuit breaker for preventing cascading failures
#[derive(Debug)]
pub struct CircuitBreaker {
    /// Number of consecutive failures
    consecutive_failures: AtomicU32,
    /// Threshold before opening
    threshold: u32,
    /// Time when circuit opened (milliseconds since UNIX epoch, 0 = not open)
    opened_at_ms: AtomicU64,
    /// Cooldown period in milliseconds
    cooldown_ms: u64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(threshold: u32, cooldown_ms: u64) -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            threshold,
            opened_at_ms: AtomicU64::new(0),
            cooldown_ms,
        }
    }

    /// Get current circuit state
    pub fn state(&self) -> CircuitState {
        let failures = self.consecutive_failures.load(Ordering::Relaxed);
        let opened_at = self.opened_at_ms.load(Ordering::Relaxed);

        if failures < self.threshold {
            return CircuitState::Closed;
        }

        if opened_at == 0 {
            return CircuitState::Closed;
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if now_ms - opened_at >= self.cooldown_ms {
            CircuitState::HalfOpen
        } else {
            CircuitState::Open
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.opened_at_ms.store(0, Ordering::Relaxed);
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;

        if failures >= self.threshold && self.opened_at_ms.load(Ordering::Relaxed) == 0 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            self.opened_at_ms.store(now_ms, Ordering::Relaxed);
            tracing::warn!(
                "Circuit breaker opened after {} consecutive failures",
                failures
            );
        }
    }

    /// Check if request should be allowed
    pub fn should_allow(&self) -> bool {
        match self.state() {
            CircuitState::Closed => true,
            CircuitState::HalfOpen => true, // Allow one test request
            CircuitState::Open => false,
        }
    }
}

/// Retry policy with exponential backoff
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    config: RetryConfig,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
}

impl RetryPolicy {
    /// Create a new retry policy
    pub fn new(config: RetryConfig) -> Self {
        let circuit_breaker = if config.circuit_breaker_enabled {
            Some(Arc::new(CircuitBreaker::new(
                config.circuit_breaker_threshold,
                config.circuit_breaker_cooldown_ms,
            )))
        } else {
            None
        };

        Self {
            config,
            circuit_breaker,
        }
    }

    /// Create with default config
    pub fn default_policy() -> Self {
        Self::new(RetryConfig::default())
    }

    /// Get the config (for inspection)
    pub fn config(&self) -> &RetryConfig {
        &self.config
    }

    /// Get the circuit breaker (for testing/inspection)
    pub fn circuit_breaker(&self) -> Option<&Arc<CircuitBreaker>> {
        self.circuit_breaker.as_ref()
    }

    /// Calculate backoff delay for a given attempt
    ///
    /// Uses full jitter: sleep = random(0, min(cap, base * 2^attempt))
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let base = self.config.base_delay_ms;
        let cap = self.config.max_delay_ms;

        // Calculate exponential delay: base * 2^attempt
        let exp_delay = base.saturating_mul(1u64 << attempt.min(20)); // Prevent overflow
        let capped_delay = exp_delay.min(cap);

        // Full jitter: random delay between 0 and capped_delay
        let jittered = if capped_delay > 0 {
            rand::thread_rng().gen_range(0..=capped_delay)
        } else {
            0
        };

        Duration::from_millis(jittered)
    }

    /// Execute an async operation with retry
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check circuit breaker
        if let Some(cb) = &self.circuit_breaker {
            if !cb.should_allow() {
                return Err(anyhow!(
                    "Circuit breaker open - refusing request after {} consecutive failures",
                    self.config.circuit_breaker_threshold
                ));
            }
        }

        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 0..=self.config.max_retries {
            match operation().await {
                Ok(result) => {
                    if let Some(cb) = &self.circuit_breaker {
                        cb.record_success();
                    }
                    return Ok(result);
                }
                Err(e) => {
                    let error_kind = classify_error(&e);
                    let is_retryable = matches!(error_kind, ErrorKind::Transient | ErrorKind::Unknown);

                    // Record failure for circuit breaker
                    if let Some(cb) = &self.circuit_breaker {
                        cb.record_failure();
                    }

                    // Don't retry non-retryable errors
                    if !is_retryable {
                        tracing::warn!(
                            "Non-retryable error (kind={:?}): {}",
                            error_kind,
                            e
                        );
                        return Err(e);
                    }

                    // Check if we have retries left
                    if attempt < self.config.max_retries {
                        let delay = self.calculate_delay(attempt);
                        tracing::debug!(
                            "Attempt {}/{} failed (kind={:?}), retrying in {:?}: {}",
                            attempt + 1,
                            self.config.max_retries + 1,
                            error_kind,
                            delay,
                            e
                        );
                        tokio::time::sleep(delay).await;
                    }

                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Retry failed with no error recorded")))
    }

    /// Execute with context for better error messages
    pub async fn execute_with_context<F, Fut, T>(&self, context: &str, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.execute(operation)
            .await
            .map_err(|e| anyhow!("{}: {}", context, e))
    }
}

/// Outcome of a retry attempt (for webhooks/logging)
#[derive(Debug, Clone)]
pub struct RetryOutcome {
    /// Operation name
    pub operation: String,
    /// Whether it ultimately succeeded
    pub success: bool,
    /// Number of attempts made
    pub attempts: u32,
    /// Total time spent retrying
    pub total_duration: Duration,
    /// Final error if failed
    pub error: Option<String>,
    /// Error classification if failed
    pub error_kind: Option<ErrorKind>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_classification() {
        // Transient errors
        assert_eq!(
            classify_error(&anyhow!("500 Internal Server Error")),
            ErrorKind::Transient
        );
        assert_eq!(
            classify_error(&anyhow!("503 Service Unavailable")),
            ErrorKind::Transient
        );
        assert_eq!(
            classify_error(&anyhow!("Connection timeout")),
            ErrorKind::Transient
        );
        assert_eq!(
            classify_error(&anyhow!("Storage error: Service unavailable (injected)")),
            ErrorKind::Transient
        );

        // Auth errors
        assert_eq!(
            classify_error(&anyhow!("401 Unauthorized")),
            ErrorKind::AuthError
        );
        assert_eq!(
            classify_error(&anyhow!("403 Forbidden")),
            ErrorKind::AuthError
        );
        assert_eq!(
            classify_error(&anyhow!("Access Denied")),
            ErrorKind::AuthError
        );

        // Client errors
        assert_eq!(
            classify_error(&anyhow!("400 Bad Request")),
            ErrorKind::ClientError
        );

        // Not found
        assert_eq!(
            classify_error(&anyhow!("404 Not Found")),
            ErrorKind::NotFound
        );
        assert_eq!(
            classify_error(&anyhow!("No such key")),
            ErrorKind::NotFound
        );
    }

    #[test]
    fn test_is_retryable() {
        assert!(is_retryable(&anyhow!("500 Internal Server Error")));
        assert!(is_retryable(&anyhow!("Connection timeout")));
        assert!(is_retryable(&anyhow!("Storage error: Service unavailable (injected)")));
        assert!(!is_retryable(&anyhow!("401 Unauthorized")));
        assert!(!is_retryable(&anyhow!("403 Forbidden")));
        assert!(!is_retryable(&anyhow!("400 Bad Request")));
    }

    #[test]
    fn test_backoff_calculation() {
        let policy = RetryPolicy::new(RetryConfig {
            base_delay_ms: 100,
            max_delay_ms: 30_000,
            ..Default::default()
        });

        // First attempt: 0-100ms
        for _ in 0..10 {
            let delay = policy.calculate_delay(0);
            assert!(delay <= Duration::from_millis(100));
        }

        // Second attempt: 0-200ms
        for _ in 0..10 {
            let delay = policy.calculate_delay(1);
            assert!(delay <= Duration::from_millis(200));
        }

        // High attempt: capped at 30s
        for _ in 0..10 {
            let delay = policy.calculate_delay(20);
            assert!(delay <= Duration::from_millis(30_000));
        }
    }

    #[test]
    fn test_circuit_breaker_states() {
        let cb = CircuitBreaker::new(3, 100);

        // Initial state: closed
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.should_allow());

        // Record failures
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.should_allow());

        // Third failure opens circuit
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.should_allow());

        // Wait for cooldown (using short test cooldown)
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        assert!(cb.should_allow());

        // Success closes circuit
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.should_allow());
    }

    #[tokio::test]
    async fn test_retry_success() {
        let policy = RetryPolicy::default_policy();
        let result: Result<i32> = policy.execute(|| async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_transient_then_success() {
        let policy = RetryPolicy::new(RetryConfig {
            max_retries: 3,
            base_delay_ms: 10,
            ..Default::default()
        });

        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result: Result<i32> = policy
            .execute(|| {
                let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async move {
                    if attempt < 2 {
                        Err(anyhow!("Service unavailable (injected)"))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_auth_error_no_retry() {
        let policy = RetryPolicy::new(RetryConfig {
            max_retries: 5,
            base_delay_ms: 10,
            ..Default::default()
        });

        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result: Result<i32> = policy
            .execute(|| {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async { Err(anyhow!("401 Unauthorized")) }
            })
            .await;

        assert!(result.is_err());
        // Should only try once - no retry for auth errors
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let policy = RetryPolicy::new(RetryConfig {
            max_retries: 2,
            base_delay_ms: 10,
            circuit_breaker_enabled: false,
            ..Default::default()
        });

        let attempts = std::sync::atomic::AtomicU32::new(0);

        let result: Result<i32> = policy
            .execute(|| {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async { Err(anyhow!("Service unavailable (injected)")) }
            })
            .await;

        assert!(result.is_err());
        // Initial attempt + 2 retries = 3 attempts
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 3);
    }
}
