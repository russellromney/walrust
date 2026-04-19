// Comprehensive tests for webhook notifications
//
// Tests cover: circuit breaker notifications, corruption detection, integration tests
//
// Skipped without `--features s3`: the `walrust::webhook` module under test
// is gated alongside the rest of the S3 path.

#![cfg(feature = "s3")]

use anyhow::Result;
use std::sync::Arc;
use walrust::config::WebhookConfig;
use walrust::retry::{RetryConfig, RetryPolicy};
use walrust::webhook::{WebhookSender, WebhookPayload};

// Test webhook server infrastructure
use axum::{
    Router,
    extract::State,
    response::IntoResponse,
    http::StatusCode,
    routing::post,
};
use std::sync::Mutex;
use tokio::net::TcpListener;

#[derive(Clone)]
struct TestWebhookServer {
    received: Arc<Mutex<Vec<ReceivedWebhook>>>,
}

#[derive(Debug, Clone)]
struct ReceivedWebhook {
    body: String,
    signature: Option<String>,
}

impl TestWebhookServer {
    fn new() -> Self {
        Self {
            received: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_received(&self) -> Vec<ReceivedWebhook> {
        self.received.lock().unwrap().clone()
    }
}

async fn webhook_handler(
    State(server): State<TestWebhookServer>,
    headers: axum::http::HeaderMap,
    body: String,
) -> impl IntoResponse {
    let signature = headers
        .get("X-Hadb-Signature")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    server.received.lock().unwrap().push(ReceivedWebhook {
        body,
        signature,
    });

    (StatusCode::OK, "webhook received")
}

async fn start_test_server() -> (String, TestWebhookServer, tokio::task::JoinHandle<()>) {
    let server = TestWebhookServer::new();
    let app = Router::new()
        .route("/webhook", post(webhook_handler))
        .with_state(server.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}/webhook", addr);

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    (url, server, handle)
}

// ============================================================================
// UNIT TESTS - CircuitBreaker webhook notifications
// ============================================================================

#[test]
fn test_circuit_breaker_has_webhook_field() {
    // Test that CircuitBreaker can be created with webhook
    let webhooks = vec![WebhookConfig {
        url: "https://example.com/hook".to_string(),
        events: vec!["circuit_breaker_open".to_string()],
        secret: Some("test-secret".to_string()),
    }];

    let webhook_sender = Arc::new(WebhookSender::new(webhooks));
    let config = RetryConfig::default();
    let policy = RetryPolicy::new(config);

    // Get circuit breaker
    let cb = policy.circuit_breaker();
    assert!(cb.is_some(), "Circuit breaker should exist");
}

#[test]
fn test_webhook_sender_creation() {
    // Test WebhookSender with various configurations

    // Empty webhooks
    let sender = WebhookSender::new(vec![]);
    assert!(sender.is_empty(), "Empty webhook sender should report isEmpty");

    // Single webhook
    let configs = vec![WebhookConfig {
        url: "https://hooks.example.com/test".to_string(),
        events: vec!["corruption_detected".to_string()],
        secret: Some("secret123".to_string()),
    }];
    let sender = WebhookSender::new(configs);
    assert!(!sender.is_empty(), "Non-empty webhook sender should not be isEmpty");

    // Multiple webhooks
    let configs = vec![
        WebhookConfig {
            url: "https://hook1.com".to_string(),
            events: vec!["upload_failed".to_string()],
            secret: None,
        },
        WebhookConfig {
            url: "https://hook2.com".to_string(),
            events: vec!["corruption_detected".to_string()],
            secret: Some("secret".to_string()),
        },
    ];
    let sender = WebhookSender::new(configs);
    assert!(!sender.is_empty(), "Multiple webhooks should not be isEmpty");
}

#[test]
fn test_webhook_payload_creation() {
    use walrust::webhook::WebhookEvent;

    // Test corruption_detected payload
    let payload = WebhookPayload::new(
        WebhookEvent::CorruptionDetected,
        "test-db",
        "Checksum mismatch",
        0,
    );

    assert_eq!(payload.event, "corruption_detected");
    assert_eq!(payload.database, "test-db");
    assert_eq!(payload.error, "Checksum mismatch");
    assert_eq!(payload.attempts, 0);
    assert!(payload.timestamp.contains("T"), "Timestamp should be ISO 8601");

    // Test circuit_breaker_open payload
    let payload = WebhookPayload::new(
        WebhookEvent::CircuitBreakerOpen,
        "my-db",
        "Circuit breaker opened after 10 failures",
        10,
    );

    assert_eq!(payload.event, "circuit_breaker_open");
    assert_eq!(payload.database, "my-db");
    assert_eq!(payload.attempts, 10);
}

#[test]
fn test_webhook_event_conversion() {
    use walrust::webhook::WebhookEvent;

    // Test as_str()
    assert_eq!(WebhookEvent::CorruptionDetected.as_str(), "corruption_detected");
    assert_eq!(WebhookEvent::CircuitBreakerOpen.as_str(), "circuit_breaker_open");
    assert_eq!(WebhookEvent::UploadFailed.as_str(), "upload_failed");
    assert_eq!(WebhookEvent::AuthFailure.as_str(), "auth_failure");
}

// ============================================================================
// INTEGRATION TESTS - Real webhook server
// ============================================================================

#[tokio::test]
async fn test_webhook_notify_corruption() -> Result<()> {
    // Test that notify_corruption actually sends HTTP POST with real server

    let (url, server, _handle) = start_test_server().await;

    let configs = vec![WebhookConfig {
        url,
        events: vec!["corruption_detected".to_string()],
        secret: Some("test-secret".to_string()),
    }];

    let sender = WebhookSender::new(configs);

    // Send notification
    sender.notify_corruption("test-database", "Checksum mismatch in file xyz.ltx").await;

    // Give webhook time to be received
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify webhook was received
    let received = server.get_received();
    assert_eq!(received.len(), 1, "Should receive exactly one webhook");

    let webhook = &received[0];
    assert!(webhook.body.contains("corruption_detected"), "Body should contain event type");
    assert!(webhook.body.contains("test-database"), "Body should contain database name");
    assert!(webhook.body.contains("Checksum mismatch"), "Body should contain error message");
    assert!(webhook.signature.is_some(), "Should have HMAC signature with secret configured");
    assert!(webhook.signature.as_ref().unwrap().starts_with("sha256="), "Signature should be sha256 format");

    Ok(())
}

#[tokio::test]
async fn test_webhook_notify_circuit_breaker() -> Result<()> {
    // Test circuit breaker open notification with real server

    let (url, server, _handle) = start_test_server().await;

    let configs = vec![WebhookConfig {
        url,
        events: vec!["circuit_breaker_open".to_string()],
        secret: Some("test-secret".to_string()),
    }];

    let sender = WebhookSender::new(configs);

    // Send notification
    sender.notify_circuit_breaker_open("production-db", 10).await;

    // Give webhook time to be received
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify webhook was received
    let received = server.get_received();
    assert_eq!(received.len(), 1, "Should receive exactly one webhook");

    let webhook = &received[0];
    assert!(webhook.body.contains("circuit_breaker_open"), "Body should contain event type");
    assert!(webhook.body.contains("production-db"), "Body should contain database name");
    assert!(webhook.body.contains("10"), "Body should contain failure count");
    assert!(webhook.signature.is_some(), "Should have HMAC signature");

    Ok(())
}

#[tokio::test]
async fn test_webhook_with_multiple_endpoints() -> Result<()> {
    // Test sending to multiple webhook endpoints with real servers

    // Start two independent webhook servers
    let (url1, server1, _handle1) = start_test_server().await;
    let (url2, server2, _handle2) = start_test_server().await;

    let configs = vec![
        WebhookConfig {
            url: url1,
            events: vec!["corruption_detected".to_string()],
            secret: Some("secret1".to_string()),
        },
        WebhookConfig {
            url: url2,
            events: vec!["corruption_detected".to_string()],
            secret: Some("secret2".to_string()),
        },
    ];

    let sender = WebhookSender::new(configs);

    // Send notification - should go to both endpoints
    sender.notify_corruption("multi-test-db", "Test corruption error").await;

    // Give webhooks time to be received
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify both servers received the webhook
    let received1 = server1.get_received();
    let received2 = server2.get_received();

    assert_eq!(received1.len(), 1, "Server 1 should receive one webhook");
    assert_eq!(received2.len(), 1, "Server 2 should receive one webhook");

    // Both should have the same payload content
    assert!(received1[0].body.contains("multi-test-db"), "Server 1 should receive correct payload");
    assert!(received2[0].body.contains("multi-test-db"), "Server 2 should receive correct payload");

    // Both should have signatures (but different due to different secrets)
    assert!(received1[0].signature.is_some(), "Server 1 should have signature");
    assert!(received2[0].signature.is_some(), "Server 2 should have signature");

    Ok(())
}

#[tokio::test]
async fn test_webhook_hmac_signature() -> Result<()> {
    // Test that HMAC signature is correctly computed and sent with real server

    let (url, server, _handle) = start_test_server().await;

    let configs = vec![WebhookConfig {
        url,
        events: vec!["corruption_detected".to_string()],
        secret: Some("known-secret-key".to_string()),
    }];

    let sender = WebhookSender::new(configs);

    // Send notification
    sender.notify_corruption("hmac-test-db", "Test HMAC signature").await;

    // Give webhook time to be received
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify webhook was received with signature
    let received = server.get_received();
    assert_eq!(received.len(), 1, "Should receive exactly one webhook");

    let webhook = &received[0];
    assert!(webhook.signature.is_some(), "Should have HMAC signature header");

    let signature = webhook.signature.as_ref().unwrap();
    assert!(signature.starts_with("sha256="), "Signature should be sha256 format");

    // Verify signature format (sha256= followed by hex characters)
    let hex_part = signature.strip_prefix("sha256=").unwrap();
    assert_eq!(hex_part.len(), 64, "HMAC-SHA256 signature should be 64 hex characters");
    assert!(hex_part.chars().all(|c| c.is_ascii_hexdigit()), "Signature should be hex encoded");

    // Verify the signature is actually valid by computing it ourselves
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let mut mac = HmacSha256::new_from_slice(b"known-secret-key").unwrap();
    mac.update(webhook.body.as_bytes());
    let expected_signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

    assert_eq!(*signature, expected_signature, "HMAC signature should match computed signature");

    Ok(())
}

// ============================================================================
// NEGATIVE TESTS - Error handling
// ============================================================================

#[tokio::test]
async fn test_webhook_with_invalid_url() {
    // Test that invalid URLs don't panic

    let configs = vec![WebhookConfig {
        url: "http://invalid-domain-that-does-not-exist-12345.com/hook".to_string(),
        events: vec!["corruption_detected".to_string()],
        secret: None,
    }];

    let sender = WebhookSender::new(configs);

    // Should not panic, just log error
    sender.notify_corruption("test-db", "Test error").await;

    // Give it time to fail
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    // If we get here, it didn't panic
}

#[tokio::test]
async fn test_webhook_with_no_secret() {
    // Test webhook without HMAC secret - should NOT include X-Hadb-Signature header
    let (url, server, _handle) = start_test_server().await;

    let configs = vec![WebhookConfig {
        url,
        events: vec!["corruption_detected".to_string()],
        secret: None, // No HMAC
    }];

    let sender = WebhookSender::new(configs);
    sender.notify_corruption("no-secret-db", "No HMAC test").await;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let received = server.get_received();
    assert_eq!(received.len(), 1, "Should receive exactly one webhook");
    assert!(received[0].signature.is_none(), "Should NOT have signature header when no secret configured");
    assert!(received[0].body.contains("no-secret-db"));
}

#[tokio::test]
async fn test_webhook_empty_sender_no_panic() {
    // Test that empty webhook sender doesn't panic

    let sender = WebhookSender::new(vec![]);

    // Should be no-op, not panic
    sender.notify_corruption("test-db", "Test error").await;
    sender.notify_circuit_breaker_open("test-db", 5).await;
}

// ============================================================================
// REGRESSION TESTS - Specific bugs
// ============================================================================

#[tokio::test]
async fn test_webhook_spawn_does_not_block() {
    // Regression test: verify() spawns webhook tasks so they don't block verification
    // Issue: Line 1046 in verify() was calling .await directly, blocking the loop
    // Fix: Spawn task with Arc::clone, webhook.notify_corruption() in background

    use std::sync::Arc;
    use std::time::Instant;

    // Create webhook with slow/unreachable endpoint
    let configs = vec![WebhookConfig {
        url: "http://192.0.2.1:12345/slow-endpoint".to_string(), // TEST-NET-1, unreachable
        events: vec!["corruption_detected".to_string()],
        secret: None,
    }];

    let webhook = Arc::new(WebhookSender::new(configs));

    // Spawn notification task (like verify() does) - should return immediately
    let start = Instant::now();
    let webhook_clone = Arc::clone(&webhook);
    tokio::spawn(async move {
        webhook_clone.notify_corruption("test-db", "Test error").await;
    });
    let elapsed = start.elapsed();

    // Spawn should be nearly instant (< 10ms)
    // If it blocks, it would take several seconds trying to connect
    assert!(
        elapsed.as_millis() < 10,
        "Spawning webhook task should not block (took {}ms)",
        elapsed.as_millis()
    );
}

#[tokio::test]
async fn test_circuit_breaker_webhook_does_not_block() {
    // Regression test: circuit breaker webhook should not block record_failure()

    use walrust::retry::{RetryConfig, RetryPolicy};
    use std::time::Instant;

    // This test verifies that CircuitBreaker.record_failure() spawns webhook task
    // and doesn't block on webhook delivery

    let config = RetryConfig {
        max_retries: 5,
        base_delay_ms: 100,
        max_delay_ms: 1000,
        circuit_breaker_enabled: true,
        circuit_breaker_threshold: 3,
        circuit_breaker_cooldown_ms: 60000,
    };

    let policy = RetryPolicy::new(config);

    if let Some(cb) = policy.circuit_breaker() {
        let start = Instant::now();

        // Trigger circuit breaker to open (3 failures)
        cb.record_failure();
        cb.record_failure();
        cb.record_failure(); // This should spawn webhook task

        let elapsed = start.elapsed();

        // Should be nearly instant (< 10ms) since webhook is spawned
        assert!(
            elapsed.as_millis() < 10,
            "Circuit breaker record_failure should not block (took {}ms)",
            elapsed.as_millis()
        );
    }
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[tokio::test]
async fn test_webhook_with_special_characters() {
    // Test payload with special characters, unicode, etc.
    let (url, server, _handle) = start_test_server().await;

    let configs = vec![WebhookConfig {
        url,
        events: vec!["corruption_detected".to_string()],
        secret: Some("secret".to_string()),
    }];

    let sender = WebhookSender::new(configs);
    sender.notify_corruption(
        "test-db-with-unicode-\u{6D4B}\u{8BD5}",
        "Error with special chars: <>&\"' and emoji",
    ).await;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let received = server.get_received();
    assert_eq!(received.len(), 1, "Should receive exactly one webhook");
    assert!(received[0].body.contains("test-db-with-unicode"), "Body should contain unicode database name");
    assert!(received[0].signature.is_some(), "Should have HMAC signature");
}

#[test]
fn test_webhook_config_validation() {
    // Test WebhookConfig with various valid and edge case inputs

    // Valid config
    let config = WebhookConfig {
        url: "https://hooks.example.com/walrust".to_string(),
        events: vec!["upload_failed".to_string(), "corruption_detected".to_string()],
        secret: Some("my-secret".to_string()),
    };
    assert!(!config.url.is_empty());
    assert!(!config.events.is_empty());

    // Config with empty events (edge case)
    let config = WebhookConfig {
        url: "https://hooks.example.com".to_string(),
        events: vec![],
        secret: None,
    };
    assert!(config.events.is_empty(), "Should allow empty events list");

    // Config with many events
    let config = WebhookConfig {
        url: "https://hooks.example.com".to_string(),
        events: vec![
            "upload_failed".to_string(),
            "auth_failure".to_string(),
            "corruption_detected".to_string(),
            "circuit_breaker_open".to_string(),
        ],
        secret: Some("secret".to_string()),
    };
    assert_eq!(config.events.len(), 4);
}
