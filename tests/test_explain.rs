// Comprehensive tests for `walrust explain` command
//
// Tests cover: positive cases, negative cases, edge cases, and integration tests
//
// Skipped without `--features s3`: the `walrust::sync` module under test is
// itself S3-only.

#![cfg(feature = "s3")]

use anyhow::Result;
use std::fs;
use walrust::config::{
    Config, DatabaseConfig, RetentionConfig, S3Config, SyncConfig, WebhookConfig,
};
use walrust::sync;

/// Helper to create a test database file
fn create_test_db(path: &str) -> Result<()> {
    let conn = rusqlite::Connection::open(path)?;
    conn.execute("CREATE TABLE test(id INTEGER)", [])?;
    conn.execute("INSERT INTO test VALUES (1)", [])?;
    Ok(())
}

/// Helper to create a minimal config
fn minimal_config() -> Config {
    Config {
        s3: S3Config {
            bucket: Some("test-bucket".to_string()),
            endpoint: None,
            ..Default::default()
        },
        sync: SyncConfig::default(),
        retention: RetentionConfig::default(),
        databases: vec![],
        webhooks: vec![],
        ..Default::default()
    }
}

/// Helper to create a DatabaseConfig with minimal fields
fn db_config(path: String, prefix: Option<String>) -> DatabaseConfig {
    DatabaseConfig {
        path,
        prefix,
        snapshot_interval: None,
        wal_sync_interval: None,
        max_changes: None,
        max_interval: None,
        on_idle: None,
        checkpoint_interval: None,
        min_checkpoint_page_count: None,
        wal_truncate_threshold_pages: None,
        validation_interval: None,
        retention: None,
    }
}

// ============================================================================
// POSITIVE TESTS - Expected successful behavior
// ============================================================================

#[test]
fn test_explain_with_valid_config() -> Result<()> {
    // Create test databases
    let tempdir = tempfile::tempdir()?;
    let db1_path = tempdir.path().join("app.db");
    let db2_path = tempdir.path().join("users.db");
    create_test_db(db1_path.to_str().unwrap())?;
    create_test_db(db2_path.to_str().unwrap())?;

    let config = Config {
        s3: S3Config {
            bucket: Some("my-bucket".to_string()),
            endpoint: Some("https://fly.storage.tigris.dev".to_string()),
            ..Default::default()
        },
        sync: SyncConfig {
            snapshot_interval: 3600,
            max_changes: 100,
            max_interval: 600,
            on_idle: 300,
            on_startup: true,
            validation_interval: 86400, // Daily validation
            ..Default::default()
        },
        retention: RetentionConfig {
            hourly: 24,
            daily: 7,
            weekly: 12,
            monthly: 12,
        },
        databases: vec![
            db_config(
                db1_path.to_str().unwrap().to_string(),
                Some("app".to_string()),
            ),
            DatabaseConfig {
                path: db2_path.to_str().unwrap().to_string(),
                prefix: Some("users".to_string()),
                snapshot_interval: Some(1800), // Override: 30 minutes
                wal_sync_interval: None,
                max_changes: None,
                max_interval: None,
                on_idle: None,
                checkpoint_interval: None,
                min_checkpoint_page_count: None,
                wal_truncate_threshold_pages: None,
                validation_interval: None,
                retention: None,
            },
        ],
        webhooks: vec![WebhookConfig {
            url: "https://hooks.example.com/walrust".to_string(),
            events: vec![
                "upload_failed".to_string(),
                "auth_failure".to_string(),
                "corruption_detected".to_string(),
            ],
            secret: Some("test-secret".to_string()),
        }],
        ..Default::default()
    };

    // Should not panic or error
    let result = sync::explain(&Some(config));
    assert!(result.is_ok(), "explain() should succeed with valid config");

    Ok(())
}

#[test]
fn test_explain_with_validation_enabled() -> Result<()> {
    let mut config = minimal_config();
    config.sync.validation_interval = 3600; // Hourly validation

    let result = sync::explain(&Some(config));
    assert!(
        result.is_ok(),
        "explain() should succeed with validation enabled"
    );

    Ok(())
}

#[test]
fn test_explain_with_webhooks_configured() -> Result<()> {
    let mut config = minimal_config();
    config.webhooks = vec![
        WebhookConfig {
            url: "https://webhook1.com".to_string(),
            events: vec!["upload_failed".to_string()],
            secret: Some("secret1".to_string()),
        },
        WebhookConfig {
            url: "https://webhook2.com".to_string(),
            events: vec![
                "auth_failure".to_string(),
                "corruption_detected".to_string(),
            ],
            secret: None, // No HMAC
        },
    ];

    let result = sync::explain(&Some(config));
    assert!(
        result.is_ok(),
        "explain() should succeed with multiple webhooks"
    );

    Ok(())
}

#[test]
fn test_explain_with_compaction_enabled() -> Result<()> {
    let mut config = minimal_config();
    config.sync.compact_after_snapshot = true;
    config.sync.compact_interval = 7200;

    let result = sync::explain(&Some(config));
    assert!(
        result.is_ok(),
        "explain() should succeed with compaction enabled"
    );

    Ok(())
}

#[test]
fn test_explain_with_per_database_overrides() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let db_path = tempdir.path().join("test.db");
    create_test_db(db_path.to_str().unwrap())?;

    let mut config = minimal_config();
    config.sync.snapshot_interval = 3600; // Global: 1 hour
    config.retention = RetentionConfig {
        hourly: 24,
        daily: 7,
        weekly: 12,
        monthly: 12,
    };

    config.databases = vec![DatabaseConfig {
        path: db_path.to_str().unwrap().to_string(),
        prefix: Some("override-test".to_string()),
        snapshot_interval: Some(900), // Override: 15 minutes
        max_changes: Some(50),        // Override
        retention: Some(RetentionConfig {
            hourly: 48, // Override
            daily: 14,  // Override
            weekly: 4,
            monthly: 6,
        }),
        wal_sync_interval: None,
        max_interval: None,
        on_idle: None,
        checkpoint_interval: None,
        min_checkpoint_page_count: None,
        wal_truncate_threshold_pages: None,
        validation_interval: None,
    }];

    let result = sync::explain(&Some(config));
    assert!(
        result.is_ok(),
        "explain() should succeed with per-database overrides"
    );

    Ok(())
}

// ============================================================================
// NEGATIVE TESTS - Expected error handling
// ============================================================================

#[test]
fn test_explain_with_no_config() -> Result<()> {
    // Should handle None gracefully
    let result = sync::explain(&None);
    assert!(
        result.is_ok(),
        "explain() should handle no config gracefully"
    );

    Ok(())
}

#[test]
fn test_explain_with_empty_databases() -> Result<()> {
    let config = minimal_config();
    // databases vec is empty

    let result = sync::explain(&Some(config));
    assert!(
        result.is_ok(),
        "explain() should handle empty databases list"
    );

    Ok(())
}

#[test]
fn test_explain_with_missing_bucket() -> Result<()> {
    let mut config = minimal_config();
    config.s3.bucket = None;

    let result = sync::explain(&Some(config));
    assert!(result.is_ok(), "explain() should handle missing bucket");

    Ok(())
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[test]
fn test_explain_with_nonexistent_databases() -> Result<()> {
    let mut config = minimal_config();
    config.databases = vec![db_config(
        "/nonexistent/path/to/database.db".to_string(),
        Some("ghost".to_string()),
    )];

    // Should handle error gracefully (print error message, not panic)
    let result = sync::explain(&Some(config));
    assert!(
        result.is_ok(),
        "explain() should handle nonexistent databases gracefully"
    );

    Ok(())
}

#[test]
fn test_explain_with_wildcard_no_matches() -> Result<()> {
    let tempdir = tempfile::tempdir()?;

    let mut config = minimal_config();
    config.databases = vec![db_config(
        format!("{}/*.db", tempdir.path().display()),
        Some("wildcard".to_string()),
    )];

    // Should handle no matches gracefully
    let result = sync::explain(&Some(config));
    assert!(
        result.is_ok(),
        "explain() should handle wildcard with no matches"
    );

    Ok(())
}

#[test]
fn test_explain_with_minimal_config() -> Result<()> {
    let config = minimal_config();

    let result = sync::explain(&Some(config));
    assert!(result.is_ok(), "explain() should work with minimal config");

    Ok(())
}

#[test]
fn test_explain_cost_estimation_with_multiple_databases() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let mut databases = vec![];

    // Create 5 test databases
    for i in 0..5 {
        let db_path = tempdir.path().join(format!("db{}.db", i));
        create_test_db(db_path.to_str().unwrap())?;
        databases.push(db_config(
            db_path.to_str().unwrap().to_string(),
            Some(format!("db{}", i)),
        ));
    }

    let mut config = minimal_config();
    config.databases = databases;
    config.retention = RetentionConfig {
        hourly: 24,
        daily: 7,
        weekly: 12,
        monthly: 12,
    };

    // Should calculate costs for 5 databases × 55 snapshots = 275 snapshots
    let result = sync::explain(&Some(config));
    assert!(
        result.is_ok(),
        "explain() should calculate costs for multiple databases"
    );

    Ok(())
}

#[test]
fn test_explain_with_validation_disabled() -> Result<()> {
    let mut config = minimal_config();
    config.sync.validation_interval = 0; // Disabled

    let result = sync::explain(&Some(config));
    assert!(
        result.is_ok(),
        "explain() should handle disabled validation"
    );

    Ok(())
}

#[test]
fn test_explain_with_no_webhooks() -> Result<()> {
    let mut config = minimal_config();
    config.webhooks = vec![]; // Empty

    let result = sync::explain(&Some(config));
    assert!(result.is_ok(), "explain() should handle no webhooks");

    Ok(())
}

// ============================================================================
// INTEGRATION TESTS - End-to-end with actual CLI
// ============================================================================

#[test]
fn test_explain_cli_integration() -> Result<()> {
    use std::process::Command;

    let tempdir = tempfile::tempdir()?;
    let config_path = tempdir.path().join("test-config.toml");
    let db_path = tempdir.path().join("test.db");

    // Create test database
    create_test_db(db_path.to_str().unwrap())?;

    // Create test config file
    let config_content = format!(
        r#"
[s3]
bucket = "integration-test-bucket"
endpoint = "https://fly.storage.tigris.dev"

[sync]
snapshot_interval = 3600
validation_interval = 86400

[retention]
hourly = 24
daily = 7
weekly = 12
monthly = 12

[[databases]]
path = "{}"
prefix = "integration-test"

[[webhooks]]
url = "https://hooks.example.com/test"
events = ["upload_failed", "corruption_detected"]
secret = "test-secret"
"#,
        db_path.display()
    );

    fs::write(&config_path, config_content)?;

    // Run walrust explain command
    let output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("explain")
        .arg("--config")
        .arg(config_path.to_str().unwrap())
        .output()?;

    assert!(
        output.status.success(),
        "walrust explain should exit successfully"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify output contains expected sections
    assert!(
        stdout.contains("Configuration Summary"),
        "Output should contain summary header"
    );
    assert!(
        stdout.contains("S3 Storage:"),
        "Output should contain S3 section"
    );
    assert!(
        stdout.contains("Validation:"),
        "Output should contain Validation section"
    );
    assert!(
        stdout.contains("Webhook Notifications:"),
        "Output should contain Webhooks section"
    );
    assert!(
        stdout.contains("Estimated Storage Costs:"),
        "Output should contain cost estimation"
    );
    assert!(
        stdout.contains("integration-test-bucket"),
        "Output should show bucket name"
    );
    assert!(
        stdout.contains("86400 seconds (24 hours)"),
        "Output should show validation interval"
    );
    assert!(
        stdout.contains("hooks.example.com"),
        "Output should show webhook URL"
    );

    Ok(())
}
