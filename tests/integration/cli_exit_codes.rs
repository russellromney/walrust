use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn test_bucket_config() -> (String, Option<String>) {
    let bucket = std::env::var("WALRUST_TEST_BUCKET")
        .unwrap_or_else(|_| "walrust-test-rr-2026/cli-exit-codes".to_string());
    let endpoint = std::env::var("AWS_ENDPOINT_URL_S3")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
        .ok();
    (bucket, endpoint)
}

fn command_with_dummy_s3_env() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_walrust"));
    command
        .env("AWS_ACCESS_KEY_ID", "walrust-test")
        .env("AWS_SECRET_ACCESS_KEY", "walrust-test")
        .env("AWS_REGION", "us-east-1")
        .env("AWS_MAX_ATTEMPTS", "1")
        .env("AWS_EC2_METADATA_DISABLED", "true");
    command
}

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
fn invalid_replicate_interval_exits_with_config_status() {
    let output = Command::new(env!("CARGO_BIN_EXE_walrust"))
        .arg("replicate")
        .arg("s3://bucket/db")
        .arg("--local")
        .arg("/tmp/walrust-replica.db")
        .arg("--interval")
        .arg("not-a-duration")
        .output()
        .expect("walrust command should run");

    assert_eq!(
        output.status.code(),
        Some(2),
        "invalid CLI config should exit with config status; stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn invalid_replicate_source_exits_with_config_status() {
    let output = command_with_dummy_s3_env()
        .arg("replicate")
        .arg("not-an-s3-source")
        .arg("--local")
        .arg("/tmp/walrust-replica.db")
        .arg("--interval")
        .arg("1ms")
        .output()
        .expect("walrust command should run");

    assert_eq!(
        output.status.code(),
        Some(2),
        "invalid replica source should exit with config status; stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn missing_restore_backup_exits_with_restore_status() {
    if !s3_test_enabled() {
        eprintln!("SKIP missing_restore_backup_exits_with_restore_status: no S3 endpoint/credentials configured");
        return;
    }
    let (bucket, endpoint) = test_bucket_config();
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let output_path = tempdir.path().join("restored.db");
    let mut command = Command::new(env!("CARGO_BIN_EXE_walrust"));
    command
        .arg("restore")
        .arg(unique_name("missing-restore"))
        .arg("--output")
        .arg(&output_path)
        .arg("--bucket")
        .arg(&bucket);
    if let Some(endpoint) = endpoint {
        command.arg("--endpoint").arg(endpoint);
    }

    let output = command.output().expect("walrust command should run");

    assert_eq!(
        output.status.code(),
        Some(6),
        "missing restore backup should exit with restore status; stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn unreachable_verify_endpoint_exits_with_s3_status() {
    let output = command_with_dummy_s3_env()
        .arg("verify")
        .arg(unique_name("unreachable-s3"))
        .arg("--bucket")
        .arg("s3://walrust-cli-exit-test")
        .arg("--endpoint")
        .arg("http://127.0.0.1:9")
        .output()
        .expect("walrust command should run");

    assert_eq!(
        output.status.code(),
        Some(4),
        "unreachable S3 endpoint should exit with S3 status; stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
