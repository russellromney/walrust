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
fn missing_restore_backup_exits_with_restore_status() {
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
