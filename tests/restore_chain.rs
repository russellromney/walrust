use anyhow::Result;
use rusqlite::Connection;
use std::path::Path;
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
        .unwrap_or_else(|_| "walrust-test-rr-2026/restore-chain-test".to_string());
    let endpoint = std::env::var("AWS_ENDPOINT_URL_S3")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
        .ok();
    (bucket, endpoint)
}

fn create_sqlite_db(path: &Path) -> Result<u32> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "
        CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO items (id, value) VALUES (1, 'base-1');
        INSERT INTO items (id, value) VALUES (2, 'base-2');
        ",
    )?;
    let page_size: u32 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    drop(conn);
    Ok(page_size)
}

#[tokio::test]
async fn restore_rejects_incremental_without_prior_chain_link() -> Result<()> {
    let (bucket_arg, endpoint) = test_bucket_config();
    let (bucket, prefix) = walrust::s3::parse_bucket(&bucket_arg);
    let client = walrust::s3::create_client(endpoint.as_deref()).await?;
    let name = unique_name("restore-chain");
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join(format!("{name}.db"));
    let restored = tmp.path().join("restored.db");
    let page_size = create_sqlite_db(&source)?;

    let mut snapshot = Vec::new();
    walrust::ltx::encode_snapshot(&mut snapshot, &source, page_size, 1)?;
    let snapshot_key = format!("{prefix}{name}/0001/0000000000000001-0000000000000001.ltx");
    walrust::s3::upload_bytes(&client, &bucket, &snapshot_key, snapshot).await?;

    let snapshot_checksum = walrust::ltx::compute_checksum_from_file(&source)?;
    let missing_pages = vec![(1, vec![0x22; page_size as usize])];
    let missing_post = walrust::ltx::chain_checksum(snapshot_checksum, &missing_pages);
    let skipped_pages = vec![(2, vec![0x33; page_size as usize])];
    let skipped_post = walrust::ltx::chain_checksum(missing_post, &skipped_pages);
    let mut skipped_incremental = Vec::new();
    walrust::ltx::encode_wal_changes(
        &mut skipped_incremental,
        &skipped_pages,
        page_size,
        3,
        3,
        2,
        Some(missing_post),
        skipped_post,
    )?;
    let skipped_key = format!("{prefix}{name}/0000/0000000000000003-0000000000000003.ltx");
    walrust::s3::upload_bytes(&client, &bucket, &skipped_key, skipped_incremental).await?;

    let err = walrust::sync::restore(
        &name,
        &restored,
        &bucket_arg,
        endpoint.as_deref(),
        None,
        None,
        None,
    )
    .await
    .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("pre-apply checksum") || msg.contains("gap") || msg.contains("chain"),
        "expected restore to reject the missing chain link, got: {msg}"
    );
    Ok(())
}
