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

fn create_marker_db(path: &Path, marker: &str) -> Result<()> {
    let conn = Connection::open(path)?;
    conn.execute("CREATE TABLE marker (value TEXT NOT NULL);", [])?;
    conn.execute(
        "INSERT INTO marker (value) VALUES (?1)",
        rusqlite::params![marker],
    )?;
    Ok(())
}

fn sqlite_page_size(path: &Path) -> Result<u32> {
    let conn = Connection::open(path)?;
    Ok(conn.query_row("PRAGMA page_size", [], |row| row.get(0))?)
}

#[tokio::test]
async fn point_in_time_restore_uses_latest_snapshot_not_after_target() -> Result<()> {
    let (bucket_arg, endpoint) = test_bucket_config();
    let (bucket, prefix) = walrust::s3::parse_bucket(&bucket_arg);
    let client = walrust::s3::create_client(endpoint.as_deref()).await?;
    let name = unique_name("restore-pit-snapshot");
    let tmp = tempfile::tempdir()?;
    let old_db = tmp.path().join("old.db");
    let new_db = tmp.path().join("new.db");
    let restored = tmp.path().join("restored.db");

    create_marker_db(&old_db, "old-snapshot")?;
    create_marker_db(&new_db, "newer-snapshot")?;

    let mut old_snapshot = Vec::new();
    walrust::ltx::encode_snapshot(&mut old_snapshot, &old_db, sqlite_page_size(&old_db)?, 1)?;
    let old_key = format!("{prefix}{name}/0001/0000000000000001-0000000000000001.ltx");
    walrust::s3::upload_bytes(&client, &bucket, &old_key, old_snapshot).await?;

    let mut new_snapshot = Vec::new();
    walrust::ltx::encode_snapshot(&mut new_snapshot, &new_db, sqlite_page_size(&new_db)?, 5)?;
    let new_key = format!("{prefix}{name}/0002/0000000000000001-0000000000000005.ltx");
    walrust::s3::upload_bytes(&client, &bucket, &new_key, new_snapshot).await?;

    walrust::sync::restore(
        &name,
        &restored,
        &bucket_arg,
        endpoint.as_deref(),
        Some("3"),
        None,
        None,
    )
    .await?;

    let conn = Connection::open(&restored)?;
    let marker: String = conn.query_row("SELECT value FROM marker", [], |row| row.get(0))?;
    assert_eq!(
        marker, "old-snapshot",
        "PIT restore at TXID 3 must choose the latest snapshot <= target, not the newer TXID 5 snapshot"
    );
    Ok(())
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

#[tokio::test]
async fn failed_restore_preserves_existing_output_database() -> Result<()> {
    let (bucket_arg, endpoint) = test_bucket_config();
    let (bucket, prefix) = walrust::s3::parse_bucket(&bucket_arg);
    let client = walrust::s3::create_client(endpoint.as_deref()).await?;
    let name = unique_name("restore-preserve-output");
    let tmp = tempfile::tempdir()?;
    let source = tmp.path().join(format!("{name}.db"));
    let restored = tmp.path().join("restored.db");
    let page_size = create_sqlite_db(&source)?;
    create_marker_db(&restored, "must-survive")?;
    let original_output = std::fs::read(&restored)?;

    let mut snapshot = Vec::new();
    walrust::ltx::encode_snapshot(&mut snapshot, &source, page_size, 1)?;
    let snapshot_key = format!("{prefix}{name}/0001/0000000000000001-0000000000000001.ltx");
    walrust::s3::upload_bytes(&client, &bucket, &snapshot_key, snapshot).await?;

    let snapshot_checksum = walrust::ltx::compute_checksum_from_file(&source)?;
    let missing_pages = vec![(1, vec![0x44; page_size as usize])];
    let missing_post = walrust::ltx::chain_checksum(snapshot_checksum, &missing_pages);
    let skipped_pages = vec![(2, vec![0x55; page_size as usize])];
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

    walrust::sync::restore(
        &name,
        &restored,
        &bucket_arg,
        endpoint.as_deref(),
        None,
        None,
        None,
    )
    .await
    .expect_err("restore must fail before publishing over the existing output");

    assert_eq!(
        std::fs::read(&restored)?,
        original_output,
        "failed restore must leave the existing output database untouched"
    );
    Ok(())
}
