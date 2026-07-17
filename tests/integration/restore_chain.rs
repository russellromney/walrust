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

/// S3-backed tests run only when S3 credentials/an endpoint are configured.
/// CI provisions MinIO and sets AWS_* env; local dev injects Tigris creds via
/// Soup. On a clean machine with no S3 configured these tests skip so that a
/// plain `cargo test --workspace` stays green (Phase 0.5).
fn s3_test_enabled() -> bool {
    std::env::var("AWS_ENDPOINT_URL_S3").is_ok()
        || std::env::var("AWS_ENDPOINT_URL").is_ok()
        || std::env::var("AWS_ACCESS_KEY_ID").is_ok()
}

#[tokio::test]
async fn point_in_time_restore_uses_latest_snapshot_not_after_target() -> Result<()> {
    if !s3_test_enabled() {
        eprintln!("SKIP point_in_time_restore_uses_latest_snapshot_not_after_target: no S3 endpoint/credentials configured");
        return Ok(());
    }
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

    // TXID 3 is absorbed by the newer snapshot's span (1..=5) and no finer
    // object covers it: per the compaction decay semantics this is a LOUD
    // typed error naming both neighbors — never a silent floor to TXID 1 and
    // never a bare chain-gap message.
    let err = walrust::sync::restore(
        &name,
        &restored,
        &bucket_arg,
        endpoint.as_deref(),
        Some("3"),
        None,
        None,
    )
    .await
    .expect_err("PIT inside a later snapshot's absorbed span must be a loud decay error");
    let msg = err.to_string();
    assert!(
        msg.contains("absorbed") && msg.contains("seq 1") && msg.contains("seq 5"),
        "decay error must name both neighbors (1 below, 5 above), got: {msg}"
    );
    assert!(
        !restored.exists(),
        "a failed PIT restore must not leave an output file"
    );

    // The A9 guarantee this test has always protected: the OLD retained
    // snapshot is still restorable at its own boundary.
    walrust::sync::restore(
        &name,
        &restored,
        &bucket_arg,
        endpoint.as_deref(),
        Some("1"),
        None,
        None,
    )
    .await?;

    let conn = Connection::open(&restored)?;
    let marker: String = conn.query_row("SELECT value FROM marker", [], |row| row.get(0))?;
    assert_eq!(
        marker, "old-snapshot",
        "PIT restore at TXID 1 must restore the old retained snapshot, not the newer TXID 5 snapshot"
    );
    Ok(())
}

#[tokio::test]
async fn restore_rejects_incremental_without_prior_chain_link() -> Result<()> {
    if !s3_test_enabled() {
        eprintln!("SKIP restore_rejects_incremental_without_prior_chain_link: no S3 endpoint/credentials configured");
        return Ok(());
    }
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
    if !s3_test_enabled() {
        eprintln!("SKIP failed_restore_preserves_existing_output_database: no S3 endpoint/credentials configured");
        return Ok(());
    }
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
