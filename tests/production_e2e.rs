use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET").unwrap_or_else(|_| "walrust-test-rr-2026".to_string())
}

fn test_endpoint() -> Option<String> {
    std::env::var("AWS_ENDPOINT_URL_S3")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
        .ok()
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn create_source_db(path: &Path, base_rows: i64) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA wal_autocheckpoint=0;
        CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
        ",
    )?;
    for id in 1..=base_rows {
        conn.execute(
            "INSERT INTO items (id, value) VALUES (?1, ?2)",
            rusqlite::params![id, format!("base-{id}")],
        )?;
    }
    Ok(conn)
}

fn open_external_autocheckpoint_connection(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=1;")?;
    Ok(conn)
}

fn append_rows(conn: &Connection, start: i64, end: i64, label: &str) -> Result<()> {
    for id in start..=end {
        conn.execute(
            "INSERT INTO items (id, value) VALUES (?1, ?2)",
            rusqlite::params![id, format!("{label}-{id}")],
        )?;
    }
    Ok(())
}

fn rows(path: &Path) -> Result<Vec<(i64, String)>> {
    let conn = Connection::open(path)?;
    let mut stmt = conn.prepare("SELECT id, value FROM items ORDER BY id")?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn assert_integrity_ok(path: &Path) -> Result<()> {
    let conn = Connection::open(path)?;
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    anyhow::ensure!(integrity == "ok", "integrity_check failed: {integrity}");
    Ok(())
}

fn run_cmd(mut cmd: Command, context: &str) -> Result<()> {
    let output = cmd.output().with_context(|| format!("run {context}"))?;
    anyhow::ensure!(
        output.status.success(),
        "{context} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn spawn_cli_watch(
    db_path: &Path,
    bucket_arg: &str,
    endpoint: Option<&str>,
    on_startup: bool,
) -> Result<Child> {
    let mut watch = Command::new(env!("CARGO_BIN_EXE_walrust"));
    watch
        .arg("watch")
        .arg(db_path)
        .arg("--bucket")
        .arg(bucket_arg)
        .arg("--snapshot-interval")
        .arg("999999")
        .arg("--wal-sync-interval")
        .arg("1")
        .arg("--checkpoint-interval")
        .arg("999999")
        .arg("--on-startup")
        .arg(on_startup.to_string())
        .arg("--no-metrics")
        .arg("--no-cache");
    if let Some(endpoint) = endpoint {
        watch.arg("--endpoint").arg(endpoint);
    }
    watch.spawn().context("spawn walrust watch")
}

fn run_cli_restore(
    name: &str,
    bucket_arg: &str,
    endpoint: Option<&str>,
    restored_path: &Path,
) -> Result<()> {
    let mut restore = Command::new(env!("CARGO_BIN_EXE_walrust"));
    restore
        .arg("restore")
        .arg(name)
        .arg("--output")
        .arg(restored_path)
        .arg("--bucket")
        .arg(bucket_arg);
    if let Some(endpoint) = endpoint {
        restore.arg("--endpoint").arg(endpoint);
    }
    run_cmd(restore, "walrust restore")
}

#[test]
fn e2e_cli_watch_restore_round_trips_sqlite_rows() -> Result<()> {
    let temp = TempDir::new()?;
    let name = unique_name("cli-e2e");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let writer = create_source_db(&db_path, 5)?;
    let _external_checkpoint = open_external_autocheckpoint_connection(&db_path)?;

    let mut child = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;

    std::thread::sleep(Duration::from_secs(2));
    append_rows(&writer, 6, 10, "watch")?;
    std::thread::sleep(Duration::from_secs(3));
    stop_child(&mut child);

    run_cli_restore(&name, &bucket_arg, endpoint.as_deref(), &restored_path)?;

    assert_integrity_ok(&restored_path)?;
    assert_eq!(rows(&db_path)?, rows(&restored_path)?);

    Ok(())
}

#[test]
fn e2e_cli_watch_sigkill_restart_round_trips_sqlite_rows() -> Result<()> {
    let temp = TempDir::new()?;
    let name = unique_name("cli-restart-e2e");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let writer = create_source_db(&db_path, 5)?;
    let _external_checkpoint = open_external_autocheckpoint_connection(&db_path)?;

    let mut first = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;
    std::thread::sleep(Duration::from_secs(2));
    append_rows(&writer, 6, 8, "pre-kill")?;
    std::thread::sleep(Duration::from_secs(2));
    stop_child(&mut first);

    let mut second = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;
    std::thread::sleep(Duration::from_secs(2));
    stop_child(&mut second);

    run_cli_restore(&name, &bucket_arg, endpoint.as_deref(), &restored_path)?;

    assert_integrity_ok(&restored_path)?;
    assert_eq!(rows(&db_path)?, rows(&restored_path)?);

    Ok(())
}

#[tokio::test]
async fn e2e_core_replicator_restore_round_trips_sqlite_rows() -> Result<()> {
    let temp = TempDir::new()?;
    let name = unique_name("core-e2e");
    let prefix = format!("e2e/{name}/");
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let writer = create_source_db(&db_path, 5)?;
    let _external_checkpoint = open_external_autocheckpoint_connection(&db_path)?;

    let storage = walrust::s3_backend_from_env(test_bucket(), test_endpoint().as_deref()).await?;
    let config = walrust::walrust_core::ReplicationConfig {
        sync_interval: Duration::from_millis(100),
        snapshot_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let replicator = walrust::walrust_core::Replicator::new(storage, &prefix, config);
    replicator.add(&name, &db_path).await?;

    append_rows(&writer, 6, 10, "core")?;
    let frames = replicator.flush(&name).await?;
    anyhow::ensure!(frames > 0, "replicator flush should upload WAL frames");
    let restored_seq = replicator.restore(&name, &restored_path).await?;
    anyhow::ensure!(
        restored_seq.is_some(),
        "replicator restore should find data"
    );

    assert_integrity_ok(&restored_path)?;
    assert_eq!(rows(&db_path)?, rows(&restored_path)?);

    Ok(())
}

#[tokio::test]
async fn e2e_core_replicator_restart_round_trips_sqlite_rows() -> Result<()> {
    let temp = TempDir::new()?;
    let name = unique_name("core-restart-e2e");
    let prefix = format!("e2e/{name}/");
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let writer = create_source_db(&db_path, 5)?;
    let _external_checkpoint = open_external_autocheckpoint_connection(&db_path)?;

    let storage = walrust::s3_backend_from_env(test_bucket(), test_endpoint().as_deref()).await?;
    let config = walrust::walrust_core::ReplicationConfig {
        sync_interval: Duration::from_millis(100),
        snapshot_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let first = walrust::walrust_core::Replicator::new(storage.clone(), &prefix, config.clone());
    first.add(&name, &db_path).await?;
    append_rows(&writer, 6, 8, "core-pre-restart")?;
    anyhow::ensure!(
        first.flush(&name).await? > 0,
        "first replicator flush should upload WAL frames"
    );

    let second = walrust::walrust_core::Replicator::new(storage, &prefix, config);
    second.add(&name, &db_path).await?;
    append_rows(&writer, 9, 12, "core-post-restart")?;
    anyhow::ensure!(
        second.flush(&name).await? > 0,
        "second replicator flush should upload WAL frames"
    );
    let restored_seq = second.restore(&name, &restored_path).await?;
    anyhow::ensure!(
        restored_seq.is_some(),
        "replicator restore should find data"
    );

    assert_integrity_ok(&restored_path)?;
    assert_eq!(rows(&db_path)?, rows(&restored_path)?);

    Ok(())
}
