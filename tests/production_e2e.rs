use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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
        CREATE TABLE walrust_e2e_pin (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
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

fn pin_read_transaction(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; BEGIN;")?;
    let _: i64 = conn.query_row("SELECT COUNT(*) FROM walrust_e2e_pin", [], |row| row.get(0))?;
    Ok(conn)
}

fn write_pin_frame(conn: &Connection, label: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO walrust_e2e_pin (label) VALUES (?1)",
        rusqlite::params![label],
    )?;
    Ok(())
}

fn append_rows(conn: &Connection, start: i64, end: i64, label: &str) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let result = (|| -> Result<()> {
        for id in start..=end {
            conn.execute(
                "INSERT INTO items (id, value) VALUES (?1, ?2)",
                rusqlite::params![id, format!("{label}-{id}")],
            )?;
        }
        Ok(())
    })();

    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT;")?;
            Ok(())
        }
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK;");
            Err(err)
        }
    }
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

fn wait_for_file_or_child_exit(child: &mut Child, path: &Path, context: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if path.exists() {
            return Ok(());
        }

        if let Some(status) = child.try_wait()? {
            anyhow::bail!("{context}: child exited early with status {status}");
        }

        if Instant::now() >= deadline {
            anyhow::bail!("{context}: timed out waiting for {}", path.display());
        }

        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_live_incremental(bucket_arg: &str, endpoint: Option<&str>, name: &str) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let (bucket, prefix) = walrust::s3::parse_bucket(bucket_arg);
        let client = walrust::s3::create_client(endpoint).await?;
        let db_prefix = format!("{prefix}{name}/0000/");
        let deadline = Instant::now() + Duration::from_secs(20);

        loop {
            let objects = walrust::s3::list_objects(&client, &bucket, &db_prefix).await?;
            if objects.iter().any(|key| key.ends_with(".ltx")) {
                return Ok(());
            }

            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for live incremental under s3://{bucket}/{db_prefix}"
                );
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
}

fn core_replicator_config() -> walrust::walrust_core::ReplicationConfig {
    walrust::walrust_core::ReplicationConfig {
        sync_interval: Duration::from_millis(100),
        snapshot_interval: Duration::from_secs(3600),
        ..Default::default()
    }
}

async fn flush_until_frames(
    replicator: &walrust::walrust_core::Replicator,
    name: &str,
    context: &str,
) -> Result<u64> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let frames = replicator.flush(name).await?;
        if frames > 0 {
            return Ok(frames);
        }
        if Instant::now() >= deadline {
            anyhow::bail!("{context}: timed out waiting for WAL frames");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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

struct CoreSigkillHelperArgs<'a> {
    phase: &'a str,
    name: &'a str,
    prefix: &'a str,
    bucket: &'a str,
    endpoint: Option<&'a str>,
    db_path: &'a Path,
    ready_path: &'a Path,
    go_path: &'a Path,
    flushed_path: &'a Path,
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

    let _setup = create_source_db(&db_path, 5)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;

    let mut child = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;

    std::thread::sleep(Duration::from_secs(2));
    append_rows(&writer, 6, 10, "watch")?;
    wait_for_live_incremental(&bucket_arg, endpoint.as_deref(), &name)?;
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

    let _setup = create_source_db(&db_path, 5)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;

    let mut first = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;
    std::thread::sleep(Duration::from_secs(2));
    append_rows(&writer, 6, 8, "pre-kill")?;
    std::thread::sleep(Duration::from_secs(2));
    stop_child(&mut first);

    let mut second = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;
    std::thread::sleep(Duration::from_secs(2));
    wait_for_live_incremental(&bucket_arg, endpoint.as_deref(), &name)?;
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

    let setup = create_source_db(&db_path, 5)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;

    let storage = walrust::s3_backend_from_env(test_bucket(), test_endpoint().as_deref()).await?;
    let config = core_replicator_config();
    let replicator = walrust::walrust_core::Replicator::new(storage, &prefix, config);
    replicator.add(&name, &db_path).await?;

    write_pin_frame(&setup, "core")?;
    let read_pin = pin_read_transaction(&db_path)?;
    append_rows(&writer, 6, 10, "core")?;
    let frames = replicator.flush(&name).await?;
    drop(read_pin);
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
async fn e2e_core_replicator_restart_reopens_state_and_restores_cleanly() -> Result<()> {
    let temp = TempDir::new()?;
    let name = unique_name("core-restart-e2e");
    let prefix = format!("e2e/{name}/");
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let setup = create_source_db(&db_path, 5)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;

    let storage = walrust::s3_backend_from_env(test_bucket(), test_endpoint().as_deref()).await?;
    let config = core_replicator_config();
    let first = walrust::walrust_core::Replicator::new(storage.clone(), &prefix, config.clone());
    first.add(&name, &db_path).await?;
    write_pin_frame(&setup, "core-pre-restart")?;
    let first_read_pin = pin_read_transaction(&db_path)?;
    append_rows(&writer, 6, 8, "core-pre-restart")?;
    anyhow::ensure!(
        first.flush(&name).await? > 0,
        "first replicator flush should upload WAL frames"
    );
    drop(first_read_pin);
    drop(first);

    let second = walrust::walrust_core::Replicator::new(storage, &prefix, config);
    second.add_without_snapshot(&name, &db_path).await?;
    write_pin_frame(&setup, "core-post-restart")?;
    let second_read_pin = pin_read_transaction(&db_path)?;
    append_rows(&writer, 9, 12, "core-post-restart")?;
    anyhow::ensure!(
        second.flush(&name).await? > 0,
        "second replicator flush should upload WAL frames"
    );
    drop(second_read_pin);
    let restored_seq = second.restore(&name, &restored_path).await?;
    anyhow::ensure!(
        restored_seq.is_some(),
        "restart restore should find the reopened state"
    );
    assert_integrity_ok(&restored_path)?;
    assert_eq!(rows(&db_path)?, rows(&restored_path)?);

    Ok(())
}

#[test]
fn e2e_core_replicator_sigkill_restart_round_trips_sqlite_rows() -> Result<()> {
    let temp = TempDir::new()?;
    let name = unique_name("core-sigkill-e2e");
    let prefix = format!("e2e/{name}/");
    let bucket = test_bucket();
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");
    let ready_path = temp.path().join("core-child-ready");
    let go_path = temp.path().join("core-child-go");
    let flushed_path = temp.path().join("core-child-flushed");

    let _setup = create_source_db(&db_path, 5)?;

    let mut first = spawn_core_sigkill_helper(CoreSigkillHelperArgs {
        phase: "first",
        name: &name,
        prefix: &prefix,
        bucket: &bucket,
        endpoint: endpoint.as_deref(),
        db_path: &db_path,
        ready_path: &ready_path,
        go_path: &go_path,
        flushed_path: &flushed_path,
    })?;
    wait_for_file_or_child_exit(&mut first, &ready_path, "first core helper startup")?;
    std::fs::write(&go_path, b"go")?;
    wait_for_file_or_child_exit(&mut first, &flushed_path, "first core helper flush")?;
    stop_child(&mut first);

    let mut second = spawn_core_sigkill_helper(CoreSigkillHelperArgs {
        phase: "second",
        name: &name,
        prefix: &prefix,
        bucket: &bucket,
        endpoint: endpoint.as_deref(),
        db_path: &db_path,
        ready_path: &ready_path,
        go_path: &go_path,
        flushed_path: &flushed_path,
    })?;
    let status = second.wait()?;
    anyhow::ensure!(status.success(), "second core helper failed with {status}");

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let storage = walrust::s3_backend_from_env(bucket, endpoint.as_deref()).await?;
        let replicator =
            walrust::walrust_core::Replicator::new(storage, &prefix, core_replicator_config());
        let restored_seq = replicator.restore(&name, &restored_path).await?;
        anyhow::ensure!(
            restored_seq.is_some(),
            "core SIGKILL restore should find the restarted stream"
        );
        Ok::<_, anyhow::Error>(())
    })?;

    assert_integrity_ok(&restored_path)?;
    assert_eq!(rows(&db_path)?, rows(&restored_path)?);

    Ok(())
}

fn spawn_core_sigkill_helper(args: CoreSigkillHelperArgs<'_>) -> Result<Child> {
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.arg("--exact")
        .arg("e2e_core_replicator_sigkill_child")
        .arg("--ignored")
        .arg("--nocapture")
        .env("WALRUST_CORE_SIGKILL_PHASE", args.phase)
        .env("WALRUST_CORE_SIGKILL_NAME", args.name)
        .env("WALRUST_CORE_SIGKILL_PREFIX", args.prefix)
        .env("WALRUST_CORE_SIGKILL_BUCKET", args.bucket)
        .env("WALRUST_CORE_SIGKILL_DB", args.db_path)
        .env("WALRUST_CORE_SIGKILL_READY", args.ready_path)
        .env("WALRUST_CORE_SIGKILL_GO", args.go_path)
        .env("WALRUST_CORE_SIGKILL_FLUSHED", args.flushed_path);
    if let Some(endpoint) = args.endpoint {
        cmd.env("WALRUST_CORE_SIGKILL_ENDPOINT", endpoint);
    }
    cmd.spawn().context("spawn core replicator SIGKILL helper")
}

#[test]
#[ignore = "spawned by e2e_core_replicator_sigkill_restart_round_trips_sqlite_rows"]
fn e2e_core_replicator_sigkill_child() -> Result<()> {
    let phase = std::env::var("WALRUST_CORE_SIGKILL_PHASE")?;
    let name = std::env::var("WALRUST_CORE_SIGKILL_NAME")?;
    let prefix = std::env::var("WALRUST_CORE_SIGKILL_PREFIX")?;
    let bucket = std::env::var("WALRUST_CORE_SIGKILL_BUCKET")?;
    let endpoint = std::env::var("WALRUST_CORE_SIGKILL_ENDPOINT").ok();
    let db_path = std::path::PathBuf::from(std::env::var("WALRUST_CORE_SIGKILL_DB")?);
    let ready_path = std::path::PathBuf::from(std::env::var("WALRUST_CORE_SIGKILL_READY")?);
    let go_path = std::path::PathBuf::from(std::env::var("WALRUST_CORE_SIGKILL_GO")?);
    let flushed_path = std::path::PathBuf::from(std::env::var("WALRUST_CORE_SIGKILL_FLUSHED")?);

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let storage = walrust::s3_backend_from_env(bucket, endpoint.as_deref()).await?;
        let replicator =
            walrust::walrust_core::Replicator::new(storage, &prefix, core_replicator_config());
        match phase.as_str() {
            "first" => {
                replicator.add(&name, &db_path).await?;
                std::fs::write(&ready_path, b"ready")?;
                let deadline = Instant::now() + Duration::from_secs(20);
                while !go_path.exists() {
                    if Instant::now() >= deadline {
                        anyhow::bail!("first helper timed out waiting for go signal");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                let setup = Connection::open(&db_path)?;
                setup.execute_batch("PRAGMA journal_mode=WAL;")?;
                let writer = open_external_autocheckpoint_connection(&db_path)?;
                write_pin_frame(&setup, "core-pre-sigkill")?;
                let read_pin = pin_read_transaction(&db_path)?;
                append_rows(&writer, 6, 8, "core-pre-sigkill")?;
                flush_until_frames(&replicator, &name, "first helper").await?;
                std::fs::write(&flushed_path, b"flushed")?;
                let _keep_read_pin_alive = read_pin;
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
            "second" => {
                replicator.add_without_snapshot(&name, &db_path).await?;
                let setup = Connection::open(&db_path)?;
                setup.execute_batch("PRAGMA journal_mode=WAL;")?;
                let writer = open_external_autocheckpoint_connection(&db_path)?;
                write_pin_frame(&setup, "core-post-sigkill")?;
                let read_pin = pin_read_transaction(&db_path)?;
                append_rows(&writer, 9, 12, "core-post-sigkill")?;
                flush_until_frames(&replicator, &name, "second helper").await?;
                drop(read_pin);
                Ok(())
            }
            _ => anyhow::bail!("unknown core SIGKILL helper phase: {phase}"),
        }
    })
}
