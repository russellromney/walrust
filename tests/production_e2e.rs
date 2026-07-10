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

/// S3-backed E2E tests run only when S3 credentials/an endpoint are configured.
/// CI provisions MinIO and sets AWS_* env; local dev injects Tigris creds via
/// Soup. On a clean machine with no S3 configured these tests skip so that a
/// plain `cargo test --workspace` stays green (Phase 0.5).
fn s3_test_enabled() -> bool {
    std::env::var("AWS_ENDPOINT_URL_S3").is_ok()
        || std::env::var("AWS_ENDPOINT_URL").is_ok()
        || std::env::var("AWS_ACCESS_KEY_ID").is_ok()
}

macro_rules! require_s3 {
    ($name:literal) => {
        if !s3_test_enabled() {
            eprintln!(concat!(
                "SKIP ",
                $name,
                ": no S3 endpoint/credentials configured (set AWS_ACCESS_KEY_ID or AWS_ENDPOINT_URL_S3)"
            ));
            return Ok(());
        }
    };
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn create_source_db(path: &Path, base_rows: i64) -> Result<Connection> {
    create_source_db_with_page_size(path, base_rows, 4096)
}

fn create_source_db_with_page_size(
    path: &Path,
    base_rows: i64,
    page_size: u32,
) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(&format!(
        "
        PRAGMA page_size={page_size};
        PRAGMA journal_mode=WAL;
        PRAGMA wal_autocheckpoint=0;
        CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE walrust_e2e_pin (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
        ",
    ))?;
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

/// Force a TRUNCATE checkpoint from `conn`. Returns the `(busy, log, checkpointed)`
/// result row. When a reader pins a live WAL frame (e.g. the shadow blocker),
/// `busy` will be non-zero and the WAL is NOT reset — this is the mechanism under
/// test in the racing variants.
fn force_truncate_checkpoint(conn: &Connection) -> Result<(i64, i64, i64)> {
    let row = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })?;
    Ok(row)
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

/// Like `append_rows` but pads each value to ~400 bytes so a batch spans several
/// SQLite pages. Used by the racing-checkpoint test: a small tail write must NOT
/// re-image the earlier pages, so any batch a checkpoint folds before walrust
/// reads it can only be recovered by a full re-snapshot (proving the re-anchor).
fn append_wide_rows(conn: &Connection, start: i64, end: i64, label: &str) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let result = (|| -> Result<()> {
        for id in start..=end {
            let value = format!("{label}-{id}-{}", "x".repeat(400));
            conn.execute(
                "INSERT INTO items (id, value) VALUES (?1, ?2)",
                rusqlite::params![id, value],
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

fn sqlite_page_size(path: &Path) -> Result<u32> {
    let conn = Connection::open(path)?;
    Ok(conn.query_row("PRAGMA page_size", [], |row| row.get(0))?)
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

/// Env override name for E2E poll deadlines.
const E2E_DEADLINE_ENV: &str = "WALRUST_E2E_DEADLINE_SECS";

/// Parse a poll-deadline override. A positive integer wins; anything else
/// (unset, empty, non-numeric, zero) falls back to `default_secs`.
fn parse_deadline_override(raw: Option<&str>, default_secs: u64) -> Duration {
    let secs = raw
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&s| s > 0)
        .unwrap_or(default_secs);
    Duration::from_secs(secs)
}

/// Poll deadline for the E2E harness. WAN-sensitive: on a slow/remote S3
/// endpoint the fixed caps below can trip even though the operation would have
/// succeeded (this is the source of the known `flush_until_frames` flake under
/// load). Set `WALRUST_E2E_DEADLINE_SECS` to a larger positive integer on a
/// high-RTT link to widen every poll loop at once; unset keeps the defaults.
fn e2e_poll_deadline(default_secs: u64) -> Duration {
    parse_deadline_override(
        std::env::var(E2E_DEADLINE_ENV).ok().as_deref(),
        default_secs,
    )
}

#[test]
fn e2e_poll_deadline_override_parses() {
    assert_eq!(parse_deadline_override(None, 30), Duration::from_secs(30));
    assert_eq!(
        parse_deadline_override(Some("120"), 30),
        Duration::from_secs(120)
    );
    // Empty / non-numeric / zero fall back to the default.
    assert_eq!(
        parse_deadline_override(Some(""), 30),
        Duration::from_secs(30)
    );
    assert_eq!(
        parse_deadline_override(Some("abc"), 30),
        Duration::from_secs(30)
    );
    assert_eq!(
        parse_deadline_override(Some("0"), 30),
        Duration::from_secs(30)
    );
    assert_eq!(
        parse_deadline_override(Some("  45 "), 30),
        Duration::from_secs(45)
    );
}

/// Wait until the shadow watcher has attached its checkpoint blocker (which
/// creates and pins the `_walrust_seq` table). A fixed sleep races the watcher's
/// S3 discovery + initial snapshot, so on a slow endpoint the blocker is not yet
/// up and any "racing checkpoint" would be racing nothing. Poll for readiness so
/// the race actually happens against a live pin.
fn wait_for_shadow_blocker(db_path: &Path, child: &mut Child) -> Result<()> {
    let deadline = Instant::now() + e2e_poll_deadline(30);
    loop {
        let conn = Connection::open(db_path)?;
        let exists: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = '_walrust_seq'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if exists > 0 {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("watcher exited before attaching checkpoint blocker: {status}");
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for shadow checkpoint blocker (_walrust_seq)");
        }
        std::thread::sleep(Duration::from_millis(150));
    }
}

fn wait_for_file_or_child_exit(child: &mut Child, path: &Path, context: &str) -> Result<()> {
    let deadline = Instant::now() + e2e_poll_deadline(20);
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
    // Polls until the first WAL frame is published and returns immediately once
    // it is, so a generous deadline only matters under heavy sequential-test
    // load where S3 latency previously tripped a 10s cap and made the core
    // SIGKILL restart E2E flaky. Default 30s; WAN-tunable via
    // WALRUST_E2E_DEADLINE_SECS (see e2e_poll_deadline). No cost on the happy path.
    let deadline = Instant::now() + e2e_poll_deadline(30);
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

/// Spawn `walrust watch --independent-tasks` with a short snapshot interval, for
/// exercising the independent (poll) mode's periodic-snapshot re-anchor (B6).
fn spawn_cli_watch_independent(
    db_path: &Path,
    bucket_arg: &str,
    endpoint: Option<&str>,
    snapshot_interval_secs: u64,
) -> Result<Child> {
    let mut watch = Command::new(env!("CARGO_BIN_EXE_walrust"));
    watch
        .arg("watch")
        .arg(db_path)
        .arg("--bucket")
        .arg(bucket_arg)
        .arg("--independent-tasks")
        .arg("--snapshot-interval")
        .arg(snapshot_interval_secs.to_string())
        .arg("--wal-sync-interval")
        .arg("1")
        .arg("--checkpoint-interval")
        .arg("999999")
        .arg("--on-startup")
        .arg("true")
        .arg("--no-metrics")
        .arg("--no-cache");
    if let Some(endpoint) = endpoint {
        watch.arg("--endpoint").arg(endpoint);
    }
    watch
        .spawn()
        .context("spawn walrust watch --independent-tasks")
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

fn wait_for_cli_restore_rows(
    name: &str,
    bucket_arg: &str,
    endpoint: Option<&str>,
    restored_path: &Path,
    expected_rows: &[(i64, String)],
) -> Result<()> {
    let deadline = Instant::now() + e2e_poll_deadline(10);

    loop {
        let _ = std::fs::remove_file(restored_path);
        let attempt_error = match run_cli_restore(name, bucket_arg, endpoint, restored_path)
            .and_then(|_| assert_integrity_ok(restored_path))
            .and_then(|_| rows(restored_path))
        {
            Ok(actual_rows) if actual_rows == expected_rows => return Ok(()),
            Ok(actual_rows) => anyhow::anyhow!(
                "restored rows did not match yet: expected {:?}, got {:?}",
                expected_rows,
                actual_rows
            ),
            Err(err) => err,
        };

        if Instant::now() >= deadline {
            return Err(attempt_error.context("timed out waiting for restore rows to match"));
        }

        std::thread::sleep(Duration::from_millis(250));
    }
}

fn run_cli_snapshot(db_path: &Path, bucket_arg: &str, endpoint: Option<&str>) -> Result<()> {
    let mut snapshot = Command::new(env!("CARGO_BIN_EXE_walrust"));
    snapshot
        .arg("snapshot")
        .arg(db_path)
        .arg("--bucket")
        .arg(bucket_arg);
    if let Some(endpoint) = endpoint {
        snapshot.arg("--endpoint").arg(endpoint);
    }
    run_cmd(snapshot, "walrust snapshot")
}

#[test]
fn e2e_cli_watch_restore_round_trips_sqlite_rows() -> Result<()> {
    require_s3!("e2e_cli_watch_restore_round_trips_sqlite_rows");
    let temp = TempDir::new()?;
    let name = unique_name("cli-e2e");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let setup = create_source_db(&db_path, 5)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;
    write_pin_frame(&setup, "cli")?;
    let read_pin = pin_read_transaction(&db_path)?;

    let mut child = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;

    std::thread::sleep(Duration::from_secs(2));
    append_rows(&writer, 6, 10, "watch")?;
    let expected_rows = rows(&db_path)?;
    wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected_rows,
    )?;
    drop(read_pin);
    stop_child(&mut child);

    Ok(())
}

#[test]
fn e2e_cli_watch_restore_round_trips_64kb_pages() -> Result<()> {
    require_s3!("e2e_cli_watch_restore_round_trips_64kb_pages");
    let temp = TempDir::new()?;
    let name = unique_name("cli-64kb-e2e");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let setup = create_source_db_with_page_size(&db_path, 4, 65_536)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;
    write_pin_frame(&setup, "cli-64kb")?;
    let read_pin = pin_read_transaction(&db_path)?;

    let mut child = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;

    std::thread::sleep(Duration::from_secs(2));
    append_rows(&writer, 5, 9, "watch-64kb")?;
    let expected_rows = rows(&db_path)?;
    wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected_rows,
    )?;
    drop(read_pin);
    stop_child(&mut child);

    assert_eq!(sqlite_page_size(&restored_path)?, 65_536);

    Ok(())
}

#[test]
fn e2e_cli_watch_sigkill_restart_round_trips_sqlite_rows() -> Result<()> {
    require_s3!("e2e_cli_watch_sigkill_restart_round_trips_sqlite_rows");
    let temp = TempDir::new()?;
    let name = unique_name("cli-restart-e2e");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let setup = create_source_db(&db_path, 5)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;
    write_pin_frame(&setup, "cli-pre-kill")?;
    let first_read_pin = pin_read_transaction(&db_path)?;

    let mut first = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;
    std::thread::sleep(Duration::from_secs(2));
    append_rows(&writer, 6, 8, "pre-kill")?;
    std::thread::sleep(Duration::from_secs(2));
    drop(first_read_pin);
    stop_child(&mut first);

    write_pin_frame(&setup, "cli-post-kill")?;
    let second_read_pin = pin_read_transaction(&db_path)?;
    let mut second = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;
    std::thread::sleep(Duration::from_secs(2));
    append_rows(&writer, 9, 10, "post-kill")?;
    let expected_rows = rows(&db_path)?;
    wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected_rows,
    )?;
    drop(second_read_pin);
    stop_child(&mut second);

    Ok(())
}

#[test]
fn e2e_prune_during_restore_keeps_backup_restorable() -> Result<()> {
    require_s3!("e2e_prune_during_restore_keeps_backup_restorable");
    let temp = TempDir::new()?;
    let name = unique_name("compact-race-e2e");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let writer = create_source_db(&db_path, 4)?;
    run_cli_snapshot(&db_path, &bucket_arg, endpoint.as_deref())?;
    append_rows(&writer, 5, 7, "snapshot-two")?;
    run_cli_snapshot(&db_path, &bucket_arg, endpoint.as_deref())?;
    append_rows(&writer, 8, 12, "snapshot-three")?;
    run_cli_snapshot(&db_path, &bucket_arg, endpoint.as_deref())?;

    let mut restore = Command::new(env!("CARGO_BIN_EXE_walrust"));
    restore
        .arg("restore")
        .arg(&name)
        .arg("--output")
        .arg(&restored_path)
        .arg("--bucket")
        .arg(&bucket_arg);
    if let Some(endpoint) = endpoint.as_deref() {
        restore.arg("--endpoint").arg(endpoint);
    }

    let mut prune = Command::new(env!("CARGO_BIN_EXE_walrust"));
    prune
        .arg("prune")
        .arg(&name)
        .arg("--bucket")
        .arg(&bucket_arg)
        .arg("--hourly")
        .arg("0")
        .arg("--daily")
        .arg("0")
        .arg("--weekly")
        .arg("0")
        .arg("--monthly")
        .arg("0")
        .arg("--force");
    if let Some(endpoint) = endpoint.as_deref() {
        prune.arg("--endpoint").arg(endpoint);
    }

    let mut restore_child = restore.spawn().context("spawn walrust restore")?;
    let prune_output = prune.output().context("run walrust prune")?;
    let restore_status = restore_child.wait().context("wait walrust restore")?;

    anyhow::ensure!(
        prune_output.status.success(),
        "walrust prune failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&prune_output.stdout),
        String::from_utf8_lossy(&prune_output.stderr)
    );
    anyhow::ensure!(
        restore_status.success(),
        "walrust restore failed during prune"
    );

    assert_integrity_ok(&restored_path)?;
    assert_eq!(rows(&db_path)?, rows(&restored_path)?);

    Ok(())
}

#[tokio::test]
async fn e2e_core_replicator_restore_round_trips_sqlite_rows() -> Result<()> {
    require_s3!("e2e_core_replicator_restore_round_trips_sqlite_rows");
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

/// RACING external checkpoint against a running walrust-OWNED core replicator.
///
/// Under D2 the core `Replicator` holds a pinned-frame `_walrust_seq` checkpoint
/// blocker in walrust-owned mode (litestream-style), exactly like shadow mode.
/// A racing external `wal_checkpoint(TRUNCATE)` from a second connection must
/// therefore be REFUSED (busy != 0): the blocker pins a live WAL frame so the WAL
/// is never reset out from under an in-flight backup, and no committed frame is
/// ever destroyed. Restore must still round-trip every row.
///
/// (Before D2 owned mode had no blocker and this test forced an actual reset,
/// relying on rollover DETECTION -> re-snapshot to recover the folded frames. The
/// re-anchor backstop is still exercised — now via a walrust-DOWN window — by
/// `replicator_flush::test_walrust_owned_flush_resnapshots_after_checkpoint_rollover`.)
///
///  1. Batch A is written AND read by walrust (an incremental is published).
///  2. Batch A2 is written on fresh pages but NOT yet read by walrust.
///  3. A racing external `wal_checkpoint(TRUNCATE)` MUST be refused (busy != 0):
///     the pinned-frame blocker prevents the WAL reset, so A2's frames survive.
///  4. A tail batch B adds more frames.
///  5. Subsequent flushes ship A2 + B normally; restore round-trips all 125 rows.
#[tokio::test]
async fn e2e_core_replicator_racing_checkpoint_reanchors_without_data_loss() -> Result<()> {
    require_s3!("e2e_core_replicator_racing_checkpoint_reanchors_without_data_loss");
    let temp = TempDir::new()?;
    let name = unique_name("core-race-e2e");
    let prefix = format!("e2e/{name}/");
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let _setup = create_source_db(&db_path, 5)?;
    // Dedicated writer with autocheckpoint OFF: checkpoint timing is driven by our
    // explicit TRUNCATEs so the race is deterministic (not at the mercy of SQLite's
    // autocheckpoint firing between assertions). The explicit TRUNCATE from this
    // separate connection is still a real external checkpoint racing walrust.
    let writer = Connection::open(&db_path)?;
    writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")?;

    let storage = walrust::s3_backend_from_env(test_bucket(), test_endpoint().as_deref()).await?;
    let replicator =
        walrust::walrust_core::Replicator::new(storage, &prefix, core_replicator_config());
    // add() opens the D2 checkpoint blocker synchronously, so it is already pinning
    // the live WAL by the time we race a checkpoint below.
    replicator.add(&name, &db_path).await?;

    // 1. Batch A: walrust reads and publishes it.
    append_wide_rows(&writer, 6, 40, "raceA")?;
    let a_frames = replicator.flush(&name).await?;
    anyhow::ensure!(a_frames > 0, "batch A should publish an incremental");

    // 2. Batch A2: written on fresh pages but NOT read by walrust yet.
    append_wide_rows(&writer, 41, 120, "raceA2")?;

    // 3. Racing external TRUNCATE. The pinned-frame blocker MUST refuse it
    //    (busy != 0); the WAL is not reset, so A2's frames survive and ship on the
    //    next flush. A busy DB can also surface as a SQLITE_BUSY error rather than a
    //    busy!=0 result row — either is proof the pin engaged.
    let busy = match force_truncate_checkpoint(&writer) {
        Ok((busy, _log, _ckpt)) => busy,
        Err(_) => 1,
    };
    anyhow::ensure!(
        busy != 0,
        "the D2 checkpoint blocker must refuse a racing external TRUNCATE (busy={busy})"
    );

    // 4. Tail batch B adds more frames on top of the still-live WAL.
    append_wide_rows(&writer, 121, 125, "raceB")?;

    // 5. Ship the remaining frames.
    for _ in 0..4 {
        replicator.flush(&name).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let expected = rows(&db_path)?;
    anyhow::ensure!(
        expected.len() == 125,
        "source should hold all 125 rows (got {})",
        expected.len()
    );
    let restored_seq = replicator.restore(&name, &restored_path).await?;
    anyhow::ensure!(
        restored_seq.is_some(),
        "restore should find data after the racing checkpoint"
    );
    assert_integrity_ok(&restored_path)?;
    assert_eq!(
        expected,
        rows(&restored_path)?,
        "no data loss: the blocker refused the racing external checkpoint"
    );

    Ok(())
}

/// RACING variant for the CLI shadow watch stack (closes the A3/A4 Phase-2A scope
/// note): NO pinned reader; an external autocheckpoint connection issues explicit
/// TRUNCATE checkpoints racing the live watch sync. The shadow blocker pins a live
/// WAL frame, so the external checkpoints must be blocked (or captured) and restore
/// must round-trip every committed row. If the watcher instead dies, that is a loud
/// failure we surface — never silent loss.
#[test]
fn e2e_cli_watch_racing_checkpoint_no_data_loss() -> Result<()> {
    require_s3!("e2e_cli_watch_racing_checkpoint_no_data_loss");
    let temp = TempDir::new()?;
    let name = unique_name("cli-race");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let setup = create_source_db(&db_path, 5)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;
    write_pin_frame(&setup, "cli-race")?;

    // Deliberately NO pin_read_transaction — this is the racing variant.
    let mut child = spawn_cli_watch(&db_path, &bucket_arg, endpoint.as_deref(), true)?;
    // Wait for the blocker to actually attach. A fixed sleep races the watcher's
    // startup (S3 discovery + initial snapshot); if the blocker is not yet up, the
    // "race" races nothing and the test proves nothing (this is exactly why the
    // original fixed-2s version passed vacuously — the pin was not up).
    wait_for_shadow_blocker(&db_path, &mut child)?;

    // The watcher runs with a huge checkpoint-interval, so once up its shadow
    // blocker holds a pinned live WAL frame for the whole test. Every external
    // TRUNCATE we race against it MUST be refused (busy != 0) — that refusal is
    // precisely the mechanism that prevents the checkpoint from destroying
    // unshipped frames. We record the results and assert the pin actually engaged;
    // if the pin were broken the TRUNCATEs would succeed (busy == 0), tripping this
    // assertion AND risking real data loss (the rows are wide/multi-page so a lost
    // generation is not masked by a single-page full-image overwrite).
    let mut busy_results: Vec<i64> = Vec::new();
    for batch in 0..3i64 {
        let start = 6 + batch * 30;
        append_wide_rows(&writer, start, start + 29, "cli-race")?;
        // Race an explicit TRUNCATE against the in-flight watch sync.
        match force_truncate_checkpoint(&writer) {
            Ok((busy, _log, _ckpt)) => busy_results.push(busy),
            // A busy DB can surface as SQLITE_BUSY rather than a busy!=0 row; that
            // is also proof the pin engaged.
            Err(_) => busy_results.push(1),
        }
        std::thread::sleep(Duration::from_millis(400));
    }
    anyhow::ensure!(
        busy_results.iter().any(|&b| b != 0),
        "shadow blocker never pinned the WAL: every racing TRUNCATE succeeded \
         (busy results = {busy_results:?}); the checkpoint race was not actually \
         defended, so this test would prove nothing about racing"
    );

    let expected_rows = rows(&db_path)?;

    if let Some(status) = child.try_wait()? {
        anyhow::bail!("watch process exited early during checkpoint race: {status}");
    }

    wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected_rows,
    )?;

    stop_child(&mut child);
    Ok(())
}

/// B6: the independent (poll) watch mode now runs a periodic snapshot timer, so a
/// low-write DB whose WAL is reset by an external checkpoint still re-anchors its
/// remote base on a cadence. Drive the real independent watch task through a
/// checkpoint reset and assert the backup still round-trips.
#[test]
fn e2e_cli_watch_independent_snapshot_timer_round_trips_through_reset() -> Result<()> {
    require_s3!("e2e_cli_watch_independent_snapshot_timer_round_trips_through_reset");
    let temp = TempDir::new()?;
    let name = unique_name("cli-indep");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");

    let _setup = create_source_db(&db_path, 5)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;

    // Short snapshot interval so the periodic re-anchor fires during the test.
    let mut child = spawn_cli_watch_independent(&db_path, &bucket_arg, endpoint.as_deref(), 2)?;
    std::thread::sleep(Duration::from_secs(2));

    append_rows(&writer, 6, 10, "indep")?;
    // Independent mode holds no checkpoint blocker, so this reset succeeds and
    // folds rows into the main DB; the snapshot timer must re-anchor from it.
    force_truncate_checkpoint(&writer)?;
    append_rows(&writer, 11, 12, "indep")?;

    // Let the 2s snapshot timer fire at least once past the reset.
    std::thread::sleep(Duration::from_secs(5));

    let expected_rows = rows(&db_path)?;

    if let Some(status) = child.try_wait()? {
        anyhow::bail!("independent watch exited early: {status}");
    }

    wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected_rows,
    )?;

    stop_child(&mut child);
    Ok(())
}

#[tokio::test]
async fn e2e_core_replicator_restart_reopens_state_and_restores_cleanly() -> Result<()> {
    require_s3!("e2e_core_replicator_restart_reopens_state_and_restores_cleanly");
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
    require_s3!("e2e_core_replicator_sigkill_restart_round_trips_sqlite_rows");
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

/// Coordination-driven spawn target for the core SIGKILL E2E — NOT a
/// standalone test.
///
/// Why it stays `#[ignore]`d (and must not be un-ignored into the default set):
/// it is not a self-contained test. It reads its bucket/prefix/db/coordination
/// paths from `WALRUST_CORE_SIGKILL_*` env vars and blocks on `go`/`ready`/
/// `flushed` file handshakes; run standalone with no parent it errors on the
/// missing env (or, in the `first` phase, loops forever waiting for `go`). The
/// parent `e2e_core_replicator_sigkill_restart_round_trips_sqlite_rows` re-execs
/// *this same test binary* with `--exact e2e_core_replicator_sigkill_child
/// --ignored`, so the helper is compiled and shipped as part of the ordinary
/// test build — nothing extra to build in CI — and it *does* execute in CI:
/// the parent is `require_s3!`-gated, MinIO satisfies that gate, and the CI log
/// shows both spawned phases start (`running 1 test` twice). Only the `second`
/// phase prints `e2e_core_replicator_sigkill_child ... ok`; the `first` phase
/// is SIGKILLed mid-run by the parent, so it never reports — which is exactly
/// the crash being tested. So the SIGKILL path runs end to end in CI; the
/// `#[ignore]` only keeps the helper out of the unparameterized default run
/// where it has no parent to drive it.
#[test]
#[ignore = "spawn target of e2e_core_replicator_sigkill_restart_round_trips_sqlite_rows; \
            runs in CI when the parent re-execs it with --ignored (see doc comment)"]
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
                let deadline = Instant::now() + e2e_poll_deadline(20);
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

// ── Compaction CLI e2e (wave C3a): watch compacts, restore/verify read levels ─

/// Write a `walrust.toml` enabling leveled compaction with tiny batches and a
/// zero keep-fine window, so L1 and L2 fire within a short test.
fn write_compaction_config(path: &Path) -> Result<()> {
    std::fs::write(
        path,
        "[compaction]\n\
         enabled = true\n\
         keep_fine_window = \"0s\"\n\
         l1_batch = 4\n\
         l2_batch = 2\n",
    )?;
    Ok(())
}

fn spawn_cli_watch_compaction(
    db_path: &Path,
    config_path: &Path,
    bucket_arg: &str,
    endpoint: Option<&str>,
) -> Result<Child> {
    let mut watch = Command::new(env!("CARGO_BIN_EXE_walrust"));
    watch
        .arg("--config")
        .arg(config_path)
        .arg("watch")
        .arg(db_path)
        .arg("--bucket")
        .arg(bucket_arg)
        .arg("--independent-tasks")
        .arg("--snapshot-interval")
        .arg("999999")
        .arg("--wal-sync-interval")
        .arg("1")
        .arg("--checkpoint-interval")
        .arg("999999")
        .arg("--on-startup")
        .arg("true")
        .arg("--no-metrics")
        .arg("--no-cache");
    if let Some(endpoint) = endpoint {
        watch.arg("--endpoint").arg(endpoint);
    }
    watch
        .spawn()
        .context("spawn walrust watch (compaction, independent)")
}

async fn list_keys(prefix: &str) -> Result<Vec<String>> {
    let storage = walrust::s3_backend_from_env(test_bucket(), test_endpoint().as_deref()).await?;
    hadb_storage::StorageBackend::list(storage.as_ref(), prefix, None).await
}

/// Parse `{min:016x}-{max:016x}.ltx` (a merged level object) from a full key.
fn parse_level_range(key: &str) -> Option<(u64, u64)> {
    let filename = key.rsplit('/').next()?;
    let stem = filename.strip_suffix(".ltx")?;
    let (lo, hi) = stem.split_once('-')?;
    Some((
        u64::from_str_radix(lo, 16).ok()?,
        u64::from_str_radix(hi, 16).ok()?,
    ))
}

/// Run `walrust restore --point-in-time <pit>` and return the process output
/// (so callers can assert on both success and a typed failure message).
fn run_cli_restore_pit_output(
    name: &str,
    bucket_arg: &str,
    endpoint: Option<&str>,
    restored_path: &Path,
    pit: u64,
) -> Result<std::process::Output> {
    let mut restore = Command::new(env!("CARGO_BIN_EXE_walrust"));
    restore
        .arg("restore")
        .arg(name)
        .arg("--output")
        .arg(restored_path)
        .arg("--bucket")
        .arg(bucket_arg)
        .arg("--point-in-time")
        .arg(pit.to_string());
    if let Some(endpoint) = endpoint {
        restore.arg("--endpoint").arg(endpoint);
    }
    restore
        .output()
        .context("run walrust restore --point-in-time")
}

/// The wave's CLI proof: a real `walrust watch` with `[compaction] enabled`
/// fires L1 and L2, deletes the superseded L0 tail, and the leveled bucket is
/// then fully readable — restore-to-latest is row-exact + integrity-clean,
/// PITR to a merged boundary succeeds, PITR strictly inside a merged window is a
/// loud decay error, and `walrust verify` exits 0.
#[tokio::test]
async fn e2e_cli_compaction_fires_levels_and_restore_reads_them() -> Result<()> {
    require_s3!("e2e_cli_compaction_fires_levels_and_restore_reads_them");
    let temp = TempDir::new()?;
    let name = unique_name("cli-compaction");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");
    let config_path = temp.path().join("walrust.toml");
    write_compaction_config(&config_path)?;

    // Object key roots (db name == file stem == `name`).
    let l0_prefix = format!("{prefix}/{name}/0000/");
    let l1_prefix = format!("{prefix}/{name}/levels/L1/");
    let l2_prefix = format!("{prefix}/{name}/levels/L2/");

    let setup = create_source_db(&db_path, 5)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;
    write_pin_frame(&setup, "cli-compaction")?;

    let mut child =
        spawn_cli_watch_compaction(&db_path, &config_path, &bucket_arg, endpoint.as_deref())?;
    std::thread::sleep(Duration::from_secs(2));

    // Drive ~16 separate sync cycles so the L0 pool fills L1 (batch 4) three
    // times and then L2 (batch 2); one commit per >1s poll window == one L0.
    let mut next_id = 100i64;
    let l2_deadline = Instant::now() + e2e_poll_deadline(90);
    let mut fired_l2 = false;
    for _ in 0..24 {
        append_rows(&writer, next_id, next_id + 1, "compact")?;
        next_id += 2;
        std::thread::sleep(Duration::from_millis(1200));
        if !list_keys(&l2_prefix).await?.is_empty() {
            fired_l2 = true;
            break;
        }
        if Instant::now() >= l2_deadline {
            break;
        }
    }
    assert!(fired_l2, "L2 must fire within the deadline");

    // L1 also present; the superseded L0 objects were deleted by the engine, so
    // the L0 pool is far smaller than the ~16+ incrementals produced.
    let l1 = list_keys(&l1_prefix).await?;
    let l2 = list_keys(&l2_prefix).await?;
    let l0 = list_keys(&l0_prefix).await?;
    assert!(!l1.is_empty(), "L1 must be present: {l1:?}");
    assert!(!l2.is_empty(), "L2 must be present: {l2:?}");
    assert!(
        l0.len() < 10,
        "superseded L0 objects must be deleted (found {}): {l0:?}",
        l0.len()
    );

    // Restore-to-latest reads across the LTX→HADBP seam: row-exact + integrity.
    // Settle so the periodic 1s sync uploads the final committed state, then stop
    // driving and poll restore until the rows match.
    std::thread::sleep(Duration::from_secs(3));
    let expected_rows = rows(&db_path)?;
    stop_child(&mut child);
    wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected_rows,
    )?;

    // PITR to the L2 window boundary (its max txid) restores cleanly.
    let (l2_min, l2_max) = parse_level_range(&l2[0]).context("parse L2 range")?;
    let boundary_out = temp.path().join("boundary.db");
    let out = run_cli_restore_pit_output(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &boundary_out,
        l2_max,
    )?;
    anyhow::ensure!(
        out.status.success(),
        "PITR to the L2 boundary txid {l2_max} must succeed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_integrity_ok(&boundary_out)?;

    // PITR strictly inside the L2 window is the loud typed decay error.
    if l2_max > l2_min + 1 {
        let inside = l2_min + 1;
        let inside_out = temp.path().join("inside.db");
        let out = run_cli_restore_pit_output(
            &name,
            &bucket_arg,
            endpoint.as_deref(),
            &inside_out,
            inside,
        )?;
        anyhow::ensure!(
            !out.status.success(),
            "PITR inside a merged window (txid {inside}) must fail loudly"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        anyhow::ensure!(
            stderr.contains("falls inside merged window"),
            "decay error must name the merged window: {stderr}"
        );
        anyhow::ensure!(
            !inside_out.exists(),
            "a failed PITR must not publish a partial database"
        );
    }

    // verify exits 0 on the healthy leveled bucket.
    let mut verify = Command::new(env!("CARGO_BIN_EXE_walrust"));
    verify
        .arg("verify")
        .arg(&name)
        .arg("--bucket")
        .arg(&bucket_arg);
    if let Some(endpoint) = endpoint.as_deref() {
        verify.arg("--endpoint").arg(endpoint);
    }
    run_cmd(verify, "walrust verify")?;

    Ok(())
}
