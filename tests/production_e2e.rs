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

/// Wait until the shadow watcher has attached its checkpoint blocker (which
/// creates and pins the `_walrust_seq` table). A fixed sleep races the watcher's
/// S3 discovery + initial snapshot, so on a slow endpoint the blocker is not yet
/// up and any "racing checkpoint" would be racing nothing. Poll for readiness so
/// the race actually happens against a live pin.
fn wait_for_shadow_blocker(db_path: &Path, child: &mut Child) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
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
    // 30s (not 10s): this polls until the first WAL frame is published and returns
    // immediately once it is, so a generous deadline only matters under heavy
    // sequential-test load where S3 latency previously tripped a 10s cap and made
    // the core SIGKILL restart E2E flaky. No cost on the happy path.
    let deadline = Instant::now() + Duration::from_secs(30);
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
    let deadline = Instant::now() + Duration::from_secs(10);

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
fn e2e_compaction_during_restore_keeps_backup_restorable() -> Result<()> {
    require_s3!("e2e_compaction_during_restore_keeps_backup_restorable");
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

    let mut compact = Command::new(env!("CARGO_BIN_EXE_walrust"));
    compact
        .arg("compact")
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
        compact.arg("--endpoint").arg(endpoint);
    }

    let mut restore_child = restore.spawn().context("spawn walrust restore")?;
    let compact_output = compact.output().context("run walrust compact")?;
    let restore_status = restore_child.wait().context("wait walrust restore")?;

    anyhow::ensure!(
        compact_output.status.success(),
        "walrust compact failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&compact_output.stdout),
        String::from_utf8_lossy(&compact_output.stderr)
    );
    anyhow::ensure!(
        restore_status.success(),
        "walrust restore failed during compaction"
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

/// RACING variant (closes the A3/A4 Phase-2A scope note) for walrust-owned core
/// mode, which has NO checkpoint blocker and relies purely on rollover DETECTION
/// -> full re-snapshot. NO pinned reader suppresses the external TRUNCATE.
///
/// This test is deliberately constructed so the re-anchor is LOAD-BEARING (it
/// FAILS with missing rows if the WalrustOwned rollover re-snapshot is disabled):
///
///  1. Batch A is written AND read by walrust (an incremental is published; this
///     also records the current WAL salt so the next rollover is *detected*).
///  2. Batch A2 is written on fresh pages but NOT yet read by walrust.
///  3. An external `wal_checkpoint(TRUNCATE)` folds A+A2 into the main DB and
///     resets the WAL — `busy==0` proves the reset really happened. A2's frames
///     are now gone from the WAL; only a re-snapshot that re-reads the folded
///     main DB can still capture them.
///  4. A tiny tail batch B opens a new WAL generation (new salt) touching only
///     the tail page, so A2's earlier pages are never re-imaged by any later
///     incremental.
///  5. The next flush observes the salt/size rollover and MUST re-anchor with a
///     fresh snapshot; otherwise A2's rows are lost forever.
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
    replicator.add(&name, &db_path).await?;

    // 1. Batch A: walrust reads and publishes it. This advances the WAL cursor and
    //    records the current salt, so a later checkpoint is DETECTED as a rollover.
    append_wide_rows(&writer, 6, 40, "raceA")?;
    let a_frames = replicator.flush(&name).await?;
    anyhow::ensure!(a_frames > 0, "batch A should publish an incremental");

    // 2. Batch A2: written on fresh pages but NOT read by walrust yet.
    append_wide_rows(&writer, 41, 120, "raceA2")?;

    // 3. External TRUNCATE folds A+A2 into the main DB and resets the WAL. With no
    //    reader pinning a live frame this MUST succeed (busy==0), destroying A2's
    //    frames in the WAL. Only a re-snapshot of the folded main DB recovers them.
    let (busy, log, ckpt) = force_truncate_checkpoint(&writer)?;
    anyhow::ensure!(
        busy == 0 && ckpt >= log,
        "unpinned external TRUNCATE must reset the WAL (busy={busy}, log={log}, ckpt={ckpt})"
    );

    // 4. Tiny tail batch B: opens a new WAL generation (new salt), touching only the
    //    tail page so A2's earlier leaf pages are never re-imaged by an incremental.
    append_wide_rows(&writer, 121, 125, "raceB")?;

    // 5. This flush observes the rollover and must re-anchor with a fresh snapshot.
    replicator.flush(&name).await?;
    for _ in 0..3 {
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
        "restore should find data after racing checkpoints"
    );
    assert_integrity_ok(&restored_path)?;
    assert_eq!(
        expected,
        rows(&restored_path)?,
        "no data loss across a racing external checkpoint that folded un-read frames"
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
