use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

#[derive(Clone, Default)]
struct WebhookCapture {
    bodies: Arc<Mutex<Vec<String>>>,
}

async fn capture_webhook(
    axum::extract::State(capture): axum::extract::State<WebhookCapture>,
    body: String,
) -> axum::http::StatusCode {
    capture.bodies.lock().unwrap().push(body);
    axum::http::StatusCode::OK
}

async fn start_webhook_capture() -> Result<(String, WebhookCapture, tokio::task::JoinHandle<()>)> {
    let capture = WebhookCapture::default();
    let app = axum::Router::new()
        .route("/webhook", axum::routing::post(capture_webhook))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/webhook", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Ok((url, capture, handle))
}

struct HeldResumeLease;

impl walrust::walrust_core::OwnedResumeLease for HeldResumeLease {
    fn ensure_held(&self) -> Result<()> {
        Ok(())
    }
}

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

/// A writer that never auto-checkpoints. Aggressive auto-checkpointing restarts
/// the WAL (new salt) on nearly every commit, which walrust records as a
/// snapshot rollover — punching a seq gap into the L0 chain per commit. That is
/// exactly the pattern the compaction liveness rule refuses to merge across
/// (each L0 file becomes a lone straddler), so a compaction e2e must drive
/// **contiguous** incrementals: keep the WAL alive and let walrust's own
/// (disabled here) checkpoint timer, not SQLite auto-checkpoint, decide snapshot
/// cadence.
fn open_external_no_autocheckpoint_connection(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")?;
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

struct LoggedWatchArgs<'a> {
    db_path: &'a Path,
    bucket_arg: &'a str,
    endpoint: Option<&'a str>,
    log_path: &'a Path,
    config_path: Option<&'a Path>,
    checkpoint_interval: u64,
    min_checkpoint_pages: u64,
    wal_truncate_threshold: u64,
    native_upload_pause_file: Option<&'a Path>,
}

fn spawn_cli_watch_logged(args: LoggedWatchArgs<'_>) -> Result<Child> {
    let log = std::fs::File::create(args.log_path)?;
    let log_err = log.try_clone()?;
    let mut watch = Command::new(env!("CARGO_BIN_EXE_walrust"));
    if let Some(config_path) = args.config_path {
        watch.arg("--config").arg(config_path);
    }
    watch
        .arg("watch")
        .arg(args.db_path)
        .arg("--bucket")
        .arg(args.bucket_arg)
        .arg("--snapshot-interval")
        .arg("999999")
        .arg("--wal-sync-interval")
        .arg("1")
        .arg("--checkpoint-interval")
        .arg(args.checkpoint_interval.to_string())
        .arg("--min-checkpoint-pages")
        .arg(args.min_checkpoint_pages.to_string())
        .arg("--wal-truncate-threshold")
        .arg(args.wal_truncate_threshold.to_string())
        .arg("--on-startup")
        .arg("true")
        .arg("--no-metrics")
        .arg("--no-cache")
        .stdout(std::process::Stdio::from(log))
        .stderr(std::process::Stdio::from(log_err));
    if let Some(endpoint) = args.endpoint {
        watch.arg("--endpoint").arg(endpoint);
    }
    if let Some(path) = args.native_upload_pause_file {
        watch.env("WALRUST_TEST_NATIVE_UPLOAD_PAUSE_FILE", path);
    }
    watch.spawn().context("spawn logged walrust watch")
}

fn ephemeral_exec(db_path: &Path, sql: &str) -> Result<()> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL;")?;
    conn.execute_batch(sql)?;
    Ok(())
}

fn watch_log(log_path: &Path) -> String {
    std::fs::read_to_string(log_path).unwrap_or_default()
}

fn wait_for_cli_startup_rearms(log_path: &Path, child: &mut Child) -> Result<()> {
    let deadline = Instant::now() + e2e_poll_deadline(30);
    loop {
        let log = watch_log(log_path);
        let snapshot_count = log
            .matches("native HADBP snapshot admitted to durable local spool")
            .count();
        if snapshot_count >= 2 {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            anyhow::bail!(
                "watch exited before both native startup snapshots were locally admitted \
                 ({status}, snapshots={snapshot_count}):\n{log}"
            );
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for both native startup snapshots to be locally admitted \
             (snapshots={snapshot_count}):\n{log}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn assert_no_shadow_reanchors_or_snapshot_storm(log_path: &Path) -> Result<()> {
    let log = watch_log(log_path);
    for forbidden in [
        "Downtime checkpoint detected",
        "Rollover snapshot uploaded",
        "re-anchored before continuing",
        "re-anchoring with a fresh snapshot",
    ] {
        anyhow::ensure!(
            !log.contains(forbidden),
            "lossless shadow watch must not need safety re-anchors ({forbidden:?} found):\n{log}"
        );
    }
    let snapshot_count = log
        .matches("native HADBP snapshot admitted to durable local spool")
        .count();
    // Shadow watch currently takes the explicit on-startup snapshot and the
    // snapshot timer's immediate first tick. That two-snapshot startup baseline
    // predates this test; a third snapshot means the writer triggered a storm.
    anyhow::ensure!(
        snapshot_count <= 2,
        "ephemeral writers caused a snapshot storm beyond the two-snapshot startup baseline \
         ({snapshot_count} snapshots):\n{log}"
    );
    Ok(())
}

fn wait_for_restore_exact(
    name: &str,
    bucket_arg: &str,
    endpoint: Option<&str>,
    restored_path: &Path,
    expected_rows: &[(i64, String)],
) -> Result<()> {
    let deadline = Instant::now() + e2e_poll_deadline(30);
    loop {
        let _ = std::fs::remove_file(restored_path);
        if run_cli_restore(name, bucket_arg, endpoint, restored_path)
            .and_then(|_| rows(restored_path))
            .map(|actual| actual == expected_rows)
            .unwrap_or(false)
        {
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for exact restore"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
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
fn e2e_cli_default_local_checkpoint_does_not_wait_for_live_s3_put() -> Result<()> {
    require_s3!("e2e_cli_default_local_checkpoint_does_not_wait_for_live_s3_put");
    let temp = TempDir::new()?;
    let name = unique_name("cli-native-local-first");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");
    let log_path = temp.path().join("watch.log");
    let pause_file = temp.path().join("pause-native-uploader");
    std::fs::write(&pause_file, b"paused")?;

    let setup = create_source_db(&db_path, 5)?;
    let writer = open_external_no_autocheckpoint_connection(&db_path)?;
    write_pin_frame(&setup, "native-local-first")?;
    let mut child = spawn_cli_watch_logged(LoggedWatchArgs {
        db_path: &db_path,
        bucket_arg: &bucket_arg,
        endpoint: endpoint.as_deref(),
        log_path: &log_path,
        config_path: None,
        checkpoint_interval: 1,
        min_checkpoint_pages: 1,
        wal_truncate_threshold: 100_000,
        native_upload_pause_file: Some(&pause_file),
    })?;
    wait_for_shadow_blocker(&db_path, &mut child)?;
    wait_for_cli_startup_rearms(&log_path, &mut child)?;
    append_wide_rows(&writer, 6, 80, "native-local-first")?;
    let expected = rows(&db_path)?;

    let deadline = Instant::now() + e2e_poll_deadline(30);
    while !watch_log(&log_path)
        .contains("controlled SQLite checkpoint completed after native spool admission")
    {
        if let Some(status) = child.try_wait()? {
            anyhow::bail!(
                "local-first watcher exited before checkpoint ({status}):\n{}",
                watch_log(&log_path)
            );
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for local-first checkpoint:\n{}",
            watch_log(&log_path)
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    let spool_parent = temp.path().join(".walrust-spool/native-v1");
    let spool_root = std::fs::read_dir(&spool_parent)?
        .next()
        .context("native spool stream directory missing")??
        .path();
    let identity = walrust::walrust_core::native_spool::NativeSpool::read_identity(&spool_root)?
        .context("native spool identity missing")?;
    let spool = walrust::walrust_core::native_spool::NativeSpool::create_or_open(
        &spool_root,
        identity,
        walrust::walrust_core::native_spool::CapacityPolicy {
            warning_bytes: u64::MAX - 1,
            hard_bytes: u64::MAX,
            minimum_free_bytes: 0,
        },
    )?;
    anyhow::ensure!(
        spool.objects().count() >= 3,
        "snapshot(s) + delta must be durable locally"
    );
    anyhow::ensure!(
        spool
            .objects()
            .all(|object| object.payload_file_name().ends_with(".hadbp")),
        "default spool must contain only native HADBP payload names"
    );
    drop(spool);

    let runtime = tokio::runtime::Runtime::new()?;
    let native_remote_prefix = format!("{prefix}/{name}/native/v1/");
    let remote_before = runtime.block_on(async {
        let storage = walrust::s3_backend_from_env(test_bucket(), endpoint.as_deref()).await?;
        hadb_storage::StorageBackend::list(storage.as_ref(), &native_remote_prefix, None).await
    })?;
    anyhow::ensure!(
        remote_before.is_empty(),
        "paused live uploader must leave native remote layout absent: {remote_before:?}"
    );
    anyhow::ensure!(
        std::fs::metadata(db_path.with_extension("db-wal"))?.len() < 1024 * 1024,
        "local checkpoint should bound the live WAL while S3 is paused"
    );
    let probe = Connection::open(&db_path)?;
    probe.execute_batch("PRAGMA busy_timeout=0;")?;
    anyhow::ensure!(
        force_truncate_checkpoint(&probe)?.0 != 0,
        "checkpoint blocker must be reacquired while uploader remains paused"
    );

    std::fs::remove_file(&pause_file)?;
    wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected,
    )?;
    assert_integrity_ok(&restored_path)?;
    stop_child(&mut child);

    runtime.block_on(async {
        let storage = walrust::s3_backend_from_env(test_bucket(), endpoint.as_deref()).await?;
        let keys =
            hadb_storage::StorageBackend::list(storage.as_ref(), &format!("{prefix}/"), None)
                .await?;
        for key in keys {
            hadb_storage::StorageBackend::delete(storage.as_ref(), &key).await?;
        }
        Ok::<_, anyhow::Error>(())
    })?;
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
fn e2e_cli_offline_restart_stages_locally_then_reconnects_in_order() -> Result<()> {
    require_s3!("e2e_cli_offline_restart_stages_locally_then_reconnects_in_order");
    let temp = TempDir::new()?;
    let name = unique_name("cli-offline-reconnect");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");
    let first_log = temp.path().join("first.log");
    let offline_log = temp.path().join("offline.log");
    let reconnect_log = temp.path().join("reconnect.log");

    let setup = create_source_db(&db_path, 5)?;
    let writer = open_external_no_autocheckpoint_connection(&db_path)?;
    write_pin_frame(&setup, "offline-base")?;
    let mut first = spawn_cli_watch_logged(LoggedWatchArgs {
        db_path: &db_path,
        bucket_arg: &bucket_arg,
        endpoint: endpoint.as_deref(),
        log_path: &first_log,
        config_path: None,
        checkpoint_interval: 999_999,
        min_checkpoint_pages: 1,
        wal_truncate_threshold: 100_000,
        native_upload_pause_file: None,
    })?;
    wait_for_cli_startup_rearms(&first_log, &mut first)?;
    let initial_rows = rows(&db_path)?;
    wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &initial_rows,
    )?;
    stop_child(&mut first);

    let mut offline = spawn_cli_watch_logged(LoggedWatchArgs {
        db_path: &db_path,
        bucket_arg: &bucket_arg,
        endpoint: Some("http://127.0.0.1:9"),
        log_path: &offline_log,
        config_path: None,
        checkpoint_interval: 999_999,
        min_checkpoint_pages: 1,
        wal_truncate_threshold: 100_000,
        native_upload_pause_file: None,
    })?;
    wait_for_cli_startup_rearms(&offline_log, &mut offline)?;
    append_rows(&writer, 6, 14, "offline-staged")?;
    let offline_deadline = Instant::now() + e2e_poll_deadline(20);
    while !watch_log(&offline_log).contains("native HADBP delta admitted to durable local spool") {
        if let Some(status) = offline.try_wait()? {
            anyhow::bail!("offline watcher exited instead of retaining spool: {status}");
        }
        anyhow::ensure!(
            Instant::now() < offline_deadline,
            "offline watcher did not durably stage the new delta:\n{}",
            watch_log(&offline_log)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    anyhow::ensure!(
        offline.try_wait()?.is_none(),
        "cloud outage must not terminate a watcher with a verified local base"
    );
    stop_child(&mut offline);

    let expected = rows(&db_path)?;
    let mut reconnect = spawn_cli_watch_logged(LoggedWatchArgs {
        db_path: &db_path,
        bucket_arg: &bucket_arg,
        endpoint: endpoint.as_deref(),
        log_path: &reconnect_log,
        config_path: None,
        checkpoint_interval: 999_999,
        min_checkpoint_pages: 1,
        wal_truncate_threshold: 100_000,
        native_upload_pause_file: None,
    })?;
    wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected,
    )?;
    assert_integrity_ok(&restored_path)?;
    stop_child(&mut reconnect);

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let storage = walrust::s3_backend_from_env(test_bucket(), endpoint.as_deref()).await?;
        let keys =
            hadb_storage::StorageBackend::list(storage.as_ref(), &format!("{prefix}/"), None)
                .await?;
        for key in keys {
            hadb_storage::StorageBackend::delete(storage.as_ref(), &key).await?;
        }
        Ok::<_, anyhow::Error>(())
    })?;
    Ok(())
}

/// DF1: short-lived application sessions must never become SQLite's last
/// connection while shadow watch is running. The pinned `_walrust_seq` frame
/// keeps the WAL alive, so the watcher neither dies on WAL lifecycle states nor
/// falls back to a snapshot re-anchor.
#[test]
fn e2e_cli_watch_survives_ephemeral_writer_without_reanchor() -> Result<()> {
    require_s3!("e2e_cli_watch_survives_ephemeral_writer_without_reanchor");
    let temp = TempDir::new()?;
    let name = unique_name("cli-df1-lossless");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");
    let log_path = temp.path().join("watch.log");

    ephemeral_exec(
        &db_path,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
    )?;
    ephemeral_exec(
        &db_path,
        "INSERT INTO items (id, value) VALUES (1, 'base-1');",
    )?;

    let mut child = spawn_cli_watch_logged(LoggedWatchArgs {
        db_path: &db_path,
        bucket_arg: &bucket_arg,
        endpoint: endpoint.as_deref(),
        log_path: &log_path,
        config_path: None,
        checkpoint_interval: 2,
        min_checkpoint_pages: 1,
        wal_truncate_threshold: 100_000,
        native_upload_pause_file: None,
    })?;
    wait_for_shadow_blocker(&db_path, &mut child)?;
    wait_for_cli_startup_rearms(&log_path, &mut child)?;

    for id in 2..=18i64 {
        ephemeral_exec(
            &db_path,
            &format!("INSERT INTO items (id, value) VALUES ({id}, 'ephemeral-{id}');"),
        )?;
        if let Some(status) = child.try_wait()? {
            anyhow::bail!(
                "DF1: watch exited at row {id} with {status}:\n{}",
                watch_log(&log_path)
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    let expected = rows(&db_path)?;
    let restore = wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected,
    );
    stop_child(&mut child);
    restore.with_context(|| format!("DF1 restore failed:\n{}", watch_log(&log_path)))?;
    assert_integrity_ok(&restored_path)?;
    assert_no_shadow_reanchors_or_snapshot_storm(&log_path)?;
    Ok(())
}

/// DF2: wait until the startup snapshot is remotely restorable, then write
/// only through short-lived sessions. The blocker must keep every post-boundary
/// frame in the WAL until shadow watch reads it; recovery by re-snapshot is not
/// accepted.
#[test]
fn e2e_cli_watch_replicates_ephemeral_writes_after_startup_without_reanchor() -> Result<()> {
    require_s3!("e2e_cli_watch_replicates_ephemeral_writes_after_startup_without_reanchor");
    let temp = TempDir::new()?;
    let name = unique_name("cli-df2-lossless");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");
    let log_path = temp.path().join("watch.log");

    ephemeral_exec(
        &db_path,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
    )?;
    for id in 1..=3i64 {
        ephemeral_exec(
            &db_path,
            &format!("INSERT INTO items (id, value) VALUES ({id}, 'day-one-{id}');"),
        )?;
    }
    let day_one = rows(&db_path)?;

    let mut child = spawn_cli_watch_logged(LoggedWatchArgs {
        db_path: &db_path,
        bucket_arg: &bucket_arg,
        endpoint: endpoint.as_deref(),
        log_path: &log_path,
        config_path: None,
        checkpoint_interval: 999_999,
        min_checkpoint_pages: 1,
        wal_truncate_threshold: 100_000,
        native_upload_pause_file: None,
    })?;
    wait_for_shadow_blocker(&db_path, &mut child)?;
    wait_for_restore_exact(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &day_one,
    )
    .with_context(|| format!("startup snapshot did not settle:\n{}", watch_log(&log_path)))?;
    let startup_deadline = Instant::now() + e2e_poll_deadline(30);
    while watch_log(&log_path)
        .matches("native HADBP snapshot admitted to durable local spool")
        .count()
        < 2
    {
        if let Some(status) = child.try_wait()? {
            anyhow::bail!(
                "DF2: watch exited before both startup snapshots completed ({status}):\n{}",
                watch_log(&log_path)
            );
        }
        anyhow::ensure!(
            Instant::now() < startup_deadline,
            "timed out waiting for the two-snapshot startup baseline:\n{}",
            watch_log(&log_path)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    wait_for_cli_startup_rearms(&log_path, &mut child)?;
    let first_writer = Connection::open(&db_path)?;
    first_writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=0;")?;
    first_writer.execute(
        "INSERT INTO items (id, value) VALUES (4, 'post-startup-4')",
        [],
    )?;
    let first_busy = force_truncate_checkpoint(&first_writer)?.0;
    anyhow::ensure!(
        first_busy != 0,
        "DF2: blocker did not stop TRUNCATE after the first post-startup commit:\n{}",
        watch_log(&log_path)
    );
    drop(first_writer);

    for id in 5..=15i64 {
        ephemeral_exec(
            &db_path,
            &format!("INSERT INTO items (id, value) VALUES ({id}, 'post-startup-{id}');"),
        )?;
        if let Some(status) = child.try_wait()? {
            anyhow::bail!(
                "DF2: watch exited at row {id} with {status}:\n{}",
                watch_log(&log_path)
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    let wal_path = db_path.with_extension("db-wal");
    let wal_size = std::fs::metadata(&wal_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    anyhow::ensure!(
        wal_size > 32,
        "DF2: post-boundary short-lived writes did not remain in the WAL (size={wal_size}):\n{}",
        watch_log(&log_path)
    );
    let checkpoint_probe = Connection::open(&db_path)?;
    checkpoint_probe.execute_batch("PRAGMA busy_timeout=0;")?;
    let busy = force_truncate_checkpoint(&checkpoint_probe)?.0;
    anyhow::ensure!(
        busy != 0,
        "DF2: watcher no longer held its checkpoint blocker after startup snapshots:\n{}",
        watch_log(&log_path)
    );

    let expected = rows(&db_path)?;
    let restore = wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected,
    );
    stop_child(&mut child);
    restore.with_context(|| format!("DF2 restore failed:\n{}", watch_log(&log_path)))?;
    assert_eq!(expected.len(), 15);
    assert_integrity_ok(&restored_path)?;
    assert_no_shadow_reanchors_or_snapshot_storm(&log_path)?;
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

/// Public embedder path against real object storage: owned snapshot + WAL,
/// restore to a fresh directory, adopt/re-anchor, mutate the restored database,
/// then restore again. Wide values, blobs, DELETE, UPDATE, and DDL force the
/// test across overflow pages and page-layout changes that tiny row tests miss.
#[tokio::test]
async fn e2e_core_owned_restore_resume_round_trips_mixed_workload() -> Result<()> {
    require_s3!("e2e_core_owned_restore_resume_round_trips_mixed_workload");
    let source_dir = TempDir::new()?;
    let restored_dir = TempDir::new()?;
    let verify_dir = TempDir::new()?;
    let name = unique_name("owned-resume-e2e");
    let prefix = format!("e2e/{name}/");
    let source_path = source_dir.path().join(format!("{name}.db"));
    let restored_path = restored_dir.path().join(format!("{name}.db"));
    let verify_path = verify_dir.path().join(format!("{name}.db"));

    let source_writer = create_source_db(&source_path, 8)?;
    source_writer.execute_batch(
        "CREATE TABLE blobs (id INTEGER PRIMARY KEY, value BLOB NOT NULL);
         INSERT INTO blobs VALUES (1, randomblob(196608));",
    )?;

    let storage = walrust::s3_backend_from_env(test_bucket(), test_endpoint().as_deref()).await?;
    let retry = walrust::walrust_core::RetryPolicy::default_policy();
    let mut source_state = walrust::walrust_core::SyncState::new(source_path.clone())?;
    source_state.name = name.clone();
    walrust::walrust_core::sync::take_snapshot_with_retry(
        storage.as_ref(),
        &prefix,
        &mut source_state,
        &retry,
    )
    .await?;
    walrust::walrust_core::sync::save_state(storage.as_ref(), &prefix, &source_state).await?;

    append_wide_rows(&source_writer, 9, 80, "before-restore")?;
    source_writer.execute_batch(
        "UPDATE items SET value = 'updated-before' WHERE id = 2;
         DELETE FROM items WHERE id = 3;
         UPDATE blobs SET value = randomblob(262144) WHERE id = 1;",
    )?;
    anyhow::ensure!(
        walrust::walrust_core::sync::sync_wal_with_retry(
            storage.as_ref(),
            &prefix,
            &mut source_state,
            &retry,
        )
        .await?
            > 0,
        "pre-restore WAL sync must publish frames"
    );

    let restored =
        walrust::walrust_core::sync::restore(storage.clone(), &prefix, &name, &restored_path, None)
            .await?;
    drop(source_writer);
    drop(source_state);

    let mut resumed = walrust::walrust_core::SyncState::new(restored_path.clone())?;
    resumed.name = name.clone();
    walrust::walrust_core::sync::resume_owned_after_restore(
        storage.as_ref(),
        &prefix,
        &mut resumed,
        &restored,
        &HeldResumeLease,
        &retry,
    )
    .await?;

    let resumed_writer = Connection::open(&restored_path)?;
    resumed_writer.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;
         ALTER TABLE items ADD COLUMN note TEXT;
         UPDATE items SET note = 'migrated' WHERE id <= 10;
         INSERT INTO items (id, value, note) VALUES (1000, 'after-restore', 'new');
         INSERT INTO blobs VALUES (2, randomblob(327680));",
    )?;
    anyhow::ensure!(
        walrust::walrust_core::sync::sync_wal_with_retry(
            storage.as_ref(),
            &prefix,
            &mut resumed,
            &retry,
        )
        .await?
            > 0,
        "post-resume WAL sync must publish frames"
    );

    let final_restore =
        walrust::walrust_core::sync::restore(storage, &prefix, &name, &verify_path, None).await?;
    anyhow::ensure!(
        final_restore.seq() == resumed.current_seq,
        "second restore stopped at seq {}, expected {}",
        final_restore.seq(),
        resumed.current_seq
    );
    assert_integrity_ok(&verify_path)?;
    assert_eq!(rows(&verify_path)?, rows(&restored_path)?);

    let verified = Connection::open(&verify_path)?;
    let blob_bytes: i64 =
        verified.query_row("SELECT SUM(length(value)) FROM blobs", [], |row| row.get(0))?;
    assert_eq!(blob_bytes, 262144 + 327680);
    let note: String = verified.query_row("SELECT note FROM items WHERE id = 1000", [], |row| {
        row.get(0)
    })?;
    assert_eq!(note, "new");

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

/// An application connection issues explicit TRUNCATE checkpoints underneath a
/// live CLI shadow watch. Every attempt must be blocked by walrust's pinned real
/// WAL frame, and restore must contain every committed row without a re-anchor.
#[test]
fn e2e_cli_watch_blocks_app_checkpoint_underneath_without_data_loss() -> Result<()> {
    require_s3!("e2e_cli_watch_blocks_app_checkpoint_underneath_without_data_loss");
    let temp = TempDir::new()?;
    let name = unique_name("cli-race");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");
    let log_path = temp.path().join("watch.log");

    let setup = create_source_db(&db_path, 5)?;
    let writer = open_external_autocheckpoint_connection(&db_path)?;
    write_pin_frame(&setup, "cli-race")?;

    // Deliberately no test-owned read pin: only walrust may block the app.
    let mut child = spawn_cli_watch_logged(LoggedWatchArgs {
        db_path: &db_path,
        bucket_arg: &bucket_arg,
        endpoint: endpoint.as_deref(),
        log_path: &log_path,
        config_path: None,
        checkpoint_interval: 999_999,
        min_checkpoint_pages: 1,
        wal_truncate_threshold: 100_000,
        native_upload_pause_file: None,
    })?;
    // Wait for the blocker to actually attach. A fixed sleep races the watcher's
    // startup (S3 discovery + initial snapshot); if the blocker is not yet up, the
    // "race" races nothing and the test proves nothing (this is exactly why the
    // original fixed-2s version passed vacuously — the pin was not up).
    wait_for_shadow_blocker(&db_path, &mut child)?;
    wait_for_cli_startup_rearms(&log_path, &mut child)?;

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
        busy_results.iter().all(|&b| b != 0),
        "every app TRUNCATE must be blocked for the watch lifetime \
         (busy results = {busy_results:?})"
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
    assert_no_shadow_reanchors_or_snapshot_storm(&log_path)?;
    Ok(())
}

/// Crossing the configured WAL ceiling is never silent: the real CLI watch
/// loop must emit both an ERROR log and a `wal_size_exceeded` webhook, durably
/// ship the shadow tail, run a controlled TRUNCATE, reacquire the blocker, and
/// remain fully restorable.
#[test]
fn e2e_cli_watch_wal_backpressure_alarms_and_preserves_data() -> Result<()> {
    require_s3!("e2e_cli_watch_wal_backpressure_alarms_and_preserves_data");
    let temp = TempDir::new()?;
    let name = unique_name("cli-wal-backpressure");
    let prefix = format!("e2e/{name}");
    let bucket_arg = format!("{}/{}", test_bucket(), prefix);
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");
    let log_path = temp.path().join("watch.log");
    let config_path = temp.path().join("walrust.toml");

    let runtime = tokio::runtime::Runtime::new()?;
    let (webhook_url, capture, webhook_handle) = runtime.block_on(start_webhook_capture())?;
    std::fs::write(
        &config_path,
        format!("[[webhooks]]\nurl = {webhook_url:?}\nevents = [\"wal_size_exceeded\"]\n"),
    )?;

    let setup = create_source_db(&db_path, 5)?;
    let writer = open_external_no_autocheckpoint_connection(&db_path)?;
    write_pin_frame(&setup, "wal-backpressure")?;
    let mut child = spawn_cli_watch_logged(LoggedWatchArgs {
        db_path: &db_path,
        bucket_arg: &bucket_arg,
        endpoint: endpoint.as_deref(),
        log_path: &log_path,
        config_path: Some(&config_path),
        checkpoint_interval: 999_999,
        min_checkpoint_pages: 1,
        wal_truncate_threshold: 20,
        native_upload_pause_file: None,
    })?;
    wait_for_shadow_blocker(&db_path, &mut child)?;
    wait_for_cli_startup_rearms(&log_path, &mut child)?;

    // At ~400 bytes per row this reliably crosses the 20-page ceiling even
    // with the schema/index pages compacted efficiently by SQLite.
    append_wide_rows(&writer, 6, 190, "backpressure")?;
    let expected = rows(&db_path)?;
    let deadline = Instant::now() + e2e_poll_deadline(30);
    loop {
        if !capture.bodies.lock().unwrap().is_empty() {
            break;
        }
        if let Some(status) = child.try_wait()? {
            anyhow::bail!(
                "watch exited before WAL backpressure alarm with {status}:\n{}",
                watch_log(&log_path)
            );
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "timed out waiting for wal_size_exceeded webhook:\n{}",
            watch_log(&log_path)
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    let body = capture.bodies.lock().unwrap()[0].clone();
    let payload: serde_json::Value = serde_json::from_str(&body)?;
    anyhow::ensure!(payload["event"] == "wal_size_exceeded", "{payload:?}");
    anyhow::ensure!(payload["database"] == name, "{payload:?}");
    anyhow::ensure!(payload["context"]["wal_pages"].as_u64().unwrap_or(0) >= 20);
    anyhow::ensure!(
        watch_log(&log_path).contains("WAL backpressure alarm"),
        "threshold must also produce an ERROR log:\n{}",
        watch_log(&log_path)
    );

    writer.execute_batch("PRAGMA busy_timeout=0;")?;
    let busy = force_truncate_checkpoint(&writer)?.0;
    anyhow::ensure!(
        busy != 0,
        "controlled backpressure checkpoint must reacquire the blocker"
    );

    let restore = wait_for_cli_restore_rows(
        &name,
        &bucket_arg,
        endpoint.as_deref(),
        &restored_path,
        &expected,
    );
    stop_child(&mut child);
    webhook_handle.abort();
    restore.with_context(|| {
        format!(
            "restore after WAL backpressure checkpoint failed:\n{}",
            watch_log(&log_path)
        )
    })?;
    assert_integrity_ok(&restored_path)?;
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
    // A compaction e2e needs a **contiguous** L0 chain to fold up the levels, so
    // the writer must not auto-checkpoint (which would restart the WAL / roll the
    // salt and make walrust punch a snapshot seq-gap per commit — every L0 a lone
    // straddler the liveness rule correctly refuses to merge). walrust's own
    // snapshot/checkpoint timers are pinned to 999999s here, so the only snapshot
    // is the on-startup base and every later sync is a chaining incremental.
    let writer = open_external_no_autocheckpoint_connection(&db_path)?;
    write_pin_frame(&setup, "cli-compaction")?;

    let mut child =
        spawn_cli_watch_compaction(&db_path, &config_path, &bucket_arg, endpoint.as_deref())?;
    std::thread::sleep(Duration::from_secs(2));

    // Drive ~16 separate sync cycles so the L0 pool fills L1 (batch 4) three
    // times and then L2 (batch 2); one commit per >1s poll window == one L0.
    // We track whether L1 was **ever** observed (not whether it is present at the
    // end): the L1→L2 merge deletes its L1 sources, so at the instant L2 first
    // appears L1 can legitimately be empty. "L1 was seen" ∧ "L2 fired" is the
    // race-free proof that the full L0→L1→L2 cascade ran.
    let mut next_id = 100i64;
    let l2_deadline = Instant::now() + e2e_poll_deadline(90);
    let mut fired_l2 = false;
    let mut seen_l1 = false;
    for _ in 0..24 {
        append_rows(&writer, next_id, next_id + 1, "compact")?;
        next_id += 2;
        std::thread::sleep(Duration::from_millis(1200));
        if !list_keys(&l1_prefix).await?.is_empty() {
            seen_l1 = true;
        }
        if !list_keys(&l2_prefix).await?.is_empty() {
            fired_l2 = true;
            break;
        }
        if Instant::now() >= l2_deadline {
            break;
        }
    }
    assert!(fired_l2, "L2 must fire within the deadline");

    // The superseded L0 objects were deleted by the engine, so the L0 pool is far
    // smaller than the ~16+ incrementals produced. L2's presence already proves
    // L1 fired (L2 can only be built from L1 sources); `seen_l1` additionally
    // pins that an L1 object was materialized on the wire.
    let l1 = list_keys(&l1_prefix).await?;
    let l2 = list_keys(&l2_prefix).await?;
    let l0 = list_keys(&l0_prefix).await?;
    assert!(
        seen_l1 || !l1.is_empty(),
        "L1 must have fired (seen during the run or still present): {l1:?}"
    );
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
        "PITR to the L2 boundary txid {l2_max} must succeed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_integrity_ok(&boundary_out)?;

    // PITR strictly inside the L2 window is the loud typed decay error. walrust's
    // tracing subscriber logs to stdout (not stderr), so the typed error surfaces
    // on stdout; assert on the combined streams so the check is stream-agnostic.
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
        let stderr = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
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

// ── Compaction embedder crash e2e (gap 4) ──────────────────────────────────

/// Aggressive compaction settings for the embedder crash e2e. `sync_interval`
/// is fast so the background sync/compaction loop `Replicator::new` spawns
/// internally (see `Replicator::run_loop_weak` / `sync_all`) ticks often;
/// `l1_batch`/`l2_batch`/`keep_fine_window` match the CLI compaction e2e's
/// aggressive knobs so L1 and L2 fire within a short write burst.
/// `snapshot_interval` is short (not the usual 3600s default): the child
/// helper below deliberately registers via `add_without_snapshot` on every
/// phase, never `add`, so the walrust-owned stream never gets a
/// `lineage_id` (see `SyncState::ensure_lineage_id`, only called from
/// `Replicator::add_with_wal_path`) -- compaction's `SeqLayout` only ever
/// reads/writes the flat non-lineage `{db}/0000/{seq}.hadbp` key shape
/// (crates/walrust-core/src/sync.rs: "Compaction only ever runs on the
/// non-lineage owned path"), so a lineage-scoped stream is invisible to it
/// and compaction silently never fires. With no `add()`-time snapshot, the
/// background loop's own periodic snapshot timer must establish the first
/// restorable base, so it needs to fire soon, not after an hour.
fn core_compaction_replicator_config() -> walrust::walrust_core::ReplicationConfig {
    walrust::walrust_core::ReplicationConfig {
        sync_interval: Duration::from_millis(150),
        snapshot_interval: Duration::from_secs(4),
        compaction: walrust::walrust_core::compaction::CompactionSettings {
            enabled: true,
            keep_fine_window: Duration::ZERO,
            l1_batch: 4,
            l2_batch: 3,
        },
        ..Default::default()
    }
}

struct CompactionSigkillHelperArgs<'a> {
    phase: &'a str,
    name: &'a str,
    prefix: &'a str,
    bucket: &'a str,
    endpoint: Option<&'a str>,
    db_path: &'a Path,
    ready_path: &'a Path,
}

fn spawn_compaction_sigkill_helper(args: CompactionSigkillHelperArgs<'_>) -> Result<Child> {
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.arg("--exact")
        .arg("e2e_core_replicator_compaction_embedder_crash_child")
        .arg("--ignored")
        .arg("--nocapture")
        .env("WALRUST_COMPACTION_SIGKILL_PHASE", args.phase)
        .env("WALRUST_COMPACTION_SIGKILL_NAME", args.name)
        .env("WALRUST_COMPACTION_SIGKILL_PREFIX", args.prefix)
        .env("WALRUST_COMPACTION_SIGKILL_BUCKET", args.bucket)
        .env("WALRUST_COMPACTION_SIGKILL_DB", args.db_path)
        .env("WALRUST_COMPACTION_SIGKILL_READY", args.ready_path);
    if let Some(endpoint) = args.endpoint {
        cmd.env("WALRUST_COMPACTION_SIGKILL_ENDPOINT", endpoint);
    }
    cmd.spawn()
        .context("spawn compaction embedder SIGKILL helper")
}

/// The wave's embedder proof (gap 4): a real `Replicator` with
/// `ReplicationConfig::compaction` enabled, embedded in a spawned child
/// process that writes continuously. The parent SIGKILLs it mid-merge
/// activity (confirmed via a real S3 listing, not a fixed sleep), respawns
/// it, and repeats -- two kill/respawn cycles across three total child
/// phases (first, second get SIGKILLed; third finishes cleanly). The parent
/// then restores VIA THE LIBRARY API (`Replicator::restore`, not the CLI) to
/// a fresh path with a fresh `Replicator` instance, and asserts row-exact
/// (against the child's own persisted, on-disk ground truth -- `db_path`
/// itself, which survives every respawn) + `integrity_check` + that levels
/// actually fired (merged L1/L2 objects present in the bucket).
#[test]
fn e2e_core_replicator_compaction_embedder_crash() -> Result<()> {
    require_s3!("e2e_core_replicator_compaction_embedder_crash");
    let temp = TempDir::new()?;
    let name = unique_name("core-compaction-crash");
    let prefix = format!("e2e/{name}/");
    let bucket = test_bucket();
    let endpoint = test_endpoint();
    let db_path = temp.path().join(format!("{name}.db"));
    let restored_path = temp.path().join("restored.db");
    let ready_path = temp.path().join("compaction-child-ready");

    let _setup = create_source_db(&db_path, 5)?;
    let l1_prefix = format!("{prefix}{name}/levels/L1/");
    let l2_prefix = format!("{prefix}{name}/levels/L2/");

    let runtime = tokio::runtime::Runtime::new()?;

    for phase in ["first", "second", "third"] {
        let _ = std::fs::remove_file(&ready_path);
        let mut child = spawn_compaction_sigkill_helper(CompactionSigkillHelperArgs {
            phase,
            name: &name,
            prefix: &prefix,
            bucket: &bucket,
            endpoint: endpoint.as_deref(),
            db_path: &db_path,
            ready_path: &ready_path,
        })?;
        wait_for_file_or_child_exit(
            &mut child,
            &ready_path,
            &format!("{phase} compaction helper startup"),
        )?;

        if phase == "third" {
            // Final phase: let it finish its write burst, flush, and exit
            // cleanly on its own -- no kill.
            let status = child.wait()?;
            anyhow::ensure!(
                status.success(),
                "third compaction embedder helper failed with {status}"
            );
        } else {
            // Kill it mid-merge activity for real: poll S3 for a merged L1
            // object rather than sleeping a fixed amount, so the SIGKILL
            // actually lands while compaction is active, not before it has
            // started or after the child's write burst already finished.
            let deadline = Instant::now() + e2e_poll_deadline(60);
            loop {
                let l1_seen = runtime.block_on(list_keys(&l1_prefix))?;
                if !l1_seen.is_empty() {
                    break;
                }
                if let Some(status) = child.try_wait()? {
                    anyhow::bail!("{phase} compaction helper exited early ({status}) before any L1 merge was observed");
                }
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "{phase}: timed out waiting for a merged L1 object before killing"
                );
                std::thread::sleep(Duration::from_millis(200));
            }
            stop_child(&mut child); // SIGKILL, mid-merge.
        }
    }

    // Levels actually fired: assert merged objects exist in the bucket
    // (checked again here, post-crash-cycle, so the assertion covers the
    // final converged state, not just the mid-run snapshot that triggered
    // the first kill).
    let (l1_keys, l2_keys) = runtime.block_on(async {
        let l1 = list_keys(&l1_prefix).await?;
        let l2 = list_keys(&l2_prefix).await?;
        Ok::<_, anyhow::Error>((l1, l2))
    })?;
    anyhow::ensure!(
        !l1_keys.is_empty() || !l2_keys.is_empty(),
        "compaction never produced a merged level object across the crash cycles: L1={l1_keys:?} L2={l2_keys:?}"
    );

    // Restore VIA THE LIBRARY API (not the CLI) to a fresh path, with a fresh
    // `Replicator` instance unrelated to any of the crashed child processes.
    runtime.block_on(async {
        let storage = walrust::s3_backend_from_env(bucket.clone(), endpoint.as_deref()).await?;
        let replicator = walrust::walrust_core::Replicator::new(
            storage,
            &prefix,
            core_compaction_replicator_config(),
        );
        let restored_seq = replicator.restore(&name, &restored_path).await?;
        anyhow::ensure!(
            restored_seq.is_some(),
            "library restore should find the compacted, crash-cycled stream"
        );
        Ok::<_, anyhow::Error>(())
    })?;

    assert_integrity_ok(&restored_path)?;
    // Ground truth is the child's own on-disk database: `db_path` survives
    // every respawn (same file across all three phases), so its final
    // content after the "third" phase exits cleanly IS the persisted ground
    // truth the task asked for.
    assert_eq!(
        rows(&db_path)?,
        rows(&restored_path)?,
        "library restore across compaction + repeated SIGKILL crashes must be row-exact"
    );

    Ok(())
}

/// Coordination-driven spawn target for the compaction embedder crash e2e --
/// NOT a standalone test. See the doc comment on
/// `e2e_core_replicator_sigkill_child` for why this pattern (re-exec the same
/// test binary with `--exact ... --ignored`) is the right shape and why
/// `#[ignore]` does not exclude it from CI: the parent
/// `e2e_core_replicator_compaction_embedder_crash` is `require_s3!`-gated, so
/// it runs against MinIO in CI, and it re-execs this exact function three
/// times per run (two of which get SIGKILLed by the parent, matching the
/// production crash scenario under test).
///
/// Registers with a real `Replicator` via `add_without_snapshot` on EVERY
/// phase (including the first) -- unlike `e2e_core_replicator_sigkill_child`,
/// which can use `add` because it does not enable compaction. Here `add` is
/// unusable: with `compaction.enabled = true` it refuses (it would create a
/// lineage compaction cannot see), so the lineage-free `add_without_snapshot`
/// path is the one under test (see `core_compaction_replicator_config`).
/// After registering it signals readiness, then writes a
/// continuous burst of small commits through an external, non-autocheckpoint
/// connection so the replicator's own background sync/compaction loop has
/// many chances to fire (aggressive `l1_batch`/`l2_batch`/`keep_fine_window`
/// via `core_compaction_replicator_config`). The "third" phase additionally
/// flushes explicitly before exiting cleanly, so its on-disk `db_path` state
/// is durably synced before the parent treats it as ground truth.
#[test]
#[ignore = "spawn target of e2e_core_replicator_compaction_embedder_crash; \
            runs in CI when the parent re-execs it with --ignored (see doc comment)"]
fn e2e_core_replicator_compaction_embedder_crash_child() -> Result<()> {
    let phase = std::env::var("WALRUST_COMPACTION_SIGKILL_PHASE")?;
    let name = std::env::var("WALRUST_COMPACTION_SIGKILL_NAME")?;
    let prefix = std::env::var("WALRUST_COMPACTION_SIGKILL_PREFIX")?;
    let bucket = std::env::var("WALRUST_COMPACTION_SIGKILL_BUCKET")?;
    let endpoint = std::env::var("WALRUST_COMPACTION_SIGKILL_ENDPOINT").ok();
    let db_path = std::path::PathBuf::from(std::env::var("WALRUST_COMPACTION_SIGKILL_DB")?);
    let ready_path = std::path::PathBuf::from(std::env::var("WALRUST_COMPACTION_SIGKILL_READY")?);

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let storage = walrust::s3_backend_from_env(bucket, endpoint.as_deref()).await?;
        let replicator = walrust::walrust_core::Replicator::new(
            storage,
            &prefix,
            core_compaction_replicator_config(),
        );
        // ALWAYS add_without_snapshot, even on "first" -- see the doc comment
        // on core_compaction_replicator_config for why: Replicator::add()
        // unconditionally creates a lineage, which compaction cannot see.
        // On "first" there is no saved state.json yet, so this just
        // registers the (already 5-row) on-disk db and lets the background
        // loop's own periodic snapshot timer take the first base snapshot.
        replicator.add_without_snapshot(&name, &db_path).await?;
        std::fs::write(&ready_path, b"ready")?;

        // Resume row IDs from wherever the on-disk db (shared across every
        // respawn) currently stands.
        let mut next_id = {
            let conn = Connection::open(&db_path)?;
            let max_id: i64 =
                conn.query_row("SELECT COALESCE(MAX(id), 0) FROM items", [], |r| r.get(0))?;
            max_id + 1
        };

        let writer = open_external_no_autocheckpoint_connection(&db_path)?;
        // ~60 commits * 120ms ~= 7.2s of sustained writes per phase, against
        // a 150ms background sync tick -- dozens of sync opportunities, and
        // with l1_batch=4 several L0->L1 merges (and often L1->L2) per phase.
        for _ in 0..60 {
            append_rows(
                &writer,
                next_id,
                next_id + 1,
                &format!("compact-embed-{phase}"),
            )?;
            next_id += 2;
            tokio::time::sleep(Duration::from_millis(120)).await;
        }

        if phase == "third" {
            // Ensure durability before exiting cleanly: explicit flush(es)
            // beyond whatever the background timer already picked up, so the
            // parent's row-exact comparison against this on-disk db isn't
            // racing an unsynced tail. (walrust's own restore then proves
            // this durability independently, via S3, not via this flush.)
            let _ = flush_until_frames(&replicator, &name, "third compaction helper").await;
            let _ = replicator.flush(&name).await;
        }

        if phase != "third" {
            // Sleep forever -- the parent SIGKILLs this process once it has
            // observed real merge activity in S3. Reaching this point without
            // being killed (e.g. under a very slow endpoint) is still safe:
            // the parent's poll loop has its own deadline and will fail
            // loudly rather than hang.
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
        Ok::<_, anyhow::Error>(())
    })
}
