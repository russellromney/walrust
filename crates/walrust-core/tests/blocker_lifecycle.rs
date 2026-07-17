//! Checkpoint-blocker lifecycle regression tests (real SQLite, external child
//! processes).
//!
//! Root cause these pin: the checkpoint blocker's protection is a pinned WAL
//! read mark (locks on the `-shm` inode) PLUS a SHARED POSIX lock on the main
//! database inode, held by the blocker connection for its whole lifetime. The
//! SHARED lock is the only thing stopping another process's last-connection
//! close from taking the EXCLUSIVE main-db lock its close-time checkpoint needs
//! before it unlinks the `-wal`/`-shm` files. Classic POSIX semantics: closing
//! ANY descriptor the process holds for that inode releases ALL its locks — so
//! one stray open/close of the main DB (page-size read, change counter, VACUUM
//! INTO snapshot connection) silently destroyed the protection while the
//! blocker object still appeared alive. The retained-handles lifecycle
//! (`blocker::BlockerLifecycle`) keeps every main-db descriptor for the watch
//! lifetime.
//!
//! The child is a separate OS process because POSIX advisory locks are
//! process-scoped: the blocker only protects against other processes if the
//! locks it holds are still in force when the child runs.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use rusqlite::Connection;

use walrust_core::blocker::BlockerLifecycle;
use walrust_core::legacy_wal_sync::{take_snapshot_to_storage, SyncInput};
use walrust_core::replicator::Replicator;
use walrust_core::shadow::ShadowWal;
use walrust_core::sync::ReplicationConfig;

// ── In-memory storage (the S3 PUT is not part of the descriptor lifecycle) ──

#[derive(Default)]
struct MemStorage {
    map: std::sync::Mutex<BTreeMap<String, Vec<u8>>>,
}

#[async_trait]
impl StorageBackend for MemStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.map
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.map.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let map = self.map.lock().unwrap();
        Ok(map
            .keys()
            .filter(|k| k.starts_with(prefix) && after.is_none_or(|a| k.as_str() > a))
            .cloned()
            .collect())
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let mut map = self.map.lock().unwrap();
        if map.contains_key(key) {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        map.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".to_string()),
        })
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        let mut map = self.map.lock().unwrap();
        let exists = map.contains_key(key);
        if !exists || etag != "mem" {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        map.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".to_string()),
        })
    }
}

// ── External child process ──────────────────────────────────────────────────
//
// Re-execs this test binary running only `child_worker`, parameterized through
// env vars. A real child process is required: POSIX fcntl locks are
// process-scoped, so a same-process connection can never prove cross-process
// protection.
#[test]
#[ignore]
fn child_worker() {
    let db = std::env::var("REPRO_DB").expect("REPRO_DB");
    let mode = std::env::var("REPRO_MODE").expect("REPRO_MODE");
    let value = std::env::var("REPRO_VALUE").unwrap_or_default();

    let conn = Connection::open(&db).expect("child open");
    // Non-blocking: the raw busy state must be observable, not retried away.
    conn.execute_batch("PRAGMA busy_timeout=0;").unwrap();
    match mode.as_str() {
        "commit" => {
            conn.execute("INSERT INTO app_data (value) VALUES (?1)", [value])
                .expect("child commit");
            // Drop triggers the last-connection close-time checkpoint attempt.
            println!("CHILD_RESULT committed");
        }
        "commit_and_truncate" => {
            conn.execute("INSERT INTO app_data (value) VALUES (?1)", [value])
                .expect("child commit");
            match conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            }) {
                Ok((busy, log, ckpt)) => {
                    println!("CHILD_RESULT busy={busy} log={log} ckpt={ckpt}")
                }
                Err(e) => println!("CHILD_RESULT error={e}"),
            }
        }
        other => panic!("unknown child mode {other}"),
    }
}

struct ChildOutcome {
    raw: String,
}

impl ChildOutcome {
    /// Parse `busy=N` out of the child's CHILD_RESULT line, if present.
    fn busy(&self) -> Option<i64> {
        self.raw
            .split_whitespace()
            .find_map(|tok| tok.strip_prefix("busy=")?.parse().ok())
    }
}

fn run_child(db_path: &Path, mode: &str, value: &str) -> ChildOutcome {
    let exe = std::env::current_exe().unwrap();
    let out = Command::new(exe)
        .arg("child_worker")
        .arg("--ignored")
        .arg("--exact")
        .arg("--nocapture")
        .env("REPRO_DB", db_path)
        .env("REPRO_MODE", mode)
        .env("REPRO_VALUE", value)
        .output()
        .expect("spawn child");
    assert!(
        out.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = stdout
        .lines()
        .find(|l| l.starts_with("CHILD_RESULT"))
        .unwrap_or_else(|| panic!("child produced no CHILD_RESULT: {stdout}"));
    ChildOutcome {
        raw: line.to_string(),
    }
}

// ── Shared fixtures ─────────────────────────────────────────────────────────

fn fresh_db(dir: &Path, name: &str) -> PathBuf {
    let db_path = dir.join(name);
    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE app_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO app_data (value) VALUES ('seed');",
    )
    .unwrap();
    drop(conn);
    db_path
}

#[derive(Debug)]
struct FileState {
    exists: bool,
    len: u64,
    ino: u64,
}

fn file_state(path: &Path) -> FileState {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(path) {
        Ok(m) => FileState {
            exists: true,
            len: m.len(),
            ino: m.ino(),
        },
        Err(_) => FileState {
            exists: false,
            len: 0,
            ino: 0,
        },
    }
}

fn wal_state(db_path: &Path) -> FileState {
    file_state(&db_path.with_extension("db-wal"))
}

fn shm_state(db_path: &Path) -> FileState {
    file_state(&db_path.with_extension("db-shm"))
}

fn wal_len(db_path: &Path) -> Option<u64> {
    std::fs::metadata(db_path.with_extension("db-wal"))
        .ok()
        .map(|m| m.len())
}

fn cli_snapshot_input(db_path: &Path) -> SyncInput {
    SyncInput {
        db_path: db_path.to_path_buf(),
        name: "app".into(),
        wal_path: db_path.with_extension("db-wal"),
        wal_offset: 0,
        wal_generation: 0,
        current_txid: 0,
        db_checksum: None,
        wal_salt: None,
        wal_checksum_chain: None,
    }
}

/// Assert the blocker contract from the child's point of view: TRUNCATE was
/// refused AND the WAL (unread frames included) survived.
fn assert_external_truncate_blocked(outcome: &ChildOutcome, db_path: &Path, ctx: &str) {
    assert_eq!(
        outcome.busy(),
        Some(1),
        "{ctx}: external wal_checkpoint(TRUNCATE) must report busy while the \
         blocker is armed; child said: {}",
        outcome.raw
    );
    let len = wal_len(db_path).unwrap_or(0);
    assert!(
        len > 0,
        "{ctx}: WAL must survive the refused TRUNCATE (unread frames must not \
         disappear); child said: {}",
        outcome.raw
    );
}

// ── Reproduction tests ──────────────────────────────────────────────────────

/// CLI shadow watch: `ShadowWal::new` arms the blocker at watch startup; the
/// startup snapshot then runs the exact production chain
/// (`take_snapshot_to_storage`: PASSIVE checkpoint conn + raw page-size read +
/// VACUUM-INTO snapshot conn). An external TRUNCATE after that lifecycle must
/// still be blocked.
#[tokio::test]
async fn cli_startup_snapshot_does_not_invalidate_blocker() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "cli.db");

    // 1. Arm the existing blocker exactly as CLI watch startup does.
    let _shadow = ShadowWal::new(&db_path).await.unwrap();

    // 2. External app commits past the blocker's mark (unread frames).
    run_child(&db_path, "commit", "after-blocker");
    assert!(
        wal_len(&db_path).unwrap_or(0) > 0,
        "setup: WAL must hold unread frames"
    );

    // 3. Exact CLI startup-snapshot lifecycle (production call chain), borrowing
    // the retained handles like the watch loop does.
    let storage = MemStorage::default();
    take_snapshot_to_storage(
        &storage,
        "",
        cli_snapshot_input(&db_path),
        _shadow.lifecycle(),
    )
    .await
    .unwrap();

    // 4. External app commits more, then issues wal_checkpoint(TRUNCATE).
    let outcome = run_child(&db_path, "commit_and_truncate", "after-snapshot");
    assert_external_truncate_blocked(&outcome, &db_path, "cli startup snapshot");
}

/// Owned mode: `Replicator::add()` arms the blocker and runs the owned
/// HADBP snapshot dance (release/TRUNCATE/reacquire + raw main-db page reads).
/// An external TRUNCATE after that lifecycle must still be blocked.
#[tokio::test]
async fn owned_replicator_add_does_not_invalidate_blocker() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "owned.db");

    let storage: Arc<dyn StorageBackend> = Arc::new(MemStorage::default());
    let config = ReplicationConfig {
        // Keep the background loop out of the measurement.
        sync_interval: std::time::Duration::from_secs(3600),
        snapshot_interval: std::time::Duration::from_secs(3600),
        ..ReplicationConfig::default()
    };
    let replicator = Replicator::new(storage, "repro/", config);

    // Owned add(): arms the blocker and runs the full snapshot lifecycle.
    replicator.add("owned", &db_path).await.unwrap();

    let outcome = run_child(&db_path, "commit_and_truncate", "after-add");
    assert_external_truncate_blocked(&outcome, &db_path, "owned replicator add");
}

/// DF2 shape: repeated short-lived writers (one process per commit, each
/// triggering SQLite's last-connection close-time checkpoint attempt) must
/// never reset or truncate the WAL while the blocker holds unread frames.
#[tokio::test]
async fn repeated_short_lived_writers_cannot_reset_unread_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "df2.db");

    let _shadow = ShadowWal::new(&db_path).await.unwrap();

    for i in 0..6 {
        run_child(&db_path, "commit", &format!("w{i}"));
        let len = wal_len(&db_path).unwrap_or(0);
        assert!(
            len > 0,
            "writer {i}: WAL must survive the short-lived writer's close-time checkpoint"
        );
    }

    // Interleave the CLI snapshot lifecycle, then more short-lived writers.
    let storage = MemStorage::default();
    take_snapshot_to_storage(
        &storage,
        "",
        cli_snapshot_input(&db_path),
        _shadow.lifecycle(),
    )
    .await
    .unwrap();

    for i in 6..10 {
        run_child(&db_path, "commit", &format!("w{i}"));
        let len = wal_len(&db_path).unwrap_or(0);
        assert!(
            len > 0,
            "writer {i}: WAL must survive after the snapshot lifecycle"
        );
    }

    let outcome = run_child(&db_path, "commit_and_truncate", "final");
    assert_external_truncate_blocked(&outcome, &db_path, "short-lived writers");
}

/// Controlled checkpoint: `ShadowWal::checkpoint()` releases and re-arms the
/// blocker through the production dance. After it completes, an external
/// TRUNCATE must be blocked again.
#[tokio::test]
async fn controlled_checkpoint_rearms_blocker() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "dance.db");

    let mut shadow = ShadowWal::new(&db_path).await.unwrap();
    run_child(&db_path, "commit", "before-dance");
    let (frames, _off) = shadow.copy_frames(0).await.unwrap();
    assert!(
        !frames.is_empty(),
        "setup: shadow must copy the commit frames"
    );

    // Production controlled checkpoint (release blocker, PASSIVE, re-arm).
    let commit_in_window = shadow.checkpoint().await.unwrap();
    assert!(
        !commit_in_window,
        "a window with no application commits must report clean"
    );

    let outcome = run_child(&db_path, "commit_and_truncate", "after-dance");
    assert_external_truncate_blocked(&outcome, &db_path, "controlled checkpoint");
}

/// Window detection at the production dance entry: an application commit that
/// lands in the release/re-acquire window must be reported, so the caller can
/// follow the safe re-anchor behavior. A racing writer commits in a tight
/// loop until one dance observes it (bounded; the window is several SQLite
/// operations wide, so hits are frequent).
#[tokio::test]
async fn controlled_checkpoint_detects_window_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "window.db");

    let mut shadow = ShadowWal::new(&db_path).await.unwrap();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let db_path = db_path.clone();
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch("PRAGMA busy_timeout=5000;").unwrap();
            let mut i = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                conn.execute(
                    "INSERT INTO app_data (value) VALUES (?1)",
                    [format!("w{i}")],
                )
                .unwrap();
                i += 1;
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
        })
    };

    let mut saw_dirty = false;
    for _ in 0..100 {
        // Drain frames like the production checkpoint path does first.
        let _ = shadow.copy_frames(0).await.unwrap();
        match shadow.checkpoint().await {
            Ok(true) => {
                saw_dirty = true;
                break;
            }
            Ok(false) => {}
            // A commit landing mid-checkpoint can make the fold incomplete;
            // that is the pre-existing racing-checkpoint error path, retry.
            Err(_) => {}
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();

    assert!(
        saw_dirty,
        "a commit in the release window must be detected by PRAGMA data_version"
    );
}

/// The owned-mode dance (TRUNCATE) on the lifecycle directly: a clean window
/// completes, an application commit forced into the window is folded by the
/// TRUNCATE and survives in the main-DB image (safe by construction — the
/// snapshot taken after the dance covers it), and an external TRUNCATE
/// afterwards is still blocked.
#[tokio::test]
async fn lifecycle_truncate_dance_preserves_window_commit() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "owned-dance.db");

    let lifecycle = BlockerLifecycle::open(&db_path).unwrap();

    lifecycle.controlled_checkpoint(true).unwrap();

    let outcome = run_child(&db_path, "commit_and_truncate", "after-dance");
    assert_external_truncate_blocked(&outcome, &db_path, "owned TRUNCATE dance");

    // Deterministic window commit: a writer holds its write transaction open
    // across the checkpoint's writer-lock wait, so its commit provably lands
    // between release and re-pin (the checkpoint blocks on the writer lock
    // until the writer commits).
    let writer = {
        let db_path = db_path.clone();
        std::thread::spawn(move || {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch("PRAGMA busy_timeout=5000;").unwrap();
            conn.execute_batch("BEGIN;").unwrap();
            conn.execute("INSERT INTO app_data (value) VALUES ('in-window')", [])
                .unwrap();
            // Give the dance time to release the pin and block on the writer lock.
            std::thread::sleep(std::time::Duration::from_millis(200));
            conn.execute_batch("COMMIT;").unwrap();
        })
    };
    lifecycle.controlled_checkpoint(true).unwrap();
    writer.join().unwrap();

    // Safe by construction: the in-window commit was folded by the TRUNCATE,
    // so it is part of the main-DB image any snapshot encodes after the dance.
    let probe = Connection::open(&db_path).unwrap();
    let found: i64 = probe
        .query_row(
            "SELECT count(*) FROM app_data WHERE value = 'in-window'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(found, 1, "window commit must survive the dance");
    drop(probe);

    let outcome = run_child(&db_path, "commit_and_truncate", "final");
    assert_external_truncate_blocked(
        &outcome,
        &db_path,
        "owned TRUNCATE dance after dirty window",
    );
}

/// Negative control: with NO blocker armed, the same external
/// commit + TRUNCATE must succeed and empty/remove the WAL — proving this
/// harness actually detects an invalidated/missing blocker.
#[tokio::test]
async fn control_without_blocker_truncate_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "control.db");

    let outcome = run_child(&db_path, "commit_and_truncate", "unprotected");
    assert_eq!(
        outcome.busy(),
        Some(0),
        "control: TRUNCATE must succeed with no blocker armed; child said: {}",
        outcome.raw
    );
    assert!(
        wal_len(&db_path).unwrap_or(0) == 0,
        "control: TRUNCATE must empty the WAL with no blocker armed"
    );
}

// ── Bisection: which lifecycle step invalidates the blocker? ────────────────
//
// Arms the blocker, then runs each individual step of the CLI snapshot
/// lifecycle (same SQL/file ops the production chain performs), probing after
/// each step with an external child commit + TRUNCATE and recording the
/// WAL/-shm state (length + inode) before and after. Evidence output only.
#[tokio::test]
async fn bisect_which_step_invalidates_blocker() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "bisect.db");

    let probe = |label: &str| {
        let outcome = run_child(&db_path, "commit_and_truncate", "probe");
        let wal = wal_state(&db_path);
        let shm = shm_state(&db_path);
        eprintln!(
            "PROBE {label:34} child=[{}] wal(exists={}, len={}, ino={}) shm(exists={}, ino={})",
            outcome.raw, wal.exists, wal.len, wal.ino, shm.exists, shm.ino
        );
    };

    let _shadow = ShadowWal::new(&db_path).await.unwrap();
    probe("armed");

    // Step 1: PASSIVE checkpoint on a fresh connection (checkpoint_wal_passive).
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)")
            .unwrap();
        drop(conn);
    }
    probe("passive-checkpoint-conn");

    // Step 2: raw page-size read (get_page_size).
    {
        let f = std::fs::File::open(&db_path).unwrap();
        let mut header = [0u8; 100];
        use std::io::Read;
        let mut f = f;
        f.read_exact(&mut header).unwrap();
        drop(f);
    }
    probe("raw-page-size-read");

    // Step 3: VACUUM INTO on a fresh connection (StableSqliteSnapshot::create).
    {
        let dest = dir.path().join("bisect-vacuum-tmp.db");
        let dest_str = dest.to_str().unwrap().to_string();
        let conn = Connection::open(&db_path).unwrap();
        conn.busy_timeout(std::time::Duration::from_secs(30))
            .unwrap();
        conn.execute("VACUUM INTO ?1", [dest_str]).unwrap();
        drop(conn);
        let _ = std::fs::remove_file(&dest);
    }
    probe("vacuum-into-conn");

    // Step 4: the full production chain in one go, for comparison.
    let storage = MemStorage::default();
    take_snapshot_to_storage(
        &storage,
        "",
        cli_snapshot_input(&db_path),
        _shadow.lifecycle(),
    )
    .await
    .unwrap();
    probe("full-production-chain");
}
