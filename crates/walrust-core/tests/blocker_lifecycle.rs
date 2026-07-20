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
        "burst" => {
            // One large transaction pushing the WAL past the default 1000-page
            // autocheckpoint threshold, so SQLite auto-checkpoints mid-write on
            // THIS connection (the realistic app-burst shape). ~300B/row puts
            // ~12 rows per 4KiB page: 15000 rows ≈ 1250 pages.
            conn.execute_batch("BEGIN;").expect("burst begin");
            let payload = "x".repeat(300);
            for _ in 0..15000 {
                conn.execute("INSERT INTO app_data (value) VALUES (?1)", [&payload])
                    .expect("burst insert");
            }
            conn.execute_batch("COMMIT;").expect("burst commit");
            println!("CHILD_RESULT burst-committed");
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

    // The borrowed-handle snapshot must carry the right CONTENT, not just
    // succeed: decode it and require the seed and post-blocker rows.
    let decode_snapshot = |storage: &MemStorage, key: &str, expect: &[&str]| {
        let bytes = storage.map.lock().unwrap().get(key).cloned().unwrap();
        let out = dir
            .path()
            .join(format!("decoded-{}.db", key.replace('/', "_")));
        walrust_core::legacy_ltx::decode_to_db(std::io::Cursor::new(bytes), &out).unwrap();
        let conn = Connection::open(&out).unwrap();
        for value in expect {
            let n: i64 = conn
                .query_row(
                    "SELECT count(*) FROM app_data WHERE value = ?1",
                    [value],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "snapshot at {key} must contain row {value}");
        }
    };
    decode_snapshot(
        &storage,
        "app/0001/0000000000000001-0000000000000001.ltx",
        &["seed", "after-blocker"],
    );

    // 4. A second snapshot on the same borrowed connection (its busy_timeout
    // save/restore must behave across calls) also contains both commits.
    run_child(&db_path, "commit", "second-round");
    take_snapshot_to_storage(
        &storage,
        "",
        cli_snapshot_input(&db_path),
        _shadow.lifecycle(),
    )
    .await
    .unwrap();
    decode_snapshot(
        &storage,
        "app/0002/0000000000000001-0000000000000002.ltx",
        &["seed", "after-blocker", "second-round"],
    );

    // 5. External app commits more, then issues wal_checkpoint(TRUNCATE).
    let outcome = run_child(&db_path, "commit_and_truncate", "after-snapshot");
    assert_external_truncate_blocked(&outcome, &db_path, "cli startup snapshot");
}

/// Owned HADBP sync with `db_checksum == None` while the blocker is armed: the
/// pre-image hash must be computed through the retained source descriptor
/// (`pre_image_checksum` in walrust-core/src/sync.rs), never by reopening the
/// main DB. An open/close of the main DB while armed drops the process's POSIX
/// locks on the inode; the TRUNCATE probe below still reports busy (read marks
/// live on the `-shm` inode), but the child's last-connection close can then
/// take the EXCLUSIVE main-db lock its close-time checkpoint needs — the DF2
/// kill shape. Neuter the fd arm of `pre_image_checksum` (back to
/// `compute_checksum_from_file`) and the close/unlink assertions below fail on
/// platforms where a bare main-db open/close is load-bearing.
#[tokio::test]
async fn owned_sync_armed_none_checksum_preserves_blocker() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "owned-none-checksum.db");

    let mut state = walrust_core::sync::SyncState::new(db_path.clone()).unwrap();
    state.checkpoint_blocker = Some(Arc::new(tokio::sync::Mutex::new(
        BlockerLifecycle::open(&db_path).unwrap(),
    )));
    state.db_checksum = None;

    // Frames for the incremental to encode, committed past the blocker's mark.
    let app = Connection::open(&db_path).unwrap();
    app.execute("INSERT INTO app_data (value) VALUES ('pre-sync')", [])
        .unwrap();

    let storage = MemStorage::default();
    let shipped = walrust_core::sync::sync_wal_with_retry(
        &storage,
        "pfx/",
        &mut state,
        &hadb_io::RetryPolicy::default_policy(),
    )
    .await
    .unwrap();
    assert!(shipped >= 1, "the incremental must ship");

    // External TRUNCATE is still refused (the read-mark layer).
    let outcome = run_child(&db_path, "commit_and_truncate", "post-sync");
    assert_external_truncate_blocked(&outcome, &db_path, "owned sync, None checksum");

    // The child's close-time checkpoint must not take EXCLUSIVE and unlink the
    // WAL (the main-db SHARED-lock layer).
    let before = wal_state(&db_path);
    run_child(&db_path, "commit", "closer");
    let after = wal_state(&db_path);
    assert!(
        after.exists && after.len > 0,
        "child close must not unlink the WAL while the blocker is armed ({before:?} -> {after:?})"
    );
    assert_eq!(
        after.ino, before.ino,
        "the WAL must be the same inode (not unlinked and recreated)"
    );
}

/// Double-registration is rejected before arming (a replaced lifecycle's raw
/// descriptor must never close after the new one arms); `remove()` clears the
/// way for a re-add, and the re-armed blocker still blocks.
#[tokio::test]
async fn replicator_rejects_double_add_until_remove() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "dbl.db");

    let storage: Arc<dyn StorageBackend> = Arc::new(MemStorage::default());
    let config = ReplicationConfig {
        sync_interval: std::time::Duration::from_secs(3600),
        snapshot_interval: std::time::Duration::from_secs(3600),
        ..ReplicationConfig::default()
    };
    let replicator = Replicator::new(storage, "dbl/", config);

    replicator.add("dbl", &db_path).await.unwrap();

    // add() again is rejected (the remote-state guard fires first here — the
    // point is rejection before any arming, by either guard).
    replicator
        .add("dbl", &db_path)
        .await
        .expect_err("second add() without remove() must be rejected");

    // add_without_snapshot has no remote-state guard: THIS is where the
    // registration guard is load-bearing.
    let err = replicator
        .add_without_snapshot("dbl", &db_path)
        .await
        .expect_err("second add_without_snapshot must be rejected by the registration guard");
    assert!(
        err.to_string().contains("already registered"),
        "error must name the double registration, got: {err}"
    );

    // The second add_without_snapshot must be rejected BEFORE anything arms
    // (the registration guard), and the ORIGINAL lifecycle must still be
    // fully functional afterwards.
    let outcome = run_child(&db_path, "commit_and_truncate", "original-lives");
    assert_external_truncate_blocked(&outcome, &db_path, "original lifecycle after rejected add");

    replicator.remove("dbl").await.unwrap();
    // add() again would trip the remote-state guard; the reopen path must work.
    replicator
        .add_without_snapshot("dbl", &db_path)
        .await
        .unwrap();

    let outcome = run_child(&db_path, "commit_and_truncate", "after-readd");
    assert_external_truncate_blocked(&outcome, &db_path, "re-add after remove");
}

/// Process-wide inode reservation: two lifecycles may not arm on the same
/// database file at once — not through the same path, an alias, or a
/// hardlink — a failed arming leaves the original untouched, and re-arming
/// after a drop succeeds.
#[tokio::test]
async fn second_lifecycle_on_same_database_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "dup.db");

    let first = BlockerLifecycle::open(&db_path).unwrap();
    let err = BlockerLifecycle::open(&db_path)
        .err()
        .expect("second arming on the same file must be rejected");
    assert!(
        err.to_string().contains("already armed"),
        "error must name the duplicate arming, got: {err}"
    );

    // A hardlink alias resolves to the same inode and must be caught too.
    let alias = dir.path().join("dup-alias.db");
    std::fs::hard_link(&db_path, &alias).unwrap();
    let err = BlockerLifecycle::open(&alias)
        .err()
        .expect("arming through a hardlink alias must be rejected");
    assert!(
        err.to_string().contains("already armed"),
        "error must name the duplicate arming, got: {err}"
    );

    // The original lifecycle is untouched by the failed armings.
    let outcome = run_child(&db_path, "commit_and_truncate", "original-lives");
    assert_external_truncate_blocked(&outcome, &db_path, "original after rejected armings");

    // Re-arming after the drop must succeed (the reservation releases).
    drop(first);
    let _second = BlockerLifecycle::open(&db_path).unwrap();
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
    let (frames, off) = shadow.copy_frames(0).await.unwrap();
    assert!(
        !frames.is_empty(),
        "setup: shadow must copy the commit frames"
    );

    // Production controlled checkpoint (release blocker, PASSIVE, re-arm).
    let outcome = shadow.checkpoint(off).await.unwrap();
    assert!(
        !outcome.commit_in_window,
        "a window with no application commits must report clean"
    );
    assert!(
        !outcome.folded_uncopied_frames,
        "folding exactly the copied prefix must not count as uncopied frames"
    );

    let outcome = run_child(&db_path, "commit_and_truncate", "after-dance");
    assert_external_truncate_blocked(&outcome, &db_path, "controlled checkpoint");
}

/// Folded-extent detection (the copy-to-dance gap): a commit that lands after
/// the shadow's last copy but before the controlled checkpoint is folded by
/// walrust's own PASSIVE and erased by the re-pin WAL restart — invisible to
/// `data_version`, so the folded-extent check must catch it.
#[tokio::test]
async fn controlled_checkpoint_detects_folded_uncopied_frames() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "folded.db");

    let mut shadow = ShadowWal::new(&db_path).await.unwrap();
    run_child(&db_path, "commit", "copied");
    let (_frames, copied_offset) = shadow.copy_frames(0).await.unwrap();
    assert!(copied_offset > 0, "setup: shadow must copy frames");

    // This commit is NOT copied before the dance: it lands in the gap between
    // the last copy and the checkpoint, exactly the hole the folded-extent
    // check exists for.
    run_child(&db_path, "commit", "uncopied");

    let outcome = shadow.checkpoint(copied_offset).await.unwrap();
    assert!(
        outcome.folded_uncopied_frames,
        "a commit folded past the shadow's copied cursor must be detected"
    );
    assert!(
        !outcome.commit_in_window,
        "the gap commit predates the data_version window and must not trip it"
    );

    // The dance is still safe: the uncopied commit was folded into the main
    // DB, so a re-anchoring snapshot would cover it; and the blocker still
    // blocks external TRUNCATE afterwards.
    let outcome2 = run_child(&db_path, "commit_and_truncate", "after");
    assert_external_truncate_blocked(&outcome2, &db_path, "folded-extent dance");
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
            // synchronous=OFF keeps the commit cadence high on slow disks so
            // the bounded loop below reliably intersects the dance window.
            conn.execute_batch("PRAGMA busy_timeout=5000; PRAGMA synchronous=OFF;")
                .unwrap();
            let mut i = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                conn.execute(
                    "INSERT INTO app_data (value) VALUES (?1)",
                    [format!("w{i}")],
                )
                .unwrap();
                i += 1;
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        })
    };

    let mut saw_dirty = false;
    let mut dance_errors = 0u32;
    for _ in 0..100 {
        // Drain frames like the production checkpoint path does first.
        let (_frames, off) = shadow.copy_frames(0).await.unwrap();
        match shadow.checkpoint(off).await {
            Ok(outcome) if outcome.commit_in_window => {
                saw_dirty = true;
                break;
            }
            Ok(_) => {}
            // A commit landing mid-checkpoint can make the fold incomplete;
            // that is the pre-existing racing-checkpoint error path, retry.
            Err(_) => dance_errors += 1,
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();

    assert!(
        saw_dirty,
        "a commit in the release window must be detected by PRAGMA data_version \
         (no detection in 100 dances, {dance_errors} swallowed dance errors)"
    );
    assert!(
        dance_errors < 100,
        "every dance errored — the error arm hid a systemic dance failure"
    );

    // The blocker survived the whole loop.
    let outcome = run_child(&db_path, "commit_and_truncate", "after-loop");
    assert_external_truncate_blocked(&outcome, &db_path, "post-loop probe");
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

    // Deterministic window commit: the writer holds its write transaction
    // open and signals once the INSERT is in flight; only then does the dance
    // begin, so the checkpoint provably blocks on the writer lock until the
    // writer commits — the commit lands between release and re-pin, always.
    let (inserted_tx, inserted_rx) = std::sync::mpsc::channel::<()>();
    let writer = {
        let db_path = db_path.clone();
        std::thread::spawn(move || {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch("PRAGMA busy_timeout=5000;").unwrap();
            conn.execute_batch("BEGIN;").unwrap();
            conn.execute("INSERT INTO app_data (value) VALUES ('in-window')", [])
                .unwrap();
            inserted_tx.send(()).unwrap();
            // Hold the write transaction open so the checkpoint must wait.
            std::thread::sleep(std::time::Duration::from_millis(200));
            conn.execute_batch("COMMIT;").unwrap();
        })
    };
    inserted_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("writer must signal its INSERT");
    lifecycle.controlled_checkpoint(true).unwrap();
    writer.join().unwrap();

    // Safe by construction: the in-window commit was folded by the TRUNCATE,
    // so it is part of the main-DB image any snapshot encodes after the dance.
    // Opening this probe connection is safe for the blocker: SQLite's unix VFS
    // parks a closing connection's fd while the inode has outstanding locks,
    // so same-process connection churn cannot release the blocker's locks
    // (only raw opens bypass the parking — that was the bug).
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
/// WAL/-shm state (length + inode) before and after. Asserts the exact zombie
/// signature: one raw main-DB open+close lets the child's close unlink
/// WAL/SHM while the read mark still refuses explicit TRUNCATE.
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

    // Hard assertion on the exact zombie signature (this is the bug, pinned as
    // deterministic evidence): after ONE raw open+close of the main DB, the
    // child's TRUNCATE was still refused (read mark intact on the old shm
    // inode) and yet its close-time checkpoint unlinked the WAL and SHM.
    let wal = wal_state(&db_path);
    let shm = shm_state(&db_path);
    assert!(
        !wal.exists && !shm.exists,
        "one raw main-DB open+close must let the child's close unlink WAL/SHM: \
         wal(exists={}, len={}) shm(exists={})",
        wal.exists,
        wal.len,
        shm.exists
    );

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
    let vacuum_probe = run_child(&db_path, "commit_and_truncate", "probe");
    probe("vacuum-into-conn");
    assert_eq!(
        vacuum_probe.busy(),
        Some(0),
        "the zombified blocker must no longer block external TRUNCATE"
    );
}

/// Deferred busy path: an external reader pinning part of the WAL makes the
/// fold incomplete — ordinary contention, so the dance returns `deferred`
/// (NOT an error), the blocker is re-pinned, and a later clean dance
/// succeeds.
#[tokio::test]
async fn checkpoint_busy_defer_still_repins_blocker() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "busy.db");

    let lifecycle = BlockerLifecycle::open(&db_path).unwrap();

    // An external reader pinning the WAL at its current end; further commits
    // then land past its read mark, so the PASSIVE checkpoint cannot fold the
    // whole log and must defer.
    let external = Connection::open(&db_path).unwrap();
    external.execute_batch("BEGIN DEFERRED;").unwrap();
    let _: i64 = external
        .query_row("SELECT count(*) FROM app_data", [], |row| row.get(0))
        .unwrap();
    run_child(&db_path, "commit", "past-the-mark");

    let outcome = lifecycle.controlled_checkpoint(false).unwrap();
    assert!(
        outcome.deferred,
        "a reader-pinned WAL must defer the checkpoint, not error it"
    );

    drop(external);

    // The dance re-pinned the blocker before returning the deferral.
    let outcome = run_child(&db_path, "commit_and_truncate", "after-defer");
    assert_external_truncate_blocked(&outcome, &db_path, "deferred checkpoint re-pin");

    // And the next clean dance succeeds and does not defer.
    let outcome = lifecycle.controlled_checkpoint(false).unwrap();
    assert!(!outcome.deferred, "a clean dance must complete");
}

/// Re-pin failure recovery: if the heartbeat upsert times out behind an
/// application writer, the dance errors loudly — and the NEXT dance must
/// succeed (the release is idempotent; an unconditional ROLLBACK would wedge
/// every later dance with "no transaction is active").
#[tokio::test]
async fn repin_failure_heals_next_dance() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "repin.db");

    let lifecycle = BlockerLifecycle::open(&db_path).unwrap();

    // A writer holding WAL_WRITE_LOCK past the lifecycle's 5s busy timeout:
    // the dance's PASSIVE fold succeeds (it needs no writer lock), but the
    // heartbeat upsert cannot write and the re-pin fails.
    let (inserted_tx, inserted_rx) = std::sync::mpsc::channel::<()>();
    let writer = {
        let db_path = db_path.clone();
        std::thread::spawn(move || {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch("PRAGMA busy_timeout=0;").unwrap();
            conn.execute_batch("BEGIN;").unwrap();
            conn.execute("INSERT INTO app_data (value) VALUES ('held')", [])
                .unwrap();
            inserted_tx.send(()).unwrap();
            // Hold past the heartbeat's 5s busy timeout.
            std::thread::sleep(std::time::Duration::from_millis(6500));
            conn.execute_batch("COMMIT;").unwrap();
        })
    };
    inserted_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("writer must signal its INSERT");

    let err = lifecycle
        .controlled_checkpoint(false)
        .expect_err("re-pin must fail while the writer holds the write lock");
    writer.join().unwrap();
    drop(err);

    // The next dance heals: release is idempotent, checkpoint, heartbeat and
    // re-pin all succeed.
    lifecycle.controlled_checkpoint(false).unwrap();

    let outcome = run_child(&db_path, "commit_and_truncate", "after-heal");
    assert_external_truncate_blocked(&outcome, &db_path, "healed dance");
}

/// Shutdown ordering: dropping the ShadowWal closes blocker, then monitor,
/// then the source descriptor — and an external TRUNCATE afterwards must
/// succeed (locks really released; no pinned-forever WAL).
#[tokio::test]
async fn shutdown_releases_locks() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "shutdown.db");

    let shadow = ShadowWal::new(&db_path).await.unwrap();
    drop(shadow);

    let outcome = run_child(&db_path, "commit_and_truncate", "post-shutdown");
    assert_eq!(
        outcome.busy(),
        Some(0),
        "TRUNCATE must succeed after the lifecycle drops; child said: {}",
        outcome.raw
    );
    assert!(
        wal_len(&db_path).unwrap_or(0) == 0,
        "the WAL must be empty/removed after a successful TRUNCATE"
    );
}

/// Per-database isolation: arming two lifecycles and snapshotting one must
/// not disturb the other's blocker.
#[tokio::test]
async fn multi_db_lifecycles_do_not_interfere() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = fresh_db(dir.path(), "a.db");
    let db_b = fresh_db(dir.path(), "b.db");

    let shadow_a = ShadowWal::new(&db_a).await.unwrap();
    let _shadow_b = ShadowWal::new(&db_b).await.unwrap();

    // Run the full CLI snapshot lifecycle against db A.
    let storage = MemStorage::default();
    take_snapshot_to_storage(
        &storage,
        "",
        cli_snapshot_input(&db_a),
        shadow_a.lifecycle(),
    )
    .await
    .unwrap();

    // db B's blocker is untouched.
    let outcome = run_child(&db_b, "commit_and_truncate", "probe-b");
    assert_external_truncate_blocked(&outcome, &db_b, "multi-DB isolation");

    // And db A is still protected too.
    let outcome = run_child(&db_a, "commit_and_truncate", "probe-a");
    assert_external_truncate_blocked(&outcome, &db_a, "multi-DB post-snapshot");
}

/// The contract holds at non-default page sizes: 512-byte and 64KiB pages.
#[tokio::test]
async fn cli_startup_snapshot_keeps_blocker_at_extreme_page_sizes() {
    for page_size in [512u32, 65536] {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join(format!("ps{page_size}.db"));
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(&format!(
            "PRAGMA page_size={page_size};
             PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE app_data (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO app_data (value) VALUES ('seed');"
        ))
        .unwrap();
        drop(conn);

        let mut shadow = ShadowWal::new(&db_path).await.unwrap();
        run_child(&db_path, "commit", "past-mark");

        let storage = MemStorage::default();
        take_snapshot_to_storage(
            &storage,
            "",
            cli_snapshot_input(&db_path),
            shadow.lifecycle(),
        )
        .await
        .unwrap();

        let outcome = run_child(&db_path, "commit_and_truncate", "after-snapshot");
        assert_external_truncate_blocked(&outcome, &db_path, "extreme page size");

        // The folded-extent math is the only page-size-dependent logic: a
        // commit folded past the copied cursor must be detected at this page
        // size too.
        let (_frames, copied_offset) = shadow.copy_frames(0).await.unwrap();
        run_child(&db_path, "commit", "uncopied");
        let dance = shadow.checkpoint(copied_offset).await.unwrap();
        assert!(
            dance.folded_uncopied_frames,
            "folded-extent detection must hold at page_size={page_size}"
        );
    }
}

/// App auto-checkpoint burst (default 1000-page threshold, one big
/// transaction): the app's own checkpoint cannot erase unread WAL frames
/// while the blocker is armed.
#[tokio::test]
async fn app_autocheckpoint_burst_cannot_reset_unread_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = fresh_db(dir.path(), "burst.db");

    let mut shadow = ShadowWal::new(&db_path).await.unwrap();

    run_child(&db_path, "burst", "");
    let len = wal_len(&db_path).unwrap_or(0);
    assert!(
        len > 0,
        "WAL must survive the app-side autocheckpoint burst and close"
    );

    // The shadow must be able to copy every burst frame.
    let (frames, _off) = shadow.copy_frames(0).await.unwrap();
    assert!(
        frames.len() > 1000,
        "shadow must copy the whole burst ({} frames)",
        frames.len()
    );

    let outcome = run_child(&db_path, "commit_and_truncate", "after-burst");
    assert_external_truncate_blocked(&outcome, &db_path, "autocheckpoint burst");
}
