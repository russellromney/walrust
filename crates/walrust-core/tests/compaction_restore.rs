//! Compaction C2b: end-to-end restore/planner proofs over **real** SQLite
//! changeset chains merged by the real engine.
//!
//! Builds a genuine SQLite database, captures its per-seq page images, uploads a
//! snapshot + a chain of incrementals, runs the real `run_level_compaction`
//! engine to fold L0 into L1/L2, then proves the C2b read side:
//!
//! - (a) restore-to-latest is row-exact vs the driver ground truth + passes
//!   `integrity_check`, going **through** merged objects (the L0 tail it needs
//!   was deleted by the merge);
//! - (b) PITR to a merged-window boundary restores exactly;
//! - (c) PITR strictly inside a merged window is a loud typed error naming the
//!   nearest restorable points on both sides;
//! - (d) with the superseded L0 tail gone, restore still reconstructs the DB
//!   exactly — proving restores genuinely traverse merged objects;
//! - (e) a crash-overlap fixture (duplicate L0 under a merged range) restores
//!   without double-apply;
//! - prefetch concurrency 1 vs 4 yields byte-identical output.
//!
//! Run with `cargo test -p walrust-core --test compaction_restore`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use rusqlite::Connection;

use walrust_core::compaction::{
    apply_plan, gather_candidates, plan_restore, run_level_compaction, CompactionLayout,
    RangeLayout, SeqLayout,
};
use walrust_core::ltx;

// ── In-memory backend (correct range_get via default slice) ─────────────────

#[derive(Default)]
struct MemStore {
    map: Mutex<HashMap<String, Vec<u8>>>,
}
impl MemStore {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}
#[async_trait]
impl StorageBackend for MemStore {
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
        let mut keys: Vec<String> = map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .filter(|k| after.map(|a| k.as_str() > a).unwrap_or(true))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
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
            etag: Some("1".into()),
        })
    }
    async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
        self.map
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("1".into()),
        })
    }
}

const PS: u32 = 4096;
const NOW: i64 = 10_000_000_000_000;

/// A built fixture: the store, the addressing, per-seq DB images (ground truth),
/// and the final row set.
struct Fixture {
    store: Arc<MemStore>,
    prefix: String,
    db: String,
    /// DB file bytes as of each seq (`state_at[&1]` == the snapshot state).
    state_at: HashMap<u64, Vec<u8>>,
    /// Final committed rows.
    rows: Vec<(i64, String)>,
    /// The highest seq (restore-to-latest target).
    max_seq: u64,
}

/// Full-page diff of two DB images (page numbers are 1-based).
fn diff_pages(old: &[u8], new: &[u8], ps: usize) -> Vec<(u32, Vec<u8>)> {
    let new_pages = new.len() / ps;
    let old_pages = old.len() / ps;
    let mut out = Vec::new();
    for i in 0..new_pages {
        let np = &new[i * ps..(i + 1) * ps];
        let changed = i >= old_pages || np != &old[i * ps..(i + 1) * ps];
        if changed {
            out.push(((i + 1) as u32, np.to_vec()));
        }
    }
    out
}

fn snapshot_key(prefix: &str, db: &str, seq: u64) -> String {
    // GENERATION_SNAPSHOT == 1.
    format!("{prefix}{db}/0001/{seq:016x}.hadbp")
}
fn l0_seq_key(prefix: &str, db: &str, seq: u64) -> String {
    format!("{prefix}{db}/0000/{seq:016x}.hadbp")
}
fn l0_range_key(prefix: &str, db: &str, seq: u64) -> String {
    format!("{prefix}{db}/0000/{seq:016x}-{seq:016x}.ltx")
}

/// Build a real SQLite history and upload snapshot(seq 1) + incrementals
/// (seq 2..=n_incrementals+1). `ext_range` selects the L0 key style: `false` =
/// seq layout (`.hadbp`), `true` = range layout (`.ltx`).
fn build_fixture(prefix: &str, db: &str, n_incrementals: u64, ext_range: bool) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("ground.db");
    let store = MemStore::new();

    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; \
         CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);",
    )
    .unwrap();
    for i in 0..20 {
        conn.execute("INSERT INTO t(id, v) VALUES (?, ?)", (i, format!("v{i}")))
            .unwrap();
    }
    // Force the page cache to the file so the on-disk image is complete.
    conn.cache_flush().unwrap();
    drop(conn);

    let mut state_at = HashMap::new();
    let v0 = std::fs::read(&db_path).unwrap();
    state_at.insert(1u64, v0.clone());

    // Snapshot at seq 1.
    let snap = ltx::encode_snapshot_with_checksum(&db_path, PS, 1, 0).unwrap();
    futures_put(&store, &snapshot_key(prefix, db, 1), &snap.bytes);
    let mut prev = snap.checksum;
    let mut prev_bytes = v0;

    let conn = Connection::open(&db_path).unwrap();
    for seq in 2..=(n_incrementals + 1) {
        // A distinct edit each step (page 1 change-counter always moves).
        let id = 1000 + seq as i64;
        conn.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            (id, format!("row-{seq}")),
        )
        .unwrap();
        conn.execute("UPDATE t SET v = ? WHERE id = 0", (format!("touch-{seq}"),))
            .unwrap();
        conn.cache_flush().unwrap();
        let new_bytes = std::fs::read(&db_path).unwrap();

        let pages = diff_pages(&prev_bytes, &new_bytes, PS as usize);
        assert!(!pages.is_empty(), "each commit must change at least page 1");
        let end_page_count = (new_bytes.len() / PS as usize) as u64;
        let (bytes, checksum) =
            ltx::encode_wal_changes_with_end_page_count(&pages, PS, seq, prev, end_page_count)
                .unwrap();
        let key = if ext_range {
            l0_range_key(prefix, db, seq)
        } else {
            l0_seq_key(prefix, db, seq)
        };
        futures_put(&store, &key, &bytes);

        state_at.insert(seq, new_bytes.clone());
        prev = checksum;
        prev_bytes = new_bytes;
    }
    let rows: Vec<(i64, String)> = {
        let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
        let r = stmt
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .map(|x| x.unwrap())
            .collect();
        r
    };
    drop(conn);

    Fixture {
        store,
        prefix: prefix.to_string(),
        db: db.to_string(),
        state_at,
        rows,
        max_seq: n_incrementals + 1,
    }
}

/// Synchronous helper to put into the store from the (sync) fixture builder.
fn futures_put(store: &MemStore, key: &str, data: &[u8]) {
    store
        .map
        .lock()
        .unwrap()
        .insert(key.to_string(), data.to_vec());
}

fn integrity_ok(path: &Path) -> bool {
    let conn = Connection::open(path).unwrap();
    let res: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    res == "ok"
}

fn query_rows(path: &Path) -> Vec<(i64, String)> {
    let conn = Connection::open(path).unwrap();
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    let r = stmt
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .map(|x| x.unwrap())
        .collect();
    r
}

/// Fold L0 into L1/L2, leaving a fine L0 tail. Returns nothing; asserts the
/// resulting shape (L2 [2..9], L1 [10..13], L0 tail [14]).
async fn compact_to_l1_l2<L: CompactionLayout>(layout: &L) {
    // Three L0→L1 merges of 4 each: [2,5],[6,9],[10,13]; seq 14 stays in L0.
    for _ in 0..3 {
        let o = run_level_compaction(layout, 0, 4, Duration::from_secs(0), NOW)
            .await
            .unwrap();
        assert_eq!(o.merged_count(), 4);
    }
    // One L1→L2 merge of the two oldest L1 files: [2,9]. Leaves L1 [10,13].
    let o = run_level_compaction(layout, 1, 2, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(o.merged_count(), 2);
}

// ── Owned SeqLayout: the full wired restore path (sync::restore) ─────────────

#[tokio::test]
async fn seq_layout_e2e_restore_through_merged_objects() {
    let fx = build_fixture("p/", "db", 13, false); // seqs 1..=14
    let layout = SeqLayout::new(fx.store.clone(), &fx.prefix, &fx.db);
    compact_to_l1_l2(&layout).await;

    // The superseded L0 tail (seqs 2..=13) is GONE; only seq 14 remains at L0.
    let l0 = fx.store.list("p/db/0000/", None).await.unwrap();
    assert_eq!(
        l0.len(),
        1,
        "only the fine tail seq 14 survives at L0: {l0:?}"
    );
    assert_eq!(
        fx.store.list("p/db/levels/L2/", None).await.unwrap().len(),
        1
    );
    assert_eq!(
        fx.store.list("p/db/levels/L1/", None).await.unwrap().len(),
        1
    );

    let dir = tempfile::tempdir().unwrap();

    // (a)/(d) restore-to-latest through merged objects: row-exact + integrity.
    let out = dir.path().join("latest.db");
    let seq = walrust_core::sync::restore(fx.store.clone(), &fx.prefix, &fx.db, &out, None)
        .await
        .unwrap();
    assert_eq!(seq, fx.max_seq);
    assert!(integrity_ok(&out));
    assert_eq!(query_rows(&out), fx.rows);
    assert_eq!(
        std::fs::read(&out).unwrap(),
        fx.state_at[&fx.max_seq],
        "restore-to-latest must be byte-exact vs ground truth"
    );

    // (b) PITR to a merged-window boundary (L2 max seq 9) restores exactly.
    let out9 = dir.path().join("at9.db");
    let seq9 = walrust_core::sync::restore(fx.store.clone(), &fx.prefix, &fx.db, &out9, Some("9"))
        .await
        .unwrap();
    assert_eq!(seq9, 9);
    assert!(integrity_ok(&out9));
    assert_eq!(std::fs::read(&out9).unwrap(), fx.state_at[&9]);

    // (b') PITR to the L1 boundary (seq 13) also restores exactly.
    let out13 = dir.path().join("at13.db");
    walrust_core::sync::restore(fx.store.clone(), &fx.prefix, &fx.db, &out13, Some("13"))
        .await
        .unwrap();
    assert_eq!(std::fs::read(&out13).unwrap(), fx.state_at[&13]);

    // (c) PITR strictly inside a merged window (seq 5, inside L2 [2,9], no fine
    //     coverage) is a loud typed error naming the neighbors (9 above, 1 below).
    let out5 = dir.path().join("at5.db");
    let err = walrust_core::sync::restore(fx.store.clone(), &fx.prefix, &fx.db, &out5, Some("5"))
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("falls inside merged window") && msg.contains("[2..=9]"),
        "decay error must name the window: {msg}"
    );
    assert!(
        msg.contains("seq 1 (below)") && msg.contains("seq 9 (above)"),
        "decay error must name nearest restorable points on both sides: {msg}"
    );
    assert!(!out5.exists(), "no partial output on a failed PITR");
}

#[tokio::test]
async fn seq_layout_crash_overlap_restores_without_double_apply() {
    let fx = build_fixture("q/", "db", 13, false);
    let layout = SeqLayout::new(fx.store.clone(), &fx.prefix, &fx.db);
    compact_to_l1_l2(&layout).await;

    // (e) Crash-overlap: re-seed L0 objects for seqs 2..=5, which the L2 [2,9]
    //     range already supersedes (a crash between merge-write and source-
    //     delete). Restore must still be exact and apply each seq once.
    for seq in 2..=5u64 {
        let bytes = {
            // Re-encode the same incremental content from ground-truth diffs.
            let prev_img = &fx.state_at[&(seq - 1)];
            let img = &fx.state_at[&seq];
            let pages = diff_pages(prev_img, img, PS as usize);
            let end = (img.len() / PS as usize) as u64;
            // prev checksum is unknown here, but the object is a duplicate the
            // planner will never choose (L2 wins the extension), so its content
            // only needs to be a parseable HADBP changeset.
            ltx::encode_wal_changes_with_end_page_count(&pages, PS, seq, 0, end)
                .unwrap()
                .0
        };
        futures_put(&fx.store, &l0_seq_key(&fx.prefix, &fx.db, seq), &bytes);
    }

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("overlap.db");
    let seq = walrust_core::sync::restore(fx.store.clone(), &fx.prefix, &fx.db, &out, None)
        .await
        .unwrap();
    assert_eq!(seq, fx.max_seq);
    assert!(integrity_ok(&out));
    assert_eq!(query_rows(&out), fx.rows);
    assert_eq!(std::fs::read(&out).unwrap(), fx.state_at[&fx.max_seq]);
}

// ── Range layout: the shared planner + executor over the `.ltx` adapter ──────

#[tokio::test]
async fn range_layout_e2e_restore_through_merged_objects() {
    let fx = build_fixture("r/", "db", 13, true); // range layout L0 keys
    let layout = RangeLayout::new(fx.store.clone(), &fx.prefix, &fx.db);
    compact_to_l1_l2(&layout).await;

    // Apply the snapshot to a staged file, then drive the shared executor.
    let dir = tempfile::tempdir().unwrap();
    let staged = dir.path().join("staged.db");
    let snap_bytes = fx
        .store
        .get(&snapshot_key(&fx.prefix, &fx.db, 1))
        .await
        .unwrap()
        .unwrap();
    let decode = ltx::decode_to_db(&snap_bytes, &staged).unwrap();

    let candidates = gather_candidates(&layout, 1, u64::MAX).await.unwrap();
    let plan = plan_restore(&candidates, 1, fx.max_seq).unwrap();
    // Coarse L2 [2,9] + coarse L1 [10,13] + fine L0 [14] == 3 objects.
    assert_eq!(plan.files.len(), 3);
    let seq = apply_plan(&layout, &plan, &staged, decode.checksum, 4)
        .await
        .unwrap();
    assert_eq!(seq, fx.max_seq);
    assert!(integrity_ok(&staged));
    assert_eq!(query_rows(&staged), fx.rows);
    assert_eq!(std::fs::read(&staged).unwrap(), fx.state_at[&fx.max_seq]);
}

#[tokio::test]
async fn prefetch_concurrency_1_vs_4_byte_identical() {
    let fx = build_fixture("s/", "db", 13, false);
    let layout = SeqLayout::new(fx.store.clone(), &fx.prefix, &fx.db);
    compact_to_l1_l2(&layout).await;

    let candidates = gather_candidates(&layout, 1, u64::MAX).await.unwrap();
    let plan = plan_restore(&candidates, 1, fx.max_seq).unwrap();

    let snap_bytes = fx
        .store
        .get(&snapshot_key(&fx.prefix, &fx.db, 1))
        .await
        .unwrap()
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let mut outputs = Vec::new();
    for depth in [1usize, 4usize] {
        let staged = dir.path().join(format!("d{depth}.db"));
        let decode = ltx::decode_to_db(&snap_bytes, &staged).unwrap();
        apply_plan(&layout, &plan, &staged, decode.checksum, depth)
            .await
            .unwrap();
        outputs.push(std::fs::read(&staged).unwrap());
    }
    assert_eq!(
        outputs[0], outputs[1],
        "prefetch depth 1 vs 4 must produce byte-identical output"
    );
    assert_eq!(outputs[0], fx.state_at[&fx.max_seq]);
}

// ── Owned-mode VACUUM (shrink) across a merge boundary (C3a scope 3) ─────────

/// Build a real SQLite history that GROWS to many pages, then **VACUUMs**
/// (shrinks) mid-history, then grows again — as owned-mode HADBP objects. The
/// snapshot is seq 1; seqs 2..=14 are incrementals, with the VACUUM at seq 4 so
/// the `[2,5]` L0→L1 merge (and the `[2,9]` L2 merge) span the shrink and must
/// exercise the orphan-page elision (a pre-shrink high page dropped above the
/// final DB size).
fn build_vacuum_fixture(prefix: &str, db: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("ground.db");
    let store = MemStore::new();

    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA auto_vacuum=NONE; \
         CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);",
    )
    .unwrap();
    // Grow to many pages: 400 fat rows (~200 bytes each ≈ 80 KB ≈ 20 pages).
    let fat = "x".repeat(200);
    for i in 0..400 {
        conn.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            (i, format!("{fat}-{i}")),
        )
        .unwrap();
    }
    conn.cache_flush().unwrap();
    drop(conn);

    let mut state_at = HashMap::new();
    let v1 = std::fs::read(&db_path).unwrap();
    state_at.insert(1u64, v1.clone());
    let big_pages = v1.len() / PS as usize;
    assert!(big_pages > 10, "snapshot must span many pages: {big_pages}");

    let snap = ltx::encode_snapshot_with_checksum(&db_path, PS, 1, 0).unwrap();
    futures_put(&store, &snapshot_key(prefix, db, 1), &snap.bytes);
    let mut prev = snap.checksum;
    let mut prev_bytes = v1;

    let conn = Connection::open(&db_path).unwrap();
    for seq in 2..=14u64 {
        if seq == 4 {
            // The shrink: delete almost everything, then VACUUM to compact the
            // file down to a couple of pages.
            conn.execute("DELETE FROM t WHERE id >= 5", []).unwrap();
            conn.execute("VACUUM", []).unwrap();
        } else {
            let id = 1000 + seq as i64;
            conn.execute(
                "INSERT INTO t(id, v) VALUES (?, ?)",
                (id, format!("row-{seq}")),
            )
            .unwrap();
            conn.execute("UPDATE t SET v = ? WHERE id = 0", (format!("touch-{seq}"),))
                .unwrap();
        }
        conn.cache_flush().unwrap();
        let new_bytes = std::fs::read(&db_path).unwrap();

        let pages = diff_pages(&prev_bytes, &new_bytes, PS as usize);
        assert!(!pages.is_empty(), "seq {seq} must change at least one page");
        let end_page_count = (new_bytes.len() / PS as usize) as u64;
        let (bytes, checksum) =
            ltx::encode_wal_changes_with_end_page_count(&pages, PS, seq, prev, end_page_count)
                .unwrap();
        futures_put(&store, &l0_seq_key(prefix, db, seq), &bytes);

        state_at.insert(seq, new_bytes.clone());
        prev = checksum;
        prev_bytes = new_bytes;
    }
    // The shrink really shrank the file below the snapshot size.
    assert!(
        state_at[&4].len() < state_at[&1].len(),
        "VACUUM at seq 4 must shrink the DB (was {} now {})",
        state_at[&1].len(),
        state_at[&4].len()
    );
    let rows: Vec<(i64, String)> = query_rows(&db_path);
    drop(conn);

    Fixture {
        store,
        prefix: prefix.to_string(),
        db: db.to_string(),
        state_at,
        rows,
        max_seq: 14,
    }
}

#[tokio::test]
async fn owned_vacuum_shrink_merges_and_restores_row_exact() {
    let fx = build_vacuum_fixture("vac/", "db");
    let layout = SeqLayout::new(fx.store.clone(), &fx.prefix, &fx.db);
    // Merges [2,5],[6,9],[10,13] then L2 [2,9] all span/cross the seq-4 shrink.
    compact_to_l1_l2(&layout).await;

    // The superseded fine tail is gone; the merged objects carry the shrink.
    let l0 = fx.store.list("vac/db/0000/", None).await.unwrap();
    assert_eq!(l0.len(), 1, "only the fine tail seq 14 remains: {l0:?}");

    let dir = tempfile::tempdir().unwrap();

    // Restore-to-latest through the shrink-spanning merged objects: byte- and
    // row-exact vs ground truth + integrity. This is the orphan-page elision
    // fix exercised under real SQLite VACUUM behavior.
    let out = dir.path().join("latest.db");
    let seq = walrust_core::sync::restore(fx.store.clone(), &fx.prefix, &fx.db, &out, None)
        .await
        .unwrap();
    assert_eq!(seq, fx.max_seq);
    assert!(integrity_ok(&out), "restored DB passes integrity_check");
    assert_eq!(query_rows(&out), fx.rows, "restore-to-latest is row-exact");
    assert_eq!(
        std::fs::read(&out).unwrap(),
        fx.state_at[&fx.max_seq],
        "restore-to-latest is byte-exact across the VACUUM"
    );

    // PITR to the L2 boundary (seq 9, post-shrink) restores exactly too.
    let out9 = dir.path().join("at9.db");
    walrust_core::sync::restore(fx.store.clone(), &fx.prefix, &fx.db, &out9, Some("9"))
        .await
        .unwrap();
    assert!(integrity_ok(&out9));
    assert_eq!(std::fs::read(&out9).unwrap(), fx.state_at[&9]);
}
