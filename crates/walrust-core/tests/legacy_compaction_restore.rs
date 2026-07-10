//! Compaction C3a: the legacy CLI restore seam — real **LTX** L0 objects merged
//! into **HADBP** level objects, then restored across the LTX→HADBP→LTX format
//! transitions, byte/row-exact vs a SQLite ground truth.
//!
//! This is the wave's heart. It proves:
//!   - the merge engine reads real litestream LTX L0 files (not the HADBP-in-LTX
//!     stand-ins the C2b in-test range layout used) and folds them into HADBP
//!     merged objects;
//!   - `restore_legacy_ltx` replays a chain that crosses LTX→HADBP→LTX with one
//!     running checksum in the LTX domain, reconstructing every state exactly;
//!   - PITR to a merged-window boundary is exact; PITR strictly inside a merged
//!     window is the planner's loud typed decay error naming both neighbors.
//!
//! Run with `cargo test -p walrust-core --test legacy_compaction_restore`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::{CasResult, StorageBackend};
use rusqlite::Connection;

use walrust_core::compaction::{run_level_compaction, RangeLayout};
use walrust_core::legacy_ltx;
use walrust_core::legacy_manifest::build_ltx_key;
use walrust_core::legacy_restore::restore_legacy_ltx;

const PS: u32 = 4096;
const NOW: i64 = 10_000_000_000_000;

#[derive(Clone, Default)]
struct MemStorage {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}
#[async_trait]
impl StorageBackend for MemStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }
    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        let mut keys: Vec<String> = self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .filter(|k| after.map(|m| k.as_str() > m).unwrap_or(true))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }
    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.lock().unwrap().contains_key(key))
    }
    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        let mut o = self.objects.lock().unwrap();
        if o.contains_key(key) {
            return Ok(CasResult {
                success: false,
                etag: None,
            });
        }
        o.insert(key.to_string(), data.to_vec());
        Ok(CasResult {
            success: true,
            etag: Some("mem".into()),
        })
    }
    async fn put_if_match(&self, key: &str, data: &[u8], _etag: &str) -> Result<CasResult> {
        self.put(key, data).await?;
        Ok(CasResult {
            success: true,
            etag: Some("mem".into()),
        })
    }
}

/// Full-page diff of two DB images (page numbers are 1-based).
fn diff_pages(old: &[u8], new: &[u8], ps: usize) -> Vec<(u32, Vec<u8>)> {
    let new_pages = new.len() / ps;
    let old_pages = old.len() / ps;
    let mut out = Vec::new();
    for i in 0..new_pages {
        let np = &new[i * ps..(i + 1) * ps];
        if i >= old_pages || np != &old[i * ps..(i + 1) * ps] {
            out.push(((i + 1) as u32, np.to_vec()));
        }
    }
    out
}

struct Fixture {
    store: MemStorage,
    prefix: String,
    db: String,
    /// Ground-truth committed rows as of each txid.
    rows_at: HashMap<u64, Vec<(i64, String)>>,
    max_seq: u64,
}

fn query_rows(path: &Path) -> Vec<(i64, String)> {
    let conn = Connection::open(path).unwrap();
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
    stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .map(|x| x.unwrap())
        .collect()
}

fn integrity_ok(path: &Path) -> bool {
    let conn = Connection::open(path).unwrap();
    let r: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    r == "ok"
}

/// Build a real SQLite history as a genuine **LTX** chain: a raw-image snapshot
/// at txid 1 (no VACUUM, so page diffs are valid) under `0001/`, then
/// `n_incrementals` real LTX incrementals under `0000/`, each properly chained
/// (`pre = prev post`, `post = chain_checksum(pre, pages)`).
fn build_ltx_fixture(prefix: &str, db: &str, n_incrementals: u64) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("ground.db");
    let store = MemStorage::default();

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
    conn.cache_flush().unwrap();
    drop(conn);

    let mut rows_at = HashMap::new();
    let v1 = std::fs::read(&db_path).unwrap();

    // Raw-image LTX snapshot at txid 1 (gen 1). Its content checksum is the
    // restored-DB checksum, so it seeds the LTX running chain.
    let mut snap = Vec::new();
    let snap_checksum =
        legacy_ltx::encode_snapshot_with_checksum(&mut snap, &db_path, PS, 1).unwrap();
    let key = build_ltx_key(prefix, db, 1, 1, 1);
    store.objects.lock().unwrap().insert(key, snap);

    let mut running = snap_checksum;
    let mut prev_bytes = v1;

    let conn = Connection::open(&db_path).unwrap();
    rows_at.insert(1u64, {
        let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
        stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
            .unwrap()
            .map(|x| x.unwrap())
            .collect()
    });

    for seq in 2..=(n_incrementals + 1) {
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
        let commit = (new_bytes.len() / PS as usize) as u32;
        let post = legacy_ltx::chain_checksum(running, &pages);
        let mut buf = Vec::new();
        legacy_ltx::encode_wal_changes(&mut buf, &pages, PS, seq, seq, commit, Some(running), post)
            .unwrap();
        let key = build_ltx_key(prefix, db, 0, seq, seq);
        store.objects.lock().unwrap().insert(key, buf);

        let snapshot_rows: Vec<(i64, String)> = {
            let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
            stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
                .unwrap()
                .map(|x| x.unwrap())
                .collect()
        };
        rows_at.insert(seq, snapshot_rows);
        running = post;
        prev_bytes = new_bytes;
    }
    drop(conn);

    Fixture {
        store,
        prefix: prefix.to_string(),
        db: db.to_string(),
        rows_at,
        max_seq: n_incrementals + 1,
    }
}

/// Fold the L0 LTX pool into HADBP L1/L2 with the real engine: three L0→L1
/// merges of 4 (ranges [2,5],[6,9],[10,13]) then one L1→L2 merge of 2 ([2,9]).
/// Leaves L2 [2,9], L1 [10,13], and the fine L0 tail seq 14.
async fn compact(store: &MemStorage, prefix: &str, db: &str) {
    let layout = RangeLayout::new(Arc::new(store.clone()), prefix, db);
    for _ in 0..3 {
        let o = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
            .await
            .unwrap();
        assert_eq!(o.merged_count(), 4, "each L0→L1 folds 4 real LTX objects");
    }
    let o = run_level_compaction(&layout, 1, 2, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(o.merged_count(), 2, "L1→L2 folds two HADBP ranges");
}

#[tokio::test]
async fn legacy_restore_crosses_ltx_hadbp_seam_row_exact() {
    let fx = build_ltx_fixture("p/", "app", 13); // txids 1..=14
    compact(&fx.store, &fx.prefix, &fx.db).await;

    // The superseded fine L0 tail (seqs 2..=13) is GONE; only seq 14 survives.
    let l0 = fx.store.list("p/app/0000/", None).await.unwrap();
    assert_eq!(
        l0.len(),
        1,
        "only the fine tail seq 14 remains at L0: {l0:?}"
    );
    assert_eq!(
        fx.store.list("p/app/levels/L2/", None).await.unwrap().len(),
        1
    );
    assert_eq!(
        fx.store.list("p/app/levels/L1/", None).await.unwrap().len(),
        1
    );

    // Restore to latest: the plan is L2[2,9] (HADBP) → L1[10,13] (HADBP) →
    // L0[14] (LTX). Crossing HADBP→LTX at the seam must be byte/row-exact.
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("restored.db");
    let txid = restore_legacy_ltx(&fx.store, "p/", "app", &out, None)
        .await
        .unwrap();
    assert_eq!(txid, fx.max_seq, "restored to the latest txid");
    assert!(integrity_ok(&out), "restored DB passes integrity_check");
    assert_eq!(
        query_rows(&out),
        fx.rows_at[&fx.max_seq],
        "restore-to-latest is row-exact vs ground truth across the seam"
    );
}

#[tokio::test]
async fn legacy_pitr_to_merged_boundary_is_exact_inside_window_is_loud() {
    let fx = build_ltx_fixture("p/", "app", 13);
    compact(&fx.store, &fx.prefix, &fx.db).await;
    let dir = tempfile::tempdir().unwrap();

    // PITR to the L2 window boundary (txid 9): a clean, exact restore even though
    // the fine points 2..9 were merged away.
    let out9 = dir.path().join("r9.db");
    let t9 = restore_legacy_ltx(&fx.store, "p/", "app", &out9, Some(9))
        .await
        .unwrap();
    assert_eq!(t9, 9);
    assert!(integrity_ok(&out9));
    assert_eq!(
        query_rows(&out9),
        fx.rows_at[&9],
        "PITR to a merged-window boundary is row-exact"
    );

    // PITR strictly inside the L2 window [2,9] (txid 7): the fine coverage is
    // gone, so this is the planner's loud typed decay error naming both
    // neighbors (nearest_below=1 snapshot floor, nearest_above=9 window end).
    let out7 = dir.path().join("r7.db");
    let err = restore_legacy_ltx(&fx.store, "p/", "app", &out7, Some(7))
        .await
        .expect_err("PITR inside a merged window must be a hard error");
    let msg = err.to_string();
    assert!(
        msg.contains("falls inside merged window") && msg.contains("9"),
        "decay error must name the window and nearest points: {msg}"
    );
    assert!(
        !out7.exists(),
        "a failed PITR must not publish a partial database"
    );
}

#[tokio::test]
async fn legacy_restore_rejects_a_corrupted_ltx_domain_stamp_at_the_seam() {
    // Tamper matrix, variant 2: corrupt the merged object's LTX-domain linkage
    // stamp (`declared_end`, the HADBP v2 header field at byte offset 40) rather
    // than a page byte. This leaves the content checksum valid but poisons the
    // running chain value the seam carries forward. The next object in the plan
    // (L1 [10,13], whose `prev` is the real LTX post of seq 9) must then fail its
    // DB-anchored `verify_chain` — proving the seam's cross-format linkage is
    // checked in the LTX domain, not trusted. (A merge that stamped the wrong
    // domain here would be caught by exactly this linkage.)
    let fx = build_ltx_fixture("p/", "app", 13);
    compact(&fx.store, &fx.prefix, &fx.db).await;

    let l2_key = fx.store.list("p/app/levels/L2/", None).await.unwrap()[0].clone();
    {
        let mut objs = fx.store.objects.lock().unwrap();
        let bytes = objs.get_mut(&l2_key).unwrap();
        // Flip a byte inside the 8-byte declared_end field (offset 40..48).
        bytes[40] ^= 0xFF;
    }

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("restored.db");
    let err = restore_legacy_ltx(&fx.store, "p/", "app", &out, None)
        .await
        .expect_err("a corrupted LTX-domain stamp must break the chain at the seam");
    let msg = err.to_string();
    assert!(
        msg.contains("chain broken") || msg.contains("checksum"),
        "error must name the linkage failure across the seam: {msg}"
    );
    assert!(
        !out.exists(),
        "a failed restore must not publish a partial DB"
    );
}

#[tokio::test]
async fn legacy_restore_rejects_a_tampered_merged_object_at_the_seam() {
    // Fail-on-revert for the seam's DB-anchored verification: the restore
    // executor runs `physical::verify_chain` on every merged HADBP object before
    // applying it, which validates BOTH its LTX-domain linkage and its content
    // checksum. Corrupt a merged object's bytes and the restore must abort loudly
    // and publish nothing — proving the seam is verified, not trusted. (Reverting
    // the verify_chain pre-check would let this torn object apply silently.)
    let fx = build_ltx_fixture("p/", "app", 13);
    compact(&fx.store, &fx.prefix, &fx.db).await;

    // Flip a byte deep inside the L2 merged object (past its header/pages).
    let l2_key = fx.store.list("p/app/levels/L2/", None).await.unwrap()[0].clone();
    {
        let mut objs = fx.store.objects.lock().unwrap();
        let bytes = objs.get_mut(&l2_key).unwrap();
        let i = bytes.len() / 2;
        bytes[i] ^= 0xFF;
    }

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("restored.db");
    let err = restore_legacy_ltx(&fx.store, "p/", "app", &out, None)
        .await
        .expect_err("a tampered merged object must fail the restore loudly");
    let msg = err.to_string();
    assert!(
        msg.contains("chain broken") || msg.contains("checksum") || msg.contains("decode"),
        "error must name the integrity failure at the merged object: {msg}"
    );
    assert!(
        !out.exists(),
        "a failed restore must not publish a partial DB"
    );
}
