//! Compaction C2a: end-to-end engine tests, the merge oracle, adapter coverage,
//! and the safety/revert proofs.
//!
//! Run with `cargo test -p walrust-core --test compaction_engine`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hadb_changeset::apply::apply_physical;
use hadb_changeset::physical::{self, chain_end, PageEntry, PageId, PageIdSize, PhysicalChangeset};
use hadb_storage::{CasResult, StorageBackend};

use walrust_core::compaction::{
    layout::CompactionLayout, run_level_compaction, CompactionError, CompactionOutcome, LayoutFile,
    Level, RangeLayout, SeqLayout, SeqRange,
};

// ── Minimal in-memory backend (correct range_get via default slice) ─────────

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

// ── Helpers ─────────────────────────────────────────────────────────────────

const PS: u32 = 4096;

/// A "now" far in the future so real `created_ms` stamps (~1.7e12) are always
/// older than it — with a zero window this makes every L0 file eligible.
const NOW: i64 = 10_000_000_000_000;

fn pg(id: u64, fill: u8, len: usize) -> PageEntry {
    PageEntry {
        page_id: PageId::U64(id),
        data: vec![fill; len],
    }
}

/// Build a normal L0 changeset chaining from `prev`.
fn cs(seq: u64, prev: u64, pages: Vec<PageEntry>) -> PhysicalChangeset {
    PhysicalChangeset::new(seq, prev, PageIdSize::U64, PS, pages)
}

/// Put an L0 seq-layout object (`{prefix}{db}/0000/{seq:016x}.hadbp`).
async fn put_l0_seq(store: &MemStore, prefix: &str, db: &str, cs: &PhysicalChangeset) {
    let key = format!("{prefix}{db}/0000/{:016x}.hadbp", cs.header.seq);
    store.put(&key, &physical::encode(cs)).await.unwrap();
}

/// Put an L0 range-layout object (`{prefix}{db}/0000/{min:016x}-{max:016x}.ltx`).
async fn put_l0_range(store: &MemStore, prefix: &str, db: &str, cs: &PhysicalChangeset) {
    let s = cs.header.seq;
    let key = format!("{prefix}{db}/0000/{:016x}-{:016x}.ltx", s, s);
    store.put(&key, &physical::encode(cs)).await.unwrap();
}

/// A contiguous chain of `n` L0 changesets starting at seq `start`, each
/// touching page `(seq % width)` plus overlapping page 0. Returns the chain.
fn make_chain(start: u64, n: u64, prev0: u64) -> Vec<PhysicalChangeset> {
    let mut out = Vec::new();
    let mut prev = prev0;
    for i in 0..n {
        let seq = start + i;
        // Every changeset rewrites page 0 (last-writer-wins target) and a
        // unique page, so the merge exercises both overwrite and unique paths.
        let c = cs(
            seq,
            prev,
            vec![pg(0, seq as u8, 16), pg(seq, 0xF0 | (i as u8 & 0x0f), 16)],
        );
        prev = chain_end(&c);
        out.push(c);
    }
    out
}

// ── Merge oracle: merged output == applying all sources in order ─────────────

/// Apply a slice of changesets to a fresh temp DB, in order, returning bytes.
fn apply_all(chain: &[PhysicalChangeset], base_prev: u64) -> Vec<u8> {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let mut prev = base_prev;
    for c in chain {
        apply_physical(tmp.path(), c, prev).unwrap();
        prev = chain_end(c);
    }
    std::fs::read(tmp.path()).unwrap()
}

/// Apply a single (merged) changeset to a fresh temp DB, returning bytes.
fn apply_one(c: &PhysicalChangeset, base_prev: u64) -> Vec<u8> {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    apply_physical(tmp.path(), c, base_prev).unwrap();
    std::fs::read(tmp.path()).unwrap()
}

async fn merged_via_engine(chain: &[PhysicalChangeset]) -> PhysicalChangeset {
    let store = MemStore::new();
    for c in chain {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");
    let outcome = run_level_compaction(&layout, 0, chain.len(), Duration::from_secs(0), NOW)
        .await
        .unwrap();
    let output = match outcome {
        CompactionOutcome::Merged { output, .. } => output,
        other => panic!("expected Merged, got {other:?}"),
    };
    let bytes = store.get(&output.key).await.unwrap().unwrap();
    physical::decode(&bytes).unwrap()
}

#[tokio::test]
async fn oracle_merged_equals_applying_sources_in_order() {
    // Table-driven cases: overwrite-in-later, only-first, only-last, interleaved.
    let cases: Vec<Vec<PhysicalChangeset>> = vec![
        // page overwritten in later source (both touch page 0)
        vec![cs(1, 0, vec![pg(0, 0xAA, 16), pg(1, 0x11, 16)]), {
            let prev = chain_end(&cs(1, 0, vec![pg(0, 0xAA, 16), pg(1, 0x11, 16)]));
            cs(2, prev, vec![pg(0, 0xBB, 16), pg(2, 0x22, 16)])
        }],
        // page only in first / only in last (disjoint) + interleaved via chain
        make_chain(10, 5, 0),
        make_chain(100, 8, 0),
    ];

    for (i, chain) in cases.iter().enumerate() {
        let base_prev = chain[0].header.prev_checksum;
        let via_sources = apply_all(chain, base_prev);
        let merged = merged_via_engine(chain).await;
        let via_merged = apply_one(&merged, base_prev);
        assert_eq!(
            via_sources, via_merged,
            "case {i}: merged output must be byte-identical to applying all sources"
        );
        // Linkage: merged chains from first.prev to last.chain_end.
        assert_eq!(merged.header.prev_checksum, base_prev);
        assert_eq!(chain_end(&merged), chain_end(chain.last().unwrap()));
    }
}

// ── End-to-end engine over both adapters ─────────────────────────────────────

async fn run_e2e_for<L: CompactionLayout>(
    store: Arc<MemStore>,
    layout: L,
    l0_dir: &str,
    l1_dir: &str,
    chain: &[PhysicalChangeset],
) {
    // Sources present at L0.
    assert_eq!(store.list(l0_dir, None).await.unwrap().len(), chain.len());

    let outcome = run_level_compaction(&layout, 0, chain.len(), Duration::from_secs(0), NOW)
        .await
        .unwrap();
    let merged_count = outcome.merged_count();
    assert_eq!(merged_count, chain.len());

    // Sources deleted, one L1 object written.
    assert!(store.list(l0_dir, None).await.unwrap().is_empty());
    let l1 = store.list(l1_dir, None).await.unwrap();
    assert_eq!(l1.len(), 1, "exactly one merged object");

    // The L1 object decodes and chains end-to-end.
    let bytes = store.get(&l1[0]).await.unwrap().unwrap();
    let cs = physical::decode(&bytes).unwrap();
    assert!(cs.is_compacted());
    assert_eq!(cs.header.prev_checksum, chain[0].header.prev_checksum);
    assert_eq!(chain_end(&cs), chain_end(chain.last().unwrap()));
}

#[tokio::test]
async fn engine_e2e_seq_layout() {
    let store = MemStore::new();
    let chain = make_chain(1, 6, 0);
    for c in &chain {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");
    run_e2e_for(
        store.clone(),
        layout,
        "p/db/0000/",
        "p/db/levels/L1/",
        &chain,
    )
    .await;
}

#[tokio::test]
async fn engine_e2e_range_layout() {
    let store = MemStore::new();
    let chain = make_chain(1, 6, 0);
    for c in &chain {
        put_l0_range(&store, "p/", "db", c).await;
    }
    let layout = RangeLayout::new(store.clone(), "p/", "db");
    run_e2e_for(
        store.clone(),
        layout,
        "p/db/0000/",
        "p/db/levels/L1/",
        &chain,
    )
    .await;
}

// ── keep_fine_window: young files exempt ─────────────────────────────────────

#[tokio::test]
async fn young_l0_files_are_exempt_and_back_off() {
    let store = MemStore::new();
    let chain = make_chain(1, 4, 0);
    for c in &chain {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");
    // now = created_ms(≈real now) but window is huge → all files younger →
    // nothing eligible → NoOp, sources preserved.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let outcome = run_level_compaction(&layout, 0, 4, Duration::from_secs(24 * 3600), now)
        .await
        .unwrap();
    assert!(matches!(outcome, CompactionOutcome::NoOp));
    assert_eq!(store.list("p/db/0000/", None).await.unwrap().len(), 4);
    assert!(store
        .list("p/db/levels/L1/", None)
        .await
        .unwrap()
        .is_empty());
}

// ── Revert proof 1: read-back verify failure preserves sources ──────────────

/// Wraps a layout and corrupts the bytes returned by `read_bytes` (the
/// read-back step) so decode fails, simulating a torn/silently-corrupted
/// merged object surfacing at verification.
struct CorruptReadBack<L> {
    inner: L,
}
#[async_trait]
impl<L: CompactionLayout> CompactionLayout for CorruptReadBack<L> {
    async fn list_level(&self, level: Level) -> Result<Vec<LayoutFile>, CompactionError> {
        self.inner.list_level(level).await
    }
    async fn read_header(
        &self,
        file: &LayoutFile,
    ) -> Result<walrust_core::compaction::layout::SourceHeader, CompactionError> {
        self.inner.read_header(file).await
    }
    async fn open(
        &self,
        file: &LayoutFile,
    ) -> Result<Box<dyn walrust_core::compaction::layout::ChangesetPageStream>, CompactionError>
    {
        self.inner.open(file).await
    }
    async fn write_merged(
        &self,
        level: Level,
        range: SeqRange,
        bytes: &[u8],
    ) -> Result<LayoutFile, CompactionError> {
        self.inner.write_merged(level, range, bytes).await
    }
    async fn read_bytes(&self, file: &LayoutFile) -> Result<Vec<u8>, CompactionError> {
        let mut bytes = self.inner.read_bytes(file).await?;
        // Flip a page byte so the trailer checksum no longer matches.
        if bytes.len() > 45 {
            bytes[44] ^= 0xFF;
        }
        Ok(bytes)
    }
    async fn delete(&self, files: &[LayoutFile]) -> Result<(), CompactionError> {
        self.inner.delete(files).await
    }
}

#[tokio::test]
async fn revert_proof_readback_failure_preserves_sources() {
    let store = MemStore::new();
    let chain = make_chain(1, 5, 0);
    for c in &chain {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = CorruptReadBack {
        inner: SeqLayout::new(store.clone(), "p/", "db"),
    };
    let err = run_level_compaction(&layout, 0, 5, Duration::from_secs(0), NOW)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CompactionError::VerificationFailed(_)),
        "expected typed VerificationFailed, got {err:?}"
    );
    // Sources are UNTOUCHED (never deleted against an unverified object).
    assert_eq!(store.list("p/db/0000/", None).await.unwrap().len(), 5);
    // The partial/unsound output was deleted (loud-failure posture).
    assert!(store
        .list("p/db/levels/L1/", None)
        .await
        .unwrap()
        .is_empty());
}

// ── Revert proof 2: crash between write and delete → idempotent converge ─────

#[tokio::test]
async fn revert_proof_crash_between_write_and_delete_converges() {
    let store = MemStore::new();
    let chain = make_chain(1, 5, 0);
    for c in &chain {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");

    // First run: do the full merge to obtain the authoritative merged bytes,
    // then re-seed the sources to simulate a crash that wrote the merged object
    // but died before deleting sources.
    let first = run_level_compaction(&layout, 0, 5, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    let merged_key = match &first {
        CompactionOutcome::Merged { output, .. } => output.key.clone(),
        other => panic!("expected Merged, got {other:?}"),
    };
    let merged_bytes = store.get(&merged_key).await.unwrap().unwrap();

    // Re-seed sources (they were deleted by the first run) AND keep the merged
    // object in place → the exact post-crash overlap state.
    for c in &chain {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    assert_eq!(store.list("p/db/0000/", None).await.unwrap().len(), 5);
    assert!(store.get(&merged_key).await.unwrap().is_some());

    // Re-run: must CONVERGE (verify existing, delete sources) without error and
    // without rewriting the merged object.
    let second = run_level_compaction(&layout, 0, 5, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert!(
        matches!(second, CompactionOutcome::ConvergedExistingDeletion { .. }),
        "expected idempotent convergence, got {second:?}"
    );
    // The merged object is byte-identical (not re-merged) and sources are gone.
    assert_eq!(store.get(&merged_key).await.unwrap().unwrap(), merged_bytes);
    assert!(store.list("p/db/0000/", None).await.unwrap().is_empty());
    assert_eq!(store.list("p/db/levels/L1/", None).await.unwrap().len(), 1);
}

// ── Revert proof 3: non-contiguous sources rejected, nothing deleted/written ─

#[tokio::test]
async fn revert_proof_non_contiguous_sources_rejected() {
    let store = MemStore::new();
    // Seqs 1,2, then a GAP (skip 3), then 4 chained from a wrong prev so the
    // chain is broken between 2 and 4.
    let c1 = cs(1, 0, vec![pg(0, 1, 16)]);
    let c2 = cs(2, chain_end(&c1), vec![pg(0, 2, 16)]);
    let c4 = cs(4, 0xDEAD_BEEF, vec![pg(0, 4, 16)]); // prev does not chain
    for c in [&c1, &c2, &c4] {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");
    let err = run_level_compaction(&layout, 0, 3, Duration::from_secs(0), NOW)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CompactionError::NonContiguous(_)),
        "expected NonContiguous, got {err:?}"
    );
    // Nothing written at L1, sources preserved.
    assert!(store
        .list("p/db/levels/L1/", None)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.list("p/db/0000/", None).await.unwrap().len(), 3);
}

// ── Cascade: L0→L1 then L1→L2 across levels ──────────────────────────────────

#[tokio::test]
async fn cascade_l0_to_l1_to_l2() {
    let store = MemStore::new();
    // 4 L0 files; merge into 2 L1 files of 2 each; then merge those into 1 L2.
    let chain = make_chain(1, 4, 0);
    for c in &chain {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");

    // First L0→L1: batch 2 → merges seqs 1,2.
    let o1 = run_level_compaction(&layout, 0, 2, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(o1.merged_count(), 2);
    // Second L0→L1: batch 2 → merges seqs 3,4.
    let o2 = run_level_compaction(&layout, 0, 2, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(o2.merged_count(), 2);

    let l1 = store.list("p/db/levels/L1/", None).await.unwrap();
    assert_eq!(l1.len(), 2, "two L1 files");

    // L1→L2: merge the two L1 files (compacted sources → declared_end path).
    let o3 = run_level_compaction(&layout, 1, 2, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(o3.merged_count(), 2);
    assert!(store
        .list("p/db/levels/L1/", None)
        .await
        .unwrap()
        .is_empty());
    let l2 = store.list("p/db/levels/L2/", None).await.unwrap();
    assert_eq!(l2.len(), 1, "one L2 file");

    // The L2 file must span the whole original chain and chain end-to-end.
    let bytes = store.get(&l2[0]).await.unwrap().unwrap();
    let merged = physical::decode(&bytes).unwrap();
    assert!(merged.is_compacted());
    assert_eq!(merged.header.prev_checksum, 0);
    assert_eq!(chain_end(&merged), chain_end(chain.last().unwrap()));

    // Oracle across levels: L2 applied == all 4 sources applied in order.
    let via_sources = apply_all(&chain, 0);
    let via_l2 = apply_one(&merged, 0);
    assert_eq!(via_sources, via_l2, "cross-level merge preserves DB bytes");
}

// ── Collision proof: compaction levels vs legacy snapshot generations ────────

/// The headline of this review. In the litestream-heritage layout, snapshots
/// increment the generation folder by one per snapshot (`snapshot_gen =
/// current_gen + 1`), so the 16th snapshot lands in `0010/` — the folder an
/// earlier design reserved for compaction L1. This test pins the redesign:
/// compaction levels live under a dedicated `levels/L{n}/` sub-path that legacy
/// discovery cannot see, and compaction listing cannot see legacy snapshots.
#[tokio::test]
async fn compaction_levels_do_not_collide_with_legacy_snapshot_generations() {
    use walrust_core::compaction::layout::CompactionLayout;
    use walrust_core::legacy_manifest::{
        build_ltx_key, discover_all_legacy_ltx, discover_legacy_state,
    };

    let store = MemStore::new();

    // A real legacy LTX snapshot in generation 0x0010 — the 16th snapshot of a
    // long-lived database. This is the folder the OLD compaction scheme
    // reserved for L1.
    let snap16 = build_ltx_key("", "db", 0x10, 1, 100);
    store.put(&snap16, b"a-real-ltx-snapshot").await.unwrap();

    // Seed L0 range-layout sources and run an L0→L1 compaction.
    let chain = make_chain(1, 6, 0);
    for c in &chain {
        put_l0_range(&store, "", "db", c).await;
    }
    let layout = RangeLayout::new(store.clone(), "", "db");

    // Compaction's L1 listing must NOT pick up the legacy snapshot in gen 0x0010.
    assert!(
        layout.list_level(1).await.unwrap().is_empty(),
        "compaction L1 listing must not ingest the legacy snapshot at gen 0x0010"
    );

    let outcome = run_level_compaction(&layout, 0, chain.len(), Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(outcome.merged_count(), chain.len());

    // The merged L1 object lives under levels/, and gen 0x0010 still holds ONLY
    // the legacy snapshot (compaction wrote nothing there).
    assert_eq!(
        store.list("db/levels/L1/", None).await.unwrap().len(),
        1,
        "L1 merged object must live under db/levels/L1/"
    );
    assert_eq!(
        store.list("db/0010/", None).await.unwrap(),
        vec![snap16.clone()],
        "db/0010/ must contain only the legacy snapshot, never compaction output"
    );

    // Legacy discovery is blind to compaction levels: the `levels/` dir does not
    // register as a generation, and no compaction object is surfaced as a legacy
    // LTX/snapshot.
    let (_txid, max_gen) = discover_legacy_state(&*store, "", "db").await.unwrap();
    assert_eq!(
        max_gen, 0x10,
        "the levels/ sub-path must not register as a generation"
    );
    let all = discover_all_legacy_ltx(&*store, "", "db").await.unwrap();
    assert!(
        all.iter().all(|f| !f.key.contains("levels/")),
        "legacy discovery must never surface a compaction level object: {:?}",
        all.iter().map(|f| f.key.clone()).collect::<Vec<_>>()
    );
}

// ── Convergence is exact-range only: a partial overlap is a loud error ───────

#[tokio::test]
async fn partial_overlap_existing_merged_is_loud_error() {
    let store = MemStore::new();
    let chain = make_chain(1, 4, 0);
    for c in &chain {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");

    // A prior run with a SMALLER batch merged only seqs 1-2 into L1 (and deleted
    // those two sources). Re-seed them so a full batch-4 run is possible again.
    let first = run_level_compaction(&layout, 0, 2, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(first.merged_count(), 2);
    for c in &chain[..2] {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    assert_eq!(store.list("p/db/0000/", None).await.unwrap().len(), 4);
    assert_eq!(store.list("p/db/levels/L1/", None).await.unwrap().len(), 1);

    // A batch-4 run targets range 1-4, which is a SUPERSET of the existing 1-2
    // L1 object — an overlap that is not an exact match. This must be a loud
    // error, with nothing written or deleted.
    let err = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CompactionError::OverlappingExisting(_)),
        "partial-overlap existing merged object must be a loud error, got {err:?}"
    );
    // All four L0 sources survive; the pre-existing L1 object is untouched.
    assert_eq!(store.list("p/db/0000/", None).await.unwrap().len(), 4);
    assert_eq!(store.list("p/db/levels/L1/", None).await.unwrap().len(), 1);
}
