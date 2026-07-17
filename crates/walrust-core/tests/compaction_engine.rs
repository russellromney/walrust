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
    Level, SeqLayout, SeqRange,
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
    // Seqs 1,2,3 with NO seq gap, but seq 3 chains from a wrong prev — a fork /
    // chain-break that the cheap seq-contiguity batch selector cannot see. The
    // C3a liveness clip therefore selects all three (they look contiguous), and
    // the merge's checksum-contiguity net MUST still reject it. This proves the
    // merge never silently bridges a broken chain even when the seqs are dense.
    // (The seq-*gap* case — a snapshot boundary — is now clipped, not errored;
    // see `straddling_snapshot_break_converges_no_eternal_noncontiguous`.)
    let c1 = cs(1, 0, vec![pg(0, 1, 16)]);
    let c2 = cs(2, chain_end(&c1), vec![pg(0, 2, 16)]);
    let c3 = cs(3, 0xDEAD_BEEF, vec![pg(0, 3, 16)]); // seq-contiguous, prev broken
    for c in [&c1, &c2, &c3] {
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

// ── E11: interrupted-delete leftovers converge, they do not collide ──────────

/// A transient fault mid `delete_many` (a serial, non-atomic per-object loop)
/// leaves a STRICT SUBSET of a merged batch's sources alive. Those survivors are
/// folded into the written, verified merged object but no longer reconstitute
/// it; a later batch that mixes a survivor with a fresh source would compute a
/// CROSSING target range that step 2 loudly refuses. The engine must instead
/// converge the interrupted delete — drop the redundant subset-covered leftovers
/// (deletion-only) — so re-runs converge rather than raise `OverlappingExisting`.
///
/// **Fail-on-revert:** without `converge_interrupted_delete` the batch-4 run
/// below selects the survivors {2,3,4} plus fresh {5}, targets range [2,5], and
/// `find_existing_merged` returns `OverlappingExisting` against the existing
/// [1,4] — the exact E11 failure. (The FULL crash-recovery set, where the
/// survivors still tile the merged object, stays on step 2's strong verify — see
/// `revert_proof_crash_between_write_and_delete_converges`; a reconstituting
/// re-seed stays loud — see `partial_overlap_existing_merged_is_loud_error`.)
#[tokio::test]
async fn interrupted_delete_leftover_subset_converges_not_loud_error() {
    let store = MemStore::new();
    let chain = make_chain(1, 5, 0); // seqs 1..=5, one contiguous chain
    for c in &chain[..4] {
        put_l0_seq(&store, "p/", "db", c).await; // seqs 1,2,3,4 at L0
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");

    // Merge seqs 1..=4 → L1 [1-4]; sources 1..=4 deleted.
    let first = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(first.merged_count(), 4);
    let l1 = store.list("p/db/levels/L1/", None).await.unwrap();
    assert_eq!(l1.len(), 1);
    let merged_key = l1[0].clone();
    let merged_bytes = store.get(&merged_key).await.unwrap().unwrap();

    // Interrupted delete: seq 1 was deleted, seqs 2,3,4 survived, and a fresh
    // seq 5 has since landed at L0.
    for c in &chain[1..4] {
        put_l0_seq(&store, "p/", "db", c).await; // re-seed the survivors 2,3,4
    }
    put_l0_seq(&store, "p/", "db", &chain[4]).await; // fresh seq 5
    assert_eq!(store.list("p/db/0000/", None).await.unwrap().len(), 4);

    // Batch-4 run: converge the interrupted delete (drop 2,3,4), leaving only
    // the fresh seq 5. NO OverlappingExisting, nothing re-merged.
    let converged = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .expect("interrupted-delete leftovers must converge, not raise OverlappingExisting");
    assert!(
        matches!(
            converged,
            CompactionOutcome::ConvergedExistingDeletion { .. }
        ),
        "expected deletion-only convergence, got {converged:?}"
    );
    assert_eq!(
        converged.merged_count(),
        3,
        "the three subset-covered leftovers 2,3,4 were dropped"
    );

    // The merged object is byte-identical (untouched); only fresh seq 5 remains.
    assert_eq!(store.get(&merged_key).await.unwrap().unwrap(), merged_bytes);
    let l0_after = store.list("p/db/0000/", None).await.unwrap();
    assert_eq!(l0_after.len(), 1, "only fresh seq 5 remains: {l0_after:?}");
    assert!(l0_after[0].ends_with(&format!("{:016x}.hadbp", 5u64)));
    assert_eq!(store.list("p/db/levels/L1/", None).await.unwrap().len(), 1);
}

/// The convergence never deletes redundant sources against an UNSOUND covering
/// object: if the merged object that covers the leftovers is torn/corrupt,
/// `verify_covers` fails, the leftovers are preserved, and the state stays a loud
/// error rather than silently dropping coverage-bearing sources. This pins the
/// soundness gate on the deletion-only convergence.
#[tokio::test]
async fn interrupted_delete_does_not_drop_leftovers_against_torn_cover() {
    let store = MemStore::new();
    let chain = make_chain(1, 5, 0);
    for c in &chain[..4] {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");

    // Merge 1..=4 → L1 [1-4]; capture the merged key.
    let first = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(first.merged_count(), 4);
    let l1 = store.list("p/db/levels/L1/", None).await.unwrap();
    let merged_key = l1[0].clone();

    // Corrupt the covering object's page content on disk so its content
    // checksum no longer verifies (a real torn/silently-corrupt merged object).
    let mut bad = store.get(&merged_key).await.unwrap().unwrap();
    let mid = bad.len() / 2;
    bad[mid] ^= 0xFF;
    store.put(&merged_key, &bad).await.unwrap();

    // Re-seed the survivors + a fresh source (the interrupted-delete state).
    for c in &chain[1..4] {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    put_l0_seq(&store, "p/", "db", &chain[4]).await;
    assert_eq!(store.list("p/db/0000/", None).await.unwrap().len(), 4);

    // The covering object fails `verify_covers` (decode/content-checksum), so
    // the leftovers are NOT dropped against it; the state stays a loud error
    // rather than silently losing coverage.
    let err = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .expect_err("must not converge against an unsound covering object");
    assert!(
        matches!(err, CompactionError::OverlappingExisting(_)),
        "torn cover must not be converged against; leftovers stay a loud error, got {err:?}"
    );
    // The survivors are UNTOUCHED (never deleted against an unverified object).
    assert_eq!(store.list("p/db/0000/", None).await.unwrap().len(), 4);
}

/// Arbitrary (NON-prefix) leftover subsets converge too. The root-cause delete
/// loop is serial today, so real leftovers are suffixes — but delete
/// implementations change (a parallel `delete_many` leaves arbitrary subsets),
/// and the convergence must not depend on the leftover shape. Survivors
/// {1,2,4} exercise all three positions at once: the cover's `min` edge (prev
/// chain evidence), an interior seq, and the `max` edge (chain-end evidence).
#[tokio::test]
async fn interrupted_delete_arbitrary_subset_leftovers_converge() {
    let store = MemStore::new();
    let chain = make_chain(1, 5, 0);
    for c in &chain[..4] {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");

    let first = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(first.merged_count(), 4);
    let merged_key = store.list("p/db/levels/L1/", None).await.unwrap()[0].clone();
    let merged_bytes = store.get(&merged_key).await.unwrap().unwrap();

    // A hypothetical parallel delete dropped only seq 3: survivors {1,2,4}
    // (min edge + interior + max edge), plus a fresh seq 5.
    for c in [&chain[0], &chain[1], &chain[3], &chain[4]] {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    assert_eq!(store.list("p/db/0000/", None).await.unwrap().len(), 4);

    let converged = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .expect("a gappy leftover subset must converge");
    assert!(matches!(
        converged,
        CompactionOutcome::ConvergedExistingDeletion { .. }
    ));
    assert_eq!(converged.merged_count(), 3, "leftovers 1,2,4 dropped");

    assert_eq!(store.get(&merged_key).await.unwrap().unwrap(), merged_bytes);
    let l0_after = store.list("p/db/0000/", None).await.unwrap();
    assert_eq!(l0_after.len(), 1, "only fresh seq 5 remains: {l0_after:?}");
    assert!(l0_after[0].ends_with(&format!("{:016x}.hadbp", 5u64)));
}

/// Leftovers from TWO different interrupted merges (two distinct sound covers)
/// converge together in one deletion-only pass. Reachable when the convergence
/// deletion itself is cut short across ticks; the per-cover loop must not stop
/// at the first cover.
#[tokio::test]
async fn interrupted_deletes_from_two_merges_converge_together() {
    let store = MemStore::new();
    let chain = make_chain(1, 10, 0); // seqs 1..=10, one chain
    for c in &chain[..4] {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");
    let m1 = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(m1.merged_count(), 4); // L1 [1,4]
    for c in &chain[4..8] {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let m2 = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(m2.merged_count(), 4); // L1 [5,8]
    let l1 = store.list("p/db/levels/L1/", None).await.unwrap();
    assert_eq!(l1.len(), 2);

    // Interrupted-delete leftovers under BOTH covers, plus fresh 9,10.
    put_l0_seq(&store, "p/", "db", &chain[1]).await; // {2} under [1,4]
    put_l0_seq(&store, "p/", "db", &chain[5]).await; // {6} under [5,8]
    put_l0_seq(&store, "p/", "db", &chain[6]).await; // {7} under [5,8]
    put_l0_seq(&store, "p/", "db", &chain[8]).await; // fresh 9
    put_l0_seq(&store, "p/", "db", &chain[9]).await; // fresh 10

    let converged = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .expect("leftovers under two covers must converge in one pass");
    assert_eq!(
        converged.merged_count(),
        3,
        "2 (under [1,4]) and 6,7 (under [5,8]) all dropped"
    );
    let l0_after = store.list("p/db/0000/", None).await.unwrap();
    assert_eq!(l0_after.len(), 2, "fresh 9,10 remain: {l0_after:?}");
    assert_eq!(
        store.list("p/db/levels/L1/", None).await.unwrap().len(),
        2,
        "both covers untouched"
    );
}

/// A leftover that is subset of TWO overlapping sound covers is deleted (and
/// counted) exactly ONCE. The engine never writes overlapping same-level
/// covers itself, but the convergence must stay correct if one exists (e.g. an
/// operator-restored object), not double-delete or double-count.
#[tokio::test]
async fn leftover_subset_of_two_overlapping_covers_converges_once() {
    let store = MemStore::new();
    let chain = make_chain(1, 4, 0);
    for c in &chain {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");
    let first = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(first.merged_count(), 4); // L1 [1,4]

    // Handcraft a second, overlapping sound cover [3,6]. Its prev_checksum is
    // seq 3's prev (chain_end of seq 2), as a real [3,6] merge would stamp.
    let cover2 = PhysicalChangeset::new_compacted(
        6,
        chain_end(&chain[1]),
        PageIdSize::U64,
        PS,
        vec![pg(0, 0x66, 16)],
        0xC0FF_EE00,
    );
    let cover2_key = format!("p/db/levels/L1/{:016x}-{:016x}.hadbp", 3u64, 6u64);
    store
        .put(&cover2_key, &physical::encode(&cover2))
        .await
        .unwrap();

    // Leftover {3}: interior of [1,4] AND min-edge of [3,6].
    put_l0_seq(&store, "p/", "db", &chain[2]).await;

    let converged = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .expect("a doubly-covered leftover must converge");
    assert_eq!(
        converged.merged_count(),
        1,
        "one leftover, deleted and counted once (no double-count from two covers)"
    );
    assert!(store.list("p/db/0000/", None).await.unwrap().is_empty());
    assert_eq!(store.list("p/db/levels/L1/", None).await.unwrap().len(), 2);
}

/// Wraps a layout and interrupts ITS delete mid-loop: the first file is
/// deleted, then a transient error fires — the same non-atomic `delete_many`
/// shape that caused E11, now aimed at the convergence's own deletion.
struct InterruptDelete<L> {
    inner: L,
}
#[async_trait]
impl<L: CompactionLayout> CompactionLayout for InterruptDelete<L> {
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
        self.inner.read_bytes(file).await
    }
    async fn delete(&self, files: &[LayoutFile]) -> Result<(), CompactionError> {
        self.inner.delete(&files[..1]).await?;
        Err(CompactionError::Storage(
            "Storage error: Service unavailable (injected)".into(),
        ))
    }
}

/// Deletion-order safety: if the CONVERGENCE deletion is itself interrupted
/// mid-loop, the resulting state is never worse (cover + fresh sources
/// untouched, remaining leftovers still subset-covered) and a re-run converges
/// — the convergence is idempotent under its own fault.
#[tokio::test]
async fn interrupted_convergence_deletion_reconverges() {
    let store = MemStore::new();
    let chain = make_chain(1, 5, 0);
    for c in &chain[..4] {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");
    let first = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(first.merged_count(), 4);
    let merged_key = store.list("p/db/levels/L1/", None).await.unwrap()[0].clone();
    let merged_bytes = store.get(&merged_key).await.unwrap().unwrap();

    // Leftovers {2,3,4} + fresh 5.
    for c in &chain[1..5] {
        put_l0_seq(&store, "p/", "db", c).await;
    }

    // Tick 1: convergence deletion interrupted after one file — a loud,
    // retryable transient. State must be strictly "less leftover", never worse.
    let faulty = InterruptDelete {
        inner: SeqLayout::new(store.clone(), "p/", "db"),
    };
    let err = run_level_compaction(&faulty, 0, 4, Duration::from_secs(0), NOW)
        .await
        .expect_err("interrupted convergence deletion must propagate");
    assert!(
        matches!(&err, CompactionError::Storage(s) if s.contains("injected")),
        "retryable transient, got {err:?}"
    );
    let mid = store.list("p/db/0000/", None).await.unwrap();
    assert_eq!(mid.len(), 3, "exactly one leftover deleted: {mid:?}");
    assert!(
        mid.iter()
            .any(|k| k.ends_with(&format!("{:016x}.hadbp", 5u64))),
        "the fresh source is never touched: {mid:?}"
    );
    assert_eq!(store.get(&merged_key).await.unwrap().unwrap(), merged_bytes);

    // Tick 2: re-run converges the remaining leftovers.
    let converged = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .expect("re-run after interrupted convergence must converge");
    assert!(matches!(
        converged,
        CompactionOutcome::ConvergedExistingDeletion { .. }
    ));
    assert_eq!(converged.merged_count(), 2, "remaining leftovers dropped");
    let l0_after = store.list("p/db/0000/", None).await.unwrap();
    assert_eq!(l0_after.len(), 1, "only fresh 5 remains: {l0_after:?}");
    assert!(l0_after[0].ends_with(&format!("{:016x}.hadbp", 5u64)));
    assert_eq!(store.get(&merged_key).await.unwrap().unwrap(), merged_bytes);

    // Tick 3: nothing left to converge; quiet NoOp (eligible 1 < batch 4).
    let after = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert!(matches!(after, CompactionOutcome::NoOp));
}

/// The content-supersession invariant test (nearest reachable approximation of
/// the proven-unreachable divergence): a subset-BY-RANGE object at a cover's
/// endpoint whose chain linkage does NOT match the cover — the fork-artifact
/// shape a rogue second writer could leave in a compaction-vacated seq key —
/// is NEVER silently deleted. The endpoint chain evidence refuses it and the
/// state stays the loud `OverlappingExisting`.
///
/// **Fail-on-revert:** dropping the `endpoint_chain_evidence` gate makes the
/// run below silently delete the foreign seq-4 object (range-subset of a sound
/// cover) and return `ConvergedExistingDeletion` — data loss this test forbids.
#[tokio::test]
async fn foreign_endpoint_subset_is_preserved_not_converged() {
    let store = MemStore::new();
    let chain = make_chain(1, 4, 0);
    for c in &chain {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");
    let first = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(first.merged_count(), 4); // sound L1 [1,4]

    // A FOREIGN object lands at the vacated seq-4 key: same range position,
    // different lineage (its chain end cannot match the cover's declared end).
    let fork4 = cs(4, 0xDEAD_BEEF, vec![pg(0, 0x44, 16), pg(9, 0x99, 16)]);
    put_l0_seq(&store, "p/", "db", &fork4).await;
    assert_ne!(
        chain_end(&fork4),
        chain_end(&chain[3]),
        "the fork artifact genuinely diverges"
    );
    // A fresh seq 5 (whatever it chains from — selection only sees seqs).
    let c5 = cs(5, chain_end(&fork4), vec![pg(0, 0x55, 16)]);
    put_l0_seq(&store, "p/", "db", &c5).await;

    // batch 2 selects the contiguous run [4,5], which crosses the cover [1,4].
    // The convergence must NOT absorb the foreign seq 4; the collision stays a
    // loud error and nothing is deleted.
    let err = run_level_compaction(&layout, 0, 2, Duration::from_secs(0), NOW)
        .await
        .expect_err("a chain-divergent endpoint subset must not converge");
    assert!(
        matches!(err, CompactionError::OverlappingExisting(_)),
        "expected the loud alarm, got {err:?}"
    );
    let l0 = store.list("p/db/0000/", None).await.unwrap();
    assert_eq!(l0.len(), 2, "the foreign object is preserved: {l0:?}");
    assert_eq!(store.list("p/db/levels/L1/", None).await.unwrap().len(), 1);
}

/// Wraps a layout and fails `read_bytes` for merged-level objects with the
/// injected transient — a transient GET hitting exactly the cover soundness
/// check during convergence.
struct TransientCoverRead<L> {
    inner: L,
}
#[async_trait]
impl<L: CompactionLayout> CompactionLayout for TransientCoverRead<L> {
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
        if file.key.contains("/levels/") {
            return Err(CompactionError::Storage(
                "Storage error: Service unavailable (injected)".into(),
            ));
        }
        self.inner.read_bytes(file).await
    }
    async fn delete(&self, files: &[LayoutFile]) -> Result<(), CompactionError> {
        self.inner.delete(files).await
    }
}

/// A TRANSIENT read failure while checking a cover's soundness must propagate
/// as a retryable storage error — it is a read problem, not evidence the cover
/// is unsound. Swallowing it (skipping the cover) would let batch selection
/// collide with the cover and decay a retryable transient into a NON-retryable
/// `OverlappingExisting`: the E11 failure class reintroduced one GET deeper.
///
/// **Fail-on-revert:** treating any `verify_covers` error as "unsound, skip"
/// makes this run return `OverlappingExisting` instead of the retryable
/// `Storage` error, and the follow-up convergence assertion still holds only
/// because the leftovers were preserved.
#[tokio::test]
async fn transient_cover_read_stays_retryable_not_loud() {
    let store = MemStore::new();
    let chain = make_chain(1, 5, 0);
    for c in &chain[..4] {
        put_l0_seq(&store, "p/", "db", c).await;
    }
    let layout = SeqLayout::new(store.clone(), "p/", "db");
    let first = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .unwrap();
    assert_eq!(first.merged_count(), 4);

    // Leftovers {2,3} + fresh 5.
    put_l0_seq(&store, "p/", "db", &chain[1]).await;
    put_l0_seq(&store, "p/", "db", &chain[2]).await;
    put_l0_seq(&store, "p/", "db", &chain[4]).await;

    // Tick with the transient on the cover GET: retryable Storage error, no
    // deletion, no OverlappingExisting.
    let faulty = TransientCoverRead {
        inner: SeqLayout::new(store.clone(), "p/", "db"),
    };
    let err = run_level_compaction(&faulty, 0, 2, Duration::from_secs(0), NOW)
        .await
        .expect_err("the transient must surface");
    assert!(
        matches!(&err, CompactionError::Storage(s) if s.contains("injected")),
        "a transient cover read must stay a retryable Storage error, got {err:?}"
    );
    assert_eq!(
        store.list("p/db/0000/", None).await.unwrap().len(),
        3,
        "nothing deleted under a transient"
    );

    // Retry without the fault: converges.
    let converged = run_level_compaction(&layout, 0, 2, Duration::from_secs(0), NOW)
        .await
        .expect("retry after the transient converges");
    assert!(matches!(
        converged,
        CompactionOutcome::ConvergedExistingDeletion { .. }
    ));
    assert_eq!(converged.merged_count(), 2);
    let l0_after = store.list("p/db/0000/", None).await.unwrap();
    assert_eq!(l0_after.len(), 1, "only fresh 5 remains: {l0_after:?}");
}

// ── Liveness (C3a): batch selection clips at snapshot chain-breaks ───────────

/// A fixed-size L0 batch straddling a snapshot chain-break must NOT stall on
/// `NonContiguous` forever. The engine clips the batch to the contiguous run
/// (skipping a lone leading straddler) and converges.
///
/// **Fail-on-revert:** restoring the old rigid `all.into_iter().take(batch)`
/// selection makes the very first `run_level_compaction` below select the
/// straddling files (seqs 1,3,4,5) and return `CompactionError::NonContiguous`
/// on every tick forever — this test would then fail at the first `.unwrap()`.
#[tokio::test]
async fn straddling_snapshot_break_converges_no_eternal_noncontiguous() {
    let store = MemStore::new();

    // Seq 1 chains from 0. A snapshot then consumed seq 2 (not in L0), so the
    // post-snapshot chain restarts from the snapshot's checksum (0x5EED), a
    // genuine chain-break: cs(3).prev != chain_end(cs(1)).
    let c1 = cs(1, 0, vec![pg(0, 1, 16), pg(1, 0x11, 16)]);
    put_l0_seq(&store, "p/", "db", &c1).await;
    assert_ne!(chain_end(&c1), 0x5EED, "the snapshot breaks the chain");

    let post = make_chain(3, 5, 0x5EED); // seqs 3,4,5,6,7 contiguous
    for c in &post {
        put_l0_seq(&store, "p/", "db", c).await;
    }

    let layout = SeqLayout::new(store.clone(), "p/", "db");
    let l0_before = store.list("p/db/0000/", None).await.unwrap();
    assert_eq!(
        l0_before.len(),
        6,
        "seqs 1,3,4,5,6,7 present: {l0_before:?}"
    );

    // Tick 1: the oldest 4 files (1,3,4,5) straddle the break. Old code errors
    // NonContiguous here forever; the fix skips the lone straddler [1] and
    // merges the contiguous run [3,4,5,6].
    let o = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .expect("must not error across the chain-break");
    assert_eq!(o.merged_count(), 4, "merged the contiguous run [3..=6]");

    let l0 = store.list("p/db/0000/", None).await.unwrap();
    let l1 = store.list("p/db/levels/L1/", None).await.unwrap();
    assert_eq!(
        l0.len(),
        2,
        "only the straddler [1] and tail [7] remain: {l0:?}"
    );
    assert_eq!(l1.len(), 1, "one merged L1 range written: {l1:?}");
    assert!(
        l1[0].ends_with(&format!("{:016x}-{:016x}.hadbp", 3u64, 6u64)),
        "merged range must be [3,6]: {l1:?}"
    );

    // Tick 2: remaining L0 is {1, 7} — lone files separated by breaks. No merge
    // is possible, but it must be a quiet NoOp, never an error loop.
    let o2 = run_level_compaction(&layout, 0, 4, Duration::from_secs(0), NOW)
        .await
        .expect("a non-mergeable window is a NoOp, not an error");
    assert_eq!(o2.merged_count(), 0, "nothing left to merge → NoOp");
    assert!(matches!(o2, CompactionOutcome::NoOp));
}
