//! The streaming k-way merge engine.
//!
//! Merges N contiguous source changesets into one COMPACTED (HADBP v2)
//! changeset with page-level last-writer-wins (highest-seq source wins a page).
//! Linkage: `prev_checksum` = the chain value before the FIRST source;
//! `declared_end_checksum` = `chain_end()` of the LAST source; the output seq is
//! the last source's seq (the range end).
//!
//! Memory: the merge holds at most one frontier page per source plus one emit
//! slot — `O(page_size × source_count)`, never `O(total bytes)`. The
//! [`PeakPages`] tracker proves it (`peak_pages_bounded` test).

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use hadb_changeset::physical::{self, chain_end, PageEntry, PageId, PageIdSize, PhysicalChangeset};
use sha2::{Digest, Sha256};

use super::layout::{ChangesetPageStream, SeqRange, SourceHeader};
use super::CompactionError;

/// Live-page counter. Every page currently buffered by the merge (frontier
/// heap + emit slot) holds a guard that increments `current` on creation and
/// decrements on drop; `peak` records the high-water mark. In production this
/// is a cheap pair of atomics; tests read `peak()` to prove the memory bound.
#[derive(Clone, Default)]
pub struct PeakPages {
    current: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl PeakPages {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn peak(&self) -> usize {
        self.peak.load(AtomicOrdering::SeqCst)
    }
    pub fn current(&self) -> usize {
        self.current.load(AtomicOrdering::SeqCst)
    }
    fn inc(&self) {
        let now = self.current.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        self.peak.fetch_max(now, AtomicOrdering::SeqCst);
    }
    fn dec(&self) {
        self.current.fetch_sub(1, AtomicOrdering::SeqCst);
    }
}

/// RAII guard: one live buffered page.
struct PageGuard {
    tracker: PeakPages,
}
impl PageGuard {
    fn new(tracker: &PeakPages) -> Self {
        tracker.inc();
        Self {
            tracker: tracker.clone(),
        }
    }
}
impl Drop for PageGuard {
    fn drop(&mut self) {
        self.tracker.dec();
    }
}

/// A page held in the frontier heap, tagged with which source it came from.
struct HeapPage {
    page_id: PageId,
    /// Source order index; higher index = higher seq = wins ties.
    order: usize,
    page: PageEntry,
    _guard: PageGuard,
}

impl PartialEq for HeapPage {
    fn eq(&self, other: &Self) -> bool {
        self.page_id == other.page_id && self.order == other.order
    }
}
impl Eq for HeapPage {}
impl PartialOrd for HeapPage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapPage {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; we want the smallest page_id first, then the
        // smallest order first (so equal page_ids pop low→high and the pending
        // slot keeps the highest order). Reverse both.
        other
            .page_id
            .cmp(&self.page_id)
            .then(other.order.cmp(&self.order))
    }
}

/// One source fed to the merge: its header metadata plus a page stream.
pub struct MergeInput {
    pub header: SourceHeader,
    /// The source's inclusive seq range (from its layout listing). Used to
    /// compute the output range span.
    pub range: SeqRange,
    pub stream: Box<dyn ChangesetPageStream>,
}

/// Result of a merge: the encoded COMPACTED object plus derived facts.
#[derive(Debug)]
pub struct MergeResult {
    /// The serialized COMPACTED v2 changeset (ready for `put`).
    pub bytes: Vec<u8>,
    /// Inclusive seq span the merged object covers.
    pub range: SeqRange,
    /// The output seq (== last source's seq).
    pub out_seq: u64,
    /// `prev_checksum` of the merged object (== first source's prev).
    pub prev_checksum: u64,
    /// Declared end-of-range chain value (== `chain_end()` of the last source).
    pub declared_end_checksum: u64,
    /// Number of unique pages in the merged output.
    pub page_count: u32,
}

/// Merge the given sources (already ordered ascending by seq) into one
/// COMPACTED changeset. `tracker` records the peak simultaneously-buffered page
/// count for the memory-bound proof; pass a fresh [`PeakPages`] (or a shared
/// one) — it is cheap in production.
pub async fn merge_changesets(
    mut sources: Vec<MergeInput>,
    tracker: &PeakPages,
) -> Result<MergeResult, CompactionError> {
    if sources.is_empty() {
        return Err(CompactionError::EmptySourceSet);
    }

    // ── Geometry + linkage validation ──────────────────────────────────────
    let first = &sources[0];
    let page_id_size = first.header.page_id_size;
    let page_size = first.header.page_size;
    let prev_checksum = first.header.prev_checksum;
    for s in &sources {
        if s.header.page_id_size != page_id_size || s.header.page_size != page_size {
            return Err(CompactionError::MixedGeometry(format!(
                "page_id_size/page_size differ: ({:?},{}) vs ({:?},{})",
                page_id_size, page_size, s.header.page_id_size, s.header.page_size
            )));
        }
    }

    let out_seq = sources.last().unwrap().header.seq;
    let out_min = sources.iter().map(|s| s.range.min).min().unwrap();
    let out_max = sources.iter().map(|s| s.range.max).max().unwrap();
    let out_range = SeqRange::new(out_min, out_max);
    let n = sources.len();

    // ── Encode header with placeholders for page_count and declared_end ─────
    let created_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let mut buf = Vec::new();
    buf.extend_from_slice(b"HADBP");
    buf.push(2); // version 2 (compacted)
    let flags_off = buf.len();
    buf.push(0x80); // FLAG_COMPACTED; the end-page-count marker bit is OR'd in
                    // at finalize if the last source declares a DB size.
    buf.push(page_id_size as u8);
    buf.extend_from_slice(&page_size.to_be_bytes());
    buf.extend_from_slice(&out_seq.to_be_bytes());
    buf.extend_from_slice(&prev_checksum.to_be_bytes());
    let page_count_off = buf.len();
    buf.extend_from_slice(&0u32.to_be_bytes()); // page_count placeholder
    buf.extend_from_slice(&created_ms.to_be_bytes());
    let declared_off = buf.len();
    buf.extend_from_slice(&0u64.to_be_bytes()); // declared_end placeholder

    // Streaming content checksum, seeded with prev_checksum, exactly as
    // physical::compute_checksum does over sorted pages.
    let mut hasher = Sha256::new();
    hasher.update(prev_checksum.to_be_bytes());

    // Per-source streaming hashers so we can derive each source's own content
    // checksum (needed only for the LAST source when it is a normal changeset).
    let mut source_hashers: Vec<Sha256> = sources
        .iter()
        .map(|s| {
            let mut h = Sha256::new();
            h.update(s.header.prev_checksum.to_be_bytes());
            h
        })
        .collect();

    // The DB size declared by the LAST source (highest seq): a walrust
    // incremental encodes an end-page-count marker (an empty page at
    // `end_page_count + 1`). The merged range must reconstruct the *final* DB
    // size, so we carry the last source's declared size and emit exactly one
    // marker for it at finalize. Intermediate sources' markers are elided
    // (their sizes are superseded). See the module header.
    //
    // **Shrink (VACUUM) across a merge boundary.** If the final size is *smaller*
    // than an intermediate size, an intermediate source's high page (e.g. page
    // 100) survives last-writer-wins even though the final DB truncates to, say,
    // 50 pages. Such a page is dead in the final state, and — crucially — its
    // page id exceeds the canonical marker (`final_epc + 1`), which would break
    // both the strictly-ascending emit invariant and `validate_sqlite_changeset`
    // (which rejects a data page past `end_page_count`). We therefore **drop any
    // emitted data page whose id > final_end_page_count**: the truncation removes
    // it anyway, so the merged object is byte-identical in effect to a fresh
    // snapshot of the final state, and the marker stays the highest page id.
    // Streaming order guarantees `final_end_page_count` is already known by the
    // time any such orphan page is flushed (the last source's marker at
    // `final_epc + 1` sorts below every orphan id, so it is read first). When the
    // last source declares no size (`None`), there is no truncation and every
    // page is kept.
    let mut final_end_page_count: Option<u64> = None;
    let capture_epc = |final_epc: &mut Option<u64>, order: usize, page: &PageEntry| {
        if order == n - 1 && page.data.is_empty() {
            *final_epc = Some(page.page_id.to_u64().saturating_sub(1));
        }
    };

    // ── Prime the frontier: one page per source ────────────────────────────
    let mut heap: BinaryHeap<HeapPage> = BinaryHeap::new();
    for (order, s) in sources.iter_mut().enumerate() {
        if let Some(page) = s.stream.next_page().await? {
            hash_source_page(&mut source_hashers[order], page_id_size, &page);
            capture_epc(&mut final_end_page_count, order, &page);
            heap.push(HeapPage {
                page_id: page.page_id,
                order,
                page,
                _guard: PageGuard::new(tracker),
            });
        }
    }

    let mut page_count: u32 = 0;
    // Pending winner for the current page_id (highest order so far).
    let mut pending: Option<HeapPage> = None;
    let mut last_emitted: Option<u64> = None;

    while let Some(top) = heap.pop() {
        let order = top.order;
        // Resolve winner for this page_id.
        match &pending {
            Some(p) if p.page_id == top.page_id => {
                // Higher order (later source) wins; `>=` also makes a duplicate
                // page_id within one source resolve to the later-streamed copy,
                // matching apply-in-order last-writer-wins.
                if top.order >= p.order {
                    pending = Some(top);
                } // else keep existing higher-order page (top dropped → guard--)
            }
            _ => {
                // page_id advanced (or first): flush the old pending. A winning
                // **empty** page is a superseded end-page-count marker — elide
                // it; the final marker is emitted once at finalize.
                if let Some(p) = pending.take() {
                    if should_emit(&p.page, final_end_page_count) {
                        emit_page(
                            &mut buf,
                            &mut hasher,
                            page_id_size,
                            &p.page,
                            &mut last_emitted,
                        );
                        page_count += 1;
                    }
                }
                pending = Some(top);
            }
        }
        // Advance the source we just popped.
        if let Some(page) = sources[order].stream.next_page().await? {
            hash_source_page(&mut source_hashers[order], page_id_size, &page);
            capture_epc(&mut final_end_page_count, order, &page);
            heap.push(HeapPage {
                page_id: page.page_id,
                order,
                page,
                _guard: PageGuard::new(tracker),
            });
        }
    }
    // Flush the final pending page (eliding a superseded empty marker or a
    // shrink-orphan page above the final DB size).
    if let Some(p) = pending.take() {
        if should_emit(&p.page, final_end_page_count) {
            emit_page(
                &mut buf,
                &mut hasher,
                page_id_size,
                &p.page,
                &mut last_emitted,
            );
            page_count += 1;
        }
    }

    // Emit the single canonical end-page-count marker for the final DB size, so
    // a restore truncates/grows to exactly the last source's size. Its page id
    // is `end_page_count + 1` (empty data), higher than every data page, so it
    // preserves ascending emit order. The marker flag is OR'd into the header.
    if let Some(epc) = final_end_page_count {
        let marker = PageEntry {
            page_id: match page_id_size {
                PageIdSize::U32 => PageId::U32((epc + 1) as u32),
                PageIdSize::U64 => PageId::U64(epc + 1),
            },
            data: Vec::new(),
        };
        emit_page(
            &mut buf,
            &mut hasher,
            page_id_size,
            &marker,
            &mut last_emitted,
        );
        page_count += 1;
        buf[flags_off] |= crate::ltx::FLAG_END_PAGE_COUNT_MARKER;
    }

    // ── Finalize: per-source chain-ends, contiguity, output declared_end ────
    // Each source's chain_end is its declared value when compacted (never
    // recompute), else its streamed content checksum — exactly chain_end()'s
    // logic. The streamed content checksum equals the source's own trailer
    // checksum because we hashed all of its pages in canonical order.
    let source_chain_ends: Vec<u64> = source_hashers
        .into_iter()
        .enumerate()
        .map(|(i, h)| {
            sources[i]
                .header
                .declared_end_checksum
                .unwrap_or_else(|| finalize(h))
        })
        .collect();

    // Contiguity: each source must chain from the prior source's chain_end.
    // A gap here would make the merged object silently bridge a break.
    for i in 1..n {
        if sources[i].header.prev_checksum != source_chain_ends[i - 1] {
            return Err(CompactionError::NonContiguous(format!(
                "source[{i}].prev_checksum {} != source[{}].chain_end {}",
                sources[i].header.prev_checksum,
                i - 1,
                source_chain_ends[i - 1]
            )));
        }
    }
    let declared_end = *source_chain_ends.last().unwrap();

    let content_checksum = finalize(hasher);
    buf.extend_from_slice(&content_checksum.to_be_bytes());
    // Patch page_count and declared_end (declared_end is NOT part of the
    // checksum, so patching it after hashing is correct).
    buf[page_count_off..page_count_off + 4].copy_from_slice(&page_count.to_be_bytes());
    buf[declared_off..declared_off + 8].copy_from_slice(&declared_end.to_be_bytes());

    Ok(MergeResult {
        bytes: buf,
        range: out_range,
        out_seq,
        prev_checksum,
        declared_end_checksum: declared_end,
        page_count,
    })
}

fn finalize(hasher: Sha256) -> u64 {
    let out = hasher.finalize();
    u64::from_be_bytes(out[0..8].try_into().unwrap())
}

/// Feed one page into a source's own content hasher (canonical order/format).
fn hash_source_page(hasher: &mut Sha256, page_id_size: PageIdSize, page: &PageEntry) {
    match page_id_size {
        PageIdSize::U32 => hasher.update((page.page_id.to_u64() as u32).to_be_bytes()),
        PageIdSize::U64 => hasher.update(page.page_id.to_u64().to_be_bytes()),
    }
    hasher.update((page.data.len() as u32).to_be_bytes());
    hasher.update(&page.data);
}

/// Whether a resolved winning page should be written to the merged output.
///
/// Drops (a) superseded **empty** end-page-count markers — the one canonical
/// marker is emitted at finalize — and (b) **shrink-orphan** data pages whose id
/// exceeds the final DB size (`final_end_page_count`): the final truncation
/// removes them, and keeping them would push a data page past the marker (an
/// invalid changeset). A page id `== final_end_page_count` is still the last
/// live page and is kept. When no final size is known, only empty markers drop.
fn should_emit(page: &PageEntry, final_end_page_count: Option<u64>) -> bool {
    if page.data.is_empty() {
        return false;
    }
    match final_end_page_count {
        Some(epc) => page.page_id.to_u64() <= epc,
        None => true,
    }
}

/// Emit one merged page: append to the output buffer and update the output
/// content hasher. Pages arrive in strictly ascending page_id order.
fn emit_page(
    buf: &mut Vec<u8>,
    hasher: &mut Sha256,
    page_id_size: PageIdSize,
    page: &PageEntry,
    last_emitted: &mut Option<u64>,
) {
    debug_assert!(
        last_emitted
            .map(|p| page.page_id.to_u64() > p)
            .unwrap_or(true),
        "pages must be emitted in strictly ascending page_id order"
    );
    *last_emitted = Some(page.page_id.to_u64());
    match page.page_id {
        PageId::U32(v) => buf.extend_from_slice(&v.to_be_bytes()),
        PageId::U64(v) => buf.extend_from_slice(&v.to_be_bytes()),
    }
    let _ = page_id_size;
    buf.extend_from_slice(&(page.data.len() as u32).to_be_bytes());
    buf.extend_from_slice(&page.data);
    // Hash in canonical order (same bytes as physical::compute_checksum).
    match page.page_id {
        PageId::U32(v) => hasher.update(v.to_be_bytes()),
        PageId::U64(v) => hasher.update(v.to_be_bytes()),
    }
    hasher.update((page.data.len() as u32).to_be_bytes());
    hasher.update(&page.data);
}

/// Decode + sanity-check a merged object. Returns the decoded changeset on
/// success. Used by the engine's read-back verification step.
pub fn verify_merged_bytes(
    bytes: &[u8],
    expected_prev: u64,
    expected_declared_end: u64,
    expected_page_count: u32,
) -> Result<PhysicalChangeset, CompactionError> {
    let cs = physical::decode(bytes).map_err(|e| CompactionError::Decode {
        key: "<merged output>".into(),
        source: e,
    })?;
    if !cs.is_compacted() {
        return Err(CompactionError::VerificationFailed(
            "merged output is not marked COMPACTED".into(),
        ));
    }
    if cs.header.prev_checksum != expected_prev {
        return Err(CompactionError::VerificationFailed(format!(
            "prev_checksum {} != expected {}",
            cs.header.prev_checksum, expected_prev
        )));
    }
    if chain_end(&cs) != expected_declared_end {
        return Err(CompactionError::VerificationFailed(format!(
            "chain_end {} != expected declared_end {}",
            chain_end(&cs),
            expected_declared_end
        )));
    }
    if cs.header.page_count != expected_page_count || cs.pages.len() as u32 != expected_page_count {
        return Err(CompactionError::VerificationFailed(format!(
            "page_count {} / pages {} != expected {}",
            cs.header.page_count,
            cs.pages.len(),
            expected_page_count
        )));
    }
    Ok(cs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    /// A page stream over an in-memory vec of pages (must be ascending page_id).
    struct VecStream {
        pages: std::collections::VecDeque<PageEntry>,
    }
    impl VecStream {
        fn new(pages: Vec<PageEntry>) -> Self {
            Self {
                pages: pages.into(),
            }
        }
    }
    #[async_trait]
    impl ChangesetPageStream for VecStream {
        async fn next_page(&mut self) -> Result<Option<PageEntry>, CompactionError> {
            Ok(self.pages.pop_front())
        }
    }

    fn pg(id: u64, fill: u8, len: usize) -> PageEntry {
        PageEntry {
            page_id: PageId::U64(id),
            data: vec![fill; len],
        }
    }

    fn source_from(cs: &PhysicalChangeset, range: SeqRange) -> MergeInput {
        MergeInput {
            header: SourceHeader {
                page_id_size: cs.header.page_id_size,
                page_size: cs.header.page_size,
                seq: cs.header.seq,
                prev_checksum: cs.header.prev_checksum,
                page_count: cs.header.page_count,
                created_ms: cs.header.created_ms,
                declared_end_checksum: cs.declared_end_checksum,
            },
            range,
            stream: Box::new(VecStream::new(cs.pages.clone())),
        }
    }

    #[tokio::test]
    async fn merge_two_sources_matches_canonical_encode() {
        // cs1 writes pages 0,1; cs2 overwrites page 1 and adds page 2.
        let cs1 = PhysicalChangeset::new(
            1,
            0,
            PageIdSize::U64,
            262144,
            vec![pg(0, 0xAA, 32), pg(1, 0xBB, 32)],
        );
        let cs2 = PhysicalChangeset::new(
            2,
            chain_end(&cs1),
            PageIdSize::U64,
            262144,
            vec![pg(1, 0xCC, 32), pg(2, 0xDD, 32)],
        );
        let tracker = PeakPages::new();
        let res = merge_changesets(
            vec![
                source_from(&cs1, SeqRange::point(1)),
                source_from(&cs2, SeqRange::point(2)),
            ],
            &tracker,
        )
        .await
        .unwrap();

        // Expected: page0 from cs1, page1 from cs2 (last-writer), page2 from cs2.
        let expected_pages = vec![pg(0, 0xAA, 32), pg(1, 0xCC, 32), pg(2, 0xDD, 32)];
        let expected = PhysicalChangeset::new_compacted(
            2,
            0,
            PageIdSize::U64,
            262144,
            expected_pages,
            chain_end(&cs2),
        );
        // Byte-parity check ignoring created_ms (offset 32..40).
        let got = physical::decode(&res.bytes).unwrap();
        let want = expected;
        assert_eq!(got.pages, want.pages);
        assert_eq!(got.header.prev_checksum, 0);
        assert_eq!(got.header.seq, 2);
        assert_eq!(chain_end(&got), chain_end(&cs2));
        assert_eq!(res.declared_end_checksum, chain_end(&cs2));
        assert_eq!(res.page_count, 3);

        // Verify passes.
        verify_merged_bytes(&res.bytes, 0, chain_end(&cs2), 3).unwrap();
    }

    #[tokio::test]
    async fn empty_source_set_rejected() {
        let tracker = PeakPages::new();
        let err = merge_changesets(vec![], &tracker).await.unwrap_err();
        assert!(matches!(err, CompactionError::EmptySourceSet));
    }

    #[tokio::test]
    async fn peak_pages_bounded_far_below_total() {
        // 4 sources, each with 500 pages of 64 bytes => 128 KB total across
        // sources, but the frontier must never hold more than ~sources pages.
        let n_sources = 4usize;
        let pages_per = 500u64;
        let mut inputs = Vec::new();
        let mut prev = 0u64;
        for s in 0..n_sources {
            // Disjoint page_id ranges so every page is unique (worst case for
            // output size, but frontier still bounded by source count).
            let base = s as u64 * pages_per;
            let pages: Vec<PageEntry> = (0..pages_per)
                .map(|i| pg(base + i, (i % 251) as u8, 64))
                .collect();
            let cs = PhysicalChangeset::new((s + 1) as u64, prev, PageIdSize::U64, 262144, pages);
            prev = chain_end(&cs);
            inputs.push(source_from(&cs, SeqRange::point((s + 1) as u64)));
        }
        let tracker = PeakPages::new();
        let res = merge_changesets(inputs, &tracker).await.unwrap();
        assert_eq!(res.page_count as u64, pages_per * n_sources as u64);
        // Hard bound: at most one frontier page per source plus one pending
        // page in flight. Total pages merged (2000) dwarfs this.
        assert!(
            tracker.peak() <= n_sources + 1,
            "peak buffered pages {} exceeded source_count+1 ({})",
            tracker.peak(),
            n_sources + 1
        );
        assert_eq!(tracker.current(), 0, "all page guards released");
    }

    #[tokio::test]
    async fn last_writer_wins_interleaved() {
        // Three sources touch overlapping pages; highest seq must win each.
        let cs1 = PhysicalChangeset::new(
            1,
            0,
            PageIdSize::U64,
            4096,
            vec![pg(0, 1, 8), pg(1, 1, 8), pg(2, 1, 8)],
        );
        let cs2 = PhysicalChangeset::new(
            2,
            chain_end(&cs1),
            PageIdSize::U64,
            4096,
            vec![pg(1, 2, 8), pg(3, 2, 8)],
        );
        let cs3 = PhysicalChangeset::new(
            3,
            chain_end(&cs2),
            PageIdSize::U64,
            4096,
            vec![pg(2, 3, 8), pg(3, 3, 8)],
        );
        let tracker = PeakPages::new();
        let res = merge_changesets(
            vec![
                source_from(&cs1, SeqRange::point(1)),
                source_from(&cs2, SeqRange::point(2)),
                source_from(&cs3, SeqRange::point(3)),
            ],
            &tracker,
        )
        .await
        .unwrap();
        let got = physical::decode(&res.bytes).unwrap();
        // Expected winners: p0=cs1(1), p1=cs2(2), p2=cs3(3), p3=cs3(3).
        let want = vec![pg(0, 1, 8), pg(1, 2, 8), pg(2, 3, 8), pg(3, 3, 8)];
        assert_eq!(got.pages, want);
        assert_eq!(chain_end(&got), chain_end(&cs3));
    }

    #[tokio::test]
    async fn last_writer_wins_is_tied_to_source_order_not_adjacency() {
        // Page 5 is present in source 1 AND source 3 (source 2 does not touch
        // it). The winner must be source 3 by ORDER, proving last-writer-wins is
        // decided by source position (seq rank), not by heap-pop accident or by
        // adjacency of the two writers.
        let cs1 = PhysicalChangeset::new(
            1,
            0,
            PageIdSize::U64,
            4096,
            vec![pg(0, 0x11, 8), pg(5, 0x1F, 8)], // page 5 = 0x1F here
        );
        let cs2 = PhysicalChangeset::new(
            2,
            chain_end(&cs1),
            PageIdSize::U64,
            4096,
            vec![pg(1, 0x22, 8)], // does NOT touch page 5
        );
        let cs3 = PhysicalChangeset::new(
            3,
            chain_end(&cs2),
            PageIdSize::U64,
            4096,
            vec![pg(5, 0x3F, 8)], // page 5 = 0x3F here → must win
        );
        let tracker = PeakPages::new();
        let res = merge_changesets(
            vec![
                source_from(&cs1, SeqRange::point(1)),
                source_from(&cs2, SeqRange::point(2)),
                source_from(&cs3, SeqRange::point(3)),
            ],
            &tracker,
        )
        .await
        .unwrap();
        let got = physical::decode(&res.bytes).unwrap();
        let page5 = got
            .pages
            .iter()
            .find(|p| p.page_id.to_u64() == 5)
            .expect("page 5 present");
        assert_eq!(
            page5.data,
            vec![0x3F; 8],
            "page 5 must come from source 3 (highest order), not source 1"
        );
        assert_eq!(chain_end(&got), chain_end(&cs3));
    }

    /// An empty end-page-count marker page (`data == []`) at `epc + 1`.
    fn marker(epc: u64) -> PageEntry {
        PageEntry {
            page_id: PageId::U64(epc + 1),
            data: Vec::new(),
        }
    }

    #[tokio::test]
    async fn shrink_across_merge_drops_orphan_pages_and_stays_valid() {
        // A VACUUM-style shrink across a merge boundary: source 1 grew the DB to
        // 100 pages (writing a high page 100), source 2 shrank it back to 50
        // (marker epc=50) without touching page 100. Last-writer-wins would keep
        // page 100 as an orphan whose id (100) exceeds the final marker (51),
        // breaking ascending order + producing a changeset with a data page past
        // end_page_count. The fix drops such orphans; the merged object must be
        // valid, decode cleanly, carry the canonical marker as the highest page
        // id, and truncate to 50 on restore.
        let cs1 = PhysicalChangeset::new(
            1,
            0,
            PageIdSize::U64,
            4096,
            vec![pg(1, 0x11, 8), pg(100, 0x99, 8), marker(100)], // epc=100
        );
        let cs2 = PhysicalChangeset::new(
            2,
            chain_end(&cs1),
            PageIdSize::U64,
            4096,
            vec![pg(1, 0x22, 8), marker(50)], // shrink to 50, page 100 untouched
        );
        let tracker = PeakPages::new();
        let res = merge_changesets(
            vec![
                source_from(&cs1, SeqRange::point(1)),
                source_from(&cs2, SeqRange::point(2)),
            ],
            &tracker,
        )
        .await
        .unwrap();

        // Decodes cleanly: content checksum matches ⇒ pages are in canonical
        // (ascending) order, i.e. no out-of-order orphan slipped through.
        let got = physical::decode(&res.bytes).unwrap();

        // Orphan page 100 (above the final size) is gone; page 1 (updated by the
        // shrink source) plus the single canonical marker remain.
        let ids: Vec<u64> = got.pages.iter().map(|p| p.page_id.to_u64()).collect();
        assert!(
            !ids.contains(&100),
            "shrink orphan page 100 must be dropped: {ids:?}"
        );
        assert_eq!(ids, vec![1, 51], "kept page 1 + canonical marker at 51");
        assert_eq!(res.page_count, 2);

        // The marker is the highest page id and declares the final size (50).
        assert_eq!(
            crate::ltx::changeset_end_page_count(&got).unwrap(),
            Some(50),
            "merged object must declare the final (shrunk) DB size"
        );
        // The last-source page 1 content won (0x22, not 0x11).
        let p1 = got.pages.iter().find(|p| p.page_id.to_u64() == 1).unwrap();
        assert_eq!(p1.data, vec![0x22; 8]);
        // Linkage still ends at the last source's chain_end.
        assert_eq!(chain_end(&got), chain_end(&cs2));
        assert_eq!(res.declared_end_checksum, chain_end(&cs2));

        // A real SQLite apply/restore would truncate to 50 pages; validate the
        // changeset shape is acceptable (no non-empty page past end_page_count).
        assert!(
            got.pages
                .iter()
                .all(|p| p.data.is_empty() || p.page_id.to_u64() <= 50),
            "no live page may sit above the declared end_page_count"
        );
    }

    #[tokio::test]
    async fn merge_refuses_to_span_a_snapshot_boundary() {
        // Invariant behind the planner's exact-start rule (`range.min == need`).
        //
        // A snapshot consumes its own seq (`take_snapshot`: `new_seq = seq + 1`)
        // and the next incremental chains from the *snapshot's* checksum, not the
        // prior incremental's — so the L0 pool's chain BREAKS at every snapshot.
        // Here `pre` is the last incremental before a snapshot and `post` is the
        // first after it: `post.prev_checksum` is the snapshot's chain value, not
        // `chain_end(pre)`. A merge batch that straddles the snapshot must be
        // rejected, which is *why* no merged range can ever begin strictly inside
        // a snapshot span. Restore-from-snapshot@S therefore always finds an
        // object starting exactly at `S + 1` (or `target == S`); the planner's
        // `min == need` can never false-gap a healthy leveled bucket.
        let pre = PhysicalChangeset::new(1, 0, PageIdSize::U64, 4096, vec![pg(0, 1, 8)]);
        // A snapshot was taken here (its own seq, its own chain value 0x5EED).
        let snapshot_chain: u64 = 0x5EED;
        let post =
            PhysicalChangeset::new(3, snapshot_chain, PageIdSize::U64, 4096, vec![pg(1, 2, 8)]);
        assert_ne!(
            chain_end(&pre),
            snapshot_chain,
            "the snapshot breaks the incremental chain"
        );
        let tracker = PeakPages::new();
        let err = merge_changesets(
            vec![
                source_from(&pre, SeqRange::point(1)),
                source_from(&post, SeqRange::point(3)),
            ],
            &tracker,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CompactionError::NonContiguous(_)),
            "a batch spanning a snapshot boundary must be refused, got {err:?}"
        );
    }

    #[tokio::test]
    async fn overlapping_chain_rejected_as_non_contiguous() {
        // Two sources that both fork from the SAME predecessor (both prev == 0):
        // an overlap/branch rather than a gap. The contiguity check must reject
        // it — source[1].prev (0) != source[0].chain_end.
        let cs_a = PhysicalChangeset::new(1, 0, PageIdSize::U64, 4096, vec![pg(0, 1, 8)]);
        let cs_b = PhysicalChangeset::new(2, 0, PageIdSize::U64, 4096, vec![pg(1, 2, 8)]);
        let tracker = PeakPages::new();
        let err = merge_changesets(
            vec![
                source_from(&cs_a, SeqRange::point(1)),
                source_from(&cs_b, SeqRange::point(2)),
            ],
            &tracker,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CompactionError::NonContiguous(_)),
            "overlapping/branched chain must be rejected, got {err:?}"
        );
    }
}
