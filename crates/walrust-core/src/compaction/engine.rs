//! The safety-ordered compaction run.
//!
//! One `run_level_compaction` call performs at most one merge at one level with
//! the E2-class ordering:
//!
//! 1. **Select** the oldest eligible sources (L0 honours `keep_fine_window`;
//!    higher levels take all), clipped to a seq-contiguous run of up to `batch`
//!    files so a batch never straddles a snapshot chain-break (liveness; see
//!    [`contiguous_batch`]).
//! 2. **Idempotency / crash recovery**: if a merged object already covers this
//!    exact range at the target level (a crash between write and delete), verify
//!    it and — if sound — skip the merge and just finish deleting the sources.
//!    If it is unsound (torn/partial), delete that partial output and re-merge.
//! 3. **Merge** the sources into a COMPACTED object (streaming, memory-bounded).
//! 4. **Self-check** the produced bytes (decode + chain_end + page count).
//! 5. **Write** the object durably.
//! 6. **Read it back** and verify again. On failure: delete the partial output,
//!    return a typed error, and **never delete the sources**.
//! 7. Only now **delete the sources**.
//!
//! A crash between 5 and 7 leaves harmless overlap; re-running converges via
//! step 2. Every failure is a typed [`CompactionError`]; the write/delete path
//! never warns-and-continues.

use std::time::Duration;

use hadb_changeset::physical::{self, chain_end};

use super::layout::{CompactionLayout, LayoutFile, Level, SeqRange};
use super::merge::{merge_changesets, verify_merged_bytes, MergeInput, PeakPages};
use super::trigger::eligible_prefix_len;
use super::CompactionError;

/// What one compaction run did.
#[derive(Debug)]
pub enum CompactionOutcome {
    /// Nothing to merge (idle, too few eligible, or a young batch backing off).
    NoOp,
    /// A fresh merge was written and its sources deleted.
    Merged {
        merged_count: usize,
        output: LayoutFile,
    },
    /// A prior crashed run had already written the merged object; this run
    /// verified it and completed the source deletion (idempotent convergence).
    ConvergedExistingDeletion {
        merged_count: usize,
        output: LayoutFile,
    },
}

impl CompactionOutcome {
    pub fn merged_count(&self) -> usize {
        match self {
            CompactionOutcome::NoOp => 0,
            CompactionOutcome::Merged { merged_count, .. }
            | CompactionOutcome::ConvergedExistingDeletion { merged_count, .. } => *merged_count,
        }
    }
}

/// Run one compaction merge from `source_level` into `source_level + 1`.
///
/// `keep_fine_window` is applied only when `source_level == 0`; higher levels
/// treat all files as eligible.
pub async fn run_level_compaction(
    layout: &dyn CompactionLayout,
    source_level: Level,
    batch: usize,
    keep_fine_window: Duration,
    now_ms: i64,
) -> Result<CompactionOutcome, CompactionError> {
    if batch == 0 {
        return Ok(CompactionOutcome::NoOp);
    }
    let target_level = source_level + 1;

    // ── 1. Select eligible sources ─────────────────────────────────────────
    let all = layout.list_level(source_level).await?;
    let eligible = if source_level == 0 {
        let ts: Vec<i64> = all.iter().map(|f| f.last_modified_ms).collect();
        eligible_prefix_len(&ts, now_ms, keep_fine_window)
    } else {
        all.len()
    };
    if eligible < batch {
        return Ok(CompactionOutcome::NoOp);
    }
    // Clip the batch to a **seq-contiguous** run within the eligible window
    // (liveness, wave C3a). The oldest `batch` files can straddle a snapshot
    // chain-break: a snapshot consumes its own seq (`take_snapshot`: `new_seq =
    // seq + 1`) and the next incremental chains from the snapshot's checksum, so
    // the L0 chain breaks and there is a seq gap at that boundary. A fixed
    // `take(batch)` across such a break makes `merge_changesets` return
    // `NonContiguous` on **every tick, forever** (the batch never changes). We
    // instead merge the contiguous prefix run (clipped at the first seq gap,
    // capped at `batch`); if the prefix is a lone straddler we skip past it to
    // the next run. Seq contiguity (`max + 1 == next.min`) is exactly the chain
    // contiguity the planner relies on and the merge re-checks, and it is read
    // straight from the listing (no header reads). A residual `NonContiguous`
    // after clipping would mean a genuine fork/corruption, not a liveness bug —
    // and is then a correct loud error.
    let ranges: Vec<(u64, u64)> = all[..eligible]
        .iter()
        .map(|f| (f.range.min, f.range.max))
        .collect();
    let Some((start, len)) = contiguous_batch(&ranges, batch) else {
        // Nothing mergeable this tick: the eligible window is only lone files
        // separated by chain-breaks (e.g. a snapshot per incremental). No error,
        // no progress — the fine points simply stay restorable at this level.
        return Ok(CompactionOutcome::NoOp);
    };
    let sources: Vec<LayoutFile> = all[start..start + len].to_vec();
    let range = SeqRange::new(
        sources.first().unwrap().range.min,
        sources.last().unwrap().range.max,
    );

    // ── 2. Idempotency / crash recovery ────────────────────────────────────
    if let Some(existing) = find_existing_merged(layout, target_level, range).await? {
        match verify_existing(layout, &existing, &sources).await {
            Ok(()) => {
                layout.delete(&sources).await?;
                return Ok(CompactionOutcome::ConvergedExistingDeletion {
                    merged_count: sources.len(),
                    output: existing,
                });
            }
            Err(_torn) => {
                // Partial/unsound prior output: delete it and re-merge. Never
                // delete sources against an unverified object.
                layout.delete(std::slice::from_ref(&existing)).await?;
            }
        }
    }

    // ── 3. Merge (streaming) ───────────────────────────────────────────────
    let mut inputs = Vec::with_capacity(sources.len());
    for f in &sources {
        let header = layout.read_header(f).await?;
        let stream = layout.open(f).await?;
        inputs.push(MergeInput {
            header,
            range: f.range,
            stream,
        });
    }
    let tracker = PeakPages::new();
    let result = merge_changesets(inputs, &tracker).await?;

    // ── 4. Self-check produced bytes ───────────────────────────────────────
    verify_merged_bytes(
        &result.bytes,
        result.prev_checksum,
        result.declared_end_checksum,
        result.page_count,
    )
    .map_err(|e| CompactionError::VerificationFailed(format!("pre-write self-check: {e}")))?;

    // ── 5. Write durably ───────────────────────────────────────────────────
    let output = layout
        .write_merged(target_level, range, &result.bytes)
        .await?;

    // ── 6. Read back and verify ────────────────────────────────────────────
    let readback = layout.read_bytes(&output).await?;
    if let Err(e) = verify_merged_bytes(
        &readback,
        result.prev_checksum,
        result.declared_end_checksum,
        result.page_count,
    ) {
        // Loud failure: delete our own partial/unsound output, keep sources.
        let _ = layout.delete(std::slice::from_ref(&output)).await;
        return Err(CompactionError::VerificationFailed(format!(
            "read-back verify failed, sources preserved: {e}"
        )));
    }

    // ── 7. Only now delete sources ─────────────────────────────────────────
    layout.delete(&sources).await?;

    Ok(CompactionOutcome::Merged {
        merged_count: sources.len(),
        output,
    })
}

/// Select a seq-contiguous run to merge from the eligible window.
///
/// `ranges` are the `(min, max)` seq spans of the eligible files at a level,
/// sorted ascending (as [`CompactionLayout::list_level`] returns). Returns
/// `Some((start, len))` — merge `ranges[start..start + len]` — or `None` when no
/// run is worth merging this tick.
///
/// Rules (see the call site for the liveness rationale):
/// - Walk from the oldest file. A **run** is a maximal seq-contiguous span
///   (`ranges[i].1 + 1 == ranges[i + 1].0`), capped at `batch` files.
/// - Merge the **first** run of length `>= 2` (the contiguous prefix normally;
///   the next run if the prefix is a lone straddler skipped past a chain-break).
/// - `batch == 1` keeps the "fold every single file up a level" semantics: it
///   merges the oldest single file (a 1-source merge is always contiguous).
/// - `None` when only lone files separated by breaks remain (nothing to do).
fn contiguous_batch(ranges: &[(u64, u64)], batch: usize) -> Option<(usize, usize)> {
    if batch == 0 || ranges.is_empty() {
        return None;
    }
    let n = ranges.len();
    let mut i = 0;
    while i < n {
        // Extend a contiguous run from `i`, capped at `batch` files.
        let mut j = i;
        while j + 1 < n && ranges[j].1 + 1 == ranges[j + 1].0 && (j - i + 1) < batch {
            j += 1;
        }
        let run_len = j - i + 1;
        if run_len >= 2 || batch == 1 {
            return Some((i, run_len));
        }
        // A lone straddler at `i` (a chain-break follows). Skip to the next run.
        i = j + 1;
    }
    None
}

/// Look for a merged object at `level` covering exactly `range`.
///
/// Convergence is **exact-range only**. If the level instead holds an object
/// whose range *overlaps* the target (a subset/superset from a prior run with a
/// different batch, or otherwise inconsistent state), that is not idempotent
/// convergence — it is a loud [`CompactionError::OverlappingExisting`]. We must
/// not merge into an overlapping level (it would strand an orphan the restore
/// planner could not reconcile), and we cannot assume it is safe to delete
/// (it may cover seqs outside this batch).
async fn find_existing_merged(
    layout: &dyn CompactionLayout,
    level: Level,
    range: SeqRange,
) -> Result<Option<LayoutFile>, CompactionError> {
    let files = layout.list_level(level).await?;
    let mut exact = None;
    for f in files {
        if f.range == range {
            exact = Some(f);
        } else if ranges_overlap(f.range, range) {
            return Err(CompactionError::OverlappingExisting(format!(
                "target range {:016x}-{:016x} overlaps existing merged object {} \
                 ({:016x}-{:016x}); only an exact-range object is convergence",
                range.min, range.max, f.key, f.range.min, f.range.max
            )));
        }
    }
    Ok(exact)
}

/// Inclusive seq ranges overlap iff each starts at or before the other ends.
fn ranges_overlap(a: SeqRange, b: SeqRange) -> bool {
    a.min <= b.max && b.min <= a.max
}

/// Verify an existing merged object fully covers the source batch: it decodes
/// (content integrity), is COMPACTED, chains from the first source's prev, ends
/// at the last source's authoritative `chain_end()`, has the range-end seq, and
/// carries at least one page. Only then is it safe to delete the sources.
async fn verify_existing(
    layout: &dyn CompactionLayout,
    existing: &LayoutFile,
    sources: &[LayoutFile],
) -> Result<(), CompactionError> {
    let bytes = layout.read_bytes(existing).await?;
    let cs = physical::decode(&bytes).map_err(|e| CompactionError::Decode {
        key: existing.key.clone(),
        source: e,
    })?;
    if !cs.is_compacted() {
        return Err(CompactionError::VerificationFailed(
            "existing object not COMPACTED".into(),
        ));
    }
    let first = sources.first().unwrap();
    let last = sources.last().unwrap();
    let first_header = layout.read_header(first).await?;
    if cs.header.prev_checksum != first_header.prev_checksum {
        return Err(CompactionError::VerificationFailed(
            "existing prev_checksum does not match first source".into(),
        ));
    }
    if cs.header.seq != last.range.max {
        return Err(CompactionError::VerificationFailed(
            "existing seq does not match range end".into(),
        ));
    }
    // Authoritative chain_end of the last source (one bounded object read).
    let last_bytes = layout.read_bytes(last).await?;
    let last_cs = physical::decode(&last_bytes).map_err(|e| CompactionError::Decode {
        key: last.key.clone(),
        source: e,
    })?;
    if chain_end(&cs) != chain_end(&last_cs) {
        return Err(CompactionError::VerificationFailed(
            "existing declared_end does not match last source chain_end".into(),
        ));
    }
    if cs.pages.is_empty() {
        return Err(CompactionError::VerificationFailed(
            "existing object has no pages".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::contiguous_batch;

    #[test]
    fn full_contiguous_prefix_capped_at_batch() {
        // Five contiguous files, batch 4 → merge the oldest 4.
        let r = [(2, 2), (3, 3), (4, 4), (5, 5), (6, 6)];
        assert_eq!(contiguous_batch(&r, 4), Some((0, 4)));
    }

    #[test]
    fn contiguous_ranges_not_just_points() {
        // Multi-seq incrementals (legacy [min,max]) chain by max+1 == next.min.
        let r = [(2, 5), (6, 9), (10, 13)];
        assert_eq!(contiguous_batch(&r, 2), Some((0, 2)));
    }

    #[test]
    fn short_prefix_merges_even_below_batch() {
        // Prefix run [2,3,4] then a break (gap at 5, snapshot). batch 4 → merge
        // the 3-file prefix rather than erroring across the break.
        let r = [(2, 2), (3, 3), (4, 4), (6, 6), (7, 7)];
        assert_eq!(contiguous_batch(&r, 4), Some((0, 3)));
    }

    #[test]
    fn leading_singleton_straddler_is_skipped_to_next_run() {
        // Oldest file [1] is alone (gap at 2 = snapshot); the next run [3,4,5,6]
        // is merged. The lone [1] stays a fine point (restore reads it at L0).
        let r = [(1, 1), (3, 3), (4, 4), (5, 5), (6, 6)];
        assert_eq!(contiguous_batch(&r, 4), Some((1, 4)));
    }

    #[test]
    fn all_singletons_separated_by_breaks_is_noop() {
        // Snapshot per incremental: nothing is safely mergeable (batch >= 2).
        let r = [(1, 1), (3, 3), (5, 5), (7, 7)];
        assert_eq!(contiguous_batch(&r, 4), None);
    }

    #[test]
    fn batch_one_folds_the_oldest_single_file() {
        // batch == 1 preserves the "merge every file up a level" cascade; a
        // 1-source merge can never be NonContiguous.
        let r = [(1, 1), (3, 3)];
        assert_eq!(contiguous_batch(&r, 1), Some((0, 1)));
    }

    #[test]
    fn empty_or_zero_batch_is_noop() {
        assert_eq!(contiguous_batch(&[], 4), None);
        assert_eq!(contiguous_batch(&[(1, 1)], 0), None);
    }
}
