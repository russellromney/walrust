//! The safety-ordered compaction run.
//!
//! One `run_level_compaction` call performs at most one merge at one level with
//! the E2-class ordering:
//!
//! 1. **Select** the oldest `batch` eligible sources (L0 honours
//!    `keep_fine_window`; higher levels take all).
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
    let sources: Vec<LayoutFile> = all.into_iter().take(batch).collect();
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
