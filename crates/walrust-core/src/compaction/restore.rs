//! Level-aware restore execution: gather candidates, plan, prefetch, apply.
//!
//! This is the read-side consumer of the compaction write side. It is
//! **layout-agnostic** — it speaks only [`CompactionLayout`] and the pure
//! [`plan_restore`] planner, so the owned seq layout and the litestream-heritage
//! range layout share one code path (both carry HADBP payloads; see the module
//! header's "both adapters carry HADBP payloads" decision).
//!
//! ## Parallel prefetch, strict-order apply
//!
//! The planner yields the full ordered object list up front, so the restore can
//! download ahead of the apply cursor. [`apply_plan`] streams the objects
//! through `futures::stream::buffered(queue_depth)`: at most `queue_depth`
//! downloads are in flight at once, and — critically — `buffered` **preserves
//! order**, so objects are still *applied* strictly in plan order while later
//! ones download. Concurrency 1 (sequential) and concurrency 4 produce a
//! byte-identical database; the plan order is what determines the result, not
//! the download schedule. (Proven by the e2e `prefetch_1_vs_4_byte_identical`.)
//!
//! ## Memory bound (documented, hard)
//!
//! Peak prefetch memory is `queue_depth × object_size` — at most `queue_depth`
//! downloaded object buffers held at once, plus the one being applied. It is
//! **not** `O(total history)`: the stream never materializes the whole plan.
//! With the default `queue_depth = 4` and typical merged objects, this is a few
//! object-sizes, independent of history length.
//!
//! ## Failure is loud and leaves no partial output
//!
//! Any prefetch failure, decode failure, chain break, or apply failure aborts
//! the whole restore with a typed error. The caller applies the plan to a
//! **staged** temp file and only renames it into place on full success (the
//! existing temp+rename discipline), so a failed restore never publishes a
//! partial database.
//!
//! ## Chain linkage through merged objects
//!
//! Each applied object is chain-verified against the running chain value with
//! [`physical::verify_chain`] (the existing DB-anchored pre-apply check), and
//! the running value then advances via [`chain_end`] — which returns a merged
//! object's *declared* end-of-range value, so the chain links through a
//! compacted range exactly as it did through the fine objects it replaced.

use std::path::Path;

use anyhow::Result;
use futures::stream::{self, StreamExt};
use hadb_changeset::physical::{self, chain_end};

use super::layout::{CompactionLayout, LayoutFile, Level};
use super::planner::{plan_restore, PlanCandidate, RestorePlan};
use super::CompactionError;
use crate::errors::WalrustError;

/// Default bounded-prefetch queue depth. Peak prefetch memory is
/// `DEFAULT_PREFETCH_DEPTH × object_size`.
pub const DEFAULT_PREFETCH_DEPTH: usize = 4;

/// Levels above L0 to probe when gathering candidates. The design builds two
/// (L1, L2) but leaves room; capped here so a hostile bucket cannot force
/// unbounded LISTs. Every level up to the cap is probed (no early stop on an
/// empty level) — see the note in [`gather_candidates`].
const MAX_LEVEL_PROBE: Level = 16;

/// Gather every restorable object relevant to a restore from `floor` up to
/// `target`: the L0 raw pool plus every merged level. Objects fully at or below
/// `floor`, or entirely above `target`, are dropped (they can never be chosen).
///
/// Cost: one LIST + N ranged header reads per non-empty level (via
/// [`CompactionLayout::list_level`]).
pub async fn gather_candidates(
    layout: &dyn CompactionLayout,
    floor: u64,
    target: u64,
) -> Result<Vec<PlanCandidate>, CompactionError> {
    let mut out = Vec::new();
    let push_from = |files: Vec<LayoutFile>, out: &mut Vec<PlanCandidate>| {
        for f in files {
            if f.range.max > floor && f.range.min <= target {
                out.push(PlanCandidate {
                    key: f.key,
                    level: f.level,
                    range: f.range,
                });
            }
        }
    };

    // L0 raw pool.
    let l0 = layout.list_level(0).await?;
    push_from(l0, &mut out);

    // Merged levels. Every level `1..=MAX_LEVEL_PROBE` is probed — the scan does
    // **not** stop at the first empty level. Promotion empties a lower level when
    // it folds all of its objects up (an L1→L2 merge deletes its L1 sources), so a
    // populated L2 routinely sits above an empty L1. Stopping early would drop that
    // L2 from the candidate pool and make the planner report a phantom ChainGap on
    // a perfectly restorable bucket. The cap still bounds the LISTs.
    for level in 1..=MAX_LEVEL_PROBE {
        let files = layout.list_level(level).await?;
        push_from(files, &mut out);
    }

    Ok(out)
}

/// Plan a restore over a [`CompactionLayout`] from `floor` to `target`.
///
/// Convenience wrapper: [`gather_candidates`] + [`plan_restore`]. The planner's
/// typed [`PlanError`](super::planner::PlanError) (chain gap / PITR-inside-
/// merged-window) is surfaced verbatim in the returned error's message.
pub async fn plan_over_layout(
    layout: &dyn CompactionLayout,
    floor: u64,
    target: u64,
) -> Result<RestorePlan> {
    let candidates = gather_candidates(layout, floor, target).await?;
    plan_restore(&candidates, floor, target)
        .map_err(|e| anyhow::Error::from(WalrustError::restore(e.to_string())))
}

/// Apply a planned restore onto a **staged** database file (the base snapshot
/// must already be materialized there), with bounded-concurrency prefetch.
///
/// `floor_checksum` is the chain value the staged base restores to (the first
/// applied object must chain from it). Returns the final restored seq
/// (`== plan.target` on success).
pub async fn apply_plan(
    layout: &dyn CompactionLayout,
    plan: &RestorePlan,
    staged_db: &Path,
    floor_checksum: u64,
    queue_depth: usize,
) -> Result<u64> {
    Ok(
        apply_plan_with_checksum(layout, plan, staged_db, floor_checksum, queue_depth)
            .await?
            .seq,
    )
}

pub(crate) struct AppliedPlan {
    pub(crate) seq: u64,
    pub(crate) checksum: u64,
}

/// Internal restore result used by owned resume. The public executor keeps its
/// historical `u64` return type, while core restore retains the already-known
/// chain checksum instead of hashing the whole restored database a second time.
pub(crate) async fn apply_plan_with_checksum(
    layout: &dyn CompactionLayout,
    plan: &RestorePlan,
    staged_db: &Path,
    floor_checksum: u64,
    queue_depth: usize,
) -> Result<AppliedPlan> {
    let depth = queue_depth.max(1);
    let mut running = floor_checksum;
    let mut cursor = plan.floor;

    // Bounded, order-preserving prefetch: at most `depth` downloads in flight;
    // objects are yielded — and therefore applied — in strict plan order.
    let mut fetched = stream::iter(plan.files.iter().cloned().map(|cand| {
        let lf = LayoutFile {
            key: cand.key.clone(),
            level: cand.level,
            range: cand.range,
            last_modified_ms: 0, // unused by read_bytes; keyed by `key`
        };
        async move {
            let bytes = layout.read_bytes(&lf).await;
            (cand, bytes)
        }
    }))
    .buffered(depth);

    while let Some((cand, bytes)) = fetched.next().await {
        let bytes = bytes.map_err(|e| {
            anyhow::Error::from(WalrustError::restore(format!(
                "restore prefetch failed for {}: {e}",
                cand.key
            )))
        })?;

        let cs = crate::ltx::decode_sqlite_changeset(&bytes)
            .map_err(|e| anyhow::anyhow!("failed to decode restore object {}: {e}", cand.key))?;

        // DB-anchored pre-apply chain check: the object must chain from the
        // running value. verify_chain also validates the object's own content
        // checksum, so a torn merged object is caught here.
        physical::verify_chain(running, &cs).map_err(|e| {
            anyhow::Error::from(WalrustError::integrity(format!(
                "restore chain broken at {} (seq range {}..={}): {e}",
                cand.key, cand.range.min, cand.range.max
            )))
        })?;

        crate::ltx::apply_decoded_changeset_to_db(&cs, staged_db)
            .map_err(|e| anyhow::anyhow!("failed to apply restore object {}: {e}", cand.key))?;

        // Advance linkage via chain_end (declared end for a merged object), not
        // the content checksum — so the chain links through a compacted range.
        running = chain_end(&cs);
        cursor = cand.range.max;
    }

    if cursor != plan.target {
        return Err(anyhow::Error::from(WalrustError::restore(format!(
            "restore incomplete: reached seq {cursor} but target is {}",
            plan.target
        ))));
    }
    Ok(AppliedPlan {
        seq: cursor,
        checksum: running,
    })
}
