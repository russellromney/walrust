//! The greedy restore planner (litestream `CalcRestorePlan` shape, adapted).
//!
//! Given the newest snapshot at or before the target and the pool of restorable
//! objects across **every level** (L0 raw points plus L1/L2… merged ranges),
//! the planner produces the *ordered* list of objects a restore must apply to
//! rebuild the database from `floor+1` up to `target`.
//!
//! ## The algorithm (pure, no I/O)
//!
//! Start a `cursor` at the snapshot's covered seq (`floor`). While
//! `cursor < target`, the chain must continue at `need = cursor + 1`:
//!
//! 1. Among candidates that **begin exactly at `need`** and do **not overshoot**
//!    (`range.min == need && range.max <= target`), pick the one that extends the
//!    contiguous range **furthest** (`max range.max`). Advance `cursor` to its
//!    `range.max` and record it. This is the greedy "extend furthest" step: a
//!    coarse merged range wins over the fine L0 points it supersedes, so a long
//!    history restores from a handful of objects.
//! 2. If nothing lands within `target` but some object *covers* `need` by
//!    overshooting it (`range.min == need && range.max > target`), the target
//!    falls **strictly inside a merged window** with no finer coverage — a hard
//!    [`PlanError::PitrInsideMergedWindow`] naming the nearest restorable points
//!    on both sides (PITR granularity decay, by design).
//! 3. Otherwise nothing continues the chain at `need`: a hard
//!    [`PlanError::ChainGap`] (an object is missing or was pruned).
//!
//! ## Why `range.min == need` exactly (chain integrity)
//!
//! Successor linkage is verified through `chain_end()`: the next object's
//! `prev_checksum` must equal the chain value at the end of what came before.
//! An object that *starts before* `need` (`range.min < need`) carries a
//! `prev_checksum` anchored to an earlier point in the chain, so it cannot link
//! onto a database already advanced to `cursor` — applying it would be a chain
//! fork, not a continuation. Requiring `range.min == need` is exactly
//! litestream's contiguity rule (`MinTXID == prev.MaxTXID + 1`), generalized
//! from one-second points to arbitrary merged ranges.
//!
//! ## Overlap tolerance (crash leftovers), no double-apply
//!
//! Overlapping candidates are legal: a crash between "write merged object" and
//! "delete sources" leaves an L1 range `[1,10]` alongside the L0 points `1..=10`
//! it supersedes. The planner picks the furthest extension at each `need`, so it
//! takes the `[1,10]` range, advances the cursor past `10`, and never revisits
//! the now-behind points — a range is applied **at most once** and no seq is
//! covered twice. (Proven by `tests::crash_overlap_l0_and_l1_no_double_apply`.)

use super::layout::{Level, SeqRange};

/// One object the planner can choose from: its storage key, the level it lives
/// at, and the inclusive seq span it covers. Gathered from L0 and every merged
/// level; see [`super::restore::gather_candidates`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCandidate {
    pub key: String,
    pub level: Level,
    pub range: SeqRange,
}

/// An ordered restore plan: apply `files` in order **after** the base snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorePlan {
    /// The snapshot's covered seq (the plan continues from `floor + 1`).
    pub floor: u64,
    /// The seq the plan restores up to (inclusive).
    pub target: u64,
    /// Objects to apply, in strict apply order.
    pub files: Vec<PlanCandidate>,
}

impl RestorePlan {
    /// The ordered storage keys, for prefetch/apply.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.files.iter().map(|f| f.key.as_str())
    }
}

/// A typed, loud planning failure. Both variants are hard errors — the planner
/// never silently returns a short or forked plan.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    /// No object continues the chain at `needed`. An L0/level object is missing
    /// or was pruned below a still-referenced point. `nearest_below` is the
    /// furthest seq the plan reached (the last restorable point at or below the
    /// target).
    #[error(
        "restore chain gap: no backup object continues the chain at seq {needed}; \
         the nearest restorable point at or below the target is seq {nearest_below} \
         (an incremental or merged object is missing or was pruned)"
    )]
    ChainGap { needed: u64, nearest_below: u64 },

    /// The requested point-in-time seq falls **strictly inside** a merged window
    /// and no finer object covers it: PITR granularity has decayed with age. The
    /// message names the nearest restorable points on **both** sides so the
    /// operator can pick a valid one.
    #[error(
        "point-in-time seq {target} falls inside merged window \
         [{window_min}..={window_max}] with no finer coverage: PITR granularity \
         has decayed for history this old. Nearest restorable points are seq \
         {nearest_below} (below) and seq {nearest_above} (above); restore to one \
         of those, or widen keep_fine_window to retain second-grain history longer"
    )]
    PitrInsideMergedWindow {
        target: u64,
        window_min: u64,
        window_max: u64,
        nearest_below: u64,
        nearest_above: u64,
    },
}

/// Plan a restore from `floor` (the snapshot's covered seq) up to `target`.
///
/// `candidates` may contain objects from any level, in any order, and may
/// overlap. Objects entirely at or below `floor` are simply never chosen. See
/// the module header for the full algorithm and its invariants.
pub fn plan_restore(
    candidates: &[PlanCandidate],
    floor: u64,
    target: u64,
) -> Result<RestorePlan, PlanError> {
    let mut files = Vec::new();
    let mut cursor = floor;

    while cursor < target {
        let need = cursor + 1;

        // 1. Furthest extension that begins exactly at `need` without overshoot.
        let best_within = candidates
            .iter()
            .filter(|c| c.range.min == need && c.range.max <= target)
            .max_by_key(|c| c.range.max);
        if let Some(pick) = best_within {
            cursor = pick.range.max;
            files.push(pick.clone());
            continue;
        }

        // 2. `need` is covered only by object(s) that overshoot `target`: the
        //    target lies strictly inside a merged window. Pick the *tightest*
        //    window so `nearest_above` is the closest restorable boundary.
        let overshoot = candidates
            .iter()
            .filter(|c| c.range.min == need && c.range.max > target)
            .min_by_key(|c| c.range.max);
        if let Some(window) = overshoot {
            return Err(PlanError::PitrInsideMergedWindow {
                target,
                window_min: window.range.min,
                window_max: window.range.max,
                nearest_below: cursor,
                nearest_above: window.range.max,
            });
        }

        // 3. Nothing continues the chain at `need`.
        return Err(PlanError::ChainGap {
            needed: need,
            nearest_below: cursor,
        });
    }

    Ok(RestorePlan {
        floor,
        target,
        files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(key: &str, level: Level, min: u64, max: u64) -> PlanCandidate {
        PlanCandidate {
            key: key.into(),
            level,
            range: SeqRange::new(min, max),
        }
    }

    /// A pool of one-second L0 points [lo..=hi].
    fn points(lo: u64, hi: u64) -> Vec<PlanCandidate> {
        (lo..=hi)
            .map(|s| cand(&format!("L0/{s}"), 0, s, s))
            .collect()
    }

    #[test]
    fn unleveled_bucket_plans_the_naive_incremental_sequence() {
        // The regression anchor: with only L0 points, the plan is byte-identical
        // to today's "apply every incremental in seq order" restore.
        let cands = points(4, 10);
        let plan = plan_restore(&cands, 3, 10).unwrap();
        let got: Vec<&str> = plan.keys().collect();
        let want: Vec<String> = (4..=10).map(|s| format!("L0/{s}")).collect();
        assert_eq!(got, want.iter().map(String::as_str).collect::<Vec<_>>());
    }

    #[test]
    fn merged_range_wins_over_fine_points() {
        // L1 [4,10] plus the L0 points it supersedes: the coarse range is chosen
        // and the plan is a single object.
        let mut cands = points(4, 10);
        cands.push(cand("L1/4-10", 1, 4, 10));
        let plan = plan_restore(&cands, 3, 10).unwrap();
        assert_eq!(plan.files.len(), 1);
        assert_eq!(plan.files[0].key, "L1/4-10");
    }

    #[test]
    fn coarse_then_fine_tail() {
        // L2 [4,60], then L1 [61,80], then fine L0 tail 81..=83.
        let mut cands = vec![cand("L2/4-60", 2, 4, 60), cand("L1/61-80", 1, 61, 80)];
        cands.extend(points(81, 83));
        let plan = plan_restore(&cands, 3, 83).unwrap();
        let got: Vec<&str> = plan.keys().collect();
        assert_eq!(got, vec!["L2/4-60", "L1/61-80", "L0/81", "L0/82", "L0/83"]);
    }

    #[test]
    fn crash_overlap_l0_and_l1_no_double_apply() {
        // Duplicate coverage: L1 [4,10] AND every L0 point 4..=10 (a crash left
        // the sources). The plan applies the range once and never the points.
        let mut cands = points(4, 10);
        cands.insert(0, cand("L1/4-10", 1, 4, 10)); // present, listed first
        let plan = plan_restore(&cands, 3, 10).unwrap();
        assert_eq!(plan.files, vec![cand("L1/4-10", 1, 4, 10)]);
        // Every covered seq appears in exactly one applied range.
        let mut covered = std::collections::BTreeSet::new();
        for f in &plan.files {
            for s in f.range.min..=f.range.max {
                assert!(covered.insert(s), "seq {s} covered twice (double-apply)");
            }
        }
    }

    #[test]
    fn pitr_on_a_merged_boundary_succeeds() {
        // Target == a merged window's max: a clean boundary, restorable.
        let cands = vec![cand("L1/4-10", 1, 4, 10)];
        let plan = plan_restore(&cands, 3, 10).unwrap();
        assert_eq!(plan.files.len(), 1);
        assert_eq!(plan.target, 10);
    }

    #[test]
    fn pitr_inside_a_merged_window_is_a_loud_error_naming_neighbors() {
        // Only the coarse L1 [4,10] survives; target 7 is strictly inside it.
        let cands = vec![cand("L1/4-10", 1, 4, 10)];
        let err = plan_restore(&cands, 3, 7).unwrap_err();
        match err {
            PlanError::PitrInsideMergedWindow {
                target,
                window_min,
                window_max,
                nearest_below,
                nearest_above,
            } => {
                assert_eq!(target, 7);
                assert_eq!(window_min, 4);
                assert_eq!(window_max, 10);
                assert_eq!(nearest_below, 3, "nearest below == the snapshot floor");
                assert_eq!(nearest_above, 10, "nearest above == the window end");
            }
            other => panic!("expected PitrInsideMergedWindow, got {other:?}"),
        }
    }

    #[test]
    fn pitr_inside_window_but_finer_coverage_exists_succeeds_at_seq_grain() {
        // The keep_fine tail still holds the points, so target 7 restores exactly.
        let mut cands = vec![cand("L1/4-10", 1, 4, 10)];
        cands.extend(points(4, 10));
        let plan = plan_restore(&cands, 3, 7).unwrap();
        let got: Vec<&str> = plan.keys().collect();
        assert_eq!(got, vec!["L0/4", "L0/5", "L0/6", "L0/7"]);
    }

    #[test]
    fn gap_is_a_loud_error_naming_the_nearest_below() {
        // Points 4,5 then a hole at 6 (nothing covers it).
        let mut cands = points(4, 5);
        cands.extend(points(7, 9));
        let err = plan_restore(&cands, 3, 9).unwrap_err();
        match err {
            PlanError::ChainGap {
                needed,
                nearest_below,
            } => {
                assert_eq!(needed, 6);
                assert_eq!(nearest_below, 5);
            }
            other => panic!("expected ChainGap, got {other:?}"),
        }
    }

    #[test]
    fn straddling_range_below_floor_is_not_used() {
        // A merged range [1,10] that starts before the snapshot floor (3) cannot
        // link onto the snapshot; only a range/point starting at 4 continues.
        let cands = vec![cand("L1/1-10", 1, 1, 10)];
        let err = plan_restore(&cands, 3, 10).unwrap_err();
        assert!(matches!(err, PlanError::ChainGap { needed: 4, .. }));
    }

    #[test]
    fn target_equals_floor_is_empty_plan() {
        let plan = plan_restore(&[], 5, 5).unwrap();
        assert!(plan.files.is_empty());
    }
}
