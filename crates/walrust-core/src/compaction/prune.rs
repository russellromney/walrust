//! Level-aware prune watermark (litestream `EnforceRetentionByTXID` shape).
//!
//! After snapshot retention decides which snapshots survive, the **watermark**
//! is `min(surviving snapshot seq)` — the earliest point any retained restore
//! can still start from (a snapshot is a full base; restores only ever walk
//! *up* from one). A merged level object whose whole range sits **below** that
//! watermark can never be part of a retained restore, so it is safe to delete.
//!
//! ## The rule (and why it is E2-safe)
//!
//! Delete a level object iff **both**:
//!   1. its `range.max < watermark` — it ends below the earliest restorable
//!      base, so no retained restore point (all at or above the watermark) needs
//!      it; **and**
//!   2. it is **not** the newest object at its level — always keep at least one
//!      object per level as a defensive tail.
//!
//! A merged object that *straddles* the watermark (`range.min < watermark <=
//! range.max`) has `range.max >= watermark` and is therefore **kept** — that is
//! exactly the object a restore from the watermark snapshot needs to bridge up
//! from the base, so the E2-class "never delete what a retained restore needs"
//! rule holds for merged files, not just snapshots. Neutering rule (1) — e.g.
//! deleting objects at or above the watermark — breaks a retained PITR; the
//! `fail-on-revert` proof pins this.

use super::layout::{CompactionLayout, LayoutFile, Level};
use super::CompactionError;
use std::collections::HashMap;

/// List every object across the merged levels (L ≥ 1) of a layout, for
/// watermark pruning. Every level `1..=MAX_LEVEL_PROBE` is probed — the scan does
/// **not** stop at the first empty level, because promotion empties a lower level
/// when it folds all of its objects up (an L1→L2 merge deletes its L1 sources),
/// so a populated L2 routinely sits above an empty L1. Stopping early would leave
/// higher-level objects below the watermark uncollected (a space leak). The cap
/// bounds the LISTs.
pub async fn list_level_files(
    layout: &dyn CompactionLayout,
) -> Result<Vec<LayoutFile>, CompactionError> {
    const MAX_LEVEL_PROBE: Level = 16;
    let mut out = Vec::new();
    for level in 1..=MAX_LEVEL_PROBE {
        let files = layout.list_level(level).await?;
        out.extend(files);
    }
    Ok(out)
}

/// Decide which level objects to delete given the retention `watermark` (the
/// oldest surviving snapshot's seq). Returns the deletable subset of
/// `level_files`; everything else is retained. See the module header for the
/// rule and its safety argument.
pub fn plan_level_prune(level_files: &[LayoutFile], watermark: u64) -> Vec<LayoutFile> {
    // Newest (highest range.max) object per level is always kept.
    let mut newest_per_level: HashMap<Level, u64> = HashMap::new();
    for f in level_files {
        newest_per_level
            .entry(f.level)
            .and_modify(|m| *m = (*m).max(f.range.max))
            .or_insert(f.range.max);
    }

    level_files
        .iter()
        .filter(|f| {
            let is_newest_at_level = newest_per_level
                .get(&f.level)
                .map(|m| f.range.max == *m)
                .unwrap_or(false);
            f.range.max < watermark && !is_newest_at_level
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::layout::SeqRange;

    fn lf(key: &str, level: Level, min: u64, max: u64) -> LayoutFile {
        LayoutFile {
            key: key.into(),
            level,
            range: SeqRange::new(min, max),
            last_modified_ms: 0,
        }
    }

    fn keys(files: &[LayoutFile]) -> Vec<String> {
        let mut k: Vec<String> = files.iter().map(|f| f.key.clone()).collect();
        k.sort();
        k
    }

    #[test]
    fn objects_wholly_below_watermark_are_deleted() {
        let files = vec![
            lf("L1/1-10", 1, 1, 10),
            lf("L1/11-20", 1, 11, 20),
            lf("L1/21-30", 1, 21, 30), // newest at L1
        ];
        // Watermark 21: only [1,10] and [11,20] end below it; [21,30] is newest.
        let del = plan_level_prune(&files, 21);
        assert_eq!(keys(&del), vec!["L1/1-10", "L1/11-20"]);
    }

    #[test]
    fn newest_per_level_is_always_kept() {
        // Even far below the watermark, the newest object at each level survives.
        let files = vec![lf("L1/1-10", 1, 1, 10), lf("L2/1-5", 2, 1, 5)];
        let del = plan_level_prune(&files, 1_000);
        assert!(del.is_empty(), "newest-per-level must never be deleted");
    }

    #[test]
    fn straddling_object_is_kept() {
        // [15,25] straddles watermark 21 (max 25 >= 21) → needed to bridge up.
        let files = vec![lf("L1/15-25", 1, 15, 25), lf("L1/26-40", 1, 26, 40)];
        let del = plan_level_prune(&files, 21);
        assert!(
            del.is_empty(),
            "a watermark-straddling merged object is needed"
        );
    }

    #[test]
    fn nothing_deleted_when_all_at_or_above_watermark() {
        let files = vec![lf("L1/21-30", 1, 21, 30), lf("L1/31-40", 1, 31, 40)];
        assert!(plan_level_prune(&files, 21).is_empty());
    }

    /// Fail-on-revert proof: with the correct watermark, a merged object a
    /// retained PITR still needs survives prune and the PITR plans; **neuter**
    /// the watermark guard (delete everything below `u64::MAX`) and that object
    /// is dropped, so the same retained PITR now fails with a chain gap.
    #[test]
    fn watermark_guard_revert_breaks_a_retained_pitr() {
        use crate::compaction::planner::{plan_restore, PlanCandidate, PlanError};

        // Oldest surviving snapshot restores state at seq 20 (the watermark).
        // A retained PITR to seq 40 needs the merged L1 objects [21,30],[31,40].
        let watermark = 20u64;
        let level_files = vec![
            lf("L1/1-10", 1, 1, 10),   // below watermark: prunable
            lf("L1/11-20", 1, 11, 20), // ends at watermark: kept (max !< 20)
            lf("L1/21-30", 1, 21, 30), // needed by the retained PITR
            lf("L1/31-40", 1, 31, 40), // needed by the retained PITR (newest)
        ];
        let candidates = |files: &[LayoutFile]| -> Vec<PlanCandidate> {
            files
                .iter()
                .map(|f| PlanCandidate {
                    key: f.key.clone(),
                    level: f.level,
                    range: f.range,
                })
                .collect()
        };

        // Correct watermark: only [1,10] is deleted; the retained PITR to 40 plans.
        let del = plan_level_prune(&level_files, watermark);
        assert_eq!(keys(&del), vec!["L1/1-10"]);
        let surviving: Vec<LayoutFile> = level_files
            .iter()
            .filter(|f| !del.iter().any(|d| d.key == f.key))
            .cloned()
            .collect();
        let plan = plan_restore(&candidates(&surviving), watermark, 40)
            .expect("retained PITR must still plan after a correct prune");
        assert_eq!(
            plan.files.len(),
            2,
            "PITR uses the two surviving L1 objects"
        );

        // Neutered guard: treat EVERYTHING below u64::MAX as prunable (the guard
        // that keeps watermark-and-above is removed). The objects the PITR needs
        // are deleted, keeping only the newest per level.
        let bad_del = plan_level_prune(&level_files, u64::MAX);
        let bad_survivors: Vec<LayoutFile> = level_files
            .iter()
            .filter(|f| !bad_del.iter().any(|d| d.key == f.key))
            .cloned()
            .collect();
        let err = plan_restore(&candidates(&bad_survivors), watermark, 40)
            .expect_err("neutered watermark must break the retained PITR");
        assert!(
            matches!(err, PlanError::ChainGap { .. }),
            "expected a chain gap after the guard is reverted, got {err:?}"
        );
    }
}
