//! Shared seq-range coverage math for the read side.
//!
//! Both level-aware **verify** (does a merged range bridge an L0 hole?) and
//! level-aware **prune** (is a retained restore point still reachable?) ask the
//! same question: does a set of inclusive seq ranges *contiguously* cover a
//! target span? This is the one place that answers it, so the two consumers can
//! never drift apart — the "built once for both layouts" rule applied to the
//! read side.

use super::layout::{CompactionLayout, Level, SeqRange};
use super::CompactionError;

/// True iff the union of `ranges` covers the whole inclusive span `[lo, hi]`
/// with **no gap** — i.e. you can walk from `lo` to `hi` never stepping over an
/// uncovered seq. `lo > hi` is a vacuously-covered empty span (`true`).
///
/// Overlaps are fine; the check is contiguity, not disjointness.
pub fn ranges_cover(lo: u64, hi: u64, ranges: &[SeqRange]) -> bool {
    if lo > hi {
        return true;
    }
    // Sort by min; sweep a frontier of "highest contiguously covered seq".
    let mut sorted: Vec<SeqRange> = ranges.iter().copied().filter(|r| r.max >= lo).collect();
    sorted.sort_by_key(|r| (r.min, r.max));

    let mut frontier: Option<u64> = None; // highest seq covered so far, from `lo`
    for r in sorted {
        match frontier {
            None => {
                if r.min <= lo {
                    frontier = Some(r.max.max(lo));
                }
            }
            Some(f) => {
                if r.min <= f + 1 {
                    frontier = Some(f.max(r.max));
                } else {
                    // A hole opened between the frontier and this range.
                    break;
                }
            }
        }
        if let Some(f) = frontier {
            if f >= hi {
                return true;
            }
        }
    }
    frontier.map(|f| f >= hi).unwrap_or(false)
}

/// List the merged seq ranges present across every level ≥ 1 of a layout (the
/// L0 raw pool is excluded — it is the fine chain, not a bridge). Used by verify
/// and prune to reason about merged coverage. One LIST + N ranged reads per level.
///
/// Every level `1..=MAX_LEVEL_PROBE` is probed — the scan does **not** stop at the
/// first empty level. Promotion *empties* a lower level when it folds all of its
/// objects into the next one (an L1→L2 merge deletes its L1 sources), so the
/// healthy steady state routinely has an empty L1 above a populated L2. Stopping
/// at the first empty level would miss that L2 and make verify hallucinate a gap
/// (and prune under-collect). The `MAX_LEVEL_PROBE` cap still bounds the LISTs.
pub async fn list_merged_ranges(
    layout: &dyn CompactionLayout,
) -> Result<Vec<SeqRange>, CompactionError> {
    const MAX_LEVEL_PROBE: Level = 16;
    let mut out = Vec::new();
    for level in 1..=MAX_LEVEL_PROBE {
        let files = layout.list_level(level).await?;
        out.extend(files.into_iter().map(|f| f.range));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(min: u64, max: u64) -> SeqRange {
        SeqRange::new(min, max)
    }

    #[test]
    fn empty_span_is_covered() {
        assert!(ranges_cover(5, 4, &[]));
    }

    #[test]
    fn single_range_covers_exactly() {
        assert!(ranges_cover(4, 10, &[r(4, 10)]));
        assert!(ranges_cover(4, 10, &[r(1, 20)]));
        assert!(!ranges_cover(4, 10, &[r(4, 9)])); // one short
        assert!(!ranges_cover(4, 10, &[r(5, 10)])); // starts too late
    }

    #[test]
    fn adjacent_ranges_stitch() {
        assert!(ranges_cover(1, 10, &[r(1, 5), r(6, 10)]));
        // A one-seq hole at 6 is NOT covered.
        assert!(!ranges_cover(1, 10, &[r(1, 5), r(7, 10)]));
    }

    #[test]
    fn overlapping_ranges_stitch() {
        assert!(ranges_cover(1, 10, &[r(1, 7), r(5, 10)]));
        assert!(ranges_cover(1, 10, &[r(5, 10), r(1, 7)])); // order-insensitive
    }

    #[test]
    fn no_relevant_range_is_uncovered() {
        assert!(!ranges_cover(4, 10, &[r(11, 20)]));
        assert!(!ranges_cover(4, 10, &[]));
    }
}
