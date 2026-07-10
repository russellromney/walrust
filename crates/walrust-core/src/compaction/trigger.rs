//! Count-based compaction triggers with a fine-grain retention window.
//!
//! Compaction is **count-triggered, not clock-triggered**: the writer knows how
//! many files it has produced at each level, so an idle database issues zero
//! compaction work and never polls LIST. The state is seeded once at startup
//! (one LIST per level) and then maintained in memory as the writer publishes.
//!
//! `keep_fine_window` protects recent history: L0 files younger than the window
//! are exempt from merging ("I can restore to the second within the last
//! hour"). The count only advances toward a merge for files old enough to be
//! eligible; the engine re-checks ages against the window at merge time (one
//! LIST when a trigger fires), so a young batch backs off and ages instead of
//! being merged prematurely.

use std::time::Duration;

/// Trigger configuration. Names speak user intent; defaults are conservative.
#[derive(Debug, Clone, Copy)]
pub struct TriggerConfig {
    /// Number of eligible L0 files that fills an L1 batch (L0 → L1 merge).
    pub l1_batch: usize,
    /// Number of L1 files that fills an L2 batch (L1 → L2 merge).
    pub l2_batch: usize,
    /// L0 files younger than this are never merged.
    pub keep_fine_window: Duration,
}

impl Default for TriggerConfig {
    fn default() -> Self {
        Self {
            l1_batch: 60,
            l2_batch: 24,
            keep_fine_window: Duration::from_secs(3600),
        }
    }
}

/// In-memory trigger state, maintained by the writer between the startup seed
/// and each publish.
#[derive(Debug, Clone)]
pub struct CompactionTriggers {
    config: TriggerConfig,
    /// Count of L0 files not yet folded into an L1 file. Seeded from a startup
    /// LIST, then `+1` per L0 publish and `-merged` per L0→L1 merge.
    l0_pending: usize,
    /// Count of L1 files not yet folded into an L2 file. Seeded from a startup
    /// LIST, then `+1` per L0→L1 merge and `-merged` per L1→L2 merge.
    l1_pending: usize,
}

impl CompactionTriggers {
    /// Seed from a one-time LIST of each level's current file counts.
    pub fn seeded(config: TriggerConfig, l0_files: usize, l1_files: usize) -> Self {
        Self {
            config,
            l0_pending: l0_files,
            l1_pending: l1_files,
        }
    }

    pub fn config(&self) -> &TriggerConfig {
        &self.config
    }

    pub fn l0_pending(&self) -> usize {
        self.l0_pending
    }
    pub fn l1_pending(&self) -> usize {
        self.l1_pending
    }

    /// Record that the writer just published one L0 (raw) file.
    pub fn on_l0_written(&mut self) {
        self.l0_pending = self.l0_pending.saturating_add(1);
    }

    /// True when the L0 count has reached the L1 batch size. The engine still
    /// verifies that enough files are older than `keep_fine_window` before
    /// merging (young batches back off).
    pub fn should_compact_l0_to_l1(&self) -> bool {
        self.l0_pending >= self.config.l1_batch
    }

    /// True when the L1 count has reached the L2 batch size.
    pub fn should_compact_l1_to_l2(&self) -> bool {
        self.l1_pending >= self.config.l2_batch
    }

    /// Record a completed L0 → L1 merge that folded `merged` L0 files into one
    /// new L1 file.
    pub fn on_l0_to_l1_merged(&mut self, merged: usize) {
        self.l0_pending = self.l0_pending.saturating_sub(merged);
        self.l1_pending = self.l1_pending.saturating_add(1);
    }

    /// Record a completed L1 → L2 merge that folded `merged` L1 files into one
    /// new L2 file.
    pub fn on_l1_to_l2_merged(&mut self, merged: usize) {
        self.l1_pending = self.l1_pending.saturating_sub(merged);
    }
}

/// Given a level's files (already sorted ascending by seq) and the current time,
/// return how many of the *oldest* files are old enough to merge (strictly
/// older than `keep_fine_window`). Only L0 uses the window; higher levels pass
/// a zero window so all files are eligible.
///
/// Returns the count of leading files whose `last_modified_ms` is older than the
/// cutoff. Because the young tail is what we protect, eligibility is a prefix of
/// the sorted list.
pub fn eligible_prefix_len(
    last_modified_ms: &[i64],
    now_ms: i64,
    keep_fine_window: Duration,
) -> usize {
    let window_ms = keep_fine_window.as_millis() as i64;
    let cutoff = now_ms - window_ms;
    let mut count = 0;
    for &ts in last_modified_ms {
        // Eligible when age >= window, i.e. ts <= now - window. With a zero
        // window nothing is exempt, so a file stamped exactly `now` qualifies.
        if ts <= cutoff {
            count += 1;
        } else {
            // The first young file ends the eligible prefix: newer files after
            // it (sorted by seq, roughly by time) stay protected too.
            break;
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_trigger_fires_at_batch() {
        let cfg = TriggerConfig {
            l1_batch: 3,
            l2_batch: 2,
            keep_fine_window: Duration::from_secs(0),
        };
        let mut t = CompactionTriggers::seeded(cfg, 0, 0);
        assert!(!t.should_compact_l0_to_l1());
        t.on_l0_written();
        t.on_l0_written();
        assert!(!t.should_compact_l0_to_l1());
        t.on_l0_written();
        assert!(t.should_compact_l0_to_l1());
    }

    #[test]
    fn l0_to_l1_merge_updates_both_levels() {
        let cfg = TriggerConfig {
            l1_batch: 3,
            l2_batch: 2,
            keep_fine_window: Duration::from_secs(0),
        };
        let mut t = CompactionTriggers::seeded(cfg, 3, 0);
        assert!(t.should_compact_l0_to_l1());
        t.on_l0_to_l1_merged(3);
        assert_eq!(t.l0_pending(), 0);
        assert_eq!(t.l1_pending(), 1);
        assert!(!t.should_compact_l0_to_l1());
    }

    #[test]
    fn l1_to_l2_cascade() {
        let cfg = TriggerConfig {
            l1_batch: 1,
            l2_batch: 2,
            keep_fine_window: Duration::from_secs(0),
        };
        let mut t = CompactionTriggers::seeded(cfg, 0, 0);
        // Two L0→L1 merges produce two L1 files → L2 batch fills.
        t.on_l0_to_l1_merged(1);
        assert!(!t.should_compact_l1_to_l2());
        t.on_l0_to_l1_merged(1);
        assert!(t.should_compact_l1_to_l2());
        t.on_l1_to_l2_merged(2);
        assert_eq!(t.l1_pending(), 0);
    }

    #[test]
    fn seed_reflects_existing_files() {
        let cfg = TriggerConfig::default();
        let t = CompactionTriggers::seeded(cfg, 120, 3);
        assert_eq!(t.l0_pending(), 120);
        assert_eq!(t.l1_pending(), 3);
        assert!(t.should_compact_l0_to_l1());
    }

    #[test]
    fn keep_fine_window_exempts_young_files() {
        let now = 1_000_000i64;
        let window = Duration::from_secs(100); // 100_000 ms
                                               // Four old files then two young ones (sorted ascending by seq/time).
        let ts = vec![
            now - 500_000, // old
            now - 400_000, // old
            now - 300_000, // old
            now - 200_000, // old
            now - 50_000,  // young (within window)
            now - 10_000,  // young
        ];
        assert_eq!(eligible_prefix_len(&ts, now, window), 4);
    }

    #[test]
    fn keep_fine_window_zero_makes_all_eligible() {
        let now = 1_000i64;
        let ts = vec![now, now, now];
        assert_eq!(eligible_prefix_len(&ts, now, Duration::from_secs(0)), 3);
    }

    #[test]
    fn all_young_yields_zero_eligible() {
        let now = 1_000_000i64;
        let window = Duration::from_secs(100);
        let ts = vec![now - 10_000, now - 5_000];
        assert_eq!(eligible_prefix_len(&ts, now, window), 0);
    }
}
