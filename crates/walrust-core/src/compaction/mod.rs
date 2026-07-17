//! Native HADBP compaction: merge many small changesets into fewer objects.
//!
//! The engine merges contiguous native HADBP source changesets into a
//! COMPACTED changeset at the next level. Level 0 uses the fixed native
//! incremental generation; higher levels use `levels/L{n}`. Object names and
//! chain checks remain versioned native HADBP invariants.
//!
//! ## Memory bound (hard requirement) — the honest two-part statement
//!
//! **Streaming frontier (the hard requirement):** the merge holds at most one
//! frontier page per source plus one emit slot — `O(page_size × source_count +
//! page-id frontier)`, **never `O(total bytes)`**. Sources are iterated via
//! [`ChangesetPageStream`], never slurped. Proven by
//! [`merge::tests::peak_pages_bounded_far_below_total`], which merges 2000 pages
//! across 4 sources while a RAII page counter records a peak of `<= sources + 1`.
//!
//! **Output/verify buffer (inherent, honestly `O(output size)`):** the engine
//! **hand-rolls a streaming encoder** in [`merge::merge_changesets`]
//! (`emit_page` appends each merged page directly to the output byte buffer). It
//! deliberately does **not** build a `PhysicalChangeset { pages: Vec<..> }` and
//! call `physical::encode`, which would require the full page vector in memory —
//! so there is no hidden `O(output)` page-vec materialization at the encode
//! stage. The one unavoidable `O(output size)` buffer is the serialized bytes
//! handed to the single `StorageBackend::put` (the trait has no streaming put),
//! and symmetrically the read-back `verify_merged_bytes` decode. `output size`
//! is the count of *unique* merged pages; in the pathological no-overlap case it
//! approaches total input size, but it is a **single** materialization, not
//! `O(sources × bytes)`. Removing even this buffer needs a streaming put +
//! streaming decode on `StorageBackend`/hadb — **deferred to C2b/hadb**; the
//! honest current bound is stated here rather than claimed away.
//!
//! ## List cost at startup and merge time (S3 request budget)
//!
//! Header reads are **ranged** (`range_get(key, 0, 48)`), never full-object
//! GETs. Two distinct costs:
//!   - **Seeding trigger state** (`CompactionTriggers::seeded`, once per DB at
//!     startup): uses `count_level`, which is **one LIST per level, zero header
//!     reads**. A level with `N` files costs 1 LIST, independent of `N`.
//!   - **A merge tick** (only when a count batch fills): `list_level` does 1
//!     LIST + `N` ranged 48-byte header reads for the level, because
//!     `last_modified_ms` (needed for the L0 `keep_fine_window` check) lives in
//!     each object's HADBP header and `StorageBackend` exposes no HEAD. So a
//!     fire costs `1 LIST + N ranged-GETs` at the source level. This is paid
//!     only when merging, not on idle ticks.

pub mod coverage;
pub mod engine;
pub mod layout;
pub mod merge;
pub mod planner;
pub mod prune;
pub mod restore;
pub mod seq_layout;
pub mod trigger;

pub use coverage::{list_merged_ranges, ranges_cover};
pub use engine::{run_level_compaction, CompactionOutcome};
pub use layout::{
    format_range_name, level_subpath, parse_range_name, ChangesetPageStream, CompactionLayout,
    LayoutFile, Level, SeqRange, SourceHeader, L0_DIR, LEVELS_DIR,
};
pub use merge::{merge_changesets, verify_merged_bytes, MergeInput, MergeResult, PeakPages};
pub use planner::{
    plan_restore, refine_gap_with_snapshot_spans, PlanCandidate, PlanError, RestorePlan,
};
pub use prune::{list_level_files, plan_level_prune};
pub use restore::{apply_plan, gather_candidates, plan_over_layout, DEFAULT_PREFETCH_DEPTH};
pub use seq_layout::SeqLayout;
pub use trigger::{CompactionSettings, CompactionTriggers, TriggerConfig};

use thiserror::Error;

/// Typed, loud compaction failures. The write/delete path never
/// warns-and-continues: any of these aborts the run and (for verify/write
/// failures) leaves sources untouched.
#[derive(Debug, Error)]
pub enum CompactionError {
    /// A merge was asked to run over zero sources.
    #[error("compaction: refusing to merge an empty source set")]
    EmptySourceSet,

    /// Sources disagree on page geometry (page_id width or page size); they
    /// cannot belong to one database chain.
    #[error("compaction: mismatched page geometry across sources: {0}")]
    MixedGeometry(String),

    /// The selected sources are not a contiguous chain: some source's
    /// `prev_checksum` does not equal the prior source's `chain_end()`.
    #[error("compaction: sources are not a contiguous chain: {0}")]
    NonContiguous(String),

    /// A merged object failed read-back verification (decode, chain_end, or
    /// page-count sanity). Sources are NOT deleted.
    #[error("compaction: merged object failed verification: {0}")]
    VerificationFailed(String),

    /// The target level already holds a merged object whose seq range *overlaps*
    /// the batch being merged but is not an exact-range match. Only an
    /// exact-range object is idempotent crash convergence; a partial
    /// subset/superset overlap is an unexpected, inconsistent state (e.g. a prior
    /// run merged a different batch size). The engine refuses to merge into it
    /// rather than silently leaving an orphan. Nothing is written or deleted.
    #[error("compaction: unexpected overlapping merged object at target level: {0}")]
    OverlappingExisting(String),

    /// A changeset object was malformed on read.
    #[error("compaction: malformed changeset at {key}: {source}")]
    Decode {
        key: String,
        source: hadb_changeset::error::ChangesetError,
    },

    /// A range filename did not match the forever key scheme.
    #[error("compaction: unparseable range key: {0}")]
    BadRangeKey(String),

    /// Underlying storage error (kept as a string so `CompactionError` stays
    /// `Clone`-free but convertible; anyhow's blanket `From<E: Error>` carries
    /// the typed variant into an `anyhow::Error` chain).
    #[error("compaction: storage error: {0}")]
    Storage(String),
}
