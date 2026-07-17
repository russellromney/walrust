//! Compaction: merge many small changesets into fewer, larger ones.
//!
//! This is the **write side** of walrust's leveled compaction (wave C2a).
//! It merges N contiguous source changesets at one level into a single
//! `COMPACTED` (HADBP version 2) changeset at the next level, so a long-history
//! database restores from a snapshot + a few coarse-grain files + a fine tail
//! instead of tens of thousands of one-second objects.
//!
//! ## Read side + config exposure (wave C2b)
//!
//! The C2b read side ships here: the greedy [`planner`] (newest snapshot ≤
//! target, then the file that extends the contiguous range furthest), the
//! layout-agnostic [`restore`] executor (bounded parallel prefetch, strict-order
//! apply, chain linkage through [`chain_end`](hadb_changeset::physical::chain_end)),
//! level-aware verify [`coverage`], and the level-aware prune watermark
//! ([`prune`]). Compaction is now controlled by the **single** user knob
//! [`CompactionSettings`] / `[compaction] enabled` (default **false** — ship-dark
//! for version skew: an old binary cannot restore a leveled bucket). The C2a
//! internal flags (`Replicator::set_compaction_enabled`, `const
//! COMPACTION_ENABLED`) are gone; the config is the only control.
//!
//! ## Key naming is forever
//!
//! The level-key scheme fossilizes like a wire format. It is fixed here and
//! must not change:
//!
//! - **Range name** (a merged object covering an inclusive seq span
//!   `[min, max]`): `{min:016x}-{max:016x}.{ext}`. Both bounds are `u64`
//!   rendered as **16 lowercase zero-padded hex digits** (generous, `u64`-safe),
//!   joined by `-` (0x2D). The separator sorts before every hex digit
//!   (`0`..=`f`), so a listing is ordered by `min` and a point file
//!   (`{s}-{s}`) interleaves correctly with ranges. `ext` is `hadbp`
//!   (`ChangesetKind::Physical`) for the seq layout and `ltx` for the
//!   litestream-heritage range layout.
//! - **Level → directory**: Level 0 (raw) is the existing incremental pool at
//!   `{prefix}{db}/0000/` (hadb's `GENERATION_INCREMENTAL`); the compaction
//!   engine only *reads* it. Level `L >= 1` lives under a **dedicated
//!   `{prefix}{db}/levels/L{L}/` sub-path**, deliberately *not* a hex generation
//!   folder.
//!
//! ### Why levels are not generation folders (the collision that shaped this)
//!
//! The litestream-heritage legacy layout also lives under `{prefix}{db}/`, and
//! it stores **snapshots in generation folders whose number increments by one
//! per snapshot** (`snapshot_gen = current_gen + 1`, `legacy_wal_sync`). So the
//! 16th snapshot of any long-lived database lands in `0010/`. An earlier design
//! put compaction L1 at generation `0x0010` — the *same folder*. That was a
//! two-way corruption:
//!   - `legacy_manifest::discover_legacy_snapshots` treats **every** file in
//!     generations `1..=max` as a snapshot, so it would classify compaction
//!     merged objects (HADBP payloads, `.ltx` extension) as snapshot bases in
//!     discovery / restore / verify / prune.
//!   - Compaction's `list_level(1)` would list `0010/` and `parse_range_name`
//!     the real `{min}-{max}.ltx` snapshots there, ingesting them as merge
//!     sources.
//! The dedicated `levels/L{L}/` sub-path is **structurally invisible** to every
//! existing scanner: legacy discovery only accepts the `{gen}/{file}` (2-part)
//! and `{file}` (1-part) relative shapes and parses the generation with
//! `from_str_radix(_, 16)`, which rejects `levels` and `L1`. Owned-mode
//! discovery only scans `0000/` (incrementals) and the fixed snapshot
//! generation. No consumer of generation numbers can misread a compaction
//! object.
//!
//! The **seq layout** (owned mode) names level-0 files `{seq:016x}.hadbp` (one
//! seq per file; hadb's canonical `format_key`) and every merged level with the
//! range-name scheme above, under `levels/L{L}/`. The **range layout**
//! (litestream heritage) uses the range-name scheme at every level — `0000/` for
//! L0 (its live LTX pool) and `levels/L{L}/` for merged levels.
//!
//! ## Decision: both adapters carry HADBP payloads
//!
//! The merge engine produces a COMPACTED v2 changeset — a HADBP-format
//! construct. Rather than invent an LTX-format merge, both adapters read and
//! write **HADBP** payloads; the range layout contributes the litestream
//! *range-key discipline* (gen folders, `{min}-{max}` filenames), not the LTX
//! byte format. Legacy `.ltx` payloads are read by the existing restore path,
//! not the compactor; walrust has emitted HADBP changesets since the C1
//! migration. This keeps the engine format-uniform and "built once for both
//! layouts", exactly as the roadmap requires. (Documented per "decide and
//! document; no one answers questions.")
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
pub mod range_layout;
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
pub use range_layout::RangeLayout;
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
