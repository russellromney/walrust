//! Compaction: merge many small changesets into fewer, larger ones.
//!
//! This is the **write side** of walrust's leveled compaction (wave C2a).
//! It merges N contiguous source changesets at one level into a single
//! `COMPACTED` (HADBP version 2) changeset at the next level, so a long-history
//! database restores from a snapshot + a few coarse-grain files + a fine tail
//! instead of tens of thousands of one-second objects.
//!
//! ## Config exposure ships with the C2b planner
//!
//! Compaction is **gated off** and unreachable from `walrust.toml` or the CLI.
//! Enabling it now would leave backups **unrestorable** by the shipped restore
//! path, which cannot yet read leveled buckets. The merge engine, adapters,
//! triggers, and safety proofs land fully tested here; the trigger wiring is
//! guarded by an internal flag (`Replicator::set_compaction_enabled`, default
//! false) and a `const COMPACTION_ENABLED: bool = false` in the legacy watch
//! path. The user-facing `[compaction] enabled` knob ships with the C2b restore
//! planner that can read the leveled layout.
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
//! - **Level → generation directory**: `{prefix}{db}/{gen:04x}/`. Level 0 (raw)
//!   is generation `0x0000` — the existing incremental pool (hadb's
//!   `GENERATION_INCREMENTAL`); the compaction engine only *reads* it. Level
//!   `L >= 1` maps to generation `COMPACTION_GEN_BASE + (L - 1)` with
//!   `COMPACTION_GEN_BASE = 0x0010`, so compaction generations sit well clear of
//!   the incremental pool (`0x0000`) and the snapshot pool (`0x0001`), leaving
//!   `0x0002..=0x000f` for future non-compaction generations.
//!
//! The **seq layout** (owned mode) names level-0 files `{seq:016x}.hadbp` (one
//! seq per file; hadb's canonical `format_key`) and every merged level with the
//! range-name scheme above. The **range layout** (litestream heritage) uses the
//! range-name scheme at *every* level, in generation folders — the min-max
//! filename discipline litestream already uses for its live LTX pool.
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
//! ## Memory bound (hard requirement)
//!
//! The merge streams sources page-by-page. Peak working set is
//! `O(page_size × source_count + page-id frontier)` — one frontier page per
//! source plus the emit slot — never `O(total bytes)`. Sources are iterated via
//! [`ChangesetPageStream`], never slurped. The serialized *output* object is
//! materialized once for the single `StorageBackend::put` (the trait has no
//! streaming put); that is `O(output size)`, inherent to producing one object
//! with a trailer checksum, and bounded by the merged result — not by
//! re-buffering all N sources. See [`merge`] and its `peak_pages` proof test.

pub mod engine;
pub mod layout;
pub mod merge;
pub mod range_layout;
pub mod seq_layout;
pub mod trigger;

pub use engine::{run_level_compaction, CompactionOutcome};
pub use layout::{
    format_range_name, level_generation, parse_range_name, ChangesetPageStream, CompactionLayout,
    LayoutFile, Level, SeqRange, SourceHeader, COMPACTION_GEN_BASE,
};
pub use merge::{merge_changesets, verify_merged_bytes, MergeInput, MergeResult, PeakPages};
pub use range_layout::RangeLayout;
pub use seq_layout::SeqLayout;
pub use trigger::{CompactionTriggers, TriggerConfig};

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
