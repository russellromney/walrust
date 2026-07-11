//! Model-based state-machine property testing for walrust.
//!
//! This is the third walrust testing instrument, after the spec property suite
//! ([`crate::properties`] / [`crate::invariants`]) and the user-drill harness.
//! Where those check hand-written sequences, this instrument lets proptest
//! GENERATE the sequences: a `Vec<Op>` of writes, flushes, snapshots,
//! checkpoints, kill/restarts, PITR marks, prunes and restores, plus an
//! optional deterministic fault plan. The machine drives the REAL walrust
//! engine through that sequence and grades every restore against an
//! independent model oracle. Interleavings nobody thought to write by hand get
//! explored, shrunk to a minimal reproducer on failure, and persisted to
//! `proptest-regressions/` as a permanent corpus.
//!
//! # What real code this drives
//!
//! The machine runs the production CLI engine surface — the legacy Litestream
//! LTX object layout, which is the layout both real criticals (the E1
//! restart-under-load halt and the E2 prune-vs-PITR foreclosure) lived in,
//! and the only layout where publish, restore, AND pruning are all product
//! code on `&dyn StorageBackend` (so the DST mock can sit underneath):
//!
//! - `WriteTxn`  -> commits rows through a real `rusqlite` connection (one
//!   long-lived writer connection, `wal_autocheckpoint=0`, as the caller
//!   contract requires).
//! - `Flush`     -> `legacy_wal_sync::sync_watched_db_once_to_storage`: reads
//!   real WAL frames (with per-frame checksum validation and the salt/size
//!   rollover heal at its head) and publishes an incremental LTX. This is the
//!   durability point.
//! - `Snapshot`  -> `legacy_wal_sync::take_snapshot_to_storage`: passive WAL
//!   fold + full-DB snapshot into a fresh generation.
//! - `Checkpoint`-> flush, then `legacy_wal_sync::checkpoint_wal_truncate`
//!   (walrust's own completeness-checked TRUNCATE fold). The NEXT flush must
//!   then survive the WAL reset via the real salt/size rollover detection —
//!   the E1 torn-tail/rollover heal — which publishes a rollover snapshot
//!   instead of a chain-breaking incremental.
//! - `KillRestart` -> drops the in-memory engine state WITHOUT graceful
//!   shutdown, then reopens exactly as the production watch startup does
//!   (`watch_independent` + the D3 eager-startup snapshot): rediscover the
//!   TXID head from the object listing (`discover_legacy_state`), reset
//!   `wal_offset`/salt/chain, recompute the checksum from the DB file, and
//!   take an eager snapshot so the chain has a fresh base. This is the E1
//!   class: restart under load with in-memory cursors lost.
//! - `Mark`      -> records a PITR point `(txid, committed row-set)` at a
//!   flushed incremental boundary.
//! - `Prune { policy }` -> `legacy_manifest::plan_legacy_prune` — the
//!   REAL retention planner, including the F7 chain-base rescue and the E2
//!   bridge-snapshot rescue — then deletes exactly the keys the plan says.
//! - `RestoreLatest` / `RestorePit` -> `legacy_restore::restore_legacy_ltx`:
//!   picks the latest snapshot at-or-before the target, applies incrementals
//!   with checksum-chain + gap verification, runs `PRAGMA integrity_check`,
//!   and publishes atomically.
//!
//! # Model oracle semantics
//!
//! The model tracks `written` (every committed row id in commit order) and
//! `durable_len` (the committed prefix confirmed replicated). `durable_len`
//! advances ONLY on the engine's own confirming `Ok` returns — a Flush that
//! shipped frames (or healed a rollover with a snapshot), or a Snapshot /
//! KillRestart eager snapshot that folded the whole file — never on sleeps or
//! best-effort attempts, and never on a 0-frame `Ok` while changes were
//! pending (see [`Harness::durable_flush`] for the rollover window where that
//! distinction is load-bearing).
//!
//! The model also records `boundary_rows`: for every engine-confirmed durable
//! boundary (the TXID an Ok'd flush/snapshot landed on), the exact committed
//! row-prefix at that boundary. Restores are graded against those boundaries:
//!
//! - `RestoreLatest` must return EXACTLY `written[..durable_len]`: never torn
//!   mid-transaction, never missing durable rows, never containing rows that
//!   were never written. After a successful Flush with no writes after it,
//!   that is exactly `written`. (Exact equality is the point; a subset check
//!   would be vacuous. The engine's own completeness check makes a silent
//!   shortfall an error for latest-restores, and the oracle re-checks rows.)
//! - `RestorePit(mark)` is graded through the model's own REACHABILITY rule,
//!   computed purely from the model's event log (never from the planner —
//!   the planner is the code under test). A target M is reachable iff a
//!   surviving snapshot S* <= M exists and every published boundary in
//!   (S*, M] is an incremental. Incrementals are never deleted; the only
//!   holes pruning can punch are deleted-snapshot TXIDs, and each deletion
//!   is loudly declared by the prune op's plan.
//!   - reachable(M): restore must return `Ok(M)` with EXACTLY the mark's
//!     recorded rows. Any shortfall, gap error, or row diff is a failure —
//!     "prune must preserve retained points" (the E2 fix). With the
//!     product intact a mark on an incremental boundary is ALWAYS reachable,
//!     because the E2 bridge rescue keeps every snapshot bridging a hole
//!     that surviving incrementals depend on.
//!   - unreachable(M) — only possible via declared snapshot deletions (e.g. a
//!     rollover snapshot at the tail whose rows were folded and never shipped
//!     as gen-0 incrementals, pruned by policy): the window legitimately
//!     shrank. Restore may cleanly fall back to `Ok(final < M)` whose rows
//!     must EXACTLY equal the model's recorded row-prefix at boundary
//!     `final` (a fallback can never smuggle torn or fabricated rows), or
//!     fail with the TYPED `WalrustError::RestoreNotFound` when no base
//!     remains at or below M. Any OTHER error — in particular a chain-gap or
//!     checksum error — is a failure even for unreachable points: that is
//!     exactly what a broken rescue produces (the E2 catch surface).
//! - Every restored database must pass `PRAGMA integrity_check` (the engine
//!   runs it internally; the harness re-runs it on the output).
//!
//! Any silent wrong outcome is a property failure. A loud typed `Err` is an
//! acceptable outcome ONLY where the model says the op could legitimately
//! fail: before any base exists, or — in the fault phase — when the fault
//! plan can tear or corrupt objects.
//!
//! # Fault schedules
//!
//! The second phase generates a [`FaultPlan`] on top of the op sequence,
//! reusing the mock's existing deterministic seeded faults: transient
//! `RandomError`, torn `PartialWrite` (truncated prefix persists, then the
//! call errors — a real torn object), and `SilentCorruption` (a real bit flip
//! in stored bytes). The mock's faults fire per-operation from one seeded RNG,
//! so the schedule is (rates, tear threshold, seed) and proptest shrinks the
//! (ops, plan) pair together; different seeds land faults on different ops.
//!
//! Invariant under faults: every restore is either model-correct or a loud
//! typed error — never silently wrong, never a panic. A restore that returns
//! `Ok` must STILL match the model exactly (the checksum chain, not luck, is
//! what makes that hold — silent wrongness is precisely what we hunt).
//! Transient faults must eventually recover: durable ops retry through a
//! bounded budget, and on a transient-only plan every op must still succeed
//! (an exhausted budget is a property failure).
//!
//! # Determinism
//!
//! Everything is seeded through proptest. No wall-clock: pruning
//! timestamps are synthetic (a fixed epoch, all entries in one hourly bucket,
//! so the deletion set depends only on sequence order + policy — and the
//! one-bucket shape is exactly the middle-pruning shape E2 needs). No real
//! sleeps: progress is driven by the engine's own return values. Cases use
//! tiny DBs in a tempdir and in-memory storage, so a case runs in
//! milliseconds and hundreds of cases finish in minutes. Failures persist to
//! `proptest-regressions/state_machine.txt` as a permanent corpus.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use proptest::prelude::*;
use proptest::test_runner::{Config, FileFailurePersistence, TestCaseError, TestRunner};
use rusqlite::Connection;
use tempfile::TempDir;

use walrust::retention::{RetentionPolicy, SnapshotEntry};
use walrust::walrust_core::compaction::{
    level_subpath, parse_range_name, run_level_compaction, CompactionError, RangeLayout,
};
use walrust::walrust_core::legacy_manifest::{
    discover_legacy_snapshots, discover_legacy_state, plan_legacy_prune,
};
use walrust::walrust_core::legacy_restore::restore_legacy_ltx;
use walrust::walrust_core::legacy_wal_sync::{
    checkpoint_wal_truncate, sync_watched_db_once_to_storage, take_snapshot_to_storage, SyncInput,
    WatchedDbState,
};

use crate::mock_storage::{MockStorageBackend, MockStorageConfig, StorageFault};

/// Fixed epoch for deterministic pruning timestamps (no wall-clock).
const FIXED_NOW_UNIX: i64 = 1_700_000_000;
/// Bounded retry budgets for transient injected faults. "Transient faults
/// eventually recover" is enforced as: an exhausted budget IS a property
/// failure. Budgets are sized so exhaustion under the generated max rate
/// (0.05/storage-op) is astronomically unlikely — a false alarm would drown
/// real signal. Durable ops (flush/snapshot) make <= ~4 storage calls per
/// attempt; bulk ops (prune, restore, discovery) make one call per
/// generation/object, up to ~100 per attempt on long sequences, hence the
/// much larger budget (attempts are in-memory and sub-millisecond).
const TRANSIENT_RETRIES: u32 = 40;
const TRANSIENT_RETRIES_BULK: u32 = 3000;

/// Synthetic "now" handed to the compaction engine (no wall-clock in the
/// oracle). Placed astronomically far in the future so every real-timestamp L0
/// object (`created_ms` ~ 2026) is older than any generated `keep_fine_window`
/// and is therefore eligible to merge — the age-gating window is generated and
/// threaded through, but eligibility stays deterministic so a Compact op
/// reliably forms the merged windows the decay grader checks. (The window's own
/// age arithmetic is unit-tested in `compaction::trigger`.)
const COMPACTION_NOW_MS: i64 = 4_000_000_000_000_000;

/// Highest compaction level the oracle observes when reading merged windows back
/// from the layout. Matches the restore planner's probe cap; the oracle only
/// drives L0→L1→L2, so this is generous headroom.
const OBSERVE_MAX_LEVEL: u32 = 16;

// ============================================================================
// Op vocabulary + strategy
// ============================================================================

/// A generated operation. Sequences of these are the unit proptest explores.
#[derive(Debug, Clone)]
pub enum Op {
    /// Commit `rows` new rows through the real SQLite connection.
    WriteTxn { rows: u32 },
    /// Publish pending WAL frames as an incremental LTX (durability point).
    Flush,
    /// Force a full-DB snapshot into a fresh generation.
    Snapshot,
    /// Walrust's controlled checkpoint: flush, then TRUNCATE-fold the WAL.
    /// The next Flush must survive the reset via the real rollover heal.
    Checkpoint,
    /// Ungraceful kill of the replication state + the production reopen path
    /// (listing discovery + D3 eager startup snapshot). The E1 class.
    KillRestart,
    /// Record a PITR point: (current TXID, current committed row-set).
    Mark,
    /// Run retention pruning with a generated GFS policy.
    Prune { hourly: u8, daily: u8, minimum: u8 },
    /// Restore latest to a fresh path and CHECK against the model.
    RestoreLatest,
    /// Restore a recorded mark to a fresh path and CHECK against the model.
    /// `mark_sel` indexes into the recorded marks (mod count).
    RestorePit { mark_sel: u16 },
    /// Run the REAL leveled-compaction merge engine over the legacy `.ltx`
    /// bucket to quiescence with the generated batch sizes, folding the fine L0
    /// incrementals into coarse merged L1/L2 objects (and deleting the folded
    /// sources). Distinct from [`Op::Prune`]: prune deletes whole snapshots by
    /// retention policy; Compact MERGES incrementals and must never lose durable
    /// coverage. After it runs, the model re-observes the merged windows from the
    /// object listing (ground truth) and grades every later `RestorePit` with the
    /// granularity-decay rules. See [`Harness::compact`].
    Compact {
        l1_batch: u8,
        l2_batch: u8,
        keep_fine_secs: u16,
    },
}

/// A deterministic, seeded fault configuration for the fault phase.
#[derive(Debug, Clone)]
pub struct FaultPlan {
    /// Per-op probability of a transient injected error (retryable).
    pub transient_rate: f64,
    /// PUTs larger than this tear: the truncated prefix persists, then Err.
    pub torn_at_bytes: Option<usize>,
    /// Per-PUT probability of a silent stored-bit flip.
    pub corruption_rate: f64,
    /// Seed for the mock's fault RNG.
    pub seed: u64,
}

impl FaultPlan {
    fn none(seed: u64) -> Self {
        Self {
            transient_rate: 0.0,
            torn_at_bytes: None,
            corruption_rate: 0.0,
            seed,
        }
    }

    /// Whether this plan can produce a torn or corrupt object. If it can, a
    /// restore / durable op may legitimately fail LOUDLY; if it cannot, every
    /// engine call must succeed and every restore must be exact.
    fn can_corrupt(&self) -> bool {
        self.torn_at_bytes.is_some() || self.corruption_rate > 0.0
    }

    fn has_transient(&self) -> bool {
        self.transient_rate > 0.0
    }

    fn build_config(&self) -> MockStorageConfig {
        let mut cfg = MockStorageConfig::new("state-machine").with_seed(self.seed);
        if self.transient_rate > 0.0 {
            cfg = cfg.with_fault(StorageFault::RandomError {
                rate: self.transient_rate,
            });
        }
        if let Some(at) = self.torn_at_bytes {
            cfg = cfg.with_fault(StorageFault::PartialWrite { at_bytes: at });
        }
        if self.corruption_rate > 0.0 {
            cfg = cfg.with_fault(StorageFault::SilentCorruption {
                rate: self.corruption_rate,
            });
        }
        cfg
    }
}

/// Strategy for a single op, weighted so restores and restarts appear in most
/// sequences (an op mix that rarely restores checks nothing).
fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (1u32..20).prop_map(|rows| Op::WriteTxn { rows }),
        4 => Just(Op::Flush),
        2 => Just(Op::Snapshot),
        2 => Just(Op::Checkpoint),
        3 => Just(Op::KillRestart),
        3 => Just(Op::Mark),
        2 => (0u8..=3, 0u8..=3, 1u8..=2).prop_map(|(hourly, daily, minimum)| Op::Prune {
            hourly,
            daily,
            minimum
        }),
        3 => Just(Op::RestoreLatest),
        3 => (0u16..1000).prop_map(|mark_sel| Op::RestorePit { mark_sel }),
    ]
}

/// Strategy for a whole scenario: an op sequence (5..40) plus a fault plan.
/// A trailing `RestoreLatest` is appended so every case checks something.
fn scenario_strategy(with_faults: bool) -> impl Strategy<Value = (Vec<Op>, FaultPlan)> {
    let ops = prop::collection::vec(op_strategy(), 5..40);
    let fault = if with_faults {
        (
            0u64..1_000_000,
            0u8..3u8,
            prop::option::of(256usize..8192),
            0u8..3u8,
        )
            // Transient rates are per STORAGE OPERATION. Bulk product calls
            // (restore, pruning discovery) issue up to ~100 storage ops,
            // so rates above ~5% stop modeling transient noise and start
            // modeling an outage no bounded retry can cross.
            .prop_map(|(seed, t, torn, c)| FaultPlan {
                transient_rate: match t {
                    0 => 0.0,
                    1 => 0.02,
                    _ => 0.05,
                },
                torn_at_bytes: torn,
                corruption_rate: match c {
                    0 => 0.0,
                    1 => 0.02,
                    _ => 0.08,
                },
                seed,
            })
            .boxed()
    } else {
        (0u64..1_000_000).prop_map(FaultPlan::none).boxed()
    };
    (ops, fault).prop_map(|(mut ops, fault)| {
        ops.push(Op::RestoreLatest);
        (ops, fault)
    })
}

// ============================================================================
// Model oracle
// ============================================================================

/// A recorded point-in-time restore target.
#[derive(Debug, Clone)]
struct Mark {
    txid: u64,
    rows: Vec<i64>,
}

/// The independent model. It grades the engine; it does not mirror it. All it
/// knows is which rows were committed, which prefix the engine confirmed, at
/// which TXID boundaries it confirmed them, and which snapshot deletions the
/// prune op loudly declared.
#[derive(Debug, Default)]
struct Model {
    /// Every committed row id, in commit order.
    written: Vec<i64>,
    /// Length of the committed prefix confirmed durable by engine returns.
    durable_len: usize,
    /// Whether any base object was ever successfully published.
    has_base: bool,
    /// Recorded PITR points.
    marks: Vec<Mark>,
    /// TXIDs consumed by published snapshots (diagnostics for messages).
    snapshot_txids: BTreeSet<u64>,
    /// Exact committed row-prefix length at every engine-confirmed durable
    /// TXID boundary. Restores may only ever land on these boundaries.
    boundary_rows: BTreeMap<u64, usize>,
    /// TXIDs of snapshots a Prune op deleted — the LOUD declaration that
    /// PITR exactly at those TXIDs may cleanly fall back to an earlier
    /// boundary. (The E2 rescue keeps every bridge the chain needs; these are
    /// only the legitimately prunable, non-load-bearing snapshots.)
    deleted_snapshot_txids: BTreeSet<u64>,
    /// Inclusive seq spans `[min, max]` (with `max > min`) of the merged
    /// compaction objects currently present at levels `>= 1`, re-read from the
    /// object listing (GROUND TRUTH, never the planner) after each `Compact` op.
    /// A merged window fossilizes the per-second points it folded away: PITR to
    /// a txid STRICTLY inside a window (`min <= txid < max`) is granularity
    /// decay, while its `max` boundary stays an exact restore point. Compaction
    /// only deletes the L0 sources it folds, so a txid inside a surviving window
    /// has no finer coverage left. See [`Model::pit_decayed`].
    merged_windows: Vec<(u64, u64)>,
}

impl Model {
    /// Record an engine-confirmed durable boundary at `txid`.
    fn mark_durable(&mut self, txid: u64) {
        self.durable_len = self.written.len();
        self.has_base = true;
        self.boundary_rows.insert(txid, self.durable_len);
    }

    fn durable_rows(&self) -> Vec<i64> {
        self.written[..self.durable_len].to_vec()
    }

    /// Exact expected rows at a confirmed boundary, if it is one.
    fn rows_at_boundary(&self, txid: u64) -> Option<Vec<i64>> {
        self.boundary_rows
            .get(&txid)
            .map(|&len| self.written[..len].to_vec())
    }

    /// Model-side reachability of a point-in-time, computed ONLY from the
    /// model's own event log (published boundaries, which of them were
    /// snapshots, and which snapshots pruning loudly deleted) — it does not
    /// consult the planner. A target M is reachable iff a SURVIVING snapshot
    /// S* <= M exists and every boundary in (S*, M] is an incremental:
    /// incrementals are never deleted, and a snapshot boundary inside that
    /// range would be a deleted one (a surviving one would have been S*),
    /// i.e. a hole the gen-0 chain cannot cross.
    fn pit_reachable(&self, target: u64) -> bool {
        let Some(s_star) = self
            .snapshot_txids
            .iter()
            .filter(|t| !self.deleted_snapshot_txids.contains(t))
            .filter(|&&t| t <= target)
            .max()
            .copied()
        else {
            return false;
        };
        if s_star == target {
            return true; // The surviving snapshot IS the target boundary.
        }
        // Any snapshot boundary strictly inside (S*, target] is a hole.
        !self
            .snapshot_txids
            .range(s_star + 1..=target)
            .any(|t| self.deleted_snapshot_txids.contains(t))
    }

    /// Granularity-decay classification for a point-in-time, computed ONLY from
    /// the merged-window listing (ground truth) recorded after the last
    /// `Compact`. A target `m` is decayed iff it falls STRICTLY inside a merged
    /// window (`min <= m < m_max`): the per-second point at `m` was folded into a
    /// coarse object and its fine source deleted, so the point is no longer
    /// individually restorable — restore must surface the loud typed decay
    /// outcome. A target ON a window's `max` boundary is NOT decayed (the merged
    /// object restores to it exactly), nor is a target in the un-merged fine L0
    /// tail above every window. Windows never span a snapshot seq (the L0 chain
    /// breaks there and the merge refuses to cross it), so a snapshot boundary is
    /// never misread as decayed.
    fn pit_decayed(&self, m: u64) -> bool {
        self.merged_windows
            .iter()
            .any(|&(lo, hi)| lo <= m && m < hi)
    }
}

// ============================================================================
// Harness
// ============================================================================

struct Harness {
    storage: MockStorageBackend,
    prefix: String,
    db_path: PathBuf,
    name: String,
    /// Production watch-loop state (walrust-core `WatchedDbState`).
    state: WatchedDbState,
    /// Long-lived writer connection (keeps the WAL alive across ops, as a real
    /// embedding process would; `wal_autocheckpoint=0` per the caller contract).
    conn: Connection,
    model: Model,
    next_id: i64,
    faults: FaultPlan,
    /// Committed rows not yet shipped (drives Mark's force-a-boundary logic).
    pending_change: bool,
}

/// Classify a mock error as the transient (retryable) injected fault.
fn is_transient(err: &anyhow::Error) -> bool {
    format!("{err:#}").contains("Service unavailable (injected)")
}

fn preview(v: &[i64]) -> String {
    if v.len() <= 12 {
        format!("{v:?}")
    } else {
        format!("[{}..{} ({} rows)]", v[0], v[v.len() - 1], v.len())
    }
}

impl Harness {
    fn new(tmp: &TempDir, faults: FaultPlan) -> Result<Self> {
        let db_path = tmp.path().join("machine.db");
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             PRAGMA page_size=4096;
             CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
        )?;

        let name = "machine".to_string();
        let wal_path = db_path.with_extension("db-wal");
        let storage = MockStorageBackend::new(faults.build_config());
        let state = WatchedDbState {
            db_path: db_path.clone(),
            name: name.clone(),
            wal_path,
            wal_offset: 0,
            wal_generation: 0,
            current_txid: 0,
            db_checksum: None,
            wal_salt: None,
            wal_checksum_chain: None,
        };

        Ok(Self {
            storage,
            prefix: "sm/".to_string(),
            db_path,
            name,
            state,
            conn,
            model: Model::default(),
            next_id: 1,
            faults,
            pending_change: false,
        })
    }

    // ---- durable engine ops (transient faults retried; returns honored) ----

    /// One production sync tick with the bounded transient-retry budget.
    ///
    /// Durability contract (learned from a generated sequence, see below): an
    /// `Ok` advances the model's durable prefix only when the tick actually
    /// confirmed the pending work — `frame_count > 0` (frames shipped),
    /// `checkpoint_detected` (a rollover snapshot folded the whole file), or
    /// nothing was pending. `Ok(0)` with pending changes confirms NOTHING:
    /// the engine's rollover path TRUNCATE-folds the WAL before publishing,
    /// so if that publish fails (e.g. a transient storage error) the next
    /// tick sees an empty WAL and honestly reports 0 frames while the folded
    /// rows are still unpublished. They ship with the next successful
    /// rollover/snapshot (the stored salt mismatch persists), and the model's
    /// durable prefix advances there — treating the interim `Ok(0)` as a
    /// durability point would blame the engine for rows it never confirmed.
    /// The instrument found this via the shrunk sequence
    /// `[Mark, KillRestart..., Mark, RestoreLatest]` under a transient plan.
    async fn durable_flush(&mut self) -> Result<u64> {
        let was_initial = self.state.current_txid == 0;
        let pending_at_entry = self.pending_change;
        let mut attempt = 0;
        let output = loop {
            match sync_watched_db_once_to_storage(&self.storage, &self.prefix, &mut self.state)
                .await
            {
                Ok(o) => break o,
                Err(e) => {
                    if self.faults.has_transient()
                        && is_transient(&e)
                        && attempt < TRANSIENT_RETRIES
                    {
                        attempt += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        };
        if output.checkpoint_detected || was_initial {
            // The engine published a snapshot: the initial base at txid 1, or
            // a rollover snapshot healing a WAL reset.
            self.model.snapshot_txids.insert(output.new_current_txid);
        }
        let confirmed = output.frame_count > 0 || output.checkpoint_detected || !pending_at_entry;
        if confirmed {
            self.model.mark_durable(self.state.current_txid);
            self.pending_change = false;
        }
        Ok(output.frame_count)
    }

    /// Force a full-DB snapshot (passive fold + fresh generation base).
    async fn durable_snapshot(&mut self) -> Result<()> {
        let mut attempt = 0;
        let output = loop {
            let input = SyncInput {
                db_path: self.state.db_path.clone(),
                name: self.state.name.clone(),
                wal_path: self.state.wal_path.clone(),
                wal_offset: self.state.wal_offset,
                wal_generation: self.state.wal_generation,
                current_txid: self.state.current_txid,
                db_checksum: self.state.db_checksum,
                wal_salt: self.state.wal_salt,
                wal_checksum_chain: self.state.wal_checksum_chain,
            };
            match take_snapshot_to_storage(&self.storage, &self.prefix, input).await {
                Ok(o) => break o,
                Err(e) => {
                    if self.faults.has_transient()
                        && is_transient(&e)
                        && attempt < TRANSIENT_RETRIES
                    {
                        attempt += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        };
        // Same state application the production watch loop performs.
        walrust::walrust_core::legacy_wal_sync::apply_sync_output_to_watched_state(
            &mut self.state,
            &output,
        );
        self.model.snapshot_txids.insert(output.new_current_txid);
        // A snapshot folds the file (passive checkpoint with no blockers in
        // this process), so every committed row is in the published base.
        self.model.mark_durable(self.state.current_txid);
        self.pending_change = false;
        Ok(())
    }

    // ---- op execution ------------------------------------------------------

    fn write_txn(&mut self, rows: u32) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE;")?;
        let result = (|| -> Result<()> {
            for _ in 0..rows {
                let id = self.next_id;
                self.conn.execute(
                    "INSERT INTO items (id, value) VALUES (?1, ?2)",
                    rusqlite::params![id, format!("val-{id}")],
                )?;
                self.next_id += 1;
                self.model.written.push(id);
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT;")?;
                if rows > 0 {
                    self.pending_change = true;
                }
                Ok(())
            }
            Err(e) => {
                // Fail loudly; a partial transaction must never linger.
                let _ = self.conn.execute_batch("ROLLBACK;");
                Err(e)
            }
        }
    }

    /// Walrust's controlled checkpoint: ship pending frames, then TRUNCATE-fold
    /// the WAL with the completeness-checked product helper. The WAL reset this
    /// causes must be healed by the rollover detection on the next flush.
    async fn checkpoint(&mut self) -> Result<()> {
        self.durable_flush().await?;
        checkpoint_wal_truncate(&self.db_path).await?;
        Ok(())
    }

    /// Ungraceful restart: throw away the in-memory watch state (no final
    /// sync, no graceful shutdown), then reopen exactly as the production
    /// watch startup does — discovery from the object listing, local cursors
    /// reset, checksum recomputed from the file, then the D3/on-startup eager
    /// snapshot so the chain has a fresh base.
    async fn kill_restart(&mut self) -> Result<()> {
        // Rediscover the TXID head from storage (with transient retry).
        let mut attempt = 0;
        let (current_txid, _max_gen) = loop {
            match discover_legacy_state(&self.storage, &self.prefix, &self.name).await {
                Ok(v) => break v,
                Err(e) => {
                    if self.faults.has_transient()
                        && is_transient(&e)
                        && attempt < TRANSIENT_RETRIES
                    {
                        attempt += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        };

        let db_checksum =
            walrust::walrust_core::legacy_ltx::compute_checksum_from_file(&self.db_path)
                .ok()
                .map(|c| c.into_inner());

        self.state = WatchedDbState {
            db_path: self.db_path.clone(),
            name: self.name.clone(),
            wal_path: self.db_path.with_extension("db-wal"),
            wal_offset: 0,
            wal_generation: 0,
            current_txid,
            db_checksum,
            wal_salt: None,
            wal_checksum_chain: None,
        };

        // D3 / on-startup eager snapshot: after downtime the chain head is
        // untrusted, so production publishes a fresh base before resuming.
        self.durable_snapshot().await
    }

    /// Record a PITR point at a flushed durable boundary. Forces a committed
    /// change first so the boundary is fresh, then flushes. (The flush may
    /// legitimately publish a rollover snapshot instead of an incremental —
    /// either way the engine confirmed the boundary, so the target is exact.)
    async fn mark(&mut self) -> Result<()> {
        if !self.pending_change {
            self.write_txn(1)?;
        }
        self.durable_flush().await?;
        self.model.marks.push(Mark {
            txid: self.state.current_txid,
            rows: self.model.durable_rows(),
        });
        Ok(())
    }

    /// Run the REAL prune planner (policy + F7 base rescue + E2 bridge
    /// rescue) and delete exactly the keys it plans. Timestamps are synthetic
    /// and deterministic: all snapshots land in one hourly bucket so the
    /// policy prunes middles — the exact shape the bridge rescue protects.
    async fn prune(&mut self, hourly: u8, daily: u8, minimum: u8) -> Result<()> {
        // The whole op is retried on transient faults: discovery, planning and
        // deletion are all idempotent (the plan is recomputed over survivors).
        let mut attempt = 0;
        loop {
            match self.prune_once(hourly, daily, minimum).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if self.faults.has_transient()
                        && is_transient(&e)
                        && attempt < TRANSIENT_RETRIES_BULK
                    {
                        attempt += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn prune_once(&mut self, hourly: u8, daily: u8, minimum: u8) -> Result<()> {
        use hadb_storage::StorageBackend;

        let discovered = discover_legacy_snapshots(&self.storage, &self.prefix, &self.name).await?;
        if discovered.is_empty() {
            return Ok(());
        }

        let now = DateTime::<Utc>::from_timestamp(FIXED_NOW_UNIX, 0).expect("fixed epoch is valid");
        let base_time = now - ChronoDuration::minutes(30);
        let mut entries: Vec<SnapshotEntry> = Vec::with_capacity(discovered.len());
        for (i, snap) in discovered.iter().enumerate() {
            let size = self
                .storage
                .get(&snap.key)
                .await
                .ok()
                .flatten()
                .map(|b| b.len() as u64)
                .unwrap_or(0);
            entries.push(SnapshotEntry {
                key: snap.key.clone(),
                created_at: base_time + ChronoDuration::seconds(i as i64),
                sequence: snap.max_txid,
                size,
            });
        }

        let policy = RetentionPolicy {
            hourly: hourly as usize,
            daily: daily as usize,
            weekly: 0,
            monthly: 0,
            minimum: minimum as usize,
        };

        let plan = plan_legacy_prune(
            &self.storage,
            &self.prefix,
            &self.name,
            &entries,
            &policy,
            now,
        )
        .await?;

        for entry in &plan.delete {
            self.storage.delete(&entry.key).await?;
            // The loud declaration: PITR exactly at this snapshot's TXID may
            // now cleanly fall back to the previous boundary.
            self.model.deleted_snapshot_txids.insert(entry.sequence);
        }
        Ok(())
    }

    // ---- restores + oracle checks -------------------------------------------

    async fn restore_to(&self, out: &Path, pit: Option<u64>) -> Result<u64> {
        let mut attempt = 0;
        loop {
            match restore_legacy_ltx(&self.storage, &self.prefix, &self.name, out, pit).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if self.faults.has_transient()
                        && is_transient(&e)
                        && attempt < TRANSIENT_RETRIES_BULK
                    {
                        attempt += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    fn restored_ids(path: &Path) -> Result<Vec<i64>> {
        let conn = Connection::open(path)?;
        let integrity: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        anyhow::ensure!(integrity == "ok", "integrity_check failed: {integrity}");
        let mut stmt = conn.prepare("SELECT id FROM items ORDER BY id")?;
        let ids = stmt
            .query_map([], |row| row.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(ids)
    }

    async fn check_restore_latest(&self, out: &Path, op_index: usize) -> Result<()> {
        let expected = self.model.durable_rows();
        match self.restore_to(out, None).await {
            Ok(_) => {
                let ids = Self::restored_ids(out).map_err(|e| {
                    anyhow::anyhow!(
                        "op[{op_index}] RestoreLatest: engine returned Ok but the restored DB is \
                         unreadable/corrupt: {e}"
                    )
                })?;
                anyhow::ensure!(
                    ids == expected,
                    "op[{op_index}] RestoreLatest returned WRONG rows{}: model durable = {} \
                     (len {}), restored = {} (len {}). Snapshot txids: {:?}",
                    if self.faults.can_corrupt() {
                        " under faults (silent corruption)"
                    } else {
                        ""
                    },
                    preview(&expected),
                    expected.len(),
                    preview(&ids),
                    ids.len(),
                    self.model.snapshot_txids,
                );
                Ok(())
            }
            Err(e) => {
                if !self.model.has_base {
                    return Ok(()); // Nothing durable was ever published.
                }
                if self.faults.can_corrupt() {
                    return Ok(()); // A torn/corrupt object may fail loudly.
                }
                Err(anyhow::anyhow!(
                    "op[{op_index}] RestoreLatest FAILED with no corrupting faults while the \
                     model holds {} durable rows: {e:#}",
                    expected.len()
                ))
            }
        }
    }

    async fn check_restore_pit(&self, out: &Path, mark: &Mark, op_index: usize) -> Result<()> {
        let reachable = self.model.pit_reachable(mark.txid);
        match self.restore_to(out, Some(mark.txid)).await {
            Ok(final_txid) => {
                anyhow::ensure!(
                    final_txid <= mark.txid,
                    "op[{op_index}] RestorePit(txid {}) overshot to txid {final_txid}",
                    mark.txid
                );
                // A reachable mark must be restored exactly; only a mark whose
                // point pruning LOUDLY foreclosed (deleted-snapshot holes)
                // may cleanly fall back to an earlier boundary.
                if final_txid < mark.txid && reachable {
                    anyhow::bail!(
                        "op[{op_index}] RestorePit(txid {}) silently stopped at txid \
                         {final_txid} although the point is reachable (surviving snapshots: \
                         {:?}, deleted: {:?}) — a retained point must restore exactly (E2)",
                        mark.txid,
                        self.model.snapshot_txids,
                        self.model.deleted_snapshot_txids,
                    );
                }
                let ids = Self::restored_ids(out).map_err(|e| {
                    anyhow::anyhow!(
                        "op[{op_index}] RestorePit(txid {}): engine returned Ok but the restored \
                         DB is unreadable/corrupt: {e}",
                        mark.txid
                    )
                })?;
                // Rows must be EXACT for the boundary the engine claims it
                // reached — a fallback can never smuggle torn/fabricated rows.
                let expected = if final_txid == mark.txid {
                    Some(mark.rows.clone())
                } else {
                    self.model.rows_at_boundary(final_txid)
                };
                let Some(expected) = expected else {
                    anyhow::bail!(
                        "op[{op_index}] RestorePit(txid {}) landed on txid {final_txid}, which \
                         is NOT an engine-confirmed durable boundary (boundaries: {:?})",
                        mark.txid,
                        self.model.boundary_rows.keys().collect::<Vec<_>>(),
                    );
                };
                anyhow::ensure!(
                    ids == expected,
                    "op[{op_index}] RestorePit(txid {}) returned WRONG rows{} at boundary \
                     {final_txid}: expected {} (len {}), restored = {} (len {}). Snapshot \
                     txids: {:?}",
                    mark.txid,
                    if self.faults.can_corrupt() {
                        " under faults (silent corruption)"
                    } else {
                        ""
                    },
                    preview(&expected),
                    expected.len(),
                    preview(&ids),
                    ids.len(),
                    self.model.snapshot_txids,
                );
                Ok(())
            }
            Err(e) => {
                if self.faults.can_corrupt() {
                    return Ok(()); // Torn/corrupt objects may fail loudly.
                }
                // The ONLY acceptable loud outcome is the typed RestoreNotFound
                // for a point pruning legitimately foreclosed (no surviving
                // base at or below it — declared by the deletions). A chain-gap
                // or checksum error is NEVER acceptable: pruning deletes
                // only snapshots, always keeps the latest, and the F7 + E2
                // rescues keep the chain base and every load-bearing bridge
                // snapshot — a reachable mark that errors is the E2 class.
                let typed_not_found = matches!(
                    e.downcast_ref::<walrust::walrust_core::errors::WalrustError>(),
                    Some(walrust::walrust_core::errors::WalrustError::RestoreNotFound(_))
                );
                if !reachable && typed_not_found {
                    return Ok(());
                }
                Err(anyhow::anyhow!(
                    "op[{op_index}] RestorePit(txid {}) FAILED ({}; reachable={reachable}; \
                     surviving snapshots {:?}, deleted {:?}): pruning must keep every \
                     reachable marked point restorable — E2: {e:#}",
                    mark.txid,
                    if typed_not_found {
                        "typed RestoreNotFound"
                    } else {
                        "NON-typed error, e.g. a chain gap"
                    },
                    self.model.snapshot_txids,
                    self.model.deleted_snapshot_txids,
                ))
            }
        }
    }

    // ---- compaction (the REAL merge engine over the legacy bucket) ----------

    /// Run leveled compaction to quiescence over the SAME mock bucket the legacy
    /// engine writes, then re-observe the merged windows into the model.
    ///
    /// The layout adapter is [`RangeLayout`] — the compaction adapter for the
    /// litestream-heritage `.ltx` heritage this harness drives (its L0 pool is
    /// `{db}/0000/{min}-{max}.ltx`, exactly what `RangeLayout::list_level(0)`
    /// reads). `RangeLayout` and the owned `SeqLayout` are thin wrappers over one
    /// `GenLayoutCore` with identical seq-contiguous merge semantics — they
    /// differ only in the level-0 filename extension — so this exercises the same
    /// engine, batching, and `levels/L*/` key scheme the owned layout uses. It is
    /// also the exact path the real `walrust watch` compaction tick runs
    /// (`sync::watch_independent::compaction_tick`), and the restore it must
    /// survive (`restore_legacy_ltx`) is the same one the oracle already grades.
    ///
    /// One `Compact` op drains ALL eligible merges (L0→L1 then L1→L2, repeated
    /// until a pass merges nothing) so a merged layout forms within a bounded op
    /// sequence. The whole pass is retried as a unit on transient injected faults
    /// (merge is crash-idempotent: a re-run converges via the engine's
    /// exact-range recovery), and a corrupting-fault plan may make it fail
    /// loudly — in which case the write-verify-delete ordering leaves the sources
    /// intact, and the always-appended `RestoreLatest` proves coverage survived.
    async fn compact(&mut self, l1_batch: u8, l2_batch: u8, keep_fine_secs: u16) -> Result<()> {
        let layout = RangeLayout::new(Arc::new(self.storage.clone()), &self.prefix, &self.name);
        let keep = Duration::from_secs(keep_fine_secs as u64);

        let mut attempt = 0;
        loop {
            match self
                .compact_pass(&layout, l1_batch as usize, l2_batch as usize, keep)
                .await
            {
                Ok(()) => break,
                Err(e) => {
                    let transient = is_compaction_transient(&e);
                    if self.faults.has_transient() && transient && attempt < TRANSIENT_RETRIES_BULK
                    {
                        attempt += 1;
                        continue;
                    }
                    return Err(anyhow::Error::new(e));
                }
            }
        }

        // GROUND TRUTH: re-read the merged windows from the object listing.
        self.observe_merged_windows(&layout).await?;
        Ok(())
    }

    /// One compaction pass: repeatedly fire L0→L1 and L1→L2 until neither merges
    /// anything. A `NonContiguous` residue after the engine's own seq-contiguous
    /// batch clipping would be a genuine fork/corruption and propagates loudly.
    async fn compact_pass(
        &self,
        layout: &RangeLayout,
        l1_batch: usize,
        l2_batch: usize,
        keep: Duration,
    ) -> Result<(), CompactionError> {
        // Bound: each merge strictly reduces object count at its source level, so
        // the fixpoint is reached in far fewer than this many passes; the cap
        // only guards against a hypothetical non-converging engine bug.
        for _ in 0..256 {
            let mut progressed = false;
            if l1_batch >= 1 {
                let o = run_level_compaction(layout, 0, l1_batch, keep, COMPACTION_NOW_MS).await?;
                progressed |= o.merged_count() > 0;
            }
            if l2_batch >= 1 {
                let o = run_level_compaction(layout, 1, l2_batch, keep, COMPACTION_NOW_MS).await?;
                progressed |= o.merged_count() > 0;
            }
            if !progressed {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Re-read the inclusive seq spans of the merged objects at levels `>= 1`
    /// straight from the object listing — one LIST per level, filenames parsed,
    /// NO planner and NO header reads. Only true windows (`max > min`) are
    /// recorded; a point-merge (`max == min`) is a clean boundary, not a decay
    /// window. Retried as a unit on transient faults.
    async fn observe_merged_windows(&mut self, layout: &RangeLayout) -> Result<()> {
        let _ = layout; // key scheme is shared; we list the raw bucket directly.
        let mut attempt = 0;
        let windows = loop {
            match self.list_merged_windows().await {
                Ok(w) => break w,
                Err(e) => {
                    if self.faults.has_transient()
                        && is_transient(&e)
                        && attempt < TRANSIENT_RETRIES_BULK
                    {
                        attempt += 1;
                        continue;
                    }
                    return Err(e);
                }
            }
        };
        self.model.merged_windows = windows;
        Ok(())
    }

    async fn list_merged_windows(&self) -> Result<Vec<(u64, u64)>> {
        use hadb_storage::StorageBackend;
        let mut windows = Vec::new();
        for level in 1..=OBSERVE_MAX_LEVEL {
            let dir = format!("{}{}/{}/", self.prefix, self.name, level_subpath(level));
            for key in self.storage.list(&dir, None).await? {
                let Some(filename) = key.rsplit('/').next() else {
                    continue;
                };
                if let Some(range) = parse_range_name(filename, "ltx") {
                    if range.max > range.min {
                        windows.push((range.min, range.max));
                    }
                }
            }
        }
        Ok(windows)
    }

    /// Grade a `RestorePit` after compaction, with the granularity-decay rules.
    ///
    /// - An EXACT point (a merged-window `max` boundary, a snapshot, or a
    ///   surviving fine tail point) must restore to EXACTLY the mark's rows.
    /// - A DECAYED point (strictly inside a surviving merged window, fine source
    ///   folded away) must be a LOUD outcome: the typed `RestoreNotFound`
    ///   (snapshot-span decay) or the surfaced "falls inside merged window" text
    ///   (`PlanError::PitrInsideMergedWindow`). Never a bare chain gap, never a
    ///   silent wrong-point `Ok`.
    /// - Under a corrupting fault plan a torn/corrupt object may fail loudly, and
    ///   an `Ok` restore must STILL be row-exact (silent wrongness is the hunt).
    ///   A partially-completed compaction under faults can leave the fine sources
    ///   intact under a merged window; a decayed-classified point that then
    ///   restores exactly is accepted precisely because its rows match.
    async fn check_restore_pit_compaction(
        &self,
        out: &Path,
        mark: &Mark,
        op_index: usize,
    ) -> Result<()> {
        let decayed = self.model.pit_decayed(mark.txid);
        match self.restore_to(out, Some(mark.txid)).await {
            Ok(final_txid) => {
                let ids = match Self::restored_ids(out) {
                    Ok(ids) => ids,
                    Err(e) => {
                        if self.faults.can_corrupt() {
                            return Ok(()); // A torn/corrupt object may fail loudly.
                        }
                        return Err(anyhow::anyhow!(
                            "op[{op_index}] RestorePit(txid {}): engine returned Ok but the \
                             restored DB is unreadable/corrupt: {e}",
                            mark.txid
                        ));
                    }
                };
                grade_pit_compaction_ok(
                    decayed,
                    self.faults.can_corrupt(),
                    final_txid,
                    mark.txid,
                    &ids,
                    &mark.rows,
                    &self.model.merged_windows,
                    op_index,
                )
            }
            Err(e) => {
                if self.faults.can_corrupt() {
                    return Ok(()); // Torn/corrupt objects may fail loudly.
                }
                if decayed {
                    // The only acceptable loud outcomes for decay: the typed
                    // snapshot-span RestoreNotFound, or the merged-window planner
                    // error surfaced verbatim. A BARE chain gap is forbidden —
                    // that is exactly the "compaction lost coverage" failure.
                    let typed_not_found = matches!(
                        e.downcast_ref::<walrust::walrust_core::errors::WalrustError>(),
                        Some(walrust::walrust_core::errors::WalrustError::RestoreNotFound(_))
                    );
                    let msg = format!("{e:#}");
                    if typed_not_found || msg.contains("falls inside merged window") {
                        return Ok(());
                    }
                    return Err(anyhow::anyhow!(
                        "op[{op_index}] RestorePit(txid {}) is granularity decay (inside merged \
                         window(s) {:?}) but FAILED with a non-decay error — a bare chain gap is \
                         forbidden, compaction must not lose coverage: {e:#}",
                        mark.txid,
                        self.model.merged_windows,
                    ));
                }
                Err(anyhow::anyhow!(
                    "op[{op_index}] RestorePit(txid {}) is an EXACT point (merged windows {:?}) but \
                     FAILED with no corrupting faults — compaction must keep every non-decayed \
                     marked point restorable: {e:#}",
                    mark.txid,
                    self.model.merged_windows,
                ))
            }
        }
    }
}

/// Classify a compaction error as the transient (retryable) injected fault: its
/// `Storage(..)` variant carries the mock's injected-error string.
fn is_compaction_transient(err: &CompactionError) -> bool {
    matches!(err, CompactionError::Storage(s) if s.contains("Service unavailable (injected)"))
}

/// Pure accept-direction verdict for a compaction PITR that returned `Ok`.
///
/// Extracted from [`Harness::check_restore_pit_compaction`] so the accept-side
/// guards are DIRECTLY testable. This matters: with a correct engine every
/// inside-window PITR errors loudly, so the real engine only ever drives the
/// `Err` arm of the grader — the "silent decayed `Ok` is a bug" and "overshoot"
/// guards are never exercised by the generated/pinned replays (proven by
/// neutering them and watching every case still pass). Routing the verdict
/// through this pure function lets `grade_pit_compaction_ok_*` unit tests
/// exercise a *simulated* buggy engine directly, giving the accept direction
/// real teeth (fail-on-revert) instead of dead defense-in-depth.
///
/// The three ways an `Ok` restore is wrong:
///   1. it OVERSHOT the target (`final_txid > mark_txid`) — always a bug;
///   2. it silently restored a DECAYED point (strictly inside a merged window,
///      fine source folded away) with no corrupting fault to excuse it — decay
///      must be a loud typed error, never a silent `Ok`;
///   3. it returned the WRONG rows for the mark (checked for every classification
///      — a decayed point that survives a partial merge under faults is exact and
///      therefore fine; anything else is silent corruption).
#[allow(clippy::too_many_arguments)]
fn grade_pit_compaction_ok(
    decayed: bool,
    can_corrupt: bool,
    final_txid: u64,
    mark_txid: u64,
    restored: &[i64],
    expected: &[i64],
    merged_windows: &[(u64, u64)],
    op_index: usize,
) -> Result<()> {
    anyhow::ensure!(
        final_txid <= mark_txid,
        "op[{op_index}] RestorePit(txid {mark_txid}) overshot to txid {final_txid}"
    );
    // A decayed point must never be a SILENT success. With no corrupting faults
    // the fine source is gone, so an Ok here means the planner failed to detect
    // the decay — a real bug.
    if decayed && !can_corrupt {
        anyhow::bail!(
            "op[{op_index}] RestorePit(txid {mark_txid}) returned Ok(final {final_txid}) for a \
             point STRICTLY INSIDE merged window(s) {merged_windows:?} — granularity decay must be \
             a loud typed error, never a silent restore. Restored {} rows.",
            restored.len(),
        );
    }
    // Whatever the classification, an Ok restore must be row-exact for the mark.
    anyhow::ensure!(
        restored == expected,
        "op[{op_index}] RestorePit(txid {mark_txid}) returned WRONG rows{} at final txid \
         {final_txid}: expected {} (len {}), restored = {} (len {}). Merged windows: {merged_windows:?}",
        if can_corrupt {
            " under faults (silent corruption)"
        } else {
            ""
        },
        preview(expected),
        expected.len(),
        preview(restored),
        restored.len(),
    );
    Ok(())
}

// ============================================================================
// Case runner
// ============================================================================

/// A durable op returned Err. With no faults every engine call must succeed.
/// On a transient-only plan the retry budget must have absorbed the faults, so
/// Err is also a failure (transient faults must eventually recover). Only a
/// plan that can tear or corrupt objects may fail loudly — the model simply
/// does not advance its durable prefix then.
fn guard_durable<T>(faults: &FaultPlan, r: Result<T>, op_index: usize, what: &str) -> Result<()> {
    match r {
        Ok(_) => Ok(()),
        Err(e) => {
            if faults.can_corrupt() {
                Ok(())
            } else if faults.has_transient() {
                Err(anyhow::anyhow!(
                    "op[{op_index}] {what} failed on a transient-only fault plan; the retry \
                     budget ({TRANSIENT_RETRIES}) must absorb transient faults: {e:#}"
                ))
            } else {
                Err(anyhow::anyhow!(
                    "op[{op_index}] {what} failed with no faults configured: {e:#}"
                ))
            }
        }
    }
}

/// Execute one generated scenario end to end against the real engine.
///
/// `compaction` selects the point-in-time grader: `false` uses the prune-
/// foreclosure oracle ([`Harness::check_restore_pit`], phases 1/2, no `Compact`
/// ops); `true` uses the granularity-decay oracle
/// ([`Harness::check_restore_pit_compaction`], the compaction phase). Both
/// phases share the exact same `RestoreLatest` grader — compaction must never
/// change latest-restore correctness.
async fn run_case(ops: &[Op], faults: FaultPlan, compaction: bool) -> Result<()> {
    let tmp = TempDir::new()?;
    let restores = TempDir::new()?;
    let mut h = Harness::new(&tmp, faults.clone())?;

    // Publish the initial base (the production initial sync at txid 0). Under
    // corrupting faults this may loudly fail; the model then has no base.
    let init = h.durable_flush().await;
    guard_durable(&faults, init, 0, "InitialBase")?;

    for (i, op) in ops.iter().enumerate() {
        match op {
            Op::WriteTxn { rows } => {
                h.write_txn(*rows)
                    .map_err(|e| anyhow::anyhow!("op[{i}] WriteTxn failed: {e:#}"))?;
            }
            Op::Flush => {
                let r = h.durable_flush().await;
                guard_durable(&faults, r, i, "Flush")?;
            }
            Op::Snapshot => {
                let r = h.durable_snapshot().await;
                guard_durable(&faults, r, i, "Snapshot")?;
            }
            Op::Checkpoint => {
                let r = h.checkpoint().await;
                guard_durable(&faults, r, i, "Checkpoint")?;
            }
            Op::KillRestart => {
                let r = h.kill_restart().await;
                guard_durable(&faults, r, i, "KillRestart")?;
            }
            Op::Mark => {
                let r = h.mark().await;
                guard_durable(&faults, r, i, "Mark")?;
            }
            Op::Prune {
                hourly,
                daily,
                minimum,
            } => {
                let r = h.prune(*hourly, *daily, *minimum).await;
                guard_durable(&faults, r, i, "Prune")?;
            }
            Op::RestoreLatest => {
                let out = restores.path().join(format!("latest_{i}.db"));
                h.check_restore_latest(&out, i).await?;
            }
            Op::RestorePit { mark_sel } => {
                if h.model.marks.is_empty() {
                    continue;
                }
                let idx = (*mark_sel as usize) % h.model.marks.len();
                let mark = h.model.marks[idx].clone();
                let out = restores.path().join(format!("pit_{i}.db"));
                if compaction {
                    h.check_restore_pit_compaction(&out, &mark, i).await?;
                } else {
                    h.check_restore_pit(&out, &mark, i).await?;
                }
            }
            Op::Compact {
                l1_batch,
                l2_batch,
                keep_fine_secs,
            } => {
                let r = h.compact(*l1_batch, *l2_batch, *keep_fine_secs).await;
                guard_durable(&faults, r, i, "Compact")?;
            }
        }
    }
    Ok(())
}

// ============================================================================
// Proptest entry points
// ============================================================================

fn config(default_cases: u32) -> Config {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default_cases);
    Config {
        cases,
        // Failures persist as a permanent regression corpus.
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/state_machine.txt",
        ))),
        ..Config::default()
    }
}

fn run_phase(default_cases: u32, with_faults: bool) -> Result<()> {
    let mut runner = TestRunner::new(config(default_cases));
    let rt = tokio::runtime::Runtime::new()?;
    let result = runner.run(&scenario_strategy(with_faults), |(ops, faults)| {
        rt.block_on(run_case(&ops, faults, false))
            .map_err(|e| TestCaseError::fail(format!("{e:#}")))
    });
    result.map_err(|e| {
        anyhow::anyhow!(
            "state machine ({}) failed:\n{e}",
            if with_faults {
                "with faults"
            } else {
                "no faults"
            }
        )
    })
}

/// Phase 1: generated op sequences, no faults. Exact-equality oracle.
pub fn run_state_machine_no_faults(default_cases: u32) -> Result<()> {
    run_phase(default_cases, false)
}

/// Phase 2: op sequences PLUS a deterministic seeded fault plan. Every restore
/// is model-correct or a loud typed error; never silently wrong, never a
/// panic; transient-only plans must fully recover.
pub fn run_state_machine_with_faults(default_cases: u32) -> Result<()> {
    run_phase(default_cases, true)
}

// ============================================================================
// Compaction phase (the REAL merge engine + granularity-decay oracle)
// ============================================================================

/// Op strategy for the compaction phase: the legacy vocabulary PLUS `Compact`,
/// and deliberately NO `Prune`. Compaction and prune both foreclose points, but
/// through independent mechanisms; grading them together would tangle two decay
/// models, so the compaction phase isolates the merge-decay rule (prune
/// foreclosure stays proven by phases 1/2). Batches are small (2..=6) so the
/// short generated sequences actually fill them and form merged windows; a mark
/// then routinely lands strictly inside one. `keep_fine_secs` is generated and
/// threaded through, but with the far-future `COMPACTION_NOW_MS` every L0 object
/// is eligible regardless, so merges reliably fire (see `COMPACTION_NOW_MS`).
fn op_strategy_compaction() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (1u32..20).prop_map(|rows| Op::WriteTxn { rows }),
        5 => Just(Op::Flush),
        2 => Just(Op::Snapshot),
        2 => Just(Op::Checkpoint),
        2 => Just(Op::KillRestart),
        4 => Just(Op::Mark),
        4 => (2u8..=6, 2u8..=4, 0u16..7200).prop_map(|(l1_batch, l2_batch, keep_fine_secs)| {
            Op::Compact {
                l1_batch,
                l2_batch,
                keep_fine_secs,
            }
        }),
        2 => Just(Op::RestoreLatest),
        4 => (0u16..1000).prop_map(|mark_sel| Op::RestorePit { mark_sel }),
    ]
}

/// A compaction scenario: an op sequence (with `Compact`, no `Prune`) plus a
/// fault plan, and a trailing `RestoreLatest` so every case proves compaction
/// preserved the latest state exactly.
fn scenario_strategy_compaction(with_faults: bool) -> impl Strategy<Value = (Vec<Op>, FaultPlan)> {
    let ops = prop::collection::vec(op_strategy_compaction(), 6..40);
    let fault = if with_faults {
        (
            0u64..1_000_000,
            0u8..3u8,
            prop::option::of(256usize..8192),
            0u8..3u8,
        )
            .prop_map(|(seed, t, torn, c)| FaultPlan {
                transient_rate: match t {
                    0 => 0.0,
                    1 => 0.02,
                    _ => 0.05,
                },
                torn_at_bytes: torn,
                corruption_rate: match c {
                    0 => 0.0,
                    1 => 0.02,
                    _ => 0.08,
                },
                seed,
            })
            .boxed()
    } else {
        (0u64..1_000_000).prop_map(FaultPlan::none).boxed()
    };
    (ops, fault).prop_map(|(mut ops, fault)| {
        ops.push(Op::RestoreLatest);
        (ops, fault)
    })
}

fn run_compaction_phase(default_cases: u32, with_faults: bool) -> Result<()> {
    let mut runner = TestRunner::new(config(default_cases));
    let rt = tokio::runtime::Runtime::new()?;
    let result = runner.run(
        &scenario_strategy_compaction(with_faults),
        |(ops, faults)| {
            rt.block_on(run_case(&ops, faults, true))
                .map_err(|e| TestCaseError::fail(format!("{e:#}")))
        },
    );
    result.map_err(|e| {
        anyhow::anyhow!(
            "compaction state machine ({}) failed:\n{e}",
            if with_faults {
                "with faults"
            } else {
                "no faults"
            }
        )
    })
}

/// Phase 3: the REAL leveled-compaction engine folds the legacy bucket, and
/// every restore is graded with the granularity-decay rules — latest exact,
/// merged-window boundary exact, strictly-inside a loud typed decay, never a
/// bare gap, never a silent wrong point.
pub fn run_compaction_state_machine(default_cases: u32) -> Result<()> {
    run_compaction_phase(default_cases, false)
}

/// Phase 3 under faults: `Compact` runs inside the torn/transient/corruption
/// fault plans. A failed merge leaves sources intact (write-verify-delete), the
/// trailing `RestoreLatest` proves coverage survived, and any `Ok` restore is
/// still row-exact.
pub fn run_compaction_state_machine_with_faults(default_cases: u32) -> Result<()> {
    run_compaction_phase(default_cases, true)
}

// ============================================================================
// Test wiring — `cargo test -p walrust-dst state_machine`
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Default modest case count so the CI tests job stays fast; the nightly
    /// drill workflow runs a deeper sweep via PROPTEST_CASES.
    const DEFAULT_CASES: u32 = 32;

    #[test]
    fn state_machine_generated_sequences() {
        run_state_machine_no_faults(DEFAULT_CASES).unwrap();
    }

    /// The first shrunk sequence this instrument surfaced while its oracle was
    /// being calibrated (a legitimate foreclosure: the Mark's flush published
    /// a rollover snapshot, pruning pruned it as non-load-bearing, and PITR
    /// correctly failed with the typed RestoreNotFound). Kept as a pinned
    /// regression replay for the oracle's foreclosure semantics.
    #[test]
    fn replay_foreclosed_rollover_mark_sequence() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ops = vec![
            Op::KillRestart,
            Op::Mark,
            Op::KillRestart,
            Op::Prune {
                hourly: 0,
                daily: 0,
                minimum: 1,
            },
            Op::RestorePit { mark_sel: 0 },
            Op::RestoreLatest,
        ];
        rt.block_on(run_case(&ops, FaultPlan::none(0), false))
            .unwrap();
    }

    /// The shrunk E2 catch-proof sequence. With the bridge-snapshot rescue in
    /// `plan_legacy_prune` disabled, this exact sequence makes pruning
    /// delete the bridge snapshots the retained chain depends on and PITR
    /// fails with "restore incremental gap" — the instrument found and shrank
    /// it within a 256-case run. With the rescue intact it passes; if the
    /// rescue ever regresses, this replay fails immediately (and the same
    /// seed is pinned in proptest-regressions/state_machine.txt).
    #[test]
    fn replay_e2_bridge_rescue_catch_sequence() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ops = vec![
            Op::Flush,
            Op::KillRestart,
            Op::Flush,
            Op::Mark,
            Op::KillRestart,
            Op::Prune {
                hourly: 0,
                daily: 0,
                minimum: 1,
            },
            Op::RestorePit { mark_sel: 0 },
            Op::RestoreLatest,
        ];
        rt.block_on(run_case(&ops, FaultPlan::none(0), false))
            .unwrap();
    }

    #[test]
    fn state_machine_generated_sequences_under_faults() {
        run_state_machine_with_faults(DEFAULT_CASES).unwrap();
    }

    /// E4 coverage guard. The generated sequences can only request a PITR at a
    /// recorded `Mark`, and a Mark always names a real published boundary at or
    /// below the discovered head — so the fuzz never asks for a point-in-time
    /// BEYOND the newest available TXID, and the E4 future-PIT guard in
    /// `restore_legacy_ltx` would go unexercised by this instrument. Pin it: a
    /// far-future point-in-time must be the TYPED `RestoreNotFound`, never a
    /// silent fall-through to the latest DB. Removing the E4 branch makes this
    /// fail (the restore then returns Ok at the head).
    #[test]
    fn replay_e4_future_pit_is_typed_not_found() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let tmp = TempDir::new().unwrap();
            let restores = TempDir::new().unwrap();
            let mut h = Harness::new(&tmp, FaultPlan::none(0)).unwrap();
            // Publish an initial base, then a durable incremental boundary.
            h.durable_flush().await.unwrap();
            h.write_txn(3).unwrap();
            h.durable_flush().await.unwrap();
            let head = h.state.current_txid;
            assert!(head >= 1, "expected a published head, got {head}");

            let out = restores.path().join("future_pit.db");
            let err = h
                .restore_to(&out, Some(head + 1000))
                .await
                .expect_err("a far-future PITR must be a hard error, not a silent latest restore");
            let typed = matches!(
                err.downcast_ref::<walrust::walrust_core::errors::WalrustError>(),
                Some(walrust::walrust_core::errors::WalrustError::RestoreNotFound(_))
            );
            assert!(
                typed,
                "far-future PITR must be a typed RestoreNotFound, got: {err:#}"
            );
        });
    }

    // ---- compaction phase (Op::Compact + granularity-decay oracle) ----------

    /// Fail-on-revert teeth for the accept-direction of the decay grader.
    ///
    /// A correct engine errors loudly on every inside-window PITR, so the
    /// generated + pinned replays only ever drive the grader's `Err` arm — remove
    /// the accept-side guards and every one of them still passes (verified during
    /// C3b adversarial review). These tests exercise the accept verdict
    /// (`grade_pit_compaction_ok`) against a SIMULATED buggy engine, so the
    /// "silent decayed Ok is a bug", "overshoot", and "wrong rows" guards have
    /// real teeth. Reverting any guard in `grade_pit_compaction_ok` fails here.
    #[test]
    fn grade_pit_compaction_ok_rejects_silent_decay() {
        let windows = [(3u64, 9u64)];
        // Simulated bug: the engine silently restored an inside-window point
        // (txid 5, strictly inside [3,9]) as an Ok, no corrupting fault. Even
        // with "correct-looking" rows this MUST be rejected — decay is loud.
        let err = grade_pit_compaction_ok(
            /*decayed*/ true,
            /*can_corrupt*/ false,
            /*final_txid*/ 5,
            /*mark_txid*/ 5,
            /*restored*/ &[1, 2, 3, 4, 5],
            /*expected*/ &[1, 2, 3, 4, 5],
            &windows,
            7,
        )
        .expect_err("a silent Ok for a strictly-inside decayed point must be rejected");
        assert!(
            format!("{err:#}").contains("STRICTLY INSIDE"),
            "got: {err:#}"
        );
    }

    #[test]
    fn grade_pit_compaction_ok_rejects_overshoot_and_wrong_rows() {
        // Overshoot: final past the target is always a bug, decayed or not.
        assert!(grade_pit_compaction_ok(false, false, 10, 5, &[1], &[1], &[], 0).is_err());
        // Wrong rows for an exact (non-decayed) point with no faults.
        let e = grade_pit_compaction_ok(false, false, 5, 5, &[1, 2], &[1, 2, 3], &[], 0)
            .expect_err("wrong rows must be rejected");
        assert!(format!("{e:#}").contains("WRONG rows"), "got: {e:#}");
    }

    #[test]
    fn grade_pit_compaction_ok_accepts_valid_outcomes() {
        // Exact non-decayed point, row-exact: fine.
        grade_pit_compaction_ok(false, false, 5, 5, &[1, 2, 3], &[1, 2, 3], &[(3, 9)], 0).unwrap();
        // Decayed point that SURVIVED a partial merge under faults (fine source
        // not yet deleted) and is row-exact: accepted precisely because the rows
        // match. Remove the `can_corrupt` escape and this would wrongly fail.
        grade_pit_compaction_ok(true, true, 5, 5, &[1, 2, 3], &[1, 2, 3], &[(3, 9)], 0).unwrap();
    }

    #[test]
    fn compaction_state_machine_generated_sequences() {
        run_compaction_state_machine(DEFAULT_CASES).unwrap();
    }

    #[test]
    fn compaction_state_machine_generated_sequences_under_faults() {
        run_compaction_state_machine_with_faults(DEFAULT_CASES).unwrap();
    }

    /// Pinned decay proof (safety-critical; fail-on-revert). Builds a real
    /// incremental chain of twelve marked boundaries, folds it with the REAL
    /// merge engine (`l1_batch=4`, `l2_batch=2`), and proves the three decay
    /// guarantees at once:
    ///   1. compaction forms merged windows (the fine sources are gone);
    ///   2. restore-to-latest is STILL row-exact through the merged objects;
    ///   3. every marked point grades correctly — a merged-window `max` boundary
    ///      (and the fine tail) restores EXACTLY; a point strictly inside a merged
    ///      window is the loud typed decay outcome, never a silent success and
    ///      never a bare chain gap.
    /// The final asserts require BOTH an exact and a decayed mark to have been
    /// exercised, so the test cannot pass vacuously. If the planner ever restored
    /// an inside-window point silently, or `restore_legacy_ltx` lost coverage and
    /// returned a bare gap, `check_restore_pit_compaction` fails here immediately.
    #[test]
    fn replay_compaction_forms_windows_and_grades_decay() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let tmp = TempDir::new().unwrap();
            let restores = TempDir::new().unwrap();
            let mut h = Harness::new(&tmp, FaultPlan::none(0)).unwrap();

            h.durable_flush().await.unwrap(); // initial base snapshot
            for _ in 0..12 {
                h.mark().await.unwrap(); // one committed row + a durable boundary
            }
            h.compact(4, 2, 0).await.unwrap();

            assert!(
                !h.model.merged_windows.is_empty(),
                "compaction must fold the fine chain into at least one merged window; \
                 got no windows (layout: {:?})",
                h.model.merged_windows
            );

            // (2) Latest restore is unchanged by compaction.
            let out = restores.path().join("latest.db");
            h.check_restore_latest(&out, 900).await.unwrap();

            // (3) Grade every marked boundary; require both classes to appear.
            let marks = h.model.marks.clone();
            let mut saw_exact = false;
            let mut saw_decay = false;
            for (k, mark) in marks.iter().enumerate() {
                if h.model.pit_decayed(mark.txid) {
                    saw_decay = true;
                } else {
                    saw_exact = true;
                }
                let o = restores.path().join(format!("pit_{k}.db"));
                h.check_restore_pit_compaction(&o, mark, k).await.unwrap();
            }
            assert!(
                saw_exact,
                "expected at least one exact (merged-boundary/tail) point among {:?} \
                 with windows {:?}",
                marks.iter().map(|m| m.txid).collect::<Vec<_>>(),
                h.model.merged_windows
            );
            assert!(
                saw_decay,
                "expected at least one point strictly inside a merged window among {:?} \
                 with windows {:?}",
                marks.iter().map(|m| m.txid).collect::<Vec<_>>(),
                h.model.merged_windows
            );
        });
    }

    /// Pinned reproducer for the compaction-aware-head PRODUCT FIX (fail-on-
    /// revert). The DST compaction phase found this shrunk sequence: compaction
    /// folds the fine L0 tail into a merged `levels/L1/` range whose `max`
    /// extends past the highest gen-folder TXID, then a `KillRestart` re-discovers
    /// the head. Before the fix, `discover_legacy_state` ignored `levels/`, so the
    /// restart discovered a stale-low head and the `--on-startup` eager snapshot
    /// landed BELOW the merged coverage — and the final `RestoreLatest` failed
    /// with "restore chain gap ... at seq 6". With the fix (discovery includes
    /// merged-level maxes) the head is correct, the eager base sits above the
    /// merged range, and restore-to-latest is exact. Reverting the fix in
    /// `legacy_manifest::discover_legacy_state` makes this replay fail.
    #[test]
    fn replay_compaction_restart_after_head_folded() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ops = vec![
            Op::WriteTxn { rows: 1 },
            Op::Checkpoint,
            Op::WriteTxn { rows: 1 },
            Op::WriteTxn { rows: 1 },
            Op::Mark,
            Op::WriteTxn { rows: 1 },
            Op::WriteTxn { rows: 1 },
            Op::Flush,
            Op::WriteTxn { rows: 1 },
            Op::Flush,
            Op::Compact {
                l1_batch: 2,
                l2_batch: 2,
                keep_fine_secs: 0,
            },
            Op::KillRestart,
            Op::RestoreLatest,
        ];
        rt.block_on(run_case(&ops, FaultPlan::none(0), true))
            .unwrap();
    }

    /// Catch-proof anchor (fail-on-revert for the C3a seq-contiguous batch
    /// clipping — a real compaction protection). A `Snapshot` between flushes
    /// breaks the L0 chain (the snapshot consumes its own seq and the next
    /// incremental chains from it, leaving a seq gap in `0000/`). The engine's
    /// `contiguous_batch` clips a merge to a seq-contiguous run and refuses to
    /// straddle that break, so this sequence compacts cleanly and every restore
    /// stays exact.
    ///
    /// Neuter that clip to a naive `take(batch)` and the SAME sequence makes
    /// `run_level_compaction` return `NonContiguous` — `Op::Compact` then fails
    /// with no faults and the compaction oracle catches it via `guard_durable`.
    /// The generated no-fault compaction phase finds and shrinks such a sequence
    /// (Snapshot/KillRestart + Compact); this pinned replay is the minimal
    /// reproducer kept as a permanent guard. With the clip intact it PASSES.
    #[test]
    fn replay_compaction_survives_snapshot_chain_break() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let ops = vec![
            Op::WriteTxn { rows: 2 },
            Op::Flush,
            Op::WriteTxn { rows: 2 },
            Op::Flush,
            Op::Snapshot, // breaks the L0 seq chain
            Op::WriteTxn { rows: 2 },
            Op::Flush,
            Op::WriteTxn { rows: 2 },
            Op::Flush,
            Op::Mark,
            Op::Compact {
                l1_batch: 2,
                l2_batch: 2,
                keep_fine_secs: 0,
            },
            Op::RestorePit { mark_sel: 0 },
            Op::RestoreLatest,
        ];
        rt.block_on(run_case(&ops, FaultPlan::none(0), true))
            .unwrap();
    }

    // ── Restart re-anchor seam regression (HIGH PRIORITY, PR #32 review) ─────
    //
    // These two tests reproduce the production restart seam DETERMINISTICALLY
    // and pin BOTH of its symptoms as fail-on-revert guards for the shared
    // startup re-anchor decision, `legacy_wal_sync::anchor_stream_on_startup`.
    //
    // Unlike `Harness::kill_restart` (which models the CORRECT eager-snapshot
    // restart the DST always assumed), these drive the SAME
    // `anchor_stream_on_startup` the production `--independent-tasks` watch loop
    // now calls on startup. Revert its resume branch (snapshot -> incremental)
    // and the restart publishes a seq-adjacent, chain-DISCONTINUOUS L0 at the
    // boundary: `reanchor_restart_restore_survives_the_chain_seam` then fails
    // with "Pre-apply checksum mismatch ... does not chain", and
    // `reanchor_restart_compaction_does_not_wedge_on_the_seam` then fails with
    // `CompactionError::NonContiguous`.
    use walrust::walrust_core::legacy_ltx::compute_checksum_from_file;
    use walrust::walrust_core::legacy_wal_sync::anchor_stream_on_startup;

    /// A bucket whose gen-0 L0 stream spans a kill/restart boundary, produced by
    /// driving the real engine + the production startup re-anchor.
    struct SeamWorld {
        storage: MockStorageBackend,
        prefix: String,
        name: String,
        expected: Vec<i64>,
        _conn: Connection, // keeps the WAL alive across the whole sequence
        _tmp: TempDir,
    }

    fn seam_insert_rows(conn: &Connection, n: usize, next_id: &mut i64, expected: &mut Vec<i64>) {
        conn.execute_batch("BEGIN IMMEDIATE;").unwrap();
        for _ in 0..n {
            let id = *next_id;
            conn.execute(
                "INSERT INTO items (id, value) VALUES (?1, ?2)",
                rusqlite::params![id, format!("v-{id}")],
            )
            .unwrap();
            expected.push(id);
            *next_id += 1;
        }
        conn.execute_batch("COMMIT;").unwrap();
    }

    async fn drive_kill_restart_seam() -> SeamWorld {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("seam.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             PRAGMA page_size=4096;
             CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
        )
        .unwrap();

        let name = "seam".to_string();
        let prefix = "sm/".to_string();
        let storage = MockStorageBackend::new(MockStorageConfig::new("seam"));
        let wal_path = db_path.with_extension("db-wal");
        let mut state = WatchedDbState {
            db_path: db_path.clone(),
            name: name.clone(),
            wal_path: wal_path.clone(),
            wal_offset: 0,
            wal_generation: 0,
            current_txid: 0,
            db_checksum: None,
            wal_salt: None,
            wal_checksum_chain: None,
        };

        let mut next_id = 1i64;
        let mut expected: Vec<i64> = Vec::new();

        // Initial base publish (the production initial sync at txid 0).
        sync_watched_db_once_to_storage(&storage, &prefix, &mut state)
            .await
            .unwrap();

        // One PRE-restart incremental. The WAL is never checkpointed, so the
        // on-disk `.db` file stays behind the chain tip — that is exactly what
        // makes the recomputed restart checksum diverge from the last L0's
        // chain_end and forge the seam on the reverted (incremental) resume path.
        seam_insert_rows(&conn, 3, &mut next_id, &mut expected);
        sync_watched_db_once_to_storage(&storage, &prefix, &mut state)
            .await
            .unwrap();

        // KILL/RESTART: rebuild state exactly as `watch_independent` startup does
        // (rediscover the head from the listing, reset the WAL cursor, recompute
        // the checksum from the `.db` FILE), then run the PRODUCTION re-anchor.
        let (current_txid, _gen) = discover_legacy_state(&storage, &prefix, &name)
            .await
            .unwrap();
        let db_checksum = compute_checksum_from_file(&db_path)
            .ok()
            .map(|c| c.into_inner());
        state = WatchedDbState {
            db_path: db_path.clone(),
            name: name.clone(),
            wal_path,
            wal_offset: 0,
            wal_generation: 0,
            current_txid,
            db_checksum,
            wal_salt: None,
            wal_checksum_chain: None,
        };
        anchor_stream_on_startup(&storage, &prefix, &mut state)
            .await
            .unwrap();

        // Two POST-restart incrementals: these MUST chain from the re-anchor.
        seam_insert_rows(&conn, 3, &mut next_id, &mut expected);
        sync_watched_db_once_to_storage(&storage, &prefix, &mut state)
            .await
            .unwrap();
        seam_insert_rows(&conn, 3, &mut next_id, &mut expected);
        sync_watched_db_once_to_storage(&storage, &prefix, &mut state)
            .await
            .unwrap();

        SeamWorld {
            storage,
            prefix,
            name,
            expected,
            _conn: conn,
            _tmp: tmp,
        }
    }

    fn seam_restored_ids(path: &Path) -> Vec<i64> {
        let conn = Connection::open(path).unwrap();
        let integrity: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok", "restored db failed integrity_check");
        let ids = conn
            .prepare("SELECT id FROM items ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        ids
    }

    /// Symptom 2 (restore): restore-to-latest must walk the post-restart chain
    /// without hitting the "does not chain" seam. Fail-on-revert of
    /// `anchor_stream_on_startup`'s resume branch.
    #[test]
    fn reanchor_restart_restore_survives_the_chain_seam() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let world = drive_kill_restart_seam().await;
            let out_dir = TempDir::new().unwrap();
            let out = out_dir.path().join("restored.db");
            restore_legacy_ltx(&world.storage, &world.prefix, &world.name, &out, None)
                .await
                .expect(
                    "restore-to-latest must survive a kill/restart seam — reverting \
                     anchor_stream_on_startup (snapshot -> incremental) reproduces \
                     'Pre-apply checksum mismatch ... does not chain'",
                );
            assert_eq!(
                seam_restored_ids(&out),
                world.expected,
                "restored rows must exactly match every committed row across the seam"
            );
        });
    }

    /// Symptom 1 (compaction): the leveled merge engine must not wedge on the
    /// seam. On the reverted (incremental) resume the oldest seq-contiguous L0
    /// run straddles the chain break and `run_level_compaction` returns
    /// `NonContiguous` forever; with the re-anchor snapshot the boundary is a
    /// clean seq gap and every merge stays within one chain.
    #[test]
    fn reanchor_restart_compaction_does_not_wedge_on_the_seam() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let world = drive_kill_restart_seam().await;
            let layout =
                RangeLayout::new(Arc::new(world.storage.clone()), &world.prefix, &world.name);
            // Drain L0->L1 (and L1->L2) to quiescence with batch 2 / keep_fine 0.
            for _ in 0..64 {
                let mut progressed = false;
                for level in [0u32, 1] {
                    match run_level_compaction(
                        &layout,
                        level,
                        2,
                        Duration::from_secs(0),
                        COMPACTION_NOW_MS,
                    )
                    .await
                    {
                        Ok(o) => progressed |= o.merged_count() > 0,
                        Err(e) => panic!(
                            "compaction wedged on the restart seam — reverting \
                             anchor_stream_on_startup reproduces CompactionError::NonContiguous \
                             at level {level}: {e}"
                        ),
                    }
                }
                if !progressed {
                    break;
                }
            }
            // The compacted, seam-crossing bucket must still restore row-exact.
            let out_dir = TempDir::new().unwrap();
            let out = out_dir.path().join("restored.db");
            restore_legacy_ltx(&world.storage, &world.prefix, &world.name, &out, None)
                .await
                .expect("restore after compaction must stay row-exact");
            assert_eq!(seam_restored_ids(&out), world.expected);
        });
    }
}
