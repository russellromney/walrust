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

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use proptest::prelude::*;
use proptest::test_runner::{Config, FileFailurePersistence, TestCaseError, TestRunner};
use rusqlite::Connection;
use tempfile::TempDir;

use walrust::retention::{RetentionPolicy, SnapshotEntry};
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
async fn run_case(ops: &[Op], faults: FaultPlan) -> Result<()> {
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
                h.check_restore_pit(&out, &mark, i).await?;
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
        rt.block_on(run_case(&ops, faults))
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
        rt.block_on(run_case(&ops, FaultPlan::none(0))).unwrap();
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
        rt.block_on(run_case(&ops, FaultPlan::none(0))).unwrap();
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
}
