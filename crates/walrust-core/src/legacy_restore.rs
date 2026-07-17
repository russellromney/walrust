//! Restore for the legacy Litestream-derived LTX object layout.
//!
//! ## Leveled restore + the LTX→HADBP seam (wave C3a)
//!
//! A legacy bucket can be **compacted**: the fine per-second L0 objects
//! (`{db}/0000/{min}-{max}.ltx`, real litestream **LTX** bytes) are folded into
//! coarser merged objects (`{db}/levels/L*/{min}-{max}.ltx`, **HADBP** payloads;
//! see the compaction module header's "both adapters carry HADBP payloads"
//! decision). Restore must therefore replay a chain that interleaves **two byte
//! formats**. It does so by reusing the compaction [`plan_restore`] planner over
//! the union of L0 LTX points and merged HADBP ranges, then applying each planned
//! object through its format's apply path:
//!   - an **LTX** object via [`legacy_ltx::apply_ltx_to_db_checked`], and
//!   - a merged **HADBP** object via [`crate::ltx::apply_decoded_changeset_to_db`]
//!     after a DB-anchored [`physical::verify_chain`] pre-check.
//!
//! The chain stays verifiable across the seam with a **single running checksum
//! in the LTX domain**: the merge stamps each produced HADBP object's
//! `prev_checksum`/`declared_end` with the LTX `pre`/`post` of the range it
//! covers (see [`crate::compaction::layout::parse_ltx_source_header`]), so a
//! merged object links onto an LTX-restored DB state exactly like the fine LTX
//! objects it replaced. `chain_end()` advances the running value past a merged
//! range; the HADBP content checksum independently guards the merged pages.

use anyhow::{anyhow, Result};
use hadb_changeset::physical::{self, chain_end};
use hadb_storage::StorageBackend;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::compaction::{level_subpath, parse_range_name, plan_restore, PlanCandidate, SeqRange};
use crate::errors::WalrustError;
use crate::legacy_ltx::{self, Checksum};
use crate::legacy_manifest::{
    database_prefix, discover_legacy_state, find_latest_legacy_snapshot,
    find_latest_legacy_snapshot_at_or_before, parse_legacy_flat_ltx_filename, parse_ltx_filename,
};

/// HADBP magic (merged level objects); anything else in the plan is real LTX.
const HADBP_MAGIC: &[u8; 5] = b"HADBP";

/// Levels above L0 to probe for merged objects (matches the restore planner's
/// cap; legacy compaction builds L1/L2 but this leaves room).
const MAX_LEVEL_PROBE: u32 = 16;

struct AtomicRestore {
    path: PathBuf,
    published: bool,
}

impl AtomicRestore {
    fn new(output: &Path) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let parent = output.parent().unwrap_or_else(|| Path::new("."));
        let file_name = output
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("restored.db");
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{file_name}.legacy-restore-{}-{id}.tmp",
            std::process::id()
        ));
        Self {
            path,
            published: false,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn publish(mut self, output: &Path) -> Result<()> {
        std::fs::rename(&self.path, output).map_err(|e| {
            anyhow!(
                "failed to atomically publish restored database {} over {}: {e}",
                self.path.display(),
                output.display()
            )
        })?;
        self.published = true;
        Ok(())
    }
}

impl Drop for AtomicRestore {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn verify_sqlite_integrity(path: &Path) -> Result<()> {
    let conn = rusqlite::Connection::open(path)
        .map_err(|e| anyhow!("failed to open restored database for integrity_check: {e}"))?;
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|e| anyhow!("failed to run integrity_check on restored database: {e}"))?;
    if result != "ok" {
        return Err(anyhow!(
            "restored database failed integrity_check: {result}"
        ));
    }
    Ok(())
}

fn restore_reached_target(final_txid: u64, target_txid: u64, explicit_pit: bool) -> Result<()> {
    if explicit_pit {
        if final_txid > target_txid {
            return Err(anyhow!(
                "restore overshot: reached TXID {final_txid} but point-in-time target is \
                 {target_txid} (no snapshot/chain at or before the requested point)"
            ));
        }
        return Ok(());
    }
    if final_txid != target_txid {
        return Err(anyhow!(
            "restore incomplete: reached TXID {final_txid} but latest is {target_txid}. \
             An incremental object is missing or the chain has a gap; the restored \
             database does not reflect the latest committed state."
        ));
    }
    Ok(())
}

async fn read_required_object(storage: &dyn StorageBackend, key: &str) -> Result<Vec<u8>> {
    storage
        .get(key)
        .await?
        .ok_or_else(|| anyhow!("missing legacy LTX object: {key}"))
}

/// Gather every restorable object for a legacy bucket: the L0 real-LTX pool
/// (`{db}/0000/`) plus the merged HADBP ranges (`{db}/levels/L*/`). Ranges are
/// parsed from filenames only (no header reads), so this is one LIST per level.
async fn gather_legacy_candidates(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
) -> Result<Vec<PlanCandidate>> {
    let db_prefix = database_prefix(prefix, db_name);
    let mut out = Vec::new();

    // L0: real LTX incrementals (and any flat legacy shape) under `0000/`.
    let l0_dir = format!("{db_prefix}0000/");
    for key in storage.list(&l0_dir, None).await? {
        let filename = key.rsplit('/').next().unwrap_or(&key);
        let range = parse_ltx_filename(filename)
            .map(|(min, max)| SeqRange::new(min, max))
            .or_else(|| parse_legacy_flat_ltx_filename(filename).map(SeqRange::point));
        if let Some(range) = range {
            out.push(PlanCandidate {
                key,
                level: 0,
                range,
            });
        }
    }

    // Merged levels: HADBP payloads named with the `{min}-{max}.ltx` range
    // scheme under the dedicated `levels/L*/` sub-path.
    for level in 1..=MAX_LEVEL_PROBE {
        let dir = format!("{db_prefix}{}/", level_subpath(level));
        for key in storage.list(&dir, None).await? {
            let filename = key.rsplit('/').next().unwrap_or(&key);
            if let Some(range) = parse_range_name(filename, "ltx") {
                out.push(PlanCandidate { key, level, range });
            }
        }
    }
    Ok(out)
}

/// Restore a database from the legacy LTX object layout, **leveled-aware**.
///
/// Reconstructs from the newest snapshot at or before the target, then replays
/// the planned chain of L0 LTX objects and merged HADBP ranges (see the module
/// header for the seam). Returns the final restored TXID.
pub async fn restore_legacy_ltx(
    storage: &dyn StorageBackend,
    prefix: &str,
    db_name: &str,
    output: &Path,
    point_in_time: Option<u64>,
) -> Result<u64> {
    let (current_txid, _max_generation) = discover_legacy_state(storage, prefix, db_name).await?;
    if current_txid == 0 {
        return Err(WalrustError::restore_not_found(format!(
            "no LTX files found for database: {db_name}"
        ))
        .into());
    }

    // The reachable pool. A merged range can extend past the highest gen-folder
    // TXID `discover_legacy_state` sees (its L0 tail was folded away), so the
    // true latest is the max over the L0 points and merged ranges too.
    let candidates = gather_legacy_candidates(storage, prefix, db_name).await?;
    let discovered_max = candidates
        .iter()
        .map(|c| c.range.max)
        .max()
        .unwrap_or(current_txid);
    let latest_txid = current_txid.max(discovered_max);

    // E4: a point-in-time beyond the newest available TXID is a hard error
    // naming the max available, not a silent fall-through returning latest.
    if let Some(pit) = point_in_time {
        if pit > latest_txid {
            return Err(WalrustError::restore_not_found(format!(
                "requested point-in-time TXID {pit} is beyond the newest available TXID \
                 {latest_txid} for database {db_name}"
            ))
            .into());
        }
    }

    let target_txid = point_in_time.unwrap_or(latest_txid);
    let snapshot = if point_in_time.is_some() {
        find_latest_legacy_snapshot_at_or_before(storage, prefix, db_name, target_txid).await?
    } else {
        find_latest_legacy_snapshot(storage, prefix, db_name).await?
    }
    .ok_or_else(|| {
        if point_in_time.is_some() {
            WalrustError::restore_not_found(format!(
                "snapshot unavailable for database {db_name} at or before TXID {target_txid}"
            ))
        } else {
            WalrustError::restore_not_found(format!("snapshot unavailable for database: {db_name}"))
        }
    })?;

    tracing::info!(
        "Restoring from LTX snapshot: {} (TXID: {}-{}, generation: {})",
        snapshot.key,
        snapshot.min_txid,
        snapshot.max_txid,
        snapshot.generation
    );

    let snapshot_bytes = read_required_object(storage, &snapshot.key).await?;
    let staged_restore = AtomicRestore::new(output);
    let staged_output = staged_restore.path();

    let decode_result =
        legacy_ltx::decode_to_db(std::io::Cursor::new(snapshot_bytes), staged_output)?;
    let floor = snapshot.max_txid;
    let mut final_txid = floor;
    // Running chain value, LTX domain. Both LTX and (LTX-stamped) HADBP objects
    // link through it.
    let mut running: Checksum = decode_result.post_apply_checksum;

    // Plan the replay over L0 LTX points + merged HADBP ranges. A PITR strictly
    // inside a merged window (granularity decay) surfaces here as the planner's
    // typed error naming the nearest restorable points on both sides.
    let plan_pool: Vec<PlanCandidate> = candidates
        .into_iter()
        .filter(|c| c.range.max > floor && c.range.min <= target_txid)
        .collect();
    let plan = match plan_restore(&plan_pool, floor, target_txid) {
        Ok(plan) => plan,
        Err(e) => {
            // A gap here may be granularity decay rather than a missing object:
            // a LATER full snapshot absorbs the target (snapshot = ultimate
            // compaction). Name both neighbors instead of reporting a bare gap.
            //
            // E10: classifying a chain gap as decay vs. a genuinely missing
            // object REQUIRES a complete snapshot listing. Under faults this
            // LIST can fail transiently; swallowing it with `unwrap_or_default()`
            // silently degraded the typed `PitrInsideSnapshotSpan`
            // (RestoreNotFound) outcome into a bare, non-typed chain gap — a
            // decision made from an incomplete view. A missing snapshot > floor
            // then read as "no later snapshot absorbs the target", so a
            // legitimately-foreclosed PITR (its boundary pruned, a later full
            // snapshot still present) surfaced as a loud gap instead of the clean
            // typed decay error, and the transient never got retried because it
            // had been rewritten into a non-transient message. Propagate the LIST
            // error so the caller retries the whole restore against a complete
            // listing, rather than mis-classifying from a truncated one.
            let later: Vec<u64> =
                crate::legacy_manifest::discover_legacy_snapshots(storage, prefix, db_name)
                    .await?
                    .iter()
                    .map(|s| s.max_txid)
                    .filter(|m| *m > floor)
                    .collect();
            let refined = crate::compaction::refine_gap_with_snapshot_spans(e, &later, target_txid);
            // Snapshot-span decay = "this point is not restorable at that
            // grain": typed as RestoreNotFound (same family as a beyond-newest
            // target) so callers and the DST oracle can match it without
            // string-parsing. Genuine gaps stay in the restore/integrity family.
            let err = if matches!(
                refined,
                crate::compaction::PlanError::PitrInsideSnapshotSpan { .. }
            ) {
                WalrustError::restore_not_found(refined.to_string())
            } else {
                WalrustError::restore(refined.to_string())
            };
            return Err(err.into());
        }
    };

    for cand in &plan.files {
        let bytes = read_required_object(storage, &cand.key).await?;
        if bytes.len() >= 5 && &bytes[0..5] == HADBP_MAGIC {
            // Merged HADBP range. DB-anchored pre-check: its LTX-domain
            // prev_checksum must equal the running value; verify_chain also
            // validates the merged object's content checksum (a torn merge is
            // caught here). Advance the running value past the whole range via
            // its declared (LTX post) chain_end.
            let cs = crate::ltx::decode_sqlite_changeset(&bytes).map_err(|e| {
                anyhow!(
                    "failed to decode merged object {} during restore: {e}",
                    cand.key
                )
            })?;
            physical::verify_chain(running.into_inner(), &cs).map_err(|e| {
                WalrustError::integrity(format!(
                    "restore chain broken at merged object {} (seq range {}..={}): {e}",
                    cand.key, cand.range.min, cand.range.max
                ))
            })?;
            crate::ltx::apply_decoded_changeset_to_db(&cs, staged_output).map_err(|e| {
                anyhow!(
                    "failed to apply merged object {} during restore: {e}",
                    cand.key
                )
            })?;
            running = Checksum::new(chain_end(&cs));
        } else {
            // Real LTX object. apply_ltx_to_db_checked verifies its pre_apply
            // equals the running value and returns the new (LTX) post.
            let apply_result = legacy_ltx::apply_ltx_to_db_checked(
                std::io::Cursor::new(bytes),
                staged_output,
                running,
            )?;
            running = apply_result.post_apply_checksum;
        }
        final_txid = cand.range.max;
    }

    restore_reached_target(final_txid, target_txid, point_in_time.is_some())?;
    verify_sqlite_integrity(staged_output)?;
    staged_restore.publish(output)?;
    Ok(final_txid)
}
