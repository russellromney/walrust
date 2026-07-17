#!/usr/bin/env bash
#
# Replica-vs-compaction drill (gap 1a): `walrust replicate` tails flat gen-0
# incrementals and knows nothing about `levels/` (see src/sync/replicate.rs —
# discover_all_ltx_from_s3 / discover_all_legacy_ltx never lists levels/L*/).
# When the compaction engine merges L0 files into a levels/ object and deletes
# the superseded L0 sources, a replica that lags behind that merge finds a hole
# in the flat incremental chain it polls. This drill proves the EXISTING F5-era
# gap handler in replicate_poll (min_txid continuity check -> re-bootstrap from
# the newest snapshot whose max_txid covers the gap) already treats a
# compacted-away tail identically to any other chain gap: no levels/-reading
# code is added to replicate (that stays future work — see ROADMAP), the
# replica just re-bootstraps.
#
# Sequence: a compacting `walrust watch --independent-tasks` primary (aggressive
# batches, keep_fine_window=0, frequent periodic snapshots so a re-bootstrap
# target exists) plus a write driver; a `walrust replicate` catches up to a few
# early rows, then is SIGSTOP'd (frozen mid-stream, holding a low current_txid)
# while the primary keeps writing and compaction folds + deletes exactly the L0
# range the frozen replica still needs. The replica is SIGCONT'd and must
# converge to row-exact within a bounded number of polls, with its own log
# showing the re-bootstrap loudly (not a silent stall or corruption).
#
# Exit 0 = replica converged via re-bootstrap and said so out loud. Nonzero =
# the replica stalled, corrupted, or the log never mentioned re-bootstrapping
# (a silent recovery is not proven recovery).

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

L1_BATCH=${WALRUST_DRILL_L1_BATCH:-4}
L2_BATCH=${WALRUST_DRILL_L2_BATCH:-2}
KEEP_FINE=${WALRUST_DRILL_KEEP_FINE:-0s}
SNAPSHOT_INTERVAL=${WALRUST_DRILL_SNAPSHOT_INTERVAL:-6}
STALL_ROWS=${WALRUST_DRILL_STALL_ROWS:-4}
CONVERGE_TIMEOUT=${WALRUST_DRILL_CONVERGE_TIMEOUT:-90}

DRILL_COMPACTION_CONFIG=

# Copied from drills/kill-mid-compaction.sh (the pattern this drill reuses):
# a real `walrust watch` with leveled compaction enabled via a config file,
# forced into --independent-tasks (the only loop that ticks compaction).
start_walrust_compaction() {
  local db=$1
  "$WALRUST_BIN" watch "$db" \
    --config "$DRILL_COMPACTION_CONFIG" \
    --independent-tasks \
    --bucket "$DRILL_BUCKET_URI" \
    --endpoint "$DRILL_ENDPOINT" \
    --snapshot-interval "$SNAPSHOT_INTERVAL" \
    --wal-sync-interval "${WALRUST_DRILL_WAL_SYNC_INTERVAL:-1}" \
    --checkpoint-interval "${WALRUST_DRILL_CHECKPOINT_INTERVAL:-999999}" \
    --on-startup true \
    --no-metrics \
    --no-cache \
    >"$DRILL_WORKDIR/walrust.log" 2>&1 &
  DRILL_WALRUST_PID=$!
  log "walrust (compaction) pid=$DRILL_WALRUST_PID"
  wait_process_alive "$DRILL_WALRUST_PID" "walrust watch --config (compaction)"
}

# Count objects under a levels/ subpath.
level_object_count() {
  local pattern=$1
  s3_list_prefix "$DRILL_RUN_PREFIX/" | grep -cE "$pattern" || true
}

# Poll until the replica local DB reports at least `min` rows (a moving-target
# progress check, mirroring wait_restore_count_at_least in lib.sh but for the
# replica file instead of a restore).
wait_replica_count_at_least() {
  local replica=$1
  local min=$2
  local timeout=${3:-45}
  local deadline=$((SECONDS + timeout))
  local actual=unavailable
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ -f "$replica" ]; then
      actual=$(db_count "$replica" 2>/dev/null || echo unavailable)
      if [ "$actual" != unavailable ] && [ "$actual" -ge "$min" ]; then
        return 0
      fi
    fi
    kill -0 "$DRILL_REPLICA_PID" >/dev/null 2>&1 \
      || fail "replica exited before reaching >= $min rows; see $DRILL_WORKDIR/replica.log"
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "replica never reached >= $min rows (last=$actual); see $DRILL_WORKDIR/replica.log"
}

drill_setup
create_db "$DRILL_DB"
append_rows "$DRILL_DB" 5 base
name=$(basename "$DRILL_DB" .db)
replica="$DRILL_WORKDIR/replica.db"

DRILL_COMPACTION_CONFIG="$DRILL_WORKDIR/compaction.toml"
cat >"$DRILL_COMPACTION_CONFIG" <<TOML
# Aggressive leveled compaction so an L1 merge (and its source deletes) lands
# early, right on top of the rows a just-started replica has already applied.
[compaction]
enabled = true
keep_fine_window = "$KEEP_FINE"
l1_batch = $L1_BATCH
l2_batch = $L2_BATCH
TOML

start_driver "$DRILL_DB" "${WALRUST_DRILL_DRIVER_INTERVAL:-0.05}" rvc
wait_driver_count_at_least 8 45
start_walrust_compaction "$DRILL_DB"

# Let the replica catch up to a handful of early rows, THEN freeze it (SIGSTOP)
# so it holds a low current_txid while the primary keeps writing and folds that
# exact range into levels/, deleting the flat L0 objects the frozen replica
# still expects to find on its next poll.
start_replica "$DRILL_BUCKET_URI/$name" "$replica"
wait_replica_count_at_least "$replica" "$STALL_ROWS" 45

kill -0 "$DRILL_REPLICA_PID" >/dev/null 2>&1 || fail "replica pid vanished before it could be frozen"
kill -STOP "$DRILL_REPLICA_PID"
log "replica pid=$DRILL_REPLICA_PID SIGSTOP'd (frozen mid-stream, holding a low current_txid)"

# Sustain writes on the primary until compaction has visibly folded L0 into a
# merged level AND at least one periodic snapshot has landed past that point
# (the re-bootstrap target replicate needs). Bounded wait, walrust must stay
# alive throughout.
l1_deadline=$((SECONDS + 90))
l1_seen=0
while [ "$SECONDS" -lt "$l1_deadline" ]; do
  kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
    || fail "primary walrust exited while folding levels; see $DRILL_WORKDIR/walrust.log"
  if [ "$(level_object_count '/levels/L1/.*\.ltx$')" -gt 0 ]; then
    l1_seen=1
    break
  fi
  sleep "$DRILL_POLL_INTERVAL"
done
[ "$l1_seen" -eq 1 ] || fail "L1 never fired within the deadline — cannot test a compacted-away tail \
(check l1_batch=$L1_BATCH / keep_fine=$KEEP_FINE)"
log "L1 fired — the frozen replica's next incremental range is now superseded"

# Give a periodic snapshot time to land past the fold so re-bootstrap has a
# target (>= 2 snapshot intervals of slack).
sleep "$((SNAPSHOT_INTERVAL * 2))"

# Thaw the replica. Its next poll must discover a chain gap (the flat object it
# needs is gone) and re-bootstrap from the newest snapshot, per the existing
# F5-era gap handler in replicate_poll — no levels/-reading code was added.
kill -CONT "$DRILL_REPLICA_PID"
log "replica pid=$DRILL_REPLICA_PID SIGCONT'd (thawed) — expecting a loud re-bootstrap on its next poll"

pause_driver
expected=$(driver_count)
wait_restore_count "$name" "$expected"
wait_replica_count "$replica" "$expected" "$CONVERGE_TIMEOUT"

# The recovery must be LOUD, not silent: the replica log must name the
# gap/re-bootstrap it took to get there.
if ! grep -qiE "gap detected|re-?bootstrap" "$DRILL_WORKDIR/replica.log"; then
  fail "replica converged but its log never mentioned a gap/re-bootstrap — recovery must be loud, \
not silent; see $DRILL_WORKDIR/replica.log"
fi
log "replica log confirms a loud re-bootstrap: $(grep -iE 'gap detected|re-?bootstrap' "$DRILL_WORKDIR/replica.log" | tail -1)"

stop_driver
stop_replica
stop_walrust

log "PASS replica-vs-compaction rows=$expected"
