#!/usr/bin/env bash
#
# Kill-mid-compaction drill: a real `walrust watch` with leveled compaction
# ENABLED at aggressive batch settings, SIGKILL'd in a loop timed to land
# between merge activity while a write driver hammers the database. After >= 4
# kill/restart cycles the bucket must still restore-to-latest row-exact (exit 0,
# integrity ok) AND have CONVERGED — a crash between "write merged object" and
# "delete sources" leaves harmless bounded overlap, never unbounded duplicate
# coverage.
#
# This is the E2-class safety proof for the compaction write path: the
# write-verify-delete ordering means a kill at any point either leaves the fine
# sources intact (re-merge converges) or leaves a sound merged object plus
# soon-to-be-deleted sources (re-run finishes the deletion). Restore-to-latest
# must never lose a durable row across any of those interleavings. It also
# exercises the compaction-aware restart head discovery: with
# `keep_fine_window = 0` the fine tail (including the head) is eligible to fold,
# so a restart must discover the head from the merged levels, not a stale
# gen-folder TXID.
#
# Knobs (env):
#   WALRUST_DRILL_KILL_CYCLES     kill/restart cycles (default 5, must be >= 4)
#   WALRUST_DRILL_KILL_PERIOD     seconds of sustained writes between kills (12)
#   WALRUST_DRILL_L1_BATCH        L0->L1 fold batch (default 4, aggressive)
#   WALRUST_DRILL_L2_BATCH        L1->L2 fold batch (default 3, aggressive)
#   WALRUST_DRILL_KEEP_FINE       keep_fine_window (default "0s": fold everything)
#   WALRUST_DRILL_INDUCE_LOSS     1 => after the pass, delete a needed object and
#                                 prove the row-diff guard FAILS (self-test)
#
# Exit 0 = converged + row-exact. Nonzero = a lost row, a gap, or unbounded
# duplicate coverage.

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

CYCLES=${WALRUST_DRILL_KILL_CYCLES:-5}
KILL_PERIOD=${WALRUST_DRILL_KILL_PERIOD:-12}
L1_BATCH=${WALRUST_DRILL_L1_BATCH:-4}
L2_BATCH=${WALRUST_DRILL_L2_BATCH:-3}
KEEP_FINE=${WALRUST_DRILL_KEEP_FINE:-0s}
INDUCE_LOSS=${WALRUST_DRILL_INDUCE_LOSS:-0}
# Periodic full snapshots re-anchor the restore floor (litestream heritage:
# restore depth is bounded by the newest snapshot at/below the target). The
# independent-tasks watch loop — the only mode that ticks leveled compaction —
# rebuilds its WAL cursor from the DB file on a hard-kill restart, so a snapshot
# cadence gives restore-to-latest a recent, re-anchored base above the merged
# history instead of forcing a rebuild from the gen-1 base through every merged
# object. This is the realistic production config (snapshots always run).
WALRUST_DRILL_SNAPSHOT_INTERVAL=${WALRUST_DRILL_SNAPSHOT_INTERVAL:-8}

[ "$CYCLES" -ge 4 ] || fail "WALRUST_DRILL_KILL_CYCLES must be >= 4 (got $CYCLES)"

DRILL_COMPACTION_CONFIG=

# Start a real `walrust watch` on ONE CLI-specified database with compaction
# enabled via a config file (compaction is a config-only knob; a positional db
# path plus `--config` resolves the db from the CLI and the compaction settings
# from the file — see main.rs `resolve_watch_config`). Leveled compaction only
# ticks in the independent-tasks watch loop (`--independent-tasks`); the default
# shadow loop ignores the `[compaction]` knob, so the flag is REQUIRED here. PID
# captured into DRILL_WALRUST_PID so the shared PID-verified kill/stop helpers
# apply.
start_walrust_compaction() {
  local db=$1
  "$WALRUST_BIN" watch "$db" \
    --config "$DRILL_COMPACTION_CONFIG" \
    --independent-tasks \
    --bucket "$DRILL_BUCKET_URI" \
    --endpoint "$DRILL_ENDPOINT" \
    --snapshot-interval "${WALRUST_DRILL_SNAPSHOT_INTERVAL:-999999}" \
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

# Mode-agnostic readiness: wait until the freshly-started watcher publishes a
# NEW L0 object beyond `baseline` (proving it re-attached and is syncing). The
# independent-tasks loop does not create the `_walrust_seq` checkpoint-blocker
# table the shadow loop uses, so bucket sync progress — not the blocker table —
# is the portable readiness signal here.
wait_for_sync_progress() {
  local baseline=$1
  local deadline=$((SECONDS + ${WALRUST_DRILL_READY_TIMEOUT:-45}))
  local cur
  while [ "$SECONDS" -lt "$deadline" ]; do
    cur=$(latest_txid)
    if [ "${cur:-0}" -gt "$baseline" ]; then
      return 0
    fi
    kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
      || fail "walrust exited before making sync progress; see $DRILL_WORKDIR/walrust.log"
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "walrust made no sync progress past TXID $baseline within timeout"
}

# Keep the write driver hammering for `secs` seconds, asserting walrust stays
# alive the whole time (a crash between kills is a real failure, not a kill).
sustain_writes() {
  local secs=$1
  local deadline=$((SECONDS + secs))
  while [ "$SECONDS" -lt "$deadline" ]; do
    kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
      || fail "walrust exited on its own between kills; see $DRILL_WORKDIR/walrust.log"
    sleep "$DRILL_POLL_INTERVAL"
  done
}

# Count objects at each level under the run prefix.
level_object_count() {
  local pattern=$1
  s3_list_prefix "$DRILL_RUN_PREFIX/" | grep -cE "$pattern" || true
}

# Convergence guard: overlap from a kill between merge-write and source-delete is
# allowed but BOUNDED. A convergence bug (e.g. re-merging into an overlapping
# range forever, or never deleting folded sources) shows up as unbounded object
# counts. Bounds are generous multiples of the batch/cycle counts.
assert_bucket_converged() {
  local l0 l1 l2 merged
  l0=$(level_object_count '/0000/.*\.ltx$')
  l1=$(level_object_count '/levels/L1/.*\.ltx$')
  l2=$(level_object_count '/levels/L2/.*\.ltx$')
  merged=$((l1 + l2))
  log "converged object counts: L0=$l0 L1=$l1 L2=$l2 (merged=$merged)"
  if [ "$merged" -eq 0 ]; then
    fail "CONVERGENCE: no merged level objects were produced — compaction never fired \
(check l1_batch=$L1_BATCH / keep_fine=$KEEP_FINE / cycles=$CYCLES)"
  fi
  local max_l0=$((L1_BATCH * 12 + 60))
  local max_merged=$((CYCLES * 12 + 60))
  [ "$l0" -le "$max_l0" ] \
    || fail "CONVERGENCE: L0 object count $l0 exceeds bound $max_l0 — unbounded fine-object growth (compaction not folding)"
  [ "$merged" -le "$max_merged" ] \
    || fail "CONVERGENCE: merged object count $merged exceeds bound $max_merged — unbounded duplicate coverage"
}

# Print every snapshot base object (min-TXID == 1 in a non-L0 generation folder).
# Snapshots are full-DB images and every restore begins from the newest one at or
# below the target, so deleting them all removes every restore base — an
# unambiguous, deterministic data loss. (A single incremental/merged deletion
# proves nothing: a recent full snapshot still reconstructs those rows.)
snapshot_base_objects() {
  s3_list_prefix "$DRILL_RUN_PREFIX/" \
    | grep -E '/[0-9a-f]{4}/0000000000000001-[0-9a-f]{16}\.ltx$' \
    | grep -v '/0000/'
}

drill_setup
create_db "$DRILL_DB"
append_rows "$DRILL_DB" 5 base

DRILL_COMPACTION_CONFIG="$DRILL_WORKDIR/compaction.toml"
cat >"$DRILL_COMPACTION_CONFIG" <<TOML
# Aggressive leveled compaction so merges fire during the drill window.
[compaction]
enabled = true
keep_fine_window = "$KEEP_FINE"
l1_batch = $L1_BATCH
l2_batch = $L2_BATCH
TOML

start_driver "$DRILL_DB" "${WALRUST_DRILL_DRIVER_INTERVAL:-0.05}" kmc
wait_driver_count_at_least 12 45
start_walrust_compaction "$DRILL_DB"
wait_for_sync_progress 0

for i in $(seq 1 "$CYCLES"); do
  log "cycle $i/$CYCLES: sustaining writes ${KILL_PERIOD}s, then SIGKILL mid-compaction"
  sustain_writes "$KILL_PERIOD"
  baseline=$(latest_txid)
  kill_walrust_pid_verified
  start_walrust_compaction "$DRILL_DB"
  # The restart must advance the head past what was durable before the kill —
  # this also exercises the compaction-aware restart discovery (with
  # keep_fine_window=0 the folded head must be rediscovered from levels/).
  wait_for_sync_progress "$baseline"
done

# Quiesce and settle: pause the driver, give the running watcher a bounded window
# to flush trailing frames and drain pending merges.
pause_driver
expected=$(driver_count)
sustain_writes 3
name=$(basename "$DRILL_DB" .db)

# (1) Restore-to-latest must be row-exact + integrity ok + exit 0.
wait_restore_count "$name" "$expected"

# (2) The bucket must have converged (bounded overlap, real merged levels).
assert_bucket_converged

stop_driver
stop_walrust

# (3) Self-test (opt-in): prove the row-diff guard has teeth. With the driver
# paused and walrust stopped the bucket is frozen; strip the newest committed
# TXIDs from every source. The very next restore MUST fall short of `expected`
# (nonzero, row diff). If it still matches, the guard is toothless — fail loudly.
if [ "$INDUCE_LOSS" = "1" ]; then
  deleted=0
  while IFS= read -r key; do
    [ -n "$key" ] || continue
    s3_delete_key "$key"
    deleted=$((deleted + 1))
  done < <(snapshot_base_objects)
  [ "$deleted" -gt 0 ] || fail "induced-loss: no snapshot base objects found to delete"
  log "induced-loss: deleted $deleted snapshot base object(s) — restore now has no base"
  if assert_restored_count_once "$name" "$expected"; then
    fail "INDUCED LOSS did not break restore — the guard is toothless \
(restored $DRILL_LAST_ACTUAL rows == expected $expected after deleting every restore base)"
  fi
  log "induced-loss proof OK: restore correctly failed after the deletion \
(actual rows=${DRILL_LAST_ACTUAL:-unavailable}; error: ${DRILL_LAST_RESTORE_ERROR:-none})"
fi

log "PASS kill-mid-compaction cycles=$CYCLES rows=$expected"
