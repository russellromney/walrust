#!/usr/bin/env bash
#
# Compaction soak drill (gap 3): a short, hilariously-aggressive stress test.
# TOTAL WINDOW: 120 seconds. A real `walrust watch --independent-tasks` with
# leveled compaction at aggressive batch settings is kept under sustained
# writes (~50 rows/sec) and SIGKILL'd + immediately restarted every ~20s (five
# to six cycles inside the 120s window), while a periodic full snapshot runs
# every ~15s. This forces many L1 merges, several L2 merges, multiple
# snapshots, and 5-6 crash-restarts to all interleave inside two minutes --
# the compaction write path, the snapshot timer, and the crash-recovery head
# discovery all racing each other repeatedly in a short window.
#
# wal-sync-interval is a plain whole-second u64 in both the CLI flag and
# walrust.toml (see src/config.rs SyncConfig::wal_sync_interval) -- there is
# no sub-second knob, so "0.2s" from the spec is not literally settable. This
# drill uses 1s, the minimum practical value (0 would busy-loop-poll and spam
# S3 without adding real stress), and compensates with the ~50 rows/sec
# driver so each 1s sync tick still batches a healthy handful of committed
# rows and the L0 pool still fills fast enough to fold repeatedly.
#
# End assertions: restore-to-latest row-exact + integrity_check, `walrust
# verify` exits 0, object counts stay bounded (no unbounded duplicate
# coverage -- a kill between merge-write and source-delete may leave harmless
# overlap, never runaway growth), and the concatenated log across every
# restart cycle has no ERROR-level lines beyond the expected restart-recovery
# notices. A one-shot induced-failure proof (delete a needed object) closes
# the loop: the row-diff guard above must have real teeth, not just pass by
# accident.
#
# The restart re-anchor seam that this drill originally exposed (a
# seq-contiguous but checksum-DIScontinuous L0 boundary after a kill/restart,
# which flaked restore-to-latest with "Pre-apply checksum mismatch ... does not
# chain" AND permanently wedged compaction with CompactionError::NonContiguous)
# is FIXED at the root: `--independent-tasks` startup now re-anchors a resumed
# stream with a fresh snapshot (walrust_core::legacy_wal_sync::
# anchor_stream_on_startup), so post-restart L0s never sit seq-adjacent to a
# chain-incompatible predecessor. This drill therefore runs UN-crutched: a
# single attempt, restore failures fail hard (no classify/retry), and ANY
# ERROR-level log line fails the drill (no NonContiguous allow-list).
#
# Wired into `make drill` (drills/run-all.sh) AND the nightly workflow (which
# runs `make drill`) -- unlike drills/version-skew.sh, this drill needs no
# network beyond the S3 endpoint already required by every other drill.

set -Eeuo pipefail

# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

TOTAL_SECONDS=${WALRUST_DRILL_SOAK_TOTAL_SECONDS:-120}
KILL_EVERY=${WALRUST_DRILL_SOAK_KILL_EVERY:-20}
L1_BATCH=${WALRUST_DRILL_SOAK_L1_BATCH:-4}
L2_BATCH=${WALRUST_DRILL_SOAK_L2_BATCH:-3}
KEEP_FINE=${WALRUST_DRILL_SOAK_KEEP_FINE:-0s}
SNAPSHOT_INTERVAL=${WALRUST_DRILL_SOAK_SNAPSHOT_INTERVAL:-15}
WAL_SYNC_INTERVAL=${WALRUST_DRILL_SOAK_WAL_SYNC_INTERVAL:-1}
DRIVER_INTERVAL=${WALRUST_DRILL_SOAK_DRIVER_INTERVAL:-0.02} # ~50 rows/sec
INDUCE_LOSS=${WALRUST_DRILL_SOAK_INDUCE_LOSS:-1}

[ "$TOTAL_SECONDS" -ge "$((KILL_EVERY * 4))" ] \
  || fail "TOTAL_SECONDS=$TOTAL_SECONDS too small for KILL_EVERY=$KILL_EVERY to yield >= 4 cycles"

# Restore-to-latest, polled up to DRILL_RESTORE_TIMEOUT. Any persistent failure
# -- a row diff, a chain error, a restore that never ran -- fails the drill hard.
# The restart re-anchor seam that used to make this flake is fixed at the root
# (see the header), so there is no classify/retry crutch: a "does not chain"
# here now means a genuine regression and MUST fail loudly.
soak_wait_restore() {
  local name=$1
  local expected=$2
  local deadline=$((SECONDS + DRILL_RESTORE_TIMEOUT))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if assert_restored_count_once "$name" "$expected"; then
      log "restore rows ok: $expected"
      return 0
    fi
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "restore-to-latest failed: expected rows=$expected \
actual rows=${DRILL_LAST_ACTUAL:-unavailable}; restore output: ${DRILL_LAST_RESTORE_ERROR:-none}"
}

DRILL_COMPACTION_CONFIG=

# Copied from drills/kill-mid-compaction.sh (start_walrust_compaction), with
# one change: logs APPEND across restarts (kill-mid-compaction truncates its
# single-cycle log each restart) so the end-of-run ERROR scan below covers the
# ENTIRE soak, not just the final cycle.
start_walrust_compaction() {
  local db=$1
  "$WALRUST_BIN" watch "$db" \
    --config "$DRILL_COMPACTION_CONFIG" \
    --independent-tasks \
    --bucket "$DRILL_BUCKET_URI" \
    --endpoint "$DRILL_ENDPOINT" \
    --snapshot-interval "$SNAPSHOT_INTERVAL" \
    --wal-sync-interval "$WAL_SYNC_INTERVAL" \
    --checkpoint-interval 999999 \
    --on-startup true \
    --no-metrics \
    --no-cache \
    >>"$DRILL_WORKDIR/walrust.log" 2>&1 &
  DRILL_WALRUST_PID=$!
  log "walrust (compaction) pid=$DRILL_WALRUST_PID"
  wait_process_alive "$DRILL_WALRUST_PID" "walrust watch --config (compaction)"
}

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

sustain_writes() {
  local secs=$1
  local deadline=$((SECONDS + secs))
  while [ "$SECONDS" -lt "$deadline" ]; do
    kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
      || fail "walrust exited on its own between kills; see $DRILL_WORKDIR/walrust.log"
    sleep "$DRILL_POLL_INTERVAL"
  done
}

level_object_count() {
  local pattern=$1
  s3_list_prefix "$DRILL_RUN_PREFIX/" | grep -cE "$pattern" || true
}

snapshot_base_objects() {
  s3_list_prefix "$DRILL_RUN_PREFIX/" \
    | grep -E '/[0-9a-f]{4}/0000000000000001-[0-9a-f]{16}\.ltx$' \
    | grep -v '/0000/'
}

# Bounded-growth guard: over ~120s at ~50 rows/sec with a 1s sync tick, expect
# on the order of TOTAL_SECONDS/WAL_SYNC_INTERVAL sync ticks total. Generous
# multiples (kill-mid-compaction's philosophy: bound overlap, don't demand a
# tight number) catch genuine runaway growth (a stuck fold, a delete that
# never happens) without false-alarming on legitimate crash-window overlap.
assert_bucket_converged() {
  local l0 l1 l2 merged max_l0 max_merged ticks
  l0=$(level_object_count '/0000/.*\.ltx$')
  l1=$(level_object_count '/levels/L1/.*\.ltx$')
  l2=$(level_object_count '/levels/L2/.*\.ltx$')
  merged=$((l1 + l2))
  ticks=$((TOTAL_SECONDS / WAL_SYNC_INTERVAL))
  log "converged object counts: L0=$l0 L1=$l1 L2=$l2 (merged=$merged); ~$ticks sync ticks over the soak"
  if [ "$merged" -eq 0 ]; then
    fail "CONVERGENCE: no merged level objects were produced — compaction never fired \
(check l1_batch=$L1_BATCH / keep_fine=$KEEP_FINE / total_seconds=$TOTAL_SECONDS)"
  fi
  max_l0=$((L1_BATCH * 6 + 40))
  max_merged=$((ticks + 80))
  [ "$l0" -le "$max_l0" ] \
    || fail "CONVERGENCE: L0 object count $l0 exceeds bound $max_l0 — unbounded fine-object growth"
  [ "$merged" -le "$max_merged" ] \
    || fail "CONVERGENCE: merged object count $merged exceeds bound $max_merged — unbounded duplicate coverage"
}

# ERROR-level log scan across the WHOLE soak (every restart cycle appended to
# one file). A kill mid-write is expected to surface at most benign,
# non-ERROR reconnection chatter on the next startup (INFO/WARN); a real,
# UNEXPECTED ERROR line here means something broke during the churn. With the
# restart re-anchor seam fixed at the root, there is NO allow-list: the old
# `CompactionError::NonContiguous` residue can no longer occur, so ANY
# ERROR-level line fails the drill hard.
assert_no_unexpected_errors() {
  local errors count
  errors="$DRILL_WORKDIR/errors.log"
  grep 'ERROR' "$DRILL_WORKDIR/walrust.log" >"$errors" 2>/dev/null || true
  count=$(grep -c 'ERROR' "$errors" 2>/dev/null || true)
  count=${count:-0}
  if [ "$count" -gt 0 ]; then
    log "UNEXPECTED ERROR-level log lines ($count):"
    cat "$errors" >&2
    fail "SOAK LOG: $count ERROR-level line(s) during the soak — see $DRILL_WORKDIR/walrust.log"
  fi
  log "log scan clean: 0 ERROR-level lines across $kills_done kill/restart cycles"
}

drill_setup
create_db "$DRILL_DB"
append_rows "$DRILL_DB" 5 base
name=$(basename "$DRILL_DB" .db)

DRILL_COMPACTION_CONFIG="$DRILL_WORKDIR/compaction.toml"
cat >"$DRILL_COMPACTION_CONFIG" <<TOML
[compaction]
enabled = true
keep_fine_window = "$KEEP_FINE"
l1_batch = $L1_BATCH
l2_batch = $L2_BATCH
TOML

start_driver "$DRILL_DB" "$DRIVER_INTERVAL" soak-compact
wait_driver_count_at_least 20 45
start_walrust_compaction "$DRILL_DB"
wait_for_sync_progress 0

end_at=$((SECONDS + TOTAL_SECONDS))
kills_done=0
snapshots_seen_at_start=$(level_object_count '/[0-9a-f]{4}/0000000000000001-[0-9a-f]{16}\.ltx$')

while [ "$SECONDS" -lt "$end_at" ]; do
  remaining=$((end_at - SECONDS))
  this_cycle=$((remaining < KILL_EVERY ? remaining : KILL_EVERY))
  [ "$this_cycle" -gt 0 ] || break
  sustain_writes "$this_cycle"
  if [ "$SECONDS" -lt "$end_at" ]; then
    baseline=$(latest_txid)
    kill_walrust_pid_verified
    start_walrust_compaction "$DRILL_DB"
    wait_for_sync_progress "$baseline"
    kills_done=$((kills_done + 1))
    log "kill/restart cycle $kills_done complete (t=${SECONDS}s of ${TOTAL_SECONDS}s soak window)"
  fi
done

[ "$kills_done" -ge 4 ] || fail "SOAK: only $kills_done kill/restart cycles ran — need >= 4 inside ${TOTAL_SECONDS}s \
(reduce KILL_EVERY or increase TOTAL_SECONDS)"

pause_driver
expected=$(driver_count)
sustain_writes 3
snapshots_seen_at_end=$(level_object_count '/[0-9a-f]{4}/0000000000000001-[0-9a-f]{16}\.ltx$')
merges_l1=$(level_object_count '/levels/L1/.*\.ltx$')
merges_l2=$(level_object_count '/levels/L2/.*\.ltx$')

# (1) Restore-to-latest row-exact + integrity + exit 0.
#
# The still-running watcher keeps ticking its periodic snapshot timer; a couple
# of snapshot cadences of headroom lets an in-flight restart re-anchor land
# before we assert. With the seam fixed at the root, restore-to-latest must
# succeed row-exact -- any failure fails the drill for real.
DRILL_RESTORE_TIMEOUT=$((SNAPSHOT_INTERVAL * 6))
soak_wait_restore "$name" "$expected"

# (2) walrust verify exits 0 on the healthy leveled bucket.
"$WALRUST_BIN" verify "$name" --bucket "$DRILL_BUCKET_URI" --endpoint "$DRILL_ENDPOINT" \
  >"$DRILL_WORKDIR/verify.out" 2>&1 \
  || fail "walrust verify exited nonzero on a healthy soaked bucket: $(cat "$DRILL_WORKDIR/verify.out")"
log "verify exit 0"

# (3) Bounded object counts (no unbounded duplicate coverage).
assert_bucket_converged

# (4) No ERROR-level log lines from the soak itself.
assert_no_unexpected_errors

stop_driver
stop_walrust

log "SOAK SUMMARY: kills=$kills_done snapshots=$snapshots_seen_at_end (started at $snapshots_seen_at_start) \
L1=$merges_l1 L2=$merges_l2 rows=$expected"

# (5) Induced-failure proof (default ON — this is the drill's required teeth
# check, not an opt-in self-test): with the driver paused and walrust
# stopped, the bucket is frozen. Strip every snapshot base; the very next
# restore-to-latest MUST fall short of `expected` (nonzero / row diff). If it
# still matches, the row-diff guard above is toothless.
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
    fail "INDUCED LOSS did not break restore — the row-diff guard is toothless \
(restored $DRILL_LAST_ACTUAL rows == expected $expected after deleting every restore base)"
  fi
  log "induced-loss proof OK: restore correctly failed after the deletion \
(actual rows=${DRILL_LAST_ACTUAL:-unavailable}; error: ${DRILL_LAST_RESTORE_ERROR:-none})"
fi

log "PASS compaction-soak total_seconds=$TOTAL_SECONDS kills=$kills_done rows=$expected \
L1=$merges_l1 L2=$merges_l2 snapshots=$snapshots_seen_at_end"
