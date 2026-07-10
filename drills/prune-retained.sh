#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

drill_setup
create_db "$DRILL_DB"
name=$(basename "$DRILL_DB" .db)
history="$DRILL_WORKDIR/snapshot-history.tsv"

for rows in 4 4 4 4; do
  append_rows "$DRILL_DB" "$rows" snapshot
  run_snapshot "$DRILL_DB" >/dev/null
  txid=$(latest_txid)
  count=$(db_count "$DRILL_DB")
  [ "$txid" -gt 0 ] || fail "snapshot did not create a discoverable TXID"
  printf '%s\t%s\n' "$txid" "$count" >>"$history"
done

run_prune "$name" >"$DRILL_WORKDIR/prune.out" 2>&1

retained="$DRILL_WORKDIR/retained-txids.txt"
snapshot_txids >"$retained"
[ -s "$retained" ] || fail "prune left no retained snapshots"

while read -r txid; do
  expected=$(awk -F '\t' -v txid="$txid" '$1 == txid { print $2; found = 1 } END { if (!found) exit 1 }' "$history") \
    || fail "retained TXID $txid was not in the recorded history"
  wait_restore_count "$name" "$expected" --point-in-time "$txid"
done <"$retained"

# ── Leveled phase (gap 5): prune must respect the level watermark too ──────
#
# Runs on its OWN, separate database (not $DRILL_DB) rather than continuing
# the flat phase's history: prune's reachability rescue (plan_legacy_prune)
# always keeps whichever snapshot is needed as a base for the earliest
# surviving flat gen-0 incremental, AND every snapshot -- including old
# flat-phase ones -- "consumes" its own seq (a permanent hole in the gen-0
# numbering only that exact snapshot object can ever fill), so reusing
# $DRILL_DB would let the flat phase's own old, no-longer-relevant snapshots
# keep anchoring the watermark down near their own low TXIDs forever,
# making the level assertions below vacuous (nothing ever prunable). A fresh
# database has no such residue: whatever snapshot the reachability rule and
# GFS retention naturally settle on will be a real, current one.
#
# A real compacting `walrust watch --independent-tasks` (aggressive batches)
# folds L0 into levels/L1 (and usually L2) while a write driver runs. A
# hand-recorded snapshot (captured from the watcher's own periodic timer,
# not a manual CLI call -- see below) anchors a reliable retained-PITR
# target. Then prune runs with the same policy the flat phase used above,
# and the watermark rule (crates/walrust-core/src/compaction/prune.rs) is
# checked directly against a before/after level-object listing: a level
# object whose whole range ends below the oldest SURVIVING snapshot's txid
# must be deleted unless it is the newest object at its level; nothing at or
# above the watermark, and never the newest-per-level object, may be
# deleted. Finally, restore-to-latest and the hand-recorded PITR point must
# both still be row-exact.
LEVEL_DB="$DRILL_WORKDIR/leveled.db"
create_db "$LEVEL_DB"
append_rows "$LEVEL_DB" 5 leveled-base
level_name=$(basename "$LEVEL_DB" .db)
DRILL_LEVEL_CONFIG="$DRILL_WORKDIR/level-compaction.toml"
LEVEL_SNAPSHOT_INTERVAL=${WALRUST_DRILL_LEVEL_SNAPSHOT_INTERVAL:-8}
cat >"$DRILL_LEVEL_CONFIG" <<TOML
[compaction]
enabled = true
keep_fine_window = "0s"
l1_batch = 4
l2_batch = 3
TOML

start_walrust_leveled() {
  local db=$1
  "$WALRUST_BIN" watch "$db" \
    --config "$DRILL_LEVEL_CONFIG" \
    --independent-tasks \
    --bucket "$DRILL_BUCKET_URI" \
    --endpoint "$DRILL_ENDPOINT" \
    --snapshot-interval "$LEVEL_SNAPSHOT_INTERVAL" \
    --wal-sync-interval "${WALRUST_DRILL_WAL_SYNC_INTERVAL:-1}" \
    --checkpoint-interval 999999 \
    --on-startup true \
    --no-metrics \
    --no-cache \
    >"$DRILL_WORKDIR/walrust-leveled.log" 2>&1 &
  DRILL_WALRUST_PID=$!
  log "walrust (leveled) pid=$DRILL_WALRUST_PID"
  wait_process_alive "$DRILL_WALRUST_PID" "walrust watch --config (leveled)"
}

wait_for_leveled_sync_progress() {
  local baseline=$1
  local deadline=$((SECONDS + 45))
  local cur
  while [ "$SECONDS" -lt "$deadline" ]; do
    cur=$(latest_txid)
    if [ "${cur:-0}" -gt "$baseline" ]; then
      return 0
    fi
    kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
      || fail "walrust exited before making sync progress; see $DRILL_WORKDIR/walrust-leveled.log"
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "walrust made no sync progress past TXID $baseline within timeout"
}

# Print "<level>\t<min-hex>\t<max-hex>\t<key>" for every levels/L*/ object
# belonging to $level_name specifically. $DRILL_RUN_PREFIX holds BOTH the
# flat phase's db ($name) and this phase's db ($level_name); the flat phase
# never runs a compacting watcher so it can never produce levels/ objects,
# but scope explicitly anyway rather than relying on that.
level_files() {
  s3_list_prefix "$DRILL_RUN_PREFIX/$level_name/" | awk -F'/' '
    /\/levels\/L[0-9]+\// {
      level = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^L[0-9]+$/) { level = $i }
      }
      fn = $NF
      sub(/\.ltx$/, "", fn)
      split(fn, mm, "-")
      printf "%s\t%s\t%s\t%s\n", level, mm[1], mm[2], $0
    }
  '
}

# Same shape as lib.sh's snapshot_txids, but scoped to $level_name: the flat
# phase's db lives under the SAME $DRILL_RUN_PREFIX, and its old snapshots
# (irrelevant to this phase, but still real objects) would otherwise pollute
# a prefix-wide listing and could get picked up as a spuriously "ancient"
# minimum, breaking the watermark computation below.
level_snapshot_txids() {
  local keys="$DRILL_WORKDIR/level-snapshot-keys.txt"
  s3_list_prefix "$DRILL_RUN_PREFIX/$level_name/" >"$keys"
  "$DRILL_PYTHON" - "$keys" <<'PY'
import re
import sys
vals = set()
with open(sys.argv[1], encoding="utf-8") as handle:
  for key in handle:
    key = key.strip()
    m = re.search(r'/([0-9a-f]{4})/([0-9a-f]{16})-([0-9a-f]{16})\.ltx$', key)
    if not m:
        continue
    generation = int(m.group(1), 16)
    min_txid = int(m.group(2), 16)
    max_txid = int(m.group(3), 16)
    if generation != 0 and min_txid == 1:
        vals.add(max_txid)
for txid in sorted(vals):
    print(txid)
PY
}

# Same shape as kill-mid-compaction.sh's snapshot_base_objects, scoped to
# $level_name: full keys (not just txids) for every snapshot base object.
level_snapshot_base_objects() {
  s3_list_prefix "$DRILL_RUN_PREFIX/$level_name/" \
    | grep -E '/[0-9a-f]{4}/0000000000000001-[0-9a-f]{16}\.ltx$' \
    | grep -v '/0000/'
}

# lib.sh's pause_driver / driver_count are hardcoded to $DRILL_DB (every
# other drill only ever has one database). This phase's driver targets
# $LEVEL_DB instead, so pause_driver's stability check would silently
# compare $DRILL_DB (static, unchanging since phase 1 finished writing to
# it) against itself -- trivially "stable" on the first read -- and stamp a
# stale count into $DRILL_DRIVER_COUNT that has nothing to do with the
# leveled phase's actual driver activity. Local, $LEVEL_DB-aware
# replacements; resume_driver/stop_driver are safe to reuse as-is (no
# $DRILL_DB reference matters for what this phase needs from them).
level_pause_driver() {
  [ -n "${DRILL_DRIVER_PID:-}" ] || fail "write driver is not running"
  touch "$DRILL_DRIVER_PAUSE"
  local before after
  before=$(db_count "$LEVEL_DB")
  while :; do
    sleep "$DRILL_POLL_INTERVAL"
    after=$(db_count "$LEVEL_DB")
    if [ "$after" = "$before" ]; then
      printf '%s\n' "$after" >"$DRILL_DRIVER_COUNT"
      return 0
    fi
    before=$after
  done
}

level_driver_count() {
  if [ -f "$DRILL_DRIVER_COUNT" ]; then
    cat "$DRILL_DRIVER_COUNT"
  else
    db_count "$LEVEL_DB"
  fi
}

level_wait_driver_count_at_least() {
  local min=$1
  local timeout=${2:-30}
  local deadline=$((SECONDS + timeout))
  local count
  while [ "$SECONDS" -lt "$deadline" ]; do
    count=$(db_count "$LEVEL_DB")
    printf '%s\n' "$count" >"$DRILL_DRIVER_COUNT"
    if [ "$count" -ge "$min" ]; then
      return 0
    fi
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "timed out waiting for leveled-phase driver count >= $min; got $(level_driver_count)"
}

baseline_txid=$(latest_txid)
start_driver "$LEVEL_DB" "${WALRUST_DRILL_DRIVER_INTERVAL:-0.05}" prune-level
level_wait_driver_count_at_least 10 45
start_walrust_leveled "$LEVEL_DB"
wait_for_leveled_sync_progress "$baseline_txid"

l1_deadline=$((SECONDS + 90))
l1_seen=0
while [ "$SECONDS" -lt "$l1_deadline" ]; do
  if [ -n "$(level_files | awk -F'\t' '$1=="L1"{print; exit}')" ]; then
    l1_seen=1
    break
  fi
  kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
    || fail "walrust exited before any L1 merge; see $DRILL_WORKDIR/walrust-leveled.log"
  sleep "$DRILL_POLL_INTERVAL"
done
[ "$l1_seen" -eq 1 ] || fail "L1 never fired in the leveled phase"

# Also wait for L2 (not just L1): the watermark test below needs compaction
# to have advanced WELL past the very first snapshot's immediate
# neighborhood, not just folded one batch. A lone L0 "straddler" right after
# a snapshot (a seq-contiguous run of length 1) is skipped by the liveness
# rule until enough MORE seq-contiguous objects accumulate after it to form
# a full batch -- L2 firing (which needs l2_batch=3 separate L1 merges) is a
# generous, real proof that compaction has churned through many batches, not
# just the first one, so the earliest live incremental has advanced and the
# reachability rescue no longer needs to anchor on the very first snapshot.
l2_deadline=$((SECONDS + 90))
l2_seen=0
while [ "$SECONDS" -lt "$l2_deadline" ]; do
  if [ -n "$(level_files | awk -F'\t' '$1=="L2"{print; exit}')" ]; then
    l2_seen=1
    break
  fi
  kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
    || fail "walrust exited before any L2 merge; see $DRILL_WORKDIR/walrust-leveled.log"
  sleep "$DRILL_POLL_INTERVAL"
done
[ "$l2_seen" -eq 1 ] || fail "L2 never fired in the leveled phase"

# Deliberate, hand-recorded snapshot (exact ground truth), WITHOUT ever
# restarting the watcher: a watcher restart under --independent-tasks is a
# known, separately-documented trigger for a pre-existing, non-compaction-
# specific chain-continuity race (see the "compaction-soak" drill / ROADMAP
# residue on gap 3 -- ~50%-per-run observed in that drill's much more
# aggressive restart loop). This phase only starts and stops the watcher
# ONCE, so it deliberately avoids that trigger: instead of stopping the
# watcher to take a one-shot manual snapshot, wait (driver paused, watcher
# still running) for the watcher's OWN periodic snapshot timer to produce a
# NEW snapshot, and record its row count at that exact, stable moment.
baseline_snap_txid=$(level_snapshot_txids | sort -n | tail -1)
baseline_snap_txid=${baseline_snap_txid:-0}
level_pause_driver
snap_deadline=$((SECONDS + LEVEL_SNAPSHOT_INTERVAL * 3 + 30))
level_txid=0
while [ "$SECONDS" -lt "$snap_deadline" ]; do
  cur=$(level_snapshot_txids | sort -n | tail -1)
  cur=${cur:-0}
  if [ "$cur" -gt "$baseline_snap_txid" ]; then
    level_txid=$cur
    break
  fi
  kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
    || fail "walrust exited before a new periodic snapshot landed; see $DRILL_WORKDIR/walrust-leveled.log"
  sleep "$DRILL_POLL_INTERVAL"
done
[ "$level_txid" -gt 0 ] || fail "no new periodic snapshot landed in the leveled phase"
snapshot_expected=$(level_driver_count)
printf '%s\t%s\n' "$level_txid" "$snapshot_expected" >>"$history"
log "leveled-phase snapshot txid=$level_txid rows=$snapshot_expected"

# Resume writing briefly so "restore-to-latest" below lands strictly AFTER
# the hand-recorded PITR point above -- otherwise the two final assertions
# would trivially coincide instead of testing genuinely different points.
# The watcher is still the SAME running process throughout.
resume_driver
sleep 3
level_pause_driver
level_expected=$(level_driver_count)

# Confirm the still-running watcher has actually synced up to this exact
# count BEFORE stopping it (matching kill-mid-compaction.sh's own ordering:
# restore-confirm, THEN stop) -- stopping first risks losing whatever
# committed rows landed after the watcher's last sync tick but before the
# kill, which would show up later as a bogus "chain gap", not a real one.
wait_restore_count "$level_name" "$level_expected"
stop_walrust

# Ensure the leveled phase's OWN oldest snapshot doesn't survive prune purely
# via analyze_retention's hard-coded "minimum 2 total" safety floor (hadb-io
# RetentionPolicy::minimum -- not exposed by any CLI flag). Every snapshot in
# this drill lands inside one real clock hour, so GFS hourly bucketing always
# collapses to a single bucket, contributing only ONE entry (the newest) to
# the keep set; the minimum-2 floor then pads by walking ALL snapshots
# ascending by sequence and adding the ABSOLUTE OLDEST -- deterministically
# the on-startup snapshot, taken before any compaction happened. Keeping it
# unconditionally would forever anchor the watermark at the very start of
# history, where by definition no level object (which exists only for
# FOLDED, i.e. later, history) can ever be "below" it -- a structural
# property of any sub-hour test run, not something --hourly/--daily/--weekly/
# --monthly tuning can avoid. Deleting every snapshot except the newest two
# directly (still satisfying the very same minimum-2 floor, just with two
# RECENT survivors) is safe here specifically because compaction has already
# folded well past the early history (L1 AND L2 both fired above), so
# nothing needs an ancient base to restore anymore.
all_level_snapshots="$DRILL_WORKDIR/all-level-snapshot-bases.txt"
level_snapshot_base_objects >"$all_level_snapshots"
[ -s "$all_level_snapshots" ] || fail "no snapshot base objects found before the leveled prune"
# The newest two by construction satisfy the minimum-2 floor with recent
# entries; $level_txid is included explicitly and unconditionally too --
# more periodic snapshots can land after it was captured (during the
# resume/sleep/wait_restore_count/stop_walrust steps above), so by the time
# this runs it may no longer BE one of the newest two, and it is the exact
# point the retained-PITR assertion at the end needs to survive.
keep_snapshot_txids=" $level_txid $(level_snapshot_txids | sort -rn | head -2 | tr '\n' ' ')"
while IFS= read -r key; do
  [ -n "$key" ] || continue
  key_txid_hex=$(printf '%s\n' "$key" | grep -oE '[0-9a-f]{16}\.ltx$' | sed 's/\.ltx$//')
  key_txid=$((16#$key_txid_hex))
  case "$keep_snapshot_txids" in
  *" $key_txid "*) continue ;; # the recorded PITR point or one of the two newest -- keep it
  esac
  s3_delete_key "$key"
done <"$all_level_snapshots"

# Capture the level listing right before pruning (after all the object
# creation above is done), so the before/after diff below only ever reflects
# what PRUNE itself deleted -- never objects created between the snapshots.
before_levels="$DRILL_WORKDIR/levels-before.tsv"
level_files | sort >"$before_levels"
[ -s "$before_levels" ] || fail "no level objects found before the leveled prune"

run_prune "$level_name" >"$DRILL_WORKDIR/prune-leveled.out" 2>&1

after_levels="$DRILL_WORKDIR/levels-after.tsv"
level_files | sort >"$after_levels"

retained_after="$DRILL_WORKDIR/retained-after-level-prune.txt"
level_snapshot_txids >"$retained_after"
[ -s "$retained_after" ] || fail "leveled prune left no retained snapshots"
watermark=$(sort -n "$retained_after" | head -1)
log "watermark (oldest surviving snapshot txid) = $watermark"

level_prune_check="$DRILL_WORKDIR/level-prune-check.py"
cat >"$level_prune_check" <<'PY'
import sys

before_path, after_path, watermark = sys.argv[1], sys.argv[2], int(sys.argv[3])


def load(path):
    files = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            level, mn, mx, key = line.split("\t")
            files[key] = (level, int(mn, 16), int(mx, 16))
    return files


before = load(before_path)
after = load(after_path)

extra = set(after) - set(before)
if extra:
    print(f"FAIL: prune-leveled produced NEW level objects: {extra}")
    sys.exit(1)

newest_per_level = {}
for key, (level, _mn, mx) in before.items():
    if level not in newest_per_level or mx > newest_per_level[level][1]:
        newest_per_level[level] = (key, mx)

deleted = set(before) - set(after)
if not deleted:
    print("FAIL: prune-leveled deleted nothing -- the watermark rule was never exercised")
    sys.exit(1)

for key, (level, _mn, mx) in before.items():
    is_newest = newest_per_level[level][0] == key
    was_deleted = key in deleted
    if is_newest and was_deleted:
        print(f"FAIL: newest object at level {level} was deleted: {key}")
        sys.exit(1)
    if mx >= watermark and was_deleted:
        print(f"FAIL: watermark-straddling/above object was deleted: {key} (max={mx:x} watermark={watermark:x})")
        sys.exit(1)
    if mx < watermark and not is_newest and not was_deleted:
        print(f"FAIL: object below watermark and not newest at its level survived: {key} (max={mx:x} watermark={watermark:x})")
        sys.exit(1)

print(f"OK: {len(deleted)} deleted, {len(after)} retained, watermark={watermark:x}")
PY

"$DRILL_PYTHON" "$level_prune_check" "$before_levels" "$after_levels" "$watermark" \
  || fail "level-aware prune watermark check failed"

# Restore-to-latest and the hand-recorded retained PITR point must both still
# be row-exact after the leveled prune.
wait_restore_count "$level_name" "$level_expected"
wait_restore_count "$level_name" "$snapshot_expected" --point-in-time "$level_txid"

stop_driver

log "PASS prune retained txids=$(tr '\n' ' ' <"$retained") leveled_watermark=$watermark leveled_rows=$level_expected"
