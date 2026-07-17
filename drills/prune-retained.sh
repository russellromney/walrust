#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

drill_setup
create_db "$DRILL_DB"
name=$(basename "$DRILL_DB" .db)
WALRUST_DRILL_SNAPSHOT_INTERVAL=${WALRUST_DRILL_SNAPSHOT_INTERVAL:-2}
start_walrust "$DRILL_DB"

for batch in 4 4 4 4; do
  append_rows "$DRILL_DB" "$batch" snapshot
  wait_restore_count "$name" "$(db_count "$DRILL_DB")"
  sleep "$((WALRUST_DRILL_SNAPSHOT_INTERVAL + 1))"
done

# Freeze publication before taking the before-prune inventory. Otherwise the
# periodic snapshot timer can publish a newer snapshot while this drill is
# restoring the earlier points; prune can legitimately retain that newcomer,
# and the test would then compare it against a stale history file.
stop_walrust
expected_latest=$(db_count "$DRILL_DB")
snapshots_before="$DRILL_WORKDIR/snapshots-before.txt"
snapshot_txids >"$snapshots_before"
[ -s "$snapshots_before" ] || fail "native watch produced no published snapshot"

history="$DRILL_WORKDIR/snapshot-history.tsv"
while read -r seq; do
  out="$DRILL_WORKDIR/before-$seq.db"
  run_restore_to "$name" "$out" --point-in-time "$seq"
  integrity_ok "$out"
  printf '%s\t%s\n' "$seq" "$(db_count "$out")" >>"$history"
done <"$snapshots_before"

run_prune "$name" >"$DRILL_WORKDIR/prune.out" 2>&1

retained="$DRILL_WORKDIR/snapshots-after.txt"
snapshot_txids >"$retained"
[ -s "$retained" ] || fail "native prune deleted every snapshot base"

while read -r seq; do
  expected=$(awk -F '\t' -v seq="$seq" '$1 == seq { print $2; found = 1 } END { if (!found) exit 1 }' "$history") \
    || fail "retained native snapshot seq $seq was absent before prune"
  wait_restore_count "$name" "$expected" --point-in-time "$seq"
done <"$retained"

wait_restore_count "$name" "$expected_latest"
log "PASS native prune retained $(tr '\n' ' ' <"$retained") and latest rows=$expected_latest"
