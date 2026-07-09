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

run_compact "$name" >"$DRILL_WORKDIR/compact.out" 2>&1

retained="$DRILL_WORKDIR/retained-txids.txt"
snapshot_txids >"$retained"
[ -s "$retained" ] || fail "compact left no retained snapshots"

while read -r txid; do
  expected=$(awk -F '\t' -v txid="$txid" '$1 == txid { print $2; found = 1 } END { if (!found) exit 1 }' "$history") \
    || fail "retained TXID $txid was not in the recorded history"
  wait_restore_count "$name" "$expected" --point-in-time "$txid"
done <"$retained"

log "PASS compact retained txids=$(tr '\n' ' ' <"$retained")"
