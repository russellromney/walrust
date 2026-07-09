#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

drill_setup
create_db "$DRILL_DB"
append_rows "$DRILL_DB" 5 base

name=$(basename "$DRILL_DB" .db)
history="$DRILL_WORKDIR/pitr-history.tsv"

run_snapshot "$DRILL_DB" >/dev/null
wait_restore_count "$name" 5
printf '%s\t%s\n' "$(latest_txid)" 5 >>"$history"

append_rows "$DRILL_DB" 6 mid
run_snapshot "$DRILL_DB" >/dev/null
wait_restore_count "$name" 11
printf '%s\t%s\n' "$(latest_txid)" 11 >>"$history"

append_rows "$DRILL_DB" 7 late
run_snapshot "$DRILL_DB" >/dev/null
wait_restore_count "$name" 18
printf '%s\t%s\n' "$(latest_txid)" 18 >>"$history"

while IFS=$'\t' read -r txid expected; do
  [ "$txid" -gt 0 ] || fail "did not discover a TXID for expected rows=$expected"
  wait_restore_count "$name" "$expected" --point-in-time "$txid"
done <"$history"

future=$(( $(latest_txid) + 1000 ))
expect_future_txid_error "$name" "$future"

log "PASS pitr history=$(tr '\n' ' ' <"$history")"
