#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

drill_setup
create_db "$DRILL_DB"
append_rows "$DRILL_DB" 5 base
name=$(basename "$DRILL_DB" .db)
replica="$DRILL_WORKDIR/replica.db"

start_driver "$DRILL_DB" "${WALRUST_DRILL_DRIVER_INTERVAL:-0.05}" replica
wait_driver_count_at_least 8 45
start_walrust "$DRILL_DB"
wait_for_shadow_blocker "$DRILL_DB"
# The driver is still writing here, so the live count is a moving target an
# exact restore would race past. Just confirm walrust has published a
# restorable snapshot beyond the initial rows before attaching the replica;
# the exact-count convergence checks happen below once the driver is paused.
wait_restore_count_at_least "$name" 8
start_replica "$DRILL_BUCKET_URI/$name" "$replica"

deadline=$((SECONDS + ${WALRUST_DRILL_REPLICA_SECONDS:-45}))
while [ "$SECONDS" -lt "$deadline" ]; do
  if [ -f "$replica" ]; then
    integrity_ok "$replica"
  fi
  if [ -n "${DRILL_REPLICA_PID:-}" ] && ! kill -0 "$DRILL_REPLICA_PID" >/dev/null 2>&1; then
    fail "replica exited early; see $DRILL_WORKDIR/replica.log"
  fi
  sleep "$DRILL_POLL_INTERVAL"
done

pause_driver
expected=$(driver_count)
wait_restore_count "$name" "$expected"
wait_replica_count "$replica" "$expected" 90
stop_driver
stop_replica
stop_walrust

log "PASS read replica rows=$expected"
