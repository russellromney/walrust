#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

drill_setup
create_db "$DRILL_DB"
append_rows "$DRILL_DB" 5 base

cycles=${WALRUST_DRILL_RESTART_CYCLES:-3}
start_driver "$DRILL_DB" "${WALRUST_DRILL_DRIVER_INTERVAL:-0.04}" restart
wait_driver_count_at_least 8 45
start_walrust "$DRILL_DB"
wait_for_shadow_blocker "$DRILL_DB"

for i in $(seq 1 "$cycles"); do
  wait_driver_count_at_least $((5 + i * 8)) 45
  restart_walrust_pid_verified "$DRILL_DB"
  wait_for_shadow_blocker "$DRILL_DB"
done

pause_driver
expected=$(driver_count)
wait_restore_count "$(basename "$DRILL_DB" .db)" "$expected"
stop_driver
stop_walrust

log "PASS restart under load cycles=$cycles rows=$expected"
