#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

drill_setup
create_db "$DRILL_DB"
append_rows "$DRILL_DB" 5 base
start_driver "$DRILL_DB" "${WALRUST_DRILL_DRIVER_INTERVAL:-0.04}" lifecycle
wait_driver_count_at_least 8 45
start_walrust "$DRILL_DB"
wait_for_shadow_blocker "$DRILL_DB"
wait_driver_count_at_least "${WALRUST_DRILL_LIFECYCLE_ROWS:-25}" 45

pause_driver
expected=$(driver_count)
wait_restore_count "$(basename "$DRILL_DB" .db)" "$expected"
stop_driver
stop_walrust

log "PASS basic lifecycle rows=$expected"
