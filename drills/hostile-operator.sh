#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

drill_setup
create_db "$DRILL_DB"
append_rows "$DRILL_DB" 5 base

name=$(basename "$DRILL_DB" .db)
start_driver "$DRILL_DB" "${WALRUST_DRILL_DRIVER_INTERVAL:-0.04}" hostile
wait_driver_count_at_least 8 45
start_walrust "$DRILL_DB"
wait_for_shadow_blocker "$DRILL_DB"
wait_driver_count_at_least 20 45

checkpoint_result=$(force_truncate_checkpoint "$DRILL_DB")
log "running external truncate checkpoint result=$checkpoint_result"

pause_driver
wait_restore_count "$name" "$(driver_count)"
stop_driver
stop_walrust

checkpoint_result=$(force_truncate_checkpoint "$DRILL_DB")
log "stopped external truncate checkpoint result=$checkpoint_result"
start_walrust "$DRILL_DB"
wait_for_shadow_blocker "$DRILL_DB"
append_rows "$DRILL_DB" 5 after-restart
wait_restore_count "$name" "$(db_count "$DRILL_DB")"
stop_walrust

log "PASS hostile operator rows=$(db_count "$DRILL_DB")"
