#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

drill_setup
create_db "$DRILL_DB"
append_rows "$DRILL_DB" 5 base
name=$(basename "$DRILL_DB" .db)
rss_log="$DRILL_WORKDIR/rss.tsv"

duration=${WALRUST_DRILL_SOAK_SECONDS:-600}
restart_every=${WALRUST_DRILL_SOAK_RESTART_SECONDS:-90}
sample_every=${WALRUST_DRILL_SOAK_SAMPLE_SECONDS:-15}

start_driver "$DRILL_DB" "${WALRUST_DRILL_DRIVER_INTERVAL:-0.05}" soak
wait_driver_count_at_least 8 45
start_walrust "$DRILL_DB"
wait_for_shadow_blocker "$DRILL_DB"

end_at=$((SECONDS + duration))
next_restart=$((SECONDS + restart_every))
next_sample=$SECONDS

while [ "$SECONDS" -lt "$end_at" ]; do
  if [ "$SECONDS" -ge "$next_sample" ]; then
    rss=$(ps -o rss= -p "$DRILL_WALRUST_PID" 2>/dev/null | tr -d ' ')
    printf '%s\t%s\t%s\n' "$SECONDS" "${rss:-0}" "$(driver_count)" >>"$rss_log"
    next_sample=$((SECONDS + sample_every))
  fi
  if [ "$SECONDS" -ge "$next_restart" ]; then
    restart_walrust_pid_verified "$DRILL_DB"
    wait_for_shadow_blocker "$DRILL_DB"
    # Pause the driver so the restore target is a fixed count, not a moving one,
    # then assert an exact row-for-row match (proving no data was lost across
    # the restart) before resuming load.
    pause_driver
    wait_restore_count "$name" "$(driver_count)"
    resume_driver
    next_restart=$((SECONDS + restart_every))
  fi
  if ! kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1; then
    fail "walrust exited during soak; see $DRILL_WORKDIR/walrust.log"
  fi
  sleep "$DRILL_POLL_INTERVAL"
done

pause_driver
expected=$(driver_count)
wait_restore_count "$name" "$expected"
stop_driver
stop_walrust

log "PASS soak seconds=$duration rows=$expected rss_samples=$rss_log"
