#!/usr/bin/env bash

set -Eeuo pipefail

DRILL_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

run_one() {
  local script=$1
  printf '[basic_e2e] running %s\n' "$script" >&2
  "$DRILL_DIR/$script"
}

WALRUST_DRILL_RESTART_CYCLES=1 run_one basic-lifecycle.sh
WALRUST_DRILL_RESTART_CYCLES=1 run_one restart-under-load.sh
run_one compact-retained.sh

printf '[basic_e2e] PASS\n' >&2
