#!/usr/bin/env bash

set -Eeuo pipefail

DRILL_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

drills=(
  basic-lifecycle.sh
  restart-under-load.sh
  kill-mid-compaction.sh
  hostile-operator.sh
  pitr.sh
  prune-retained.sh
  read-replica.sh
  soak.sh
)

for drill in "${drills[@]}"; do
  printf '[drill] running %s\n' "$drill" >&2
  if ! "$DRILL_DIR/$drill"; then
    printf 'DRILL_FAILED: %s\n' "$drill" >&2
    exit 1
  fi
done

printf '[drill] PASS\n' >&2
