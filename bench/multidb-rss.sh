#!/usr/bin/env bash
#
# Multi-database RSS scaling: does memory stay flat or grow with database
# count? Runs walrust (one `watch` process, N databases via walrust.toml) and
# litestream (one `replicate` process, N databases in its yaml) sequentially
# against a local MinIO, sampling RSS over time for each (tool, N, shape)
# cell. The claim under test is constant-vs-linear scaling in database count.
#
# Measurement, not a gate. Every cell ends with the drill restore/row-diff
# validity check on a sample of its databases; a failed check aborts the run
# with a nonzero exit and no results.
#
# Knobs (env):
#   BENCH_DB_COUNTS               space-separated Ns (default "1 10 50")
#   BENCH_SHAPES                  subset of "idle steady bursty" (default steady)
#   BENCH_MULTIDB_SECONDS         measurement window per cell (default 120)
#   BENCH_MULTIDB_DRIVER_INTERVAL commit interval per driver (default 0.2
#                                 = 5 rows/s per database, a modest rate)
#   BENCH_SYNC_INTERVAL_SECONDS   sync interval for BOTH tools (default 1)
#   BENCH_RSS_SAMPLE_SECONDS      RSS sample cadence (default 5)
#   BENCH_BURST_ON_SECONDS / BENCH_BURST_OFF_SECONDS  bursty duty cycle (10/50)
#   BENCH_MINIO_PORT, BENCH_BUCKET, WALRUST_BIN       as in compare-litestream
#
# Results: bench/results-<utc>/ (gitignored): rss-<tool>-n<N>-<shape>.tsv raw
# samples, results.json, plain-text table on stdout.

set -Eeuo pipefail

BENCH_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WALRUST_BIN=${WALRUST_BIN:-"$(cd "$BENCH_SCRIPT_DIR/.." && pwd)/target/release/walrust"}
export WALRUST_BIN

# shellcheck source=drills/lib.sh
source "$BENCH_SCRIPT_DIR/../drills/lib.sh"
# shellcheck source=bench/common.sh
source "$BENCH_SCRIPT_DIR/common.sh"

BENCH_DB_COUNTS=${BENCH_DB_COUNTS:-"1 10 50"}
BENCH_SHAPES=${BENCH_SHAPES:-"steady"}
BENCH_MULTIDB_SECONDS=${BENCH_MULTIDB_SECONDS:-120}
BENCH_MULTIDB_DRIVER_INTERVAL=${BENCH_MULTIDB_DRIVER_INTERVAL:-0.2}
BENCH_SYNC_INTERVAL_SECONDS=${BENCH_SYNC_INTERVAL_SECONDS:-1}
BENCH_RSS_SAMPLE_SECONDS=${BENCH_RSS_SAMPLE_SECONDS:-5}
BENCH_BURST_ON_SECONDS=${BENCH_BURST_ON_SECONDS:-10}
BENCH_BURST_OFF_SECONDS=${BENCH_BURST_OFF_SECONDS:-50}
BENCH_MINIO_PORT=${BENCH_MINIO_PORT:-19000}
BENCH_BUCKET=${BENCH_BUCKET:-walrust-bench}

require_cmd docker
LITESTREAM_VERSION=$(ensure_litestream)
WALRUST_VERSION=$("$WALRUST_BIN" --version 2>/dev/null || echo "unknown")

RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)-$$
MINIO_NAME="walrust-bench-minio-$RUN_ID"
RESULTS_DIR=$(bench_results_dir)

CELL_DRIVER_PIDS=()
CELL_RSS_PID=

bench_cleanup() {
  set +e
  local pid
  for pid in ${CELL_DRIVER_PIDS[@]+"${CELL_DRIVER_PIDS[@]}"}; do
    stop_pid "$pid"
  done
  stop_pid "$CELL_RSS_PID"
  stop_litestream
  drill_cleanup
  # drill_cleanup's rm -rf can lose a race with a final buffered write on
  # macOS (ENOTEMPTY); a second sweep after everything is dead is silent.
  if [ -n "${DRILL_WORKDIR:-}" ]; then
    rm -rf "$DRILL_WORKDIR" 2>/dev/null
  fi
  stop_minio_container "$MINIO_NAME"
}

trap bench_cleanup EXIT INT TERM
start_minio_container "$MINIO_NAME" "$BENCH_MINIO_PORT" "$BENCH_BUCKET"

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-minioadmin}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-minioadmin}
export AWS_REGION=${AWS_REGION:-us-east-1}
export WALRUST_DRILL_BUCKET=$BENCH_BUCKET
export WALRUST_DRILL_ENDPOINT="http://127.0.0.1:$BENCH_MINIO_PORT"
export WALRUST_DRILL_PREFIX="bench/multidb-$RUN_ID"

drill_setup
trap bench_cleanup EXIT INT TERM

DRIVER_ROWS_PER_SEC=$(awk -v i="$BENCH_MULTIDB_DRIVER_INTERVAL" 'BEGIN { printf "%.1f", 1/i }')
cat <<HDR
============================================================
multi-database RSS scaling: walrust vs litestream
============================================================
walrust:    $WALRUST_VERSION ($WALRUST_BIN)
litestream: $LITESTREAM_VERSION ($(command -v litestream))
S3 target:  local MinIO (docker), endpoint $WALRUST_DRILL_ENDPOINT
db counts:  $BENCH_DB_COUNTS    shapes: $BENCH_SHAPES
per cell:   ${BENCH_MULTIDB_SECONDS}s, RSS sampled every ${BENCH_RSS_SAMPLE_SECONDS}s

matched knobs (set explicitly on both tools):
  sync interval   walrust: wal_sync_interval = $BENCH_SYNC_INTERVAL_SECONDS (walrust.toml)
                  litestream: replica.sync-interval: ${BENCH_SYNC_INTERVAL_SECONDS}s
  process model   ONE process watching all N databases for both tools
  workload        per shape: idle = write once before start, no live writes;
                  steady = ${DRIVER_ROWS_PER_SEC} rows/s per db; bursty =
                  ${BENCH_BURST_ON_SECONDS}s on / ${BENCH_BURST_OFF_SECONDS}s off at the same rate

known asymmetries:
  - walrust runs product defaults otherwise (--no-metrics --no-cache);
    litestream v0.5 always runs its built-in compaction monitor.
============================================================
HDR

run_cell() {
  local tool=$1
  local n=$2
  local shape=$3
  local cell="$tool-n$n-$shape"
  local celldir="$DRILL_WORKDIR/cell-$cell"
  local cell_prefix="$DRILL_RUN_PREFIX/$cell"
  local rss_file="$RESULTS_DIR/rss-$cell.tsv"
  local i db pad
  mkdir -p "$celldir"
  log "=== cell $cell ==="

  local dbs=()
  for i in $(seq 1 "$n"); do
    pad=$(printf 'db%03d' "$i")
    db="$celldir/$pad.db"
    create_db "$db" >/dev/null
    append_rows "$db" 5 base >/dev/null
    if [ "$shape" = "idle" ]; then
      append_rows "$db" 100 idle >/dev/null
    fi
    dbs+=("$db")
  done

  # Drivers (steady/bursty); idle has no live writers.
  CELL_DRIVER_PIDS=()
  if [ "$shape" != "idle" ]; then
    for db in "${dbs[@]}"; do
      spawn_driver "$db" "$db.count" "$db.pause" "$BENCH_MULTIDB_DRIVER_INTERVAL" "$cell" "$db.driver"
      CELL_DRIVER_PIDS+=("$SPAWNED_DRIVER_PID")
    done
  fi

  # Start the tool (single process, N databases).
  local tool_pid
  if [ "$tool" = "walrust" ]; then
    local toml="$celldir/walrust.toml"
    {
      printf '[sync]\n'
      printf 'wal_sync_interval = %s\n\n' "$BENCH_SYNC_INTERVAL_SECONDS"
      for db in "${dbs[@]}"; do
        printf '[[databases]]\n'
        printf 'path = "%s"\n\n' "$db"
      done
    } >"$toml"
    start_walrust_config "$toml" "s3://$BENCH_BUCKET/$cell_prefix" "$celldir/walrust.log"
    tool_pid=$DRILL_WALRUST_PID
    wait_for_shadow_blocker "${dbs[0]}"
  else
    local yaml="$celldir/litestream.yml"
    local pairs=()
    for db in "${dbs[@]}"; do
      pairs+=("$db:$cell_prefix/$(basename "$db" .db)")
    done
    write_litestream_config "$yaml" "$WALRUST_DRILL_ENDPOINT" "$BENCH_BUCKET" \
      "$BENCH_SYNC_INTERVAL_SECONDS" "${pairs[@]}"
    start_litestream "$yaml" "$celldir/litestream.log"
    tool_pid=$BENCH_LITESTREAM_PID
  fi

  start_rss_sampler "$tool_pid" "$BENCH_RSS_SAMPLE_SECONDS" "$rss_file"
  CELL_RSS_PID=$SPAWNED_RSS_SAMPLER_PID

  # Measurement window with liveness checks; bursty toggles pause files.
  local end_at=$((SECONDS + BENCH_MULTIDB_SECONDS))
  local burst_on=1
  local next_toggle=$((SECONDS + BENCH_BURST_ON_SECONDS))
  while [ "$SECONDS" -lt "$end_at" ]; do
    kill -0 "$tool_pid" >/dev/null 2>&1 || fail "$tool exited during cell $cell; see $celldir/$tool.log"
    if [ "$shape" = "bursty" ] && [ "$SECONDS" -ge "$next_toggle" ]; then
      if [ "$burst_on" = "1" ]; then
        for db in "${dbs[@]}"; do touch "$db.pause"; done
        burst_on=0
        next_toggle=$((SECONDS + BENCH_BURST_OFF_SECONDS))
      else
        for db in "${dbs[@]}"; do rm -f "$db.pause"; done
        burst_on=1
        next_toggle=$((SECONDS + BENCH_BURST_ON_SECONDS))
      fi
    fi
    sleep 1
  done

  # Quiesce all drivers, then validity-check a sample of databases (first,
  # middle, last) with restore + integrity + exact row count.
  if [ "$shape" != "idle" ]; then
    for db in "${dbs[@]}"; do touch "$db.pause"; done
  fi
  local sample_idx=(0)
  if [ "$n" -ge 3 ]; then sample_idx+=($((n / 2)) $((n - 1))); elif [ "$n" -eq 2 ]; then sample_idx+=(1); fi
  local idx expected name
  for idx in "${sample_idx[@]}"; do
    db="${dbs[$idx]}"
    if [ "$shape" != "idle" ]; then
      pause_driver_files "$db" "$db.pause"
    fi
    expected=$(db_count "$db")
    name=$(basename "$db" .db)
    if [ "$tool" = "walrust" ]; then
      local s_uri=$DRILL_BUCKET_URI
      DRILL_BUCKET_URI="s3://$BENCH_BUCKET/$cell_prefix"
      wait_restore_count "$name" "$expected"
      DRILL_BUCKET_URI=$s_uri
    else
      wait_litestream_restore_count "$celldir/litestream.yml" "$db" "$expected" "$celldir/restored-$name.db"
    fi
  done

  # Teardown: sampler, drivers, tool, cell objects and files.
  stop_pid "$CELL_RSS_PID"; CELL_RSS_PID=
  local pid
  for pid in ${CELL_DRIVER_PIDS[@]+"${CELL_DRIVER_PIDS[@]}"}; do
    stop_pid "$pid"
  done
  CELL_DRIVER_PIDS=()
  if [ "$tool" = "walrust" ]; then
    stop_walrust
  else
    stop_litestream
  fi
  s3_delete_prefix "$cell_prefix" >/dev/null 2>&1 || true
  rm -rf "$celldir"
  log "=== cell $cell done ==="
}

CELLS=()
for shape in $BENCH_SHAPES; do
  case "$shape" in
    idle|steady|bursty) ;;
    *) fail "unknown shape: $shape (expected idle|steady|bursty)" ;;
  esac
  for n in $BENCH_DB_COUNTS; do
    for tool in walrust litestream; do
      run_cell "$tool" "$n" "$shape"
      CELLS+=("$tool-n$n-$shape")
    done
  done
done

# --- report -------------------------------------------------------------------
RUN_ID="$RUN_ID" \
BENCH_DB_COUNTS="$BENCH_DB_COUNTS" \
BENCH_SHAPES="$BENCH_SHAPES" \
BENCH_MULTIDB_SECONDS="$BENCH_MULTIDB_SECONDS" \
BENCH_MULTIDB_DRIVER_INTERVAL="$BENCH_MULTIDB_DRIVER_INTERVAL" \
BENCH_SYNC_INTERVAL_SECONDS="$BENCH_SYNC_INTERVAL_SECONDS" \
BENCH_RSS_SAMPLE_SECONDS="$BENCH_RSS_SAMPLE_SECONDS" \
BENCH_BURST_ON_SECONDS="$BENCH_BURST_ON_SECONDS" \
BENCH_BURST_OFF_SECONDS="$BENCH_BURST_OFF_SECONDS" \
WALRUST_VERSION="$WALRUST_VERSION" \
LITESTREAM_VERSION="$LITESTREAM_VERSION" \
CELLS="${CELLS[*]}" \
  "$DRILL_PYTHON" - "$RESULTS_DIR" <<'PY'
import json, os, sys
run_dir = sys.argv[1]
e = os.environ
meta = {
    "run_id": e["RUN_ID"],
    "db_counts": [int(x) for x in e["BENCH_DB_COUNTS"].split()],
    "shapes": e["BENCH_SHAPES"].split(),
    "cell_duration_seconds": int(e["BENCH_MULTIDB_SECONDS"]),
    "driver_commit_interval_seconds": float(e["BENCH_MULTIDB_DRIVER_INTERVAL"]),
    "sync_interval_seconds": int(e["BENCH_SYNC_INTERVAL_SECONDS"]),
    "rss_sample_seconds": int(e["BENCH_RSS_SAMPLE_SECONDS"]),
    "burst_on_seconds": int(e["BENCH_BURST_ON_SECONDS"]),
    "burst_off_seconds": int(e["BENCH_BURST_OFF_SECONDS"]),
    "versions": {"walrust": e["WALRUST_VERSION"], "litestream": e["LITESTREAM_VERSION"]},
    "cells": e["CELLS"].split(),
    "validity": "restore + integrity_check + exact row-count match passed on sampled dbs in every cell",
}
with open(os.path.join(run_dir, "run-meta.json"), "w") as fh:
    json.dump(meta, fh, indent=2)
PY

"$DRILL_PYTHON" "$BENCH_SCRIPT_DIR/report-multidb.py" "$RESULTS_DIR" | tee "$RESULTS_DIR/report.txt"

log "PASS multidb-rss results in $RESULTS_DIR"
