#!/usr/bin/env bash
#
# Honest head-to-head: walrust vs litestream replicating the same workload to
# the same local MinIO, with server-side request auditing (mc admin trace),
# replication-lag sentinels, and RSS sampling.
#
# Measurement, not a gate: this script never runs in PR CI. It still ends
# with the drill restore/row-diff validity check for BOTH tools and aborts
# (nonzero, no results) if either fails — a benchmark of a broken sync must
# not produce numbers.
#
# Knobs (env):
#   BENCH_DURATION_SECONDS        measurement window (default 300)
#   BENCH_SYNC_INTERVAL_SECONDS   sync interval set on BOTH tools (default 1)
#   BENCH_DRIVER_INTERVAL         seconds between driver commits (default 0.05
#                                 = 20 rows/s per database)
#   BENCH_SENTINEL_PERIOD_SECONDS lag sentinel cadence (default 10)
#   BENCH_RSS_SAMPLE_SECONDS      RSS sample cadence (default 5)
#   BENCH_MINIO_PORT              host port for the MinIO container (19000)
#   BENCH_BUCKET                  bucket name inside the container
#   WALRUST_BIN                   walrust binary (default ../target/release)
#
# Results: bench/results-<utc>/ (gitignored): plain-text table on stdout,
# results.json + raw tsv/trace inputs in the directory.

set -Eeuo pipefail

BENCH_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WALRUST_BIN=${WALRUST_BIN:-"$(cd "$BENCH_SCRIPT_DIR/.." && pwd)/target/release/walrust"}
export WALRUST_BIN

# shellcheck source=drills/lib.sh
source "$BENCH_SCRIPT_DIR/../drills/lib.sh"
# shellcheck source=bench/common.sh
source "$BENCH_SCRIPT_DIR/common.sh"

BENCH_DURATION_SECONDS=${BENCH_DURATION_SECONDS:-300}
BENCH_SYNC_INTERVAL_SECONDS=${BENCH_SYNC_INTERVAL_SECONDS:-1}
BENCH_DRIVER_INTERVAL=${BENCH_DRIVER_INTERVAL:-0.05}
BENCH_SENTINEL_PERIOD_SECONDS=${BENCH_SENTINEL_PERIOD_SECONDS:-10}
BENCH_RSS_SAMPLE_SECONDS=${BENCH_RSS_SAMPLE_SECONDS:-5}
BENCH_MINIO_PORT=${BENCH_MINIO_PORT:-19000}
BENCH_BUCKET=${BENCH_BUCKET:-walrust-bench}
# walrust non-sync knobs are kept at product defaults for honesty:
BENCH_WALRUST_SNAPSHOT_INTERVAL=${BENCH_WALRUST_SNAPSHOT_INTERVAL:-3600}
BENCH_WALRUST_CHECKPOINT_INTERVAL=${BENCH_WALRUST_CHECKPOINT_INTERVAL:-60}

require_cmd docker
LITESTREAM_VERSION=$(ensure_litestream)
WALRUST_VERSION=$("$WALRUST_BIN" --version 2>/dev/null || echo "unknown")

RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)-$$
MINIO_NAME="walrust-bench-minio-$RUN_ID"
TRACE_NAME="walrust-bench-trace-$RUN_ID"

RESULTS_DIR=$(bench_results_dir)

# --- process/container bookkeeping for cleanup -----------------------------
LS_DRIVER_PID=
W_RSS_PID=
LS_RSS_PID=
W_PROBE_PID=
LS_PROBE_PID=
TRACE_STOPPED=0

bench_cleanup() {
  set +e
  stop_pid "$W_PROBE_PID"
  stop_pid "$LS_PROBE_PID"
  stop_pid "$W_RSS_PID"
  stop_pid "$LS_RSS_PID"
  stop_pid "$LS_DRIVER_PID"
  stop_litestream
  stop_driver
  drill_cleanup
  # drill_cleanup's rm -rf can lose a race with a final buffered write on
  # macOS (ENOTEMPTY); a second sweep after everything is dead is silent.
  if [ -n "${DRILL_WORKDIR:-}" ]; then
    rm -rf "$DRILL_WORKDIR" 2>/dev/null
  fi
  if [ "$TRACE_STOPPED" != "1" ]; then
    docker rm -f "$TRACE_NAME" >/dev/null 2>&1
  fi
  stop_minio_container "$MINIO_NAME"
}

# --- MinIO + trace ----------------------------------------------------------
# Trap before starting containers so an early failure still tears them down;
# drill_setup installs its own trap, so re-arm ours right after it.
trap bench_cleanup EXIT INT TERM
start_minio_container "$MINIO_NAME" "$BENCH_MINIO_PORT" "$BENCH_BUCKET"
start_minio_trace "$TRACE_NAME" "$MINIO_NAME"

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-minioadmin}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-minioadmin}
export AWS_REGION=${AWS_REGION:-us-east-1}
export WALRUST_DRILL_BUCKET=$BENCH_BUCKET
export WALRUST_DRILL_ENDPOINT="http://127.0.0.1:$BENCH_MINIO_PORT"
export WALRUST_DRILL_PREFIX="bench/compare-$RUN_ID"

drill_setup
trap bench_cleanup EXIT INT TERM

# lz4 lets the lag probe search compressed snapshot objects; best-effort.
"$DRILL_PYTHON" -m pip install -q lz4 >/dev/null 2>&1 || true

# --- knob-matching header ---------------------------------------------------
DRIVER_ROWS_PER_SEC=$(awk -v i="$BENCH_DRIVER_INTERVAL" 'BEGIN { printf "%.1f", 1/i }')
cat <<HDR
============================================================
walrust vs litestream head-to-head
============================================================
walrust:    $WALRUST_VERSION ($WALRUST_BIN)
litestream: $LITESTREAM_VERSION ($(command -v litestream))
S3 target:  local MinIO (docker), endpoint $WALRUST_DRILL_ENDPOINT,
            bucket $BENCH_BUCKET, prefix $WALRUST_DRILL_PREFIX
duration:   ${BENCH_DURATION_SECONDS}s

matched knobs (set explicitly on both tools):
  sync interval   walrust: --wal-sync-interval $BENCH_SYNC_INTERVAL_SECONDS
                  litestream: replica.sync-interval: ${BENCH_SYNC_INTERVAL_SECONDS}s
  workload        one DB per tool, same drill write driver,
                  ${DRIVER_ROWS_PER_SEC} rows/s (1 row per ${BENCH_DRIVER_INTERVAL}s)
  S3 target       same MinIO container, same bucket

known asymmetries (cannot be matched knob-for-knob):
  - walrust runs product defaults otherwise: on-startup snapshot,
    snapshot-interval ${BENCH_WALRUST_SNAPSHOT_INTERVAL}s,
    checkpoint-interval ${BENCH_WALRUST_CHECKPOINT_INTERVAL}s, --no-metrics.
  - litestream v0.5 always runs its built-in multi-level compaction
    monitor (L1 30s upward) and snapshot levels; it has no off switch.
    Its compaction requests are counted as litestream requests.
  - request counts come from server-side MinIO trace; harness/probe
    traffic (minio-py user agent) is excluded from both tools' counts.
============================================================
HDR

# --- databases + drivers ----------------------------------------------------
DRILL_DB="$DRILL_WORKDIR/walrust.db"
LS_DB="$DRILL_WORKDIR/litestream.db"
LS_COUNT="$DRILL_WORKDIR/ls-driver.count"
LS_PAUSE="$DRILL_WORKDIR/ls-driver.pause"

create_db "$DRILL_DB" >/dev/null
create_db "$LS_DB" >/dev/null
append_rows "$DRILL_DB" 5 base >/dev/null
append_rows "$LS_DB" 5 base >/dev/null

start_driver "$DRILL_DB" "$BENCH_DRIVER_INTERVAL" bench-walrust
spawn_driver "$LS_DB" "$LS_COUNT" "$LS_PAUSE" "$BENCH_DRIVER_INTERVAL" bench-litestream "$DRILL_WORKDIR/ls-driver"
LS_DRIVER_PID=$SPAWNED_DRIVER_PID

wait_driver_count_at_least 8 45
ls_deadline=$((SECONDS + 45))
while [ "$(db_count "$LS_DB")" -lt 8 ]; do
  [ "$SECONDS" -lt "$ls_deadline" ] || fail "litestream driver made no progress"
  sleep "$DRILL_POLL_INTERVAL"
done

# --- start both tools -------------------------------------------------------
WALRUST_DRILL_SNAPSHOT_INTERVAL=$BENCH_WALRUST_SNAPSHOT_INTERVAL \
WALRUST_DRILL_WAL_SYNC_INTERVAL=$BENCH_SYNC_INTERVAL_SECONDS \
WALRUST_DRILL_CHECKPOINT_INTERVAL=$BENCH_WALRUST_CHECKPOINT_INTERVAL \
  start_walrust "$DRILL_DB"
wait_for_shadow_blocker "$DRILL_DB"

LS_CONFIG="$DRILL_WORKDIR/litestream.yml"
write_litestream_config "$LS_CONFIG" "$WALRUST_DRILL_ENDPOINT" "$BENCH_BUCKET" \
  "$BENCH_SYNC_INTERVAL_SECONDS" \
  "$LS_DB:$DRILL_RUN_PREFIX/litestream"
start_litestream "$LS_CONFIG" "$DRILL_WORKDIR/litestream.log"

# --- samplers + lag probes ---------------------------------------------------
start_rss_sampler "$DRILL_WALRUST_PID" "$BENCH_RSS_SAMPLE_SECONDS" "$RESULTS_DIR/rss-walrust.tsv"
W_RSS_PID=$SPAWNED_RSS_SAMPLER_PID
start_rss_sampler "$BENCH_LITESTREAM_PID" "$BENCH_RSS_SAMPLE_SECONDS" "$RESULTS_DIR/rss-litestream.tsv"
LS_RSS_PID=$SPAWNED_RSS_SAMPLER_PID

"$DRILL_PYTHON" "$BENCH_SCRIPT_DIR/lag-probe.py" \
  --db "$DRILL_DB" --bucket "$BENCH_BUCKET" --prefix "$DRILL_RUN_PREFIX/walrust/" \
  --endpoint "$WALRUST_DRILL_ENDPOINT" --duration "$BENCH_DURATION_SECONDS" \
  --period "$BENCH_SENTINEL_PERIOD_SECONDS" \
  --out "$RESULTS_DIR/lag-walrust.tsv" >"$DRILL_WORKDIR/lag-walrust.log" 2>&1 &
W_PROBE_PID=$!
"$DRILL_PYTHON" "$BENCH_SCRIPT_DIR/lag-probe.py" \
  --db "$LS_DB" --bucket "$BENCH_BUCKET" --prefix "$DRILL_RUN_PREFIX/litestream/" \
  --endpoint "$WALRUST_DRILL_ENDPOINT" --duration "$BENCH_DURATION_SECONDS" \
  --period "$BENCH_SENTINEL_PERIOD_SECONDS" \
  --out "$RESULTS_DIR/lag-litestream.tsv" >"$DRILL_WORKDIR/lag-litestream.log" 2>&1 &
LS_PROBE_PID=$!
log "lag probes pids=$W_PROBE_PID,$LS_PROBE_PID"

# --- measurement window ------------------------------------------------------
end_at=$((SECONDS + BENCH_DURATION_SECONDS))
next_progress=$SECONDS
while [ "$SECONDS" -lt "$end_at" ]; do
  kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 || fail "walrust exited during bench; see $DRILL_WORKDIR/walrust.log"
  kill -0 "$BENCH_LITESTREAM_PID" >/dev/null 2>&1 || fail "litestream exited during bench; see $DRILL_WORKDIR/litestream.log"
  kill -0 "$DRILL_DRIVER_PID" >/dev/null 2>&1 || fail "walrust write driver exited; see $DRILL_WORKDIR/driver.log"
  kill -0 "$LS_DRIVER_PID" >/dev/null 2>&1 || fail "litestream write driver exited; see $DRILL_WORKDIR/ls-driver/driver.log"
  if [ "$SECONDS" -ge "$next_progress" ]; then
    log "t=$((SECONDS - end_at + BENCH_DURATION_SECONDS))s/${BENCH_DURATION_SECONDS}s rows: walrust=$(driver_count) litestream=$(db_count "$LS_DB")"
    next_progress=$((SECONDS + 30))
  fi
  sleep 1
done

# Let the probes drain their final poll (bounded by their own poll-timeout).
probe_deadline=$((SECONDS + 45))
while kill -0 "$W_PROBE_PID" >/dev/null 2>&1 || kill -0 "$LS_PROBE_PID" >/dev/null 2>&1; do
  [ "$SECONDS" -lt "$probe_deadline" ] || break
  sleep "$DRILL_POLL_INTERVAL"
done
stop_pid "$W_PROBE_PID"; W_PROBE_PID=
stop_pid "$LS_PROBE_PID"; LS_PROBE_PID=

# --- quiesce, capture trace, validate ----------------------------------------
pause_driver
pause_driver_files "$LS_DB" "$LS_PAUSE"
EXPECTED_W=$(driver_count)
EXPECTED_LS=$(db_count "$LS_DB")

# Give both tools a bounded window to upload trailing frames: wait until the
# object listing under the run prefix is stable across two samples.
stable_deadline=$((SECONDS + 30))
prev_listing=-1
while [ "$SECONDS" -lt "$stable_deadline" ]; do
  cur_listing=$(s3_list_prefix "$DRILL_RUN_PREFIX/" | wc -l | tr -d ' ')
  if [ "$cur_listing" = "$prev_listing" ]; then
    break
  fi
  prev_listing=$cur_listing
  sleep $((BENCH_SYNC_INTERVAL_SECONDS + 1))
done

# Snapshot the request trace BEFORE the validity restores so the restores'
# GET traffic cannot contaminate the per-tool request counts.
stop_minio_trace "$TRACE_NAME" "$RESULTS_DIR/trace.jsonl"
TRACE_STOPPED=1

# Validity check (both tools still running so trailing syncs can converge).
wait_restore_count walrust "$EXPECTED_W"
wait_litestream_restore_count "$LS_CONFIG" "$LS_DB" "$EXPECTED_LS" "$DRILL_WORKDIR/restored-litestream.db"

stop_pid "$W_RSS_PID"; W_RSS_PID=
stop_pid "$LS_RSS_PID"; LS_RSS_PID=
stop_driver
stop_pid "$LS_DRIVER_PID"; LS_DRIVER_PID=
stop_walrust
stop_litestream

# --- report -------------------------------------------------------------------
RUN_ID="$RUN_ID" \
BENCH_BUCKET="$BENCH_BUCKET" \
BENCH_PREFIX="$DRILL_RUN_PREFIX" \
BENCH_DURATION_SECONDS="$BENCH_DURATION_SECONDS" \
BENCH_SYNC_INTERVAL_SECONDS="$BENCH_SYNC_INTERVAL_SECONDS" \
BENCH_DRIVER_INTERVAL="$BENCH_DRIVER_INTERVAL" \
BENCH_SENTINEL_PERIOD_SECONDS="$BENCH_SENTINEL_PERIOD_SECONDS" \
BENCH_RSS_SAMPLE_SECONDS="$BENCH_RSS_SAMPLE_SECONDS" \
BENCH_WALRUST_SNAPSHOT_INTERVAL="$BENCH_WALRUST_SNAPSHOT_INTERVAL" \
BENCH_WALRUST_CHECKPOINT_INTERVAL="$BENCH_WALRUST_CHECKPOINT_INTERVAL" \
WALRUST_VERSION="$WALRUST_VERSION" \
LITESTREAM_VERSION="$LITESTREAM_VERSION" \
ROWS_WALRUST="$EXPECTED_W" \
ROWS_LITESTREAM="$EXPECTED_LS" \
  "$DRILL_PYTHON" - "$RESULTS_DIR" <<'PY'
import json, os, sys
run_dir = sys.argv[1]
e = os.environ
meta = {
    "run_id": e["RUN_ID"],
    "bucket": e["BENCH_BUCKET"],
    "prefix": e["BENCH_PREFIX"],
    "endpoint": "local-minio-docker",
    "duration_seconds": int(e["BENCH_DURATION_SECONDS"]),
    "sync_interval_seconds": int(e["BENCH_SYNC_INTERVAL_SECONDS"]),
    "driver_commit_interval_seconds": float(e["BENCH_DRIVER_INTERVAL"]),
    "sentinel_period_seconds": int(e["BENCH_SENTINEL_PERIOD_SECONDS"]),
    "rss_sample_seconds": int(e["BENCH_RSS_SAMPLE_SECONDS"]),
    "walrust_snapshot_interval_seconds": int(e["BENCH_WALRUST_SNAPSHOT_INTERVAL"]),
    "walrust_checkpoint_interval_seconds": int(e["BENCH_WALRUST_CHECKPOINT_INTERVAL"]),
    "versions": {"walrust": e["WALRUST_VERSION"], "litestream": e["LITESTREAM_VERSION"]},
    "rows": {"walrust": int(e["ROWS_WALRUST"]), "litestream": int(e["ROWS_LITESTREAM"])},
    "object_prefix": {
        "walrust": e["BENCH_PREFIX"] + "/walrust/",
        "litestream": e["BENCH_PREFIX"] + "/litestream/",
    },
    "validity": "restore + integrity_check + exact row-count match passed for both tools",
}
with open(os.path.join(run_dir, "run-meta.json"), "w") as fh:
    json.dump(meta, fh, indent=2)
PY

"$DRILL_PYTHON" "$BENCH_SCRIPT_DIR/report-compare.py" "$RESULTS_DIR" | tee "$RESULTS_DIR/report.txt"

log "PASS compare-litestream results in $RESULTS_DIR"
