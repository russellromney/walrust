#!/usr/bin/env bash
#
# Restore-speed benchmark: how fast is cold restore-to-latest, and how many
# objects does it fetch, for a LONG history — walrust WITH leveled compaction vs
# walrust WITHOUT compaction vs litestream (its own default compaction). All
# three replicate the same workload shape (same driver rate, same duration, same
# 1s sync) to the same local MinIO, then each is restored cold three times to a
# FRESH output path (no OS/page-cache reuse) and the MEDIAN wall time is reported.
#
# The two walrust cells isolate compaction's effect independently of litestream:
# "compaction makes walrust restore Nx faster" is the walrust-vs-walrust ratio;
# the litestream cell is the external reference point.
#
# Measurement, not a gate: never runs in PR CI. Still ends with a row-exact +
# integrity validity check for EVERY restore and aborts (nonzero, no numbers) if
# any restore is wrong — a benchmark of a broken restore must not produce a
# number.
#
# No kills/restarts here (that is the drill's job): a continuous run keeps one
# unbroken incremental chain, so restore-to-latest genuinely traverses the whole
# history — the fine incrementals for the plain/litestream cells, the merged
# levels for the compacted cell. Periodic snapshots are DISABLED so restore
# rebuilds from the single base through that history (otherwise a recent snapshot
# would short-circuit the traversal and hide the difference).
#
# Knobs (env):
#   BENCH_BUILD_SECONDS     history build window (default 300; thousands of
#                           objects need ~this at 1s sync + a fast driver)
#   BENCH_SYNC_INTERVAL     sync interval on ALL tools (default 1s)
#   BENCH_DRIVER_INTERVAL   seconds between driver commits (default 0.02)
#   BENCH_RESTORE_RUNS      cold restores per subject for the median (default 3)
#   BENCH_L1_BATCH          walrust L0->L1 fold batch (default 20)
#   BENCH_L2_BATCH          walrust L1->L2 fold batch (default 10)
#   BENCH_KEEP_FINE         walrust keep_fine_window (default "0s": fold all)
#   BENCH_MINIO_PORT        MinIO container host port (default 19100)
#   BENCH_BUCKET            bucket name inside the container
#
# Results: bench/results-<utc>/ (gitignored): table on stdout, results.json.

set -Eeuo pipefail

BENCH_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WALRUST_BIN=${WALRUST_BIN:-"$(cd "$BENCH_SCRIPT_DIR/.." && pwd)/target/release/walrust"}
export WALRUST_BIN

# shellcheck source=drills/lib.sh
source "$BENCH_SCRIPT_DIR/../drills/lib.sh"
# shellcheck source=bench/common.sh
source "$BENCH_SCRIPT_DIR/common.sh"

BENCH_BUILD_SECONDS=${BENCH_BUILD_SECONDS:-300}
BENCH_SYNC_INTERVAL=${BENCH_SYNC_INTERVAL:-1}
BENCH_DRIVER_INTERVAL=${BENCH_DRIVER_INTERVAL:-0.02}
BENCH_RESTORE_RUNS=${BENCH_RESTORE_RUNS:-3}
BENCH_L1_BATCH=${BENCH_L1_BATCH:-20}
BENCH_L2_BATCH=${BENCH_L2_BATCH:-10}
BENCH_KEEP_FINE=${BENCH_KEEP_FINE:-0s}
BENCH_MINIO_PORT=${BENCH_MINIO_PORT:-19100}
BENCH_BUCKET=${BENCH_BUCKET:-walrust-restore-bench}

require_cmd docker
LITESTREAM_VERSION=$(ensure_litestream)
WALRUST_VERSION=$("$WALRUST_BIN" --version 2>/dev/null || echo "unknown")

RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)-$$
MINIO_NAME="walrust-rbench-minio-$RUN_ID"
TRACE_NAME="walrust-rbench-trace-$RUN_ID"
RESULTS_DIR=$(bench_results_dir)

# --- process bookkeeping ---------------------------------------------------
WC_PID=          # walrust compacted watcher
WP_PID=          # walrust plain watcher
WC_DRIVER=
WP_DRIVER=
LS_DRIVER=
TRACE_STOPPED=0

bench_cleanup() {
  set +e
  stop_pid "$WC_PID"
  stop_pid "$WP_PID"
  stop_litestream
  stop_pid "$WC_DRIVER"
  stop_pid "$WP_DRIVER"
  stop_pid "$LS_DRIVER"
  stop_driver
  drill_cleanup
  if [ -n "${DRILL_WORKDIR:-}" ]; then
    rm -rf "$DRILL_WORKDIR" 2>/dev/null
  fi
  if [ "$TRACE_STOPPED" != "1" ]; then
    docker rm -f "$TRACE_NAME" >/dev/null 2>&1
  fi
  stop_minio_container "$MINIO_NAME"
}

trap bench_cleanup EXIT INT TERM
start_minio_container "$MINIO_NAME" "$BENCH_MINIO_PORT" "$BENCH_BUCKET"
start_minio_trace "$TRACE_NAME" "$MINIO_NAME"

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-minioadmin}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-minioadmin}
export AWS_REGION=${AWS_REGION:-us-east-1}
export WALRUST_DRILL_BUCKET=$BENCH_BUCKET
export WALRUST_DRILL_ENDPOINT="http://127.0.0.1:$BENCH_MINIO_PORT"
export WALRUST_DRILL_PREFIX="bench/restore-$RUN_ID"

drill_setup
trap bench_cleanup EXIT INT TERM

WC_DB="$DRILL_WORKDIR/walrust-compacted.db"
WP_DB="$DRILL_WORKDIR/walrust-plain.db"
LS_DB="$DRILL_WORKDIR/litestream.db"
WC_PREFIX="$DRILL_RUN_PREFIX/walrust-compacted"
WP_PREFIX="$DRILL_RUN_PREFIX/walrust-plain"
LS_PREFIX="$DRILL_RUN_PREFIX/litestream"

DRIVER_ROWS_PER_SEC=$(awk -v i="$BENCH_DRIVER_INTERVAL" 'BEGIN { printf "%.1f", 1/i }')
cat <<HDR
============================================================
walrust restore-speed benchmark
============================================================
walrust:    $WALRUST_VERSION ($WALRUST_BIN)
litestream: $LITESTREAM_VERSION ($(command -v litestream))
S3 target:  local MinIO (docker) $WALRUST_DRILL_ENDPOINT, bucket $BENCH_BUCKET
build:      ${BENCH_BUILD_SECONDS}s at ${DRIVER_ROWS_PER_SEC} rows/s, ${BENCH_SYNC_INTERVAL}s sync
restores:   $BENCH_RESTORE_RUNS cold runs/subject, fresh output path each (median)

matched knobs (identical on all three subjects):
  sync interval   walrust: --wal-sync-interval ${BENCH_SYNC_INTERVAL}
                  litestream: replica.sync-interval: ${BENCH_SYNC_INTERVAL}s
  workload        one DB per subject, same drill driver, same rate/duration
  snapshots       walrust: DISABLED during build (--snapshot-interval + --checkpoint-interval
                  huge) so restore traverses the full history from the single base
  compaction      walrust-compacted: [compaction] enabled, l1_batch=$BENCH_L1_BATCH,
                  l2_batch=$BENCH_L2_BATCH, keep_fine_window=$BENCH_KEEP_FINE
                  walrust-plain: compaction OFF (same history shape, no folding)
                  litestream: its built-in default compaction (no off switch)
============================================================
HDR

# Compaction config for the compacted walrust cell.
WC_CONFIG="$DRILL_WORKDIR/compaction.toml"
cat >"$WC_CONFIG" <<TOML
[compaction]
enabled = true
keep_fine_window = "$BENCH_KEEP_FINE"
l1_batch = $BENCH_L1_BATCH
l2_batch = $BENCH_L2_BATCH
TOML

# Start a walrust watcher in independent-tasks mode with snapshots/checkpoints
# disabled (long unbroken incremental chain). Optional 2nd arg: config file.
start_walrust_watch() {
  local db=$1
  local uri=$2
  local logfile=$3
  local config=${4:-}
  local args=(watch "$db" --independent-tasks
    --bucket "$uri" --endpoint "$WALRUST_DRILL_ENDPOINT"
    --wal-sync-interval "$BENCH_SYNC_INTERVAL"
    --snapshot-interval 999999 --checkpoint-interval 999999
    --on-startup true --no-metrics --no-cache)
  if [ -n "$config" ]; then
    args+=(--config "$config")
  fi
  "$WALRUST_BIN" "${args[@]}" >"$logfile" 2>&1 &
  local pid=$!
  wait_process_alive "$pid" "walrust watch ($uri)"
  printf '%s\n' "$pid"
}

# Median of the numbers on stdin (one per line).
median() {
  "$DRILL_PYTHON" -c '
import statistics, sys
vals = [float(x) for x in sys.stdin.read().split() if x.strip()]
print(f"{statistics.median(vals):.3f}" if vals else "nan")
'
}

# Count GetObject / List requests to a key prefix in the trace lines produced
# AFTER `since_lines` (a running docker-logs line offset). Excludes probe
# (minio-py) traffic. Prints "GETS LISTS".
trace_ops_since() {
  local since_lines=$1
  local key_prefix=$2
  docker logs "$TRACE_NAME" 2>/dev/null \
    | tail -n "+$((since_lines + 1))" \
    | "$DRILL_PYTHON" -c '
import json, sys
kp = sys.argv[1]
gets = lists = 0
for line in sys.stdin:
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        e = json.loads(line)
    except json.JSONDecodeError:
        continue
    api = e.get("api", "")
    path = e.get("path", "") or ""
    ua = ((e.get("request") or {}).get("headers") or {}).get("User-Agent", "")
    if "minio-py" in ua:
        continue
    if kp not in path:
        continue
    name = api.split(".", 1)[-1]
    if name == "GetObject":
        gets += 1
    elif name.startswith("ListObjects"):
        lists += 1
print(gets, lists)
' "$key_prefix"
}

trace_line_count() {
  docker logs "$TRACE_NAME" 2>/dev/null | wc -l | tr -d ' '
}

# --- build the history ------------------------------------------------------
create_db "$WC_DB" >/dev/null
create_db "$WP_DB" >/dev/null
create_db "$LS_DB" >/dev/null
append_rows "$WC_DB" 5 base >/dev/null
append_rows "$WP_DB" 5 base >/dev/null
append_rows "$LS_DB" 5 base >/dev/null

spawn_driver "$WC_DB" "$DRILL_WORKDIR/wc.count" "$DRILL_WORKDIR/wc.pause" "$BENCH_DRIVER_INTERVAL" bench-wc "$DRILL_WORKDIR/wc-driver"
WC_DRIVER=$SPAWNED_DRIVER_PID
spawn_driver "$WP_DB" "$DRILL_WORKDIR/wp.count" "$DRILL_WORKDIR/wp.pause" "$BENCH_DRIVER_INTERVAL" bench-wp "$DRILL_WORKDIR/wp-driver"
WP_DRIVER=$SPAWNED_DRIVER_PID
spawn_driver "$LS_DB" "$DRILL_WORKDIR/ls.count" "$DRILL_WORKDIR/ls.pause" "$BENCH_DRIVER_INTERVAL" bench-ls "$DRILL_WORKDIR/ls-driver"
LS_DRIVER=$SPAWNED_DRIVER_PID

WC_PID=$(start_walrust_watch "$WC_DB" "s3://$BENCH_BUCKET/$WC_PREFIX" "$DRILL_WORKDIR/wc.log" "$WC_CONFIG")
WP_PID=$(start_walrust_watch "$WP_DB" "s3://$BENCH_BUCKET/$WP_PREFIX" "$DRILL_WORKDIR/wp.log")

LS_CONFIG="$DRILL_WORKDIR/litestream.yml"
write_litestream_config "$LS_CONFIG" "$WALRUST_DRILL_ENDPOINT" "$BENCH_BUCKET" \
  "$BENCH_SYNC_INTERVAL" "$LS_DB:$LS_PREFIX"
start_litestream "$LS_CONFIG" "$DRILL_WORKDIR/litestream.log"

log "building history for ${BENCH_BUILD_SECONDS}s..."
build_end=$((SECONDS + BENCH_BUILD_SECONDS))
while [ "$SECONDS" -lt "$build_end" ]; do
  kill -0 "$WC_PID" >/dev/null 2>&1 || fail "walrust-compacted exited; see $DRILL_WORKDIR/wc.log"
  kill -0 "$WP_PID" >/dev/null 2>&1 || fail "walrust-plain exited; see $DRILL_WORKDIR/wp.log"
  kill -0 "$BENCH_LITESTREAM_PID" >/dev/null 2>&1 || fail "litestream exited; see $DRILL_WORKDIR/litestream.log"
  sleep 5
done

# --- quiesce ----------------------------------------------------------------
pause_driver_files "$WC_DB" "$DRILL_WORKDIR/wc.pause"
pause_driver_files "$WP_DB" "$DRILL_WORKDIR/wp.pause"
pause_driver_files "$LS_DB" "$DRILL_WORKDIR/ls.pause"
WC_ROWS=$(db_count "$WC_DB")
WP_ROWS=$(db_count "$WP_DB")
LS_ROWS=$(db_count "$LS_DB")

# Let trailing syncs + a final compaction pass settle: wait for a stable listing.
stable_deadline=$((SECONDS + 40))
prev=-1
while [ "$SECONDS" -lt "$stable_deadline" ]; do
  cur=$(s3_list_prefix "$DRILL_RUN_PREFIX/" | wc -l | tr -d ' ')
  [ "$cur" = "$prev" ] && break
  prev=$cur
  sleep $((BENCH_SYNC_INTERVAL + 2))
done

# Stop the writers so nothing mutates the buckets during restores.
stop_pid "$WC_PID"; WC_PID=
stop_pid "$WP_PID"; WP_PID=
stop_litestream
stop_pid "$WC_DRIVER"; WC_DRIVER=
stop_pid "$WP_DRIVER"; WP_DRIVER=
stop_pid "$LS_DRIVER"; LS_DRIVER=

# End-state object counts per subject (fetched cost proxy + convergence).
count_objs() { s3_list_prefix "$1/" | grep -cE '\.ltx$' || true; }
WC_OBJS=$(count_objs "$WC_PREFIX")
WP_OBJS=$(count_objs "$WP_PREFIX")
LS_OBJS=$(count_objs "$LS_PREFIX")
WC_MERGED=$(s3_list_prefix "$WC_PREFIX/" | grep -cE '/levels/L[0-9]+/.*\.ltx$' || true)
log "end-state objects: walrust-compacted=$WC_OBJS (merged=$WC_MERGED) walrust-plain=$WP_OBJS litestream=$LS_OBJS"
[ "$WC_MERGED" -gt 0 ] || fail "walrust-compacted produced no merged levels — compaction never fired; numbers would be meaningless"

# --- restore timing ---------------------------------------------------------
# macOS ships bash 3.2 (no associative arrays), so results land in plain globals
# named `<OUTVAR>_MED/_GETS/_LISTS/_OK` via `printf -v`. Runs in the MAIN shell
# (not a subshell) so a `fail` inside propagates.

# Time a cold restore: run the command, print elapsed seconds, propagate exit.
time_restore() {
  "$DRILL_PYTHON" -c '
import subprocess, sys, time
t = time.time()
r = subprocess.run(sys.argv[1:], capture_output=True)
sys.stderr.write(r.stderr.decode(errors="replace"))
print(f"{time.time()-t:.3f}")
sys.exit(r.returncode)
' "$@"
}

# measure_restores OUTVAR KIND NAME URI EXPECTED TRACE_PREFIX
#   KIND = walrust | litestream ; NAME = db name (walrust) or db path (litestream)
measure_restores() {
  local outvar=$1 kind=$2 name=$3 uri=$4 expected=$5 tprefix=$6
  local times="" g="" l="" ok=1 run out elapsed since gl
  for run in $(seq 1 "$BENCH_RESTORE_RUNS"); do
    out="$DRILL_WORKDIR/restore-$outvar-$run.db"
    rm -f "$out"
    since=$(trace_line_count)
    if [ "$kind" = "litestream" ]; then
      elapsed=$(time_restore litestream restore -config "$LS_CONFIG" -o "$out" "$name") \
        || fail "litestream restore run $run failed (see stderr above)"
    else
      elapsed=$(time_restore "$WALRUST_BIN" restore "$name" --output "$out" \
        --bucket "$uri" --endpoint "$WALRUST_DRILL_ENDPOINT") \
        || fail "$outvar restore run $run failed (see stderr above)"
    fi
    integrity_ok "$out"
    [ "$(db_count "$out")" = "$expected" ] \
      || { ok=0; log "$outvar restore run $run ROW DIFF: $(db_count "$out") != $expected"; }
    gl=$(trace_ops_since "$since" "$tprefix")
    times+="$elapsed "
    g+="${gl%% *} "
    l+="${gl##* } "
  done
  printf -v "${outvar}_MED" '%s' "$(printf '%s\n' "$times" | median)"
  printf -v "${outvar}_GETS" '%s' "$(printf '%s\n' "$g" | median)"
  printf -v "${outvar}_LISTS" '%s' "$(printf '%s\n' "$l" | median)"
  printf -v "${outvar}_OK" '%s' "$ok"
  local mv="${outvar}_MED" gv="${outvar}_GETS" lv="${outvar}_LISTS" ov="${outvar}_OK"
  log "$outvar: median ${!mv}s, GETs ${!gv}, LISTs ${!lv}, row-exact=${!ov}"
}

measure_restores WC walrust walrust-compacted "s3://$BENCH_BUCKET/$WC_PREFIX" "$WC_ROWS" "$WC_PREFIX"
measure_restores WP walrust walrust-plain "s3://$BENCH_BUCKET/$WP_PREFIX" "$WP_ROWS" "$WP_PREFIX"
measure_restores LS litestream "$LS_DB" "" "$LS_ROWS" "$LS_PREFIX"

# Validity gate: a wrong restore invalidates the whole run.
[ "$WC_OK" = "1" ] || fail "VALIDITY: walrust-compacted restore was not row-exact — aborting"
[ "$WP_OK" = "1" ] || fail "VALIDITY: walrust-plain restore was not row-exact — aborting"
[ "$LS_OK" = "1" ] || fail "VALIDITY: litestream restore was not row-exact — aborting"

stop_minio_trace "$TRACE_NAME" "$RESULTS_DIR/trace.jsonl"
TRACE_STOPPED=1

# --- report -----------------------------------------------------------------
SPEEDUP=$(awk -v a="$WP_MED" -v b="$WC_MED" 'BEGIN { if (b>0) printf "%.2f", a/b; else print "nan" }')
VS_LS=$(awk -v a="$LS_MED" -v b="$WC_MED" 'BEGIN { if (b>0) printf "%.2f", a/b; else print "nan" }')

RESULTS_JSON="$RESULTS_DIR/results.json"
RUN_ID="$RUN_ID" RESULTS_DIR="$RESULTS_DIR" \
BENCH_BUILD_SECONDS="$BENCH_BUILD_SECONDS" BENCH_SYNC_INTERVAL="$BENCH_SYNC_INTERVAL" \
BENCH_DRIVER_INTERVAL="$BENCH_DRIVER_INTERVAL" BENCH_RESTORE_RUNS="$BENCH_RESTORE_RUNS" \
BENCH_L1_BATCH="$BENCH_L1_BATCH" BENCH_L2_BATCH="$BENCH_L2_BATCH" BENCH_KEEP_FINE="$BENCH_KEEP_FINE" \
WALRUST_VERSION="$WALRUST_VERSION" LITESTREAM_VERSION="$LITESTREAM_VERSION" \
WC_ROWS="$WC_ROWS" WP_ROWS="$WP_ROWS" LS_ROWS="$LS_ROWS" \
WC_OBJS="$WC_OBJS" WP_OBJS="$WP_OBJS" LS_OBJS="$LS_OBJS" WC_MERGED="$WC_MERGED" \
WC_MED="$WC_MED" WP_MED="$WP_MED" LS_MED="$LS_MED" \
WC_GETS="$WC_GETS" WP_GETS="$WP_GETS" LS_GETS="$LS_GETS" \
WC_LISTS="$WC_LISTS" WP_LISTS="$WP_LISTS" LS_LISTS="$LS_LISTS" \
SPEEDUP="$SPEEDUP" VS_LS="$VS_LS" \
  "$DRILL_PYTHON" -c '
import json, os
e = os.environ
def num(x):
    try:
        return float(x)
    except ValueError:
        return x
subjects = {
  "walrust-compacted": {"median_restore_s": num(e["WC_MED"]), "restore_gets": num(e["WC_GETS"]), "restore_lists": num(e["WC_LISTS"]), "rows": int(e["WC_ROWS"]), "end_objects": int(e["WC_OBJS"]), "merged_objects": int(e["WC_MERGED"])},
  "walrust-plain":     {"median_restore_s": num(e["WP_MED"]), "restore_gets": num(e["WP_GETS"]), "restore_lists": num(e["WP_LISTS"]), "rows": int(e["WP_ROWS"]), "end_objects": int(e["WP_OBJS"])},
  "litestream":        {"median_restore_s": num(e["LS_MED"]), "restore_gets": num(e["LS_GETS"]), "restore_lists": num(e["LS_LISTS"]), "rows": int(e["LS_ROWS"]), "end_objects": int(e["LS_OBJS"])},
}
out = {
  "benchmark": "restore-speed",
  "schema_version": 1,
  "run_id": e["RUN_ID"],
  "knobs": {
    "build_seconds": int(e["BENCH_BUILD_SECONDS"]),
    "sync_interval_seconds": int(e["BENCH_SYNC_INTERVAL"]),
    "driver_commit_interval_seconds": float(e["BENCH_DRIVER_INTERVAL"]),
    "restore_runs": int(e["BENCH_RESTORE_RUNS"]),
    "l1_batch": int(e["BENCH_L1_BATCH"]), "l2_batch": int(e["BENCH_L2_BATCH"]),
    "keep_fine_window": e["BENCH_KEEP_FINE"],
    "snapshots_during_build": "disabled",
  },
  "versions": {"walrust": e["WALRUST_VERSION"], "litestream": e["LITESTREAM_VERSION"]},
  "subjects": subjects,
  "compaction_speedup_vs_plain": num(e["SPEEDUP"]),
  "compacted_speedup_vs_litestream": num(e["VS_LS"]),
  "validity": "restore + integrity_check + exact row-count match passed for all subjects",
}
json.dump(out, open(e["RESULTS_DIR"] + "/results.json", "w"), indent=2)
open(e["RESULTS_DIR"] + "/results.json", "a").write("\n")
'

cat <<REPORT | tee "$RESULTS_DIR/report.txt"

== walrust restore-speed: results ==

subject             rows    end objs  restore GETs  median restore (s)
------------------  ------  --------  ------------  ------------------
walrust-compacted   $WC_ROWS   $WC_OBJS ($WC_MERGED merged)   $WC_GETS         $WC_MED
walrust-plain       $WP_ROWS   $WP_OBJS         $WP_GETS         $WP_MED
litestream          $LS_ROWS   $LS_OBJS         $LS_GETS         $LS_MED

compaction speedup (walrust plain / walrust compacted): ${SPEEDUP}x
compacted vs litestream (litestream / walrust compacted): ${VS_LS}x
results json: $RESULTS_JSON
REPORT

log "PASS restore-speed results in $RESULTS_DIR"
