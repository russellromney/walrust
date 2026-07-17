#!/usr/bin/env bash

set -Eeuo pipefail

DRILL_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$DRILL_DIR/.." && pwd)
DRILL_NAME=${DRILL_NAME:-$(basename "${BASH_SOURCE[1]:-$0}" .sh)}
WALRUST_BIN=${WALRUST_BIN:-"$REPO_ROOT/target/debug/walrust"}
DRILL_PYTHON=${DRILL_PYTHON:-python3}
DRILL_TMP_ROOT=${DRILL_TMP_ROOT:-${TMPDIR:-/tmp}}
DRILL_POLL_INTERVAL=${DRILL_POLL_INTERVAL:-0.25}
DRILL_RESTORE_TIMEOUT=${DRILL_RESTORE_TIMEOUT:-45}

DRILL_WORKDIR=
DRILL_RUN_PREFIX=
DRILL_BASE_PREFIX=
DRILL_BUCKET_NAME=
DRILL_BUCKET_URI=
DRILL_ENDPOINT=
DRILL_DB=
DRILL_RESTORE=
DRILL_WALRUST_PID=
DRILL_DRIVER_PID=
DRILL_DRIVER_PAUSE=
DRILL_REPLICA_PID=
DRILL_DRIVER_COUNT=
DRILL_LAST_ACTUAL=
DRILL_LAST_RESTORE_ERROR=

log() {
  printf '[%s] %s\n' "$DRILL_NAME" "$*" >&2
}

fail() {
  printf '[%s] ERROR: %s\n' "$DRILL_NAME" "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

bucket_from_env() {
  local bucket=${WALRUST_DRILL_BUCKET:-${WALRUST_TEST_BUCKET:-${WALRUST_S3_TEST_BUCKET:-${TIERED_TEST_BUCKET:-${S3_TEST_BUCKET:-}}}}}
  bucket=${bucket#s3://}
  bucket=${bucket%%/*}
  [ -n "$bucket" ] || fail "set WALRUST_DRILL_BUCKET, WALRUST_TEST_BUCKET, TIERED_TEST_BUCKET, or S3_TEST_BUCKET"
  printf '%s\n' "$bucket"
}

base_prefix_from_env() {
  local raw=${WALRUST_DRILL_BUCKET:-${WALRUST_TEST_BUCKET:-${WALRUST_S3_TEST_BUCKET:-${TIERED_TEST_BUCKET:-${S3_TEST_BUCKET:-}}}}}
  raw=${raw#s3://}
  if [[ "$raw" == */* ]]; then
    printf '%s\n' "${raw#*/}"
  fi
}

endpoint_from_env() {
  local endpoint=${WALRUST_DRILL_ENDPOINT:-${AWS_ENDPOINT_URL_S3:-${AWS_ENDPOINT_URL:-}}}
  [ -n "$endpoint" ] || fail "set WALRUST_DRILL_ENDPOINT, AWS_ENDPOINT_URL_S3, or AWS_ENDPOINT_URL"
  printf '%s\n' "$endpoint"
}

ensure_minio_python() {
  local venv=${WALRUST_DRILL_PYTHON_VENV:-"$REPO_ROOT/target/drill-venv"}
  if [ -x "$venv/bin/python" ]; then
    DRILL_PYTHON="$venv/bin/python"
  fi
  if "$DRILL_PYTHON" - <<'PY' >/dev/null 2>&1
import minio
PY
  then
    return 0
  fi
  log "installing Python MinIO client in $venv"
  python3 -m venv "$venv"
  DRILL_PYTHON="$venv/bin/python"
  "$DRILL_PYTHON" -m pip install minio >/dev/null
}

drill_setup() {
  require_cmd sqlite3
  require_cmd python3
  [ -x "$WALRUST_BIN" ] || fail "walrust binary not executable: $WALRUST_BIN"
  ensure_minio_python

  DRILL_BUCKET_NAME=$(bucket_from_env)
  DRILL_BASE_PREFIX=$(base_prefix_from_env)
  DRILL_ENDPOINT=$(endpoint_from_env)
  DRILL_WORKDIR=$(mktemp -d "$DRILL_TMP_ROOT/walrust-$DRILL_NAME.XXXXXX")
  DRILL_RUN_PREFIX=${WALRUST_DRILL_PREFIX:-"drills/$DRILL_NAME-$(date +%Y%m%d%H%M%S)-$$"}
  if [ -n "$DRILL_BASE_PREFIX" ]; then
    DRILL_RUN_PREFIX="$DRILL_BASE_PREFIX/$DRILL_RUN_PREFIX"
  fi
  DRILL_BUCKET_URI="s3://$DRILL_BUCKET_NAME/$DRILL_RUN_PREFIX"
  DRILL_DB="$DRILL_WORKDIR/$DRILL_NAME.db"
  DRILL_RESTORE="$DRILL_WORKDIR/restored.db"
  DRILL_DRIVER_COUNT="$DRILL_WORKDIR/driver.count"
  DRILL_DRIVER_PAUSE="$DRILL_WORKDIR/driver.pause"

  export AWS_ENDPOINT_URL_S3="$DRILL_ENDPOINT"
  trap drill_cleanup EXIT INT TERM
  log "prefix $DRILL_BUCKET_URI"
}

drill_cleanup() {
  set +e
  stop_driver
  stop_replica
  stop_walrust
  if [ "${WALRUST_DRILL_KEEP_ARTIFACTS:-0}" = "1" ]; then
    log "keeping artifacts in $DRILL_WORKDIR and s3://$DRILL_BUCKET_NAME/$DRILL_RUN_PREFIX"
    return 0
  fi
  if [ -n "${DRILL_BUCKET_NAME:-}" ] && [ -n "${DRILL_RUN_PREFIX:-}" ]; then
    s3_delete_prefix "$DRILL_RUN_PREFIX" >/dev/null 2>&1
  fi
  if [ -n "${DRILL_WORKDIR:-}" ]; then
    rm -rf "$DRILL_WORKDIR"
  fi
}

s3_client_python() {
  "$DRILL_PYTHON" - "$@" <<'PY'
import os
import sys
from urllib.parse import urlparse
from minio import Minio
from minio.deleteobjects import DeleteObject

cmd, bucket, prefix = sys.argv[1:4]
endpoint = os.environ.get("WALRUST_DRILL_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
access = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("MINIO_ROOT_USER") or "minioadmin"
secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("MINIO_ROOT_PASSWORD") or "minioadmin"
if not endpoint:
    raise SystemExit("missing S3 endpoint")
parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
client = Minio(parsed.netloc or parsed.path, access_key=access, secret_key=secret, secure=parsed.scheme == "https")

if cmd == "list":
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        print(obj.object_name)
elif cmd == "delete-prefix":
    objects = [DeleteObject(obj.object_name) for obj in client.list_objects(bucket, prefix=prefix, recursive=True)]
    for err in client.remove_objects(bucket, objects):
        print(err, file=sys.stderr)
elif cmd == "delete-key":
    key = sys.argv[4]
    client.remove_object(bucket, key)
else:
    raise SystemExit(f"unknown command: {cmd}")
PY
}

s3_list_prefix() {
  s3_client_python list "$DRILL_BUCKET_NAME" "$1"
}

s3_delete_prefix() {
  s3_client_python delete-prefix "$DRILL_BUCKET_NAME" "$1"
}

s3_delete_key() {
  s3_client_python delete-key "$DRILL_BUCKET_NAME" "$DRILL_RUN_PREFIX" "$1"
}

create_db() {
  local db=$1
  sqlite3 "$db" <<'SQL'
PRAGMA journal_mode=WAL;
PRAGMA wal_autocheckpoint=0;
CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY,
  value TEXT NOT NULL
);
SQL
}

append_rows() {
  local db=$1
  local rows=$2
  local label=$3
  local start
  local end
  start=$(sqlite3 "$db" "SELECT COALESCE(MAX(id), 0) + 1 FROM items;")
  end=$((start + rows - 1))
  sqlite3 "$db" <<SQL
PRAGMA journal_mode=WAL;
PRAGMA wal_autocheckpoint=0;
BEGIN IMMEDIATE;
WITH RECURSIVE seq(id) AS (
  SELECT $start
  UNION ALL
  SELECT id + 1 FROM seq WHERE id < $end
)
INSERT INTO items(id, value)
SELECT id, '$label-' || id FROM seq;
COMMIT;
SQL
}

db_count() {
  # .timeout sets a busy timeout (with no output) so a read that collides with
  # a concurrent writer or a hostile TRUNCATE checkpoint waits briefly instead
  # of erroring out with SQLITE_BUSY.
  sqlite3 "$1" ".timeout 5000" "SELECT COUNT(*) FROM items;"
}

# Report the write driver's committed row count. Takes an optional database
# path (default $DRILL_DB) so a drill running more than one database can ask
# about a specific one -- every single-database drill omits the argument and
# gets the historical behavior unchanged.
driver_count() {
  local db=${1:-$DRILL_DB}
  if [ -f "$DRILL_DRIVER_COUNT" ]; then
    cat "$DRILL_DRIVER_COUNT"
  else
    db_count "$db"
  fi
}

start_driver() {
  local db=$1
  local interval=${2:-0.05}
  local label=${3:-driver}
  local count
  rm -f "$DRILL_DRIVER_PAUSE"
  count=$(db_count "$db")
  printf '%s\n' "$count" >"$DRILL_DRIVER_COUNT"
  (
    set -Eeuo pipefail
    "$DRILL_PYTHON" - "$db" "$DRILL_DRIVER_COUNT" "$DRILL_DRIVER_PAUSE" "$interval" "$label" <<'PY'
import sqlite3
import sys
import time
import uuid
from pathlib import Path

db, count_path, pause_path, interval, label = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4]), sys.argv[5]
pause = Path(pause_path)
conn = sqlite3.connect(db, timeout=5.0, isolation_level=None)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA wal_autocheckpoint=0")
count = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
with open(count_path, "w", encoding="utf-8") as handle:
    handle.write(f"{count}\n")
while True:
    if pause.exists():
        time.sleep(interval)
        continue
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("INSERT INTO items(value) VALUES(?)", (f"{label}-{time.time_ns()}-{uuid.uuid4().hex}",))
    conn.execute("COMMIT")
    count += 1
    with open(count_path, "w", encoding="utf-8") as handle:
        handle.write(f"{count}\n")
    time.sleep(interval)
PY
  ) >"$DRILL_WORKDIR/driver.log" 2>&1 &
  DRILL_DRIVER_PID=$!
  log "write driver pid=$DRILL_DRIVER_PID"
}

# Pause the write driver and wait until its committed row count is stable.
# Takes an optional database path (default $DRILL_DB): a drill whose driver
# targets a second database passes that path so the stability check samples the
# right file, instead of trivially comparing an unrelated, already-static
# $DRILL_DB against itself.
pause_driver() {
  local db=${1:-$DRILL_DB}
  [ -n "${DRILL_DRIVER_PID:-}" ] || fail "write driver is not running"
  touch "$DRILL_DRIVER_PAUSE"
  # Wait until the driver stops committing, polling the authoritative committed
  # row count from the DB itself. The driver writes its count file just AFTER
  # each commit, so that file can lag the DB by one row; sampling it here (as
  # this used to) let pause_driver return a stale count while the DB already
  # held one more committed row, producing an off-by-one restore mismatch.
  local before
  local after
  before=$(db_count "$db")
  while :; do
    sleep "$DRILL_POLL_INTERVAL"
    after=$(db_count "$db")
    if [ "$after" = "$before" ]; then
      printf '%s\n' "$after" >"$DRILL_DRIVER_COUNT"
      return 0
    fi
    before=$after
  done
}

resume_driver() {
  [ -n "${DRILL_DRIVER_PID:-}" ] || fail "write driver is not running"
  rm -f "$DRILL_DRIVER_PAUSE"
}

stop_driver() {
  if [ -n "${DRILL_DRIVER_PID:-}" ] && kill -0 "$DRILL_DRIVER_PID" >/dev/null 2>&1; then
    # The driver is a subshell whose python child does the actual writing.
    # Killing only the subshell orphaned that python child, which kept
    # committing rows and rewriting its count file forever (an orphan from an
    # old drill run was found still alive on the dev box). Kill the child
    # first, then the subshell.
    pkill -TERM -P "$DRILL_DRIVER_PID" >/dev/null 2>&1 || true
    kill "$DRILL_DRIVER_PID" >/dev/null 2>&1 || true
    wait "$DRILL_DRIVER_PID" >/dev/null 2>&1 || true
  fi
  if [ -n "${DRILL_DB:-}" ] && [ -f "$DRILL_DB" ]; then
    db_count "$DRILL_DB" >"$DRILL_DRIVER_COUNT" 2>/dev/null || true
  fi
  DRILL_DRIVER_PID=
}

start_walrust() {
  local db=$1
  shift || true
  "$WALRUST_BIN" watch "$db" \
    --bucket "$DRILL_BUCKET_URI" \
    --endpoint "$DRILL_ENDPOINT" \
    --snapshot-interval "${WALRUST_DRILL_SNAPSHOT_INTERVAL:-999999}" \
    --wal-sync-interval "${WALRUST_DRILL_WAL_SYNC_INTERVAL:-1}" \
    --checkpoint-interval "${WALRUST_DRILL_CHECKPOINT_INTERVAL:-999999}" \
    --on-startup true \
    --no-metrics \
    "$@" >"$DRILL_WORKDIR/walrust.log" 2>&1 &
  DRILL_WALRUST_PID=$!
  log "walrust pid=$DRILL_WALRUST_PID"
  wait_process_alive "$DRILL_WALRUST_PID" "walrust watch"
}

stop_walrust() {
  if [ -n "${DRILL_WALRUST_PID:-}" ] && kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1; then
    kill "$DRILL_WALRUST_PID" >/dev/null 2>&1 || true
    wait "$DRILL_WALRUST_PID" >/dev/null 2>&1 || true
  fi
  DRILL_WALRUST_PID=
}

pid_is_walrust() {
  local pid=$1
  ps -p "$pid" -o command= 2>/dev/null | grep -q '[w]alrust'
}

kill_walrust_pid_verified() {
  local pid=$DRILL_WALRUST_PID
  [ -n "$pid" ] || fail "walrust pid is empty"
  kill -0 "$pid" >/dev/null 2>&1 || fail "walrust pid $pid is not running"
  pid_is_walrust "$pid" || fail "refusing to kill pid $pid because it is not walrust"
  kill -9 "$pid"
  wait "$pid" >/dev/null 2>&1 || true
  DRILL_WALRUST_PID=
  log "killed walrust pid=$pid"
}

restart_walrust_pid_verified() {
  local db=$1
  kill_walrust_pid_verified
  start_walrust "$db"
}

wait_process_alive() {
  local pid=$1
  local label=$2
  local deadline=$((SECONDS + 5))
  while [ "$SECONDS" -lt "$deadline" ]; do
    kill -0 "$pid" >/dev/null 2>&1 && return 0
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "$label did not stay alive"
}

wait_for_shadow_blocker() {
  local db=$1
  local deadline=$((SECONDS + ${WALRUST_DRILL_READY_TIMEOUT:-45}))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if sqlite3 "$db" "SELECT COUNT(*) FROM sqlite_master WHERE name = '_walrust_seq';" 2>/dev/null | grep -q '^1$'; then
      return 0
    fi
    if [ -n "${DRILL_WALRUST_PID:-}" ] && ! kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1; then
      fail "walrust exited before creating _walrust_seq; see $DRILL_WORKDIR/walrust.log"
    fi
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "timed out waiting for _walrust_seq checkpoint blocker"
}

run_restore_to() {
  local name=$1
  local output=$2
  shift 2
  # Restore intentionally refuses any existing SQLite destination or sidecar.
  # These paths are private drill artifacts, so clear the complete prior test
  # destination rather than weakening the production no-clobber gate.
  rm -f "$output" "$output-wal" "$output-shm"
  "$WALRUST_BIN" restore "$name" \
    --output "$output" \
    --bucket "$DRILL_BUCKET_URI" \
    --endpoint "$DRILL_ENDPOINT" \
    "$@"
}

integrity_ok() {
  local db=$1
  local result
  result=$(sqlite3 "$db" "PRAGMA integrity_check;")
  [ "$result" = "ok" ] || fail "integrity_check failed for $db: $result"
}

assert_restored_count_once() {
  local name=$1
  local expected=$2
  shift 2
  local output="$DRILL_RESTORE"
  DRILL_LAST_RESTORE_ERROR=
  DRILL_LAST_ACTUAL=
  if ! DRILL_LAST_RESTORE_ERROR=$(run_restore_to "$name" "$output" "$@" 2>&1); then
    return 1
  fi
  integrity_ok "$output"
  DRILL_LAST_ACTUAL=$(db_count "$output")
  [ "$DRILL_LAST_ACTUAL" = "$expected" ]
}

wait_restore_count() {
  local name=$1
  local expected=$2
  shift 2
  local timeout=${DRILL_RESTORE_TIMEOUT}
  local deadline=$((SECONDS + timeout))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if assert_restored_count_once "$name" "$expected" "$@"; then
      log "restore rows ok: $expected"
      return 0
    fi
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "ROW DIFF: restored row count mismatch for $name; expected rows=$expected actual rows=${DRILL_LAST_ACTUAL:-unavailable}; restore output: ${DRILL_LAST_RESTORE_ERROR:-none}"
}

# Like wait_restore_count, but asserts the restored row count is at least
# `min` instead of exactly `expected`. Use this for progress checks taken while
# the write driver is still running, where an exact live count is a moving
# target that the restore would race past. Still fails loudly (nonzero) if the
# restore never succeeds or never reaches the lower bound within the timeout.
wait_restore_count_at_least() {
  local name=$1
  local min=$2
  shift 2
  local deadline=$((SECONDS + DRILL_RESTORE_TIMEOUT))
  while [ "$SECONDS" -lt "$deadline" ]; do
    DRILL_LAST_RESTORE_ERROR=
    DRILL_LAST_ACTUAL=
    if DRILL_LAST_RESTORE_ERROR=$(run_restore_to "$name" "$DRILL_RESTORE" "$@" 2>&1); then
      integrity_ok "$DRILL_RESTORE"
      DRILL_LAST_ACTUAL=$(db_count "$DRILL_RESTORE")
      if [ "$DRILL_LAST_ACTUAL" -ge "$min" ]; then
        log "restore rows ok (>= $min): $DRILL_LAST_ACTUAL"
        return 0
      fi
    fi
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "ROW DIFF: restore never reached >= $min for $name; last actual rows=${DRILL_LAST_ACTUAL:-unavailable}; restore output: ${DRILL_LAST_RESTORE_ERROR:-none}"
}

# Wait until the write driver has committed at least `min` rows. Takes an
# optional database path (default $DRILL_DB) as the third argument so a
# multi-database drill can poll the correct one.
wait_driver_count_at_least() {
  local min=$1
  local timeout=${2:-30}
  local db=${3:-$DRILL_DB}
  local deadline=$((SECONDS + timeout))
  local count
  while [ "$SECONDS" -lt "$deadline" ]; do
    count=$(db_count "$db")
    printf '%s\n' "$count" >"$DRILL_DRIVER_COUNT"
    if [ "$count" -ge "$min" ]; then
      return 0
    fi
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "timed out waiting for driver count >= $min; got $(driver_count "$db")"
}

latest_txid() {
  local keys="$DRILL_WORKDIR/keys.txt"
  s3_list_prefix "$DRILL_RUN_PREFIX/" >"$keys"
  "$DRILL_PYTHON" - "$keys" <<'PY'
import re
import sys
max_txid = 0
with open(sys.argv[1], encoding="utf-8") as handle:
  for key in handle:
    m = re.search(r'/published/([0-9a-f]{16})\.json$', key.strip())
    if m:
        max_txid = max(max_txid, int(m.group(1), 16))
print(max_txid)
PY
}

snapshot_txids() {
  local keys="$DRILL_WORKDIR/keys.txt"
  s3_list_prefix "$DRILL_RUN_PREFIX/" >"$keys"
  "$DRILL_PYTHON" - "$keys" <<'PY'
import re
import sys
vals = set()
with open(sys.argv[1], encoding="utf-8") as handle:
  for key in handle:
    key = key.strip()
    m = re.search(r'/lineages/[^/]+/([0-9a-f]{4})/([0-9a-f]{16})\.hadbp$', key)
    if not m:
        continue
    generation = int(m.group(1), 16)
    seq = int(m.group(2), 16)
    if generation == 1:
        vals.add(seq)
for txid in sorted(vals):
    print(txid)
PY
}

first_incremental_key() {
  s3_list_prefix "$DRILL_RUN_PREFIX/" | awk '/\/lineages\/[^/]+\/0000\/[0-9a-f]+\.hadbp$/ { print; exit }'
}

run_prune() {
  local name=$1
  "$WALRUST_BIN" prune "$name" \
    --bucket "$DRILL_BUCKET_URI" \
    --endpoint "$DRILL_ENDPOINT" \
    --hourly "${WALRUST_DRILL_RETAIN_HOURLY:-2}" \
    --daily "${WALRUST_DRILL_RETAIN_DAILY:-0}" \
    --weekly "${WALRUST_DRILL_RETAIN_WEEKLY:-0}" \
    --monthly "${WALRUST_DRILL_RETAIN_MONTHLY:-0}" \
    --force
}


expect_future_txid_error() {
  local name=$1
  local txid=$2
  local out="$DRILL_WORKDIR/future.db"
  if run_restore_to "$name" "$out" --point-in-time "$txid" >"$DRILL_WORKDIR/future.out" 2>&1; then
    fail "future TXID restore unexpectedly succeeded: $txid"
  fi
  [ ! -e "$out" ] || fail "future TXID restore left an output file behind: $out"
  log "future TXID restore failed as expected (no file): $txid"
}

force_truncate_checkpoint() {
  local db=$1
  sqlite3 "$db" "PRAGMA wal_checkpoint(TRUNCATE);"
}

start_replica() {
  local source=$1
  local local_db=$2
  "$WALRUST_BIN" replicate "$source" \
    --local "$local_db" \
    --interval "${WALRUST_DRILL_REPLICA_INTERVAL:-1s}" \
    --endpoint "$DRILL_ENDPOINT" >"$DRILL_WORKDIR/replica.log" 2>&1 &
  DRILL_REPLICA_PID=$!
  log "replica pid=$DRILL_REPLICA_PID"
  wait_process_alive "$DRILL_REPLICA_PID" "walrust replicate"
}

stop_replica() {
  if [ -n "${DRILL_REPLICA_PID:-}" ] && kill -0 "$DRILL_REPLICA_PID" >/dev/null 2>&1; then
    kill "$DRILL_REPLICA_PID" >/dev/null 2>&1 || true
    wait "$DRILL_REPLICA_PID" >/dev/null 2>&1 || true
  fi
  DRILL_REPLICA_PID=
}

wait_replica_count() {
  local replica=$1
  local expected=$2
  local timeout=${3:-60}
  local deadline=$((SECONDS + timeout))
  local actual=unavailable
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ -f "$replica" ]; then
      integrity_ok "$replica"
      actual=$(db_count "$replica")
      if [ "$actual" = "$expected" ]; then
        log "replica rows ok: $expected"
        return 0
      fi
    fi
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "ROW DIFF: replica row count mismatch; expected rows=$expected actual rows=$actual"
}

# ---------------------------------------------------------------------------
# Shared helpers for bench/ scripts (append-only extension).
#
# bench/ scripts source this library to reuse the write driver and process
# management instead of duplicating them. Everything below is additive: no
# drill behavior changes. Drills are correctness gates; bench scripts are
# measurement tools that reuse this plumbing.
# ---------------------------------------------------------------------------

# Spawn an additional write driver with caller-owned count/pause files and log
# directory. Reuses start_driver (single source of driver code) by swapping the
# globals it reads, then restoring them. Sets SPAWNED_DRIVER_PID.
# shellcheck disable=SC2034  # read by sourcing bench scripts
SPAWNED_DRIVER_PID=
spawn_driver() {
  local db=$1
  local count_file=$2
  local pause_file=$3
  local interval=$4
  local label=$5
  local logdir=$6
  local s_count=$DRILL_DRIVER_COUNT
  local s_pause=$DRILL_DRIVER_PAUSE
  local s_workdir=$DRILL_WORKDIR
  local s_pid=${DRILL_DRIVER_PID:-}
  mkdir -p "$logdir"
  DRILL_DRIVER_COUNT=$count_file
  DRILL_DRIVER_PAUSE=$pause_file
  DRILL_WORKDIR=$logdir
  start_driver "$db" "$interval" "$label"
  # shellcheck disable=SC2034  # read by sourcing bench scripts
  SPAWNED_DRIVER_PID=$DRILL_DRIVER_PID
  DRILL_DRIVER_COUNT=$s_count
  DRILL_DRIVER_PAUSE=$s_pause
  DRILL_WORKDIR=$s_workdir
  DRILL_DRIVER_PID=$s_pid
}

# Pause a spawned driver via its pause file and wait (bounded) until the DB
# commit count is stable, mirroring pause_driver's convergence logic.
pause_driver_files() {
  local db=$1
  local pause_file=$2
  local timeout=${3:-30}
  touch "$pause_file"
  local before
  local after
  local deadline=$((SECONDS + timeout))
  before=$(db_count "$db")
  while [ "$SECONDS" -lt "$deadline" ]; do
    sleep "$DRILL_POLL_INTERVAL"
    after=$(db_count "$db")
    if [ "$after" = "$before" ]; then
      return 0
    fi
    before=$after
  done
  fail "driver for $db did not quiesce within ${timeout}s"
}

# Stop an arbitrary spawned process by PID (driver, sampler, probe).
# Kills direct children FIRST: a driver is a subshell whose python child
# would otherwise be orphaned by killing only the subshell and keep writing
# its count file forever (racing directory cleanup).
stop_pid() {
  local pid=${1:-}
  if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
    pkill -TERM -P "$pid" >/dev/null 2>&1 || true
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
}

# RSS sampler: append "epoch_seconds<TAB>rss_kb" to outfile every interval
# seconds until the target pid exits. Run via start_rss_sampler; sets
# SPAWNED_RSS_SAMPLER_PID. The sleep here is pacing, not synchronization.
rss_sampler_loop() {
  local pid=$1
  local interval=$2
  local outfile=$3
  local rss
  while kill -0 "$pid" >/dev/null 2>&1; do
    rss=$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')
    if [ -n "$rss" ]; then
      printf '%s\t%s\n' "$(date +%s)" "$rss" >>"$outfile"
    fi
    sleep "$interval"
  done
}

# shellcheck disable=SC2034  # read by sourcing bench scripts
SPAWNED_RSS_SAMPLER_PID=
start_rss_sampler() {
  rss_sampler_loop "$1" "$2" "$3" &
  # shellcheck disable=SC2034  # read by sourcing bench scripts
  SPAWNED_RSS_SAMPLER_PID=$!
}

# ---------------------------------------------------------------------------
# Local MinIO container + request-trace helpers (bench scripts and CI).
# ---------------------------------------------------------------------------

# Start a MinIO container with a pre-created bucket (created directly in the
# data dir before the server starts, so no client bootstrap is needed).
# Usage: start_minio_container NAME HOST_PORT BUCKET
start_minio_container() {
  local name=$1
  local port=$2
  local bucket=$3
  require_cmd docker
  require_cmd curl
  docker run -d --name "$name" -p "$port:9000" \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    --entrypoint sh \
    minio/minio:latest \
    -c "mkdir -p /data/$bucket && exec minio server /data" >/dev/null
  local deadline=$((SECONDS + 60))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -sf "http://127.0.0.1:$port/minio/health/live" >/dev/null 2>&1; then
      log "minio container $name healthy on port $port (bucket $bucket)"
      return 0
    fi
    sleep 1
  done
  docker logs "$name" >&2 2>/dev/null || true
  fail "MinIO container $name did not become healthy"
}

stop_minio_container() {
  local name=${1:-}
  if [ -n "$name" ]; then
    docker rm -f "$name" >/dev/null 2>&1 || true
  fi
}

# Start an `mc admin trace --json -v` sidecar sharing the MinIO container's
# network namespace. Every S3 request (api, path, headers incl. User-Agent)
# is captured as JSON lines retrievable via stop_minio_trace.
# Usage: start_minio_trace TRACE_NAME MINIO_NAME
start_minio_trace() {
  local trace_name=$1
  local minio_name=$2
  docker run -d --name "$trace_name" \
    --network "container:$minio_name" \
    --entrypoint sh \
    minio/mc \
    -c 'mc alias set local http://127.0.0.1:9000 minioadmin minioadmin >/dev/null && exec mc admin trace --json -v local' >/dev/null
  # Bounded wait for the tracer to attach (its first log line is the alias add).
  local deadline=$((SECONDS + 30))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$trace_name" 2>/dev/null)" = "true" ]; then
      log "minio trace container $trace_name running"
      return 0
    fi
    sleep 1
  done
  docker logs "$trace_name" >&2 2>/dev/null || true
  fail "MinIO trace container $trace_name did not start"
}

# Snapshot the trace log to a file and remove the tracer.
# Usage: stop_minio_trace TRACE_NAME OUTFILE
stop_minio_trace() {
  local trace_name=$1
  local outfile=$2
  docker logs "$trace_name" >"$outfile" 2>/dev/null || true
  docker rm -f "$trace_name" >/dev/null 2>&1 || true
}

# Start walrust in config-file mode (multi-database watch via walrust.toml).
# Mirrors start_walrust's process handling; sync knobs come from the config
# file rather than CLI flags so nothing silently overrides it.
# Usage: start_walrust_config CONFIG_TOML BUCKET_URI LOGFILE
start_walrust_config() {
  local config=$1
  local bucket_uri=$2
  local logfile=$3
  "$WALRUST_BIN" watch \
    --config "$config" \
    --bucket "$bucket_uri" \
    --endpoint "$DRILL_ENDPOINT" \
    --no-metrics \
    >"$logfile" 2>&1 &
  DRILL_WALRUST_PID=$!
  log "walrust (config mode) pid=$DRILL_WALRUST_PID"
  wait_process_alive "$DRILL_WALRUST_PID" "walrust watch --config"
}
