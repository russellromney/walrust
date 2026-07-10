#!/usr/bin/env bash
#
# Shared helpers for bench/ scripts. Bench scripts source drills/lib.sh FIRST
# (write driver, walrust process management, restore assertions, S3 cleanup,
# MinIO container/trace helpers) and this file second for the bench-only
# pieces: litestream process management, results directories, and the
# litestream validity check.
#
# Contract reminder: drills/ = correctness gates; bench/ = measurement.
# Bench never gates PRs, but every bench run ends with the same restore +
# row-count validity check the drills use — a benchmark of a broken sync
# must not produce numbers.

# Not a standalone script; sourced after drills/lib.sh.
[ -n "${DRILL_DIR:-}" ] || {
  echo "bench/common.sh must be sourced after drills/lib.sh" >&2
  exit 1
}

BENCH_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Create bench/results-<utc-timestamp>/ (gitignored) and echo its path.
bench_results_dir() {
  local dir
  dir="$BENCH_DIR/results-$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$dir"
  printf '%s\n' "$dir"
}

# Ensure litestream is installed; on macOS try brew (with retries for a flaky
# network). Prints the version.
ensure_litestream() {
  if ! command -v litestream >/dev/null 2>&1; then
    if command -v brew >/dev/null 2>&1; then
      local attempt
      for attempt in 1 2 3; do
        if brew install litestream >/dev/null 2>&1; then
          break
        fi
        log "brew install litestream failed (attempt $attempt), retrying"
        sleep 5
      done
    fi
  fi
  require_cmd litestream
  litestream version
}

# Write a litestream v0.5 config for N databases.
# Usage: write_litestream_config OUTFILE ENDPOINT BUCKET SYNC_INTERVAL_SECONDS DB_PATH:REPLICA_PATH [...]
# Each remaining arg is "local_db_path:replica_path_within_bucket".
write_litestream_config() {
  local outfile=$1
  local endpoint=$2
  local bucket=$3
  local sync_interval=$4
  shift 4
  : >"$outfile"
  printf 'dbs:\n' >>"$outfile"
  local pair db_path replica_path
  for pair in "$@"; do
    db_path=${pair%%:*}
    replica_path=${pair#*:}
    {
      printf '  - path: %s\n' "$db_path"
      printf '    replica:\n'
      printf '      type: s3\n'
      printf '      bucket: %s\n' "$bucket"
      printf '      path: %s\n' "$replica_path"
      printf '      endpoint: %s\n' "$endpoint"
      printf '      region: us-east-1\n'
      printf '      access-key-id: %s\n' "${AWS_ACCESS_KEY_ID:-minioadmin}"
      printf '      secret-access-key: %s\n' "${AWS_SECRET_ACCESS_KEY:-minioadmin}"
      printf '      sync-interval: %ss\n' "$sync_interval"
    } >>"$outfile"
  done
}

BENCH_LITESTREAM_PID=
start_litestream() {
  local config=$1
  local logfile=$2
  litestream replicate -config "$config" >"$logfile" 2>&1 &
  BENCH_LITESTREAM_PID=$!
  log "litestream pid=$BENCH_LITESTREAM_PID"
  wait_process_alive "$BENCH_LITESTREAM_PID" "litestream replicate"
}

pid_is_litestream() {
  local pid=$1
  ps -p "$pid" -o command= 2>/dev/null | grep -q '[l]itestream'
}

stop_litestream() {
  if [ -n "${BENCH_LITESTREAM_PID:-}" ] && kill -0 "$BENCH_LITESTREAM_PID" >/dev/null 2>&1; then
    # PID-verified: refuse to signal a recycled PID that isn't litestream.
    if pid_is_litestream "$BENCH_LITESTREAM_PID"; then
      kill "$BENCH_LITESTREAM_PID" >/dev/null 2>&1 || true
      wait "$BENCH_LITESTREAM_PID" >/dev/null 2>&1 || true
    fi
  fi
  BENCH_LITESTREAM_PID=
}

# Validity check for a litestream-replicated database: restore (bounded
# retries), integrity_check, exact row-count match against the driver's
# ground truth. Mirrors the drills' wait_restore_count contract.
# Usage: wait_litestream_restore_count CONFIG DB_PATH EXPECTED OUTPUT
wait_litestream_restore_count() {
  local config=$1
  local db_path=$2
  local expected=$3
  local output=$4
  local deadline=$((SECONDS + DRILL_RESTORE_TIMEOUT))
  local actual=unavailable
  local restore_err=none
  while [ "$SECONDS" -lt "$deadline" ]; do
    rm -f "$output"
    if restore_err=$(litestream restore -config "$config" -o "$output" "$db_path" 2>&1); then
      integrity_ok "$output"
      actual=$(db_count "$output")
      if [ "$actual" = "$expected" ]; then
        log "litestream restore rows ok: $expected"
        return 0
      fi
    fi
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "ROW DIFF: litestream restore mismatch for $db_path; expected rows=$expected actual rows=$actual; restore output: $restore_err"
}
