#!/usr/bin/env bash
#
# Fresh-user drill (dogfooding item 2): a stranger reads the README, installs
# walrust from crates.io, and tries to protect a database — no repo checkout,
# no improvising. Every command below is traceable to a README instruction
# (the README section is named in a comment above each step). Every place the
# README's literal instructions fail, are ambiguous, or omit a required step
# is recorded as a numbered FINDING in the output; the same PR that added this
# drill fixed each finding in the README. The drill still fails loudly on real
# product errors — a docs deviation is a finding, a wrong restore is a failure.
#
# Deliberately does NOT source drills/lib.sh: the whole point is that no repo
# code (including the repo-built walrust binary) is on any path the drill uses
# at runtime. The only repo file involved is this script.
#
# Requirements (see .github/workflows/fresh-user.yml for the CI shape):
#   - cargo + network access to crates.io (the drill installs the published
#     walrust into an isolated --root prefix; the workspace binary is never
#     used, and a guard asserts that)
#   - sqlite3 (the user's own tool, used for ground-truth checks only)
#   - AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY and an S3 endpoint via
#     AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL (MinIO in CI, anything locally)
#   - a bucket via WALRUST_DRILL_BUCKET / WALRUST_TEST_BUCKET /
#     TIERED_TEST_BUCKET / S3_TEST_BUCKET (same convention as drills/lib.sh)
#
# Knobs:
#   WALRUST_DRILL_INDUCE_LOSS=1   one-shot teeth proof: deletes every object
#                                 under the run prefix right before the main
#                                 restore; the drill MUST then fail loudly.
#                                 Requires python3 with the minio module.
#   WALRUST_DRILL_KEEP_ARTIFACTS=1  keep the temp dir and the S3 prefix.
#   WALRUST_DRILL_DEADLINE_SECS   per-gate poll deadline (default 180).
#
# Local example against MinIO:
#   docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin \
#     -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
#   AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
#   AWS_ENDPOINT_URL_S3=http://127.0.0.1:9000 TIERED_TEST_BUCKET=walrust-test \
#   drills/fresh-user.sh
# or: make drill-fresh-user

set -Eeuo pipefail

DRILL_NAME=fresh-user
DEADLINE=${WALRUST_DRILL_DEADLINE_SECS:-180}
INDUCE_LOSS=${WALRUST_DRILL_INDUCE_LOSS:-0}
INSTALL_RETRIES=${WALRUST_DRILL_INSTALL_RETRIES:-3}
MAX_PITR_PROBES=${WALRUST_DRILL_MAX_PITR_PROBES:-60}

log() {
  printf '[%s] %s\n' "$DRILL_NAME" "$*" >&2
}

WATCH_LOG=
fail() {
  printf '[%s] ERROR: %s\n' "$DRILL_NAME" "$*" >&2
  if [ -n "$WATCH_LOG" ] && [ -f "$WATCH_LOG" ]; then
    printf '[%s] --- last 40 lines of %s ---\n' "$DRILL_NAME" "$WATCH_LOG" >&2
    tail -n 40 "$WATCH_LOG" >&2 || true
  fi
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

require_cmd sqlite3
require_cmd cargo

# --- environment (the part the README never mentions; see FINDING F1) -------

# Same bucket-env convention as drills/lib.sh, reimplemented here because this
# drill must not source repo code.
RAW_BUCKET=${WALRUST_DRILL_BUCKET:-${WALRUST_TEST_BUCKET:-${WALRUST_S3_TEST_BUCKET:-${TIERED_TEST_BUCKET:-${S3_TEST_BUCKET:-}}}}}
RAW_BUCKET=${RAW_BUCKET#s3://}
BUCKET_NAME=${RAW_BUCKET%%/*}
BASE_PREFIX=
if [[ "$RAW_BUCKET" == */* ]]; then
  BASE_PREFIX=${RAW_BUCKET#*/}
fi
[ -n "$BUCKET_NAME" ] || fail "set WALRUST_DRILL_BUCKET, WALRUST_TEST_BUCKET, TIERED_TEST_BUCKET, or S3_TEST_BUCKET"

ENDPOINT=${WALRUST_DRILL_ENDPOINT:-${AWS_ENDPOINT_URL_S3:-${AWS_ENDPOINT_URL:-}}}
[ -n "$ENDPOINT" ] || fail "set WALRUST_DRILL_ENDPOINT, AWS_ENDPOINT_URL_S3, or AWS_ENDPOINT_URL"
[ -n "${AWS_ACCESS_KEY_ID:-}" ] || fail "AWS_ACCESS_KEY_ID must be set"
[ -n "${AWS_SECRET_ACCESS_KEY:-}" ] || fail "AWS_SECRET_ACCESS_KEY must be set"

# FINDING F1 (fixed in README "Quick start"): the README never said that
# walrust reads standard AWS_* env vars, that AWS_REGION must be set for the
# AWS SDK, or that the bare `walrust list/restore/verify` examples only reach a
# non-AWS endpoint if AWS_ENDPOINT_URL_S3 is exported. Without this block,
# nothing in the README works. A probe below demonstrates it.
export AWS_ENDPOINT_URL_S3="$ENDPOINT"
export AWS_REGION="${AWS_REGION:-us-east-1}"

RUN_PREFIX="drills/$DRILL_NAME-$(date +%Y%m%d%H%M%S)-$$"
if [ -n "$BASE_PREFIX" ]; then
  RUN_PREFIX="$BASE_PREFIX/$RUN_PREFIX"
fi
BUCKET_URI="s3://$BUCKET_NAME/$RUN_PREFIX"

WORK=$(mktemp -d "${TMPDIR:-/tmp}/walrust-$DRILL_NAME.XXXXXX")
PREFIX_DIR="$WORK/cargo-prefix" # isolated cargo install --root
USER_HOME="$WORK/home"          # fresh $HOME-like dir for the simulated user
MACHINE1="$WORK/machine1"       # primary machine
MACHINE2="$WORK/machine2"       # disaster-recovery machine
FINDINGS="$WORK/findings.txt"
mkdir -p "$PREFIX_DIR" "$USER_HOME" "$MACHINE1" "$MACHINE2"
: >"$FINDINGS"

finding() {
  local id=$1
  shift
  printf 'F%s: %s\n' "$id" "$*" >>"$FINDINGS"
  log "FINDING F$id: $*"
}

# S3 prefix deletion, used by the induce-loss teeth proof and by cleanup.
# Objects are passed via argv, never via a pipe into the heredoc (a heredoc
# claims stdin; see the note in drills/lib.sh).
s3_delete_prefix() {
  local prefix=$1
  python3 - "$BUCKET_NAME" "$prefix" <<'PY'
import os
import sys
from urllib.parse import urlparse
from minio import Minio
from minio.deleteobjects import DeleteObject

bucket, prefix = sys.argv[1], sys.argv[2]
endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
client = Minio(
    parsed.netloc or parsed.path,
    access_key=os.environ["AWS_ACCESS_KEY_ID"],
    secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    secure=parsed.scheme == "https",
)
objects = [DeleteObject(o.object_name) for o in client.list_objects(bucket, prefix=prefix, recursive=True)]
print(f"deleting {len(objects)} objects under {prefix}", file=sys.stderr)
for err in client.remove_objects(bucket, objects):
    print(err, file=sys.stderr)
PY
}

WATCH_PID=
stop_watch() {
  # Simulates the user hitting Ctrl-C on their `walrust watch` terminal.
  local pid=${WATCH_PID:-}
  WATCH_PID=
  [ -n "$pid" ] || return 0
  kill -0 "$pid" >/dev/null 2>&1 || return 0
  kill -INT "$pid" >/dev/null 2>&1 || true
  local waited=0
  while kill -0 "$pid" >/dev/null 2>&1 && [ "$waited" -lt 20 ]; do
    sleep 1
    waited=$((waited + 1))
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    # Not a docs finding: a watcher that survives SIGINT for 20s is a real
    # product problem. Fail loudly rather than mask it with SIGKILL.
    kill -9 "$pid" >/dev/null 2>&1 || true
    fail "walrust watch (pid $pid) did not exit within 20s of SIGINT"
  fi
  wait "$pid" >/dev/null 2>&1 || true
}

cleanup() {
  set +e
  if [ -n "${WATCH_PID:-}" ]; then
    kill -9 "$WATCH_PID" >/dev/null 2>&1
    wait "$WATCH_PID" >/dev/null 2>&1
  fi
  if [ "${WALRUST_DRILL_KEEP_ARTIFACTS:-0}" = "1" ]; then
    log "keeping artifacts in $WORK and $BUCKET_URI"
    return 0
  fi
  if python3 -c 'import minio' >/dev/null 2>&1; then
    s3_delete_prefix "$RUN_PREFIX" >/dev/null 2>&1
  else
    log "python3+minio unavailable; leaving S3 prefix $BUCKET_URI behind"
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

# Generic bounded poll: wait_until "description" deadline_secs command...
wait_until() {
  local what=$1
  local deadline_secs=$2
  shift 2
  local deadline=$((SECONDS + deadline_secs))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if "$@"; then
      return 0
    fi
    sleep 1
  done
  fail "timed out after ${deadline_secs}s waiting for: $what"
}

row_hash() {
  # Ground-truth content hash over the data (schema-independent, ordered).
  local db=$1
  local out
  out=$(sqlite3 "$db" ".timeout 5000" "SELECT id, value, COALESCE(note,'') FROM items ORDER BY id;") || return 1
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s\n' "$out" | sha256sum | awk '{print $1}'
  else
    printf '%s\n' "$out" | shasum -a 256 | awk '{print $1}'
  fi
}

row_count() {
  sqlite3 "$1" ".timeout 5000" "SELECT COUNT(*) FROM items;"
}

integrity_ok() {
  local db=$1
  local result
  result=$(sqlite3 "$db" "PRAGMA integrity_check;")
  [ "$result" = "ok" ] || fail "integrity_check failed for $db: $result"
}

# `walrust list` parsing. The per-database line is:
#   app (TXID: 42, 17 incrementals, snapshot gen 1 (TXID 12))
# That single line is EVERYTHING `list` gives a user — see FINDING F5.
list_output() {
  walrust list -b "$BUCKET_URI" 2>&1
}

current_txid() {
  list_output | sed -nE 's/^ *app \(TXID: ([0-9]+),.*/\1/p'
}

app_listed() {
  list_output | grep -qE '^ *app \(TXID: '
}

txid_at_least() {
  local floor=$1
  local cur
  cur=$(current_txid)
  [ -n "$cur" ] && [ "$cur" -gt "$floor" ]
}

# Wait until the bucket's TXID has advanced past `floor` and then stopped
# moving (two identical reads 2s apart). The user-visible equivalent of
# "my writes have replicated": README "Safety and design" says the recovery
# point is bounded by the ~1s sync interval, so a quiesced database's TXID
# settles within a couple of seconds.
wait_txid_settled() {
  local floor=$1
  wait_until "bucket TXID to advance past $floor" "$DEADLINE" txid_at_least "$floor"
  local prev cur
  prev=$(current_txid)
  local deadline=$((SECONDS + DEADLINE))
  while [ "$SECONDS" -lt "$deadline" ]; do
    sleep 2
    cur=$(current_txid)
    if [ -n "$cur" ] && [ "$cur" = "$prev" ]; then
      printf '%s\n' "$cur"
      return 0
    fi
    prev=$cur
  done
  fail "bucket TXID never settled (last=$prev)"
}

start_watch() {
  # README "Configuration": `walrust watch  # auto-discovers walrust.toml`.
  # Run from the directory holding walrust.toml, exactly as a user would.
  local dir=$1
  local logfile=$2
  WATCH_LOG=$logfile
  (cd "$dir" && exec walrust watch) >"$logfile" 2>&1 &
  WATCH_PID=$!
  log "walrust watch pid=$WATCH_PID (cwd=$dir, log=$logfile)"
}

watch_alive() {
  [ -n "${WATCH_PID:-}" ] && kill -0 "$WATCH_PID" >/dev/null 2>&1
}

# =============================================================================
# Step 0 — install walrust from crates.io.
# README "Quick start (CLI)": `cargo install walrust`.
# Drill mechanics, not README deviations: `--root` isolates the install in an
# empty prefix (proving no repo binary is involved) and `--locked` makes the
# run reproducible against the crate's own Cargo.lock.
# =============================================================================
log "installing walrust from crates.io into $PREFIX_DIR"
install_log="$WORK/cargo-install.log"
attempt=1
installed=0
while [ "$attempt" -le "$INSTALL_RETRIES" ]; do
  # cwd is outside any cargo workspace so no local .cargo/config.toml or
  # [patch] section can influence dependency resolution.
  # --color never: the version-parse below reads this log, and CI exports
  # CARGO_TERM_COLOR=always, which would thread ANSI escapes through the
  # "Installed package" line.
  if (cd "$WORK" && cargo install walrust --locked --color never --root "$PREFIX_DIR") >"$install_log" 2>&1; then
    installed=1
    break
  fi
  log "cargo install attempt $attempt/$INSTALL_RETRIES failed; retrying"
  attempt=$((attempt + 1))
  sleep 5
done
if [ "$installed" -ne 1 ]; then
  tail -n 40 "$install_log" >&2 || true
  fail "cargo install walrust --locked failed after $INSTALL_RETRIES attempts (full log: $install_log)"
fi

# Guard: the binary the drill uses is the crates.io one inside the isolated
# prefix — never the workspace build. (Wrong: testing the workspace binary.
# Right: crates.io binary only.)
export PATH="$PREFIX_DIR/bin:$PATH"
resolved=$(command -v walrust) || fail "walrust not on PATH after install"
[ "$resolved" = "$PREFIX_DIR/bin/walrust" ] \
  || fail "walrust resolved to $resolved, expected $PREFIX_DIR/bin/walrust — a non-crates.io binary is shadowing the drill"
# Strip any ANSI escapes before parsing, belt-and-braces alongside
# --color never above.
# shellcheck disable=SC2016  # the backticks are literal text in cargo's output
installed_version=$(sed -e $'s/\x1b\\[[0-9;]*m//g' "$install_log" \
  | sed -nE 's/.*Installed package `walrust v([0-9][^`]*)`.*/\1/p' | tail -n 1)
[ -n "$installed_version" ] || fail "could not parse installed version from $install_log"
reported_version=$(walrust --version | awk '{print $2}')
[ "$reported_version" = "$installed_version" ] \
  || fail "walrust --version reports '$reported_version' but cargo installed '$installed_version'"
log "installed walrust $installed_version from crates.io at $resolved"

# Everything from here on runs as the simulated fresh user: their own HOME,
# their own working directories, only the installed binary + sqlite3.
export HOME="$USER_HOME"

# =============================================================================
# Step 1 — demonstrate FINDING F1: the README's own commands, in the README's
# own environment (no AWS_* env vars), do not work. `walrust list -b ...` is
# the cheapest probe. `timeout` bounds the SDK's retry/backoff.
# =============================================================================
if command -v timeout >/dev/null 2>&1; then
  if env -u AWS_ENDPOINT_URL_S3 -u AWS_ENDPOINT_URL -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
    timeout 30 walrust list -b "$BUCKET_URI" >"$WORK/no-env-probe.log" 2>&1; then
    fail "ANOMALY: walrust list succeeded with no credentials and no endpoint — the F1 finding (and its README fix) need re-examination"
  fi
  finding 1 "README Quick start never mentioned credentials or endpoint env: with no AWS_* env vars the README's own 'walrust list -b ...' fails (probe exit captured in no-env-probe.log). Fixed: Quick start now shows the required AWS_* exports."
else
  finding 1 "README Quick start never mentioned credentials or endpoint env (probe skipped: no 'timeout' on this host). Fixed: Quick start now shows the required AWS_* exports."
fi

# =============================================================================
# Step 2 — the user creates their database with their own tool (sqlite3).
# The README never says the database must be in WAL mode (outside the
# library-embedders notes), and a stock `sqlite3` database is NOT in WAL mode.
# Create it exactly as a stranger would — no PRAGMAs — to see what happens.
# =============================================================================
cd "$MACHINE1"
sqlite3 app.db <<'SQL'
CREATE TABLE items (
  id INTEGER PRIMARY KEY,
  value TEXT NOT NULL,
  note TEXT
);
INSERT INTO items(value, note) VALUES ('seed-1', 'note-1'), ('seed-2', 'note-2'), ('seed-3', 'note-3');
SQL
log "created machine1 app.db (journal_mode=$(sqlite3 app.db 'PRAGMA journal_mode;'))"

# =============================================================================
# Step 3 — write the config the README shows.
# README "Configuration": walrust.toml with [s3] bucket/endpoint and
# [[databases]] path, then bare `walrust watch` auto-discovers it.
# =============================================================================
cat >walrust.toml <<TOML
[s3]
bucket = "$BUCKET_URI"
endpoint = "$ENDPOINT"

[[databases]]
path = "$MACHINE1/app.db"
TOML

# =============================================================================
# Step 4 — `walrust watch`, first attempt: expected to refuse the non-WAL
# database. That refusal is correct fail-loudly behavior, but the README never
# warned the CLI user — FINDING F2.
# =============================================================================
start_watch "$MACHINE1" "$WORK/watch1-nonwal.log"
refused=0
waited=0
while [ "$waited" -lt 30 ]; do
  if ! watch_alive; then
    refused=1
    break
  fi
  sleep 1
  waited=$((waited + 1))
done
if [ "$refused" -ne 1 ]; then
  fail "walrust watch accepted a non-WAL database (still running after 30s) — the F2 finding and the WAL-mode refusal both need re-examination"
fi
wait "$WATCH_PID" >/dev/null 2>&1 || true
WATCH_PID=
grep -qi "journal_mode" "$WORK/watch1-nonwal.log" \
  || fail "walrust watch exited on the fresh database but not for the expected journal_mode reason; see $WORK/watch1-nonwal.log"
finding 2 "README's CLI sections never said the database must already be in WAL mode: 'walrust watch' on a stock sqlite3 database refuses with a journal_mode error (correct fail-loudly behavior, undocumented for CLI users). Fixed: Quick start now says so and points at 'walrust pragma'."

# Remedy using the product's own guidance: README "Use as a library" notes
# say `walrust pragma` prints the recommended settings; apply them.
walrust pragma | sqlite3 app.db
[ "$(sqlite3 app.db 'PRAGMA journal_mode;')" = "wal" ] || fail "walrust pragma did not switch app.db to WAL mode"

# =============================================================================
# Step 5 — `walrust watch`, for real this time.
# README "Configuration": `walrust watch  # auto-discovers walrust.toml`.
# =============================================================================
start_watch "$MACHINE1" "$WORK/watch1.log"

# README "Quick start (CLI)" more-commands: `walrust list -b s3://my-bucket`.
# NOTE the name: we watched app.db, and list shows it as "app" — the file stem.
# The README's restore/verify examples said "mydb" while the watch example
# used app.db, and never defined the name — FINDING F3 (fixed: examples now
# use the same database throughout and the name rule is stated).
wait_until "database 'app' to appear in walrust list" "$DEADLINE" app_listed
watch_alive || fail "walrust watch exited while waiting for first sync; see $WORK/watch1.log"
finding 3 "README restore/verify examples used the name 'mydb' while the watch example used 'app.db', and the name rule (file stem of the watched path, as shown by 'walrust list') was never stated. Fixed: examples now agree and the rule is spelled out."

# =============================================================================
# Step 6 — the user's app writes rows while walrust watches.
# README "How it works": walrust polls the WAL and uploads changes (~1s sync
# interval per "Safety and design"), so pauses between batches let ticks land.
# =============================================================================
t0=$(current_txid)
for batch in 1 2 3; do
  sqlite3 app.db <<SQL
BEGIN IMMEDIATE;
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 12)
INSERT INTO items(value, note)
SELECT 'batch$batch-' || n, 'note-b$batch-' || n FROM seq;
COMMIT;
SQL
  sleep 1.2
done
settled_txid=$(wait_txid_settled "$t0")
log "machine1 writes settled at TXID $settled_txid ($(row_count app.db) rows)"

# =============================================================================
# Step 7 — verify the backup per the README.
# README "Quick start (CLI)" / "Monitoring": `walrust verify app -b ...`
# exits nonzero on real chain problems, so it can run in cron.
# =============================================================================
walrust verify app -b "$BUCKET_URI" >"$WORK/verify1.log" 2>&1 \
  || fail "walrust verify failed on a healthy freshly-watched bucket (exit $?); see $WORK/verify1.log"
log "walrust verify: ok"

# =============================================================================
# Step 7b — FINDING F4: the README's prune example was not a runnable command.
# "walrust prune -b s3://my-bucket" omits the required database-name
# positional and dies with a usage error before touching S3 (and prune is
# dry-run without --force anyway), so demonstrating it is safe.
# =============================================================================
if walrust prune -b "$BUCKET_URI" >"$WORK/prune-probe.log" 2>&1; then
  fail "ANOMALY: 'walrust prune -b ...' (README's literal example, missing the name positional) unexpectedly succeeded"
fi
finding 4 "README's prune example 'walrust prune -b s3://my-bucket' omitted the required database-name positional and cannot run (usage error). Fixed: the example is now 'walrust prune app -b s3://my-bucket'."

# =============================================================================
# Step 8 — stop the watcher (Ctrl-C), record ground truth, lose the database.
# =============================================================================
stop_watch
expected_count=$(row_count app.db)
expected_hash=$(row_hash app.db)
log "ground truth before disaster: $expected_count rows, hash $expected_hash"

rm -f app.db app.db-wal app.db-shm
log "deleted the local database (the disaster)"

# Induced-failure teeth proof: with the bucket emptied, the README restore
# below MUST fail loudly. If the drill still exits 0, the drill has no teeth.
if [ "$INDUCE_LOSS" = "1" ]; then
  python3 -c 'import minio' >/dev/null 2>&1 \
    || fail "WALRUST_DRILL_INDUCE_LOSS=1 requires python3 with the minio module"
  log "INDUCE_LOSS: deleting every object under $BUCKET_URI before restore"
  s3_delete_prefix "$RUN_PREFIX"
fi

# =============================================================================
# Step 9 — restore per the README and prove row-exactness.
# README "Quick start (CLI)": `walrust restore app -o restored.db -b s3://...`.
# sqlite3 is the user's own ground-truth tool, not a deviation.
# =============================================================================
walrust restore app -o restored.db -b "$BUCKET_URI" >"$WORK/restore1.log" 2>&1 \
  || fail "walrust restore failed (exit $?); see $WORK/restore1.log"
integrity_ok restored.db
restored_count=$(row_count restored.db)
restored_hash=$(row_hash restored.db)
[ "$restored_count" = "$expected_count" ] \
  || fail "ROW DIFF: restored $restored_count rows, expected $expected_count"
[ "$restored_hash" = "$expected_hash" ] \
  || fail "CONTENT DIFF: restored content hash $restored_hash != expected $expected_hash"
log "restore is row-exact: $restored_count rows, hash matches"

# =============================================================================
# Step 10 — carry on from the restore on a new machine (machine2), then run
# the bad-migration exercise.
# README "Safety and design" documents that a restart against an existing
# stream re-anchors with a fresh snapshot; continuing to replicate the same
# bucket after a restore is the disaster-recovery journey.
# =============================================================================
cp restored.db "$MACHINE2/app.db"
cd "$MACHINE2"
cat >walrust.toml <<TOML
[s3]
bucket = "$BUCKET_URI"
endpoint = "$ENDPOINT"

[[databases]]
path = "$MACHINE2/app.db"
TOML
# `walrust pragma` says to run its settings "once when creating your database,
# or on every connection" — apply to the restored copy before watching it.
walrust pragma | sqlite3 app.db

start_watch "$MACHINE2" "$WORK/watch2.log"
sleep 3
watch_alive || fail "walrust watch (machine2) exited during startup; see $WORK/watch2.log"

# Post-restore "good" writes: the state we will need to get back to. Written
# immediately after startup; the settle gate below proves both that the new
# watcher came up against the existing bucket AND that it shipped these rows.
sqlite3 app.db <<'SQL'
BEGIN IMMEDIATE;
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 20)
INSERT INTO items(value, note)
SELECT 'postrestore-' || n, 'note-pr-' || n FROM seq;
COMMIT;
SQL
sleep 1.2
good_txid_settled=$(wait_txid_settled "$settled_txid")
pre_migration_count=$(row_count app.db)
pre_migration_hash=$(row_hash app.db)
log "pre-migration state settled (bucket TXID $good_txid_settled): $pre_migration_count rows, hash $pre_migration_hash"

# The bad migration: destructive schema + data change, synced to the bucket.
sqlite3 app.db <<'SQL'
BEGIN IMMEDIATE;
DELETE FROM items WHERE id % 3 = 0;
COMMIT;
ALTER TABLE items DROP COLUMN note;
SQL
sleep 1.2
bad_txid_settled=$(wait_txid_settled "$good_txid_settled")
log "bad migration synced (bucket TXID $bad_txid_settled)"

stop_watch

# Restore-to-latest must reflect the migration (the disaster is real and PITR
# is genuinely needed).
rm -f probe-latest.db
walrust restore app -o probe-latest.db -b "$BUCKET_URI" >"$WORK/restore-latest.log" 2>&1 \
  || fail "restore-to-latest failed after migration (exit $?); see $WORK/restore-latest.log"
latest_has_note=$(sqlite3 probe-latest.db "SELECT COUNT(*) FROM pragma_table_info('items') WHERE name='note';")
[ "$latest_has_note" = "0" ] \
  || fail "restore-to-latest still has the dropped column; the migration never reached the bucket"

# =============================================================================
# Step 11 — find the pre-migration restore point using ONLY what `walrust
# list` gives a user, then `walrust restore --point-in-time` back to it.
# README "Quick start (CLI)": `walrust restore app ... --point-in-time <txid>`.
#
# What `walrust list` actually shows (FINDING F5): one line per database with
# the CURRENT TXID and the NEWEST SNAPSHOT's TXID. No history enumeration, no
# timestamps, no way to correlate a TXID with "10 minutes ago, before my
# migration". Everything a user can infer:
#   - an upper bound: the current TXID (the migration is at or below it)
#   - a guaranteed-restorable floor: the newest snapshot's TXID
# So the only user-executable strategy is trial restores: walk TXIDs down from
# the current one, inspect each restored file with sqlite3, stop at the first
# pre-migration state. That is what this step does — the drill does NOT peek
# at S3 listings, walrust logs, or any internal knowledge to pick the TXID.
# (Fixed in README: the restore section now documents exactly this procedure
# and recommends noting `walrust list`'s TXID before risky migrations.)
# =============================================================================
final_list=$(list_output) || fail "walrust list failed after migration"
log "walrust list output the user works from:"
printf '%s\n' "$final_list" | sed 's/^/  | /' >&2
cur=$(printf '%s\n' "$final_list" | sed -nE 's/^ *app \(TXID: ([0-9]+),.*/\1/p')
floor=$(printf '%s\n' "$final_list" | sed -nE 's/^ *app \(TXID: [0-9]+,.*snapshot gen [0-9]+ \(TXID ([0-9]+)\)\).*/\1/p')
[ -n "$cur" ] || fail "could not parse current TXID from walrust list output"
if [ -z "$floor" ]; then
  floor=1
  log "list shows no snapshot; using TXID 1 as the search floor"
fi
finding 5 "walrust list shows only the current TXID ($cur) and the newest snapshot TXID ($floor) — no restore-point history and no timestamps, so 'the TXID from before my migration 10 minutes ago' cannot be read off; the only user strategy is trial --point-in-time restores walking down from the current TXID. Fixed: README now documents that procedure (and says to note the TXID before risky migrations)."

probes=0
found_txid=
t=$((cur - 1))
while [ "$t" -ge "$floor" ]; do
  probes=$((probes + 1))
  [ "$probes" -le "$MAX_PITR_PROBES" ] \
    || fail "no pre-migration state found within $MAX_PITR_PROBES trial restores (TXIDs $((t + 1))..$((cur - 1))); either history is damaged or the trial-restore strategy is unusable at this scale"
  rm -f probe.db
  if walrust restore app -o probe.db -b "$BUCKET_URI" --point-in-time "$t" >"$WORK/probe-$t.log" 2>&1; then
    has_note=$(sqlite3 probe.db "SELECT COUNT(*) FROM pragma_table_info('items') WHERE name='note';")
    cnt=$(row_count probe.db)
    log "probe TXID $t: restored ($cnt rows, note column present: $has_note)"
    if [ "$has_note" = "1" ] && [ "$cnt" = "$pre_migration_count" ]; then
      found_txid=$t
      break
    fi
    # Restorable but still post-migration (or mid-migration): keep walking.
  else
    log "probe TXID $t: not restorable ($(head -n 1 "$WORK/probe-$t.log" 2>/dev/null || echo 'no output')); continuing"
  fi
  t=$((t - 1))
done
[ -n "$found_txid" ] || fail "walked TXIDs $floor..$((cur - 1)) without finding the pre-migration state"

# Ground-truth gate: the state we found by the user's strategy must be
# row-exact against the recorded pre-migration state — a near-miss (right
# shape, wrong rows) is a hard failure, not a shrug.
integrity_ok probe.db
probe_hash=$(row_hash probe.db)
[ "$probe_hash" = "$pre_migration_hash" ] \
  || fail "CONTENT DIFF at PITR TXID $found_txid: hash $probe_hash != pre-migration $pre_migration_hash"
log "bad-migration recovery: pre-migration state found at TXID $found_txid after $probes trial restore(s), row-exact"

# =============================================================================
# Verdict
# =============================================================================
log "=================================================================="
log "PASS fresh-user drill"
log "  installed from crates.io: walrust $installed_version ($resolved)"
log "  machine1: $expected_count rows replicated, verified, restored row-exact"
log "  machine2: restored copy re-watched, bad migration recovered via"
log "            --point-in-time $found_txid ($probes trial restores)"
log "FINDINGS (each fixed in the README by the PR that added this drill):"
sed 's/^/  /' "$FINDINGS" >&2
log "=================================================================="
