#!/usr/bin/env bash
#
# Format-stability fixture generator (dogfooding item 1).
#
# Freezes two buckets written by the PUBLISHED walrust 0.7.0 artifacts into
# this directory, so tests/format_stability.rs can prove forever that current
# code restores buckets written by 0.7.0 row-exact (latest + PITR):
#
#   cli-v0.7.0/    written by the crates.io CLI binary
#                  (`cargo install walrust --version 0.7.0 --locked`) running
#                  `watch --independent-tasks` with leveled compaction at
#                  comedy knobs. Contains >= 2 snapshots, a live L0 tail,
#                  populated L1 AND L2 levels, and a prune boundary
#                  (`walrust prune` visibly deleted superseded objects).
#                  This layout interleaves litestream-heritage LTX L0 objects
#                  with HADBP `levels/L{n}/` merged objects -- the exact
#                  cross-format seam worth freezing.
#   owned-v0.7.0/  written by REGISTRY walrust-core 0.7.0 (a throwaway
#                  generator crate scaffolded in a scratch dir -- never in the
#                  repo, no path/git deps, no [patch]) using the supported
#                  library compaction path: `add_without_snapshot()` +
#                  `autonomous_snapshots` + a short `snapshot_interval`.
#                  Contains snapshots + levels + an L0 tail.
#
# Each fixture dir holds the raw objects mirrored under `objects/` (bucket
# keys become directory paths) plus MANIFEST.json recording the generator
# version, the exact knobs, the expected latest row-content checksum + row
# count, one mid-history PITR point (TXID/seq) with its expected checksum,
# and the full object index the proving test uploads from.
#
# Row-content checksum (canonical, shared with tests/format_stability.rs):
#   SHA-256 over the concatenation of "{id}|{value}\n" for
#   `SELECT id, value FROM items ORDER BY id`.
#
# MANUAL ONLY -- like drills/version-skew.sh, this needs crates.io network
# access (cargo install + a registry-dep generator crate build) on top of the
# live-S3 credentials every drill needs. Run it via soup:
#
#   soup run -p turbolite -e development -- tests/fixtures/format-stability/generate.sh
#
# Rerun with WALRUST_FIXTURE_VERSION=0.X.0 after a major format change to
# mint a new frozen-version fixture pair the same way.
#
# Known trap (greppable): retention minimum-2 floor. hadb-io's
# `RetentionPolicy` hard-codes `minimum: 2`; any sub-hour run collapses GFS
# hourly bucketing to one bucket, so the floor pads the keep-set with the
# ABSOLUTE OLDEST snapshot -- unconditionally protecting the on-startup
# snapshot and anchoring the prune watermark at the very start of history,
# where nothing can ever be below it. drills/prune-retained.sh documents the
# safe workaround this script reuses: once compaction has folded well past
# the early history (L1 AND L2 fired), hand-delete every snapshot OLDER than
# the recorded PITR snapshot, so the same minimum-2 floor is satisfied by
# recent survivors and prune has real work to do.

set -Eeuo pipefail

FIXTURE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

PIN_VERSION=${WALRUST_FIXTURE_VERSION:-0.7.0}
CORE_PIN_VERSION=${WALRUST_FIXTURE_CORE_VERSION:-$PIN_VERSION}
HADB_PIN_VERSION=${WALRUST_FIXTURE_HADB_VERSION:-0.5.0}

CLI_OUT_DIR="$FIXTURE_DIR/cli-v$PIN_VERSION"
OWNED_OUT_DIR="$FIXTURE_DIR/owned-v$PIN_VERSION"

# Comedy knobs, mirroring drills/compaction-soak.sh / prune-retained.sh.
L1_BATCH=4
L2_BATCH=3
KEEP_FINE="0s"
WAL_SYNC_INTERVAL=1
DRIVER_INTERVAL=${WALRUST_FIXTURE_DRIVER_INTERVAL:-0.08} # ~12 rows/sec
INSTALL_RETRIES=${WALRUST_FIXTURE_INSTALL_RETRIES:-3}

# ── Published CLI binary (never a workspace build) ─────────────────────────
#
# The whole point of the fixture is pinning what 0.7.0 ACTUALLY SHIPPED, so
# the bucket must be written by the crates.io artifact, not current main.
DRILL_NAME=format-fixture-gen
# shellcheck source=drills/lib.sh
source "$(cd "$FIXTURE_DIR/../../../drills" && pwd)/lib.sh"

BIN_CACHE_ROOT=${WALRUST_FIXTURE_BIN_CACHE:-"$REPO_ROOT/target/fixture-bin"}
PIN_BIN="$BIN_CACHE_ROOT/$PIN_VERSION/bin/walrust"
if [ ! -x "$PIN_BIN" ]; then
  log "installing walrust $PIN_VERSION from crates.io into $BIN_CACHE_ROOT/$PIN_VERSION (network required)"
  attempt=1
  until cargo install walrust --version "$PIN_VERSION" --locked \
    --root "$BIN_CACHE_ROOT/$PIN_VERSION" >"${TMPDIR:-/tmp}/fixture-cargo-install.log" 2>&1; do
    [ "$attempt" -lt "$INSTALL_RETRIES" ] \
      || fail "cargo install walrust=$PIN_VERSION failed after $INSTALL_RETRIES attempts (see ${TMPDIR:-/tmp}/fixture-cargo-install.log)"
    attempt=$((attempt + 1))
    sleep 3
  done
fi
[ -x "$PIN_BIN" ] || fail "no executable at $PIN_BIN after install"
WALRUST_BIN="$PIN_BIN"
PIN_BIN_VERSION_OUTPUT=$("$WALRUST_BIN" --version 2>&1)
case "$PIN_BIN_VERSION_OUTPUT" in
*"$PIN_VERSION"*) ;;
*) fail "installed binary reports '$PIN_BIN_VERSION_OUTPUT', expected version $PIN_VERSION" ;;
esac

WALRUST_DRILL_PREFIX=${WALRUST_DRILL_PREFIX:-"dogfood-fixture-bucket/gen-$(date +%Y%m%d%H%M%S)-$$"}
drill_setup
log "generating fixtures with $PIN_BIN_VERSION_OUTPUT at $DRILL_BUCKET_URI"

# ── Shared helpers ──────────────────────────────────────────────────────────

# Canonical row-content checksum (see header). sqlite3 prints one row per
# line, so the output stream IS the concatenation of "{id}|{value}\n".
rows_sha() {
  sqlite3 "$1" ".timeout 5000" "SELECT id||'|'||value FROM items ORDER BY id;" \
    | shasum -a 256 | awk '{print $1}'
}

# Download every object under bucket prefix $1 into local dir $2, mirroring
# key paths (keys only ever contain hex names, `levels/L{n}`, and the db
# name, so key-as-path is safe). Strips leading prefix $3 from each key.
s3_download_prefix() {
  "$DRILL_PYTHON" - "$DRILL_BUCKET_NAME" "$1" "$2" "$3" <<'PY'
import os
import sys
from urllib.parse import urlparse
from minio import Minio

bucket, prefix, outdir, strip = sys.argv[1:5]
endpoint = os.environ.get("WALRUST_DRILL_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
access = os.environ.get("AWS_ACCESS_KEY_ID")
secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
client = Minio(parsed.netloc or parsed.path, access_key=access, secret_key=secret, secure=parsed.scheme == "https")
count = 0
for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
    key = obj.object_name
    rel = key[len(strip):] if key.startswith(strip) else key
    dest = os.path.join(outdir, rel)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    client.fget_object(bucket, key, dest)
    count += 1
if count == 0:
    raise SystemExit(f"downloaded zero objects from {bucket}/{prefix}")
print(count)
PY
}

# Write MANIFEST.json for a fixture. Args:
#   $1 out_dir  $2 fixture_name  $3 generator_json  $4 config_json
#   $5 db_name  $6 latest_rows  $7 latest_sha  $8 pitr_txid  $9 pitr_rows
#   $10 pitr_sha
write_manifest() {
  "$DRILL_PYTHON" - "$@" <<'PY'
import json
import os
import sys

(out_dir, fixture, generator_json, config_json, db_name,
 latest_rows, latest_sha, pitr_txid, pitr_rows, pitr_sha) = sys.argv[1:11]

objects_dir = os.path.join(out_dir, "objects")
objects = []
for root, _dirs, files in os.walk(objects_dir):
    for fn in sorted(files):
        path = os.path.join(root, fn)
        key = os.path.relpath(path, objects_dir)
        objects.append({"key": key.replace(os.sep, "/"), "size": os.path.getsize(path)})
objects.sort(key=lambda o: o["key"])
if not objects:
    raise SystemExit(f"no objects under {objects_dir}")

manifest = {
    "fixture": fixture,
    "generator": json.loads(generator_json),
    "config": json.loads(config_json),
    "db_name": db_name,
    "checksum_definition": (
        "SHA-256 over the concatenation of '{id}|{value}\\n' for "
        "SELECT id, value FROM items ORDER BY id"
    ),
    "latest": {"row_count": int(latest_rows), "rows_sha256": latest_sha},
    "pitr": {
        "txid": int(pitr_txid),
        "row_count": int(pitr_rows),
        "rows_sha256": pitr_sha,
    },
    "objects": objects,
}
with open(os.path.join(out_dir, "MANIFEST.json"), "w", encoding="utf-8") as fh:
    json.dump(manifest, fh, indent=2, sort_keys=True)
    fh.write("\n")
print(f"MANIFEST.json: {len(objects)} objects")
PY
}

# ════════════════════════════════════════════════════════════════════════════
# Phase 1: CLI fixture (published binary, watch --independent-tasks)
# ════════════════════════════════════════════════════════════════════════════

CLI_NAME=fixturecli
CLI_DB="$DRILL_WORKDIR/$CLI_NAME.db"
CLI_PREFIX="$DRILL_RUN_PREFIX/$CLI_NAME"

CLI_COMPACTION_CONFIG="$DRILL_WORKDIR/compaction.toml"
cat >"$CLI_COMPACTION_CONFIG" <<TOML
[compaction]
enabled = true
keep_fine_window = "$KEEP_FINE"
l1_batch = $L1_BATCH
l2_batch = $L2_BATCH
TOML

# Snapshot interval is pinned huge: every snapshot in this fixture is either
# the on-startup snapshot or a restart re-anchor snapshot (the
# `anchor_stream_on_startup` path -- a resumed stream re-anchors with a fresh
# snapshot). That makes each snapshot's content deterministic: it is exactly
# the quiesced database at the moment of the restart.
start_walrust_cli_fixture() {
  local db=$1
  "$WALRUST_BIN" watch "$db" \
    --config "$CLI_COMPACTION_CONFIG" \
    --independent-tasks \
    --bucket "$DRILL_BUCKET_URI" \
    --endpoint "$DRILL_ENDPOINT" \
    --snapshot-interval 999999 \
    --wal-sync-interval "$WAL_SYNC_INTERVAL" \
    --checkpoint-interval 999999 \
    --on-startup true \
    --no-metrics \
    --no-cache \
    >>"$DRILL_WORKDIR/walrust-cli.log" 2>&1 &
  DRILL_WALRUST_PID=$!
  log "walrust (cli fixture) pid=$DRILL_WALRUST_PID"
  wait_process_alive "$DRILL_WALRUST_PID" "walrust watch (cli fixture)"
}

# Snapshot txids scoped to the CLI db (same parse as lib.sh snapshot_txids,
# but the owned phase later shares $DRILL_RUN_PREFIX, so scope explicitly).
# Keys go through a temp file, never a pipe: `python - <<heredoc` reads the
# heredoc as stdin, so piping keys in as well silently loses them.
cli_snapshot_txids() {
  local keys="$DRILL_WORKDIR/cli-snapshot-keys.txt"
  s3_list_prefix "$CLI_PREFIX/" >"$keys"
  "$DRILL_PYTHON" - "$keys" <<'PY'
import re
import sys
vals = set()
with open(sys.argv[1], encoding="utf-8") as handle:
    for key in handle:
        m = re.search(r'/([0-9a-f]{4})/([0-9a-f]{16})-([0-9a-f]{16})\.ltx$', key.strip())
        if not m:
            continue
        if int(m.group(1), 16) != 0 and int(m.group(2), 16) == 1:
            vals.add(int(m.group(3), 16))
for txid in sorted(vals):
    print(txid)
PY
}

cli_snapshot_base_objects() {
  s3_list_prefix "$CLI_PREFIX/" \
    | grep -E '/[0-9a-f]{4}/0000000000000001-[0-9a-f]{16}\.ltx$' \
    | grep -v '/0000/'
}

cli_count() {
  local pattern=$1
  s3_list_prefix "$CLI_PREFIX/" | grep -cE "$pattern" || true
}

# Max txid across the CLI db's L0 pool only (temp file for the same
# heredoc-vs-pipe reason as cli_snapshot_txids).
cli_max_l0_txid() {
  local keys="$DRILL_WORKDIR/cli-l0-keys.txt"
  s3_list_prefix "$CLI_PREFIX/0000/" >"$keys"
  "$DRILL_PYTHON" - "$keys" <<'PY'
import re
import sys
mx = 0
with open(sys.argv[1], encoding="utf-8") as handle:
    for key in handle:
        m = re.search(r'/([0-9a-f]{16})-([0-9a-f]{16})\.ltx$', key.strip())
        if m:
            mx = max(mx, int(m.group(2), 16))
print(mx)
PY
}

wait_for_cli_level() {
  local level=$1
  local deadline=$((SECONDS + ${WALRUST_FIXTURE_LEVEL_TIMEOUT:-150}))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$(cli_count "/levels/$level/.*\.ltx$")" -gt 0 ]; then
      return 0
    fi
    kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
      || fail "walrust exited before any $level merge; see $DRILL_WORKDIR/walrust-cli.log"
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "$level never fired in the CLI fixture phase"
}

# Restart the watcher and wait for its re-anchor snapshot (a NEW snapshot
# txid) to land. Sets CLI_NEW_SNAPSHOT_TXID. NOT usable inside a command
# substitution: it restarts the watcher, and $(...) would strand the new
# DRILL_WALRUST_PID in a subshell (leaking the process past cleanup).
CLI_NEW_SNAPSHOT_TXID=
cli_restart_and_wait_snapshot() {
  local before after deadline
  before=$(cli_snapshot_txids | sort -n | tail -1)
  before=${before:-0}
  stop_walrust
  start_walrust_cli_fixture "$CLI_DB"
  deadline=$((SECONDS + 60))
  while [ "$SECONDS" -lt "$deadline" ]; do
    after=$(cli_snapshot_txids | sort -n | tail -1)
    after=${after:-0}
    if [ "$after" -gt "$before" ]; then
      CLI_NEW_SNAPSHOT_TXID=$after
      return 0
    fi
    kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
      || fail "walrust exited before the restart re-anchor snapshot; see $DRILL_WORKDIR/walrust-cli.log"
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "restart re-anchor snapshot never landed"
}

restore_and_check() {
  local expected_rows=$1
  local expected_sha=$2
  local label=$3
  shift 3
  run_restore_to "$CLI_NAME" "$DRILL_RESTORE" "$@" \
    || fail "$label: restore failed"
  integrity_ok "$DRILL_RESTORE"
  local actual_rows actual_sha
  actual_rows=$(db_count "$DRILL_RESTORE")
  actual_sha=$(rows_sha "$DRILL_RESTORE")
  [ "$actual_rows" = "$expected_rows" ] \
    || fail "$label: rows mismatch (expected $expected_rows got $actual_rows)"
  [ "$actual_sha" = "$expected_sha" ] \
    || fail "$label: rows sha mismatch (expected $expected_sha got $actual_sha)"
  log "$label OK: rows=$actual_rows sha=$actual_sha"
}

log "── CLI fixture phase ──"
create_db "$CLI_DB"
append_rows "$CLI_DB" 20 base

start_walrust_cli_fixture "$CLI_DB"
start_driver "$CLI_DB" "$DRIVER_INTERVAL" fixture
wait_driver_count_at_least 40 45 "$CLI_DB"

# Build enough leveled history that both L1 and L2 fire.
wait_for_cli_level L1
wait_for_cli_level L2
log "L1 and L2 both fired"

# Mid-history PITR point: quiesce, restart, and let the re-anchor snapshot
# freeze exactly this state. Its content is the quiesced database.
pause_driver "$CLI_DB"
cli_restart_and_wait_snapshot
CLI_PITR_TXID=$CLI_NEW_SNAPSHOT_TXID
CLI_PITR_ROWS=$(db_count "$CLI_DB")
CLI_PITR_SHA=$(rows_sha "$CLI_DB")
log "PITR point: txid=$CLI_PITR_TXID rows=$CLI_PITR_ROWS sha=$CLI_PITR_SHA"

# More leveled history strictly after the PITR snapshot.
resume_driver
sleep 15
pause_driver "$CLI_DB"

# Late anchor snapshot via a second restart, then a short L0 tail after it.
cli_restart_and_wait_snapshot
CLI_ANCHOR_TXID=$CLI_NEW_SNAPSHOT_TXID
log "late anchor snapshot txid=$CLI_ANCHOR_TXID"

# Live L0 tail past the newest snapshot: nudge until at least one trailing
# L0 object survives (compaction may momentarily fold the whole tail).
CLI_LATEST_ROWS=
CLI_LATEST_SHA=
tail_ok=0
for attempt in 1 2 3 4 5; do
  resume_driver
  sleep 3
  pause_driver "$CLI_DB"
  CLI_LATEST_ROWS=$(driver_count "$CLI_DB")
  wait_restore_count "$CLI_NAME" "$CLI_LATEST_ROWS"
  newest_snap=$(cli_snapshot_txids | sort -n | tail -1)
  if [ "$(cli_max_l0_txid)" -gt "$newest_snap" ]; then
    tail_ok=1
    break
  fi
  log "no trailing L0 past snapshot txid=$newest_snap yet (attempt $attempt); nudging"
done
[ "$tail_ok" = 1 ] || fail "could not produce a live L0 tail past the newest snapshot"
CLI_LATEST_SHA=$(rows_sha "$CLI_DB")
restored_sha=$(rows_sha "$DRILL_RESTORE")
[ "$restored_sha" = "$CLI_LATEST_SHA" ] \
  || fail "restored sha $restored_sha != live db sha $CLI_LATEST_SHA"
log "latest: rows=$CLI_LATEST_ROWS sha=$CLI_LATEST_SHA"

stop_driver
stop_walrust

# ── Prune boundary ──────────────────────────────────────────────────────────
# The minimum-2 floor workaround (see header): delete every snapshot OLDER
# than the PITR snapshot, so the floor is satisfied by {PITR, anchor} and the
# prune watermark lands at the PITR txid -- putting the entire pre-PITR
# leveled history below it, where prune has real deletions to make.
deleted_snaps=0
while IFS= read -r key; do
  [ -n "$key" ] || continue
  key_txid_hex=$(printf '%s\n' "$key" | grep -oE '[0-9a-f]{16}\.ltx$' | sed 's/\.ltx$//')
  key_txid=$((16#$key_txid_hex))
  if [ "$key_txid" -lt "$CLI_PITR_TXID" ]; then
    s3_delete_key "$key"
    deleted_snaps=$((deleted_snaps + 1))
  fi
done < <(cli_snapshot_base_objects)
[ "$deleted_snaps" -ge 1 ] || fail "expected to hand-delete at least the on-startup snapshot (floor workaround)"
log "floor workaround: hand-deleted $deleted_snaps pre-PITR snapshot(s)"

before_objects="$DRILL_WORKDIR/cli-before-prune.txt"
s3_list_prefix "$CLI_PREFIX/" | sort >"$before_objects"

run_prune "$CLI_NAME" >"$DRILL_WORKDIR/prune.out" 2>&1 \
  || fail "walrust prune failed: $(cat "$DRILL_WORKDIR/prune.out")"

after_objects="$DRILL_WORKDIR/cli-after-prune.txt"
s3_list_prefix "$CLI_PREFIX/" | sort >"$after_objects"
pruned_count=$(comm -23 "$before_objects" "$after_objects" | wc -l | tr -d ' ')
[ "$pruned_count" -ge 1 ] \
  || fail "prune deleted nothing -- the fixture would carry no prune boundary (see drills/prune-retained.sh for the floor workaround)"
log "prune deleted $pruned_count superseded object(s)"

# ── Post-prune structural + restore assertions (published binary) ───────────
snap_count=$(cli_snapshot_txids | wc -l | tr -d ' ')
l1_count=$(cli_count '/levels/L1/.*\.ltx$')
l2_count=$(cli_count '/levels/L2/.*\.ltx$')
l0_count=$(cli_count '/0000/.*\.ltx$')
log "final CLI bucket: snapshots=$snap_count L0=$l0_count L1=$l1_count L2=$l2_count"
[ "$snap_count" -ge 2 ] || fail "fixture needs >= 2 snapshots, got $snap_count"
[ "$l1_count" -ge 1 ] || fail "fixture needs a populated L1"
[ "$l2_count" -ge 1 ] || fail "fixture needs a populated L2"
[ "$l0_count" -ge 1 ] || fail "fixture needs a live L0 tail"

restore_and_check "$CLI_LATEST_ROWS" "$CLI_LATEST_SHA" "post-prune restore-to-latest"
restore_and_check "$CLI_PITR_ROWS" "$CLI_PITR_SHA" "post-prune PITR" --point-in-time "$CLI_PITR_TXID"
"$WALRUST_BIN" verify "$CLI_NAME" --bucket "$DRILL_BUCKET_URI" --endpoint "$DRILL_ENDPOINT" \
  >"$DRILL_WORKDIR/verify.out" 2>&1 \
  || fail "walrust verify exited nonzero on the final CLI fixture bucket: $(cat "$DRILL_WORKDIR/verify.out")"

# ── Freeze ──────────────────────────────────────────────────────────────────
rm -rf "$CLI_OUT_DIR"
mkdir -p "$CLI_OUT_DIR/objects"
s3_download_prefix "$CLI_PREFIX/" "$CLI_OUT_DIR/objects/$CLI_NAME" "$CLI_PREFIX/"

cli_generator_json=$(cat <<JSON
{"artifact": "crates.io CLI binary", "install": "cargo install walrust --version $PIN_VERSION --locked", "version": "$PIN_VERSION", "version_output": "$PIN_BIN_VERSION_OUTPUT"}
JSON
)
cli_config_json=$(cat <<JSON
{"mode": "watch --independent-tasks", "compaction": {"enabled": true, "keep_fine_window": "$KEEP_FINE", "l1_batch": $L1_BATCH, "l2_batch": $L2_BATCH}, "snapshot_interval": 999999, "wal_sync_interval": $WAL_SYNC_INTERVAL, "checkpoint_interval": 999999, "snapshots": "on-startup + restart re-anchor (2 restarts)", "prune": {"command": "walrust prune --hourly 2 --daily 0 --weekly 0 --monthly 0 --force", "floor_workaround": "hand-deleted snapshots older than the PITR snapshot before pruning (hadb-io RetentionPolicy minimum-2 floor; see drills/prune-retained.sh)"}}
JSON
)
write_manifest "$CLI_OUT_DIR" "cli-v$PIN_VERSION" "$cli_generator_json" "$cli_config_json" \
  "$CLI_NAME" "$CLI_LATEST_ROWS" "$CLI_LATEST_SHA" "$CLI_PITR_TXID" "$CLI_PITR_ROWS" "$CLI_PITR_SHA"
log "CLI fixture frozen at $CLI_OUT_DIR ($(du -sh "$CLI_OUT_DIR" | awk '{print $1}'))"

# ════════════════════════════════════════════════════════════════════════════
# Phase 2: owned fixture (registry walrust-core, library write path)
# ════════════════════════════════════════════════════════════════════════════

OWNED_NAME=fixtureowned
OWNED_PREFIX="$DRILL_RUN_PREFIX/owned/"
OWNED_GEN_DIR="$DRILL_WORKDIR/owned-gen"

log "── owned fixture phase (scaffolding throwaway generator crate in $OWNED_GEN_DIR) ──"
mkdir -p "$OWNED_GEN_DIR/src"

cat >"$OWNED_GEN_DIR/Cargo.toml" <<TOML
# Throwaway generator -- lives in a scratch dir, never in the repo. All deps
# resolve from crates.io by exact version: the bucket must be written by what
# walrust-core $CORE_PIN_VERSION actually shipped, never a workspace build.
[package]
name = "walrust-fixture-owned-gen"
version = "0.0.0"
edition = "2021"
publish = false

[workspace]

[dependencies]
walrust-core = "=$CORE_PIN_VERSION"
hadb-storage = "=$HADB_PIN_VERSION"
hadb-storage-s3 = "=$HADB_PIN_VERSION"
tokio = { version = "1", features = ["full"] }
rusqlite = { version = "0.35", features = ["bundled"] }
anyhow = "1"
sha2 = "0.10"
serde_json = "1"
TOML

cat >"$OWNED_GEN_DIR/src/main.rs" <<'RUST'
//! Owned-mode fixture generator: writes a compacted walrust-core 0.7.0 bucket
//! (snapshots + levels + L0 tail) via the supported library compaction path
//! (`add_without_snapshot` + `autonomous_snapshots` + short
//! `snapshot_interval` -- `add()` refuses under compaction because it creates
//! a lineage compaction cannot see), then self-verifies restore-to-latest and
//! PITR with the same registry crate before emitting the recorded points.
use anyhow::{ensure, Context, Result};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn env(k: &str) -> Result<String> {
    std::env::var(k).with_context(|| format!("missing env {k}"))
}

/// Canonical row-content checksum (matches generate.sh / format_stability.rs):
/// SHA-256 over "{id}|{value}\n" for SELECT id, value FROM items ORDER BY id.
fn rows_sha(path: &Path) -> Result<(u64, String)> {
    let conn = Connection::open(path)?;
    let mut stmt = conn.prepare("SELECT id, value FROM items ORDER BY id")?;
    let mut hasher = Sha256::new();
    let mut count = 0u64;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (id, value) = row?;
        hasher.update(format!("{id}|{value}\n"));
        count += 1;
    }
    Ok((count, format!("{:x}", hasher.finalize())))
}

async fn list(storage: &Arc<dyn hadb_storage::StorageBackend>, prefix: &str) -> Result<Vec<String>> {
    hadb_storage::StorageBackend::list(storage.as_ref(), prefix, None).await
}

/// Max seq parsed from `{seq:016x}.hadbp` keys under a generation prefix.
fn max_seq(keys: &[String]) -> u64 {
    keys.iter()
        .filter_map(|k| {
            let fname = k.rsplit('/').next()?;
            let stem = fname.strip_suffix(".hadbp")?;
            u64::from_str_radix(stem, 16).ok()
        })
        .max()
        .unwrap_or(0)
}

fn owned_config(snapshot_interval: Duration) -> walrust_core::ReplicationConfig {
    walrust_core::ReplicationConfig {
        sync_interval: Duration::from_millis(200),
        snapshot_interval,
        autonomous_snapshots: true,
        compaction: walrust_core::compaction::CompactionSettings {
            enabled: true,
            keep_fine_window: Duration::ZERO,
            l1_batch: 4,
            l2_batch: 3,
        },
        ..Default::default()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let bucket = env("FIXGEN_BUCKET")?;
    let prefix = env("FIXGEN_PREFIX")?; // must end with '/'
    ensure!(prefix.ends_with('/'), "FIXGEN_PREFIX must end with '/'");
    let endpoint = std::env::var("FIXGEN_ENDPOINT").ok();
    let name = env("FIXGEN_NAME")?;
    let db_path = PathBuf::from(env("FIXGEN_DB")?);
    let out_path = PathBuf::from(env("FIXGEN_OUT")?);

    let conn = Connection::open(&db_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
         CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
    )?;
    let mut next_id = 1i64;
    for _ in 0..20 {
        conn.execute(
            "INSERT INTO items (id, value) VALUES (?1, ?2)",
            rusqlite::params![next_id, format!("base-{next_id}")],
        )?;
        next_id += 1;
    }

    let storage: Arc<dyn hadb_storage::StorageBackend> = Arc::new(
        hadb_storage_s3::S3Storage::from_env(bucket.clone(), endpoint.as_deref()).await?,
    );
    let l0_prefix = format!("{prefix}{name}/0000/");
    let snap_prefix = format!("{prefix}{name}/0001/");
    let l1_prefix = format!("{prefix}{name}/levels/L1/");
    let l2_prefix = format!("{prefix}{name}/levels/L2/");

    // ── Stage 1: build leveled history + >= 2 snapshots ────────────────────
    let r1 = walrust_core::Replicator::new(storage.clone(), &prefix, owned_config(Duration::from_secs(4)));
    r1.add_without_snapshot(&name, &db_path).await?;

    let deadline = Instant::now() + Duration::from_secs(180);
    let mut iter = 0u64;
    loop {
        conn.execute_batch("BEGIN IMMEDIATE;")?;
        for _ in 0..2 {
            conn.execute(
                "INSERT INTO items (id, value) VALUES (?1, ?2)",
                rusqlite::params![next_id, format!("stage1-{next_id}")],
            )?;
            next_id += 1;
        }
        conn.execute_batch("COMMIT;")?;
        tokio::time::sleep(Duration::from_millis(150)).await;

        iter += 1;
        if iter % 5 == 0 {
            let snaps = list(&storage, &snap_prefix).await?;
            let l1 = list(&storage, &l1_prefix).await?;
            let l2 = list(&storage, &l2_prefix).await?;
            if snaps.len() >= 2 && !l1.is_empty() && !l2.is_empty() {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "stage 1 timed out: snapshots={} L1={} L2={}",
                snaps.len(),
                l1.len(),
                l2.len()
            );
        }
    }

    // PITR point: quiesce writes, then wait for the next autonomous snapshot
    // (the timer snapshots unconditionally), whose content is exactly the
    // quiesced database.
    let before_snap = max_seq(&list(&storage, &snap_prefix).await?);
    let deadline = Instant::now() + Duration::from_secs(30);
    let pitr_seq = loop {
        let cur = max_seq(&list(&storage, &snap_prefix).await?);
        if cur > before_snap {
            break cur;
        }
        ensure!(Instant::now() < deadline, "no post-quiesce snapshot landed");
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let (pitr_rows, pitr_sha) = rows_sha(&db_path)?;
    drop(r1); // stop the stage-1 timers before the controlled tail stage
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── Stage 2: L0 tail past the newest snapshot, no further snapshots ────
    // A last stage-1 timer snapshot can land between the PITR capture and
    // drop(r1); it has the same quiesced content, so it is harmless -- take
    // the stage-2 baseline from the listing, not from pitr_seq.
    let stage2_base_snap = max_seq(&list(&storage, &snap_prefix).await?);
    let r2 = walrust_core::Replicator::new(storage.clone(), &prefix, owned_config(Duration::from_secs(3600)));
    r2.add_without_snapshot(&name, &db_path).await?;
    for burst in 0..8 {
        conn.execute_batch("BEGIN IMMEDIATE;")?;
        for _ in 0..2 {
            conn.execute(
                "INSERT INTO items (id, value) VALUES (?1, ?2)",
                rusqlite::params![next_id, format!("tail-{burst}-{next_id}")],
            )?;
            next_id += 1;
        }
        conn.execute_batch("COMMIT;")?;
        r2.flush(&name).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Final durability flush, then require a trailing L0 past the newest
    // snapshot (stage 2 takes no snapshots, so this is stable once true).
    r2.flush(&name).await?;
    let newest_snap = max_seq(&list(&storage, &snap_prefix).await?);
    ensure!(
        newest_snap == stage2_base_snap,
        "a snapshot landed during stage 2 (newest={newest_snap} base={stage2_base_snap}); the tail would not be stable"
    );
    let tail = max_seq(&list(&storage, &l0_prefix).await?);
    ensure!(
        tail > newest_snap,
        "no live L0 tail past the newest snapshot (tail={tail} snap={newest_snap})"
    );
    let (latest_rows, latest_sha) = rows_sha(&db_path)?;

    // ── Self-verify with the SAME registry crate ────────────────────────────
    let restored = db_path.with_extension("restored.db");
    let seq = r2.restore(&name, &restored).await?;
    ensure!(seq.is_some(), "restore-to-latest found no data");
    let (got_rows, got_sha) = rows_sha(&restored)?;
    ensure!(
        got_rows == latest_rows && got_sha == latest_sha,
        "restore-to-latest mismatch: got {got_rows}/{got_sha} want {latest_rows}/{latest_sha}"
    );
    let pitr_restored = db_path.with_extension("pitr.db");
    walrust_core::sync::restore(
        storage.clone(),
        &prefix,
        &name,
        &pitr_restored,
        Some(&pitr_seq.to_string()),
    )
    .await?;
    let (got_rows, got_sha) = rows_sha(&pitr_restored)?;
    ensure!(
        got_rows == pitr_rows && got_sha == pitr_sha,
        "PITR self-verify mismatch: got {got_rows}/{got_sha} want {pitr_rows}/{pitr_sha}"
    );
    drop(r2);

    let out = serde_json::json!({
        "pitr_seq": pitr_seq,
        "pitr_rows": pitr_rows,
        "pitr_sha": pitr_sha,
        "latest_rows": latest_rows,
        "latest_sha": latest_sha,
    });
    std::fs::write(&out_path, serde_json::to_string_pretty(&out)?)?;
    println!("owned fixture generated: {out}");
    Ok(())
}
RUST

OWNED_DB="$DRILL_WORKDIR/$OWNED_NAME.db"
OWNED_OUT_JSON="$DRILL_WORKDIR/owned-gen.json"
log "building + running owned generator (registry walrust-core =$CORE_PIN_VERSION; network required)"
(
  cd "$OWNED_GEN_DIR"
  # Deliberately NOT --offline: the whole point is resolving walrust-core
  # from crates.io. No [patch], no path deps -- see Cargo.toml above.
  FIXGEN_BUCKET="$DRILL_BUCKET_NAME" \
    FIXGEN_PREFIX="$OWNED_PREFIX" \
    FIXGEN_ENDPOINT="$DRILL_ENDPOINT" \
    FIXGEN_NAME="$OWNED_NAME" \
    FIXGEN_DB="$OWNED_DB" \
    FIXGEN_OUT="$OWNED_OUT_JSON" \
    cargo run >"$DRILL_WORKDIR/owned-gen.log" 2>&1
) || fail "owned generator failed; see $DRILL_WORKDIR/owned-gen.log"
[ -s "$OWNED_OUT_JSON" ] || fail "owned generator produced no output json"

OWNED_PITR_SEQ=$("$DRILL_PYTHON" -c "import json,sys; print(json.load(open(sys.argv[1]))['pitr_seq'])" "$OWNED_OUT_JSON")
OWNED_PITR_ROWS=$("$DRILL_PYTHON" -c "import json,sys; print(json.load(open(sys.argv[1]))['pitr_rows'])" "$OWNED_OUT_JSON")
OWNED_PITR_SHA=$("$DRILL_PYTHON" -c "import json,sys; print(json.load(open(sys.argv[1]))['pitr_sha'])" "$OWNED_OUT_JSON")
OWNED_LATEST_ROWS=$("$DRILL_PYTHON" -c "import json,sys; print(json.load(open(sys.argv[1]))['latest_rows'])" "$OWNED_OUT_JSON")
OWNED_LATEST_SHA=$("$DRILL_PYTHON" -c "import json,sys; print(json.load(open(sys.argv[1]))['latest_sha'])" "$OWNED_OUT_JSON")

rm -rf "$OWNED_OUT_DIR"
mkdir -p "$OWNED_OUT_DIR/objects"
s3_download_prefix "$OWNED_PREFIX" "$OWNED_OUT_DIR/objects" "$OWNED_PREFIX"

owned_generator_json=$(cat <<JSON
{"artifact": "registry walrust-core crate (throwaway scratch generator, no path/git deps, no [patch])", "version": "$CORE_PIN_VERSION", "dependency": "walrust-core = \"=$CORE_PIN_VERSION\" (crates.io)", "hadb": "=$HADB_PIN_VERSION"}
JSON
)
owned_config_json=$(cat <<JSON
{"mode": "library owned-mode (add_without_snapshot + autonomous_snapshots)", "compaction": {"enabled": true, "keep_fine_window": "0s", "l1_batch": 4, "l2_batch": 3}, "sync_interval_ms": 200, "stage1_snapshot_interval_s": 4, "stage2_snapshot_interval_s": 3600, "pitr_semantics": "seq/TXID of an autonomous snapshot taken while writes were quiesced"}
JSON
)
write_manifest "$OWNED_OUT_DIR" "owned-v$PIN_VERSION" "$owned_generator_json" "$owned_config_json" \
  "$OWNED_NAME" "$OWNED_LATEST_ROWS" "$OWNED_LATEST_SHA" "$OWNED_PITR_SEQ" "$OWNED_PITR_ROWS" "$OWNED_PITR_SHA"
log "owned fixture frozen at $OWNED_OUT_DIR ($(du -sh "$OWNED_OUT_DIR" | awk '{print $1}'))"

total=$(du -sh "$CLI_OUT_DIR" "$OWNED_OUT_DIR" | awk '{print $2": "$1}')
log "PASS fixture generation complete"
log "$total"
log "scratch prefix $DRILL_BUCKET_URI is deleted by drill_cleanup on exit"
