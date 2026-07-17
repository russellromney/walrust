#!/usr/bin/env bash
#
# Version-skew drill (gap 2): the compaction docs claim "an old binary cannot
# restore a leveled bucket" -- until this drill, that was never empirically
# tested, only asserted. This drill builds a REAL leveled bucket with the
# current binary, then runs a REAL old (pre-compaction) `walrust restore`
# against it and characterizes what actually happens.
#
# CONFIRMED (see CHANGELOG/README/ROADMAP for the citation): an old binary
# restoring a leveled bucket exits 0 -- it does not error -- and produces a
# CORRUPT database (SQLite `integrity_check` fails with malformed b-tree
# pages), because the old restore path is structurally blind to `levels/`: it
# only ever walks the flat `NNNN/` generation folders, so once compaction
# folds+deletes an L0 range, the old binary silently skips straight from
# whatever snapshot/incremental it CAN see to the next one it can see, with no
# continuity check on the ones in between. This is WORSE than the "fewer
# rows" hazard this drill was designed to catch: on a small dataset the
# rows just happen to still round-trip (get lucky: last-writer-wins on a
# single surviving page); with the wide multi-page dataset this drill builds,
# the pages that only ever existed inside the deleted/merged range are gone
# for good and SQLite refuses to even open the tree.
#
# The three possible outcomes an old-binary restore can hit, and how this
# drill treats each:
#   1. Loud nonzero failure                -> ACCEPTABLE, drill exits 0.
#   2. Exit 0, row-exact + integrity ok     -> ACCEPTABLE (surprising but
#                                              harmless), drill exits 0.
#   3. Exit 0, corrupt OR row-short/long    -> the CONFIRMED KNOWN HAZARD.
#                                              Prints a prominent warning
#                                              block and exits 0 with a
#                                              KNOWN-HAZARD marker (this is
#                                              real, observed behavior, not a
#                                              drill bug -- see README).
# Any other shape (e.g. the "old" binary crashing the harness itself) is an
# unanticipated anomaly and fails the drill for real investigation.
#
# MANUAL / `make drill-version-skew` ONLY. Deliberately NOT wired into
# drills/run-all.sh or the nightly workflow: obtaining the old binary needs
# crates.io network access (flaky in CI) and, in the worst case, a full
# from-source build of a historical commit (expensive).

set -Eeuo pipefail
# shellcheck source=drills/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

# crates.io publish history at the time this drill was written tops out at
# 0.5.1 -- `cargo install walrust --version 0.5.2` (the version this drill was
# originally specified against) 404s, so 0.5.2 is tried first (in case it is
# published later) and falls back to 0.5.1, the newest version actually on
# crates.io. Both predate compaction by a wide margin (compaction shipped well
# after v0.6.0), so either is a valid "old binary" for this drill.
# shellcheck disable=SC2206  # intentional word-splitting of a space-separated version list
OLD_CANDIDATE_VERSIONS=(${WALRUST_DRILL_OLD_VERSIONS:-0.5.2 0.5.1})
OLD_FALLBACK_COMMIT=${WALRUST_DRILL_OLD_COMMIT:-98c1793}
OLD_BIN_CACHE_DIR=${WALRUST_DRILL_OLD_BIN_CACHE:-"$REPO_ROOT/target/drill-old-bin"}
OLD_BIN=${WALRUST_DRILL_OLD_BIN:-}
INSTALL_RETRIES=${WALRUST_DRILL_OLD_INSTALL_RETRIES:-3}

DRILL_COMPACTION_CONFIG=
DRILL_WIDE_DRIVER_PID=

start_walrust_compaction() {
  local db=$1
  "$WALRUST_BIN" watch "$db" \
    --config "$DRILL_COMPACTION_CONFIG" \
    --independent-tasks \
    --bucket "$DRILL_BUCKET_URI" \
    --endpoint "$DRILL_ENDPOINT" \
    --snapshot-interval 999999 \
    --wal-sync-interval 1 \
    --checkpoint-interval 999999 \
    --on-startup true \
    --no-metrics \
    --no-cache \
    >"$DRILL_WORKDIR/walrust.log" 2>&1 &
  DRILL_WALRUST_PID=$!
  log "walrust (compaction) pid=$DRILL_WALRUST_PID"
  wait_process_alive "$DRILL_WALRUST_PID" "walrust watch --config (compaction)"
}

level_object_count() {
  local pattern=$1
  s3_list_prefix "$DRILL_RUN_PREFIX/" | grep -cE "$pattern" || true
}

wait_for_sync_progress() {
  local baseline=$1
  local deadline=$((SECONDS + 60))
  local cur
  while [ "$SECONDS" -lt "$deadline" ]; do
    cur=$(latest_txid)
    if [ "${cur:-0}" -gt "$baseline" ]; then
      return 0
    fi
    kill -0 "$DRILL_WALRUST_PID" >/dev/null 2>&1 \
      || fail "walrust exited before making sync progress; see $DRILL_WORKDIR/walrust.log"
    sleep "$DRILL_POLL_INTERVAL"
  done
  fail "walrust made no sync progress past TXID $baseline within timeout"
}

# Stop the wide-row driver by SIGKILL, never a normal exit. A normal Python
# process exit closes its sqlite3 connection, and closing the LAST connection
# to a WAL-mode database triggers SQLite's own implicit checkpoint -- a
# harness artifact that can race the watcher's poll cycle and look like data
# loss that has nothing to do with compaction. drills/lib.sh's own driver
# avoids this the same way (infinite loop, killed not closed); this drill
# needs its own wide-row variant because lib.sh's driver writes ~70-byte rows
# that all fit on a single SQLite page for a small drill dataset, which is not
# enough pressure to strand a b-tree page inside a merged-and-deleted range.
stop_wide_driver() {
  if [ -n "$DRILL_WIDE_DRIVER_PID" ] && kill -0 "$DRILL_WIDE_DRIVER_PID" >/dev/null 2>&1; then
    kill -9 "$DRILL_WIDE_DRIVER_PID" >/dev/null 2>&1 || true
    wait "$DRILL_WIDE_DRIVER_PID" >/dev/null 2>&1 || true
  fi
  DRILL_WIDE_DRIVER_PID=
}

# Obtain an old (pre-compaction) walrust binary. Prefers a cache under
# target/drill-old-bin/<version>/bin/walrust so repeated drill runs during
# development don't re-download/re-compile every time. Tries each candidate
# crates.io version in order (INSTALL_RETRIES attempts each, network allowed
# for this drill only); if every candidate fails, falls back to a full
# from-source build at OLD_FALLBACK_COMMIT via a throwaway git worktree.
OLD_WORKTREE_DIR=
cleanup_old_worktree() {
  if [ -n "$OLD_WORKTREE_DIR" ] && [ -d "$OLD_WORKTREE_DIR" ]; then
    git -C "$REPO_ROOT" worktree remove --force "$OLD_WORKTREE_DIR" >/dev/null 2>&1 || true
    rm -rf "$OLD_WORKTREE_DIR"
  fi
}

obtain_old_binary() {
  if [ -n "$OLD_BIN" ]; then
    [ -x "$OLD_BIN" ] || fail "WALRUST_DRILL_OLD_BIN=$OLD_BIN is not executable"
    log "using caller-provided old binary: $OLD_BIN ($("$OLD_BIN" --version 2>&1))"
    return 0
  fi

  local version cache_bin attempt
  for version in "${OLD_CANDIDATE_VERSIONS[@]}"; do
    cache_bin="$OLD_BIN_CACHE_DIR/$version/bin/walrust"
    if [ -x "$cache_bin" ]; then
      OLD_BIN="$cache_bin"
      log "using cached old binary $version: $OLD_BIN ($("$OLD_BIN" --version 2>&1))"
      return 0
    fi
    log "installing walrust $version from crates.io into $OLD_BIN_CACHE_DIR/$version (network allowed for this drill only)"
    attempt=1
    while [ "$attempt" -le "$INSTALL_RETRIES" ]; do
      if cargo install walrust --version "$version" --locked --root "$OLD_BIN_CACHE_DIR/$version" \
        >"$DRILL_WORKDIR/cargo-install-$version.log" 2>&1; then
        if [ -x "$cache_bin" ]; then
          OLD_BIN="$cache_bin"
          log "installed walrust $version: $OLD_BIN ($("$OLD_BIN" --version 2>&1))"
          return 0
        fi
      fi
      log "cargo install walrust=$version attempt $attempt/$INSTALL_RETRIES failed \
(see $DRILL_WORKDIR/cargo-install-$version.log)"
      attempt=$((attempt + 1))
      sleep 3
    done
  done

  log "all crates.io candidates (${OLD_CANDIDATE_VERSIONS[*]}) failed after $INSTALL_RETRIES attempts each \
-- falling back to a from-source build at commit $OLD_FALLBACK_COMMIT (expensive)"
  OLD_WORKTREE_DIR=$(mktemp -d "$DRILL_TMP_ROOT/walrust-old-src.XXXXXX")
  git -C "$REPO_ROOT" worktree add "$OLD_WORKTREE_DIR" "$OLD_FALLBACK_COMMIT" \
    || fail "could not create a worktree at commit $OLD_FALLBACK_COMMIT for the source-build fallback"
  ( cd "$OLD_WORKTREE_DIR" && CARGO_TARGET_DIR="$OLD_WORKTREE_DIR/target" cargo build --release -p walrust --bin walrust ) \
    || fail "source-build fallback at commit $OLD_FALLBACK_COMMIT failed to compile"
  OLD_BIN="$OLD_WORKTREE_DIR/target/release/walrust"
  [ -x "$OLD_BIN" ] || fail "source-build fallback did not produce an executable at $OLD_BIN"
  log "built old binary from source at $OLD_FALLBACK_COMMIT: $OLD_BIN ($("$OLD_BIN" --version 2>&1))"
}

warn_block() {
  log "############################################################"
  while IFS= read -r line; do
    log "# $line"
  done
  log "############################################################"
}

drill_setup
# drill_setup already installed its own EXIT/INT/TERM trap (drill_cleanup);
# replace it with one that also cleans up the wide driver and the old-binary
# worktree fallback, in addition to (not instead of) the normal drill teardown.
trap 'stop_wide_driver; cleanup_old_worktree; drill_cleanup' EXIT INT TERM

obtain_old_binary

create_db "$DRILL_DB"
append_rows "$DRILL_DB" 5 base
name=$(basename "$DRILL_DB" .db)

DRILL_COMPACTION_CONFIG="$DRILL_WORKDIR/compaction.toml"
cat >"$DRILL_COMPACTION_CONFIG" <<TOML
# Aggressive compaction, no periodic full snapshots: the only way to reach
# latest is through the merged levels/, which is exactly what an old binary
# cannot read.
[compaction]
enabled = true
keep_fine_window = "0s"
l1_batch = 4
l2_batch = 2
TOML

start_walrust_compaction "$DRILL_DB"
wait_for_sync_progress 0

# Wide-row driver (~420 bytes/row): spans many b-tree pages quickly. Once the
# insertion frontier moves past a page it is never dirtied again, so that
# page's only wire representation is whichever sync tick captured it -- once
# that tick is folded into levels/ and its L0 source deleted, a levels-blind
# binary has no way to ever see that page's rows again.
"$DRILL_PYTHON" - "$DRILL_DB" >"$DRILL_WORKDIR/wide-driver.log" 2>&1 <<'PY' &
import sqlite3, sys, time
db = sys.argv[1]
conn = sqlite3.connect(db, timeout=5.0, isolation_level=None)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA wal_autocheckpoint=0")
next_id = conn.execute("SELECT COALESCE(MAX(id),0)+1 FROM items").fetchone()[0]
pad = "x" * 400
while True:
    conn.execute("BEGIN IMMEDIATE")
    for _ in range(20):
        conn.execute("INSERT INTO items(id, value) VALUES(?, ?)", (next_id, f"wide-{next_id}-{pad}"))
        next_id += 1
    conn.execute("COMMIT")
    print(f"tick up to id={next_id-1}", flush=True)
    time.sleep(1.1)
PY
DRILL_WIDE_DRIVER_PID=$!
log "wide driver pid=$DRILL_WIDE_DRIVER_PID"

sleep "${WALRUST_DRILL_BUILD_SECONDS:-33}"
stop_wide_driver
# Let the watcher settle onto whatever was durably committed before the kill.
sleep 5

expected=$(db_count "$DRILL_DB")
log "ground-truth rows (live DB) = $expected"

deadline=$((SECONDS + 30))
l2_seen=0
while [ "$SECONDS" -lt "$deadline" ]; do
  if [ "$(level_object_count '/levels/L2/.*\.ltx$')" -gt 0 ]; then
    l2_seen=1
    break
  fi
  sleep 1
done
[ "$l2_seen" -eq 1 ] || fail "L2 never fired -- cannot test a deeply leveled bucket"
log "L2 fired -- bucket has a substantial merged-and-deleted history"

wait_restore_count "$name" "$expected"
stop_walrust

log "current binary restores this leveled bucket row-exact (expected=$expected); \
now running the OLD binary's restore against the same bucket"

old_restore="$DRILL_WORKDIR/old-restore.db"
rm -f "$old_restore"
old_output="$DRILL_WORKDIR/old-restore.out"
old_exit=0
"$OLD_BIN" restore "$name" \
  --output "$old_restore" \
  --bucket "$DRILL_BUCKET_URI" \
  --endpoint "$DRILL_ENDPOINT" \
  >"$old_output" 2>&1 || old_exit=$?

log "old binary ($("$OLD_BIN" --version 2>&1)) restore exit=$old_exit"

if [ "$old_exit" -ne 0 ]; then
  log "ACCEPTABLE outcome: loud nonzero failure (exit $old_exit). Old binary correctly refused \
to (mis)restore a leveled bucket it cannot understand:"
  sed 's/^/  | /' "$old_output" >&2
  log "PASS version-skew: old binary failed loudly, no silent corruption"
  exit 0
fi

if [ ! -f "$old_restore" ]; then
  fail "ANOMALY: old binary exited 0 but produced no output file at $old_restore -- unexpected \
shape, needs investigation (see $old_output)"
fi

old_integrity=$(sqlite3 "$old_restore" "PRAGMA integrity_check;" 2>&1 || true)
old_rows=$(sqlite3 "$old_restore" ".timeout 5000" "SELECT COUNT(*) FROM items;" 2>&1 || echo "unreadable")

if [ "$old_integrity" = "ok" ] && [ "$old_rows" = "$expected" ]; then
  log "ACCEPTABLE outcome: old binary exited 0 and restored row-exact + integrity ok \
(surprising, but not dangerous). rows=$old_rows"
  log "PASS version-skew: old binary happened to restore correctly"
  exit 0
fi

# CONFIRMED KNOWN HAZARD (see README "Version skew" section): exit 0 with
# corruption or a wrong row count. Empirically, this drill has observed the
# old binary produce a CORRUPT database (integrity_check fails on the pages
# that only ever existed inside the merged-and-deleted range) -- worse than
# merely short, and entirely silent from the operator's point of view (exit
# code 0, no error printed). This branch is the documented, expected outcome,
# not a drill failure: it exits 0 with a KNOWN-HAZARD marker.
{
  echo "KNOWN-HAZARD: old binary ($("$OLD_BIN" --version 2>&1)) silently mis-restored a leveled bucket"
  echo "exit code:        0 (no error reported to the operator)"
  echo "ground-truth rows: $expected"
  echo "old-restore rows:  $old_rows"
  echo "integrity_check:   $old_integrity"
  echo "This is the CONFIRMED version-skew hazard the [compaction] enabled=false"
  echo "default exists to prevent. Never enable compaction while any binary that"
  echo "might restore this bucket predates levels/ support."
} | warn_block

log "PASS version-skew: KNOWN-HAZARD confirmed and reported (drill exits 0 by design -- this \
is real, observed, documented behavior, not a bug in the drill)"
