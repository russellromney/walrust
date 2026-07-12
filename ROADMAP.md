# walrust Roadmap

## Vision

**Simple, reliable SQLite backups to S3 with integrity verification.**

Core differentiators:
- HADBP changeset format (formerly "LTX") with integrity verification
- Lower memory footprint than Litestream
- Built for production: verify, explain, webhook alerting
- Honest about what works (no vaporware)

---

## Current Capabilities (v0.6.0)

> **Review history.** Two full adversarial reviews (F1–F15, then A/B/D/E
> findings) found and fixed silent-data-loss and silent-restore-corruption
> paths across phased fix waves, each gated by revert-proven tests and CI E2E
> round-trips with an external autocheckpointing writer. The dual src/ and
> walrust-core trees are now one engine (the source of half the original
> findings). The full ledgers (every fixed finding named its proving test)
> lived in `ADVERSARIAL_REVIEW*.md`, removed 2026-07-11 — see git history and
> CHANGELOG.md. Still-open residuals moved to the "Residual risk register"
> section below. The "Experimental" warning stands.

**Core features that work:**
- `walrust watch` - Watch and sync multiple databases
- `walrust snapshot` - Take immediate snapshot
- `walrust restore` - Restore database from S3
- `walrust list` - List backups
- `walrust prune` - Clean up old snapshots with GFS retention (`compact` is a
  deprecated hidden alias)
- `walrust replicate` - Poll-based read replica
- `walrust explain` - Configuration preview with cost estimation
- `walrust verify` - Backup integrity verification with exit codes
- HADBP changeset format (formerly "LTX") with per-object integrity verification
- Chained page checksums (O(changed pages) not O(entire DB))
- Point-in-time restore by TXID/sequence number (timestamp PITR is not
  implemented — object keys carry only TXID range, not commit wall-clock time)
- Multi-database support
- Prometheus metrics + dashboard
- Webhook notifications (corruption, circuit breaker)
- Retry logic with circuit breaker
- Shadow WAL mode
- Constant RSS regardless of write throughput (~23-31MB with streaming + mimalloc)
- Shared infrastructure via hadb-io (S3, retry, webhooks, retention)

---

---

## Dogfooding (next up)

0.7.0 is on crates.io. The drills prove the mechanisms; dogfooding proves the
product. The frame: walrust is a standalone SQLite replication tool. It must
work in all four postures — CLI sidecar watching one database, CLI sidecar
watching many, embedded as a library inside an app, and as a read replica.
(turbolite is one possible consumer of the library posture, not the target.)
Every scenario below plays a real user and every gap found gets a finding in
the ledger, same rules as the adversarial reviews.

Order of work (each lands as its own PR through the normal gate):

1. **Format-stability fixture (now-or-never).** Freeze a bucket written by the
   published 0.7.0 binary — snapshots, L0 tail, L1+L2 levels, a prune
   boundary — into `tests/fixtures/`, with recorded expected rows and PITR
   points. An S3-gated test uploads the fixture objects to a scratch prefix
   and proves current code restores them row-exact (latest + PITR). Every
   future version must pass it: buckets written today restore forever. Cheap
   now, impossible to create retroactively.
   **DONE** — `tests/fixtures/format-stability/` (cli-v0.7.0 + owned-v0.7.0 +
   `generate.sh`), proven by `tests/format_stability.rs`; see CHANGELOG.
2. **Fresh-user drill.** DONE — `drills/fresh-user.sh` (nightly + dispatchable
   via `.github/workflows/fresh-user.yml`; locally `make drill-fresh-user`);
   five README findings fixed in the same PR, plus two product bugs recorded
   under "Dogfooding findings" above: DF2 (silent replication stall for
   intermittent writers, serious) and DF3 (`walrust pragma` logs a tracing
   line to stdout, breaking the `| sqlite3` pipe). See CHANGELOG Unreleased.
3. **Library dogfood app.** A small real app (axum; sessions table, job-queue
   table with DELETE churn, blob table) depending on `walrust-core` from the
   **registry**, exercising the patterns real embedders need:
   restore-or-create on boot, `shutdown()` on SIGTERM mid-burst, app-owned
   connections alongside the replicator, app-issued `wal_checkpoint(TRUNCATE)`,
   `VACUUM`, an `ALTER TABLE` migration. Runs nightly against live S3; doubles
   as the README's library example. Restore-or-create on boot should also
   settle the ergonomics gap around `sync::resume_owned_after_restore`:
   `Replicator`-level embedders currently have to drop to the sync layer and
   hand-build a `SyncState` to resume a restored database (a
   `Replicator`-level wrapper is candidate follow-up work).
4. **Footgun drill.** Each plausible misuse asserts a *specific loud error*,
   never exit 0 with weirdness: two watchers on the same prefix (fencing, from
   the CLI as a user hits it), restore onto a live open database, bucket-prefix
   typo (must not silently start a fresh lineage over nothing), `prune`/
   `compact` run from a second machine while the watcher runs.
5. **Fleet posture.** Extend `bench/multidb-rss.sh` into a correctness drill:
   100+ databases watched by one process, startup storm, per-DB failure
   isolation (revoke one prefix mid-run; the other 99 keep replicating and the
   failure is loud and attributable), all N restorable at the end.
6. **Real-time soak.** Two parts: (a) the laptop test — `walrust watch` on a
   real database on a real machine for a week (sleep/wake, wifi flaps, clock
   jumps) against live S3; (b) a multi-day VM soak at *production* knobs
   (30s sync, real compaction cadence — not comedy knobs) with a daily
   RSS/lag report. Credential rotation mid-run belongs here: fail loudly,
   recover cleanly, never wedge silently.

### Dogfooding findings (open)

- **DF1 — shadow watch DIES on an ephemeral-connection writer (WAL
  unlink/recreate race).** Found 2026-07-11 by a 10-minute live-Tigris
  laptop-test run of the published 0.7.0 binary, default shadow `walrust
  watch`, writer = `sqlite3` CLI one connection per INSERT every 2s. When the
  last connection on a WAL database closes, SQLite checkpoints and DELETES the
  WAL file; the next write recreates it. ~3 minutes in, the shadow copy read
  the WAL in its transient zeroed state and the process exited:
  `ERROR notes: Shadow copy failed: Invalid WAL magic number: 0x0` (twice,
  then death). Loud, but wrong reaction: an ephemeral-connection writer (shell
  scripts, cron jobs) is normal user behavior, and this is the same event
  class as the downtime-checkpoint / rollover races walrust already survives
  by re-anchoring. Watch should treat invalid-magic/zero-length WAL as a
  re-anchor trigger (loud WARN + snapshot re-anchor, like the salt-mismatch
  path), never process death. Drills never caught it because every drill
  driver holds one long-lived connection. Needs: repro test with an
  ephemeral-connection writer, the re-anchor fix, and a revert-proof test.

- **DF2 — shadow watch silently STOPS REPLICATING after an external WAL
  restart (intermittent writers).** Found 2026-07-12 by `drills/fresh-user.sh`
  (finding F7) against the published 0.7.0 binary, default shadow `walrust
  watch` on a freshly WAL-converted database, writer = short-lived `sqlite3`
  CLI sessions (cron/script usage). The on-startup snapshot's PASSIVE
  checkpoint fully backfills the WAL; the next write from a short-lived
  session (no long-lived app connection pinning the WAL) restarts the WAL, and
  the shadow copier never ships another frame. Silent, which is the wrong
  reaction twice over: no error is logged, `walrust list` keeps showing a
  plausible TXID, `walrust verify` exits 0, SIGINT's "syncing remaining data"
  ships nothing, and a later restore silently returns only the startup
  snapshot (observed 3/3; secondary evidence: a doubled startup snapshot
  gen1+gen2 and a duplicated initial shadow copy at offset 0). Right reaction:
  a WAL restart after full backfill is the same event class as DF1's
  unlink/recreate race — re-anchor loudly and keep replicating, never wedge.
  Continuous-writer workloads (every pre-existing drill) never hit it because
  in-flight writes keep the backfill incomplete. Repro + permanent regression
  probe: `drills/fresh-user.sh` step 12 — on known-buggy 0.7.0 it records F7;
  on any later published version it hard-fails the drill if the rows don't
  replicate, so the probe starts enforcing the moment a new release ships.
  Suspect area: `ShadowWal::copy_frames` salt-change handling vs the checked
  WAL reader after a full-backfill restart. Fix is a durability-path change
  (full adversarial gate), under active fix together with DF1 on
  `fix/df1-ephemeral-writer-wal-race` — deliberately not bundled into the
  drill PR.

- **DF3 — `walrust pragma` logs to stdout, breaking the natural pipe.** Found
  2026-07-12 by `drills/fresh-user.sh` (finding F6) on the published 0.7.0
  binary: with a `walrust.toml` in cwd, config auto-load prints an
  ANSI-colored tracing line ("INFO walrust::config Loaded config from
  ./walrust.toml") on STDOUT ahead of the SQL, so
  `walrust pragma | sqlite3 app.db` dies with a sqlite3 parse error. Wrong
  reaction: tracing belongs on stderr; stdout of `pragma` should be clean SQL.
  Workaround (now in README Quick start):
  `walrust pragma --output pragma.sql && sqlite3 app.db < pragma.sql`. Fix:
  route tracing to stderr — and audit the other commands whose stdout users
  parse (`list`, `explain`) for the same pollution.

---

## Compaction (shipped — default off)

**Status: SHIPPED behind `[compaction] enabled` (default `false`).** All five
waves (C1 format, C2a engine, C2b planner/read-side, C3a CLI wiring, C3b proof
layer) are merged; compaction works end to end for both the CLI and owned mode.
The default staying `false` is a version-skew safety choice (an old binary
cannot restore a leveled bucket); **flipping the default to `true` is a separate
release decision** for once every binary that might restore a bucket understands
the `levels/` layout.

**Shadow-loop guard (C3b adversarial review): leveled compaction only ticks in
the independent-tasks watch loop.** The default shadow loop has no compaction
tick, so rather than silently ignore `[compaction] enabled = true` (a config
no-op that would let a bucket the operator believes is compacting grow
unbounded — an E7 fail-loudly violation), `walrust watch` now **refuses to
start** in shadow mode with compaction enabled and points the operator at
`--independent-tasks`. Unit-tested (`shadow_watch_rejects_enabled_compaction`);
the drill and bench both run `--independent-tasks`.

**Non-blocking residue** (small future items, not shipped in C3b): the C3a
reviewer's note that the legacy L0→L1 idempotency path in
`engine::verify_existing` re-reads the last source's full bytes to recompute
`chain_end` (an extra bounded GET on the crash-recovery convergence path only —
correct, just not the cheapest). Wiring a compaction tick into the shadow loop
itself (so it need not sever) is possible future work, but not required for
correctness now that the sever is loud.

**Residue (e2e gap closure): `replicate` stays levels-blind by design.**
`walrust replicate` only tails the flat gen-0 incremental pool; it never reads
`levels/L*/`. Proven safe as-is (`drills/replica-vs-compaction.sh`, S3-gated):
a replica frozen mid-stream while compaction folds and deletes the exact L0
range it needs next re-bootstraps from the newest snapshot through the
existing F5-era chain-gap handler in `replicate_poll` — no product change was
needed. Teaching `replicate` to read `levels/` directly (skipping the
snapshot re-download) is future work, not required for correctness.

**Residue (e2e gap closure): version skew is now empirically confirmed, not
theoretical.** `drills/version-skew.sh` (manual/`make drill-version-skew`
only — needs crates.io network access) builds a real leveled bucket and runs
a real pre-compaction `walrust restore` (crates.io `0.5.1`, the newest
version published there; `0.5.2` does not exist on crates.io despite being
the version this drill was originally specified against — falls back to
`0.5.1` automatically, both predate compaction by a wide margin) against it.
Observed: **exit 0** with a **corrupt database** (`integrity_check` fails on
the pages that existed only inside the merged-and-deleted range) — worse than
a short restore, and silent (no error surfaced to the operator). The README
version-skew warning is upgraded from theoretical to confirmed with this
citation.

**FIXED (was HIGH PRIORITY): the restart re-anchor seam.** The gap-3 soak drill
(`drills/compaction-soak.sh`) exposed a real, pre-existing bug in the
`--independent-tasks` restart path (NOT compaction-specific): after a
kill/restart, startup resumed the stream with an *incremental* whose
`prev_checksum` was recomputed from the on-disk `.db` file (behind the chain tip,
because SQLite had not checkpointed and the resumed read restarts at
`wal_offset == 0`). That published an L0 object seq/TXID-adjacent to the last
pre-crash L0 but chain-DIScontinuous at the boundary — a "seam". Two symptoms
followed: (1) restore-to-latest walked the seq-adjacent L0s across the break and
hard-failed its pre-apply chain check ("Pre-apply checksum mismatch ... does not
chain"); (2) compaction's `contiguous_batch` selected a seq-contiguous batch
spanning the break that the merge refused (`CompactionError::NonContiguous`)
every tick forever — a permanent liveness wedge, since the pre-seam files stayed
oldest. Data was never wrong (write-verify-delete ordering + the content-anchored
restore chain kept every run row-exact), but restores flaked and compaction
stalled, blocking compaction's default-on.

**Fix (root, not symptom):** startup now re-anchors a *resumed* stream with a
fresh snapshot instead of an incremental
(`walrust_core::legacy_wal_sync::anchor_stream_on_startup`, called from
`sync::watch_independent` startup via `anchor_stream_on_startup_with_retry`). A
snapshot consumes its own seq, so the next incremental starts strictly past every
stale pre-crash L0 (a clean seq GAP at the boundary — exactly the shape
`contiguous_batch` already skips and the restore planner already floors at), it
is a self-consistent base, and post-restart incrementals chain from it. Both
symptoms dissolve together; no chain check was weakened (they still fire loudly on
genuine forks), restore-to-latest still floors at the newest snapshot, and the
E2/watermark/`keep_fine_window` semantics are unchanged. This aligns production
with what the DST state machine's `KillRestart` op always modeled (an eager
re-anchor snapshot), so the DST needed no model change. Guarded by two
fail-on-revert regression tests reproducing the seam deterministically
(`walrust-dst` `reanchor_restart_restore_survives_the_chain_seam` and
`reanchor_restart_compaction_does_not_wedge_on_the_seam`), and the soak now runs
un-crutched (single attempt, restore failures fail hard, any ERROR-level line
fails — the exit-42 retry and the NonContiguous allow-list are removed). The
un-crutched soak also surfaced (and this fix includes) a follow-on `walrust
verify` E3 generalization: `detect_live_txid_gaps` now understands snapshot
supersession (a full snapshot at S covers every TXID <= S), so the healthy
re-anchor shape — snapshot at a hole's start, levels covering the rest — is no
longer a false exit-5 alarm, while unbridged holes still alarm
(`e3_reanchor_snapshot_plus_levels_bridge_is_not_a_gap`).

**Known cost of the fix (documented, not conditioned): a full snapshot on every
restart.** The re-anchor is *unconditional* for `current_txid > 0` — it fires on
clean restarts (deploys, host reboots) as well as crashes. Conditioning it
("resume incrementally when the stream verifiably chains, snapshot only when it
does not") was evaluated and rejected as not cheaply/safely decidable at
startup:
- The chain cursor an incremental resumes from is the in-memory *running*
  page-hash (`chain_checksum`: `SHA256(prev || pgno || page || …)`). The only
  value cheaply derivable from the restarted process is `compute_checksum_from_file`
  (a whole-`.db` SHA), a *different* hash space — and snapshots even VACUUM the
  image first, reordering pages — so the resume point can never be verified equal
  to the head object's `chain_end` from the `.db` alone.
- Seeding the next incremental from the head object's `chain_end` (read from S3)
  *would* chain, but it has a silent **data-loss** hole: if SQLite checkpointed
  and truncated the WAL for pages walrust had not yet shipped while it was down,
  those pages survive only in the live `.db`; a `wal_offset == 0` re-read no
  longer sees them, so an incremental resume would drop them from the stream.
  Only a snapshot of the current `.db` captures them. Startup cannot cheaply
  distinguish "clean, nothing truncated" from "truncated unshipped pages"
  (the same in-memory state that would tell it is gone), so it always takes the
  safe path. This mirrors the existing WAL-rollover handler, which already
  publishes a snapshot rather than chain across a WAL discontinuity.
- **Cost math:** the re-anchor uploads ~one DB's worth of bytes and blocks
  replication of new writes until it completes. For a 10 GB database restarted
  routinely (e.g. per deploy), that is ~10 GB re-shipped and tens of seconds to
  minutes of startup stall *per restart*, plus storage churn until prune/compaction
  reclaims the superseded objects. The drills mask this with tiny DBs (soak runs
  are a few thousand rows), so it does not show up as a drill regression — it is a
  production cost that grows with DB size. A durable local chain-cursor sidecar
  (persist `wal_offset` + salt + running chain-hash on each sync; on restart,
  resume incrementally only when the sidecar still matches the live WAL header and
  `.db`, else snapshot) would let a provably-clean restart skip the snapshot; it
  is deferred as its own change (needs durability + host-move handling + DST
  modeling of its own).

**Residue (e2e gap closure): gap 4 found and fixed a real E7 gap — owned-mode
`add()` was silently incompatible with compaction.** Building
`e2e_core_replicator_compaction_embedder_crash` (tests/production_e2e.rs) — a
real `Replicator` embedded in a spawned child, compaction enabled via
`ReplicationConfig`, written to continuously, SIGKILLed mid-merge activity and
respawned twice — first hit a wall: `Replicator::add()` unconditionally calls
`SyncState::ensure_lineage_id()`, moving the stream's changesets to the
`{db}/lineages/{id}/...` key shape (added by the recent phase-4 delta work).
Compaction's `SeqLayout` only reads/writes the flat, non-lineage
`{db}/0000/...` shape, so a lineage-scoped stream was **completely invisible**
to `maybe_compact_owned` — `compaction.enabled = true` combined with the
normal `add()` path silently never compacted anything, forever, no error, no
warning. The exact same class of violation as the already-fixed CLI
shadow-mode gap (E7: a bucket the operator believes is compacting grows
unbounded). **Fixed**: `add()`/`add_with_wal_path()` now refuse up front
(before touching storage) when `compaction.enabled` is true, naming the
incompatibility and pointing at `add_without_snapshot()` (which never creates
a lineage) as the workaround — mirroring `reject_shadow_compaction`'s shape.
Fail-on-revert proven (`add_with_compaction_enabled_refuses_to_create_a_lineage`
+ a companion test pinning that compaction-off `add()` is unaffected, both in
`crates/walrust-core/tests/replicator_drop.rs`). The e2e itself now registers
via `add_without_snapshot()` on every phase (relying on
`autonomous_snapshots` + a short `snapshot_interval` for the initial base) and
passes: L1/L2 fire, two SIGKILL/respawn cycles survive, and the library
`restore()` API reads the compacted, crash-cycled stream row-exact.
**Adversarial-review follow-up (PR #32):** the guard makes `add_without_snapshot()`
the ONLY working library-mode compaction path, so the README library +
compaction sections now say so explicitly (`add()` is documented as the primary
embedder flow, and it refuses under compaction — the two must agree). Teaching
compaction to fold **lineage-scoped** streams (so `add()` and compaction can
coexist, and multi-node lineage replication can compact) is deliberate future
work: it requires `SeqLayout` (and the planner/prune/restore seam) to understand
the `{db}/lineages/{id}/...` key shape, which is a real feature, not a bug fix.
Until then the lineage-free `add_without_snapshot()` path is the supported one
and the error message names it.

**Residue (e2e gap closure): gap 5 — a non-obvious retention-policy floor that
any short-lived leveled-prune test needs to know about.** Extending
`drills/prune-retained.sh` with a leveled phase (real compacting
`walrust watch --independent-tasks`, L1 **and** L2 both required to fire,
`walrust prune` run against it, before/after level-object listing checked
directly against the watermark rule in
`crates/walrust-core/src/compaction/prune.rs`) kept retaining the on-startup
snapshot no matter how long compaction ran, making the watermark permanently
`1` and the rule untestable (nothing ever below it). Root cause: hadb-io's
`RetentionPolicy` has a hard-coded `minimum: 2` safety floor (not exposed by
`walrust prune`'s `--hourly/--daily/--weekly/--monthly` flags). Any test
whose whole run fits inside one real clock hour collapses GFS hourly
bucketing to a single bucket (one entry: the newest), so the minimum-2 floor
always pads by walking every snapshot **ascending by sequence** and adding
the single oldest one — deterministically the on-startup snapshot. This is
correct, intentional retention-safety behavior, not a bug, but it means the
oldest snapshot in any sub-hour drill run is unconditionally protected
however aggressive the count-based policy is. The drill works around it
directly and safely (see the code comment at the relevant step): once L1 and
L2 have both fired, deletes every snapshot in the leveled phase except the
newest two plus the one hand-recorded PITR target, satisfying the exact same
minimum-2 floor with recent survivors instead of the ancient one — safe
specifically because compaction has, by that point, already folded well past
the early history, so nothing needs an ancient base to restore. Also fixed
along the way (drill-only, not a product issue): `drills/lib.sh`'s
`pause_driver`/`driver_count`/`wait_driver_count_at_least` are hard-coded to
`$DRILL_DB`, an implicit one-database-per-run assumption every other drill
happens to satisfy; this phase needed its own database (to avoid the
minimum-2 floor anchoring on the FLAT phase's old snapshots too) and so
needed local, database-aware replacements (`level_pause_driver` etc.),
documented inline. Passed 5/5 consecutive live-S3 runs after the fix.

**Status:** rename `compact`→`prune` shipped. C1 (COMPACTED v2 format) shipped.
**C2a (layout-agnostic merge engine, write side) shipped** — `CompactionLayout`
trait + seq/range adapters, streaming k-way merge with a proven memory bound,
count-based triggers with `keep_fine_window`, E2-class write→verify→delete
ordering with idempotent crash recovery, and the merge oracle. Merged levels
(`L≥1`) live under a dedicated `{db}/levels/L{n}/` sub-path, **not** a hex
generation folder: the legacy layout increments its snapshot generation per
snapshot, so a `0x0010`-based L1 would collide with the 16th snapshot's `0010/`
folder in both directions; the non-hex `levels/` path is invisible to every
existing discovery scanner. The engine is
wired into both write paths but **gated off** (`compaction_enabled`, default
false, not config-reachable) because enabling it would make backups
unrestorable by the shipped restore path.

**C2b (read side + user exposure) shipped** — the greedy restore planner
(litestream `CalcRestorePlan` shape: newest snapshot ≤ target, then the object
that extends the contiguous range furthest, over `CompactionLayout` + snapshot
discovery), the layout-agnostic restore executor (bounded parallel prefetch
`queue_depth × object_size`, strict-order apply, chain linkage through
`chain_end()`), PITR decay as a hard typed error naming the nearest restorable
points on both sides, level-aware verify (a merged range covering an L0 hole is
a compaction, not a gap; an uncovered hole still exits 5), the level-aware prune
watermark (E2-class, with a fail-on-revert proof), and the single
`[compaction] enabled` config knob (default false; the C2a internal gates are
removed). Un-leveled buckets restore byte-identically to before. A merge-engine
fix now preserves SQLite's end-page-count marker so merged objects apply
cleanly. The e2e proves both layouts restore-to-latest row-exact + PITR
boundary/inside-window + deleted-L0-tail + crash-overlap. C2b **severed the CLI**
(its restore path was not wired to the planner) — leveled compaction was
library/owned-mode only.

**C3a (CLI planner wiring + batch-boundary liveness) shipped** — the C2b CLI
sever is **lifted**. (1) **Liveness**: `run_level_compaction` clips a batch to a
seq-contiguous run instead of a rigid oldest-`batch` window, so a batch that
straddles a snapshot chain-break merges the contiguous prefix (or skips a lone
straddler) and **converges** instead of erroring `NonContiguous` forever
(fail-on-revert proven). (2) **The LTX→HADBP restore seam**: the merge engine
now reads real litestream **LTX** L0 sources (magic-sniffing layout,
`litepages` page stream + synthetic end-page-count marker) and stamps the
produced **HADBP** merged object's `prev`/`declared_end` with the LTX pre/post
of its range, so `legacy_restore::restore_legacy_ltx` — now leveled-aware over
the reused `plan_restore` planner — replays an interleaved LTX↔HADBP chain with
**one running checksum in the LTX domain** (DB-anchored `verify_chain` across the
seam), reusing the C2b TXID PITR-decay error. Cache substitution bypasses
`levels/L*/` objects. `reject_cli_compaction` and its tests are removed;
`[compaction] enabled` works for the CLI too (still default false for version
skew). Proven byte/row-exact across the seam, plus an owned-mode VACUUM-shrink
e2e and an S3-gated real-`walrust watch` CLI e2e (L1+L2 fire, superseded L0
deleted, restore/PITR/verify all correct). The C3a adversarial review executed
that S3 e2e for the first time and fixed three defects it exposed: `verify`'s
snapshot-chain check was not level-aware (false gap on a compacted bucket), and
three read consumers (`list_merged_ranges`, owned `gather_candidates`,
`prune::list_level_files`) stopped at the first empty level — missing a populated
L2 above a fully-promoted (empty) L1 — plus the cache-bypass was made structural.

**C3b (the proof layer) shipped** — the guarantees are now permanent. (1) The DST
state machine learns real compaction (`Op::Compact` drives the real merge engine
to quiescence over the legacy bucket, then grades restores with the
granularity-decay rules from the object listing as ground truth: latest exact,
merged-window boundary exact, strictly-inside a loud typed decay, never a bare
gap, never a silent wrong point — under the existing fault plans too). The
catch-proof (neutering the seq-contiguous batch clip) makes the machine find and
shrink a failing sequence. (2) A `kill-mid-compaction` drill SIGKILLs a real
compacting `walrust watch` in a loop and proves restore-to-latest stays row-exact
and the bucket converges (bounded overlap). (3) A `restore-speed` bench measures
cold restore-to-latest for walrust-with-compaction vs walrust-without vs
litestream — compaction makes walrust's own restore ~7x faster and fetch ~48x
fewer objects (5 vs 242 on a 10k-row history; measured table in the README
Performance section). **Honest gap:** walrust-compacted fetches fewer objects
than litestream (5 vs 25) but does not yet beat its wall-clock restore at small
scale (0.29 s vs 0.09 s) — litestream's per-object apply path is more optimized;
closing that is future work. (4) The oracle found and this wave fixed a real product bug: restart head
discovery was not compaction-aware. **The default stays `false`** — flipping it
is a release decision.

Merge many small incremental changesets into fewer, larger ones so long-history
databases restore fast and buckets stay small. Litestream's level design is the
direct inspiration (studied from its source); walrust adapts it to its own
shape. Serves the goals in order: never corrupt data, restore fast, stay
memory-efficient, don't spam S3.

**The user this serves:** a long-term database whose snapshots are expensive
(big file, daily/weekly snapshot interval). Without compaction, restoring means
fetching tens of thousands of second-grain objects sequentially. With it:
snapshot + a few hour-files + a few minute-files + a seconds tail.

**Design:**

- Two levels above raw sync files. L0 = raw (~1s) sync objects. L1 = minutes-
  grain, merged from L0. L2 = hours-grain, merged from L1. Then snapshots, then
  GFS prune — both unchanged. Config leaves room for more levels; only these
  two get built.
- **Count-triggered, not clock-triggered.** The watcher merges when a batch
  fills (it already knows how many files it wrote — no LIST polling). Idle
  database = zero compaction requests. This beats litestream's wall-clock ticks
  for many-database and mostly-idle deployments.
- Knobs (names speak user intent, defaults conservative):
  `[compaction] enabled` (false in the release that ships it — see version
  skew), `keep_fine_window = "1h"` (L0 younger than this is never merged: "I
  can restore to the second within the last hour"), `l1_batch`, `l2_batch`.
- **Chain linkage survives compaction** (unlike litestream, whose compacted
  files verify individually but not as a sequence): a small HADBP extension —
  a COMPACTED flag plus a declared end-of-range chain value copied from the
  last source file. Linkage stays verifiable end to end; content keeps its own
  checksum; restore still verifies pre/post-apply against actual bytes.
  Requires a one-field, backward-compatible hadb-changeset change (we own it).
- **Restore speed is the point.** The greedy planner (ported from litestream's
  CalcRestorePlan: newest snapshot ≤ target, then any-level file that extends
  the contiguous range furthest) knows the full file list upfront — so restore
  prefetches the plan with bounded parallelism and applies strictly in order.
- **Memory rule:** merging streams with peak memory O(page_size × sources +
  index), never O(total bytes). Hard requirement, not an optimization.
- **Safety invariants:** write merged file durably → verify it → only then
  delete sources (crash between = harmless overlap; the planner tolerates
  overlap and re-compaction is idempotent). Prune must never delete objects a
  retained restore point needs — same E2-class rule, now level-aware. PITR
  granularity decays with age by design; a point inside a merged window
  restores at window grain or fails loudly — never a chain gap.
- **Version skew is the compat risk:** old binaries restoring a compacted
  bucket don't know levels exist. Ship restore/verify/planner support first
  with `enabled = false`; flip the default a release later.

**Built once, for both layouts.** The CLI already uses walrust-core everywhere;
what remains split is two storage layouts inside the core (the litestream-
heritage `min-max.ltx` range layout and the owned-mode one-object-per-seq
layout). Compaction is written once in walrust-core against a thin layout
trait (list-at-level / read / write-ranged / delete), with both layouts as
small adapters — the merge engine, triggers, planner, and safety proofs are
shared. Never give one layout a capability the other lacks; that is the
dual-tree disease that caused half the original findings.

(Separate future item: assess unifying the two layouts entirely. Bucket
migration risk keeps it out of this wave, but the split itself is debt.)

**Order of work:** rename `compact` → `prune` first (retention expiry is
pruning, not compaction — borg/restic precedent; frees the name). Then the
hadb-changeset extension, then the layout-agnostic compactor + planner in
walrust-core with both layout adapters, then the state-machine oracle
extension (granularity decay), kill-mid-compaction drill, and a bench
restore-speed comparison on a long history — the "faster restore than
litestream" claim ships as a measured number or not at all.

---

## Phase Drain: Synchronous Flush for Graceful Shutdown

> After: v0.7.0 (hadb-io migration) · Before: SnapshotSource trait

`SqliteReplicator::sync()` (haqlite's `Replicator` impl) is currently a no-op. walrust syncs WAL frames to S3 on a background timer (1-2s). There is no "flush now and wait" API. This means `close()` in haqlite cannot guarantee that the last 1-2s of writes are in S3 before releasing the lease.

### Drain-a: Add `flush()` to walrust-core Replicator

Add a synchronous flush method that:
1. Captures any pending WAL frames since the last background sync
2. Encodes them as LTX
3. Uploads to S3 (blocking until PUT completes)
4. Returns only after S3 has confirmed receipt

This is the internal API. The `shadow::ShadowReplicator` and `sync::SyncReplicator` both need it.

Source: `walrust-core/src/sync.rs` (background sync loop has the encode+upload logic, extract into a callable `flush()`)

### Drain-b: Wire into SqliteReplicator::sync()

`haqlite/src/replicator.rs:69-76` -- replace the no-op with `self.inner.flush().await`. One-line change.

### Drain-c: Tests

- flush() uploads pending frames immediately (not on timer)
- flush() returns only after S3 PUT succeeds
- flush() is idempotent (no pending frames = no-op)
- haqlite close() after flush() has zero data loss

---

## SnapshotSource trait (turbolite integration) -- partially done

Pluggable snapshot source for restore/recovery. Instead of downloading an LTX snapshot,
walrust calls a trait method to materialize the base DB. turbolite implements this using
S3 page groups as the snapshot.

- [x] `SnapshotSource` trait in walrust-core with `materialize()` and `checkpoint_version()`
- [x] `restore_with_snapshot_source()` applies incrementals after materialized version
- [x] Tests: 8 unit (mock) + 3 S3 integration (real Tigris)

---

## Phase Somme: Replay Cursor Alignment

> After: SnapshotSource · Before: Rename

Historical note: the first design tried to make `manifest.version`, SQLite's file
change counter, and walrust txid all be the same number. Sashimono split those
concepts:

- Turbolite `manifest.version` is a monotonic object-key/publication version.
- Turbolite `manifest.change_counter` is the durable replay cursor/floor.
- walrust/HADBP delta sequences must be greater than the replay cursor.
- SQLite's file change counter is one useful input, but WAL mode and direct page
  replay mean it is not the whole contract.

Current work should preserve that split and test recovery as "materialize base
at replay cursor N, apply delta objects with seq > N."

## Rename to walsync (future)

walrust is Rust-specific. For cross-language composability (Python/Node/Go SDKs), rename to
walsync. The Rust crate stays walsync-core, packages are walsync-python, walsync-node, etc.

## Residual risk register

Carried over from the adversarial-review ledgers (`ADVERSARIAL_REVIEW.md` /
`ADVERSARIAL_REVIEW_2.md`, removed 2026-07-11; the full ledgers live in git
history). Everything not listed here was fixed with a revert-proof test. The
rule stands: nothing vanishes silently — update this register when touching
anything it covers.

- **R1 (was D1) — no multi-writer lease, by design.** walrust is
  single-writer. A second writer fails loudly on CAS collision; the local
  `PublishIntent` authorship proof is a durability fence, not a distributed
  lease. A true lease/epoch token belongs in an external coordinator (the
  hadb internal-lease-store work), not in walrust.
  `sync::resume_owned_after_restore` follows this split: its
  `OwnedResumeLease` is a caller-supplied guard hook (re-checked at every
  phase and inside every retried storage attempt), not a walrust-acquired
  lease — walrust never acquires, renews, or releases it.
- **R2 (was D6) — cross-generation same-(min,max) cache collision.**
  Theoretically possible; backstopped by restore's `pre/post_apply` checksums
  (computed from actual DB bytes), which catch a wrong-lineage substitution
  downstream. Trigger: two generations producing an identical `(min,max)`
  range while the local cache is warm. Suggested fix: bind the cache key to
  lineage/etag.
- **R3 (was E9) — rollover publish-failure window.**
  `upload_rollover_snapshot` folds the WAL into the local `.db`
  (`checkpoint_wal_truncate`) *before* putting the rollover snapshot; if the
  put fails, the folded rows sit local-only until the next successful tick or
  restart re-detects the rollover and republishes (idempotent, full-DB
  snapshot). Adjudicated NOT silent loss: no `Ok` ever acknowledges those
  rows as durable (the durability cursor advances only on `frame_count > 0`),
  and the DST oracle models the window exactly. Suggested defensive
  narrowing: reverse the truncate/put order — critical-path work, take it
  deliberately.
- **R4 — legacy CLI snapshots still go through `VACUUM INTO`.** The owned-mode
  snapshot paths (`sync::take_snapshot` / `take_snapshot_with_retry`) were
  fixed to encode the checkpointed main file directly, because VACUUM can
  renumber b-tree/overflow pages and later WAL-frame incrementals then target
  the wrong physical pages in the restored image (proven: reverting the owned
  fix makes `public_owned_resume_round_trips_lineaged_mixed_workload` fail
  integrity_check with a broken overflow chain and orphan pages). The legacy
  CLI path (`legacy_ltx.rs` `StableSqliteSnapshot::create`) still snapshots
  via `VACUUM INTO`, and its incrementals are also physical WAL frames of the
  live layout — the same hazard class, unproven either way for legacy.
  Trigger: a fragmented database whose vacuumed page layout differs from the
  live layout, plus post-snapshot incrementals, then restore. Drills have not
  hit it (append-heavy workloads vacuum to a near-identical layout).
  Suggested fix: check whether the legacy checkpoint story pins the main file
  the way owned mode's blocker does, and if so make the same swap there — its
  own careful wave with a fragmented-layout regression test, not a drive-by.

## Future Considerations (v1.0+)

**Not planning yet, but might be useful:**

### Push-Based Read Replicas
- Push-based replication (requires network)
- Lower latency than polling

### Additional Features
- Multi-region replication
- Encryption at rest

**Philosophy:** Ship working features, not roadmaps. Only add features when users ask for them.

---

## Completed Features (see CHANGELOG.md)

**v0.7.0 (unreleased):**
- Migrated to hadb-io for shared S3/retry/webhook/retention infrastructure (~4,275 lines deleted)
- README rewritten with architecture diagrams, simplified config, read replica docs

**v0.6.0:**
- Concurrent S3 uploads via JoinSet (max_concurrent configurable, default 4)
- Shadow mode cache integration (disk-based upload queue + crash recovery)
- Cache cleanup timer in shadow mode (every 5min)
- Proper shutdown drain via JoinHandle for spawned uploader tasks
- 31 new tests (18 uploader + 13 shadow cache)

**v0.5.2:**
- Streaming snapshot encoding — BufReader(1MB) + page-by-page instead of std::fs::read()
- Streaming compute_checksum_from_file — same pattern
- mimalloc global allocator — returns freed memory to OS
- RSS profiling bench tools (profile_rss.rs, measure_rss.py, measure_rss_s3.py)
- RSS: 70MB → 20MB

**v0.5.1:**
- Fixed RSS scaling linearly with write throughput (67MB→361MB) — now constant ~70MB
- Streaming `ChainHasher` for incremental checksum verification during LTX decode
- `read_frames_as_page_map()` — streaming WAL dedup (peak memory = unique pages)
- Shadow WAL streaming dedup, retry buffer sharing via `Arc<Vec<u8>>`

**v0.5.0:**
- Chained page checksums — eliminated full-DB read from sync hot path
- `wal_page_overlay` HashMap removed
- Page clone elimination in dedup and encode paths

**v0.4.0:**
- Split watch.rs (1856 lines) and restore.rs (1083 lines) into focused modules
- Wired periodic validation into watch_independent mode
- Wired cache cleanup (retention_duration, max_cache_size) into watch_independent
- Deleted dead watch modes (watch_simple, watch_config) and ~350 lines of dead code
- Removed all `#[ignore]` tests — 346 tests pass, 0 ignored

**v0.3.2:**
- `walrust explain` command with cost estimation
- `walrust verify` with exit codes, continuity checks, webhook integration
- Webhook notifications for corruption and circuit breaker events
- Published to crates.io

**v0.3.1:**
- Refactored sync.rs into focused modules
- Extracted litepages to separate repo

**v0.3.0 and earlier:**
- LTX format integration
- Point-in-time restore
- Multi-database support
- GFS retention policy
- Prometheus metrics
- Webhook notifications
- Retry logic with circuit breaker
- Shadow WAL mode
- Read replicas
- DST (Deterministic Simulation Testing)
- See CHANGELOG.md for full history
