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
2. **Fresh-user drill.** Clean container, `cargo install walrust` from
   crates.io, follow the README *verbatim* to a verified restore — no repo
   checkout, no improvising. Every deviation forced by reality is a docs bug.
   Include the "bad migration 10 minutes ago" exercise: find the right PITR
   point using `walrust list` output alone.
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
  by re-anchoring. Drills never caught it because every drill driver holds one
  long-lived connection.
  **DF1 and DF2 are now understood as symptoms of the architectural gap in the
  "Lossless watch" section below — do not fix them in isolation.** PR #41
  (re-anchor-after-the-fact) is held unmerged: it makes data safe but treats
  the symptom and pays a full snapshot per event (the R5 storm). The root fix
  is holding the checkpoint blocker so the WAL is never lost in the first
  place.

- **DF2 — shadow watch SILENTLY stops replicating short-lived-session writes
  after the startup passive checkpoint.** Found 2026-07-11 by the fresh-user
  drill (PR #40), reproduced 4/4 on 0.7.0. Same ephemeral-writer WAL lifecycle
  as DF1, worse symptom: `list`/`verify` look healthy, restore silently
  returns the day-one snapshot (silent wrong data — the worst failure class).
  Also a symptom of the gap below.

---

## Lossless watch: adopt the checkpoint-blocker model (the litestream contract)

**This is the key correctness issue for walrust as a litestream replacement.**
Surfaced by dogfooding (DF1/DF2) and confirmed at the primitive level with a
direct SQLite probe.

### Current state (the problem, with evidence)

walrust's CLI `watch` is built on a checkpoint race it can lose. A SQLite
checkpoint folds WAL frames into the main `.db` and can reset/truncate the
`-wal` file. If that happens **before** walrust has read those frames, they are
gone from the sync stream. Measured directly:

```
[file-tailer, no held reader]  WAL 12392B -> 0B after an app wal_checkpoint(TRUNCATE)  => unread frames GONE
[held read-mark]               WAL 12392B -> 12392B, checkpoint returns busy=1        => BLOCKED, frames preserved
```

Three ways the race bites, all the same root: an app autocheckpoint burst
(default 1000 pages) between walrust polls; an explicit app
`wal_checkpoint(TRUNCATE)`; an ephemeral-connection writer whose last-close
checkpoint deletes the WAL (DF1 = crash on the zeroed WAL, DF2 = silent loss
when the reset isn't detected). Today walrust survives *most* of these by
detecting a WAL reset (salt mismatch) and re-anchoring with a full snapshot —
**lossy-but-recovered**. DF2 is the case the recovery missed.

Per-mode reality (verified in code):
- **Owned / library mode (`Replicator`) holds the blocker but invalidated it
  after arming.** It holds
  `crate::shadow::ShadowWal::open_checkpoint_blocker` for the DB's lifetime:
  `wal_autocheckpoint=0`, a `_walrust_seq` heartbeat row, and a `BEGIN DEFERRED`
  read transaction pinning a real WAL frame — exactly litestream's
  `_litestream_seq`. The held read-mark is what makes it lossless: another
  connection's checkpoint cannot truncate past walrust's mark. `sync.rs` already
  has the controlled `release_checkpoint_blocker` / `reacquire_checkpoint_blocker`
  dance around walrust's own checkpoints (D2). **But** its snapshot lifecycle
  then reopened/closed other main-DB descriptors (one-shot checkpoint
  connections, raw `get_page_size` / change-counter / snapshot-encode reads),
  which silently released the blocker's process-wide POSIX locks — see the
  measured diagnosis below.
- **CLI shadow mode (`src/sync/watch_shadow.rs`) arms the same blocker in
  `ShadowWal::new()`** (the old claim here that it "holds no connection at all"
  was wrong: `ShadowWal::new()` has opened `open_checkpoint_blocker()` since it
  landed). Its snapshot path (`take_snapshot_to_storage`: one-shot PASSIVE
  connection, raw page-size read, fresh `VACUUM INTO` connection) invalidated
  the blocker the same way, and its `ShadowWal::checkpoint` dropped and
  re-opened the blocker connection every dance.

- **CLI independent mode (`crates/walrust-core/src/legacy_wal_sync.rs`)** runs
  `wal_checkpoint(PASSIVE/TRUNCATE)` through **one-shot** `Connection::open`s, so
  it holds no persistent read-mark either, and by opening/closing may itself
  trigger last-close checkpoints.

#### Measured diagnosis (2026-07-17, macOS, bundled SQLite 3.46)

The blocker's protection is two locks, not one:

1. The pinned **read mark** (POSIX lock on the `-shm` inode) — blocks
   `wal_checkpoint(TRUNCATE/RESTART)` from other processes. Robust: nothing in
   the process ever opens the `-shm` file outside SQLite's VFS.
2. A **SHARED POSIX lock on the main-DB inode**, taken by the blocker
   connection at its first write and held for the connection's lifetime
   (measured `F_GETLK`: `F_RDLCK` at `PENDING_BYTE+2..+510`, persisting across
   ROLLBACK/re-BEGIN/COMMIT). This is the only thing stopping another
   process's last-connection close from taking the EXCLUSIVE main-db lock its
   close-time checkpoint needs before it **unlinks `-wal` and `-shm`** —
   PASSIVE close-checkpoint returns OK on a partial fold, so the delete
   proceeds with unread frames still in the WAL.

Classic POSIX semantics: closing **any** descriptor the process holds for an
inode releases **all** of the process's fcntl locks on that inode. One stray
raw `File::open(db)` + close (page-size read, change counter, checksum,
snapshot encode) destroyed lock (2) while the blocker object still appeared
alive. (Same-process SQLite connection churn is safe — the unix VFS parks a
closing connection's fd while the inode has outstanding locks — but the fix
routes everything through retained handles anyway.) Measured at the
production call sites: external `TRUNCATE` still reported `busy=1` (read
mark intact), and the WAL was unlinked one close later (`cli.db`/`owned.db`
repro; DF2 shape: short-lived writer's WAL gone right after the snapshot
lifecycle). The fix is a retained-handles lifecycle:
open monitor connection + source descriptor before arming, arm last, never
open/close another main-DB descriptor while armed, reuse and re-pin the
blocker connection across the controlled checkpoint, and detect window
commits two ways: `PRAGMA data_version` catches commits inside the
release/re-pin window (PASSIVE dance; a TRUNCATE restarts the wal-index
header, which itself bumps data_version, so the owned dance is
safe-by-construction instead: folded commits land in the post-dance snapshot,
later commits ride the fresh WAL), and a folded-extent check
(`checkpointed_frames` vs the shadow's copied WAL cursor) catches a commit
that landed between the last shadow copy and the dance — folded by walrust's
own PASSIVE and erased by the re-pin WAL restart, invisible to data_version.

### What must not change

- **Data correctness and every never-weaken test.** This is a durability path.
- **Restore-side WAL-header strictness** (`wal::read_header`) — watch-side only.
- **Owned/library mode's existing blocker semantics** (D2) — we are extending
  the *same* proven mechanism to the CLI, not inventing a second one.
- **The single-writer / fencing / split-brain guarantees.**

### The fix, phased (each phase its own PR through the full adversarial gate)

- **Phase 0 — prove the primitive on a real DB.** Formalize the probe above as a
  test: a held `open_checkpoint_blocker` on a real SQLite DB makes a concurrent
  app `wal_checkpoint(TRUNCATE)` return `busy=1` and preserves the WAL, across
  every WAL config walrust supports (page sizes, `synchronous` levels). This is
  the load-bearing assumption; pin it before building on it.
- **Phase 1 — CLI shadow watch holds the blocker.** Give `ShadowDbState` a
  persistent `open_checkpoint_blocker` connection per DB, reuse the
  release/reacquire dance for walrust's own controlled checkpoints, and add
  **WAL-size backpressure** (see new failure mode below). With the blocker held,
  the DF1/DF2 e2es must pass with **zero re-anchors and zero storm** — a strictly
  stronger result than PR #41's. Add an "app-checkpoints-underneath" e2e: an app
  connection issues `wal_checkpoint(TRUNCATE)` mid-stream and **nothing is lost**.
- **Phase 2 — CLI independent mode holds the blocker.** Replace its one-shot
  checkpoint connections with the held blocker + controlled dance; same proofs.
- **Phase 3 — demote the file-tailer to an explicit, labeled degraded mode.**
  For the genuine "can't open the DB" case (read-only mount, no write access,
  strict no-touch policy), keep the connection-free tailer behind an explicit
  opt-in (e.g. `--file-tailer` / `watch_mode = "file-tailer"`), documented as
  **best-effort, lossy under checkpoint races, mitigated by resnapshot**. This is
  where PR #41's surviving machinery lives: `read_header_classified`
  (missing/zero-length vs nonzero-garbage magic) and the re-anchor-on-reset path
  become the degraded mode's hardening — not the default's front line. Decide
  during this phase whether the degraded mode earns its keep at all.
- **Phase 4 — docs & positioning.** The "lossless like litestream" claim becomes
  true *and provable*; update README (the ephemeral-writer known-issue note flips
  once a release ships the fix — until then a crates.io 0.7.0 user is still
  exposed, so no false safety claim), and the fresh-user drill's DF2 probe flips
  from "record known-buggy" to "enforce replication."

### The new failure mode this introduces (must be handled, not just accepted)

Once walrust holds the read-mark, **walrust becomes the only thing that can let
the WAL truncate.** If walrust falls behind or wedges, no one can checkpoint, the
WAL grows unbounded, and the app's writes eventually slow or stall. This is the
litestream tradeoff — a strictly *better* failure mode than silent data loss
("backup is behind, WAL is growing" is loud and observable) but it is a real new
responsibility. Phase 1 must: bound WAL growth via walrust's own checkpoint
cadence, and **alarm loudly (webhook + error log) when the WAL exceeds a
threshold** rather than let it bloat silently. Register this as the explicit cost
of the model in the docs, next to the `_walrust_seq` write (shadow mode stops
being "zero-touch": it opens the DB and writes one heartbeat row, exactly as
litestream does — call it out plainly for operators).

### Known traps (greppable)

- **Wrong:** disabling the app's autocheckpoint to stop truncation. You can't —
  `wal_autocheckpoint` is per-connection and you don't own the app's connection.
  **Right:** the held read-mark blocks truncation *past your mark* regardless of
  the app's autocheckpoint; the WAL grows, you read it, then you checkpoint.
- **Wrong:** using SQLite's file change counter (header offset 24) as a
  dirty-check — it does **not** advance per-commit in WAL mode (verified). **Use
  `PRAGMA data_version`** (verified: bumps on another connection's commit,
  pre-checkpoint) for any residual dirty-check, not a whole-file hash.
- **Wrong:** treating the blocker as free. It creates `_walrust_seq` and holds a
  read txn — a real, litestream-precedented change to shadow mode's contract.
- **Wrong:** letting the WAL grow without a loud alarm. Silent bloat that stalls
  the app's writes is a fail-loudly violation.
- The release/reacquire window around walrust's own checkpoint is the one place a
  reset can still slip in — that boundary keeps the re-anchor handling (this is
  what survives from PR #41).

### How we prove it's done

- Phase 0 primitive test green.
- DF1/DF2 e2es pass with **zero re-anchors, zero storm** (grep the logs), both
  modes.
- New app-checkpoint-underneath e2e: nothing lost, live S3.
- WAL-bloat alarm fires (revert-proof: neuter the alarm, watch the test catch a
  silent bloat).
- The "lossless like litestream" claim ships as a passing drill, not prose.

### Disposition of the open PRs

- **PR #41 stays open, unmerged**, until Phase 1/3 land and cherry-pick the parts
  that survive (`read_header_classified`, degraded-mode re-anchor). Do not merge
  it as the primary fix; do not close it and lose the machinery.
- **PR #40 (fresh-user drill)** can merge independently — its DF2 known-issue note
  and version-gated probe are accurate while 0.7.0 is the shipped (lossy) binary.

### Exact commands (for the executor)

- Worktree: `git -C /Users/russellromney/Documents/Github/walrust worktree add
  ../walrust-lossless -b feat/lossless-watch-blocker origin/main`
- Wrapper-only `.cargo/config.toml`; `CARGO_TARGET_DIR=/Users/russellromney/Documents/Github/walrust/target`,
  `RUSTC_WRAPPER=sccache`, `--offline`; narrowest targets
  (`cargo test -p walrust --lib watch_shadow`, `-p walrust-core --lib`).
- Live S3: `~/.soup/bin/soup run -p turbolite -e development -- <cmd>`.

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
