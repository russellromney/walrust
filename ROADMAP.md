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
- **Owned / library mode (`Replicator`) already does it right.** It holds
  `crate::shadow::ShadowWal::open_checkpoint_blocker` for the DB's lifetime:
  `wal_autocheckpoint=0`, a `_walrust_seq` heartbeat row, and a `BEGIN DEFERRED`
  read transaction pinning a real WAL frame — exactly litestream's
  `_litestream_seq`. The held read-mark is what makes it lossless: another
  connection's checkpoint cannot truncate past walrust's mark. `sync.rs` already
  has the controlled `release_checkpoint_blocker` / `reacquire_checkpoint_blocker`
  dance around walrust's own checkpoints (D2).
- **CLI shadow mode (`src/sync/watch_shadow.rs`) holds no connection at all.**
  `ShadowWal::new()` opens no SQLite connection; it tails the `-wal` file on
  disk. Its "checkpoint" (`checkpoint_shadow_after_durable_sync`) only flushes
  the shadow segment to S3 — it never checkpoints the real DB. So WAL truncation
  is entirely at the app's mercy. `git log -S open_checkpoint_blocker` confirms
  this path has **never** held the blocker (despite an old ledger note claiming
  "shadow mode uses" it — the note was aspirational).
- **CLI independent mode (`crates/walrust-core/src/legacy_wal_sync.rs`)** runs
  `wal_checkpoint(PASSIVE/TRUNCATE)` through **one-shot** `Connection::open`s, so
  it holds no persistent read-mark either, and by opening/closing may itself
  trigger last-close checkpoints.

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
- **Wrong:** opening and closing the main database file after taking the
  blocker. On systems without open-file-description locks, closing any file
  descriptor for that inode can release the process's SQLite locks while the
  transaction object still appears live. Open one read-only source descriptor
  before the blocker, retain it until every SQLite source handle is closed, and
  reuse that exact descriptor for native snapshot base-page reads.
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

## Local-first native HADBP spool for lossless CLI watch (required Phase 1 refinement)

**Do not merge PR #43 as it stands.** Its real SQLite checkpoint blocker is the
right primitive, but its checkpoint gate still waits for a confirmed S3 PUT. The
default CLI shadow-watch pipeline must instead be:

```
SQLite WAL -> fsynced shadow frames -> fsynced native HADBP object + journal
           -> controlled SQLite checkpoint + immediate blocker reacquisition
           -> asynchronous upload of those exact HADBP bytes
```

`crates/walrust-core/src/ltx.rs` is the native HADBP codec despite its historical
module name. `legacy_ltx.rs`, `legacy_shadow*`, `legacy_wal_sync`, `LocalCache`,
and `.ltx` spool files are actual Litestream-heritage compatibility machinery.
They remain readers for published 0.7 history, not the new write architecture.

### Proven current format map

- Fresh CLI snapshots are actual LTX1 snapshot files at
  `{prefix}{db}/{generation:04x}/0000000000000001-{txid:016x}.ltx`.
  CLI incrementals are actual LTX1 files in generation `0000` named
  `{min_txid:016x}-{max_txid:016x}.ltx`. `manifest.json`, legacy discovery,
  verify, pruning, replication, and CLI restore all use this TXID/checksum
  domain. `LocalCache` stores the same LTX1 bytes as `ltx/{txid:08}.ltx`.
- CLI compaction's RangeLayout consumes those LTX1 objects. Its `levels/L*`
  merged payloads are HADBP but retain the historical `.ltx` key suffix; the
  legacy restore path sniffs their `HADBP` magic and bridges them in the LTX
  checksum domain. This frozen compatibility seam is not a precedent for new
  HADBP keys.
- Owned/library replication uses native HADBP snapshots and deltas under
  generation directories with `.hadbp` suffixes (and optional `lineages/`
  scope). SeqLayout compaction also writes HADBP under `levels/L*/*.hadbp`.
  Native restore enforces HADBP sequence, predecessor, checksum, page-count,
  lineage, and fencing rules.
- Published 0.7 CLI restore-to-latest/PITR therefore remains the legacy reader;
  published 0.7 owned buckets remain the native reader. New CLI restore first
  resolves the versioned native boundary below, and uses the legacy reader for
  targets before that boundary.

### Versioned remote layout and visibility

New CLI-native streams use this disjoint namespace:

```
{prefix}{db}/native/v1/stream.json
{prefix}{db}/native/v1/lineages/{lineage}/0001/{seq:016x}.hadbp  # snapshot
{prefix}{db}/native/v1/lineages/{lineage}/0000/{seq:016x}.hadbp  # delta
{prefix}{db}/native/v1/lineages/{lineage}/published/{seq:016x}.json
```

`stream.json` is an immutable, create-if-absent descriptor binding the canonical
stream/destination identity, lineage, first native snapshot sequence, and an
optional verified legacy-LTX boundary TXID. A full native snapshot is always the
migration boundary; no LTX-to-HADBP incremental checksum seam is invented.

Each `published/` record is immutable and binds the exact object key, kind,
sequence, predecessor publish-record digest, HADBP predecessor/ending checksums,
declared end-page count, payload length, and SHA-256 payload digest. Publication
uses create-if-absent and verifies exact existing bytes after a failed CAS. The
visible remote head is the highest contiguous verified publish-record chain
starting at the descriptor's snapshot base. A raw object PUT without its record
is not a recovery point. Restore, verify, prune, replicate, and compaction must
never traverse past that visible head. This makes PUT-before-record crashes
retryable and prevents a delta from becoming visible before its base.

The namespace cannot collide with legacy LTX: legacy object discovery accepts
`.ltx` generation/range shapes, while this layout uses an extra `native/v1`
scope and `.hadbp`. It cannot collide with compaction levels because no path has
the structural `levels/L{n}` pair. Frozen legacy and `levels/L*` readers remain
unchanged except for combined boundary selection in current CLI commands.

### Durable local identities and object record

The spool is independent of `LocalCache` and is rooted in a collision-safe hash
of canonical database path plus destination bucket/prefix/database identity.
Its immutable payload filenames end in `.hadbp`, never `.ltx`. The versioned
stream journal binds canonical local and remote identity, lineage, verified
remote base/boundary, local source cursor, local admitted cursor, and contiguous
remote publish cursor. Every immutable object record binds at least:

- journal/object schema version and canonical stream/lineage identity;
- destination bucket, prefix, and database identity;
- native sequence and snapshot/delta kind;
- previous and ending chain checksums and declared end-page count;
- intended remote key, payload length, and SHA-256 payload digest;
- source shadow/WAL cursor covered by the object;
- local creation state and remote upload/publication state.

Payload installation is: write a same-directory temporary file, fsync it,
rename atomically, fsync the directory, then atomically write/fsync/rename the
journal and fsync its directory. A channel message is only a coalesced wake hint.
An existing sequence is accepted only after header, lineage, predecessor,
sequence, source cursor, length, and digest all match; any divergence is a hard
equivocation error.

### Exact crash/restart state machine

The blocker is held except for the bounded controlled-checkpoint window.

1. **Before shadow fsync:** the live WAL remains pinned; restart recopies only a
   validated committed frame prefix. No cursor advances.
2. **After shadow fsync, before HADBP encode:** the durable shadow cursor is
   replayed into the next native object; SQLite is not checkpointed.
3. **After payload fsync, before or after payload rename, before journal
   commit:** the snapshot source intent plus the fixed same-directory temporary,
   or the generic install intent plus temporary/final payload, binds the exact
   source cursor and object identity. Startup validates the HADBP header/body,
   predecessor, sequence, page size/count, checksum, source cursor, intended
   key, length, and digest. It adopts a uniquely proven object by installing the
   exact bytes and committing the journal; otherwise a complete divergent object
   is retained and fails loudly. A demonstrably incomplete pre-admission encode
   may be removed because no checkpoint was released for it.
4. **After journal commit, before SQLite checkpoint:** the object is locally
   admitted. Restart may checkpoint that exact admitted cursor without S3.
5. **After SQLite checkpoint, before blocker reacquisition:** startup opens and
   pins the blocker before any other mutable work, compares `data_version`, WAL
   salt/cursor, shadow/admitted cursor, and main DB. A dirty controlled window
   creates a full native snapshot re-anchor through the same spool before any
   delta continuation. On POSIX, blocker reacquisition is the final SQLite
   operation in the successful checkpoint path.
   Before opening that window, checkpoint preflight repeats the checked shadow
   copy and native delta admission until one complete `data_version` sample to
   sample interval is stable. A commit copied and admitted while the blocker is
   still held is therefore drained as another delta, not misclassified as a
   dirty-window re-anchor. Sustained writers bound this preflight and defer the
   checkpoint with the blocker held; a commit after the stable sample remains a
   dirty controlled-window event and requires the full snapshot re-anchor.
6. **After PUT, before uploaded-state commit:** the uploader GET-verifies exact
   remote bytes and idempotently records the object uploaded locally. Divergent
   remote bytes are split brain/equivocation and are never overwritten.
7. **After uploaded-state commit, before visible-head advance:** the uploader
   verifies the descriptor, remote predecessor publish record, base snapshot,
   and object, then create-if-absent publishes the exact next record. Restart
   repeats this operation; a divergent record is split brain.
8. **During local cleanup:** journal state is authoritative. Temporary/orphan
   files are removed only after validation; pending/unpublished objects and the
   only locally restorable snapshot base are never removed. Each delete is
   followed by directory fsync and is restart-idempotent.
9. **During snapshot creation:** with the checkpoint blocker held, copy and
   fsync the checked live-WAL committed prefix into shadow, then freeze its
   generation/frame cursor, WAL salt/checksum chain, page size, and final commit
   page count in a durable snapshot intent. Resolve each page at that exact
   boundary from the latest shadow frame or (when absent) the pinned main DB,
   and encode directly into the native HADBP payload temporary. Fsync/rename
   the HADBP payload and commit its journal record before checkpoint release.
   Main-database pages use the one descriptor opened before blocker acquisition;
   snapshot creation never reopens/closes the source database inode, preserving
   classic POSIX locks on non-OFD platforms.
   The payload file and its directory entry are fsynced before the named crash
   boundary. No intermediate SQLite backup/VACUUM database is part of this path.
   Partial HADBP temporaries are validated or removed on restart; a complete
   fsynced temporary or installed orphan with a matching intent is adopted; an
   admitted snapshot is never regenerated at the same sequence.
10. **During shutdown:** stop admitting new checkpoint windows, keep/reacquire
    the blocker, durably finish any in-progress local admission, persist pending
    uploader state, and optionally drain cloud for a bounded time. Timeout does
    not delete pending work. SIGKILL follows the same startup reconciliation.

### Checkpoint release policy

The explicit setting is `checkpoint_release = "local" | "remote"`.

- `local` is the default. Release requires durable native HADBP bytes and the
  matching durable local cursor/lineage record. It never waits for S3 PUT,
  LIST/GET, retries, uploader channel capacity, or remote-head advancement.
- `remote` stages locally first, then waits for the contiguous remote publish
  cursor covering the admitted object before release. This adds cloud latency
  to walrust-controlled checkpoints; it does **not** make every SQLite commit
  synchronously cloud-durable.

PASSIVE busy/partial results are contention: record progress, rearm, and retry.
Emergency TRUNCATE is bounded and observable. Failure rearms the blocker and
leaves watch alive in a degraded non-checkpointing state.

### Startup, ownership, and publication

First startup must successfully verify remote absence or the existing legacy or
native head before creating local identity. With a complete matching spool, a
watcher may restart and stage offline only atop its last verified remote
base/lineage. Missing or mismatched identity/base is a loud offline refusal.
Reconnect verifies the recorded remote predecessor before every publication.
An incompatible advanced head retains the spool and hard-fails publication as
split brain; it is never rebased or overwritten. This is crash fencing and CAS
equivocation protection, not a distributed lease, and does not make concurrent
offline multi-host ownership safe.

The uploader scans pending disk records at startup and periodically, retries
with bounded backoff, and accepts only nonblocking/coalesced wake notifications.
Dead/full notification channels cannot impede local admission. Cloud errors set
a loud `remote_lag` state while local capture continues. Initial, periodic,
max-changes, idle/max-interval, downtime, and dirty-window snapshots all enter
this same native spool before upload.

### Capacity, restore, pruning, and shutdown invariants

Capacity accounts for the live WAL, fsynced shadow, HADBP encode temporary,
installed payload, journal/intents, and a filesystem free-space reserve (on the
actual custom spool filesystem). No full SQLite stable-copy or rollback-journal
transient is part of native watcher snapshots. A warning watermark emits
`local_spool_high`; hard capacity/reserve emits `local_spool_full`, retains the
blocker, stops checkpointing, and keeps watch alive. `remote_lag` is separate.
Pending objects and the only local snapshot base are never capacity victims.

Local restore may traverse a complete journal-verified native chain without S3.
Remote restore exposes only descriptor-selected contiguous publish records.
Pruning cannot remove a legacy/native base referenced by unpublished local
descendants. Graceful shutdown never deletes pending work.

Native-v1 retention uses immutable, versioned snapshot-floor records:

```
{prefix}{db}/native/v1/retention/v1/{snapshot_seq:016x}.json
```

A floor record binds the stream digest, lineage, snapshot sequence, exact
snapshot publish-record digest, and that snapshot record's predecessor digest.
Readers select the highest canonical floor, verify its publish record and exact
HADBP snapshot payload, then traverse the normal contiguous publish chain from
that snapshot. Publishing a floor is create-if-absent and exact-idempotent.
Only after the new floor reproduces the prior visible head may prune delete
older publish records and their payloads. A crash before the floor leaves the
old chain authoritative; a crash after the floor leaves either extra old bytes
or a complete new recovery base. A missing object at or above the selected
floor remains corruption. A PIT below the floor is reported as intentionally
expired, not as a chain gap.

For CLI native-v1, a full native snapshot is the compaction output. The
`compact` compatibility command already routes to retention pruning, so native
compaction means: publish a normal durable spool snapshot, advance a verified
retention floor according to policy, then delete history below it. Native-v1
does not write `levels/L*`, merge immutable publish records, reuse legacy LTX
identities, or mutate a delta in place. The `[compaction]` leveled-engine knob
remains rejected in default shadow mode because it controls a different layout;
the normal snapshot triggers plus native retention implement this stream's
compaction model without changing Phase 2 semantics.

### Mandatory proof and PR disposition

Add call-site-revert-proof tests for local admission versus the old remote gate,
remote policy, every crash boundary, orphan adoption/divergence, paused/dead
uploader, offline restart/reconnect conflict, repeated snapshots, PASSIVE busy,
capacity/custom paths, native latest/PITR, legacy migration latest/old PITR, and
exact restore/integrity. Preserve DF1/DF2, app checkpoint, WAL backpressure,
two-writer/fencing, racing checkpoint, SIGKILL, strict WAL header, native chain,
S3 gating, and frozen 0.7 fixtures unchanged. Measure local stage/fsync,
checkpoint time, upload time, remote lag, WAL bytes, spool bytes, and free space;
injected PUT delay may increase lag but not local checkpoint latency.

Amend PR #43 with atomic commits. Do not change Phase 2 independent semantics,
PR #42, or PRs #40/#41; do not merge. After implementation, use a fresh
independent reviewer/fixer on this worktree, run replacement CI and required
unique-prefix live-Tigris gates, clean only those prefixes, and stop for the user
to merge.

### PR #43 adversarial remediation gate — complete 2026-07-14

Two independent adversarial passes are closed. The second pass specifically
proved and fixed seven gaps left by the first completion claim:

- every configured database is checkpoint-pinned before S3 client creation or
  discovery, then unconditionally rearmed as the final startup SQLite action;
- SIGTERM at local admission failure retries with the blocker held, and only a
  deliberate SIGKILL forces an incomplete local shutdown;
- an atomic fsynced shadow durable-tail marker, not frame alignment, defines the
  restart-safe prefix;
- snapshot preflight uses the exact fsynced shadow commit boundary and reserves
  HADBP temporary/installed, journal, intent, source, and filesystem peaks;
- ordinary object admission reserves install-intent and complete journal
  rewrite peaks as well as payload bytes;
- an advisory spool owner lock makes active local restore fail loudly and keeps
  mutating recovery out of watcher write windows; offline restore remains exact;
- v2 local paths length-prefix every identity component, discover matching v1
  spools, reject duplicate v1/v2 ownership, and route a colliding foreign v1
  tuple to its distinct v2 path.

The live user-path gates cover startup discovery delay plus app TRUNCATE,
shutdown at hard capacity and restart, WAL-grown snapshots on a custom spool,
and active-refusal/offline local restore. The shadow SIGKILL matrix covers both
sides of its fsync marker. Narrow tests construct an aligned pre-fsync crash
image and adversarial identity segmentation collision. Each new call site was
neutered and failed before restoration. See `CHANGELOG.md` and the atomic PR
history for the proof ledger. The user retains merge authority.

The final independent follow-up also closes upgrade and live-error boundaries:
markerless shadow directories are never adopted as durable merely because they
are aligned, but are discarded and rotated into a full-snapshot cursor domain;
failed live appends restore both the fsynced marker/file boundary and the WAL
checksum/generation cursor before retry. Native snapshots now use the
Litestream-shaped page-selection proof directly: latest page from the exact
fsynced shadow commit prefix, otherwise the main DB, encoded immediately as
HADBP. This removes the SQLite Backup/VACUUM handoff, its full stable-copy and
rollback-journal transients, and the possibility of binding a later SQLite
snapshot to an older shadow cursor. Live gates pin the frozen-cursor exclusion,
markerless snapshot→new-generation-delta path, and blocked application
TRUNCATE with exact restore.

The closing scheduled-retention audit found one more call-site gap: both shadow
watch retention timers still invoked a legacy-only helper even though the
interactive prune command already enforced native migration and floor rules.
Both timers now call the same native-aware implementation. A valid native
descriptor with no contiguous published snapshot base preserves every legacy
object (disabling that guard deletes an asserted recovery object), while a live
Tigris watcher producing repeated native snapshots automatically advances a
verified native floor and still restores latest row-exact with
`integrity_check = ok` (disabling the watcher call site leaves the floor at 1
and fails). The watcher also checks its durable local migration journal before
either remote prune call: while the first native snapshot is pending and even
`stream.json` is absent, it preserves the legacy base. The gate opens only once
the contiguous remote cursor covers native publication and a retained published
snapshot exists; it remains open after safe local cleanup removes the original
first snapshot. Neutering this pre-descriptor guard deletes the asserted legacy
object. Replacement-CI hardening also made the legacy cursor fixtures write
fsynced generation files plus their durable-tail marker, and made the snapshot
handoff proof wait boundedly for the durable-delta observation after exact
remote restore instead of racing log visibility.

The direct-snapshot pivot has its own call-site proof ledger. Disabling the
shadow-page-over-main selection made the frozen-boundary restore test fail on a
page that exists only in WAL. Disabling exact prewritten-temp installation made
the inode/byte-identity admission test reject a divergent rewrite. Skipping the
durable snapshot-source intent made the default-local checkpoint test fail
before release. Deleting a complete fsynced temp instead of adopting it made the
pre-install-intent crash test fail; restoring production adoption returned it
green. The first live frozen-cursor run also caught Tokio's immediate first
snapshot-timer tick producing a second snapshot where the next recovery object
had to be a delta; consuming that initial tick fixes the production call site
without weakening the assertion. The live markerless-upgrade gate then exposed
that discarded shadow bytes still left their old `wal_copy_offset` in progress;
direct page selection correctly failed when a WAL-only page was absent from the
shorter main DB. Markerless recovery now ignores that stale offset and
checksum-recopies the pinned live WAL from zero before its native snapshot
re-anchor, with a focused unit regression and the unchanged live restore gate.

The macOS DF2 rerun exposed a further Litestream-shaped lock invariant: direct
snapshot encoding reopened and closed the main database after blocker
acquisition. On non-OFD POSIX locks that close invalidated SQLite's process
locks even though the blocker transaction still appeared live. Watch startup
now opens one source descriptor before any blocker and native snapshots reuse
it until shutdown closes blocker, monitor, then descriptor. Neutering only that
production call site made unchanged live DF2 fail immediately with
`first_checkpoint=(1,5,4)` and a zero-byte WAL after every short-lived writer;
restoring descriptor reuse returned DF2, DF1, app-TRUNCATE, paused-uploader,
remote-release, partial-PASSIVE, and legacy-migration live gates to green.

The final fresh review closed the remaining controlled-handoff gap. The old
path sampled `data_version`, closed and replaced that monitor, then let the new
blocker connection commit its own heartbeat. An application commit plus
TRUNCATE in that interval could disappear while the replacement heartbeat made
the later state look clean. The retained pre-blocker monitor now commits the
heartbeat itself, so its own `data_version` remains unchanged; a pin-only
replacement blocker then acquires the read mark, and any application commit
across the full handoff forces the journaled snapshot re-anchor. A deterministic
test commits and TRUNCATEs after the final sample and fails when the post-rearm
comparison is disabled. The same pass made local PIT restore fall through to
remote history below a cleaned local snapshot base and removed failed
legacy-migration verification scratch files durably.

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
