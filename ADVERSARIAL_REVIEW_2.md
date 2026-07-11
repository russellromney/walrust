# Adversarial Review 2 — walrust

Second adversarial pass (2026-07-06), following ADVERSARIAL_REVIEW.md (F1–F15).
Six parallel deep reviews over: core sync engine, WAL/LTX format layer, sidecar
watch pipeline, restore/verify/compact/CLI, test adequacy + DST, and SQLite
live-DB checkpoint safety. Every CRITICAL finding below was re-verified by hand
against source before inclusion; A1 was verified empirically against a WAL
written by real SQLite.

Verdict: walrust does not yet achieve "no data loss" or "always-restorable
consistent remote state." There are multiple independent, confirmed paths to
silent data loss or silent restore corruption. Several F1–F15 fixes landed in
only one of the two parallel trees; F2 is dead code due to an endianness
inversion; F9/F10 works only in its own unit tests.

Line numbers are approximate against commit e556bd5 — re-locate before editing.

---

## CRITICAL

### A1 — WAL checksum endianness inverted; frame validation never runs on real WALs
Status: Fixed — proven by
`wal::tests::test_real_sqlite_wal_checked_reader_validates_checksum_chain`
in both `src/` and `crates/walrust-core/`.

Verify (Wave 1, 2026-07-07): VERIFIED. Fix present in
`crates/walrust-core/src/wal.rs:124` (`magic_is_big_endian` = `magic & 1 == 1`)
with constants corrected (`WAL_MAGIC_LE=0x377F_0682`, `WAL_MAGIC_BE=0x377F_0683`).
Reverting the predicate to `magic & 1 == 0` makes the named test FAIL
(`chain.is_some()` unwrap panics because the checked reader picks BE on a real
LE WAL). The test drives the production reader `read_frames_as_page_map_checked`
against `build_real_sqlite_wal()` (a live rusqlite WAL, `journal_mode=WAL`).
"Both trees": Phase-4 converged the WAL layer into `walrust-core`; `src/wal.rs`
is now a 6-line shim (`pub use walrust_core::wal::*;`), so there is one
implementation and the second tree cannot rot. No adjacent uncovered path found.

- `crates/walrust-core/src/wal.rs:91-94`, `src/wal.rs:76-78`
- `magic_is_big_endian` returns true for `0x377f0682`. SQLite writes
  `WAL_MAGIC | SQLITE_BIGENDIAN`: `0x377f0682` = little-endian checksums (all
  common hardware), `0x377f0683` = big-endian. Verified empirically: a real
  SQLite WAL's header checksum matches the LE computation only; walrust picks
  BE, `validate_header_checksum` returns `None`, and the checked reader maps
  `None` to `validate=false`. All torn-tail/stale-frame protection (F2) is
  inert in production. Tests pass because `build_valid_wal` fixtures use the
  same inverted convention (`WAL_MAGIC_BE = 0x377f0682` + BE checksums — a WAL
  no real SQLite produces).
- Fix: flip the predicate (`magic & 1 == 1` => big-endian) in BOTH crates;
  rename `WAL_MAGIC_BE`/`WAL_MAGIC_LE` to match reality; add a golden test
  that opens a WAL produced by real SQLite (rusqlite) and asserts header +
  frame chain validation passes.

### A2 — No frame-salt check; zeroed header checksum silently disables validation
Status: Fixed — proven by
`wal::tests::test_checked_reader_rejects_frame_salt_mismatch` and
`wal::tests::test_checked_reader_rejects_zero_header_checksum` in both `src/`
and `crates/walrust-core/`.

Verify (Wave 1, 2026-07-07): VERIFIED. Frame-salt check present at
`crates/walrust-core/src/wal.rs:481` (`if frame_salt != header_salt { break }`);
disabling it (`if false && ...`) makes `test_checked_reader_rejects_frame_salt_mismatch`
FAIL. Zero/invalid header checksum is a hard, typed error at
`wal.rs:152-154` (`validate_header_checksum` returns
`Err(InvalidHeaderChecksum)`, propagated via `?` at the reader's header parse);
reverting it to `Ok((0,0))` (the old carve-out) makes
`test_checked_reader_rejects_zero_header_checksum` FAIL. The validate path these
crafted-WAL tests exercise is the same one A1 proved active on a real SQLite
WAL; the crafted fixtures use real (not zero) checksums per the fix. Single
converged tree (see A1). No adjacent uncovered path found.

- `crates/walrust-core/src/wal.rs:424-453` (frame salt bytes 8..16 never
  compared to header salt), `wal.rs:106-130, 398-418` (`None` from
  `validate_header_checksum` => validate=false, silently)
- After an in-place WAL reset (checkpoint RESTART), stale prior-generation
  frames physically remain past the new write head. With validation dead (A1)
  and no salt check, the reader consumes `available / frame_size` frames over
  the whole file and takes a stale old-generation commit frame as the commit
  boundary => delta mixes generations => silently corrupt restore. Separately,
  corruption/attacker zeroing header bytes 24..32 is a validation kill switch;
  SQLite treats a bad-header-checksum WAL as empty, walrust ships all of it.
- Fix: reject frames whose salt != header salt; make unverifiable header
  checksum a hard, typed error (remove the synthetic-WAL carve-out; fix the
  tests to build real-checksum WALs instead).

### A3 — Checkpoint races are structural data loss in every mode
- Library: no blocker at all; safety is a doc comment
  (`crates/walrust-core/src/replicator.rs:92` "Set PRAGMA wal_autocheckpoint=0")
  — per-connection, unenforced, useless against other processes. On rollover,
  `read_next_wal_batch` (`sync.rs:350-361`) resets offset/generation and
  continues; frames checkpointed between polls are never shipped, never
  healed, and the shipped-pages chain verifies green over the gap.
- Shadow mode (CLI default): the "checkpoint blocker" is `BEGIN DEFERRED` +
  `SELECT COUNT(*) FROM sqlite_master` (`src/shadow.rs:117-131`). On a fully
  backfilled WAL this takes read-mark 0, which does NOT block walRestartLog —
  and walrust re-opens the blocker right after its own PASSIVE checkpoint,
  exactly when it degrades to a no-op. Litestream pins a live WAL frame
  (its `_litestream_seq` write) for this reason.
- Poll mode: no blocker; syncs only when WAL GROWS
  (`src/sync/watch_independent.rs:449-455`) so an in-place reset at equal size
  never triggers a sync; and this mode has NO snapshot timer, so a rollover
  gap is permanent by construction.
- No mode treats a detected rollover as a data-loss event requiring a base
  re-snapshot.
- Fix (pick one, enforce in code): (a) Litestream-grade blocker — keep a read
  transaction pinned on a live WAL frame (e.g. write to a `_walrust_seq` table
  from the blocker connection before BEGIN so read-mark 0 cannot happen),
  copy frames to shadow BEFORE any walrust-initiated checkpoint, gate
  checkpoints on "all frames copied AND uploaded"; or (b) treat every salt/
  size rollover as a mandatory immediate re-snapshot of the main DB (the DB
  file contains the checkpointed data, so re-snapshot restores consistency).
  Either way, emit a loud event (error log + webhook) on rollover.

Status: Fixed — walrust-owned rollover now re-anchors with a fresh production
snapshot, external-base and fenced external modes hard-fail until the external
base is re-anchored, and poll mode no longer waits for WAL growth before
running the production sync path. Root direct mode mirrors the re-snapshot
behavior and the watch path emits a rollover webhook event via
`checkpoint_detected`. Proven by
`test_walrust_owned_flush_resnapshots_after_checkpoint_rollover`,
`test_external_mode_refuses_checkpoint_rollover_until_reanchored`, and
`test_fenced_external_mode_refuses_checkpoint_rollover_until_reanchored`.
Second-pass gate review found the shadow blocker still only read
`sqlite_master`, and CLI startup snapshots could leave copied pre-snapshot
shadow bytes and active-WAL offsets out of sync. Core `ShadowWal` now writes
and pins a real `_walrust_seq` WAL frame, exposes the shadow segment offset
used by root watch state, root startup snapshots advance the shadow sync
cursor past already-covered shadow bytes, and the WAL reader restarts at the
header if SQLite reuses/truncates a WAL below the saved offset. Proven by the
Soup-backed production-path
`e2e_cli_watch_restore_round_trips_sqlite_rows` and the full
`production_e2e` test binary.

Verify (Wave 1, 2026-07-07): VERIFIED (with scope note). All three named
unit tests pass and FAIL on revert: disabling the WalrustOwned re-anchor
(`take_snapshot` on rollover, `crates/walrust-core/src/sync.rs:985`) breaks
`test_walrust_owned_flush_resnapshots_after_checkpoint_rollover`; turning the
external/fenced `anyhow::bail!` into a warn (`sync.rs:994`, `sync.rs:1481`)
breaks `test_external_mode_refuses_checkpoint_rollover_until_reanchored` and
`test_fenced_external_mode_refuses_checkpoint_rollover_until_reanchored`. These
drive the real core `Replicator` (`add`/`flush`) against a live rusqlite WAL and
force a real rollover via `PRAGMA wal_checkpoint(TRUNCATE)`. The `_walrust_seq`
pinned-frame blocker exists in `crates/walrust-core/src/shadow.rs:138-151`;
poll-mode's WAL-growth gate is gone from `src/sync/watch_independent.rs`.
Scope note: the unit tests force the rollover synchronously between flushes and
assert re-anchor/refusal (a new snapshot key appears / a hard error is
returned); they do NOT run a concurrent external autocheckpointer racing an
in-flight flush, nor do they assert end-to-end restore row-equality. That
end-to-end/no-loss proof is `e2e_cli_watch_restore_round_trips_sqlite_rows` +
`production_e2e`, which are credential-gated (skip without S3; run in CI/Soup).

Audit (Phase 0+1, 2026-07-08): the credential-gated `production_e2e` cases
stabilize the round-trip by holding a `pin_read_transaction` reader on the
watched DB, which pins the WAL so the co-resident `wal_autocheckpoint=1`
connection cannot RESTART/TRUNCATE mid-stream. The autocheckpointer is present
but its destructive rollover is suppressed, so no E2E case exercises a
checkpoint race that actually drops frames. DEFERRED to Phase 2A (session 4):
add a racing variant with the reader pin removed and decide whether the current
re-anchor/refuse behavior holds end-to-end, per the session-4 "un-ignore the
harness's known-failing cases as you fix them" model (there is currently no
such ignored case to un-ignore — this note is the placeholder for it).

Phase 2A (2026-07-08): DEFERRED racing scope CLOSED. Added two racing E2E
variants with NO pinned reader, both S3-gated and passing against live Tigris:
`e2e_core_replicator_racing_checkpoint_reanchors_without_data_loss` lets an
external `wal_autocheckpoint=1` connection issue explicit
`PRAGMA wal_checkpoint(TRUNCATE)` that actually resets the WAL between writes
(asserting `busy==0`, i.e. the reset really happened), then asserts the
walrust-owned engine re-anchors and restore yields full row-equality +
`integrity_check == ok`. `e2e_cli_watch_racing_checkpoint_no_data_loss` races
explicit TRUNCATEs against the live shadow watch sync with no pin; the shadow
blocker pins a live `_walrust_seq` frame so the external checkpoint cannot
destroy unshipped frames, and restore round-trips every committed row (or the
watcher fails loudly — the test surfaces early child exit as an error, never
silent loss). Remaining A3/A4 residuals also closed this phase: the one-shot
`walrust snapshot` command (`sync::compact::snapshot`) now folds the WAL with a
completeness-checked `checkpoint_wal_truncate` before encoding (the shared
watch-path snapshot keeps its intentional PASSIVE — see B10), and the
direct/independent rollover event is now an error-level log alongside the
existing `upload_failed` webhook. Verified: blocker pins a live frame pre-BEGIN
(`crates/walrust-core/src/shadow.rs` `open_checkpoint_blocker`), checkpoints
gate on copied+encoded+upload-confirmed (`checkpoint_shadow_after_durable_sync`
-> `wait_for_cache_checkpoint_durability`), and `wal_checkpoint` result rows are
checked with `busy_timeout` in every live checkpoint site. Note: no dedicated
`CheckpointDetected` webhook variant was added — the `hadb-io` webhook enum is a
pinned external dependency (Phase-0 decision), so the rollover event rides the
`upload_failed` channel with a distinct message; that is the only residual.

Adversarial review (Phase 2A, 2026-07-08): the two racing E2E cases as first
written were GREEN FOR THE WRONG REASON and were rebuilt so the safety code is
actually load-bearing (each now fails, for the right reason, when its protection
is reverted):
- `e2e_core_replicator_racing_checkpoint_reanchors_without_data_loss` originally
  used a single-page DB and never actually triggered rollover DETECTION (after
  each external TRUNCATE the WAL was empty, so `read_header` returned `None` and
  the flush returned early); the re-anchor branch never fired and the data
  survived only because a single leaf page carries the whole table, so the final
  incremental re-imaged everything. Rewritten to a deterministic, MULTI-PAGE
  scenario: walrust reads batch A (recording the salt so the next reset is
  detected), batch A2 is written on fresh pages but NOT read, an external
  `wal_checkpoint(TRUNCATE)` (asserted `busy==0`, `ckpt>=log`) folds A+A2 and
  resets the WAL, then a tiny tail batch B opens a new generation. The next flush
  MUST re-anchor with a full snapshot to recover A2's pages. Revert-verified:
  disabling the WalrustOwned re-anchor makes the restored DB fail
  `integrity_check` (A2's pages missing).
- `e2e_cli_watch_racing_checkpoint_no_data_loss` originally slept a fixed 2s and
  ignored the TRUNCATE result. In practice the shadow blocker was NOT yet attached
  at 2s (S3 discovery + initial snapshot are slower), so every racing TRUNCATE
  succeeded (`busy==[0,0,0]`) and the "race" raced nothing; data survived only via
  the single-page full-image path. Now it polls for blocker readiness
  (`_walrust_seq` present), uses wide multi-page rows, and ASSERTS the pin engaged
  (at least one racing TRUNCATE refused, `busy!=0`). Revert-verified: removing the
  blocker's held read transaction makes every TRUNCATE succeed (`busy==[0,0,0]`)
  and the test fails.

Loud-event coverage (plan 2.1 "rollover in every mode emits a loud event"): the
direct/independent root path emits `tracing::error!` + `notify_upload_failed`
(this PR). The core walrust-owned re-anchor sites (`sync.rs` sync_wal_with_sequence
and the `_with_retry` variant) were bumped from `warn!` to `error!` this review —
an external checkpoint of a walrust-OWNED WAL is unexpected (we set
autocheckpoint=0) so it does not spam in normal operation. Residual: the core
library has NO webhook channel, so the webhook half is emitted only on the
binary's paths; and shadow mode treats a salt change as a routine generation roll
(walrust's OWN checkpoints legitimately change the salt, and the blocker is meant
to prevent EXTERNAL rollover), so a blocker-failure external rollover in shadow
mode is not distinctly alerted — recorded as a Phase-2B/observability residual.

Residual (core walrust-owned first-checkpoint window): rollover DETECTION needs a
previously-recorded salt, which is `None` immediately after a snapshot (the WAL is
empty). An external checkpoint that folds un-read frames in that brief window,
before walrust's first incremental read, is not detected as a rollover and its
un-re-imaged pages can be lost. Steady-state operation (walrust reads the WAL every
sync interval) is covered; this is the same class as the B4 restart-window
residual and is recorded for Phase 2B state-durability.

Phase 2B (2026-07-08): ATTEMPTED, REVERTED — remains a documented residual. Two
detection signals were tried and both rejected:
- SQLite's offset-24 file change counter: unreliable in WAL mode — it only
  advances when the DB *grows*, so a same-size external fold is invisible.
- Raw DB-file hash baselined at snapshot, compared in the first-read window
  (`wal_salt == None`): this correctly detects that the main file changed, but it
  cannot distinguish a benign external PASSIVE checkpoint (which folds frames into
  the file yet LEAVES them readable in the WAL — no loss) from a destructive
  TRUNCATE/RESTART (frames gone from the WAL — loss). It therefore false-positives
  on PASSIVE folds and fired a spurious re-anchor whose `checkpoint_wal(TRUNCATE)`
  is blocked by the pinned-reader S3 E2Es (`e2e_core_replicator_*`,
  `busy=1`). Since the goal is not to weaken those honest E2Es and the
  PASSIVE-vs-reset distinction is not cheaply available in the first-read window
  (there is no recorded salt for the folded-away generation), the change was
  reverted. The residual stands: walrust-owned mode sets `autocheckpoint=0` so an
  external checkpoint in this window is a shouldn't-happen edge, and the periodic
  snapshot timer bounds the exposure (the next snapshot re-images the DB file,
  which contains any externally-folded pages). A correct fix needs a cheap
  WAL-reset signal in the window (e.g. recording the first post-snapshot WAL salt
  as SQLite writes it, without a full read) and is left for a later phase.

Phase 2B rollover-webhook plumbing (2026-07-08): CLOSED (the "core library has NO
webhook channel" residual). Added `RolloverObserver` — a cloneable, `Debug`,
default-noop event sink on `SyncState` and `ReplicationConfig`. The core emits a
`RolloverEvent` on walrust-owned re-anchor (recovered=true), external-base refusal
(recovered=false), and the B11 crash-window re-anchor; the `Replicator` and
`run_replication`/`run_wal_replication` install the config's observer onto state.
The root binary drives its own webhook-wired watch loops, so this channel is for
library embedders (haqlite, etc.). Proven by
`test_rollover_observer_fires_on_walrust_owned_reanchor` (revert-verified).

Adversarial review (Phase 2B, 2026-07-08): the observer coverage was PARTIAL — it
fired on the walrust-owned re-anchor, external-base refusal, and B11 re-anchor, but
NOT on the fenced external-mode rollover refusal (`sync_wal_fenced_delta`), which is
a fourth rollover site. Fixed: that refusal now emits a `recovered=false`
`mode="fenced-external"` event before its bail, matching the external-base refusal.
Proven by the new `test_rollover_observer_fires_on_fenced_external_refusal`
(fenced flush after a forced TRUNCATE emits the event). All four core rollover
sites now emit.

### A4 — walrust's own checkpoint timer destroys unshipped frames
- `src/sync/watch_shadow.rs:663-681`: comment says "First, ensure all shadow
  data is uploaded" — no such code exists. `ShadowWal::checkpoint()`
  (`src/shadow.rs:360-384`) drops the blocker, runs
  `PRAGMA wal_checkpoint(PASSIVE)`, re-opens the blocker; no prior
  `copy_frames`, no upload gating. Frames written since the last sync tick are
  backfilled and the WAL reset before they ever reach the shadow.
- Also: blocker re-open failure leaves `checkpoint_blocker = None` forever
  (silent; one log line). No `busy_timeout` anywhere in the repo.
- Fix: `copy_frames` + full shadow encode/upload drain BEFORE checkpoint;
  check the wal_checkpoint result row (busy/log/ckpt counts); retry + webhook
  on blocker re-open failure; set busy_timeout.

Status: Fixed — shadow checkpointing now copies active WAL frames, syncs the
shadow segments, waits for local-cache upload confirmation before checkpointing,
returns a hard error/webhook on failure, and `ShadowWal::checkpoint()` in both
trees checks the `wal_checkpoint(PASSIVE)` result row with a busy timeout while
re-opening the blocker before returning. Proven by
`sync::watch_shadow::tests::test_shadow_checkpoint_copies_syncs_and_waits_for_cache_upload`
and
`sync::watch_shadow::tests::test_shadow_checkpoint_refuses_pending_cache_upload`.

Verify (Wave 1, 2026-07-07): VERIFIED (with scope note). The orchestration
`checkpoint_shadow_after_durable_sync` (`src/sync/watch_shadow.rs:116`) does
copy_frames -> shadow sync -> `wait_for_cache_checkpoint_durability` ->
`ShadowWal::checkpoint()`. Disabling the copy-offset advance breaks
`test_shadow_checkpoint_copies_syncs_and_waits_for_cache_upload`; removing the
durability wait breaks `test_shadow_checkpoint_refuses_pending_cache_upload`.
The tests drive the real path against a live rusqlite WAL + real `LocalCache`.
"Both trees": shadow is Phase-4 converged — `src/shadow.rs` is a shim; the
`ShadowWal::checkpoint()` busy_timeout + `wal_checkpoint(PASSIVE)` result-row
check live in `crates/walrust-core/src/shadow.rs:404-416`. The copy/sync/wait
orchestration is root-only (the watch loop). Scope note: the unit test calls
`checkpoint_shadow_after_durable_sync` directly rather than exercising the live
watch checkpoint timer racing an external autocheckpointer; that end-to-end
path is the credential-gated `e2e_cli_watch_*` in `production_e2e`. DEFERRED to
Phase 2A: the `e2e_cli_watch_*` cases pin a reader (see the A3 audit note), so
the live checkpoint timer never actually destroys unshipped frames under
test — the racing case is Phase 2A's to add and un-ignore.

Phase 2A (2026-07-08): CLOSED. `e2e_cli_watch_racing_checkpoint_no_data_loss`
now exercises the live shadow checkpoint path with an external
`wal_autocheckpoint=1` connection issuing explicit `PRAGMA wal_checkpoint(TRUNCATE)`
racing the in-flight watch sync WITHOUT a pinned reader; the shadow blocker's
pinned live `_walrust_seq` frame prevents the racing checkpoint from destroying
unshipped frames and restore round-trips every committed row. The core stack's
equivalent is `e2e_core_replicator_racing_checkpoint_reanchors_without_data_loss`
(re-anchor on real WAL reset). Both S3-gated, passing on live Tigris.

### A5 — Shadow generation rollover drops data: sync offset never reset, un-uploaded segments deleted
- `src/sync/watch_shadow.rs:438` is the only assignment of
  `shadow_sync_offset`; `ShadowWal::copy_frames` bumps generation and restarts
  segments at byte 0 (`src/shadow.rs:184-195`) but the encoder cursor carries
  the OLD generation's byte offset forward. Encoder filters to current gen
  (`src/sync/shadow.rs:56-58`) and skips segments below the stale offset
  (`:67-70`) => first `old_offset` bytes of every new generation are never
  uploaded; old generation's unencoded tail is never read again.
- `cleanup_segments(current_gen)` (`watch_shadow.rs:676-681`) deletes all
  older-generation segments with no uploaded-check.
- Fix: make generation part of the sync cursor (reset offset to 0 on gen
  change; encode remaining old-gen segments before switching); refuse to
  delete segments not fully encoded+uploaded.

Status: Fixed — root shadow watch state now tracks
`shadow_sync_generation` separately from `shadow_sync_offset`, drains the old
generation before switching, resets offset to 0 on generation advance, and
cleans up only generations below the synced cursor. `walrust-core` has the
segment primitive but no shadow watch/uploader cursor, so this drift is root
only. Proven by
`sync::watch_shadow::tests::test_shadow_sync_cursor_resets_offset_when_advancing_generation`.

Verify (Phase 2A, 2026-07-08): VERIFIED already-fixed in the converged tree.
The cursor is generation-aware in `walrust_core::legacy_shadow_watch`:
`advance_shadow_sync_cursor_if_drained` only rolls to `gen+1` once
`shadow_sync_offset >= generation_size` (old generation fully drained) and then
resets the offset to 0; the encoder (`legacy_shadow::encode_shadow_to_ltx`)
filters to `input.generation` and reads the new generation from byte 0.
`ShadowWal::cleanup_segments` is only called from
`checkpoint_shadow_after_durable_sync` with `state.shadow_sync_generation` (the
synced cursor) AFTER `wait_for_cache_checkpoint_durability` confirms uploads are
durable, so no un-uploaded segment is deleted. The named proving test still
passes; the mid-stream rollover + restore + row-equality regression is now also
covered end-to-end by the racing E2E cases added for A3/A4. Residual: the
drain condition relies on the invariant that a generation ends at a commit
boundary (uncommitted trailing frames would stall the cursor, which is the
intended fail-safe, not enforced).

### A6 — Restore chain verification is self-referential; gaps/wrong-lineage apply cleanly as success
Status: Fixed — root restore is proven by
`restore_rejects_incremental_without_prior_chain_link`; core restore is proven
by `sync::tests::restore_errors_on_noncontiguous_incremental_sequence`. The
production restart E2E now asserts fail-closed behavior with
`e2e_core_replicator_restart_rejects_divergent_chain` until A10's state reload
fix removes that divergent lineage.

Verify (Wave 1b, 2026-07-07): VERIFIED already-fixed. Non-gated core proof
`sync::tests::restore_errors_on_noncontiguous_incremental_sequence` passes and
FAILS on revert: neutering the contiguity gate in `restore` at
`crates/walrust-core/src/sync.rs:1710` (`if false && inc.seq != expected_seq`)
makes it FAIL. That path also threads the running DB checksum through
`apply_changeset_to_db(.., current_checksum)` and runs
`verify_sqlite_integrity` before publish. The root proof
`restore_rejects_incremental_without_prior_chain_link` and the S3-gated e2e
could not be run locally: the sandbox Docker/MinIO clock is skewed vs the AWS
request signer (`RequestTimeTooSkewed`) and an external MinIO binary download
was policy-denied, so all ~22 S3-gated tests skip locally (they run in CI/Soup).
Scope note: `e2e_core_replicator_restart_rejects_divergent_chain` no longer
exists under that name — the divergent-chain e2e assertion was folded into the
Phase-4 convergence; the surviving named restart e2e is
`e2e_core_replicator_restart_reopens_state_and_restores_cleanly` (S3-gated).

- `src/ltx.rs:177-250` `apply_ltx_to_db`: chain hasher is seeded from the LTX
  file's OWN `pre_apply_checksum`, hashes the file's OWN pages, compares to
  the file's OWN trailer. `pre_apply_checksum` is logged, never compared to
  the target DB's actual state. Any internally-valid LTX applies to any base.
- `src/sync/restore.rs:153-155`: incrementals filter is range-only; no
  contiguity check (`min == final_txid + 1`) in the apply loop; a durable
  mid-chain gap (see A11) yields a Frankenstein DB reported as success. No
  `PRAGMA integrity_check` on the result.
- Core is worse: `crates/walrust-core/src/sync.rs:1015-1027` (restore),
  `:1098-1107`, `:1357-1367` — any chain break (including a missing object)
  is classified "stale lineage", loop breaks, returns Ok. Short restore is
  indistinguishable from complete restore. F1's ensure! exists only in the
  binary. `_point_in_time` is silently ignored in core (`sync.rs:959`).
- Fix: track the running DB checksum across restore and verify each file's
  `pre_apply_checksum` against it; enforce contiguity; hard error on any
  break/gap/short restore; run integrity_check before reporting success.

### A7 — Snapshots don't fold the WAL (core) and are torn-prone raw copies (both)
- `crates/walrust-core/src/sync.rs:904-948` + `:1145-1196`: `take_snapshot`
  encodes the raw main DB file; NO checkpoint call exists anywhere in
  walrust-core. Library mode requires `wal_autocheckpoint=0`, so all recent
  commits live only in the WAL => every periodic snapshot is a stale image;
  restore = latest snapshot + seq > snapshot.seq => everything shipped before
  the snapshot is lost, chain verifies green (F11 fixed in binary only).
- TOCTOU: snapshot bytes (`:925`) and `db_checksum` (`:932`) are two separate
  reads of a live file; a write between them poisons the chain hand-off (and
  per A6 core semantics, later restores silently truncate post-snapshot
  history). Same pattern in `src/sync/wal_sync.rs:725-756`.
- Both crates: `encode_snapshot` (`src/ltx.rs:16-62`,
  `crates/walrust-core/src/ltx.rs:32-61`) streams the raw file with no read
  lock / backup API / VACUUM INTO; a concurrent checkpoint mid-copy tears the
  base image undetectably.
- Fix: in core, checkpoint (or overlay WAL pages) before snapshot + reset
  wal cursor/salt/chain like the binary's F11 fix; take the snapshot under a
  read transaction (or backup API / VACUUM INTO); compute db_checksum from
  the bytes actually encoded, in one pass.

Status: Fixed — core snapshot paths now checkpoint/reset the WAL cursor and
encode from a stable SQLite `VACUUM INTO` copy; root snapshot paths use the
same stable-copy helper. Both trees return the checksum from the bytes encoded
into the snapshot instead of re-reading the live DB after upload. The raw
`encode_snapshot` helpers remain for already-stable byte fixtures, while
production callers use `encode_sqlite_snapshot*`. Proven by
`sync::tests::take_snapshot_state_checksum_matches_uploaded_snapshot_bytes` in
`walrust-core`, and
`ltx::tests::test_encode_sqlite_snapshot_includes_wal_and_returns_encoded_checksum`
in both trees.

Verify (Phase 2B, 2026-07-08): VERIFIED already-fixed by convergence. Both named
proofs pass. Revert-verified: replacing `state.db_checksum = Some(db_checksum)`
(the checksum of the encoded snapshot bytes) with a post-upload
`compute_checksum_from_file` re-read makes
`take_snapshot_state_checksum_matches_uploaded_snapshot_bytes` FAIL (the mock
mutates the source DB on PUT, so a second read no longer matches the uploaded
bytes) — confirming the single-pass checksum-of-encoded-bytes is load-bearing.
`take_snapshot`/`take_snapshot_with_retry` fold the WAL with a
completeness-checked `checkpoint_wal` (TRUNCATE) and `reset_wal_cursor_after_snapshot`
before encoding from the stable copy. No 2B code change needed for A7 itself
(the first-checkpoint-window residual it left is closed under A3 above).

### A8 — Cache-mode uploads are unrestorable (key layout mismatch)
- Uploader PUTs flat keys: `src/uploader.rs:100`
  `format!("{}/{:08}.ltx", prefix, txid)`. Discovery/restore parse only
  `GGGG/min-max.ltx` two-segment keys (`src/sync/manifest.rs:148-160`,
  `src/sync/restore.rs:149-155`). Snapshots go direct in litestream layout;
  every cached incremental is invisible to restore and to
  `discover_state_from_s3`. Restore succeeds, validation passes, all writes
  since the last snapshot are gone.
- Related mode divergence: cache dir differs between modes (nested
  `-walrust-walrust` in independent mode, `src/sync/watch_independent.rs:222`
  vs `watch_shadow.rs:155`); S3 prefix differs (`{prefix}{name}` vs
  `{prefix}/{name}`, `watch_shadow.rs:158` vs `watch_independent.rs:275`).
- Fix: one canonical key layout + one cache-dir convention shared by
  uploader, snapshot, discovery, restore; migration/detection for old keys.

Status: Fixed — root cache uploads now publish through the canonical
Litestream-style key builder (`db/GEN/min-max.ltx`) using LTX header range
metadata stored in the cache manifest; both watch modes pass the same base
prefix to the uploader, and independent-mode cache creation now uses an
explicit cache directory instead of nesting `-walrust` twice. Discovery,
snapshot selection, and live-generation listing also detect legacy flat
`00000003.ltx` cache objects for migration. `walrust-core` is unaffected
because it does not use the root `LocalCache`/uploader/LTX S3 key path. Proven
by `uploader::tests::test_uploader_basic_upload` and
`sync::manifest::tests::build_ltx_key_normalizes_prefix_separator`.

Verify (Phase 2B, 2026-07-08): VERIFIED already-fixed by convergence. The
canonical key builder + legacy-flat detection are now owned by
`crates/walrust-core/src/legacy_manifest.rs` (root `src/sync/manifest.rs`
delegates). Revert-verified: dropping the `prefix_with_separator` normalization
in `legacy_manifest::build_ltx_key` makes
`legacy_manifest::tests::build_ltx_key_normalizes_prefix_separator` FAIL
(`"base"` + `"db"` no longer normalizes to `base/db/...`). One layout across
uploader / snapshot / discovery / restore. No 2B code change needed for A8.

### A9 — PIT restore can only use the newest snapshot; GFS retention is dead weight
Status: Fixed — root restore now selects the latest snapshot whose `max_txid`
is `<= --point-in-time`, and core restore now does the same for HADBP
sequence numbers while filtering incrementals at the target. Timestamp PITR is
not implementable from current object metadata, so CLI/help/docs now document
TXID/sequence-based PITR instead of ISO 8601 timestamps. Proven by
`point_in_time_restore_uses_latest_snapshot_not_after_target` and
`sync::tests::restore_point_in_time_uses_latest_snapshot_not_after_target`.

Verify (Phase 2C, 2026-07-08): VERIFIED. Timestamp-PITR-vs-doc-fix decision
(2.7) re-affirmed: the doc fix stands — object keys carry only TXID/sequence
range, not commit wall-clock time, so ISO-8601 PITR is not implementable
without new commit-time metadata; every `--point-in-time` help/doc string
documents a TXID/sequence target and the parser rejects non-numeric input
(`WalrustError::restore("... Use TXID (number)")`). Selection re-revert-verified:
relaxing the core snapshot filter `snapshots.retain(|c| c.seq <= target_seq)` to
keep-all makes `sync::tests::restore_point_in_time_uses_latest_snapshot_not_after_target`
FAIL (restore picks the newest snapshot past the target instead of the latest
snapshot at-or-before it). The core-only non-gated proof passes; the root
`point_in_time_restore_uses_latest_snapshot_not_after_target` is S3-gated
(runs in CI/Soup).

Verify (Phase 2C review, 2026-07-08): swept every user-facing PIT string
(`grep -ri "8601|point.in.time|PITR|timestamp"` across src, docs, README).
CLI help (`main.rs`: "Restore to specific TXID/sequence number"), the parser
error (`restore.rs`: "Use TXID (number)"), the docs site, and README are all
correct. FOUND ONE MISSED false promise: the Python binding docstring
(`src/python.rs`) still read "point_in_time: Optional ISO 8601 timestamp" —
corrected to document a TXID/sequence number and that timestamp PITR is not
implemented. `src/sync/types.rs` "Upload timestamp (ISO 8601)" is an internal
`created_at` field description (accurate), not the restore param.

- `src/sync/restore.rs:84` hard-codes `find_latest_snapshot` (no target
  param). If `target_txid < snapshot_max_txid`, restore always fails
  ("overshot") even when an older retained snapshot + incrementals cover the
  target. `compact.rs:57-58`'s F7 guard assumes the opposite restore
  algorithm.
- `--point-in-time` documented as ISO 8601 (`src/main.rs:221-223`) but only
  parses a TXID; timestamp PITR doesn't exist anywhere.
- Fix: select latest snapshot with `max_txid <= target`; either implement
  timestamp PITR (needs commit-time metadata) or fix the docs/help.

### A10 — Replication progress state is not durable / not fenced
Status: Fixed — Phase 1.5 reload-half fixed in core, and Phase 2.5
walrust-owned object publication now uses CAS/idempotence checks for snapshots
and live WAL changesets. New walrust-owned streams now mint a `lineage_id`,
persist it in `state.json`, write HADBP objects under a lineage namespace, and
restore from that active namespace. Saved `state.json` now round-trips
`wal_salt` and `wal_checksum_chain`, and read/parse failures propagate instead
of becoming a cold start. Proven by
`test_walrust_owned_reload_restores_saved_wal_salt`,
`test_walrust_owned_reload_restores_saved_wal_checksum_chain`, and
`test_walrust_owned_reload_state_transport_error_is_hard_error`, plus
`walrust_owned_sync_rejects_divergent_existing_changeset` and
`walrust_owned_snapshot_rejects_divergent_existing_changeset`,
`test_walrust_owned_new_stream_writes_lineage_state_and_keys`, and
`test_walrust_owned_restore_uses_active_lineage_namespace`, and the external
base no-chain offset regression
`test_external_mode_registration_does_not_skip_unpublished_wal_bytes`.
The root CLI shadow watch path now writes a local fsynced `progress.json`
sidecar after durable cache/direct shadow sync and snapshot state advances,
reloads it on restart, and hard-fails if the record cannot be read or
persisted. Proven by
`test_shadow_sync_persists_restart_progress_after_durable_cache_write`.
Fresh walrust-owned lineage creation now refuses an existing active
`state.json` and publishes initial state with CAS, preventing competing
`add()` calls from replacing the active lineage. Proven by
`test_walrust_owned_add_refuses_existing_active_state`. External-base mode now
writes a local fsynced progress record after successful HADBP delta publication
and refuses to reopen a remote chain head without a matching local WAL cursor
proof. Proven by
`test_external_mode_rejects_remote_chain_without_local_progress` and
`test_external_mode_reopen_derives_head_without_remote_state`.

Phase 2B (2026-07-08): remaining Session-5 A10 sub-items closed.
- "stop seeding seq from the SQLite change counter on restart": the
  walrust-owned reopen path (`Replicator::add_without_snapshot_with_wal_path`)
  no longer seeds `current_seq` from the file change counter. `current_seq` now
  comes only from the durable `state.json` (falling back to 0, never the change
  counter), so a lost `state.json` cannot silently fork the object chain at an
  unrelated seq. `current_txid` (change-detection only) keeps its change-counter
  hint. Proven by
  `test_walrust_owned_reopen_does_not_seed_seq_from_change_counter`
  (revert-verified: restoring the change-counter seq seed makes it FAIL).
- CAS + lineage + fenced fresh-lineage creation were already landed in earlier
  waves (see the proofs above); re-confirmed passing.
- `initialize_external_base_state`'s "current WAL size was all published"
  assumption + `unwrap_or(0)`: already removed by convergence — the function now
  sets `wal_offset = 0` when the remote chain has no delta after the base and
  otherwise REQUIRES a matching local fsynced progress record (no WAL-size
  guessing), verified present at `crates/walrust-core/src/sync.rs`
  (`initialize_external_base_state`, `head.count == 0` branch + progress-anchor
  checks). No 2B code change needed for this sub-item.

Verify (Wave 1b, 2026-07-07): VERIFIED already-fixed (reload-half). All three
named non-gated proofs pass and FAIL on revert. Dropping the salt/chain reload
at `crates/walrust-core/src/replicator.rs:355-356` (`state.wal_salt = ...` /
`state.wal_checksum_chain = ...`) breaks
`test_walrust_owned_reload_restores_saved_wal_salt` and
`test_walrust_owned_reload_restores_saved_wal_checksum_chain`; reverting the
transport-error guard at `replicator.rs:328` from
`get(..).with_context(..)?` back to `if let Ok(Some(data)) = get(..)` (swallow)
breaks `test_walrust_owned_reload_state_transport_error_is_hard_error`. (Both
reverts applied together in one build; all three FAILED, then restored.)

- `state.json` save/load asymmetry: `save_state` persists `wal_salt` +
  `wal_checksum_chain` (`crates/walrust-core/src/sync.rs:258-267`) but reload
  (`replicator.rs:300-327`) never reads them back => after every restart,
  salt-rollover detection and frame validation are OFF (compounds A1–A3).
- `if let Ok(Some(data))` (`replicator.rs:300`) swallows transport errors as
  "no saved state" => cursor reset (the repo fixed this exact class for
  load_manifest; same discipline needed here).
- Walrust-owned mode: blind changeset `storage.put()` is fixed in core via
  `put_changeset_if_absent` (`crates/walrust-core/src/sync.rs:407-446`,
  `:638-646`, `:1126-1135`, `:1405-1414`, `:1501-1510`). New walrust-owned
  streams now carry `lineage_id` in state and keys via
  `SyncState::ensure_lineage_id` (`crates/walrust-core/src/sync.rs:230-234`),
  lineaged key builders/discovery (`sync.rs:263-396`), initial replicator
  mint/save (`crates/walrust-core/src/replicator.rs:253-263`), and state reload
  (`replicator.rs:337`). Fresh lineage creation is now fenced by
  `ensure_no_saved_state` and `save_initial_state` CAS so a competing `add()`
  cannot replace the active `state.json`.
- Sidecar watch path persists nothing: `current_txid` in memory only;
  production never writes `manifest.json`; restart re-mints TXIDs and
  overwrites remote history (`src/sync/watch_shadow.rs:103-111`).
- `initialize_external_base_state` no longer skips local WAL bytes when the
  external base has no published delta chain (`crates/walrust-core/src/sync.rs:879-884`);
  WAL-size guessing for non-empty remote chains has been removed. A matching
  fsynced local progress record is required before reusing a local WAL cursor.
- Fix: symmetrical state round-trip; transport errors propagate; CAS
  (`put_if_absent`/`put_if_match`) in walrust-owned mode; a lineage ID minted
  at bootstrap and embedded in keys + state; one durable fsynced local
  progress record written only after durability events.

### A11 — Failed uploads create permanent holes; pipeline ships around them; durable cursor is broken
Status: Fixed — root `LocalCache` now persists `(min_txid, max_txid)` interval
metadata from production LTX bytes, advances the durable cursor across uploaded
intervals, protects uploaded proof above the contiguous cursor from cleanup,
re-enqueues failed uploads on cache restart, and the uploader halts on a
permanent upload failure instead of shipping later TXIDs around the hole. Proven
by `test_contiguous_cursor_advances_over_uploaded_ltx_intervals`,
`test_failed_uploads_reenqueue_on_restart`,
`test_cleanup_keeps_uploaded_entries_above_contiguous_cursor`, and
`test_uploader_halts_after_permanent_failure_without_shipping_later_txids`.
`walrust-dst` uses this root cache/uploader pipeline; `walrust-core` has no
corresponding local-cache uploader tree.

Verify (Phase 2C, 2026-07-08): VERIFIED. All four named proofs (now owned by
`walrust_core::legacy_cache`/`legacy_uploader` after convergence) pass on this
branch. Revert-verified the central 2.6 regression: replacing the interval walk
in `legacy_cache::recompute_contiguous` (`if min_txid <= next && next <=
max_txid`) with the old bare-max keying (`if next == max_txid`) makes
`test_contiguous_cursor_advances_over_uploaded_ltx_intervals` FAIL (a multi-page
LTX interval stalls the cursor). Halt-vs-block decision (2.6): walrust HALTS the
uploader on a permanent failure (`handle_join_result_nonfatal` returns the error;
`test_uploader_halts_after_permanent_failure_without_shipping_later_txids`) rather
than shipping later TXIDs around the hole — the pipeline stops loudly and the
failed TXID re-enqueues on restart (`test_failed_uploads_reenqueue_on_restart`).

- `src/uploader.rs:164-185` + `src/cache.rs:305-314`: permanently-failed TXID
  is removed from pending; restart resume reads only `pending_uploads()` —
  failed TXIDs are never re-enqueued by anyone (`failed_uploads()` has zero
  production callers). Later TXIDs keep uploading around the hole; when cache
  cleanup evicts the local copy the gap becomes unrecoverable, and A6 restores
  through it silently.
- F9/F10 contiguous cursor: `recompute_contiguous` (`src/cache.rs:319-333`)
  walks integer-by-integer but entries are keyed by `max_txid` only
  (`wal_sync.rs:617`, `sync/shadow.rs:249`); any multi-page LTX (txids advance
  by pages.len()) stalls the cursor forever. Unit tests use only 1-page
  increments. Also `cleanup()` removing uploaded entries above the cursor
  wedges it permanently; `failed_txids` grows unboundedly.
- Fix: store (min_txid, max_txid) per entry and walk intervals; failed upload
  => halt the pipeline (or block cursor + aggressive retry + loud alert),
  never continue around a hole; re-enqueue failed on restart.

### A12 — Nothing on the ack path is fsynced
Status: Fixed — root cache LTX and manifest writes now use temp-file
write+`sync_all`, atomic rename, and parent-directory fsync before publishing
the cache/manifest ack; root and core shadow segment writers now flush,
`sync_all`, and fsync the shadow directory; core `decode_to_db` writes a synced
temp DB, renames it into place, and fsyncs the parent directory. Root uploader
now re-verifies cached LTX bytes with the production decoder before S3 PUT and
hard-fails/marks the TXID failed if cached bytes are corrupt, preventing
truncated cache entries from becoming remote backups. Proven by
`test_uploader_rejects_corrupt_cached_ltx_before_put`; `disk_queue_tests`
were converted to production-valid LTX fixtures so uploader/cache integration
continues through the real LTX path. Power-loss fsync behavior is not directly
simulated in unit tests, but the production ack paths now issue the required
file and directory fsyncs.

Verify (Phase 2C, 2026-07-08): VERIFIED. `test_uploader_rejects_corrupt_cached_ltx_before_put`
passes and FAILS on revert: gating the `verify_ltx(Cursor::new(&data))`
pre-PUT decode in `legacy_uploader::upload_txid` (behind `if false`) makes a
corrupt cached LTX ship to storage, failing the test's "corrupt cached bytes
must not be PUT" assertion. The fsync file+parent-dir ack paths in
`legacy_cache` (temp-write + `sync_all` + rename + dir fsync) and the shadow
segment / `decode_to_db` fsync paths remain in the converged core.
- `src/cache.rs:236-246` (`write_ltx_inner`) and `:211-222` (`save_manifest`):
  `fs::write` + `rename`, no `File::sync_all`, no directory fsync. Shadow
  segments only `flush()` (`src/shadow.rs:251`). Core shadow same
  (`crates/walrust-core/src/shadow.rs:234`). Restore output `decode_to_db`
  (`crates/walrust-core/src/ltx.rs:116`): `std::fs::write`, no fsync, no
  temp+rename.
- Failure: power loss => truncated LTX in cache under a manifest that lists
  it pending => truncated bytes PUT to S3 as-is (uploader does no re-verify),
  or read fails identically forever.
- Fix: sync_all on file + parent dir before rename/ack; verify LTX integrity
  (decode header/trailer) before PUT.

### A13 — Verification is theater at both levels
Status: Fixed — `walrust verify` now decodes each production LTX through
`verify_ltx_with_result`, validates snapshot-to-incremental TXID and
`post_apply_checksum -> pre_apply_checksum` linkage, fails closed on empty
backups, and prints integrity exit code 5 for verification failures. Periodic
daemon validation now uses listing-based discovery instead of the phantom
manifest and hard-errors on empty backups. Watch-mode auto-compaction now uses
S3 listing discovery with the same reachability guard as manual compaction, and
disabled watch timers no longer materialize `u64::MAX` intervals that panic
before the guarded branches run. Proven by
`sync::verify::tests::test_verify_chain_rejects_snapshot_to_incremental_checksum_mismatch`
`test_verify_no_backup_found`, and
`sync::shadow::tests::test_watch_auto_compaction_uses_listing_without_manifest`;
the disabled-timer regression is covered by
`e2e_cli_watch_restore_round_trips_sqlite_rows`. Current refs:
`src/ltx.rs:440-472`, `src/sync/verify.rs:31-181`,
`src/sync/verify.rs:262-269`, `src/sync/verify.rs:322-394`,
`src/sync/verify.rs:470-486`, `src/sync/shadow.rs:383-480`, and
`src/sync/watch_shadow.rs:475-502`. `walrust-core` has no verify command,
daemon validation path, or watch-mode auto-compaction path for this finding.

Verify (Wave 1b, 2026-07-07): VERIFIED already-fixed (honesty-half). Non-gated
proof `sync::verify::tests::test_verify_chain_rejects_snapshot_to_incremental_checksum_mismatch`
passes and FAILS on revert: gating the cross-file linkage check at
`src/sync/verify.rs:81` (`if false && file.pre_apply_checksum != Some(..)`)
makes it FAIL — with the check off, a snapshot->incremental checksum break is
no longer flagged. `sync::shadow::tests::test_watch_auto_compaction_uses_listing_without_manifest`
also passes non-gated (not revert-checked). `test_verify_no_backup_found` is
S3-gated and skipped locally (sandbox MinIO clock skew, see A6).

- `walrust verify` (`src/sync/verify.rs:233-322`): per-file internal
  checksums + TXID continuity among gen-0 files only, starting from whichever
  file is first. Never checks snapshot->first-incremental linkage, never
  checks cross-file `post_apply == next pre_apply` (checked nowhere in the
  codebase). Certifies exactly the states A6/A8 fail on. Printed exit codes
  (2/1) don't match what `classify_error` actually produces (5).
- Periodic in-daemon validation (`verify.rs:37-47` via `load_manifest`,
  `manifest.rs:299-305`): every error collapses to an empty manifest =>
  early-return `is_valid: true`. Production never writes manifest.json =>
  "Validation passed (0 files)" forever, even against an empty bucket.
  `explain` recommends enabling it.
- Watch-mode auto-compaction (`src/sync/shadow.rs:381-465` run_compaction)
  reads the same phantom manifest => silent no-op; if a manifest ever existed
  it deletes keys missing the `GGGG/` segment and has no F7 guard.
- Fix: verify = full chain walk (snapshot base -> head, cross-file checksum
  linkage, gap detection anchored at the snapshot); validation must hard-fail
  on missing manifest/state; route auto-compaction through the F6-fixed
  listing-based path; align exit codes.

### A14 — Restore/replica destroy existing local data before success is known
Status: Fixed — restore-half fixed in root and core. Root production restore
is proven by `failed_restore_preserves_existing_output_database`; core restore
is proven by `sync::tests::restore_failure_preserves_existing_output_database`
and
`sync::tests::restore_with_snapshot_source_failure_preserves_existing_output_database`.
Root read-replica bootstrap now decodes snapshots to a staged file and
atomically publishes only after decode, fsync, and integrity check; incremental
replica apply now operates on a staged copy and atomically swaps it into place
only after the LTX apply and integrity check succeed. Replica gap handling now
re-seeds only from a snapshot at or past the gap and otherwise returns a hard
error without mutating local data/state. Proven by
`sync::replicate::tests::replica_failed_incremental_apply_preserves_existing_database`
and
`sync::replicate::tests::replica_gap_without_future_snapshot_errors_and_preserves_existing_database`.
`walrust-core` has no read-replica loop corresponding to root
`src/sync/replicate.rs`; its restore-half coverage is the relevant tree.

Verify (Phase 2C, 2026-07-08): VERIFIED (replica half). Both named root
proofs pass on this branch:
`sync::replicate::tests::replica_failed_incremental_apply_preserves_existing_database`
(incremental apply runs on a staged copy that is atomically renamed only after
LTX apply + integrity check succeed — a bad incremental preserves the live DB)
and
`sync::replicate::tests::replica_gap_without_future_snapshot_errors_and_preserves_existing_database`
(a mid-chain gap with no snapshot past it hard-errors and preserves local
data/state — no unbounded re-bootstrap loop, no backwards time travel). The
atomic staged-apply/bootstrap engine is owned by
`walrust_core::legacy_replica`; the root loop keeps the gap decision + backoff.

Verify (Wave 1b, 2026-07-07): VERIFIED already-fixed (restore-half). Non-gated
core proofs `sync::tests::restore_failure_preserves_existing_output_database`
and `..._with_snapshot_source_failure_preserves_existing_output_database` pass
and BOTH FAIL on revert: making `AtomicRestore::new`
(`crates/walrust-core/src/sync.rs:32`) stage directly on the output path
instead of a temp `.restore-*.tmp` (so a failed apply mutates the live output
in place) breaks both — the existing DB is no longer preserved. The restore
path stages to the temp file, runs `verify_sqlite_integrity`, then
`staged_restore.publish(output)` (atomic rename) only on success. Root proof
`failed_restore_preserves_existing_output_database` is S3-gated and skipped
locally (sandbox MinIO clock skew, see A6).

- `src/ltx.rs:127` writes the full DB image THEN checks the trailer; every
  incremental applies in place to the output. A failed restore leaves the
  operator's existing DB destroyed and a half-applied file in place.
- Read replica (`src/sync/replicate.rs:222, 266`): pages written in place and
  bootstrap = truncate+rewrite on the live file readers may have open; no
  atomic swap, no lock. Mid-chain gap older than the latest snapshot =>
  unbounded re-bootstrap loop, replica state jumps backwards every cycle
  (`replicate.rs:179-214`), full snapshot re-downloaded every poll.
- Fix: restore to temp + rename after all checks; replica applies to a temp
  copy then atomically renames (or applies via SQLite with proper locking);
  gap => pick a snapshot PAST the gap or hard-error, with backoff.

---

## HIGH

- B1 — Two-pass checked reader ignores pass-2 verification failures: page
  inserted anyway, chain silently stops advancing
  (`crates/walrust-core/src/wal.rs:489-507`; same in `src/wal.rs`). No
  post-read size/salt re-check (TOCTOU with concurrent reset).
  Status: Fixed (Phase 2C, 2026-07-08). Two halves in
  `crates/walrust-core/src/wal.rs` (WAL is Phase-4 converged, so `src/wal.rs`
  is a shim and this lands once):
  (1) Pass-2 hard stop. In `read_frames_as_page_map_checked`, a pass-2
  `verify_frame_checksum` returning `None` (the frame's bytes changed between
  the pass-1 validate and the pass-2 re-read — a concurrent checkpoint/reset)
  is now a hard error that refuses to emit a partially-validated page map,
  instead of silently inserting the page while the chain stops advancing.
  (2) Post-read stability guard. Both checked readers
  (`read_frames_as_page_map_checked`, `read_frames_as_pages_checked`) now call
  `detect_reset_during_read` before returning success: if the WAL shrank below
  the frames being returned, or the header salt changed since the read began,
  the frames may be a torn mix of two generations, so the reader hard-errors.
  A WAL that merely grew (new frames appended, same salt) stays benign. Proven
  by `wal::tests::test_detect_reset_during_read_rejects_truncated_wal`,
  `..._rejects_salt_change`, and `..._accepts_stable_or_grown_wal`
  (revert-verified: neutering `detect_reset_during_read` to `Ok(())` makes the
  two rejection tests FAIL). Root uses these core readers via the shim.
  Verify (Phase 2C review, 2026-07-08): the three original proofs exercised the
  guard HELPER directly, not the reader CALL SITE — neutering both call-site
  invocations (`if false { detect_reset_during_read(..) }`) left the ENTIRE
  walrust-core suite green (vacuous-guard soft spot confirmed). Added two
  call-site integration tests
  (`wal::tests::test_checked_reader_call_site_rejects_reset_shrink_during_read`,
  `..._rejects_salt_change_during_read`) that drive the guard through the real
  `read_frames_as_pages_checked`: the reader opens its fd on generation A, a
  concurrent task atomically renames a smaller/different-salt generation over
  the path mid-read (Unix open-fd semantics keep the read on gen A), and the
  post-read guard re-opens the path and hard-errors. Both FAIL with the call
  site neutered and PASS restored (revert-verified). Pass-2 hard-error path:
  reached only under in-place mid-call frame rewrite (SQLite RESTART reuse);
  the early `Err` return mutates no caller state (chain seed / page_map are
  locals returned only on `Ok`), and the sole production caller
  (`legacy_wal_sync.rs:234` via `?`) does not advance offset/chain/txid on
  `Err`, so a retry re-reads consistently.
- B2 — `pull_incremental` / `pull_incremental_into_sink` re-anchor the chain
  from `None` every call (`sync.rs:1346, 1510`): a steady-state follower
  pulling 1 changeset/poll never verifies anything (F13 holds only
  intra-batch). API must accept/return the running checksum.
  Status: Fixed — core pull APIs now take and return a `PullCursor { seq,
  checksum }`, decode and verify the complete discovered chain from the caller's
  checksum before applying to the follower DB or `PageReplaySink`, and hard-error
  on gaps or checksum breaks. Proven by
  `sync::tests::pull_incremental_rejects_first_changeset_with_wrong_anchor_checksum`,
  `sync::tests::pull_into_sink_rejects_first_changeset_with_wrong_anchor_checksum`,
  and
  `sync::tests::pull_into_sink_errors_on_broken_chain_without_applying_pages`.
  Root is unaffected: these pull APIs and `PageReplaySink` exist only in
  `crates/walrust-core/`.
  Verify (Phase 2C, 2026-07-08): VERIFIED. All three B2 proofs plus the B3
  proof `restore_with_snapshot_source_rejects_first_incremental_with_wrong_anchor_checksum`
  pass on this branch. The pull APIs take/return a `PullCursor { seq, checksum }`
  and `SnapshotSource` returns a `SnapshotCheckpoint { seq, checksum }`, so a
  steady-state 1-changeset poll and a snapshot-anchored restore both verify the
  first changeset against the caller's running chain (not `None`).
- B3 — `restore_with_snapshot_source` applies the first incremental unverified
  (trait provides no anchor checksum, `snapshot_source.rs:40`,
  `sync.rs:1087-1135`) and returns Ok on a later chain break. Use
  `discover_strict_physical_chain` + add a checksum to the trait.
  Status: Fixed — `SnapshotSource` now returns `SnapshotCheckpoint { seq,
  checksum }`, and `restore_with_snapshot_source` verifies the first incremental
  against the materialized base chain checksum before applying it. Later gaps and
  chain breaks remain hard restore errors, and failed staged restores do not
  publish over the existing output. Proven by
  `sync::tests::restore_with_snapshot_source_rejects_first_incremental_with_wrong_anchor_checksum`.
  Root is unaffected: `SnapshotSource` is a core-only extension point.
- B4 — Shadow copy path uses the unvalidated reader `read_frames_as_pages`
  (`src/shadow.rs:198-199`; also `crates/walrust-core/src/shadow.rs:182`):
  no salt/checksum validation on the DEFAULT path; torn/stale frames shipped
  as commits. Route through the checked reader.
  Status: Fixed (Phase 2A, 2026-07-08) — added
  `wal::read_frames_as_pages_checked` (an ordered-`ParsedFrame` variant of the
  checked page-map reader: validates the frame checksum chain + frame salt,
  stops at the last good committed frame, returns the running chain to seed the
  next incremental read). `ShadowWal::copy_frames` now reads through it,
  carrying `wal_chain` across incremental reads and reseeding from the header on
  rollover. Shadow is Phase-4 converged, so this lands once in
  `crates/walrust-core/src/{wal,shadow}.rs`. Proven by
  `wal::tests::test_read_frames_as_pages_checked_{accepts_ordered_valid_chain,
  rejects_torn_tail,rejects_stale_salt_tail}` and the shadow copy path is
  additionally exercised end-to-end by the A3/A4 racing E2E cases. Residual: an
  incremental read from a non-zero offset with no carried chain (only the first
  read immediately after a process restart, before the next generation) skips
  per-frame checksum validation — same limitation as the existing checked
  page-map reader; the salt-change rollover check is unaffected. Persisting the
  chain in the shadow progress record is A10/2B state-durability scope.
  Phase 2B (2026-07-08): CLOSED. `ShadowProgress` now persists `wal_copy_offset`,
  `wal_salt`, and the running `wal_checksum_chain` at that offset (all
  `serde(default)` for back-compat). On restart the root shadow watch loop
  resumes the live-WAL read from the persisted offset and seeds `ShadowWal`'s
  salt + chain via `ShadowWal::restore_read_cursor`, so the first post-restart
  `copy_frames` validates the frame checksum chain per-frame from the resumed
  offset (instead of re-reading — and re-appending — the whole live WAL from
  offset 0), and detects a checkpoint that reset the WAL during downtime (salt
  mismatch => rollover). Proven by
  `shadow::tests::test_restore_read_cursor_resumes_without_re_reading_wal`,
  `shadow::tests::test_restore_read_cursor_detects_downtime_checkpoint`
  (revert-verified), and the extended
  `legacy_shadow_progress_persistence_is_owned_by_core` round-trip.

  Adversarial review (Phase 2B, 2026-07-08): two additions.
  (a) Back-compat now has a dedicated proof: a pre-B4 progress file (no
  `wal_copy_offset`/`wal_salt`/`wal_checksum_chain`) loads with no panic and falls
  back conservatively — `wal_copy_offset` defaults to 0 (re-read from the WAL
  head) and salt/chain to None (`restore_read_cursor` becomes a no-op, seeding
  nothing stale). Proven by
  `legacy_pre_b4_shadow_progress_loads_with_safe_defaults`. And
  `test_restore_read_cursor_detects_downtime_checkpoint` was re-revert-verified
  with a TRUE no-op `restore_read_cursor` (it FAILS: generation does not advance),
  confirming the salt restore — not just the offset-shrink path — is load-bearing.
  (b) Recovery-completeness scope: the downtime detection bumps the generation and
  re-seeds per-frame validation, but does NOT eagerly re-snapshot. The frames an
  external checkpoint folded away during downtime live in the main DB FILE and are
  re-imaged by the next periodic snapshot (the snapshot timer re-reads the whole
  DB file). An eager re-snapshot on every generation bump is deliberately NOT
  wired because a generation bump in shadow mode is normally BENIGN (walrust's own
  copy-then-checkpoint also bumps it), and the PASSIVE-fold-vs-destructive-reset
  distinction is not cheaply available (the same residual class as the A3
  first-checkpoint window and B5). So a downtime checkpoint's completeness is
  bounded by `snapshot_interval`, matching the A3/B5 residual; it is not silent
  loss (the generation-boundary chain re-seed keeps restore fail-closed on any gap
  rather than short).
- B5 — Shadow WAL salt seeded `(0,0)` when no WAL exists at startup and only
  updated inside the rollover branch (`src/shadow.rs:83-86, 184`): for a
  fresh DB, all future checkpoints are invisible. Also page_size defaults to
  4096 and is never refreshed => misaligned parsing for non-4096 DBs whose
  WAL appears after start.
  Status: Fixed (Phase 2A, 2026-07-08) — `ShadowWal` now tracks `header_seeded`.
  When the first real WAL header appears after a fresh start, `copy_frames`
  seeds `page_size` and `wal_salt` from that header as initialization (not a
  rollover, so the generation is not bumped), and `page_size` is also refreshed
  on rollover. This removes the stuck `(0,0)` salt (which had made every later
  checkpoint invisible) and the stale 4096 page_size (which mis-framed the
  readback/upload path for non-4096 DBs). Proven by
  `shadow::tests::test_shadow_reseeds_salt_and_page_size_when_wal_appears_after_startup`.
- B6 — Poll-mode sync trigger is "WAL grew" only
  (`watch_independent.rs:449-455`): TRUNCATE/RESTART resets never fire a
  sync; unbounded RPO on low-write DBs.
  Status: Fixed (Phase 2A, 2026-07-08). Growth-gate half was already resolved
  by the convergence: the independent `poll_timer` arm calls `do_sync`
  unconditionally every interval, re-reading the WAL from the persisted cursor
  and detecting TRUNCATE/RESTART resets via `checkpoint_detected` (no size/salt
  precondition). The remaining half — this mode had no time-based full-snapshot
  cadence, so a reset between syncs left an unbounded RPO — is closed by adding
  a `snapshot_timer` arm (disabled when `snapshot_interval == 0`) that drives
  `take_snapshot_with_retry`, mirroring the default shadow mode. Decision: add
  the timer rather than remove the experimental mode (smaller diff, no user
  removal, parity with shadow mode). Proven end-to-end by
  `e2e_cli_watch_independent_snapshot_timer_round_trips_through_reset`.
- B7 — `remove()` and shutdown final syncs swallow upload failures
  Status: Fixed — core `Replicator::remove`, `run_replication`, and
  `run_wal_replication` now return hard errors on final sync failure and
  `remove` keeps the database registered; root uploaders return drain errors,
  independent shutdown awaits final sync/uploader drain, and shadow shutdown
  copies final real WAL frames through the normal shadow cache/direct sync path
  before awaiting uploader drain. Proven by
  `test_remove_keeps_database_registered_when_final_sync_fails`,
  `test_run_replication_returns_final_sync_error_on_shutdown`, and
  `test_run_wal_replication_returns_final_sync_error_on_shutdown`,
  `test_uploader_shutdown_returns_error_after_failed_upload`, and
  `test_shadow_shutdown_syncs_final_real_wal_frames_to_cache`.
  (`crates/walrust-core/src/replicator.rs:426-452`;
  `crates/walrust-core/src/sync.rs:1798-1806, 1875-1883`;
  `src/uploader.rs:280-338, 346-379, 392-400`;
  `src/sync/watch_independent.rs:286-292, 369-397, 456-480`;
  `src/sync/watch_shadow.rs:138-194, 919-934`).
  Verify (Wave 1b, 2026-07-07): VERIFIED already-fixed. Four non-gated proofs
  pass and FAIL on revert. Making the final sync in `Replicator::remove`
  swallow errors (`crates/walrust-core/src/replicator.rs:445`,
  `.with_context(..)?` -> `.unwrap_or(0)`, which also lets the db be
  de-registered) breaks `test_remove_keeps_database_registered_when_final_sync_fails`;
  turning the two shutdown final-sync `return Err(..)` arms at
  `crates/walrust-core/src/sync.rs:2484` and `:2562` into `warn!` breaks
  `test_run_replication_returns_final_sync_error_on_shutdown` and
  `test_run_wal_replication_returns_final_sync_error_on_shutdown`; gating the
  drain error at `crates/walrust-core/src/legacy_uploader.rs:321`
  (`if false && !failed.is_empty()`) breaks
  `test_uploader_shutdown_returns_error_after_failed_upload`. The fifth proof
  `test_shadow_shutdown_syncs_final_real_wal_frames_to_cache` (root) passes
  non-gated (not revert-checked; the cluster is already proven active).
- B8 — `apply_changeset_to_db` / `pull_incremental` accept `page_id = 0` =>
  u64 underflow offset (`crates/walrust-core/src/ltx.rs:170`,
  `sync.rs:1121, 1381`) — F4 guard landed only in decode_to_db. Also no file
  truncation on shrink (VACUUM leaves stale tail pages; snapshot then derives
  num_pages from the inflated size).
  Status: Fixed — core HADBP decode/apply now preflights page size/page count,
  rejects SQLite page ID 0 before offset math, routes restore/pull direct
  apply loops through the shared checked writer, and new core WAL changesets
  carry an end-page-count marker so followers truncate after shrink/VACUUM.
  Proven by
  `ltx::tests::test_apply_rejects_page_id_zero_without_mutating_database`,
  `sync::tests::pull_incremental_rejects_page_id_zero_without_mutating_database`,
  and
  `sync::tests::pull_incremental_truncates_database_to_encoded_end_page_count`.
  Root uses the separate `litepages` LTX path; page IDs/page sizes are
  constructed through `PageNum::new`/`PageSize::new` and this HADBP
  `apply_changeset_to_db`/`pull_incremental` bug does not exist there.
  Verify (Phase 2C, 2026-07-08): VERIFIED. `test_apply_rejects_page_id_zero_without_mutating_database`,
  `pull_incremental_rejects_page_id_zero_without_mutating_database`, and
  `pull_incremental_truncates_database_to_encoded_end_page_count` pass on this
  branch: page_id >= 1 is guarded before offset math in all core apply paths
  and a shrink/VACUUM truncates the follower to the encoded end-page count.
  Also confirmed the root legacy decode path guards `pn < 1 || pn > num_pages`
  and bounds `num_pages * page_size` before allocating (`legacy_ltx::decode_to_db`).
- B9 — Untrusted-size allocations: `page_size`/`max_page` magnitude unchecked
  (`crates/walrust-core/src/ltx.rs:87-91`; page_size never checked for
  0/pow2/<=65536); `hadb-changeset` `Vec::with_capacity(page_count)` with
  untrusted count. DoS on crafted objects.
  Status: Fixed — core now preflights HADBP headers before
  `hadb-changeset::decode` can allocate a page vector, enforces SQLite page
  sizes as powers of two in 512..=65536, streams snapshot decode to a synced
  temp file instead of materializing the whole output DB in memory, and rejects
  decoded DB sizes beyond a 1 TiB safety cap. Proven by
  `ltx::tests::test_apply_rejects_invalid_sqlite_page_size_without_mutating_database`
  and `ltx::tests::test_decode_rejects_invalid_sqlite_page_size`.
  Verify (Phase 2C, 2026-07-08): VERIFIED. Both named proofs pass on this
  branch: HADBP headers are preflighted (page_size a power of two in
  512..=65536, decoded DB size capped) before `hadb-changeset::decode` can
  allocate a page vector, so a crafted/corrupt object cannot force a giant
  allocation.
- B10 — Snapshot TXID cursor claims WAL-resident commits the raw file copy
  doesn't contain (`sync.rs:913-919` counts WAL commits; binary path PASSIVE
  may not backfill) => transactions in neither snapshot nor any post-snapshot
  incremental.
  Status: Fixed (Phase 2A, 2026-07-08). The active core `take_snapshot`
  (`crates/walrust-core/src/sync.rs`) already folds the WAL with a
  completeness-checked `checkpoint_wal` (TRUNCATE, busy/log/checkpointed row
  verified) BEFORE `count_wal_commits`, so a counted commit is always present in
  the encoded image. The legacy `snapshot_database_to_storage` derives its TXID
  from discovery (`current_txid + 1`), not from a WAL-commit count, so it does
  not have the phantom-TXID arithmetic bug; and it is shared by the shadow watch
  loop, whose checkpoint blocker intentionally pins a live WAL frame (a TRUNCATE
  there hard-fails every tick, and the shadow WAL carries un-folded frames as
  incrementals), so it correctly KEEPS a best-effort PASSIVE checkpoint.
  Decision/deviation: an early attempt to switch that shared function to TRUNCATE
  broke the pinned-reader watch E2E cases (`busy=1` snapshot failures) and was
  reverted; instead the one-shot `walrust snapshot` command
  (`sync::compact::snapshot`), which has no shadow to carry incrementals, now
  performs a completeness-checked `checkpoint_wal_truncate` fold before encoding
  and fails closed if another process pins the WAL. Proven by
  `legacy_wal_sync::legacy_manual_snapshot_folds_wal_resident_rows` (unblocked
  snapshot folds all WAL-resident rows into the decoded image with
  `integrity_check == ok`).
- B11 — Crash window between changeset put and save_state => same-seq
  re-publish with different bytes (blind put overwrites; live followers hit a
  chain break misdiagnosed as stale lineage) (`sync.rs:473-493`).
  External-mode variant: lost PUT response => permanent equivocation wedge
  with no re-anchor path (`sync.rs:441-470, 684-717`).
  Status: Fixed (Phase 2B, 2026-07-08). Two halves:
  (1) Fail-loudly + typed. `put_changeset_if_absent` and
  `publish_delta_envelope` already refuse a same-seq overwrite with different
  bytes (CAS from earlier phases); that refusal is now a typed
  `WalrustError::Equivocation` (exit 5) instead of an untyped bail, so callers
  can classify it. Identical bytes remain an idempotent success (retry / lost
  PUT-response safety).
  (2) Recovery/re-anchor. In walrust-owned mode, a same-seq conflict whose
  existing object is *provably our own* prior publish at that seq is adopted as
  committed and the stream re-anchors with a fresh snapshot, folding in any
  frames written after the crash. A foreign object, a second live writer's
  divergent object, or garbage still hard-fails (typed equivocation).
  External-base mode surfaces the typed equivocation; its documented recovery is
  `initialize_external_base_state` from a new base (which already adopts a
  matching same-seq changeset via `external_same_seq_changeset_checksum`).

  Adversarial review (Phase 2B, 2026-07-08): the walrust-owned recovery's
  ORIGINAL discriminator was `(seq match, prev_checksum match)` and was
  documented as "the exact signature of a durable put whose save_state never
  ran". That claim was OVERSTATED: `prev_checksum` is the pre-apply base
  checksum, so ANY second live writer sharing the same lineage + base state
  (e.g. an HA failover where the promoted node restored from the same backup and
  the original node came back) publishes a DIFFERENT changeset at the same seq
  with the IDENTICAL `(seq, prev_checksum)` signature. The recovery therefore
  silently ADOPTED a second writer's object and re-anchored on it — actively
  re-legitimizing split-brain (a regression versus the pre-B11 hard-fail).
  Confirmed empirically by a two-writer test that the original code passed
  through `Ok(1)` instead of erroring.

  FIX (this review): the recovery is now gated on a durable, local, fsynced
  publish-intent write-ahead record (`PublishIntent`,
  `.walrust-<db>/publish-intent.json`, temp+fsync+rename+dir-fsync). Before each
  walrust-owned CAS put, we load the PRIOR intent (the pre-crash one) and then
  record the intent for THIS publish. A same-seq conflict is adopted ONLY if the
  prior local intent names this exact publish (matching lineage, seq, base
  checksum) AND the stored object's checksum is byte-for-byte the one we
  recorded — proof THIS process authored it. A second writer records its own
  intent on its OWN disk, so its object's checksum never matches ours and the
  conflict correctly hard-fails; a failover node whose last intent is at a
  different seq also hard-fails. This is a state-durability authorship proof, NOT
  a distributed fence: a full writer-fencing/lease token is later-phase, but the
  local intent closes the silent split-brain re-legitimization within Phase-2B
  scope. Proven by
  `sync::tests::walrust_owned_reanchors_after_crash_window_same_seq_conflict`
  (revert-verified: disabling the adoption makes the post-crash row
  unrestorable), the new
  `sync::tests::walrust_owned_sync_rejects_second_writer_same_base_same_seq`
  (revert-verified: it FAILED against the original `(seq, prev_checksum)`
  discriminator and PASSES with the intent gate), and
  `sync::tests::walrust_owned_sync_rejects_foreign_same_seq_changeset`; the
  existing `walrust_owned_sync_rejects_divergent_existing_changeset` and
  `publish_delta_envelope_rejects_same_seq_divergent_bytes` still pass.
- B12 — journal_mode change away from WAL => replication silently freezes
  (0 frames forever, no error/webhook) (`wal_sync.rs:99-115`,
  `sync.rs:400-403`). `open_checkpoint_blocker` silently converts DELETE-mode
  DBs to WAL as a side effect (`src/shadow.rs:120`).
  Status: Fixed. Current refs: root WAL sync now checks `PRAGMA journal_mode`
  before accepting no-WAL no-ops and hard-errors if SQLite is not in WAL mode
  (`src/sync/wal_sync.rs:43-48`, `src/sync/wal_sync.rs:102-118`,
  `src/sync/wal_sync.rs:829-847`); root shadow construction/copy now rejects
  non-WAL mode without converting it (`src/shadow.rs:39-50`,
  `src/shadow.rs:129-135`, `src/shadow.rs:187-202`). Root watch loops return
  hard errors and emit `upload_failed` webhooks on WAL/shadow sync failure
  (`src/sync/watch_independent.rs:500-506`,
  `src/sync/watch_shadow.rs:572-582`). Core has the same hard WAL-mode and
  shadow conversion guards (`crates/walrust-core/src/sync.rs:445-463`,
  `crates/walrust-core/src/sync.rs:471-476`,
  `crates/walrust-core/src/sync.rs:891-896`,
  `crates/walrust-core/src/sync.rs:1302-1307`,
  `crates/walrust-core/src/sync.rs:1842-1854`,
  `crates/walrust-core/src/sync.rs:1920-1931`,
  `crates/walrust-core/src/shadow.rs:22-33`,
  `crates/walrust-core/src/shadow.rs:111-118`,
  `crates/walrust-core/src/shadow.rs:170-183`). Proven by
  `sync::wal_sync::tests::test_sync_wal_concurrent_rejects_database_out_of_wal_mode`,
  `sync::wal_sync::tests::test_sync_wal_retry_notifies_webhook_when_database_leaves_wal_mode`,
  `shadow::tests::test_shadow_wal_new_rejects_delete_mode_without_converting`,
  `sync::tests::sync_wal_rejects_database_out_of_wal_mode`, and core
  `shadow::tests::test_shadow_wal_new_rejects_delete_mode_without_converting`.
  Verify (Wave 1b, 2026-07-07): VERIFIED already-fixed. Non-gated core proof
  `sync::tests::sync_wal_rejects_database_out_of_wal_mode` passes and FAILS on
  revert: relaxing the WAL-mode gate at `crates/walrust-core/src/sync.rs:939`
  (`if mode.eq_ignore_ascii_case("wal") || true`) makes it FAIL — a non-WAL DB
  is no longer a hard error. The core delete-mode shadow guard
  `shadow::tests::test_shadow_wal_new_rejects_delete_mode_without_converting`
  also passes non-gated (not revert-checked). The root
  `test_sync_wal_concurrent_rejects_database_out_of_wal_mode` and
  `test_sync_wal_retry_notifies_webhook_when_database_leaves_wal_mode` pass
  non-gated as well.
- B13 — Restore cache substitution keyed by bare TXID, no lineage/etag binding
  (`restore.rs:108-118`); NO_CHECKSUM litestream files skip even internal
  checks.
  Status: Fixed (Phase 2C, 2026-07-08). Root `CachedLegacyStorage::get`
  (`src/sync/restore.rs`) no longer substitutes a cached LTX on a bare max-TXID
  match. `cache_substitute_for_key` now (a) parses the FULL `(min, max)` TXID
  interval the requested object KEY names (`key_txid_range`, canonical
  `db/GEN/min-max.ltx` and legacy-flat shapes), (b) decodes and verifies the
  cached bytes with `walrust_core::legacy_ltx::verify_ltx`, and (c) substitutes
  ONLY when the cached LTX's encoded `(min, max)` range equals the key range;
  any mismatch or verification failure falls back to authoritative S3. This
  closes the bare-TXID hole (two objects from different generations/lineages
  can share a max TXID; the cache dir is already per-DB-path scoped, so the
  cross-object confusion that remained was the TXID-interval collision). The
  NO_CHECKSUM half is covered on two fronts: `verify_ltx` runs the internal LTX
  trailer/decode check regardless of the litestream NO_CHECKSUM per-page flag,
  and the restore chain (`legacy_restore`) links files via walrust-computed
  DB-anchored `pre/post_apply` checksums (`apply_ltx_to_db_checked`), which are
  computed from actual DB bytes even for NO_CHECKSUM files — so a NO_CHECKSUM
  object cannot silently break the chain. Proven by
  `sync::restore::tests::cache_substitution_binds_to_key_txid_range_not_bare_txid`
  (revert-verified: restoring the bare-`has_txid` substitution makes it FAIL —
  a same-max/different-min key wrongly returns the cached bytes). Core is
  unaffected: this cache-over-S3 substitution is a root-only restore adapter.
  Verify (Phase 2C review, 2026-07-08): revert-verified independently
  (`if true || cached == (min,max)` → test FAILS). Attack edges checked:
  (a) NO_CHECKSUM litestream files — `verify_ltx` parses the header + decodes
  every page regardless of the NO_CHECKSUM flag (that flag only skips the
  per-page checksum COMPARE, not the header/range decode), so the decoded
  `(min,max)` used for the binding is authoritative even for those files; the
  restore chain separately re-checks DB-anchored pre/post checksums.
  (b) Interval parse failures — `key_txid_range` returns `None` and
  `cache_substitute_for_key` short-circuits to S3 (no panic); added assertions
  for an unparseable key and an empty key to the proof.
  (c) SNAPSHOT vs INCREMENTAL at the same key — the cache holds one file per
  max-TXID; the binding compares the DECODED `(min,max)` to the key range, so a
  cached snapshot (min=1) never satisfies an incremental key (min>1) and vice
  versa. Residual: a same-`(min,max)` collision across generations after a
  TXID-rewinding reset would pass the binding, but is backstopped by the
  DB-anchored restore chain checksums (documented, later-phase).
- B14 — `list_delta_envelopes_after` never asserts payload.seq == key-derived
  seq (`sync.rs:748-767`); genesis sentinel ambiguity (empty vs 32 zero
  bytes) in `external_delta.rs`.
  Status: Fixed (Wave 3b, Phase-4 remainder). `list_delta_envelopes_after`
  (`crates/walrust-core/src/sync.rs`) now hard-errors with a typed
  `WalrustError::Integrity` when a decoded `payload.seq` disagrees with the
  key-derived seq — the key carries the ordering authority (zero-padded hex
  sorts numerically), so a mislabeled envelope can no longer be returned for a
  follower to apply out of order. This protects every consumer, including the
  new production follower API (see Phase 4.1 below). Proven by
  `sync::tests::list_delta_envelopes_after_rejects_seq_key_mismatch`
  (revert-verified: dropping the check returns the mislabeled envelope). The
  genesis-sentinel half is a non-issue on the production fenced-follower path:
  `reconstruct_fenced_follower` anchors on an explicit 32-byte
  `base_envelope_checksum` and requires each `prev_checksum` to equal the
  running anchor exactly, so an empty (genesis) `prev_checksum` never matches a
  real anchor and fails closed — no empty-vs-32-zeros conflation is reachable.

## MEDIUM / LOW (abbreviated)

- Config glob matching nothing is a warn+skip, not an error
  (`config.rs:314-317`).
- CLI clap defaults silently override walrust.toml values
  (`main.rs:585-595`).
- Stringly-typed error classification everywhere
  (`e.to_string().contains(...)` in `sync.rs:1015`, `replicator.rs:432`,
  `errors.rs:114-183`); printed exit codes wrong (`verify.rs:375-386`).
  Status: Fixed (Phase 4.2 + confirmed Wave 3b). `errors.rs` classifies only
  typed `WalrustError` values found in the `anyhow` chain
  (`classify_error`/`classify_or_else` downcast; no substring match), and
  `replicator.rs` restore uses a `downcast_ref::<WalrustError>` on
  `RestoreNotFound` instead of a "No snapshot found" string test. A Wave-3b
  grep of `crates/walrust-core/src/{sync.rs,replicator.rs,errors.rs}` and
  `src/` for `.contains(` on error strings finds only test assertions and
  non-error string ops — no production control-flow classification remains. The
  new fenced-follower fence rejections are typed `WalrustError::Integrity`
  (exit 5), keeping the end-to-end typing invariant. Exit-code contract held
  by the `cli_exit_codes` tests.
- Corruption webhook fired via tokio::spawn on the exit path — usually lost
  (`restore.rs:129`).
- Legacy `sync_wal_and_manifest` grows manifest unboundedly (`sync.rs:1275`).
- Naming: `ltx.rs` in core is HADBP, not LTX; `external_delta.rs` calls HADBP
  payloads "raw LTX bytes"; `WAL_MAGIC_BE/LE` names encode the A1 inversion.
  Status: Fixed (Wave 3b). `WAL_MAGIC_LE/BE` were already corrected in Phase 1
  (`WAL_MAGIC_LE=0x377F_0682`, `WAL_MAGIC_BE=0x377F_0683` — the real SQLite
  convention). `external_delta.rs` no longer calls the payload "raw LTX bytes":
  the module doc, `DeltaPayloadV1::ltx_payload` doc, and `MagicMismatch` doc now
  say "raw HADBP changeset bytes" and note the `ltx_payload` field name is a
  back-compat alias. Core `ltx.rs`'s header documents that the `walrust_core::ltx`
  module name is a LEGACY ALIAS for the HADBP codec (not byte-compatible with
  litepages LTX). Decision: doc fix, not rename — the `ltx` module and
  `ltx_payload` field are public API used across the root crate, the DST
  harness, and embedders, so a rename is pure churn in multiple trees for zero
  behavior change (the Session-8 override explicitly permits the doc fix here).

---

## Test adequacy (summary)

- No CI runs any tests (workflows: benchmarks/deploys/pypi only). No cargo
  workspace: root `make test` doesn't touch walrust-core or walrust-dst.
  Both manifests `[patch]` hadb to `../hadb` (sibling checkout, specific
  branch) — repo can't build on a clean machine.
- Root integration tests hard-require live Tigris credentials, not
  #[ignore]-gated.
- The production pipeline never round-trips anywhere: no test does real
  SQLite + concurrent writes -> production watch loop / Replicator -> storage
  -> restore -> integrity_check + content diff. DST invariants drive
  `src/testable.rs` (codec-only), not the shadow/cache/uploader/Replicator
  paths — which is why A1/A3/A5/A8 survived a "fixed and tested" review.
- Crash consistency is simulated, never real (no SIGKILL tests; chaos
  "crashes" fault prints `[TODO]`; stress/soak cannot fail).
  CLOSED (Phase 3): real process-SIGKILL E2Es run in CI on both stacks
  (`e2e_cli_watch_sigkill_restart_round_trips_sqlite_rows`,
  `e2e_core_replicator_sigkill_restart_round_trips_sqlite_rows` + its spawned
  `e2e_core_replicator_sigkill_child`); the chaos `crashes` fault is now a real
  child-process kill (`chaos::chaos_process_crash_recovery`, CI-covered by
  `walrust-dst/tests/crash_recovery.rs`); stress/soak now hard-fail on a
  threshold breach (`evaluate_stress_result`/`evaluate_soak_result`,
  `threshold_tests`).
- Phase-4 external-delta mode: publish semantics well tested; NO
  restore-from-published-deltas test.
  CLOSED (Phase 3): `walrust-dst` `prop_fenced_delta_restore`
  (`test_prop_fenced_delta_restore`) drives the real fenced writer path
  (`Replicator` external mode + `set_external_delta_base` +
  `sync_wal_fenced_delta`) and reconstructs the exact DB in a follower from the
  published `.tlmd` envelope sequence (epoch/writer fence + BLAKE3 chain +
  `apply_changeset_to_db`), asserting `integrity_check` + row equality.
- Untested: compaction-vs-restore races, two watchers on one DB, ENOSPC,
  DBs > ~2MB, 64KB pages, timestamp PITR.
  CLOSED (Phase 3, except timestamp PITR — see A9 doc-fix decision):
  `e2e_compaction_during_restore_keeps_backup_restorable`,
  `test_walrust_owned_concurrent_two_watchers_only_one_wins`,
  `test_walrust_owned_enospc_during_add_is_hard_error`,
  `e2e_cli_watch_restore_round_trips_64kb_pages`,
  `ltx::tests::test_sqlite_snapshot_over_100mb_smoke`, and
  `ltx::tests::test_snapshot_various_page_sizes` (both trees).

---

## Fix plan (phased; each phase gated on the previous)

### Phase 0 — Make correctness provable (foundation)
0.1 Cargo workspace (root + crates/walrust-core + walrust-dst); `make test`
    runs everything.
    Status: Fixed — root workspace now includes all three crates; `make test`
    runs `cargo test --workspace` through Soup locally and without Soup in CI.
0.2 Resolve the `../hadb` patch problem (vendor, pin to git rev, or gate) so
    a clean clone builds. CI workflow: fmt/clippy + all three crates' tests +
    MinIO service for storage tests. Gate cred-requiring tests behind
    #[ignore] or env detection.
    Status: Fixed — `hadb-*` dependencies are pinned to git rev
    c3eab301aa680bc647641d78f0aa3d640589ef9b; CI provisions MinIO and runs
    fmt, clippy, and `make test USE_SOUP=0`. Live S3 tests remain active and
    use Soup credentials locally or MinIO credentials in CI.
0.3 Test helper: build REAL SQLite WALs via rusqlite (replace zero-checksum
    synthetic fixtures); keep synthetic builders only where they compute real
    checksums.
    Status: Fixed — `wal::tests::test_real_sqlite_wal_helper_produces_live_wal`
    exists in both `src/` and `crates/walrust-core/` and asserts a real SQLite
    WAL header, nonzero checksum, and live WAL bytes.
0.4 Skeleton E2E harness (used by later phases): real SQLite writer +
    external autocheckpointing connection -> production pipeline -> MinIO ->
    restore -> `PRAGMA integrity_check` + row diff. Add SIGKILL-restart
    variant. One per stack (binary watch, core Replicator).
    Status: Fixed — `tests/production_e2e.rs` adds
    `e2e_cli_watch_restore_round_trips_sqlite_rows`,
    `e2e_cli_watch_sigkill_restart_round_trips_sqlite_rows`,
    `e2e_core_replicator_restore_round_trips_sqlite_rows`,
    `e2e_core_replicator_restart_reopens_state_and_restores_cleanly`, and
    `e2e_core_replicator_sigkill_restart_round_trips_sqlite_rows`.

Phase 0 — Wave 1 verification (2026-07-07):
- 0.1 Confirmed: `[workspace]` members are `.`, `crates/walrust-core`,
  `walrust-dst`; `make test` runs `cargo test --workspace`.
- 0.2 Confirmed a clean clone builds from the pinned rev with NO local patch:
  `cargo build --workspace` and `cargo test --workspace` pass with no
  `.cargo/config.toml` present. The `[patch."…hadb.git"]` blocks are already
  deleted from `Cargo.toml` and `walrust-dst/Cargo.toml`. `.gitignore` ignores
  `.cargo/`, and a gitignored `.cargo/config.toml` `[patch]` -> `../hadb` is
  the local-dev override (not committed). Decision/deviation: the hadb pin was
  KEPT at `rev = c3eab301…` (proven-good, CI-fetchable) rather than bumped to
  the current branch head of `hoist-prereq-internal-lease-store`
  (remote `6d8ae0c…`); Cargo cannot carry both `branch` and `rev`, so the
  branch is named in a comment on each `hadb-*` dependency. Rationale: the
  paramount Phase-0 requirement is a green clean build; bumping to an
  actively-developed branch head risks an API break with no benefit here.
- 0.3 Confirmed the real-WAL helper exists (`build_real_sqlite_wal` in
  `crates/walrust-core/src/wal.rs`; `create_real_wal_db` in the root shadow
  tests; `create_source_db` + `open_external_autocheckpoint_connection` in
  `tests/production_e2e.rs`). WAL is Phase-4 converged to one tree, so the
  "share across both trees" clause is moot (root re-exports core).
- 0.4 Confirmed the E2E + SIGKILL harness exists (CLI watch, CLI SIGKILL,
  core Replicator, core restart, core SIGKILL) and uses a real external
  autocheckpointing connection (`wal_autocheckpoint=1`). These are
  credential-gated, not "known-failing pending a finding," so they use env
  gating rather than `#[ignore]`.
- 0.5 Gap found and fixed in Wave 1: ~22 S3-dependent tests (in
  `production_e2e.rs`, `restore_chain.rs`, `test_verify.rs`, `cli_exit_codes.rs`,
  `snapshot_source_s3.rs`, and the `sync::replicate`/`sync::shadow` unit tests)
  were neither `#[ignore]` nor env-gated, so a clean-machine `cargo test
  --workspace` failed on S3 service errors. Added a `s3_test_enabled()` guard
  (skips when neither `AWS_ENDPOINT_URL_S3`/`AWS_ENDPOINT_URL` nor
  `AWS_ACCESS_KEY_ID` is set). CI (MinIO) and local Soup set these, so the
  tests still run there; a clean machine skips them. `cargo test --workspace`
  now exits 0 with no `.cargo/config.toml`. CI workflow is `ci.yml`
  (fmt + clippy + `make test USE_SOUP=0` against a MinIO service), which
  satisfies the 0.5 `test.yml` intent.

Phase 0+1 — independent audit (2026-07-08, fresh reviewer, review/phase-0-1-audit):
- Verdict: Phase 0 (0.1-0.5) and Phase 1 (1.3-1.7) are SATISFIED. No code
  defects found; no dodged Phase-1 obligations. The only residuals are the
  A3/A4 racing-checkpoint E2E cases, which are correctly Phase-2A scope (see
  the DEFERRED notes on A3/A4).
- Revert-proofs re-run and re-confirmed (fix reverted -> named test FAILS ->
  restored): A1 (`magic_is_big_endian` predicate, core `wal.rs`) ->
  `test_real_sqlite_wal_checked_reader_validates_checksum_chain`; A6
  (restore contiguity gate, core `sync.rs`) ->
  `restore_errors_on_noncontiguous_incremental_sequence`; A10 (state-reload
  transport-error guard, core `replicator.rs`) ->
  `test_walrust_owned_reload_state_transport_error_is_hard_error`; A13
  (cross-file linkage, root `verify.rs`) ->
  `test_verify_chain_rejects_snapshot_to_incremental_checksum_mismatch`. All
  four drive production paths (real rusqlite WAL / core restore / core
  Replicator reload / root verify chain), not fixtures.
- 0.5 CI holes checked against the real run (PR #10 CI run 28921404005): the
  ~22 S3-gated tests RUN and PASS in CI against MinIO, not skip. Evidence:
  `production_e2e` = 7 passed / 0 failed / 1 ignored (the ignored one is the
  SIGKILL child helper); `snapshot_source_s3` = 9 passed; the
  `restore_chain` / `test_verify` / `cli_exit_codes` gated tests all `... ok`;
  zero "SKIP"/"no S3 endpoint" lines anywhere in the log; zero FAILED. No
  env-var-name mismatch: `s3_test_enabled()` keys off `AWS_ACCESS_KEY_ID` /
  `AWS_ENDPOINT_URL*`, all set by the CI env block.
- Clean-machine green re-confirmed locally: `cargo test --workspace` with all
  S3 env unset exits 0 (27 test-result summaries, 0 failed; S3 cases skip).
- Shims audited: `src/wal.rs` (6-line `pub use`), `src/ltx.rs`, and
  `src/shadow.rs` are pure re-exports of `walrust-core` (the shadow shim adds
  only a tested `format_segment_name` helper); no semantic drift, so the
  dual-tree rot that spawned half these findings cannot recur for WAL/LTX/
  shadow. No new TODO/unimplemented/`#[ignore]` half-work introduced by the
  Phase 0/1 work (the sole `#[ignore]` is the legitimate SIGKILL child helper;
  the walrust-dst chaos `[TODO]` is pre-existing Phase-3.2 scope).

### Phase 1 — Stop lying (small diffs, loud errors)
1.1 A1: flip endianness predicate (both crates), rename constants, golden
    real-WAL test.
1.2 A2: frame-salt check; unverifiable header checksum = hard error; migrate
    tests off the zero-checksum carve-out and remove it.
1.3 A6: DB-anchored pre_apply verification + contiguity checks + hard error
    on chain break/short restore (both crates); integrity_check before
    success; core restore stops returning Ok on breaks.
1.4 A14 (restore half): restore to temp file + atomic rename after all
    verification.
1.5 A10 (reload half): state.json reload restores wal_salt +
    wal_checksum_chain; transport errors propagate (no Ok(Some) swallowing).
1.6 B7: remove()/shutdown final syncs return Result; drains actually drain.
1.7 A13 (honesty half): periodic validation hard-fails on phantom manifest;
    verify checks snapshot->incremental linkage + cross-file chain; fix exit
    codes. B12: journal-mode change = hard error + webhook.

### Phase 2 — Stop losing data (architecture)
2.1 A3/A4: checkpoint-safety mechanism (pinned-frame blocker + copy-then-
    checkpoint gated on upload durability, and/or rollover => mandatory
    re-snapshot + loud event). Applies to all three modes; poll mode gets a
    snapshot timer or is removed.
2.2 A5: generation-aware shadow sync cursor; never delete un-uploaded
    segments. B4/B5: shadow path uses the checked reader; fix salt/page_size
    seeding.
2.3 A7: core take_snapshot folds WAL + resets cursor (port F11); snapshots
    under read txn / backup API; single-pass checksum-of-uploaded-bytes.
2.4 A8: one canonical key layout + cache dir + prefix across all modes;
    restore/discovery reads it; migration note for old layouts.
2.5 A10 (rest): CAS put in walrust-owned mode; lineage ID in keys/state;
    durable fsynced local progress record; fix external-base wal_offset
    assumption.
2.6 A11: interval-aware contiguous cursor; halt-on-permanent-failure policy;
    re-enqueue failed on restart. A12: fsync file+dir on ack path; verify LTX
    before PUT.
2.7 A9: PIT snapshot selection (latest <= target); decide timestamp PITR vs
    doc fix. A14 (replica half): atomic replica swap; gap => snapshot past
    the gap or hard error with backoff.
2.8 B2/B3: thread running checksum through pull APIs and SnapshotSource.
    B8/B9: page_id=0 guards + truncation-on-shrink + allocation sanity caps
    in all apply paths.

### Phase 3 — Prove it
3.1 E2E round-trip + SIGKILL tests from 0.4 running in CI, both stacks,
    with an external autocheckpointing writer (regression for A1-A5).
    Status: Fixed — `.github/workflows/ci.yml` runs `make test USE_SOUP=0`
    against MinIO, and `tests/production_e2e.rs` now covers CLI watch
    round-trip, CLI SIGKILL restart, core Replicator round-trip, core Replicator
    restart, and core Replicator process-SIGKILL restart through external
    autocheckpointing writer connections. The core cases pin a live WAL frame
    during the flush window so SQLite autocheckpointing cannot erase the
    production frames before the replicator observes them. Proven by
    `e2e_cli_watch_restore_round_trips_sqlite_rows`,
    `e2e_cli_watch_sigkill_restart_round_trips_sqlite_rows`,
    `e2e_core_replicator_restore_round_trips_sqlite_rows`,
    `e2e_core_replicator_restart_reopens_state_and_restores_cleanly`, and
    `e2e_core_replicator_sigkill_restart_round_trips_sqlite_rows`.
    Known-flaky hardening (Phase 2A adversarial review, 2026-07-08): the core
    SIGKILL child polled for its first published WAL frame with a 10s deadline,
    which could trip under heavy sequential-test S3 latency (it passed standalone
    and on re-run). `flush_until_frames` now uses a 30s deadline — it returns the
    instant a frame is published, so this only affects the under-load path and has
    no happy-path cost.
    Phase 3 (2026-07-08): VERIFIED CI-runnable, both stacks. Read the latest green
    run's log (CI run 28938797148, `production_e2e` binary): 10 passed / 0 failed /
    1 ignored in 35.9s, and every E2E in both stacks executes —
    CLI: `e2e_cli_watch_restore_round_trips_sqlite_rows`,
    `e2e_cli_watch_sigkill_restart_round_trips_sqlite_rows`,
    `e2e_cli_watch_racing_checkpoint_no_data_loss`,
    `e2e_cli_watch_independent_snapshot_timer_round_trips_through_reset`,
    `e2e_cli_watch_restore_round_trips_64kb_pages`,
    `e2e_compaction_during_restore_keeps_backup_restorable`; core:
    `e2e_core_replicator_restore_round_trips_sqlite_rows`,
    `e2e_core_replicator_restart_reopens_state_and_restores_cleanly`,
    `e2e_core_replicator_racing_checkpoint_reanchors_without_data_loss`,
    `e2e_core_replicator_sigkill_restart_round_trips_sqlite_rows`. The one
    `ignored` line is the SIGKILL child spawn target — and the SAME log shows both
    spawned phases start (`running 1 test` twice); only the `second` phase prints
    `e2e_core_replicator_sigkill_child ... ok`, because the `first` phase is
    SIGKILLed mid-run by the parent and never reports — exactly the crash under
    test. (Phase-3 review 2026-07-08: re-verified on the branch's own green run
    28941451610 — same 10 passed / 1 ignored, same one-ok/one-killed child shape;
    the earlier wording "running twice as ... ok" overstated the log and was
    corrected, since a first-phase "ok" would mean the SIGKILL missed.)
    Decision: the child STAYS `#[ignore]`d — it is a
    coordination-driven spawn target (blocks on `WALRUST_CORE_SIGKILL_*` env +
    go/ready/flushed file handshakes; hangs/errors run standalone). It is compiled
    into the ordinary test binary (nothing extra to build in CI), the parent
    re-execs it with `--ignored`, and it executes in CI. The `#[ignore]` doc
    comment now states this precisely. No CI gating hole found: nothing is skipped
    in CI, both stacks' E2Es (including both SIGKILL parents) run against MinIO.
3.2 DST drives the production pipeline (not testable.rs) for at least one
    property; restore-from-published-deltas test for phase-4 mode.
    Status: Fixed — `walrust-dst` now has
    `prop_production_published_delta_restore`, which drives
    `walrust-core` production `take_snapshot`, `sync_wal`, object discovery,
    and `restore` against the deterministic `MockStorageBackend`. It verifies
    that published HADBP snapshots and incrementals exist, restore reaches the
    stream head, `PRAGMA integrity_check` passes, and restored rows match the
    source. Proven by
    `invariants::tests::test_prop_production_published_delta_restore` and
    `walrust-dst invariants --invariant production_published_deltas`.
    Phase 3 (2026-07-08): the three remaining 3.2 obligations closed.
    (a) Chaos "crashes" [TODO] REPLACED with a real child-process kill:
    `walrust-dst` re-execs itself as a durable on-disk cache writer, SIGKILLs it
    mid-write, then reopens the cache and proves the committed prefix survived
    byte-for-byte and the manifest is not torn (regression for A12 fsync +
    atomic-rename). Wired into `chaos --faults crashes` / `--faults all`;
    CI-covered by `walrust-dst/tests/crash_recovery.rs`. Spot-checked: neutering
    the cache manifest atomic-rename makes it FAIL (torn JSON on reopen).
    (b) Stress/soak can now FAIL: threshold decisions are typed evaluators
    (`evaluate_stress_result`, `evaluate_soak_result`) that hard-error (non-zero
    exit) on an error-rate / memory-growth / memory-trend / fd-growth breach
    instead of printing `[WARN]` and returning Ok. Unit-proven by
    `threshold_tests::{stress_breach_is_hard_failure, soak_breach_is_hard_failure}`.
    (c) Fenced external (TLM_DELTA) restore-from-published-deltas ADDED:
    `prop_fenced_delta_restore` (`test_prop_fenced_delta_restore`) drives the real
    fenced writer path (`Replicator` external mode + `set_external_delta_base` +
    `sync_wal_fenced_delta` shipping `.tlmd` envelopes) against the deterministic
    `MockStorageBackend`, then plays the follower: `list_delta_envelopes_after` +
    epoch/writer fence + BLAKE3 envelope chain + `apply_changeset_to_db` with the
    running DB checksum threaded from the external base, asserting `integrity_check`
    + exact source-row equality. Self-validating (a base-checksum/chain divergence
    trips the HADBP chain verify). Spot-checked: neutering the dispatch's
    envelope-chain advance makes it FAIL ("envelope chain break at seq N").
    Phase-3 review (2026-07-08): finding — the property only checked the fence
    POSITIVELY (all honest envelopes carry the right epoch/writer); it never
    proved a follower rejects a hostile envelope. FIXED: the follower loop is now
    a fallible `fenced_follower_reconstruct` helper, and after the honest
    reconstruction the property forges an envelope at head+1 (wrong epoch, wrong
    writer, broken chain — each otherwise valid, carrying real LTX bytes and, for
    the epoch/writer cases, the CORRECT chain link) and asserts each is rejected
    by the named fence before apply. Neuter-verified: disabling the epoch fence
    makes the property FAIL (forgery reaches apply and dies with the wrong error).
    Also independently re-verified the author's neuters: chain-advance removal →
    "envelope chain break at seq 4" FAIL; cache manifest atomic-rename removal →
    crash_recovery FAIL. Stress exit path verified end-to-end: forcing the stress
    threshold to breach makes `walrust-dst stress` exit 1 (soak shares the same
    `?`-propagated evaluator tail). 3.3 spot-check: neutering the CAS lineage
    fence + snapshot/state error propagation in `Replicator::add` makes BOTH
    `test_walrust_owned_concurrent_two_watchers_only_one_wins` (2 winners) and
    `test_walrust_owned_enospc_during_add_is_hard_error` FAIL — both are real.
3.3 Compaction-vs-restore race test; two-watchers test; ENOSPC test;
    64KB pages; >100MB DB smoke test.
    Status: Fixed — production and dual-tree coverage now exercises these
    previously untested edges. Root CLI/S3 coverage proves restore remains
    valid while forced compaction runs and proves watch/restore round-trips a
    real SQLite database with 64KB pages. Core Replicator coverage proves
    concurrent walrust-owned watchers race through the active-lineage CAS and
    only one wins, and that an ENOSPC-style storage failure is a hard add
    error that leaves the DB unregistered. Both `src/ltx.rs` and
    `crates/walrust-core/src/ltx.rs` now include 64KB snapshot round-trips and
    real SQLite >100MiB snapshot smoke tests verified by integrity/row/byte
    aggregate checks; `walrust-dst` page-size property now includes 64KB.
    Proven by `e2e_compaction_during_restore_keeps_backup_restorable`,
    `e2e_cli_watch_restore_round_trips_64kb_pages`,
    `test_walrust_owned_concurrent_two_watchers_only_one_wins`,
    `test_walrust_owned_enospc_during_add_is_hard_error`,
    `ltx::tests::test_snapshot_various_page_sizes`,
    `ltx::tests::test_sqlite_snapshot_over_100mb_smoke`, and
    `properties::tests::test_prop_wal_page_sizes`.

### Phase 4 — Converge (larger, optional but recommended)
4.1 Decide the surviving stack (likely walrust-core as the engine, src/ as
    thin CLI over it); delete the duplicate WAL/LTX/sync/restore
    implementations so invariants live in exactly one place.
    Status: Fixed — `walrust-core` is now the canonical WAL, shadow-WAL, and
    legacy Litestream-derived LTX implementation. Root `src/wal.rs`,
    `src/shadow.rs`, and `src/ltx.rs` are compatibility shims over
    `walrust-core`, preserving the root module paths while deleting the
    duplicate root implementations. Before shimming shadow, the missing core
    segment-name regression was reproduced by
    `shadow::tests::test_segment_name_width_keeps_lexical_order_past_u32`
    (failed with 8-hex variable-width names), then fixed by porting the
    16-hex formatter into core. Legacy LTX ownership was then reproduced with
    `legacy_ltx_codec_is_owned_by_core_and_round_trips_real_sqlite` (failed
    because `walrust_core::legacy_ltx` did not exist), then fixed by moving the
    existing production codec into `walrust-core::legacy_ltx` and re-exporting
    it from root. The moved codec keeps its original unit coverage under
    `walrust-core::legacy_ltx`; targeted proof includes
    `legacy_ltx::tests::test_encode_sqlite_snapshot_includes_wal_and_returns_encoded_checksum`
    and `legacy_ltx::tests::test_snapshot_various_page_sizes`. Legacy object
    layout ownership was then reproduced with
    `legacy_ltx_object_layout_is_owned_by_core` (failed because
    `walrust_core::legacy_manifest` did not exist), then fixed by moving the
    pure key-formatting, generation, snapshot-classification, and discovered
    file types into `walrust-core::legacy_manifest`; root `src/sync/manifest.rs`
    now reuses those definitions. Legacy object discovery ownership was then
    reproduced with
    `legacy_manifest::tests::legacy_ltx_discovery_is_owned_by_core_storage_backend`
    (failed because core had no storage-backed legacy discovery API), then
    fixed by moving snapshot selection, generation listing, state discovery,
    and all-file discovery into `walrust-core::legacy_manifest` over
    `hadb_storage::StorageBackend`; root `src/sync/manifest.rs` now delegates
    its S3 discovery wrappers through `hadb_storage_s3::S3Storage`. Legacy
    restore ownership was then reproduced with
    `legacy_restore_is_owned_by_core_and_replays_real_wal_incremental` (failed
    because `walrust_core::legacy_restore` did not exist), then fixed by adding
    storage-backed core legacy restore with latest/PIT snapshot selection,
    gap checks, checksum-checked incremental apply, integrity check, and atomic
    publish. Root `restore` now delegates all restore engine work to
    `walrust-core::legacy_restore`; cache support is a root-only
    cache-over-S3 `StorageBackend` adapter and webhook handling is now error
    notification around the core call, not a second decode/apply path. Legacy
    compaction planning was then reproduced with
    `legacy_manifest::tests::legacy_ltx_compaction_plan_is_owned_by_core_and_rescues_chain_base`
    (failed because core had no reachability-aware legacy compaction planner),
    then fixed by moving retention-plus-live-chain reachability planning into
    `walrust-core::legacy_manifest::plan_legacy_compaction`; root manual
    compact and watch-mode auto-compaction now use that core planner and keep
    only S3 metadata lookup, output/logging, and deletion orchestration. Legacy
    read-replica apply ownership was then reproduced with
    `legacy_replica_engine_is_owned_by_core_and_preserves_live_db_on_bad_incremental`
    and `legacy_replica_engine_bootstraps_snapshot_through_core` (failed
    because `walrust_core::legacy_replica` did not exist), then fixed by moving
    atomic snapshot bootstrap and incremental staged-apply into
    `walrust-core::legacy_replica`; root `src/sync/replicate.rs` now keeps S3
    polling, gap decisions, and local replica state while delegating live-file
    mutation to core. Root production path coverage remains
    `sync::replicate::tests::replica_failed_incremental_apply_preserves_existing_database`
    and
    `sync::replicate::tests::replica_gap_without_future_snapshot_errors_and_preserves_existing_database`.
    Legacy local-cache ownership was then reproduced with
    `legacy_cache_is_owned_by_core_and_persists_pending_ltx` (failed because
    `walrust_core::legacy_cache` did not exist), then fixed by moving
    `LocalCache`, cache manifest persistence, interval-aware durable cursors,
    fsynced cache writes, cleanup, and verification into
    `walrust-core::legacy_cache`; root `src/cache.rs` is now a compatibility
    shim. The moved cache keeps its original unit coverage under
    `walrust-core::legacy_cache::tests::*`.
    Legacy uploader ownership was then reproduced with
    `legacy_uploader_is_owned_by_core_and_uploads_cached_ltx` (failed because
    `walrust_core::legacy_uploader` did not exist), then fixed by moving the
    cache-to-storage uploader, retry handling, corrupt-cache rejection,
    durable cursor advancement, failed-upload shutdown posture, and concurrent
    drain behavior into `walrust-core::legacy_uploader`; root
    `src/uploader.rs` is now a compatibility shim. The moved uploader keeps
    its original unit coverage under `walrust-core::legacy_uploader::tests::*`.
    Legacy shadow-WAL encoding/cache ownership was then reproduced with
    `legacy_shadow_sync_to_cache_is_owned_by_core` (failed because
    `walrust_core::legacy_shadow` did not exist), then fixed by moving shadow
    segment discovery, committed-frame filtering, LTX encoding, storage upload,
    cache write, and uploader notification into
    `walrust-core::legacy_shadow`; root `src/sync/shadow.rs` now keeps retry,
    webhook, S3 metadata, compaction orchestration, and test wrappers while
    delegating the sync engine to core. Root wrapper coverage remains
    `sync::shadow::tests::test_encode*` and
    `sync::shadow::tests::test_sync_shadow_to_cache*`.
    Legacy direct WAL-to-storage ownership was then reproduced with
    `legacy_wal_sync_initial_snapshot_is_owned_by_core` (failed because
    `walrust_core::legacy_wal_sync` did not exist), then fixed by moving
    storage-backed initial snapshot, incremental WAL-frame encoding,
    rollover re-snapshot, WAL-mode fail-closed checks, and object-key
    publishing into `walrust-core::legacy_wal_sync`; root
    `src/sync/wal_sync.rs::sync_wal_concurrent` now adapts the S3 client to
    `hadb_storage_s3::S3Storage` and delegates the direct upload engine to
    core. Root retry/webhook coverage remains
    `sync::wal_sync::tests::test_sync_wal_concurrent_rejects_database_out_of_wal_mode`
    and
    `sync::wal_sync::tests::test_sync_wal_retry_notifies_webhook_when_database_leaves_wal_mode`.
    Legacy cache-mode WAL sync ownership was then reproduced with
    `legacy_wal_sync_cache_initial_snapshot_is_owned_by_core` (failed because
    `walrust_core::legacy_wal_sync::sync_wal_to_cache` did not exist), then
    fixed by moving shadow-backed cache snapshot/incremental encoding, cache
    writes, and uploader notification into `walrust-core::legacy_wal_sync`;
    root `src/sync/wal_sync.rs::sync_wal_to_cache` was left as a compatibility
    wrapper and later deleted after the watched sync-once path moved into
    core. Legacy periodic snapshot ownership was then reproduced with
    `legacy_wal_sync_periodic_snapshot_is_owned_by_core` (failed because
    `walrust_core::legacy_wal_sync::take_snapshot_to_storage` did not exist),
    then fixed by moving checkpointed storage-backed snapshot publication into
    `walrust-core::legacy_wal_sync`; root
    `src/sync/wal_sync.rs::take_snapshot` now adapts S3 and updates CLI state
    from the core output. Legacy manual snapshot command ownership was then
    reproduced with `legacy_manual_snapshot_is_owned_by_core` (failed because
    `walrust_core::legacy_wal_sync::snapshot_database_to_storage` did not
    exist), then fixed by moving the `walrust snapshot` storage-backed
    publication path into `walrust-core::legacy_wal_sync`; root
    `src/sync/compact.rs::snapshot` now keeps only CLI validation and output
    formatting. Legacy watch sync-once ownership was then reproduced with
    `legacy_watch_sync_once_state_machine_is_owned_by_core` (failed because
    `walrust_core::legacy_wal_sync::{WatchedDbState,
    sync_watched_db_once_to_cache}` did not exist), then fixed by moving the
    watched database cursor mutation for cache-mode WAL sync into
    `walrust-core::legacy_wal_sync`; root `src/sync/wal_sync.rs::do_sync`
    now delegates cache-mode state advancement to core and uses the same core
    transition after direct-upload retry success. Legacy shadow progress
    ownership was then reproduced with
    `legacy_shadow_progress_persistence_is_owned_by_core` (failed because
    `walrust_core::legacy_shadow_watch` did not exist), then fixed by moving
    atomic/fsynced `progress.json` save/load and stale-generation validation
    into `walrust-core::legacy_shadow_watch`; root `src/sync/watch_shadow.rs`
    now only adapts `ShadowDbState` into the core progress DTO. Legacy shadow
    checkpoint-drain ownership was then reproduced with
    `legacy_shadow_checkpoint_drain_wait_is_owned_by_core` (failed because
    `walrust_core::legacy_shadow_watch::wait_for_cache_checkpoint_durability`
    did not exist), then fixed by moving the failed-upload/pending-upload
    hard-error gate into `walrust-core::legacy_shadow_watch`; root
    `checkpoint_shadow_after_durable_sync` now calls the core wait helper.
    Legacy multi-DB shadow sync state application was then reproduced with
    `legacy_shadow_multi_db_sync_apply_is_owned_by_core` (failed because
    `walrust_core::legacy_shadow_watch::{ShadowWatchState,
    apply_shadow_sync_results_strict}` did not exist), then fixed by moving
    shadow watch state, shadow-sync input construction, strict result
    application, cursor advancement across drained generations, and progress
    persistence into `walrust-core::legacy_shadow_watch`; root
    `src/sync/watch_shadow.rs` now keeps CLI scheduling, retry/webhook/S3
    adaptation, metrics, and shutdown orchestration while delegating shadow
    engine/state transitions to core.
4.2 Error taxonomy: replace substring classification with typed errors
    end-to-end.
    Status: Fixed — root `src/errors.rs` and core
    `crates/walrust-core/src/errors.rs` now classify only typed
    `WalrustError` values found in the `anyhow` error chain, and untyped
    messages no longer receive category-specific exit statuses from substring
    matches. Root config/database validation now returns typed config/database
    errors, restore no-snapshot/PIT failures return typed restore errors, and
    verify hard-failures return typed integrity errors at the production CLI
    boundary. Proven first by failing root/core
    `errors::tests::test_untyped_messages_are_not_classified_by_substring`
    (plain `anyhow!("Checksum mismatch...")` incorrectly classified as
    integrity), by failing `test_verify_no_backup_found` (printed exit 5 but
    process exited 1), by failing
    `invalid_replicate_interval_exits_with_config_status` (invalid production
    `replicate --interval` exited 1 instead of 2), and by failing
    `missing_restore_backup_exits_with_restore_status` (missing production
    restore exited 1 instead of 6). The fixed tests now pass along with the
    root/core typed classifier tests.
    Second-pass adversarial review found this was only partially landed:
    database and S3 startup failures still collapsed to generic exit 1, core
    restore no-snapshot failures were untyped, and core `Replicator` still
    used `to_string().contains("No snapshot found")`. These were reproduced
    before fixing with
    `missing_snapshot_database_exits_with_database_status`,
    `unreachable_verify_endpoint_exits_with_s3_status`,
    `invalid_replicate_source_exits_with_config_status`, and
    `sync::tests::restore_no_snapshot_returns_typed_restore_error`. The fix
    adds typed `RestoreNotFound`, preserves typed causes through production
    restore/verify/compact/replicate/watch paths, maps explicit S3/database
    startup failures to typed errors, and replaces the core replica
    no-snapshot string guard with a typed `WalrustError` downcast. The
    second-pass tests now pass.

### Phase 4 — Wave 3b (convergence remainder, 2026-07-08)

The Session-8 module convergence (4.1 above) had already landed; Wave 3b
executed the recorded remainder.

- **4.1 (fenced follower → production API).** The fence-enforcing follower
  reconstruction previously lived only as `fenced_follower_reconstruct`, an
  executable spec inside `walrust-dst/src/invariants.rs`. It is now the public,
  documented `walrust_core::reconstruct_fenced_follower` +
  `FencedFollowerCursor`/`FencedFollowerResult` in `crates/walrust-core/src/sync.rs`,
  alongside `list_delta_envelopes_after`. It copies the external base, discovers
  published envelopes, and enforces seq contiguity, epoch fence, writer fence,
  BLAKE3 envelope chain, and byte-identity chain BEFORE applying each payload
  with the running DB checksum threaded through. Every fence rejection is a
  typed `WalrustError::Integrity` (exit 5) whose message still names the fence.
  The DST `prop_fenced_delta_restore` follower now CALLS this production API, so
  spec and implementation have converged. Proven by the non-gated
  `sync::tests::reconstruct_fenced_follower_replays_published_deltas` (drives
  the real `sync_wal_fenced_delta` writer over a real rusqlite WAL and asserts
  integrity + exact row equality) and
  `sync::tests::reconstruct_fenced_follower_rejects_forged_envelopes` (a forged
  head+1 envelope — wrong epoch / wrong writer / broken chain — is rejected with
  a typed integrity error before any apply). The fence is not weakened.
- **4.2 (typed errors end-to-end).** Confirmed already complete from the earlier
  Phase-4.2 wave; the new fence rejections are typed. See the MEDIUM/LOW
  stringly-typed classification Status above.
- **Naming/doc honesty.** See the MEDIUM/LOW naming Status above.
- **WAN flake.** The `production_e2e` poll deadlines are now WAN-tunable via
  `WALRUST_E2E_DEADLINE_SECS` (`e2e_poll_deadline`), removing the fixed 30s/20s/10s
  magic numbers that made the core SIGKILL restart E2E flaky under load. Proven
  by `e2e_poll_deadline_override_parses`.
- **B14.** Closed this wave (see the B14 Status above).

---

## DEFERRED (final register)

### D1 — Writer-lease / split-brain immunity token (from B11)
- **Status:** Out of scope by design.
- **Rationale:** walrust is single-writer. A second writer fails loudly on CAS
  collision; the local `PublishIntent` authorship proof is a durability fence,
  not a distributed lease. A true multi-writer lease/epoch token belongs in the
  external coordinator (the hadb internal-lease-store work), not in walrust.

### D2 — Core walrust-owned first-checkpoint window (from A3)
- **Status:** Fixed.
- **Design note:** walrust-owned mode now holds the same pinned-frame checkpoint
  blocker shadow mode uses (litestream-style): `add()` creates `_walrust_seq`,
  writes a row so the WAL always has a real frame, and opens a long-running read
  transaction. `take_snapshot` / `take_snapshot_with_retry` release the blocker,
  run `wal_checkpoint(TRUNCATE)`, then reacquire it immediately and write a fresh
  `_walrust_seq` row so the new WAL is pinned at once and its salt is recorded.
- **Proving test:** `walrust_core::sync::tests::test_owned_replicator_holds_checkpoint_blocker_after_add`
  drives a real `Replicator::add` and asserts an external `wal_checkpoint(TRUNCATE)`
  from a second connection returns `busy != 0`.
- **Behavior change vs Phase-2A (adversarial review, 2026-07-08):** owned mode
  previously had NO blocker, so the Phase-2A racing E2E
  `e2e_core_replicator_racing_checkpoint_reanchors_without_data_loss` forced an
  actual external TRUNCATE and asserted `busy==0` (the reset happened), then relied
  on rollover DETECTION -> re-snapshot to recover the folded frames. With D2 the
  blocker now REFUSES that external checkpoint (`busy != 0`), so that E2E was
  updated to assert refusal + full round-trip (prevention supersedes recovery in
  steady state). The re-anchor backstop is still load-bearing and is now exercised
  via a walrust-DOWN window by
  `walrust_core::replicator_flush::test_walrust_owned_flush_resnapshots_after_checkpoint_rollover`
  and `..::test_rollover_observer_fires_on_walrust_owned_reanchor` (drop the
  replicator, external TRUNCATE while down, reopen -> re-anchor). Revert-verified.
- **Cost note:** the blocker writes one `_walrust_seq` bookkeeping frame per
  open/reopen and per post-checkpoint reacquire (same as Litestream's
  `_litestream_seq`); this is a single row, not per-frame or per-sync work.

### D3 — Shadow downtime-checkpoint completeness (from B4)
- **Status:** Fixed.
- **Fix:** on restart, when the initial shadow copy detects a WAL salt mismatch
  (external checkpoint while walrust was down), the database is flagged for an
  eager snapshot before the periodic timers start.
- **Proving tests:** `walrust::sync::watch_shadow::tests::test_initial_shadow_copy_detects_downtime_checkpoint`
  (fires on salt mismatch) and
  `walrust::sync::watch_shadow::tests::test_initial_shadow_copy_no_eager_snapshot_without_downtime_checkpoint`
  (does NOT fire when a normal restart appends to the same WAL with no external checkpoint).

### D4 — Rollover CheckpointDetected webhook variant (from A3)
- **Status:** Fixed.
- **Fix:** added a `CheckpointDetected` variant to `hadb-io/src/webhook.rs` in the
  `checkpoint-detected` branch of `hadb`, bumped the pinned `hadb` rev in
  walrust's `Cargo.toml` files, and routed rollover notifications through the new
  variant instead of `upload_failed`.
- **hadb rev bumped from → to:** `c3eab301…` → `1100d3b615743405c676ecefa5743f2c277ea25f`.
- **Proving test:** `walrust::sync::wal_sync::tests::test_checkpoint_detected_webhook_event_is_sent_for_rollover`.

### D5 — A5 drain-at-commit-boundary invariant (from A5)
- **Status:** Fixed.
- **Fix:** `advance_shadow_sync_cursor_if_drained` now checks the last frame of a
  fully-drained shadow generation and returns a typed
  `WalrustError::ShadowGenerationNotAtCommitBoundary` instead of silently
  advancing past a non-commit tail.
- **Proving test:** `walrust_core::legacy_shadow_watch::tests::test_advance_cursor_rejects_generation_tail_not_at_commit_boundary`.

### D6 — B13 cross-generation same-(min,max) cache collision (from B13)
- **Status:** Accepted.
- **Rationale:** a same-`(min,max)` cross-generation cache collision is
  theoretically possible, but it is backstopped by the restore chain's
  `pre/post_apply` checksums (`apply_ltx_to_db_checked`), which are computed from
  actual DB bytes and catch a wrong-lineage substitution downstream. Fixing the
  cache binding to include lineage/etag is future work outside this register.

### D7 — MEDIUM/LOW ergonomics (not on a durability path)

#### D7.1 — Empty config glob matches nothing
- **Status:** Fixed.
- **Fix:** a glob that matches zero databases is now a startup `ERROR`. Added
  `allow_empty_globs = true` to opt out for genuinely optional patterns.
- **Proving tests:** `walrust::config::tests::test_empty_glob_is_error_by_default`,
  `walrust::config::tests::test_empty_glob_allowed_with_opt_in`.

#### D7.2 — CLI clap defaults override walrust.toml
- **Status:** Fixed.
- **Fix:** `resolve_cache_config` now uses `matches.value_source()` to apply CLI
  `cache_retention` / `uploader_concurrency` only when the user actually passed the
  flag, so toml values win over clap defaults.
- **Proving test:** `walrust::config::tests::test_resolve_cache_config_from_config_file`.

#### D7.3 — Corruption webhook lost on restore exit path
- **Status:** Fixed.
- **Fix:** `restore.rs` now awaits the corruption webhook notification (with a
  5-second timeout) before returning, instead of firing it via `tokio::spawn`.
- **Proving test:** `walrust::sync::restore::tests::test_restore_failure_awaits_corruption_webhook_before_returning`.

#### D7.4 — Legacy `sync_wal_and_manifest` path
- **Status:** Fixed.
- **Fix:** deleted `sync_wal_and_manifest`, the `Manifest`/`LtxEntry` structs,
  `load_manifest`, `save_manifest`, and their tests from
  `crates/walrust-core/src/sync.rs`. The remaining manifest helpers in
  `src/sync/manifest.rs` are manifest-free discovery functions only.
- **Proving coverage:** verified by `cargo check` and the continued passing of the
  replicator / manifest convergence tests that previously shared the codebase
  with the deleted path.

## E — User-drill findings (2026-07-09)

A user-realistic end-to-end drill session against `main` (bd3cbde) surfaced eight
findings the spec-driven suite missed. Each is fixed test-first below (one commit
per finding). Proving tests were confirmed to FAIL with the fix reverted at the
call site.

### E1 — Restart-under-load permanently and silently halts replication
- **Repro:** `t10.sh` (200 rows/sec, PID-verified kill/restart of `walrust watch`
  every ~2 min). After a restart every shadow sync failed forever with
  `Shadow sync failed after 7 attempts: Invalid page num: transaction ID must be
  non-zero`. Restore/verify reported success while ~4 min of committed data was
  missing.
- **Root cause:** A `kill -9` mid-write left a partial (torn) frame at the tail of
  the active shadow segment. Shadow segments are a stream of fixed-size frames
  (24-byte header + page_size); `ShadowWal::copy_frames` appends new frames in
  append mode after that torn tail, shifting every following frame off its
  boundary. `encode_shadow_to_ltx` then decodes a header out of a frame's
  zero-padded region, yielding page number 0, which litepages rejects. (Its
  `PageNumError` Display string is mislabeled "transaction ID must be non-zero",
  which is why the drill looked like a TXID-0 bug — it is actually page-0.) The
  "never exits" observation was two concurrent instances (see E5); a single
  instance already propagates the error and exits nonzero.
- **Fix:** (a) `ShadowWal::new` truncates every shadow segment down to a
  whole-frame boundary on startup, dropping any torn tail; the durable
  `wal_copy_offset` re-copies dropped frames cleanly so restarts resume
  correctly. (b) Encode/validation errors are classified permanent and fail fast
  (one attempt) with an error log + webhook instead of spinning the retry budget
  and burying a hard fault under WARN spam.
- **Proving tests:**
  `walrust_core::shadow::tests::e1_shadow_new_heals_torn_tail_so_sync_resumes`,
  `walrust_core::shadow::tests::e1_torn_segment_tail_decodes_as_invalid_page_num`,
  `walrust::sync::shadow::tests::e1_permanent_encode_error_fails_fast_without_spinning`,
  `walrust::sync::shadow::tests::e1_transient_error_still_retries_to_budget`,
  `walrust::sync::shadow::tests::e1_classifies_corrupt_frame_error_as_permanent`.
- **Status:** Fixed. Re-verified with a shortened t10 (see E-drill re-run note).

### E2 — `compact` deletes generations retained PITR points need
- **Repro:** `t5b.sh` — 5+ auto-snapshot generations; `compact --hourly 1 --daily 0
  --weekly 0 --monthly 0 --force` pruned old generations; afterward PITR to a
  pre-compact marked TXID failed ("restore incremental gap: expected next TXID 47,
  got 48-52", exit 6). Latest restore still worked.
- **Root cause:** Taking a full snapshot at TXID H consumes H, so the gen-0
  incremental stream jumps from ..H-1 to H+1.., leaving a single-TXID hole at H
  that only the snapshot at H can fill. The reachability rescue protected only
  one base, so compaction deleted these bridge snapshots and stranded every
  incremental after each hole.
- **Fix:** `plan_legacy_compaction` now rescues every snapshot that bridges a hole
  in the surviving live-incremental chain; `compact` also prints the earliest
  restorable point-in-time so any dropped older window is declared explicitly.
- **Proving test:**
  `walrust_core::legacy_manifest::tests::e2_compaction_rescues_bridge_snapshots_for_retained_pitr_points`.
- **Status:** Fixed.

### E3 — `verify` false-positives "CRITICAL - data may be unrecoverable"
- **Repro:** `t2c_clean.sh` — all restores (including PITR into the "gap")
  succeeded while verify screamed exit 5 after any restart or interval snapshot.
- **Root cause:** The gen-0 continuity check flagged every single-TXID hole a full
  snapshot punches as a critical gap, not understanding that the snapshot
  supersedes it.
- **Fix:** A hole is only reported when no full snapshot sits at the TXID just
  below where incrementals resume — i.e. only when an advertised restore point
  actually depends on the missing range. Extracted into a pure
  `detect_live_txid_gaps` helper, tested both ways.
- **Proving tests:**
  `walrust::sync::verify::tests::e3_snapshot_superseded_holes_are_not_reported_as_gaps`,
  `walrust::sync::verify::tests::e3_unbridged_hole_is_still_a_real_gap`.
- **Status:** Fixed.

### E4 — `restore --point-in-time <far-future>` silently returns latest (exit 0)
- **Repro:** A far-future TXID returned the latest DB with exit 0; `--point-in-time
  0` correctly errored.
- **Fix:** `restore_legacy_ltx` rejects a point-in-time beyond the newest available
  TXID with a hard error naming the max available.
- **Proving test:**
  `walrust_core` integration `e4_far_future_point_in_time_is_a_hard_error_naming_the_max`.
- **Status:** Fixed.

### E5 — No guard against two concurrent instances on the same DB
- **Repro:** Supervisor double-start; drills reproduced severe unrecoverable
  corruption with two live instances (and contaminated the t10 run).
- **Fix:** New advisory `flock` on `.walrust-<db>.lock` next to the DB, taken by
  watch (both modes) and replicate and held for the process lifetime; a second
  same-host instance fails fast with a clear message. Same-host guard only,
  matching the single-writer design.
- **Proving tests:** `walrust::lock::tests::e5_second_lock_on_same_db_fails_fast`,
  `e5_lock_released_on_drop_allows_reacquire`, `e5_lock_path_is_next_to_db`.
- **Status:** Fixed.

### E6 — `walrust snapshot` while `watch` runs fails with a confusing `busy=1`
- **Fix:** `snapshot` detects a watcher owns the DB via the E5 lock and returns an
  actionable error (snapshots are taken by the watcher; use its interval or stop
  it first) instead of the raw "snapshot checkpoint incomplete (busy=1...)".
  README note added.
- **Proving test:**
  `walrust::sync::compact::tests::e6_snapshot_reports_actionable_error_when_watcher_holds_lock`.
- **Status:** Fixed.

### E7 — `allow_empty_globs = true` still hard-fails when all globs are empty
- **Fix:** With the opt-in, zero matched databases starts and idles with a warning
  instead of exiting 2; without it the empty case still errors. Code + README now
  agree.
- **Proving tests:**
  `walrust` (bin) `tests::e7_allow_empty_globs_resolves_to_zero_databases_without_error`,
  `tests::e7_empty_globs_without_opt_in_still_errors`.
- **Status:** Fixed.

### E8 — Docs (one commit)
- **Fix:** (a) `--help` no longer claims "Litestream-compatible LTX format" — it
  names the HADBP changeset format the code uses. (b) The unreproducible README
  memory table (23–31 MB vs 36 MB) is replaced with the measured ~7–10 MB RSS for
  both tools plus a workload-dependence caveat. (c) Added an S3 request-volume
  section documenting the ~1s `wal-sync-interval` cost tradeoff (~9x more PUTs than
  Litestream batching in drills) and the knobs that tune it.
- **Status:** Fixed.

#### E-drill re-run note
Shortened t10 re-run against the rebuilt release binary (200 rows/sec, 3x
60-second cycles = 2 PID-verified kill/restart cycles under load):
- `Invalid page num` occurrences in watch.log: **0** (was: permanent spam after
  the first restart).
- No ERROR lines; replication resumed cleanly across both restarts.
- restore exit 0, `PRAGMA integrity_check` ok, chain intact through TXID 1553.
- verify exit **0** (E3: no false CRITICAL).
- ground-truth rows 38156, restored 38070 — the 86-row delta is the sub-second
  in-flight tail before the final `kill -9` (~0.4s at 200/s), i.e. the normal
  async recovery-point window, not the multi-minute halt the bug caused.

## E9 — Rollover publish-failure window (proptest-harness bonus finding, adjudicated)

Raised by the proptest state-machine harness (PR #23) while its oracle was being
calibrated, and flagged in the PR for product-side eyes. Adjudicated here during
adversarial review of that PR.

- **Observation.** `upload_rollover_snapshot`
  (`walrust-core::legacy_wal_sync`) folds the WAL into the local `.db` file with
  `checkpoint_wal_truncate` **before** it `storage.put`s the rollover snapshot
  (`legacy_wal_sync.rs:622` truncate, `:645` put). If that put fails (e.g. a
  transient storage error), the committed rows are already folded into the local
  DB file but not yet in remote storage. The next sync tick can then read an
  empty WAL and honestly report `Ok(0 frames)` while those folded rows are still
  unpublished. The harness surfaced this via the shrunk sequence
  `[Mark, KillRestart…, Mark, RestoreLatest]` under a transient fault plan.

- **Adjudication: NOT a silent-loss bug. Correctly-handled durability-ack
  subtlety / the known single-node async-replication boundary.** Grounded in the
  code:
  1. On a failed rollover publish, `sync_wal_to_storage` returns `Err`;
     `sync_watched_db_once_to_storage` only calls
     `apply_sync_output_to_watched_state` on `Ok` (`legacy_wal_sync.rs:94-96`).
     The in-memory cursors (`wal_offset`, `current_txid`, `wal_salt`) do **not**
     advance past the unpublished rows.
  2. `current_txid` (the durability cursor) advances **only** when
     `frame_count > 0` (`apply_sync_output_to_watched_state`,
     `legacy_wal_sync.rs:79-81`). The empty-WAL early return has
     `frame_count: 0` and `new_current_txid: input.current_txid`
     (`:239-250`): **no `Ok(0)` tick ever acknowledges those rows as durable.**
  3. The folded rows are durably in the local `.db` file. Because state did not
     advance, the next tick re-detects the rollover (`size_rollover`: current WAL
     size < the stale `wal_offset`, or a salt mismatch) and re-runs
     `upload_rollover_snapshot`, which re-folds (idempotent) and re-encodes a
     **full-DB** snapshot — now containing the folded rows — and publishes it.
     On a full restart the same rows survive in the local `.db` and are
     republished by the D3 eager-startup snapshot.
  4. So the rows are **recoverable on the next successful tick or restart**: they
     leave the gen-0 incremental *stream* momentarily but are captured by the
     next snapshot/rollover. They are lost only under the universal single-node
     boundary — the local disk is destroyed before any successful publish —
     which is true of every not-yet-replicated write and is not widened here,
     precisely because no `Ok` falsely signals them durable.

- **Harness verdict.** The oracle models this soundly and conservatively: an
  `Ok(0)`-with-pending flush confirms nothing (`Harness::durable_flush`,
  `confirmed = frame_count > 0 || checkpoint_detected || !pending_at_entry`), so
  `durable_len` does not advance during the window and catches up exactly when
  the rows actually ship (the next rollover returns `frame_count:1`,
  `checkpoint_detected:true`). `RestoreLatest`'s exact-equality check therefore
  holds at every point in the window — no false accept, no false alarm.

- **Product change:** none. The order (truncate-before-put) could be reversed as
  a defensive narrowing of the window, but it is not a correctness defect and
  touching the critical path is out of scope for a test-harness PR (whose
  `git diff origin/main -- crates/ src/` must stay empty). Registered as a known
  residual, adjacent to D2 (core walrust-owned first-checkpoint window).

- **Harness coverage added (this review).** The E4 far-future-PITR guard in
  `restore_legacy_ltx` (a fixed product bug, see E4) was **not** exercised by the
  generated sequences: a `Mark` only ever names a real published boundary at or
  below the head, so the fuzz never requests a point-in-time beyond the newest
  TXID. Pinned it with `replay_e4_future_pit_is_typed_not_found`
  (`walrust-dst/src/state_machine.rs`), verified test-first (fails with the E4
  branch reverted — the restore then silently returns `Ok` at the head).

## E10 — Prune-foreclosed PITR crashes with a bare chain gap when a transient LIST hits restore's decay classifier

Found by the DST fault phase (`state_machine_generated_sequences_under_faults`)
during PR #34 review, and again on post-merge `main` CI with a second shape. The
suite is `walrust-dst`'s legacy state machine driving the REAL prune planner and
restore engine over the mock bucket under a seeded transient-fault plan.

- **Repro (both pinned in `walrust-dst/proptest-regressions/state_machine.txt`):**
  - seed 8666: `[RestoreLatest, RestoreLatest, KillRestart, Mark, KillRestart,
    Prune { minimum: 2 }, RestoreLatest, RestorePit(mark 0), RestoreLatest]`,
    `transient_rate: 0.05` → `op[7] RestorePit(txid 3) FAILED (NON-typed error,
    e.g. a chain gap; reachable=false; surviving snapshots {1,2,3,4}, deleted
    {2,3}): … restore chain gap: no backup object continues the chain at seq 2;
    the nearest restorable point at or below the target is seq 1`.
  - seed 439865 (post-merge `main` CI run 29142314079): `[Snapshot, Mark,
    WriteTxn { rows: 1 }, Flush, Prune { minimum: 2 }, RestorePit(mark 0),
    RestoreLatest]`, `transient_rate: 0.05` → `op[5] RestorePit(txid 2) FAILED
    (… reachable=false; surviving snapshots {1,2,3}, deleted {2}): … restore
    chain gap: no backup object continues the chain at seq 2; nearest restorable
    point at or below the target is seq 1`.
  - The failure is nondeterministic per run (fresh proptest cases each run), so
    `main` CI was intermittently red on this bug.

- **Root cause (adjudicated (b): a decision made from a listing that silently
  defaulted to empty on a transient fault — NOT (a) a prune protected-set hole).**
  Both shapes are the SAME cause. Each `KillRestart`/`Snapshot` re-anchors with a
  full-DB base snapshot in a fresh generation, so a marked point can sit on a
  snapshot boundary (e.g. the mark at txid 12/2). Prune LEGITIMATELY forecloses
  such a point per retention: it deletes the snapshot at the mark's boundary while
  a LATER full snapshot survives (a full snapshot is the ultimate compaction — the
  history it absorbs is restorable only at the snapshot's own boundary). That
  foreclosure is by design and is meant to surface as the TYPED decay outcome
  `PlanError::PitrInsideSnapshotSpan` → `WalrustError::RestoreNotFound`, which the
  oracle accepts for an unreachable mark. `restore_legacy_ltx` decides gap-vs-decay
  by re-listing snapshots (`discover_legacy_snapshots`) and asking whether a later
  snapshot absorbs the target. That LIST was swallowed with `.unwrap_or_default()`:
  under a transient fault it returned an EMPTY snapshot set, so "a later snapshot
  absorbs the target" read false, the typed decay error collapsed into a bare,
  non-typed `PlanError::ChainGap` (`WalrustError::restore`), and — because the
  transient had been rewritten into a non-transient message — the caller's retry
  wrapper never re-ran the restore. A destructive-looking classification decision
  (is this point recoverable-at-a-coarser-grain, or a genuine gap?) was made from
  an incomplete view. Confirmed directly by instrumenting the error path: the
  failing replay prints `disc_ok=false disc_len=Err(Service unavailable
  (injected))`, while every passing case prints `disc_ok=true`.

- **Fix (`crates/walrust-core/src/legacy_restore.rs`):** propagate the
  `discover_legacy_snapshots` error in restore's gap-refinement path (`.await?`
  instead of `.await.unwrap_or_default()`). A transient LIST now surfaces as the
  transient it is, so the caller retries the whole restore against a complete
  listing and the point resolves to the typed decay outcome; a genuine LIST
  failure aborts loudly instead of masquerading as a chain gap. Destructive /
  irreversible-classification operations must never decide from a listing that
  could be incomplete.

- **Proving tests (fail-on-revert):**
  - `walrust-dst` `state_machine::tests::replay_e10_foreclosed_mark_under_transient_list_fault`
    — deterministic replay of BOTH seeds (8666, 439865). Reverting the `.await?`
    to `.await.unwrap_or_default()` reproduces the exact "restore chain gap" the
    oracle forbids (verified).
  - The two corpus lines are pinned permanently in
    `walrust-dst/proptest-regressions/state_machine.txt`.

- **Sibling sweep.** `PROPTEST_CASES=256` full fault phase run multiple times:
  the E10 class (`state_machine_generated_sequences_under_faults`) is green every
  run. The sweep separately surfaced an UNRELATED pre-existing bug in the
  compaction merge engine (`compaction_state_machine_generated_sequences_under_faults`,
  seed 477981: a transient fault during a merge leaves a merged object, and a
  later `Compact` plans an overlapping target range —
  `CompactionError::OverlappingExisting`). Different subsystem, different root
  cause, not touched by this fix; filed as a separate follow-up.

- **Status:** Fixed.

- **Adversarial review (PR #36).** Propagation path verified end-to-end: the
  DST's `restore_to` retry classifies by the rendered chain (`{err:#}` contains
  `Service unavailable (injected)`), and the bare `.await?` preserves the chain
  untouched, so the transient is retried and resolves to the typed decay
  outcome. In production the LIST error is already typed `WalrustError::S3` at
  the storage adapter, and `classify_or_else(e, WalrustError::restore)` at the
  CLI boundary preserves an existing typed error — no double-wrapped
  "restore error: restore error:", exit code 4 (S3) as a transport failure
  should. The review then swept the workspace for the same class (a listing /
  transport error swallowed into a default that feeds a CLASSIFICATION or
  DELETION decision) and found the MODERN `sync::restore` had the bug twice:
  - the `leveled` check (`list_merged_ranges(..).await.unwrap_or_default()`):
    a transient levels LIST classified a compacted bucket as un-leveled and
    walked the linear path off the deleted L0 tail — a bogus non-typed
    "incremental gap" (or, with L0 fully folded, a SILENT short restore to the
    snapshot seq);
  - the decay refinement (`discover_latest_snapshot_in_namespace(..).ok()`):
    the exact E10 collapse, byte-for-byte the same "restore chain gap" message,
    in the modern path.
  Both fixed by propagation; proven test-first in
  `crates/walrust-core/tests/compaction_restore.rs`
  (`leveled_check_transient_list_failure_propagates_not_misread_as_unleveled`,
  `decay_refinement_transient_list_failure_propagates_not_bare_gap` — both fail
  with the pre-fix code, reproducing the misclassifications above). Also fixed
  `src/sync/prune.rs` level-prune planning, which swallowed a failed levels
  LIST into an empty DELETION plan and misreported "Nothing to delete"
  (conservative direction, but a persistent LIST failure would silently leak
  level objects forever). Cleared as safe: `src/sync/verify.rs`'s two
  `list_merged_ranges(..).unwrap_or_default()` uses (deliberate, documented
  fail-toward-alarming in a non-destructive report path — a transient can only
  cause a false alarm, never silence), and the `Option`-default sites
  (`wal.rs` chain_seed, `shadow.rs` file_stem, `main.rs` cache config).
  Noted as a follow-up, not fixed here: `src/sync/watch_shadow.rs` treats ANY
  manifest GET failure (transient included) and any manifest parse failure as
  "fresh database, txid 0" at startup; durable local shadow progress overrides
  it when present and publishes are `put_if_absent`-guarded (fail-loud, not
  silent fork), but the startup classification is the same shape and deserves
  its own pass.
