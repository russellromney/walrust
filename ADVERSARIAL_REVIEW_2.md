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

### A6 — Restore chain verification is self-referential; gaps/wrong-lineage apply cleanly as success
Status: Fixed — root restore is proven by
`restore_rejects_incremental_without_prior_chain_link`; core restore is proven
by `sync::tests::restore_errors_on_noncontiguous_incremental_sequence`. The
production restart E2E now asserts fail-closed behavior with
`e2e_core_replicator_restart_rejects_divergent_chain` until A10's state reload
fix removes that divergent lineage.

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

### A9 — PIT restore can only use the newest snapshot; GFS retention is dead weight
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
Status: Partial — Phase 1.5 reload-half fixed in core. Saved `state.json`
now round-trips `wal_salt` and `wal_checksum_chain`, and read/parse failures
propagate instead of becoming a cold start. Proven by
`test_walrust_owned_reload_restores_saved_wal_salt`,
`test_walrust_owned_reload_restores_saved_wal_checksum_chain`, and
`test_walrust_owned_reload_state_transport_error_is_hard_error`. The root CLI
watch path has no equivalent remote `state.json` reload path; its durable
progress gap plus walrust-owned CAS/lineage/fencing remain open for Phase 2.5.

- `state.json` save/load asymmetry: `save_state` persists `wal_salt` +
  `wal_checksum_chain` (`crates/walrust-core/src/sync.rs:258-267`) but reload
  (`replicator.rs:300-327`) never reads them back => after every restart,
  salt-rollover detection and frame validation are OFF (compounds A1–A3).
- `if let Ok(Some(data))` (`replicator.rs:300`) swallows transport errors as
  "no saved state" => cursor reset (the repo fixed this exact class for
  load_manifest; same discipline needed here).
- Walrust-owned mode: blind `storage.put()` (no CAS, `sync.rs:472-474`); seq
  re-seeded from the SQLite change counter on every `add()`
  (`replicator.rs:222-227`), which barely moves in WAL mode => routine restart
  overwrites the previous run's objects with a divergent lineage; two
  instances on one prefix silently interleave. No lease/fence (external mode
  has CAS + epoch fencing; walrust-owned has none). No lineage/generation ID
  anywhere in keys => stream resets are undetectable by replicas
  (`replicate.rs:141-170` stalls forever, silently, if S3 is re-seeded).
- Sidecar watch path persists nothing: `current_txid` in memory only;
  production never writes `manifest.json`; restart re-mints TXIDs and
  overwrites remote history (`src/sync/watch_shadow.rs:103-111`).
- `initialize_external_base_state` (`sync.rs:557`) sets
  `wal_offset = current WAL size` — assumes every WAL byte was published;
  after a crash between commit and publish those transactions are skipped
  forever. `unwrap_or(0)` swallows I/O errors.
- Fix: symmetrical state round-trip; transport errors propagate; CAS
  (`put_if_absent`/`put_if_match`) in walrust-owned mode; a lineage ID minted
  at bootstrap and embedded in keys + state; one durable fsynced local
  progress record written only after durability events.

### A11 — Failed uploads create permanent holes; pipeline ships around them; durable cursor is broken
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
Status: Partial — restore-half fixed in root and core. Root production restore
is proven by `failed_restore_preserves_existing_output_database`; core restore
is proven by `sync::tests::restore_failure_preserves_existing_output_database`
and
`sync::tests::restore_with_snapshot_source_failure_preserves_existing_output_database`.
Replica in-place apply/bootstrap remains open for the Phase 2 replica-half.

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
- B2 — `pull_incremental` / `pull_incremental_into_sink` re-anchor the chain
  from `None` every call (`sync.rs:1346, 1510`): a steady-state follower
  pulling 1 changeset/poll never verifies anything (F13 holds only
  intra-batch). API must accept/return the running checksum.
- B3 — `restore_with_snapshot_source` applies the first incremental unverified
  (trait provides no anchor checksum, `snapshot_source.rs:40`,
  `sync.rs:1087-1135`) and returns Ok on a later chain break. Use
  `discover_strict_physical_chain` + add a checksum to the trait.
- B4 — Shadow copy path uses the unvalidated reader `read_frames_as_pages`
  (`src/shadow.rs:198-199`; also `crates/walrust-core/src/shadow.rs:182`):
  no salt/checksum validation on the DEFAULT path; torn/stale frames shipped
  as commits. Route through the checked reader.
- B5 — Shadow WAL salt seeded `(0,0)` when no WAL exists at startup and only
  updated inside the rollover branch (`src/shadow.rs:83-86, 184`): for a
  fresh DB, all future checkpoints are invisible. Also page_size defaults to
  4096 and is never refreshed => misaligned parsing for non-4096 DBs whose
  WAL appears after start.
- B6 — Poll-mode sync trigger is "WAL grew" only
  (`watch_independent.rs:449-455`): TRUNCATE/RESTART resets never fire a
  sync; unbounded RPO on low-write DBs.
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
- B8 — `apply_changeset_to_db` / `pull_incremental` accept `page_id = 0` =>
  u64 underflow offset (`crates/walrust-core/src/ltx.rs:170`,
  `sync.rs:1121, 1381`) — F4 guard landed only in decode_to_db. Also no file
  truncation on shrink (VACUUM leaves stale tail pages; snapshot then derives
  num_pages from the inflated size).
- B9 — Untrusted-size allocations: `page_size`/`max_page` magnitude unchecked
  (`crates/walrust-core/src/ltx.rs:87-91`; page_size never checked for
  0/pow2/<=65536); `hadb-changeset` `Vec::with_capacity(page_count)` with
  untrusted count. DoS on crafted objects.
- B10 — Snapshot TXID cursor claims WAL-resident commits the raw file copy
  doesn't contain (`sync.rs:913-919` counts WAL commits; binary path PASSIVE
  may not backfill) => transactions in neither snapshot nor any post-snapshot
  incremental.
- B11 — Crash window between changeset put and save_state => same-seq
  re-publish with different bytes (blind put overwrites; live followers hit a
  chain break misdiagnosed as stale lineage) (`sync.rs:473-493`).
  External-mode variant: lost PUT response => permanent equivocation wedge
  with no re-anchor path (`sync.rs:441-470, 684-717`).
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
- B13 — Restore cache substitution keyed by bare TXID, no lineage/etag binding
  (`restore.rs:108-118`); NO_CHECKSUM litestream files skip even internal
  checks.
- B14 — `list_delta_envelopes_after` never asserts payload.seq == key-derived
  seq (`sync.rs:748-767`); genesis sentinel ambiguity (empty vs 32 zero
  bytes) in `external_delta.rs`.

## MEDIUM / LOW (abbreviated)

- Config glob matching nothing is a warn+skip, not an error
  (`config.rs:314-317`).
- CLI clap defaults silently override walrust.toml values
  (`main.rs:585-595`).
- Stringly-typed error classification everywhere
  (`e.to_string().contains(...)` in `sync.rs:1015`, `replicator.rs:432`,
  `errors.rs:114-183`); printed exit codes wrong (`verify.rs:375-386`).
- Corruption webhook fired via tokio::spawn on the exit path — usually lost
  (`restore.rs:129`).
- Legacy `sync_wal_and_manifest` grows manifest unboundedly (`sync.rs:1275`).
- Naming: `ltx.rs` in core is HADBP, not LTX; `external_delta.rs` calls HADBP
  payloads "raw LTX bytes"; `WAL_MAGIC_BE/LE` names encode the A1 inversion.

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
- Phase-4 external-delta mode: publish semantics well tested; NO
  restore-from-published-deltas test.
- Untested: compaction-vs-restore races, two watchers on one DB, ENOSPC,
  DBs > ~2MB, 64KB pages, timestamp PITR.

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
    `e2e_core_replicator_restore_round_trips_sqlite_rows`, and
    `e2e_core_replicator_restart_rejects_divergent_chain`. Note: a stricter
    CLI restart variant that writes rows while/down after restart reproduced
    missing restored rows; the core restart variant now hard-errors on the
    divergent chain after A6. Leave those as A10/A11 evidence rather than
    masking them in Phase 0.

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
3.2 DST drives the production pipeline (not testable.rs) for at least one
    property; restore-from-published-deltas test for phase-4 mode.
3.3 Compaction-vs-restore race test; two-watchers test; ENOSPC test;
    64KB pages; >100MB DB smoke test.

### Phase 4 — Converge (larger, optional but recommended)
4.1 Decide the surviving stack (likely walrust-core as the engine, src/ as
    thin CLI over it); delete the duplicate WAL/LTX/sync/restore
    implementations so invariants live in exactly one place.
4.2 Error taxonomy: replace substring classification with typed errors
    end-to-end.
