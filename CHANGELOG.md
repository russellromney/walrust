# Changelog

All notable changes to walrust will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Safe walrust-owned resume after restore:** `sync::restore` now returns an
  opaque `RestoreResult`, and `sync::resume_owned_after_restore` consumes that
  identity to publish a fresh walrust-owned snapshot above the restored tip.
  The checksum and lineage stay inside walrust; embedders cannot accidentally
  route an owned base through the external-base protocol. The API rejects PITR,
  prefix, database, path, and non-fresh-state mismatches, and documents that the
  caller must hold exclusive writer ownership before re-anchoring. The API now
  requires an `OwnedResumeLease` guard supplied and maintained by the embedder;
  walrust does not acquire, renew, or release leases itself.
- **Format-stability fixture (dogfooding item 1)**: two frozen buckets written by the PUBLISHED 0.7.0 artifacts live under `tests/fixtures/format-stability/` — `cli-v0.7.0` (crates.io `cargo install walrust --version 0.7.0 --locked` binary running `watch --independent-tasks` with leveled compaction at comedy knobs: ≥2 snapshots, live L0 tail, populated L1 AND L2, and a real `walrust prune` boundary with superseded objects actually deleted — the interleaved litestream-heritage-LTX ⟷ HADBP `levels/` seam frozen on disk) and `owned-v0.7.0` (registry `walrust-core = "=0.7.0"` via a throwaway scratch generator crate, `add_without_snapshot()` + autonomous snapshots + compaction: snapshots + levels + L0 tail). Each carries a `MANIFEST.json` (generator version, exact knobs, expected latest row-count/row-content SHA-256, one mid-history PITR TXID with its expected checksum, full object index) plus `generate.sh` (manual-only, needs crates.io network like `drills/version-skew.sh`) to mint future `vX` fixtures the same way. The S3-gated proving test `tests/format_stability.rs` uploads each fixture to a unique scratch prefix and drives the SAME restore path a real user uses (CLI binary for the CLI fixture; library `Replicator::restore()`/`sync::restore()` for the owned one), asserting restore-to-latest + PITR row-exact against the manifest and `integrity_check` clean — buckets written by 0.7.0 must restore forever. Skips ONLY on missing S3 env; a missing/corrupt fixture with S3 present FAILS loudly. Failure-proven at the call site: a one-byte tamper in a level/L0 object fails with a HADBP checksum mismatch (both fixtures), and an un-uploaded (empty) prefix fails loudly (both restore paths), never a skip or a pass. Review hardening (same PR): adversarial review found that restore-to-latest/PITR plan forward from the newest snapshot ≤ target, so level objects below it (cli L2; owned L1/L2 — pre-PITR history) were frozen but never decoded — a tampered byte in them passed. Closed three ways, each proven by a tamper-neuter: every HADBP payload in both fixtures (all `levels/` objects + every owned `.hadbp`) must decode with the current checksum-verifying decoder; the cli bucket must pass current `walrust verify` (downloads + checksum-verifies every real-LTX object, exercises the prune-/level-aware gap logic); and the owned fixture is PITR-restored through each 0.7.0-written L1 merged object (targets derived from the manifest: snapshot base at `min-1`, target `max`), with the restored rows required to be a strict non-empty prefix of the manifest-anchored latest rows. `load_manifest` now also fails on manifest↔disk drift (objects on disk the manifest does not list).

### Fixed

- **DF1 — `walrust watch` no longer dies on an ephemeral-connection writer
  (WAL unlink/recreate race).** SQLite deletes the WAL when the last connection
  on a WAL database closes and recreates it on the next write, so a writer that
  opens one connection per statement (shell scripts, cron jobs, the `sqlite3`
  CLI) routinely leaves the WAL missing / zero-length / zeroed-header. The
  published 0.7.0 binary read that zeroed transient and the watch process
  **exited**: `Shadow copy failed: Invalid WAL magic number: 0x0`. Watch is now
  lifecycle-aware: `wal::read_header_classified` distinguishes a missing /
  too-short / all-zero header (legal last-close states) from a NONZERO garbage
  magic (still a loud error — corruption, kept load-bearing). The shadow copy
  treats the legal states over an established read cursor as a **re-anchor
  trigger** — loud WARN naming the event, then the same eager snapshot the D3
  downtime-checkpoint path takes — never process death, never a silent skip.
  The independent-tasks direct path (`sync_wal_to_storage`) re-anchors the same
  way (rollover snapshot). Watch-side only; restore-path WAL-header strictness
  is unchanged. Proven by `wal::tests::df1_read_header_classified_*`,
  `shadow::tests::df1_*`, `watch_shadow::tests::df1_initial_shadow_copy_*`,
  `legacy_wal_sync` `df1_independent_sync_*`, and the live-S3
  `e2e_cli_watch_survives_ephemeral_connection_writer`; the corruption pins keep
  a nonzero garbage magic failing loudly. Revert-proven (neutered at both
  decision points → the exact 0.7.0 error).
- **DF2 — `walrust watch` no longer silently stops replicating an
  ephemeral-connection writer after the startup snapshot's checkpoint
  (silent-wrong-data).** After the on-startup snapshot's PASSIVE checkpoint
  fully backfills the WAL, an ephemeral writer folds every subsequent commit
  straight into the `.db` and deletes/truncates the WAL before the 1 s poll can
  read a single frame — so the WAL-based copy shipped **nothing** and a restore
  silently returned only the day-one snapshot (no error, `verify` exit 0). The
  shadow copy now **re-arms** the re-anchor on every tick while the WAL stays
  absent-after-data, and the watch loop publishes a fresh snapshot whenever the
  `.db` content checksum advanced (an *idle* DB whose WAL merely churns is
  skipped via the same checksum — SQLite's file change counter is unreliable in
  WAL mode, hence a content checksum). Note the guard only debounces *unchanged*
  content: a large DB written by a busy ephemeral-connection writer can take up
  to one full snapshot per poll tick — see ROADMAP residual R5 for the cost
  bound. Independent-tasks mode gets
  the same re-anchor (`maybe_reanchor_ephemeral_writer`, both cache and no-cache
  paths). Proven by `shadow::tests::df1_missing_wal_after_observed_frames_reanchors`
  (re-arm) and the live-S3 `e2e_cli_watch_replicates_short_lived_writes_after_startup_checkpoint`
  + `e2e_cli_watch_independent_replicates_ephemeral_writer`; both e2es
  deliberately cross the startup passive-checkpoint boundary and, with the fix
  neutered, restore the WRONG (day-one-only) rows — the silent-stall — while the
  fix restores row-exact.
- **`sync::restore` is `Send` in spawned tasks:** compaction restore prefetch now
  owns each planned candidate instead of retaining borrowed plan entries across
  the async stream, fixing the non-general `Send` future seen by embedders using
  `tokio::spawn`.
- **Snapshot page-layout corruption:** walrust-owned snapshots no longer use
  `VACUUM INTO`. Vacuum can renumber b-tree and overflow pages; later WAL frames
  from the live database then target the wrong physical pages in the restored
  vacuumed image. Snapshots now encode the exact checkpointed main file while
  the checkpoint blocker pins it. Wide-row/blob E2E coverage caught the old
  path producing an invalid overflow chain and dozens of orphan pages.
- **Owned-resume conflict and retry safety:** resume now refuses an already
  published next sequence across snapshot, incremental, and compacted storage,
  checks sequence overflow, and completes all remote preflight before mutating
  `SyncState`. A transient preflight failure therefore leaves the same fresh
  state retryable. Documentation now reserves `add_without_snapshot` for
  reopening the same local database/WAL files rather than newly restored files.
- **Restore I/O regression:** `RestoreResult` reuses the checksum already
  produced by linear or leveled restore instead of hashing the entire restored
  database again. The public compaction executor keeps its existing `u64`
  return type while the internal executor retains the final chain checksum.

### Testing

- Added public-API integration coverage for flat and lineaged owned histories,
  leveled-compaction restore/resume, mixed DDL/INSERT/UPDATE/DELETE/blob loads,
  PITR refusal with no storage writes, competing-writer refusal with byte-exact
  preservation of the winner, transient preflight retry, and the
  snapshot-upload/state-save crash window. Added the same mixed-workload
  restore/resume/restore path against live S3-compatible storage. A revoked
  lease is rejected before storage access or `SyncState` mutation, and lease
  expiry during the final state PUT is detected before the API can return
  success while leaving the published snapshot/state recoverable. Lease
  validity is also checked inside every retried snapshot and state PUT attempt,
  so retry backoff cannot silently outlive the caller's lease — proven by a
  retry test that fails the first snapshot CAS transiently, lapses the lease
  during backoff, and asserts the retried attempt refuses before writing any
  object. The snapshot-published/state-save-failed crash window's documented
  recovery (restore again, resume again above the recovered tip) is exercised
  end to end.

### Removed
- Removed the adversarial-review ledgers (`ADVERSARIAL_REVIEW.md`, `ADVERSARIAL_REVIEW_2.md`) — dev artifacts, fully resolved except three residuals now tracked as R1–R3 in ROADMAP.md's "Residual risk register" (multi-writer lease out of scope by design; cross-generation cache collision backstopped by restore chain checksums; rollover truncate-before-put publish window, adjudicated not silent loss). Full ledgers remain in git history.

## [0.7.0] - 2026-07-10

### Added
- **Compaction e2e gap closure — gap 5, CLI prune on a leveled bucket**: extends `drills/prune-retained.sh` with a leveled phase (on its own database, so it can't inherit the flat phase's old snapshots): a real compacting `walrust watch --independent-tasks` (aggressive batches) runs until **both** L1 and L2 fire, `walrust prune` runs against the resulting bucket, and a before/after level-object listing is checked directly against the watermark rule in `crates/walrust-core/src/compaction/prune.rs` — a level object whose whole range ends below the oldest surviving snapshot's TXID must be deleted unless it's the newest object at its level; nothing at or above the watermark, and never the newest-per-level object, may be deleted. Restore-to-latest and one retained PITR point (hand-recorded, exact known row count, deliberately captured without ever restarting the watcher — see the gap-3 note on why) both stay row-exact afterward. Found a non-obvious, non-bug retention-policy interaction along the way (see the ROADMAP residue entry): hadb-io's `RetentionPolicy` has an unconfigurable `minimum: 2` floor that, for any test whose whole run fits inside one real clock hour, always pads the keep set with the single OLDEST snapshot — permanently anchoring the watermark at the very start of history and making the rule untestable. Worked around directly and safely once compaction has folded well past the early history (deleting every snapshot except the newest two plus the hand-recorded PITR target). Also fixed the ROOT of a shared-helper hazard surfaced by needing a second database in one drill run: `drills/lib.sh`'s `pause_driver`/`driver_count`/`wait_driver_count_at_least` were hard-coded to `$DRILL_DB` (an implicit one-database-per-run assumption). They now take an optional database-path argument (default `$DRILL_DB`, so every single-database consumer is unchanged); the leveled phase passes `$LEVEL_DB` directly instead of shadowing the shared helpers with local copies (local shadowing of shared drill helpers is exactly how a bench framework rots). Passed 5/5 consecutive live-S3 runs.
- **Compaction e2e gap closure — gap 4, embedder crash e2e (found and fixed a real bug)**: new S3-gated `e2e_core_replicator_compaction_embedder_crash` in `tests/production_e2e.rs`, copying the `e2e_core_replicator_sigkill_child` spawn-target pattern: a child process embeds a real `Replicator` with `ReplicationConfig::compaction` enabled (aggressive: `l1_batch=4`, `l2_batch=3`, `keep_fine_window=0`), writes continuously; the parent SIGKILLs it mid-merge activity (confirmed via a real S3 listing poll, not a fixed sleep), respawns it, and repeats once more (three total child phases, two kills), then restores VIA THE LIBRARY API (`Replicator::restore`, not the CLI) to a fresh path and asserts row-exact against the child's own on-disk ground truth + `integrity_check`, and that levels actually fired (merged L1/L2 objects present in the bucket). **Found and fixed a real product bug along the way**: `Replicator::add()` unconditionally creates a walrust-owned "lineage" (`SyncState::ensure_lineage_id`, from the recent phase-4 delta work), which moves changesets to a `{db}/lineages/{id}/...` key shape that compaction's `SeqLayout` cannot see (it only reads/writes the flat `{db}/0000/...` shape) — so `compaction.enabled = true` combined with the normal `add()` path silently never compacted anything. Same E7 class as the already-fixed CLI shadow-mode gap. `add()`/`add_with_wal_path()` now refuse up front with a clear error naming the incompatibility and pointing at `add_without_snapshot()` (which never creates a lineage) as the fix; fail-on-revert proven in `crates/walrust-core/tests/replicator_drop.rs` (`add_with_compaction_enabled_refuses_to_create_a_lineage` + a companion pinning that compaction-off `add()` is unaffected). The e2e itself registers via `add_without_snapshot()` (relying on `autonomous_snapshots` + a short `snapshot_interval` for the initial base) and passes cleanly across repeated runs.
- **Compaction e2e gap closure — gap 3, 2-minute compaction soak (found a real, pre-existing bug)**: new `drills/compaction-soak.sh`, a 120-second, deliberately hilarious stress test — 1s wal-sync (the config's minimum whole-second granularity; sub-second is not settable), ~50 rows/sec writer, `[compaction] enabled` at aggressive batches (`l1_batch=4`, `l2_batch=3`, `keep_fine_window=0`), a periodic full snapshot every ~15s, and a PID-verified SIGKILL+restart every ~20s (5-6 cycles inside the window). Wired into `make drill` (`drills/run-all.sh`) and therefore the nightly workflow, with a one-shot induced-failure proof (delete every snapshot base, confirm the row-diff guard has teeth). **This drill found two real issues**, one benign and now understood, one serious and NOT fixed (see the ROADMAP "Compaction (shipped — default off)" residue entry for full technical detail): (1) a seq-contiguous-but-checksum-broken L0 boundary can make the compaction engine log `CompactionError::NonContiguous` at ERROR every tick forever (a liveness wedge on that one boundary, confirmed via repeated runs to never lose or corrupt data — sources are never deleted on a failed merge) — the drill allow-lists exactly this one signature in its log scan while still failing hard on any other ERROR line; (2) rarer (~2 of 4 local runs) and NOT compaction-specific, `walrust restore` itself can hard-fail with a typed `Pre-apply checksum mismatch ... does not chain` error that does not self-heal even given several more periodic-snapshot ticks — root-caused (not fixed) to a likely mismatch between the restart-time checksum baseline in `src/sync/watch_independent.rs` and the incremental chaining in `crates/walrust-core/src/legacy_wal_sync.rs::sync_wal_to_storage`. This second issue is deliberately left unfixed (correctness-critical checksum-chain code deserves a careful, dedicated fix, not a rushed one under this task's scope) and NOT hidden in the drill — a single attempt still fails for real when it happens (full output logged). Since it reproduced in ~2 of 4 local single-attempt runs, the drill's outer layer retries the whole soak (fresh S3 prefix, fully independent process) up to `WALRUST_DRILL_SOAK_ATTEMPTS` (default 3) times so the nightly gate stays actionable instead of alarm-fatigued on a known, tracked, ~50%-per-run flake. **Adversarial-review hardening (PR #32 review pass):** that retry now tolerates **only** issue (2)'s exact signature — the inner attempt classifies the restore failure from its own error text and exits a distinct code, and the outer wrapper retries only that code; a silent wrong-count restore, a different typed error, an unexpected ERROR line, a convergence/toothless-guard failure, or any brand-new flaky bug propagates immediately and is never retried (the previous plain "retry on any nonzero" could have swallowed a regression or a rate change in anything). The review also proved issue (2) is loud-only/never-silent-wrong (restore is content-anchored end to end) and issue (1) is a *permanent* liveness stall sharing issue (2)'s restart-re-anchor root cause, not a cosmetic quirk — both remain HIGH PRIORITY, tracked in ROADMAP with the safe fix directions. Exhausting every attempt still fails the drill for real via the existing drill-nightly-to-GitHub-issue automation.
- **Compaction e2e gap closure — gap 2, version-skew empirical pin**: the "an old binary can't restore a leveled bucket" warning was theoretical — never actually tested. New manual-only drill `drills/version-skew.sh` (`make drill-version-skew`; deliberately NOT in `make drill`/nightly — needs crates.io network access, with an expensive from-source fallback build at a pinned commit) obtains a real pre-compaction `walrust` binary (crates.io `0.5.1`; the task's originally-specified `0.5.2` does not exist on crates.io, confirmed via the registry index, so the drill tries it first — in case it's published later — then falls back automatically), builds a real leveled bucket with a wide-row driver (multi-page dataset, unlike a toy single-page one where stale pages coincidentally still round-trip), and runs the old binary's `restore` against it. **Confirmed, not theoretical**: the old binary exits **0** — no error reported — and produces a **corrupt database** (`PRAGMA integrity_check` fails with `btreeInitPage() returns error code 11` on the pages that existed only inside the merged-and-deleted range). This is worse than the anticipated "silent short restore" hazard: it's silent corruption with a success exit code. The drill characterizes all three possible outcomes (loud failure / row-exact / the confirmed KNOWN-HAZARD) and exits 0 in every case that isn't a genuine anomaly, since the hazard itself is the documented expected result, not a drill bug. README's version-skew caveat is upgraded from theoretical to confirmed, quoting the observed failure mode.
- **Compaction e2e gap closure — gap 1, replicate vs a compacting primary**: `walrust replicate` never reads `levels/` (it only tails the flat gen-0 incremental pool), so a lagging replica whose tail compaction folded and deleted could plausibly stall or corrupt. New S3-gated drill `drills/replica-vs-compaction.sh`: a compacting `walrust watch --independent-tasks` primary (aggressive batches, `keep_fine_window=0`, frequent periodic snapshots) plus a `walrust replicate` replica frozen mid-stream (`SIGSTOP`) right after it applies a handful of early rows, held while compaction folds and deletes exactly the L0 range it needs next, then thawed (`SIGCONT`). Result: **no product change was needed** — the existing F5-era chain-gap handler in `replicate_poll` treats a compacted-away tail identically to any other TXID gap and re-bootstraps from the newest snapshot, converging to row-exact within a bounded number of polls with a loud (not silent) `TXID gap ... Re-bootstrapping from snapshot at TXID N` log line. README's read-replica section now states the re-bootstrap-on-compaction behavior and its cost (full snapshot download; `keep_fine_window` is the slack knob); ROADMAP records that `replicate` reading `levels/` directly stays deliberate future work, not a correctness gap.
- **Compaction C3b — the proof layer (oracle decay, kill-mid-compaction drill, restore-speed bench)**: makes the compaction guarantees permanent and produces the restore-speed number. **Oracle learns real compaction**: the DST state machine gains `Op::Compact`, which drives the REAL `run_level_compaction` merge engine (over a `RangeLayout` on the same legacy `.ltx` bucket the harness writes — the seq-layout adapter for the litestream heritage, identical seq-contiguous merge semantics and `levels/L*/` scheme to `SeqLayout`) to quiescence with generated `l1_batch`/`l2_batch`/`keep_fine_window`, then re-observes the merged windows from the object LISTING (ground truth, never the planner). Restore-to-latest grading is unchanged (compaction must never change latest correctness); a new granularity-decay PIT grader requires a merged-window `max` boundary (and the fine tail) to restore EXACTLY, and a point strictly inside a merged window to be the loud typed decay outcome (the typed `RestoreNotFound` snapshot-span, or the surfaced `PitrInsideMergedWindow` text) — never a bare chain gap, never a silent wrong-point `Ok`. `Op::Compact` runs inside the existing torn/transient/corruption fault plans (the write-verify-delete ordering keeps sources intact on a failed merge; the trailing `RestoreLatest` proves coverage survived) and every `Ok` restore under faults must still be row-exact. A no-faults and a with-faults compaction phase run in CI and the nightly deep sweep; pinned replays prove the decay path, the C3a seq-contiguous batch clip (catch-proof: neutering `contiguous_batch` to naive `take(batch)` makes the machine find+shrink a `NonContiguous` sequence), and the head-fold discovery fix below. **Kill-mid-compaction drill** (`drills/kill-mid-compaction.sh`): a real `walrust watch --independent-tasks` with `[compaction] enabled` at aggressive batch settings, SIGKILL'd (PID-verified) every ~12s during sustained writes across ≥4 cycles, then restore-to-latest row-exact + integrity + exit 0 and a convergence guard (real merged levels formed AND bounded object counts — a kill between merge-write and source-delete leaves harmless bounded overlap, never unbounded duplicate coverage). Wired into `make drill` and the nightly workflow; an opt-in self-test (`WALRUST_DRILL_INDUCE_LOSS=1`) deletes every restore base and asserts the row-diff guard fails loudly. **Restore-speed bench** (`bench/restore-speed.sh`): builds a long unbroken incremental history against local MinIO for three subjects at matched knobs (walrust with compaction, walrust without, litestream default), disables periodic snapshots during the build so restore traverses the full history, then times cold restore-to-latest (3 runs, median, fresh output path each) and counts objects fetched from the server-side trace, with a row-exact validity gate. The headline: **compaction makes walrust restore markedly faster and fetch an order of magnitude fewer objects** (see the README Performance section for the measured table and the `bench/results-*` file it cites). Compaction ships **enabled=false by default** — flipping the default is a separate release decision (version-skew safety: an old binary cannot restore a leveled bucket).
- **Compaction C3a — CLI planner wiring + batch-boundary liveness**: lifts the C2b CLI **sever** and fixes the known merge liveness stall, so leveled compaction now works end-to-end for **both** the `walrust` CLI and library/owned mode. **Liveness fix (write side, both modes)**: `run_level_compaction` no longer selects a rigid oldest-`batch` window that could straddle a snapshot chain-break and error `NonContiguous` on every tick forever; batch selection now clips to a **seq-contiguous run** (a snapshot punches a seq gap, so seq contiguity == chain contiguity — read straight from the listing, no header reads), merging the contiguous prefix even when it is smaller than the batch and skipping a lone leading straddler to the next boundary. A fixed-size batch across a chain-break converges (merges what it can) instead of stalling; a fail-on-revert test (`straddling_snapshot_break_converges_no_eternal_noncontiguous`) shows the old `take(batch)` reproduces the stall, and the merge's checksum-contiguity net still rejects a genuine (seq-dense) fork. **The LTX→HADBP restore seam**: the legacy CLI L0 pool is real litestream **LTX** bytes while merged level objects are **HADBP** (C2a decision). The merge engine now reads real LTX L0 sources (a magic-sniffing layout: LTX pages decompressed via `litepages`, a synthetic end-page-count marker appended so the merged object declares the final DB size) and stamps each produced HADBP object's `prev_checksum`/`declared_end` with the LTX `pre`/`post` of the range it covers — so a restore that interleaves LTX points and HADBP ranges links with **one running checksum in the LTX domain**, while the HADBP content checksum independently guards the merged pages. `legacy_restore::restore_legacy_ltx` is now leveled-aware: it reuses the C2b `plan_restore` planner over the union of L0 LTX points and merged HADBP ranges, applies each object through its format's path (`apply_ltx_to_db_checked` / `apply_decoded_changeset_to_db`) with a DB-anchored `verify_chain` pre-check across the seam, and surfaces the same TXID PITR-decay error (inside-window = loud typed error naming both neighbors; boundary = exact) — the C2b decay logic reused, not forked. **Cache substitution** bypasses `levels/L*/` objects (their `{min}-{max}.ltx` names parse as a TXID range but carry HADBP payloads; substituting a cached LTX would corrupt a leveled restore). **The sever is gone**: `reject_cli_compaction` and its tests are removed; `[compaction] enabled = true` is supported for the CLI watch too (`enabled` still defaults false for version skew); `walrust explain` and the README drop the "library mode only" caveat and keep the version-skew warning. Verify is now fully level-aware end to end: C2b made the between-incrementals gap detector level-aware, and C3a additionally makes the snapshot→incremental chain check bridge a merged-range-covered hole (it previously false-alarmed a "TXID gap after snapshot chain" on a healthy compacted CLI bucket — see the C3a adversarial-review fixes below). **Proofs**: the LTX→HADBP→LTX seam is byte/row-exact vs a real SQLite ground truth (`legacy_compaction_restore`), PITR-to-boundary is exact and PITR-inside-window is the loud typed decay error, a tampered merged object fails the restore loudly (fail-on-revert for the seam verification); an owned-mode **VACUUM (shrink) e2e** merges across the shrink and restores byte/row-exact (orphan-page elision under real SQLite); and an S3-gated CLI e2e drives a real `walrust watch --independent-tasks` with compaction enabled until L1 and L2 fire, the superseded L0 tail is deleted, then `walrust restore` to latest is row-exact + integrity, PITR to a boundary succeeds, PITR inside a window errors loudly, and `walrust verify` exits 0. **C3b next**: oracle granularity-decay extension, kill-mid-compaction drill, restore-speed bench, default-on.
- **Compaction C2b — restore planner, level-aware verify/prune, config exposure (read side + user exposure)**: the C2b read side that makes leveled buckets restorable, plus the single user-facing control. **Greedy restore planner** (`compaction::planner`, litestream `CalcRestorePlan` shape): newest snapshot ≤ target, then repeatedly pick — across L0 and every merged level — the object that *begins exactly at the next needed seq* and *extends the contiguous range furthest*; a coarse merged range wins over the fine points it supersedes. Overlap tolerant (a crash-leftover L0 point alongside the L1 range that covers it is legal — the range wins and no seq is applied twice), chain-integrity strict (`range.min == need` so successor linkage through `chain_end()` always holds). Un-leveled buckets plan **byte-identically to today** (regression-tested by plan-equality). **PITR granularity decay** is a hard typed error: a target strictly inside a merged window with no finer coverage fails loudly naming the nearest restorable points on *both* sides (`… falls inside merged window [a..=b] … Nearest restorable points are seq X (below) and seq Y (above) …`), never a silent chain gap; a target on a window boundary succeeds. **Parallel-prefetch restore executor** (`compaction::restore`): the plan is downloaded through `futures::buffered(queue_depth)` (default 4) — bounded concurrency, order-preserving, so objects apply strictly in plan order while later ones download; peak prefetch memory is `queue_depth × object_size`, not `O(history)`; any prefetch/decode/chain failure aborts the whole restore (temp+rename staging means no partial output). Restore-to-latest and PITR are byte-identical at prefetch concurrency 1 vs 4. Wired into the owned-mode `sync::restore` (leveled path engages only when merged levels exist; the flat path is untouched). **Verify learns levels**: the E3 continuity detector now treats a merged L1/L2 range that contiguously covers an L0 hole as a compaction, not a gap; a hole no level covers still alarms (exit 5) — both directions tested. **Level-aware prune watermark** (`EnforceRetentionByTXID` shape): after snapshot retention, merged objects whose whole range ends below the oldest surviving snapshot's seq are deleted, always keeping the newest object per level and never a watermark-straddling object a retained restore needs; a fail-on-revert proof shows a neutered watermark guard breaks a retained PITR. **Merge marker fix** (write-side, required to make merged objects restorable to SQLite): the merge engine now elides superseded end-page-count markers and emits one canonical marker for the last source's DB size, so a merged object applies and truncates/grows to the exact final size (proven end-to-end via real SQLite restore + `integrity_check`). **Config exposure**: `[compaction] enabled` (default **false** — ship-dark for version skew), `keep_fine_window`, `l1_batch`, `l2_batch` in `walrust.toml`; equivalent `ReplicationConfig::compaction` (`CompactionSettings`) for embedders; `walrust explain` shows the resolved values with the version-skew warning; README gains a "Compaction (experimental, off by default)" section. The C2a internal-only gates (`Replicator::set_compaction_enabled`, `const COMPACTION_ENABLED`) are **removed** — the config is the single control. **E2E proof**: both layouts write real history through the merge engine, then restore-to-latest is row-exact + `integrity_check` **through** merged objects (the superseded L0 tail is gone), PITR-to-boundary succeeds, PITR-inside-window errors naming neighbors, and a crash-overlap fixture restores without double-apply. Every safety-critical behavior (watermark guard, PITR-inside-window error, gap-still-alarms) has a proving test. **C3 next**: oracle granularity-decay extension, kill-mid-compaction drill, restore-speed bench.
- **Compaction C2a — layout-agnostic merge engine (write side)**: a streaming k-way merge that folds N contiguous source changesets into one COMPACTED (HADBP v2) changeset with page-level last-writer-wins. Linkage is preserved via `chain_end()`: output `prev_checksum` = value before the first source, `declared_end_checksum` = last source's `chain_end`. Built once in `walrust-core` against a thin `CompactionLayout` trait with two adapters — the owned-mode seq layout (`{seq}.hadbp`) and the litestream-heritage range layout (`{min}-{max}.ltx`). **Forever key scheme**: merged objects are named `{min:016x}-{max:016x}.{ext}` (16-hex, u64-safe, lexicographic == numeric by min); compaction level `L≥1` lives under a dedicated `{db}/levels/L{n}/` sub-path — deliberately **not** a hex generation folder, because the legacy layout's snapshot generation increments by one per snapshot (`snapshot_gen = current_gen + 1`), so the 16th snapshot lands in `0010/` and would collide two-ways with a `0x0010`-based L1 (legacy discovery classifying merged objects as snapshots; compaction listing ingesting real snapshots as sources). The non-hex `levels/` sub-path is structurally invisible to every existing discovery scanner. **Memory bound — honest two-part statement**: the streaming frontier is `O(page_size × sources + page-id frontier)`, **never `O(total bytes)`**, proven by a peak-buffer counter test that merges 2000 pages while never buffering more than `sources+1`; the one unavoidable `O(output size)` buffer is the serialized object handed to the single non-streaming `put` (the engine hand-rolls a streaming encoder to avoid a second full page-vec copy) — a streaming put/decode is a C2b/hadb TODO. **Safety ordering** (E2-class): write durably → read back and verify (decode + `chain_end` + page count) → only then delete sources; a crash between write and delete converges idempotently (detects the existing merged range and finishes the deletion instead of re-merging). **Count-based triggers** with a `keep_fine_window` that exempts young L0 files. Every merge oracle case (overwrite-in-later, only-first, only-last, interleaved, cross-level) proves the merged output applied to a base is byte-identical to applying all sources in order. Loud typed failures throughout; the write/delete path never warns-and-continues. **Gated OFF and unreachable from config/CLI** (`Replicator::set_compaction_enabled`, default false; legacy watch `const COMPACTION_ENABLED = false`) — config exposure and the restore planner ship with C2b, which is required before enabling compaction keeps backups restorable.
- **Compaction C1 — COMPACTED changeset format (wire-format groundwork)**: bumped the `hadb-changeset` pin to add a version-2 COMPACTED changeset that DECLARES its end-of-range chain value, so a future merged range (C2) stays linkage-verifiable end to end. The **version byte** is the compatibility gate: v1-only decoders reject a compacted file with `UnsupportedVersion(2)` rather than silently misreading it (a flag-only scheme would be silently ignored by old readers). Version-1 changesets are byte-identical to before (frozen golden vector; empirically re-verified byte-identical against the pre-change hadb rev). Content integrity is unchanged (the trailer checksum still covers `prev_checksum + pages`); a tampered declared value passes content decode but breaks the successor's chain check. walrust does not use the new flag yet — this wave only proves the workspace builds and existing chain tests pass against the bumped pin.

### Changed
- **Retention `compact` → `prune`** (retention expiry is pruning, not compaction; borg/restic/git precedent — frees the `compact` name for real compaction in C2). `walrust prune` is the command; `walrust compact` stays as a hidden deprecated alias that warns once on stderr and behaves identically (same exit codes). Renamed internally: `plan_legacy_compaction` → `plan_legacy_prune`, `sync::compact` → `sync::prune`, `shadow::run_compaction` → `run_prune`, DST `Op::Compact` → `Op::Prune`, `drills/compact-retained.sh` → `drills/prune-retained.sh`. TOML keys `compact_after_snapshot`/`compact_interval` gained preferred `prune_*` spellings kept backward-compatible via serde aliases. The `--compact-*` watch CLI flags keep their names for now (renaming user-facing flags needs its own deprecation cycle).

- **Phase 1b: Migrate walrust + walrust-core to hadb-io** — eliminated ~3,200 lines of duplicate retry/S3/storage/webhook/retention/config code
  - walrust-core: deleted `retry.rs`, `s3.rs`, `storage.rs`; re-exports from hadb-io (88 tests passing)
  - walrust CLI: replaced `retry.rs` (642→2), `s3.rs` (471→2), `storage.rs` (182→5), `webhook.rs` (288→2), `retention.rs` (547→2) with thin re-export wrappers
  - `config.rs`: removed shared types (S3Config, WebhookConfig, CacheConfig, parse_duration_string), re-exported from hadb-io
  - Type renames applied: `SyncFailed`→`UploadFailed`, `X-Walrust-Signature`→`X-Hadb-Signature`, SnapshotEntry `filename`→`key` / `max_txid`→`sequence`
  - hadb-io now re-exports `aws_sdk_s3` crate for consumer type access
  - 303 tests passing, 0 failures

### Fixed
- **E10 — a transient S3 LIST during restore's gap-vs-decay classification was swallowed (`unwrap_or_default`), collapsing the typed decay outcome into a bare chain-gap error**: fixed by propagating the listing error (it rides restore's own retry). The review sweep found and fixed the same swallow-class three more times: the modern restore's leveled-bucket check (a transient could misread a compacted bucket as un-leveled — bogus gap error or silent short restore), the modern decay refinement, and level-prune planning (a failed LIST silently skipped level pruning). Deterministic replays pinned for both fault seeds.
- **`busy_timeout` set on the remaining sync.rs connections** (WAL-mode probe and staged-restore integrity checks), eliminating spurious "database is locked" sync failures under contention; a proving contention test rides along. All designed fail-fast refusals (checkpoint blocker, snapshot-vs-watcher) verified unaffected.
- **E11 — mid-merge transient fault left a partial merged-overlap that a later batch collided with**: found by the DST compaction fault phase (`compaction_state_machine_generated_sequences_under_faults`, low-frequency — reproduced at `PROPTEST_CASES=4096`, seed 699302). C2a compaction is write-durably → read-back verify → **delete sources**, and `layout.delete` maps to `StorageBackend::delete_many` whose default is a **serial, non-atomic per-object loop**. A transient injected error mid-loop dropped a strict prefix of a merged batch's sources and left the rest; the `Compact` pass retried, and the next `contiguous_batch` mixed a surviving subset-source with a fresh source into a target range that **crossed** the already-written merged object (e.g. target `[4,5]` crossing existing `L1 [2,4]`). `find_existing_merged` rightly refuses any non-exact overlap with the loud `CompactionError::OverlappingExisting` — the C2a review believed a partial overlap unreachable from crash shapes; the interrupted delete makes it reachable, and on a transient-only plan the loud error can't be retried, failing the case. **Fixed** by converging the interrupted delete before batch selection (`compaction::engine::converge_interrupted_delete`): a source whose seq range is a strict subset of a **sound** merged object at the target level (a leftover of the interrupted deletion) is finished off as a deletion-only convergence, so re-runs converge instead of colliding. Exact-range C2a semantics are untouched — a merged object still tiled exactly by present sources (the full crash-recovery set) stays on step 2's strong `verify_existing`, a covering object that fails `verify_covers` (decode/content-checksum) is never deleted against and is re-merged instead, and a genuinely foreign non-exact overlap stays a loud `OverlappingExisting`. Fail-on-revert: `replay_e11_interrupted_delete_partial_overlap_converges` (DST, deterministic seed 699302 + pinned corpus line) and `interrupted_delete_leftover_subset_converges_not_loud_error` / `interrupted_delete_does_not_drop_leftovers_against_torn_cover` (`compaction_engine`). Sweep green at 256 (×2) and 4096. **Hardened by the PR #37 adversarial review** (see `ADVERSARIAL_REVIEW_2.md` E11): the convergence additionally requires **endpoint chain evidence** — a leftover at the cover's `min`/`max` must carry the cover's exact `prev_checksum`/`chain_end`, so a subset-by-range object of foreign lineage (the fork-artifact shape a rogue second writer could leave in a compaction-vacated seq key) is preserved and stays the loud error instead of being silently deleted (`foreign_endpoint_subset_is_preserved_not_converged`); interior leftovers are content-superseded by a written invariant proof on `converge_interrupted_delete`. A **transient** storage failure while reading a cover for `verify_covers` now **propagates** as a retryable error instead of being swallowed as "unsound, skip" — swallowing let batch selection collide with the cover and decayed the retryable transient into a non-retryable `OverlappingExisting`, the E11 class one GET deeper (`transient_cover_read_stays_retryable_not_loud`). A source subset of two overlapping sound covers is deleted and counted once (`leftover_subset_of_two_overlapping_covers_converges_once`), arbitrary NON-prefix leftover subsets converge (`interrupted_delete_arbitrary_subset_leftovers_converge`), leftovers from two different interrupted merges converge in one pass (`interrupted_deletes_from_two_merges_converge_together`), and the convergence's own deletion being interrupted mid-loop is never-worse and re-convergeable (`interrupted_convergence_deletion_reconverges`).
- **Shadow-watch startup no longer silently starts fresh on a transient manifest fetch (E11 Part 2, H8 cousin)**: `src/sync/watch_shadow.rs` treated ANY `manifest.json` GET failure — transient included — and any parse failure as "fresh database, txid 0". Local durable shadow progress + CAS-guarded publishes prevent a silent fork, but on a fresh host with no local progress a transient (or a corrupt manifest) still misclassifies as fresh. Startup now seeds via `seed_state_from_manifest_fetch`: a **confirmed** not-found starts fresh (correct for a brand-new DB); a parse failure and any non-not-found fetch error **propagate** so startup fails loudly and is retried against a complete view. Not-found is classified from the **typed** AWS SDK error (`s3::download_error_is_not_found`: the `NoSuchKey` service error, or a service response with HTTP status 404) — never message-string matching, which the PR #37 review showed re-opens the bug (free text like a DNS "host not found" or a proxy body mentioning 404 would misread a transient as a missing manifest); any ambiguous error fails safe toward a loud retry, never a silent fresh start. Both directions unit-tested (`manifest_not_found_starts_fresh`, `manifest_transient_fetch_does_not_start_fresh`, `manifest_corrupt_parse_does_not_start_fresh`, plus `not_found_classifier_is_typed_not_string_matched`).
- **Restart re-anchor seam (HIGH PRIORITY — unblocks compaction default-on)**: after a kill/restart, the `--independent-tasks` watch loop resumed a stream by publishing an *incremental* whose `prev_checksum` was recomputed from the on-disk `.db` file — behind the chain tip (SQLite had not checkpointed and the resumed read restarts at `wal_offset == 0`), so it did **not** equal the last pre-crash L0's `chain_end`. That produced an L0 seq/TXID-adjacent to the last pre-crash L0 but chain-DIScontinuous at the boundary (the "seam"), with two loud symptoms: restore-to-latest walked the seq-adjacent L0s across the break and hard-failed its pre-apply chain check (`Pre-apply checksum mismatch ... does not chain`, ~50% of soak attempts), and compaction's `contiguous_batch` selected a seq-contiguous batch spanning the break that the merge refused (`CompactionError::NonContiguous`) every tick forever — a permanent liveness wedge (the pre-seam files stayed oldest). Data was never wrong, but restores flaked and compaction stalled. **Fixed at the root**: startup now re-anchors a *resumed* stream with a fresh snapshot instead of an incremental (`walrust_core::legacy_wal_sync::anchor_stream_on_startup`, wired through `sync::watch_independent` via `anchor_stream_on_startup_with_retry`). A snapshot consumes its own seq, so the next incremental starts strictly past every stale pre-crash L0 (a clean seq GAP — exactly the shape `contiguous_batch` already skips and the restore planner already floors at), it is a self-consistent base, and post-restart incrementals chain from it. Both symptoms dissolve together; no chain check was weakened, restore-to-latest still floors at the newest snapshot, and E2/watermark/`keep_fine_window` semantics are unchanged. This matches what the DST `KillRestart` op always modeled (an eager re-anchor snapshot), so the oracle needed no change. Two deterministic fail-on-revert regression tests reproduce the seam and pin each symptom (`walrust-dst` `reanchor_restart_restore_survives_the_chain_seam`, `reanchor_restart_compaction_does_not_wedge_on_the_seam`), and `drills/compaction-soak.sh` now runs un-crutched (single attempt; restore failures fail hard; any ERROR-level line fails — the exit-42 whole-soak retry and the `NonContiguous` allow-list are removed). One follow-on surfaced by the un-crutched soak: `walrust verify`'s E3 gen-0 continuity rule (`detect_live_txid_gaps`) only bridged a hole via a snapshot exactly at the hole's END or level ranges covering the WHOLE hole, so it false-alarmed (exit 5) on the healthy shape the re-anchor routinely produces — the startup snapshot consumes the TXID at the hole's START and compaction folds the post-restart L0s into levels (snapshot at `expected`, levels covering the rest; restore-to-latest row-exact on the same bucket). The rule is now snapshot-supersession-aware: a full snapshot at `S` supersedes every TXID `<= S`, so only the hole's suffix above the newest in-hole snapshot needs contiguous level coverage — a strict generalization of both prior rules; unbridged and partially-covered holes still alarm (`e3_reanchor_snapshot_plus_levels_bridge_is_not_a_gap` pins both directions).
- **Shadow watch loop silently ignored `[compaction] enabled` (C3b adversarial review, E7)**: leveled compaction only ticks in the independent-tasks watch loop (`maybe_compact_legacy`); the default shadow loop has no compaction tick, so `walrust watch` (shadow mode) with `[compaction] enabled = true` accepted the config and never compacted — a config no-op that would let a bucket the operator believes is being compacted grow unbounded. `walrust watch` now **refuses to start** in shadow mode when compaction is enabled, with an error pointing at `--independent-tasks` (fail-loudly, the C3a sever pattern). Unit-tested (`shadow_watch_rejects_enabled_compaction` / `shadow_watch_allows_compaction_off`); README and ROADMAP updated. The restore-speed bench also gained a **cross-subject row-count validity band**: the three subjects run the same driver rate/duration, so a restore-speed comparison across wildly divergent row counts is apples-to-oranges — if any subject's built (== restored, per the per-subject exactness gate) row count falls outside a 25% band of the max, the whole run aborts with no numbers. And the DST decay grader's accept-side (a decayed point returning a silent `Ok`, an overshoot, or wrong rows) was refactored into a pure `grade_pit_compaction_ok` with direct fail-on-revert unit tests: a correct engine only ever drives the loud `Err` path, so those accept-side guards were previously never exercised (vacuous) — they now have teeth.
- **Compaction restart head discovery (found by the C3b oracle)**: `legacy_manifest::discover_legacy_state` ignored `levels/L*/` merged objects, so once compaction folded the fine L0 tail into a merged range whose `max` extends past the highest gen-folder TXID (e.g. `keep_fine_window = 0`, or a batch that reached the head), a watch RESTART discovered a stale-low head. It then resumed writing incrementals at a seq the merged range already owned (a chain fork) and, with `--on-startup`, published an eager base snapshot BELOW the merged coverage that poisoned restore-to-latest with a chain gap. Discovery now folds the max over `levels/L*/` range ends into the head (levels are not generations, so `max_generation` is untouched). Fail-on-revert: the unit test `discover_head_reflects_merged_level_coverage` and the DST replay `replay_compaction_restart_after_head_folded` (the shrunk `Compact → KillRestart → RestoreLatest` sequence the oracle surfaced).
- **Compaction C3a adversarial review**: the previously-unrun S3 CLI e2e was executed against live storage and root-caused three real defects plus test-fixture bugs. (1) **Verify false-alarmed on a healthy compacted bucket (critical)** — the snapshot→incremental chain check (`verify_ltx_chain`) was **not** level-aware (only the between-incrementals detector was), so once the fine L0 seqs were folded into merged levels it reported a phantom `TXID gap after snapshot chain: expected min_txid=2, got 12` and exited 5. It now bridges a merged-range-covered hole (and skips the by-design-broken L0 checksum link across the seam); periodic `validate_backup_integrity` got the same fix. (2) **A fully-promoted lower level was invisible to read consumers (critical)** — `list_merged_ranges` (verify), `compaction::restore::gather_candidates` (owned restore), and `prune::list_level_files` all stopped scanning at the first empty level. But an L1→L2 merge **deletes** its L1 sources, so the healthy steady state has an empty L1 above a populated L2; the early stop made verify hallucinate a gap, made owned-mode restore report a phantom `ChainGap` on a perfectly restorable bucket, and made prune under-collect. All three now probe every level up to the cap. New regression `restore_finds_l2_when_l1_is_fully_promoted_away`. (3) **Cache-bypass hardened** — the `levels/L*` restore-cache bypass now matches **path segments** structurally instead of a bare `"/levels/L"` substring, so it can never be tricked by a db name/prefix and never false-negative a real level object. (4) **Tamper matrix extended** — added a fail-on-revert proving a corrupted LTX-domain `declared_end` stamp on a merged object breaks the chain at the seam (linkage, not just the content checksum, is verified). (5) **e2e fixture bugs** — the CLI e2e writer auto-checkpointed every commit (rolling the WAL salt → a snapshot per commit → a non-contiguous L0 the liveness rule correctly refuses to merge, so levels never fired); it now drives a contiguous chain. The L1-presence assertion was a race (an L1→L2 merge empties L1) and now checks L1 was *ever* observed; the PITR-decay assertion read the wrong stream (walrust logs to stdout) and now checks combined output.
- **Compaction C2b adversarial review**: (1) **Exposure-vs-read-path gap (critical)** — `[compaction] enabled = true` drove the CLI watch to compact through the range layout (merging + deleting superseded L0), but the CLI restore path (`legacy_restore::restore_legacy_ltx`) is **not** wired to the leveled planner, so a CLI-compacted bucket was unrestorable by `walrust restore`. The CLI watch now **refuses to start** when compaction is enabled (`reject_cli_compaction`), pointing to library/owned mode; the config still parses. `walrust explain` and the README say so. Leveled compaction over the CLI restore path is deferred to a later wave. (2) **Merge shrink (VACUUM) correctness** — when a merge boundary spans a DB shrink, an intermediate source's high page survived last-writer-wins above the final end-page-count marker, which broke the ascending-emit invariant and produced a changeset with a live page past `end_page_count` (a debug-assert panic in tests; a checksum-mismatch/stuck merge in release). The merge now **drops orphan pages above the final DB size**, so shrink merges produce valid, restorable objects that truncate correctly. New `shrink_across_merge_drops_orphan_pages_and_stays_valid` test. (3) **Planner exact-start rule proven safe** — documented and tested (`merge_refuses_to_span_a_snapshot_boundary`) that snapshots consume their own seq and break the L0 chain, so no merged range can begin strictly inside a snapshot span; restore-from-snapshot always finds an object at `floor + 1` and the `range.min == need` rule never false-gaps a healthy bucket. (4) PITR-decay error wording tightened so the `keep_fine_window` hint can't be read as recovering already-merged points.
- **Adversarial review overhaul (PRs #9–#17)**: full second adversarial review found and fixed critical durability bugs across the WAL/checkpoint/upload/restore stack. Highlights: WAL checksum endianness was inverted (frame validation never ran on real SQLite WALs); checkpoint rollovers now re-anchor with a fresh snapshot (walrust-owned) or hard-fail until re-anchored (external/fenced modes); restore verifies the actual DB checksum chain with contiguity checks and writes to a temp file; canonical S3 key layout shared by uploader and restore; CAS + fsynced publish-intent closes a split-brain crash-window; interval-aware upload cursor with halt-on-gap policy; fsync before every ack; fenced follower reconstruction promoted to a production API (`reconstruct_fenced_follower`). Every fix has a proving test verified to fail with the fix disabled. CI (fmt/clippy/full workspace vs MinIO, sccache-cached) now gates every PR; the DST harness drives the production pipeline with real process-kill crash tests. Ledger: `ADVERSARIAL_REVIEW_2.md` (A1–A14, B1–B14 all Fixed/Verified; open edges in its DEFERRED register D1–D7). Working docs `SESSION_PROMPTS.md` and `PHASE4_PLAN.md` served the fix waves and were removed.
- **Deterministic TXID in WAL mode**: Phase Somme assumed SQLite's file change counter increments on every transaction, but in WAL mode it only updates during checkpoints. `sync_wal` and `take_snapshot` now fall back to WAL commit counting (number of frames with non-zero `db_size_after_commit`) when the change counter hasn't advanced. This is deterministic from file content: any process reading the same WAL bytes computes the same TXID. `read_frames_as_page_map` returns `commit_count` as a 5th tuple element. New `count_wal_commits()` scans WAL frame headers without reading page data (for `take_snapshot`). 4 new tests.

## [0.6.0] - 2026-03-23

### Added
- **Concurrent S3 uploads**: Uploader rewrites sequential loop with `tokio::task::JoinSet` for bounded concurrency (default 4, configurable via `--uploader-concurrency`)
  - `UploadTaskContext` pattern extracts shared `Arc` state into a `Clone` struct, avoiding `&self` lifetime issues with JoinSet
  - `tokio::select!` with conditional guard (`if in_flight.len() < max_concurrent`) provides backpressure
  - `resume_pending_uploads` also concurrent (respects max_concurrent)
  - `last_uploaded_txid` tracks highest seen TXID (not last to complete)
- **Shadow mode cache integration**: `sync_shadow_to_cache()` writes LTX to local disk cache + notifies uploader, giving shadow mode the same crash recovery as independent mode
  - `sync_shadow_to_cache_with_retry()` retry wrapper matching existing `sync_shadow_concurrent_with_retry()` pattern
  - Shared encoding extracted into `encode_shadow_to_ltx()` — eliminates ~100 lines of duplication between direct-S3 and cache paths
  - `Box::pin()` with explicit type annotation for dynamic dispatch between cache/direct future types
- **Cache cleanup timer in shadow mode**: Every 5 minutes, matching `watch_independent.rs` pattern
- **Proper shutdown drain**: `spawn_uploader()` returns `(Sender, JoinHandle)` — shadow mode awaits handles with 10s timeout
- **`--uploader-concurrency` CLI flag** (default 4), wired through `CacheConfig.uploader_concurrency`
- **31 new tests**:
  - 18 uploader tests (8 ported + 5 concurrent + 4 edge case + 1 performance)
  - 13 shadow cache tests (7 encoding + 5 sync_shadow_to_cache + 1 build_output)
  - `MockStorage` with `upload_delay`, `active_uploads` (AtomicUsize), `peak_concurrent` tracking

### Changed
- `Uploader::new()` takes `max_concurrent: usize` (7th param, clamped to `.max(1)`)
- `watch_with_shadow()` accepts `CacheConfig` parameter
- `ShadowSyncOutput` derives `Debug`

## [0.5.2] - 2026-03-23

### Fixed
- **RSS 70MB → 20MB**: `encode_snapshot()` and `compute_checksum_from_file()` were reading entire DB into memory via `std::fs::read()`. macOS system allocator never returned freed pages — RSS permanently reflected peak snapshot allocation.
  - Replaced with streaming via `BufReader::with_capacity(1MB, file)` — page-by-page encode + incremental SHA-256 hashing
  - Peak memory is now ~1MB (BufReader) + 4KB (page buffer), not entire DB size
  - Applied to both `src/ltx.rs` and `crates/walrust-core/src/ltx.rs`

### Added
- **mimalloc global allocator**: Returns freed memory to OS (macOS system allocator doesn't). One-line change in `src/main.rs`.
- **RSS profiling tools**: `bench/profile_rss.rs` (component-level RSS measurement), `bench/measure_rss.py` (real walrust with dummy bucket), `bench/measure_rss_s3.py` (real walrust with S3 uploads)

### Performance
- Before: ~70MB RSS for 13MB database (snapshot peak retained by macOS allocator)
- After: ~20MB RSS without S3, ~26MB with real S3 uploads
- mimalloc actively returns freed pages — RSS trends down after peak load

## [0.5.1] - 2026-03-23

### Fixed
- **Memory accumulation under load**: RSS was scaling linearly with write throughput (67MB at 100 w/s → 361MB at 6700 w/s on 50MB DB). Now constant at ~70MB regardless of throughput.
  - `apply_ltx_to_db()` accumulated decoded pages in `Vec<(u32, Vec<u8>)>` for chain checksum verification — replaced with streaming `ChainHasher` that computes incrementally during decode
  - `read_frames_as_pages()` read ALL WAL frames into memory before dedup — replaced with `read_frames_as_page_map()` that deduplicates into HashMap during read (peak memory = unique pages only)
  - Shadow WAL `sync_shadow_concurrent()` accumulated frames then deduplicated — now reads directly into HashMap
  - Retry wrappers cloned LTX buffers per attempt — now use `Arc<Vec<u8>>` for zero-copy sharing

### Added
- `ChainHasher` struct for streaming chain checksum computation
- `read_frames_as_page_map()` in both walrust-core and CLI WAL modules
- Regression tests: `test_chain_hasher_matches_chain_checksum`, `test_chain_hasher_page_count`, `test_apply_ltx_no_memory_accumulation`, `test_read_frames_as_page_map_deduplicates`, `test_read_frames_as_page_map_matches_old_api`

### Removed
- `wal_page_overlay` from `SyncState` (walrust-core)
- `compute_expected_post_with_overlay()` — the full-DB-read bottleneck function
- `crates/walrust-core/target/` from git tracking (was committed despite .gitignore)

## [0.5.0] - 2026-03-22

### Changed
- **Chained page checksums**: Incremental LTX files now use chained page hash instead of full-DB hash
  - `post_checksum = SHA-256(pre_checksum || page1_num || page1_data || ...)` — pages sorted by number
  - Snapshots keep full-DB hash (data already in memory during encode)
  - Eliminates full database read from sync hot path entirely
- **Page clone elimination**: Move frame data instead of cloning during dedup; index-based sorting in `encode_wal_changes()` instead of `pages.to_vec()`

### Performance
- Before: 50MB disk read + 50MB hash = ~100MB I/O per sync cycle (every 1s)
- After: 10 dirty pages x 4KB = 40KB hash per sync cycle

## [0.4.0] - 2026-03-22

### Changed
- **Module split**: Split `watch.rs` (1856 lines) into `watch_independent.rs`, `watch_shadow.rs`, `wal_sync.rs`, `compact.rs`
- **Module split**: Split `restore.rs` (1083 lines) into `restore.rs`, `verify.rs`, `explain.rs`
- **Simplified watch**: Deleted dead watch modes (`watch_simple`, `watch_config`) and ~350 lines of dead code
- **`make test`** now uses `soup run` for S3 credentials — no separate `test-integration` target

### Added
- **Periodic validation in watch_independent**: `--validation-interval` now wired into the independent task event loop (was only in shadow mode)
- **Cache cleanup in watch_independent**: `retention_duration` and `max_cache_size` now consumed — 5-minute cleanup timer evicts stale cache entries

### Fixed
- Removed all `#[ignore]` test attributes — 346 tests pass, 0 ignored
- Fixed integration tests to use `env!("CARGO_BIN_EXE_walrust")` instead of hardcoded `target/release/walrust`
- Rewrote `test_walrust_ltx_litestream_restore` as self-referential round-trip test (litestream can't read walrust LTX format)
- Fixed verify test assertions to match actual output format (no emoji in verify output)

### Removed
- `sync_wal_with_retry()` and `sync_wal()` (~190 lines) — only used by deleted watch modes
- `get_wal_page_count()`, `CheckpointMode`, `run_checkpoint()` (~70 lines) — only used by deleted watch modes
- `save_state()` in manifest.rs (~25 lines) — only called by deleted `sync_wal`
- `watch_simple.rs` and `watch_config.rs` — dead watch modes
- `make test-integration` and `make test-all` Makefile targets (unified into `make test`)

## [0.3.2] - 2026-03-22

### Added
- **`walrust explain` command**: Preview configuration before running watch mode
  - Shows validation intervals, webhook notifications, and cost estimation
  - Displays database list, S3 destination, snapshot schedule, and GFS retention policy
  - Estimates monthly storage costs for Tigris ($0.02/GB) and S3 ($0.023/GB)
- **`walrust verify` command enhancements**:
  - Better output format with ✅/⚠️ symbols for visual clarity
  - Exit codes: 0 (success), 1 (issues found), 2 (critical errors)
  - Explicit snapshot existence check to prevent incomplete backups
  - Per-file verification output with TXID counts and sizes
  - Always reports continuity status (including "Snapshot only" for backups without incrementals)
- **Webhook notifications** for production alerting:
  - `notify_corruption()` called on LTX decode failures and checksum mismatches
  - `notify_circuit_breaker_open()` called when retry circuit breaker trips
  - Fire-and-forget delivery (spawned tasks don't block operations)
  - Integrated into `verify()` and `restore()` commands
- **Comprehensive test coverage**:
  - 15 tests for `explain()` (valid configs, edge cases, CLI integration)
  - 9 tests for `verify()` (6 integration + 3 unit tests)
  - 11 unit tests + 4 integration tests for webhooks
  - Regression tests for webhook blocking and size double-counting bugs

### Fixed
- **Webhook blocking bug**: `verify()` now spawns webhook tasks instead of awaiting inline (prevented slow endpoints from blocking verification)
- **Double-counting file sizes**: Removed duplicate size addition in `verify()` (line 1048)
- **Continuity reporting**: Now always shows status even for snapshot-only backups
- Missing `std::sync::Arc` import in restore.rs
- Test type errors with `rusqlite::params!` macro

### Removed
- `restore_legacy()` function (66 lines) - unused legacy restore path
- Duplicate `CheckpointMode` enum and unused WAL functions (74 lines)
- Total: 140 lines of dead code removed

### Polish
- All 15 webhook tests now run without `#[ignore]` - created real axum HTTP test servers
- Removed 280+ lines of unused code (RetryOutcome, FrameHeader, CompactionConfig, CompactionStats, compact_incrementals(), should_compact())
- Fixed 17 clippy warnings (unused imports, variables, doc formatting)
- Removed ~450 lines of duplicated code from sync module split (explain, verify types, validate_backup_integrity)
- Wired up verify() summary output (verified_count, total_size were tracked but never printed)
- Removed silently-ignored `--fix` flag from verify command
- Removed 213 build artifacts from git tracking (crates/litetx/target/)

## [0.3.1] - Previous

### Changed
- **Pure Polling Architecture**: Removed file watcher (notify crate) entirely
  - WAL changes now detected by polling WAL file size at `wal_sync_interval` intervals
  - Simpler and more reliable than FSEvents/inotify (which miss mmap writes on macOS)
  - Works consistently across all platforms
  - Single config knob: `wal_sync_interval` controls both polling and sync frequency
- Removed `monitor_interval` config option (no longer needed without file watcher)
- Removed `notify` crate dependency

### Added
- **Benchmark Framework (Phase 1)**: Comprehensive benchmarking for data loss verification
  - `bench/lib/workload.py`: DatabaseWriter with rate-limited writes and timestamp tracking
  - `bench/lib/runners.py`: WalrustRunner and LitestreamRunner for process management
  - `bench/lib/monitor.py`: ResourceMonitor for CPU/memory tracking
  - `bench/lib/verify.py`: ReplicationVerifier for S3 restore and data loss detection
  - `bench/benchmark.py`: Main CLI orchestrator with YAML config support
  - `bench/lib/config.py`: BenchmarkConfig with matrix expansion support
  - Config files: `bench/configs/quick.yml` and `bench/configs/scalability-matrix.yml`
  - Documentation: `bench/BENCHMARK_FRAMEWORK.md` with complete usage guide
- Measures data loss (expected vs replicated writes), sync latency (P50/P95/P99), and resource usage

### Performance
- **Phase 1 & 2 Optimizations**: Breaking the 5K w/s throughput ceiling
  - Pre-allocated Vec buffers for LTX encoding (2x estimated size for compression headroom)
  - Offloaded CPU-bound LTX encoding to tokio blocking thread pool via `spawn_blocking`
  - Configured S3 client with HyperClientBuilder for improved connection pooling
  - Added rayon dependency for future parallel processing expansion
  - Memory footprint increased from ~20 MB to ~50-100 MB (acceptable trade-off)
  - Expected throughput gain: 2-5x increase (targeting 10K+ w/s at 250 DBs)

### Changed
- `src/sync.rs`: All WAL sync functions now encode LTX in blocking thread pool
- `src/s3.rs`: S3 client uses aws-smithy-runtime HyperClientBuilder
- `src/config.rs`: Added documentation for aggressive 0.5s sync interval tuning

### Added
- Dependencies: `rayon 1.10`, `aws-smithy-runtime 1`
- Python dependencies for benchmarking: `pyyaml`, `boto3`, `psutil`

### Notes
- **Benchmark Phase 2**: Planned fly-benchmark-engine integration for production infrastructure testing
- **Phase 3 (Batch S3 uploads)** remains pending - test Phase 1+2 results first
- Target metrics: 80%+ achievement at 250 DBs (10K+ w/s), 75%+ at 400 DBs (15K+ w/s)
- Next step: Run comprehensive benchmarks to measure actual throughput gains

## [0.1.9] - 2026-01-15

### Added
- **Full Shadow WAL Integration**: `--shadow-wal` flag now fully functional
  - `watch_with_shadow()` function implements Litestream-style shadow architecture
  - WAL notifications immediately copy frames to shadow directory via `shadow.copy_frames()`
  - Sync timer reads from shadow segments (decoupled from active WAL file)
  - Checkpoint timer uses `shadow.checkpoint()` for controlled checkpoint behavior
  - Concurrent shadow sync with retry logic and webhook notifications
  - Graceful shutdown syncs remaining shadow data before exit
- **New types**: `ShadowDbState`, `ShadowSyncInput`, `ShadowSyncOutput` for shadow mode

### Changed
- Main sync loop now branches based on `--shadow-wal` flag:
  - Without flag: Uses `watch_with_config()` (standard mode)
  - With flag: Uses `watch_with_shadow()` (shadow mode)

### Performance
- Shadow WAL mode decouples S3 upload latency from SQLite write throughput
- No file contention between SQLite writes and S3 uploads
- Checkpoint control prevents race conditions and preserves WAL history
- **Comprehensive benchmark results** (30s duration, 3s warmup, Tigris S3):

  **Throughput Comparison:**
  | DBs | Target | Walrust Standard | Walrust Shadow | Litestream | Winner |
  |-----|--------|-----------------|----------------|------------|---------|
  | 100 | 5,000 | 4,341 (86.8%) ❌ | 4,989 (99.8%) ✅ | 5,016 (100.3%) ✅ | Litestream +0.5% |
  | 250 | 12,500 | 4,077 (32.6%) ❌ | **4,194 (33.5%)** ❌ | 3,762 (30.1%) ❌ | **Walrust +11%** |
  | 400 | 20,000 | 2,013 (10.1%) ❌ | 2,295 (11.5%) ❌ | 3,205 (16.0%) ❌ | Litestream +40% |

  **Memory Usage:**
  | DBs | Walrust Standard | Walrust Shadow | Litestream | Walrust Efficiency |
  |-----|-----------------|----------------|------------|-------------------|
  | 100 | 0 MB (crash) | **19.0 MB** | 646.1 MB | **34x less** |
  | 250 | 13.4 MB | **18.3 MB** | 691.6 MB | **38x less** |
  | 400 | 13.1 MB | **21.5 MB** | 680.3 MB | **32x less** |

  **Key Findings:**
  - **Walrust Shadow WAL is competitive with Litestream** at production scales (100-250 dbs)
  - At 100 dbs: Near-parity performance (99.8% vs 100.3% of target)
  - At 250 dbs: Walrust wins by 11% throughput (4,194 vs 3,762 w/s)
  - At 400+ dbs: Litestream's Go concurrency gives it 40% advantage
  - **Memory efficiency: 30-40x less than Litestream** (19-21 MB vs 646-692 MB)
  - **Recommendation**: Shadow WAL is production-ready for workloads up to 5K w/s with exceptional memory efficiency

## [0.1.8] - 2026-01-15

### Added
- **Concurrent WAL Sync**: Refactored sync loop to process databases concurrently
  - Uses `futures::join_all` instead of sequential `for` loop
  - Added `SyncInput`/`SyncOutput` structs for immutable concurrent processing
  - At 100 DBs, sync cycle now runs in parallel instead of 100x sequential
  - Added `futures` crate dependency
- **`walrust pragma` Command**: Output recommended SQLite PRAGMA settings
  - Includes `wal_autocheckpoint=0` (walrust manages checkpoints)
  - Includes `synchronous=NORMAL`, `journal_mode=WAL`, cache and mmap settings
  - `--output` flag to write to file, `--comments` flag for explanatory comments
- **Shadow WAL Module** (`src/shadow.rs`): Foundation for Litestream-style architecture
  - `ShadowWal` struct with checkpoint blocker (read transaction prevents auto-checkpoint)
  - Frame copier to shadow directory (decouples uploads from active WAL)
  - Segment file management with generation tracking
  - Manual checkpoint trigger with shadow rotation
  - Cleanup of old shadow segments
- **`--shadow-wal` CLI Flag**: Experimental flag to enable shadow WAL mode
  - Creates shadow directories for each database
  - Integration completed in v0.1.9

### Changed
- `RetryPolicy` now derives `Clone` for use in concurrent sync futures

### Performance
- Benchmark at 100 DBs x 50 w/s: Sequential processing was the bottleneck
- After concurrent fix: S3 upload latency becomes the limiting factor
- Shadow WAL decouples uploads from writes for better throughput

## [0.1.7] - 2026-01-14

### Fixed
- **Soak Test Warmup Period**: Fixed false positive memory warnings in short soak tests
  - Added `--warmup-secs` CLI flag (default: 5 seconds)
  - Warmup runs typical operations before taking memory baseline
  - Baseline measurement now reflects steady-state memory, not startup overhead
  - Eliminates false positive "memory growth" warnings for short test runs

### Added
- **Real S3 Integration Testing** (`walrust-dst s3-test`)
  - Tests against real Tigris/S3 storage (not mocks)
  - 12 comprehensive integration tests covering core functionality, scale, and error handling:
    - `basic_upload_download` - S3 operations verification
    - `snapshot_restore` - Full snapshot and restore cycle (100 rows)
    - `incremental_sync` - WAL sync with multiple batches (3 batches, 30 rows total)
    - `point_in_time` - PITR restore to specific TXID (restore at TXID 6)
    - `concurrent_snapshots` - Multi-database parallel snapshots (5 databases)
    - `large_database` - Large database handling (10MB+, 2500 rows, 11MB)
    - `binary_data` - Binary data preservation (BLOB patterns with PASSIVE checkpoint)
    - `many_incrementals` - Many incremental syncs (50+ syncs, TXID 1→51)
    - `large_wal` - Large WAL file handling (1000+ frames, 1013 frames synced)
    - `manifest_corruption` - Manifest corruption detection (invalid JSON)
    - `corruption_detection` - Corrupted LTX file detection (checksum failure)
    - `missing_files` - Restore with missing S3 files (error handling)
  - Automatic cleanup of test objects after each run
  - Configurable via `S3_TEST_BUCKET` and `AWS_ENDPOINT_URL_S3` env vars
  - `--no-cleanup` flag to preserve test objects for debugging
  - `--test <name>` flag to run specific test
- **Improved Soak Test Reporting**
  - Shows initial (pre-warmup) and baseline (post-warmup) memory separately
  - Warmup operation count reported for transparency

## [0.1.6] - 2026-01-14

### Fixed
- **PITR Bug Fixed**: `testable::restore` now correctly parses point-in-time parameter
  - Supports `txid:N` format (e.g., `txid:12345`) for specific transaction ID restore
  - Supports ISO8601 timestamp format (e.g., `2024-01-15T10:30:00Z`) for time-based restore
  - Selects correct snapshot + incrementals for target TXID
  - Un-ignored `test_prop_point_in_time_restore` - all 7 invariants now tested

### Added
- **Production Hardening** (walrust-dst)
  - `walrust-dst stress` command: Multi-database stress testing
    - Configurable database count, writes/sec, duration
    - 20% fault injection with retry handling
    - Memory and FD tracking
    - Error rate reporting (<10% threshold)
  - `walrust-dst soak` command: Long-running stability testing
    - Configurable duration (e.g., `1h`, `24h`)
    - Memory checkpoint every 60s
    - Trend analysis for leak detection
    - Memory growth threshold (<10% warning)
  - Resource leak detection: Memory and FD monitoring throughout tests
- **Phase 4 Complete**: All 7 core invariants passing
  - Point-in-time restore: Restore at TXID T gives exact state at T (FIXED)
  - Transaction recovery: Every committed transaction recoverable from S3
  - WAL batching: WAL batching never loses frames
  - Snapshot atomicity: Snapshots are atomic (no partial state)
  - TXID monotonicity: No gaps, no duplicates in TXID sequence
  - Binary preservation: Restored DB byte-identical to source
  - Recovery under failure: Recovery succeeds even with S3 errors
- 174 tests total (140 walrust + 34 walrust-dst)

- **Retry Logic with Exponential Backoff**: Automatic retry for transient S3 failures
  - Exponential backoff: 100ms -> 200ms -> 400ms -> ... capped at 30s
  - Full jitter to avoid thundering herd
  - Configurable max retries (default: 5)
  - Error classification: retry 500/502/503/504/timeouts, fail fast on 401/403
  - Circuit breaker: opens after N consecutive failures (default: 10)
  - Config: `[retry]` section in `walrust.toml`
  - **CLI flags** (new): `--max-retries`, `--base-delay-ms`, `--max-delay-ms`, `--no-circuit-breaker`, `--circuit-breaker-threshold`
- **Failure Webhooks**: HTTP POST notifications for failure events
  - Event types: `sync_failed`, `auth_failure`, `corruption_detected`, `circuit_breaker_open`
  - Configurable URL targets with event filtering
  - HMAC-SHA256 signatures for webhook authentication
  - Config: `[[webhooks]]` section in `walrust.toml`
  - **Production integration** (new): All sync operations now send webhooks on failures
- **Production Retry Integration**: Main sync loop now uses retry logic
  - `sync_wal_with_retry()` and `take_snapshot_with_retry()` wrap all S3 operations
  - Auth errors (401/403) fail fast and notify via webhook
  - Transient errors (500/502/503/504/timeouts) retry with exponential backoff
  - Structured logging for all retry attempts
- **Retry-enabled testable functions**: `take_snapshot_with_retry`, `sync_wal_with_retry`
  - Used by DST chaos tests to verify retry behavior
  - 150+ tests passing including `chaos_s3_errors` (80%+ success under 20% error injection)
- **StorageBackend Trait**: Abstraction for S3 operations enabling testability
  - `StorageBackend` trait in `src/storage.rs` with `S3Backend` implementation
  - `walrust::testable` module exposing `sync_wal`, `take_snapshot`, `restore` for DST
  - Enables fault injection testing without MadSim complexity
- **DST Framework (walrust-dst)**: Deterministic Simulation Testing for chaos testing
  - `MockStorageBackend` with configurable fault injection (RandomError, Latency, PartialWrite, SilentCorruption, EventualConsistency)
  - Property-based tests (7 properties, 100+ cases each)
  - Real chaos tests calling actual walrust sync functions
  - 23 tests passing
- **Structured Exit Codes**: Specific exit codes for different error categories
  - 0: Success
  - 1: General/unknown error
  - 2: Configuration error (invalid config, missing CLI args)
  - 3: Database error (file not found, WAL corruption)
  - 4: S3 error (network, auth, bucket access)
  - 5: Integrity error (checksum mismatch, LTX verification failed)
  - 6: Restore error (no snapshot found, PITR unavailable)
  - Enables scripted error handling and monitoring integration

## [0.1.4] - 2026-01-14

### Added
- **Monitor Interval** (`monitor_interval`): Configurable file watcher debouncing
  - Reduces CPU usage on high-write workloads
  - Default: 1 second (check for changes every second)
  - Higher values reduce CPU but increase sync latency
  - Configurable via CLI (`--monitor-interval`) and config file
  - Per-database override support
- **Validation Interval** (`validation_interval`): Automated backup integrity verification
  - Periodic verification of LTX checksums and TXID continuity
  - Default: 0 (disabled)
  - Recommended: 86400 (daily) for production
  - Prometheus metrics: `walrust_validation_success_total`, `walrust_validation_failure_total`, `walrust_last_validation_timestamp`
  - Configurable via CLI (`--validation-interval`) and config file
  - Per-database override support
- **WAL Checkpoint Controls**: Production-grade WAL management to prevent unbounded growth
  - `checkpoint_interval`: Periodic PASSIVE checkpoint (default: 60s)
  - `min_checkpoint_page_count`: Only checkpoint if WAL ≥ N pages (default: 1000, ~4MB)
  - `wal_truncate_threshold_pages`: Emergency TRUNCATE checkpoint threshold (default: 121359, ~500MB)
  - Configurable via CLI flags (`--checkpoint-interval`, `--min-checkpoint-pages`, `--wal-truncate-threshold`)
  - Configurable per-database in `walrust.toml`
  - Non-blocking PASSIVE checkpoints for efficiency
  - Blocking TRUNCATE checkpoints for emergency safety brake
- **WAL Sync Batching**: `wal_sync_interval` to batch WAL changes (default: 1s) instead of syncing on every write
- **DST Framework Roadmap**: Comprehensive battle testing plan for v1.0 (see [ROADMAP.md](./ROADMAP.md))
  - Phase 1: Basic crash/network failure testing
  - Phase 2: S3 fault injection and WAL edge cases
  - Phase 3: Property-based chaos testing (10K+ iterations)
  - Success criteria: Zero data loss under any failure scenario
- **Documentation**:
  - [BATTLE_TESTING.md](./BATTLE_TESTING.md) - DST architecture and test scenarios

### Fixed
- All production-critical config options now implemented (was blocking v0.3 production readiness)

## [0.3.0] - 2026-01-13

### Added
- **LTX Format Integration**: Snapshots now stored as LTX files (Litestream-compatible)
  - Compressed, checksummed, industry-standard format
  - SHA256 verification on top of LTX CRC64 checksums
- **Point-in-Time Restore**: Restore databases to specific moments
  - By TXID: `--point-in-time 12345`
  - By timestamp: `--point-in-time 2024-01-15T10:30:00Z`
- **GFS Retention Policies**: Grandfather/Father/Son compaction
  - Configurable hourly/daily/weekly/monthly tiers
  - `walrust compact` command with dry-run default
  - Auto-compaction via `--compact-after-snapshot` and `--compact-interval`
- **Config File Support**: TOML configuration for multi-database deployments
  - Per-database settings overrides (interval, retention, prefix)
  - Wildcard path expansion (`/data/*.db`)
  - `walrust.toml` auto-discovery in current directory
- **Poll-based Read Replicas**: `walrust replicate` command
  - Auto-bootstrap from latest snapshot
  - TXID-based tracking with resume capability
  - Configurable poll interval
- **`walrust explain` Command**: Preview configuration without executing
  - Shows resolved database paths
  - Displays per-database overrides
  - Calculates total snapshots retained
- **`walrust verify` Command**: Verify LTX integrity in S3
  - Checks file existence, checksums, TXID continuity
  - `--fix` flag to remove orphaned manifest entries
- **Prometheus Metrics Dashboard**: Built-in observability
  - `/metrics` endpoint at configurable port (default: 16767)
  - Tracks: last_sync, wal_size, snapshot_count, current_txid, uptime
- **Sync Triggers**: Smarter snapshot scheduling
  - `max_changes`: Sync after N WAL frames
  - `max_interval`: Maximum time between snapshots
  - `on_idle`: Snapshot after idle period
  - `on_startup`: Snapshot when watch starts

### Changed
- Improved CLI help text with detailed descriptions
- Enhanced config validation with better error messages
- Version displayed via `--version` flag

### Fixed
- Config validation now catches global retention with all zeros
- S3 bucket validation rejects empty strings and spaces

## [0.2.0] - 2024-12-01

### Added
- SHA256 checksums stored in S3 metadata
- Multi-database support (single process handles multiple DBs)
- Comprehensive data integrity test suite
- Python bindings via PyO3

### Changed
- Improved restore reliability with checksum verification

## [0.1.0] - 2024-11-01

### Added
- Initial release
- Basic WAL sync to S3/Tigris
- Simple snapshot/restore commands
- `walrust watch` for continuous sync
- `walrust list` to show databases in S3

[0.3.0]: https://github.com/russellromney/walrust/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/russellromney/walrust/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/russellromney/walrust/releases/tag/v0.1.0
