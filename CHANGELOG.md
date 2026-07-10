# Changelog

All notable changes to walrust will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Compaction e2e gap closure — gap 2, version-skew empirical pin**: the "an old binary can't restore a leveled bucket" warning was theoretical — never actually tested. New manual-only drill `drills/version-skew.sh` (`make drill-version-skew`; deliberately NOT in `make drill`/nightly — needs crates.io network access, with an expensive from-source fallback build at a pinned commit) obtains a real pre-compaction `walrust` binary (crates.io `0.5.1`; the task's originally-specified `0.5.2` does not exist on crates.io, confirmed via the registry index, so the drill tries it first — in case it's published later — then falls back automatically), builds a real leveled bucket with a wide-row driver (multi-page dataset, unlike a toy single-page one where stale pages coincidentally still round-trip), and runs the old binary's `restore` against it. **Confirmed, not theoretical**: the old binary exits **0** — no error reported — and produces a **corrupt database** (`PRAGMA integrity_check` fails with `btreeInitPage() returns error code 11` on the pages that existed only inside the merged-and-deleted range). This is worse than the anticipated "silent short restore" hazard: it's silent corruption with a success exit code. The drill characterizes all three possible outcomes (loud failure / row-exact / the confirmed KNOWN-HAZARD) and exits 0 in every case that isn't a genuine anomaly, since the hazard itself is the documented expected result, not a drill bug. README's version-skew caveat is upgraded from theoretical to confirmed, quoting the observed failure mode.
- **Compaction e2e gap closure — gap 1, replicate vs a compacting primary**: `walrust replicate` never reads `levels/` (it only tails the flat gen-0 incremental pool), so a lagging replica whose tail compaction folded and deleted could plausibly stall or corrupt. New S3-gated drill `drills/replica-vs-compaction.sh`: a compacting `walrust watch --independent-tasks` primary (aggressive batches, `keep_fine_window=0`, frequent periodic snapshots) plus a `walrust replicate` replica frozen mid-stream (`SIGSTOP`) right after it applies a handful of early rows, held while compaction folds and deletes exactly the L0 range it needs next, then thawed (`SIGCONT`). Result: **no product change was needed** — the existing F5-era chain-gap handler in `replicate_poll` treats a compacted-away tail identically to any other TXID gap and re-bootstraps from the newest snapshot, converging to row-exact within a bounded number of polls with a loud (not silent) `TXID gap ... Re-bootstrapping from snapshot at TXID N` log line. README's read-replica section now states the re-bootstrap-on-compaction behavior and its cost (full snapshot download; `keep_fine_window` is the slack knob); ROADMAP records that `replicate` reading `levels/` directly stays deliberate future work, not a correctness gap.
- **Compaction C3b — the proof layer (oracle decay, kill-mid-compaction drill, restore-speed bench)**: makes the compaction guarantees permanent and produces the restore-speed number. **Oracle learns real compaction**: the DST state machine gains `Op::Compact`, which drives the REAL `run_level_compaction` merge engine (over a `RangeLayout` on the same legacy `.ltx` bucket the harness writes — the seq-layout adapter for the litestream heritage, identical seq-contiguous merge semantics and `levels/L*/` scheme to `SeqLayout`) to quiescence with generated `l1_batch`/`l2_batch`/`keep_fine_window`, then re-observes the merged windows from the object LISTING (ground truth, never the planner). Restore-to-latest grading is unchanged (compaction must never change latest correctness); a new granularity-decay PIT grader requires a merged-window `max` boundary (and the fine tail) to restore EXACTLY, and a point strictly inside a merged window to be the loud typed decay outcome (the typed `RestoreNotFound` snapshot-span, or the surfaced `PitrInsideMergedWindow` text) — never a bare chain gap, never a silent wrong-point `Ok`. `Op::Compact` runs inside the existing torn/transient/corruption fault plans (the write-verify-delete ordering keeps sources intact on a failed merge; the trailing `RestoreLatest` proves coverage survived) and every `Ok` restore under faults must still be row-exact. A no-faults and a with-faults compaction phase run in CI and the nightly deep sweep; pinned replays prove the decay path, the C3a seq-contiguous batch clip (catch-proof: neutering `contiguous_batch` to naive `take(batch)` makes the machine find+shrink a `NonContiguous` sequence), and the head-fold discovery fix below. **Kill-mid-compaction drill** (`drills/kill-mid-compaction.sh`): a real `walrust watch --independent-tasks` with `[compaction] enabled` at aggressive batch settings, SIGKILL'd (PID-verified) every ~12s during sustained writes across ≥4 cycles, then restore-to-latest row-exact + integrity + exit 0 and a convergence guard (real merged levels formed AND bounded object counts — a kill between merge-write and source-delete leaves harmless bounded overlap, never unbounded duplicate coverage). Wired into `make drill` and the nightly workflow; an opt-in self-test (`WALRUST_DRILL_INDUCE_LOSS=1`) deletes every restore base and asserts the row-diff guard fails loudly. **Restore-speed bench** (`bench/restore-speed.sh`): builds a long unbroken incremental history against local MinIO for three subjects at matched knobs (walrust with compaction, walrust without, litestream default), disables periodic snapshots during the build so restore traverses the full history, then times cold restore-to-latest (3 runs, median, fresh output path each) and counts objects fetched from the server-side trace, with a row-exact validity gate. The headline: **compaction makes walrust restore markedly faster and fetch an order of magnitude fewer objects** (see the README Performance section for the measured table and the `bench/results-*` file it cites). Compaction ships **enabled=false by default** — flipping the default is a separate release decision (version-skew safety: an old binary cannot restore a leveled bucket).
- **Compaction C3a — CLI planner wiring + batch-boundary liveness**: lifts the C2b CLI **sever** and fixes the known merge liveness stall, so leveled compaction now works end-to-end for **both** the `walrust` CLI and library/owned mode. **Liveness fix (write side, both modes)**: `run_level_compaction` no longer selects a rigid oldest-`batch` window that could straddle a snapshot chain-break and error `NonContiguous` on every tick forever; batch selection now clips to a **seq-contiguous run** (a snapshot punches a seq gap, so seq contiguity == chain contiguity — read straight from the listing, no header reads), merging the contiguous prefix even when it is smaller than the batch and skipping a lone leading straddler to the next boundary. A fixed-size batch across a chain-break converges (merges what it can) instead of stalling; a fail-on-revert test (`straddling_snapshot_break_converges_no_eternal_noncontiguous`) shows the old `take(batch)` reproduces the stall, and the merge's checksum-contiguity net still rejects a genuine (seq-dense) fork. **The LTX→HADBP restore seam**: the legacy CLI L0 pool is real litestream **LTX** bytes while merged level objects are **HADBP** (C2a decision). The merge engine now reads real LTX L0 sources (a magic-sniffing layout: LTX pages decompressed via `litepages`, a synthetic end-page-count marker appended so the merged object declares the final DB size) and stamps each produced HADBP object's `prev_checksum`/`declared_end` with the LTX `pre`/`post` of the range it covers — so a restore that interleaves LTX points and HADBP ranges links with **one running checksum in the LTX domain**, while the HADBP content checksum independently guards the merged pages. `legacy_restore::restore_legacy_ltx` is now leveled-aware: it reuses the C2b `plan_restore` planner over the union of L0 LTX points and merged HADBP ranges, applies each object through its format's path (`apply_ltx_to_db_checked` / `apply_decoded_changeset_to_db`) with a DB-anchored `verify_chain` pre-check across the seam, and surfaces the same TXID PITR-decay error (inside-window = loud typed error naming both neighbors; boundary = exact) — the C2b decay logic reused, not forked. **Cache substitution** bypasses `levels/L*/` objects (their `{min}-{max}.ltx` names parse as a TXID range but carry HADBP payloads; substituting a cached LTX would corrupt a leveled restore). **The sever is gone**: `reject_cli_compaction` and its tests are removed; `[compaction] enabled = true` is supported for the CLI watch too (`enabled` still defaults false for version skew); `walrust explain` and the README drop the "library mode only" caveat and keep the version-skew warning. Verify is now fully level-aware end to end: C2b made the between-incrementals gap detector level-aware, and C3a additionally makes the snapshot→incremental chain check bridge a merged-range-covered hole (it previously false-alarmed a "TXID gap after snapshot chain" on a healthy compacted CLI bucket — see the C3a adversarial-review fixes below). **Proofs**: the LTX→HADBP→LTX seam is byte/row-exact vs a real SQLite ground truth (`legacy_compaction_restore`), PITR-to-boundary is exact and PITR-inside-window is the loud typed decay error, a tampered merged object fails the restore loudly (fail-on-revert for the seam verification); an owned-mode **VACUUM (shrink) e2e** merges across the shrink and restores byte/row-exact (orphan-page elision under real SQLite); and an S3-gated CLI e2e drives a real `walrust watch --independent-tasks` with compaction enabled until L1 and L2 fire, the superseded L0 tail is deleted, then `walrust restore` to latest is row-exact + integrity, PITR to a boundary succeeds, PITR inside a window errors loudly, and `walrust verify` exits 0. **C3b next**: oracle granularity-decay extension, kill-mid-compaction drill, restore-speed bench, default-on.
- **Compaction C2b — restore planner, level-aware verify/prune, config exposure (read side + user exposure)**: the C2b read side that makes leveled buckets restorable, plus the single user-facing control. **Greedy restore planner** (`compaction::planner`, litestream `CalcRestorePlan` shape): newest snapshot ≤ target, then repeatedly pick — across L0 and every merged level — the object that *begins exactly at the next needed seq* and *extends the contiguous range furthest*; a coarse merged range wins over the fine points it supersedes. Overlap tolerant (a crash-leftover L0 point alongside the L1 range that covers it is legal — the range wins and no seq is applied twice), chain-integrity strict (`range.min == need` so successor linkage through `chain_end()` always holds). Un-leveled buckets plan **byte-identically to today** (regression-tested by plan-equality). **PITR granularity decay** is a hard typed error: a target strictly inside a merged window with no finer coverage fails loudly naming the nearest restorable points on *both* sides (`… falls inside merged window [a..=b] … Nearest restorable points are seq X (below) and seq Y (above) …`), never a silent chain gap; a target on a window boundary succeeds. **Parallel-prefetch restore executor** (`compaction::restore`): the plan is downloaded through `futures::buffered(queue_depth)` (default 4) — bounded concurrency, order-preserving, so objects apply strictly in plan order while later ones download; peak prefetch memory is `queue_depth × object_size`, not `O(history)`; any prefetch/decode/chain failure aborts the whole restore (temp+rename staging means no partial output). Restore-to-latest and PITR are byte-identical at prefetch concurrency 1 vs 4. Wired into the owned-mode `sync::restore` (leveled path engages only when merged levels exist; the flat path is untouched). **Verify learns levels**: the E3 continuity detector now treats a merged L1/L2 range that contiguously covers an L0 hole as a compaction, not a gap; a hole no level covers still alarms (exit 5) — both directions tested. **Level-aware prune watermark** (`EnforceRetentionByTXID` shape): after snapshot retention, merged objects whose whole range ends below the oldest surviving snapshot's seq are deleted, always keeping the newest object per level and never a watermark-straddling object a retained restore needs; a fail-on-revert proof shows a neutered watermark guard breaks a retained PITR. **Merge marker fix** (write-side, required to make merged objects restorable to SQLite): the merge engine now elides superseded end-page-count markers and emits one canonical marker for the last source's DB size, so a merged object applies and truncates/grows to the exact final size (proven end-to-end via real SQLite restore + `integrity_check`). **Config exposure**: `[compaction] enabled` (default **false** — ship-dark for version skew), `keep_fine_window`, `l1_batch`, `l2_batch` in `walrust.toml`; equivalent `ReplicationConfig::compaction` (`CompactionSettings`) for embedders; `walrust explain` shows the resolved values with the version-skew warning; README gains a "Compaction (experimental, off by default)" section. The C2a internal-only gates (`Replicator::set_compaction_enabled`, `const COMPACTION_ENABLED`) are **removed** — the config is the single control. **E2E proof**: both layouts write real history through the merge engine, then restore-to-latest is row-exact + `integrity_check` **through** merged objects (the superseded L0 tail is gone), PITR-to-boundary succeeds, PITR-inside-window errors naming neighbors, and a crash-overlap fixture restores without double-apply. Every safety-critical behavior (watermark guard, PITR-inside-window error, gap-still-alarms) has a proving test. **C3 next**: oracle granularity-decay extension, kill-mid-compaction drill, restore-speed bench.
- **Compaction C2a — layout-agnostic merge engine (write side)**: a streaming k-way merge that folds N contiguous source changesets into one COMPACTED (HADBP v2) changeset with page-level last-writer-wins. Linkage is preserved via `chain_end()`: output `prev_checksum` = value before the first source, `declared_end_checksum` = last source's `chain_end`. Built once in `walrust-core` against a thin `CompactionLayout` trait with two adapters — the owned-mode seq layout (`{seq}.hadbp`) and the litestream-heritage range layout (`{min}-{max}.ltx`). **Forever key scheme**: merged objects are named `{min:016x}-{max:016x}.{ext}` (16-hex, u64-safe, lexicographic == numeric by min); compaction level `L≥1` lives under a dedicated `{db}/levels/L{n}/` sub-path — deliberately **not** a hex generation folder, because the legacy layout's snapshot generation increments by one per snapshot (`snapshot_gen = current_gen + 1`), so the 16th snapshot lands in `0010/` and would collide two-ways with a `0x0010`-based L1 (legacy discovery classifying merged objects as snapshots; compaction listing ingesting real snapshots as sources). The non-hex `levels/` sub-path is structurally invisible to every existing discovery scanner. **Memory bound — honest two-part statement**: the streaming frontier is `O(page_size × sources + page-id frontier)`, **never `O(total bytes)`**, proven by a peak-buffer counter test that merges 2000 pages while never buffering more than `sources+1`; the one unavoidable `O(output size)` buffer is the serialized object handed to the single non-streaming `put` (the engine hand-rolls a streaming encoder to avoid a second full page-vec copy) — a streaming put/decode is a C2b/hadb TODO. **Safety ordering** (E2-class): write durably → read back and verify (decode + `chain_end` + page count) → only then delete sources; a crash between write and delete converges idempotently (detects the existing merged range and finishes the deletion instead of re-merging). **Count-based triggers** with a `keep_fine_window` that exempts young L0 files. Every merge oracle case (overwrite-in-later, only-first, only-last, interleaved, cross-level) proves the merged output applied to a base is byte-identical to applying all sources in order. Loud typed failures throughout; the write/delete path never warns-and-continues. **Gated OFF and unreachable from config/CLI** (`Replicator::set_compaction_enabled`, default false; legacy watch `const COMPACTION_ENABLED = false`) — config exposure and the restore planner ship with C2b, which is required before enabling compaction keeps backups restorable.
- **Compaction C1 — COMPACTED changeset format (wire-format groundwork)**: bumped the `hadb-changeset` pin to add a version-2 COMPACTED changeset that DECLARES its end-of-range chain value, so a future merged range (C2) stays linkage-verifiable end to end. The **version byte** is the compatibility gate: v1-only decoders reject a compacted file with `UnsupportedVersion(2)` rather than silently misreading it (a flag-only scheme would be silently ignored by old readers). Version-1 changesets are byte-identical to before (frozen golden vector; empirically re-verified byte-identical against the pre-change hadb rev). Content integrity is unchanged (the trailer checksum still covers `prev_checksum + pages`); a tampered declared value passes content decode but breaks the successor's chain check. walrust does not use the new flag yet — this wave only proves the workspace builds and existing chain tests pass against the bumped pin.

### Changed
- **Retention `compact` → `prune`** (retention expiry is pruning, not compaction; borg/restic/git precedent — frees the `compact` name for real compaction in C2). `walrust prune` is the command; `walrust compact` stays as a hidden deprecated alias that warns once on stderr and behaves identically (same exit codes). Renamed internally: `plan_legacy_compaction` → `plan_legacy_prune`, `sync::compact` → `sync::prune`, `shadow::run_compaction` → `run_prune`, DST `Op::Compact` → `Op::Prune`, `drills/compact-retained.sh` → `drills/prune-retained.sh`. TOML keys `compact_after_snapshot`/`compact_interval` gained preferred `prune_*` spellings kept backward-compatible via serde aliases. The `--compact-*` watch CLI flags keep their names for now (renaming user-facing flags needs its own deprecation cycle).

### Fixed
- **Shadow watch loop silently ignored `[compaction] enabled` (C3b adversarial review, E7)**: leveled compaction only ticks in the independent-tasks watch loop (`maybe_compact_legacy`); the default shadow loop has no compaction tick, so `walrust watch` (shadow mode) with `[compaction] enabled = true` accepted the config and never compacted — a config no-op that would let a bucket the operator believes is being compacted grow unbounded. `walrust watch` now **refuses to start** in shadow mode when compaction is enabled, with an error pointing at `--independent-tasks` (fail-loudly, the C3a sever pattern). Unit-tested (`shadow_watch_rejects_enabled_compaction` / `shadow_watch_allows_compaction_off`); README and ROADMAP updated. The restore-speed bench also gained a **cross-subject row-count validity band**: the three subjects run the same driver rate/duration, so a restore-speed comparison across wildly divergent row counts is apples-to-oranges — if any subject's built (== restored, per the per-subject exactness gate) row count falls outside a 25% band of the max, the whole run aborts with no numbers. And the DST decay grader's accept-side (a decayed point returning a silent `Ok`, an overshoot, or wrong rows) was refactored into a pure `grade_pit_compaction_ok` with direct fail-on-revert unit tests: a correct engine only ever drives the loud `Err` path, so those accept-side guards were previously never exercised (vacuous) — they now have teeth.
- **Compaction restart head discovery (found by the C3b oracle)**: `legacy_manifest::discover_legacy_state` ignored `levels/L*/` merged objects, so once compaction folded the fine L0 tail into a merged range whose `max` extends past the highest gen-folder TXID (e.g. `keep_fine_window = 0`, or a batch that reached the head), a watch RESTART discovered a stale-low head. It then resumed writing incrementals at a seq the merged range already owned (a chain fork) and, with `--on-startup`, published an eager base snapshot BELOW the merged coverage that poisoned restore-to-latest with a chain gap. Discovery now folds the max over `levels/L*/` range ends into the head (levels are not generations, so `max_generation` is untouched). Fail-on-revert: the unit test `discover_head_reflects_merged_level_coverage` and the DST replay `replay_compaction_restart_after_head_folded` (the shrunk `Compact → KillRestart → RestoreLatest` sequence the oracle surfaced).
- **Compaction C3a adversarial review**: the previously-unrun S3 CLI e2e was executed against live storage and root-caused three real defects plus test-fixture bugs. (1) **Verify false-alarmed on a healthy compacted bucket (critical)** — the snapshot→incremental chain check (`verify_ltx_chain`) was **not** level-aware (only the between-incrementals detector was), so once the fine L0 seqs were folded into merged levels it reported a phantom `TXID gap after snapshot chain: expected min_txid=2, got 12` and exited 5. It now bridges a merged-range-covered hole (and skips the by-design-broken L0 checksum link across the seam); periodic `validate_backup_integrity` got the same fix. (2) **A fully-promoted lower level was invisible to read consumers (critical)** — `list_merged_ranges` (verify), `compaction::restore::gather_candidates` (owned restore), and `prune::list_level_files` all stopped scanning at the first empty level. But an L1→L2 merge **deletes** its L1 sources, so the healthy steady state has an empty L1 above a populated L2; the early stop made verify hallucinate a gap, made owned-mode restore report a phantom `ChainGap` on a perfectly restorable bucket, and made prune under-collect. All three now probe every level up to the cap. New regression `restore_finds_l2_when_l1_is_fully_promoted_away`. (3) **Cache-bypass hardened** — the `levels/L*` restore-cache bypass now matches **path segments** structurally instead of a bare `"/levels/L"` substring, so it can never be tricked by a db name/prefix and never false-negative a real level object. (4) **Tamper matrix extended** — added a fail-on-revert proving a corrupted LTX-domain `declared_end` stamp on a merged object breaks the chain at the seam (linkage, not just the content checksum, is verified). (5) **e2e fixture bugs** — the CLI e2e writer auto-checkpointed every commit (rolling the WAL salt → a snapshot per commit → a non-contiguous L0 the liveness rule correctly refuses to merge, so levels never fired); it now drives a contiguous chain. The L1-presence assertion was a race (an L1→L2 merge empties L1) and now checks L1 was *ever* observed; the PITR-decay assertion read the wrong stream (walrust logs to stdout) and now checks combined output.
- **Compaction C2b adversarial review**: (1) **Exposure-vs-read-path gap (critical)** — `[compaction] enabled = true` drove the CLI watch to compact through the range layout (merging + deleting superseded L0), but the CLI restore path (`legacy_restore::restore_legacy_ltx`) is **not** wired to the leveled planner, so a CLI-compacted bucket was unrestorable by `walrust restore`. The CLI watch now **refuses to start** when compaction is enabled (`reject_cli_compaction`), pointing to library/owned mode; the config still parses. `walrust explain` and the README say so. Leveled compaction over the CLI restore path is deferred to a later wave. (2) **Merge shrink (VACUUM) correctness** — when a merge boundary spans a DB shrink, an intermediate source's high page survived last-writer-wins above the final end-page-count marker, which broke the ascending-emit invariant and produced a changeset with a live page past `end_page_count` (a debug-assert panic in tests; a checksum-mismatch/stuck merge in release). The merge now **drops orphan pages above the final DB size**, so shrink merges produce valid, restorable objects that truncate correctly. New `shrink_across_merge_drops_orphan_pages_and_stays_valid` test. (3) **Planner exact-start rule proven safe** — documented and tested (`merge_refuses_to_span_a_snapshot_boundary`) that snapshots consume their own seq and break the L0 chain, so no merged range can begin strictly inside a snapshot span; restore-from-snapshot always finds an object at `floor + 1` and the `range.min == need` rule never false-gaps a healthy bucket. (4) PITR-decay error wording tightened so the `keep_fine_window` hint can't be read as recovering already-merged points.
- **Adversarial review overhaul (PRs #9–#17)**: full second adversarial review found and fixed critical durability bugs across the WAL/checkpoint/upload/restore stack. Highlights: WAL checksum endianness was inverted (frame validation never ran on real SQLite WALs); checkpoint rollovers now re-anchor with a fresh snapshot (walrust-owned) or hard-fail until re-anchored (external/fenced modes); restore verifies the actual DB checksum chain with contiguity checks and writes to a temp file; canonical S3 key layout shared by uploader and restore; CAS + fsynced publish-intent closes a split-brain crash-window; interval-aware upload cursor with halt-on-gap policy; fsync before every ack; fenced follower reconstruction promoted to a production API (`reconstruct_fenced_follower`). Every fix has a proving test verified to fail with the fix disabled. CI (fmt/clippy/full workspace vs MinIO, sccache-cached) now gates every PR; the DST harness drives the production pipeline with real process-kill crash tests. Ledger: `ADVERSARIAL_REVIEW_2.md` (A1–A14, B1–B14 all Fixed/Verified; open edges in its DEFERRED register D1–D7). Working docs `SESSION_PROMPTS.md` and `PHASE4_PLAN.md` served the fix waves and were removed.
- **Deterministic TXID in WAL mode**: Phase Somme assumed SQLite's file change counter increments on every transaction, but in WAL mode it only updates during checkpoints. `sync_wal` and `take_snapshot` now fall back to WAL commit counting (number of frames with non-zero `db_size_after_commit`) when the change counter hasn't advanced. This is deterministic from file content: any process reading the same WAL bytes computes the same TXID. `read_frames_as_page_map` returns `commit_count` as a 5th tuple element. New `count_wal_commits()` scans WAL frame headers without reading page data (for `take_snapshot`). 4 new tests.

### Changed
- **Phase 1b: Migrate walrust + walrust-core to hadb-io** — eliminated ~3,200 lines of duplicate retry/S3/storage/webhook/retention/config code
  - walrust-core: deleted `retry.rs`, `s3.rs`, `storage.rs`; re-exports from hadb-io (88 tests passing)
  - walrust CLI: replaced `retry.rs` (642→2), `s3.rs` (471→2), `storage.rs` (182→5), `webhook.rs` (288→2), `retention.rs` (547→2) with thin re-export wrappers
  - `config.rs`: removed shared types (S3Config, WebhookConfig, CacheConfig, parse_duration_string), re-exported from hadb-io
  - Type renames applied: `SyncFailed`→`UploadFailed`, `X-Walrust-Signature`→`X-Hadb-Signature`, SnapshotEntry `filename`→`key` / `max_txid`→`sequence`
  - hadb-io now re-exports `aws_sdk_s3` crate for consumer type access
  - 303 tests passing, 0 failures

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
