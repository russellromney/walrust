# Phase 4 convergence — remainder plan (Wave 3b)

The bulk of Session-8 convergence already landed: `src/` collapsed into
`crates/walrust-core` `legacy_*` modules with re-export shims, one WAL / LTX /
shadow / cache / uploader / sync / restore engine. This plan covers only the
REMAINDER, in the orchestrator's priority order. One plan step per commit;
full relevant tests green after each; every behavior change gets a proving
test that fails without it.

## Inventory of what remains

Read from ADVERSARIAL_REVIEW_2.md (all residual/deferred lines) + the
Session-8 overrides.

1. **Fenced follower is not production API.** The fence-enforcing follower
   reconstruction (seq/epoch/writer fence + BLAKE3 envelope chain +
   base-checksum-anchored apply) lives only as `fenced_follower_reconstruct`,
   an executable spec inside `walrust-dst/src/invariants.rs`. Production code
   exposes the raw pieces (`list_delta_envelopes_after`, `external_delta`,
   `ltx::apply_changeset_to_db`) but not the composed, fence-enforcing
   reconstruct. Spec and implementation have not converged.

2. **B14 is genuinely open** (no Status line): `list_delta_envelopes_after`
   never asserts the decoded `payload.seq` equals the key-derived seq. A
   mislabeled envelope (right key, wrong inner seq) is returned unflagged.

3. **Typed errors end-to-end (scope 2).** Prior Phase-4.2 already replaced
   substring classification in `errors.rs`/`sync.rs`/`replicator.rs` with typed
   `WalrustError` downcasts (verified by grep: the only remaining `.contains(`
   on error strings are test assertions and non-error string ops). Remaining
   work: keep the contract intact and make the NEW follower fence rejections
   typed, not stringly.

4. **Naming/doc honesty (scope 3).** `WAL_MAGIC_BE/LE` were already corrected
   in Phase 1 (LE=0x377F_0682, BE=0x377F_0683 — verified). Core `ltx.rs`
   header already says HADBP. Remaining lies: `external_delta.rs` calls HADBP
   payloads "raw LTX bytes" (4 places); the core `ltx` module name is a legacy
   alias that its doc does not flag as such.

5. **WAN flake (scope 4).** `tests/production_e2e.rs` hard-codes a 30s
   `flush_until_frames` deadline (and 30s/20s/10s poll deadlines) — a known
   WAN-sensitive knob. Make it env-tunable (cheap), not a fixed magic number.

6. **Residual register (scope 4).** Open residuals to triage:
   writer-lease/split-brain immunity token (B11), core first-checkpoint window
   (A3), shadow downtime-checkpoint completeness (B4), rollover CheckpointDetected
   webhook variant beyond observer (A3), A5 drain-at-commit-boundary invariant,
   B13 cross-generation same-(min,max) cache collision, plus MEDIUM/LOW
   (config glob warn+skip, clap-over-toml, exit-path webhook via tokio::spawn,
   legacy manifest unbounded growth).

7. **Final sweep (scope 5).** README/ROADMAP claims + ADVERSARIAL_REVIEW.md
   (v1) header should note the second review + fix waves.

## Ordered steps (one commit each)

- **S0 (this file).** Commit the plan.
- **S1 — Fenced follower production API + B14.** Lift the reconstruct logic
  into `walrust-core::sync` as `reconstruct_fenced_follower` +
  `FencedFollowerCursor`/`FencedFollowerResult`, exported from `lib.rs`. Fence
  violations are typed `WalrustError::Integrity` whose messages still name the
  fence ("epoch fence" / "writer fence" / "envelope chain break") for honesty.
  Add the B14 seq-binding check inside `list_delta_envelopes_after` (hard error
  on `payload.seq != key seq`). Rewrite the DST `prop_fenced_delta_restore` so
  its follower loop CALLS the production API (spec == impl). Proving tests:
  (a) core `reconstruct_fenced_follower_replays_published_deltas` — drives the
  real `sync_wal_fenced_delta` writer over a real rusqlite WAL, reconstructs a
  follower, asserts integrity + row equality (production path, not a fixture);
  (b) core `reconstruct_fenced_follower_rejects_{epoch,writer,chain}` — forged
  head+1 envelope rejected before apply; (c) core B14
  `list_delta_envelopes_after_rejects_seq_key_mismatch`. Do NOT weaken the
  fence.
- **S2 — Naming/doc honesty.** `external_delta.rs`: "raw LTX bytes" →
  "raw HADBP changeset bytes" (least-churn doc fix; the field name
  `ltx_payload` is public API used by DST + shims, so renaming it is high-churn
  and out of scope — documented as such). Core `ltx.rs`: add a header line that
  the module name is a legacy alias for the HADBP codec. Doc-only, no behavior
  change (so no new proving test; existing golden-hash test guards the wire).
- **S3 — WAN flake env-tunable.** Add `e2e_poll_deadline(default_secs)` reading
  `WALRUST_E2E_DEADLINE_SECS` (absolute override) with the existing values as
  defaults; route the four poll deadlines through it. Proving test: a small
  unit that the parser honors the env override and falls back to the default.
- **S4 — Docs sweep + DEFERRED register + statuses.** Update B14 Status,
  Phase-4.2/4.3 residual notes, and add a `DEFERRED (final)` register at the
  bottom of ADVERSARIAL_REVIEW_2.md (per item: risk, trigger, suggested fix).
  Update README + ROADMAP "Current Capabilities" + ADVERSARIAL_REVIEW.md (v1)
  header with one honest paragraph on the second review + fix waves.

## What I will NOT do, and why

- **Not renaming the core `ltx` module or the `ltx_payload` field.** Both are
  public crate API (`walrust_core::ltx`, re-exported to root and used by DST).
  A rename is pure churn across two trees for zero behavior change; the
  orchestrator explicitly allows a doc fix when renames would touch the public
  API. I document the alias instead.
- **Not implementing the writer-lease / split-brain immunity token.** It is a
  distributed-fencing feature (a real lease store), far larger than this wave;
  the local `PublishIntent` authorship proof already closes the silent
  split-brain re-legitimization within its scope. DEFERRED with a register
  entry.
- **Not adding a dedicated `CheckpointDetected` webhook variant.** The
  `hadb-io` webhook enum is a pinned external dependency (Phase-0 decision);
  the rollover already rides the `upload_failed` channel with a distinct
  message and the `RolloverObserver` covers library embedders. DEFERRED.
- **Not fixing the core first-checkpoint window or shadow downtime-checkpoint
  completeness.** Both were attempted and reverted in Phase 2B because the
  cheap detection signals false-positive on benign PASSIVE folds and would
  weaken the honest pinned-reader E2Es. Exposure is bounded by
  `snapshot_interval`. DEFERRED with triggers.
- **Not changing config-glob warn→error or clap-over-toml precedence.** Not on
  a durability path; changing them risks surprising existing configs. DEFERRED.
- **Not re-planning the completed convergence** (the `legacy_*` module moves).

## Risk notes

- The DST property is the primary production-path proof for the fenced
  follower; it uses the deterministic `MockStorageBackend` (not S3-gated), so
  it runs in plain `cargo test`. The added core tests are likewise non-gated.
- Fence errors must stay typed AND keep their descriptive substrings so the
  DST negative assertions and the exit-code contract both hold.
- `list_delta_envelopes_after` gaining a hard error is a behavior change: any
  existing caller relying on it silently tolerating a mislabeled envelope would
  now fail — that is the intended fail-closed direction and is the correct
  reading of B14.
