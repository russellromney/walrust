# Adversarial Review — walrust

A bug-hunt of the WAL shipping / shadow / LTX / sync / restore / DST surface.
Each finding lists severity, location, the bug, the fix, and a Status:
**Fixed** (implemented + build green + tested), **Partial** (safest correct
fix landed; remainder deferred with a reason), or **Documented** (verified
real; fix specified). Line numbers are approximate against the reviewed
revision; re-locate before editing.

Status: F1–F13, F15 are **Fixed**. F14 is **Partial** — the mock-storage fault
fixes landed; the rest of the DST harness predates the current crate API and
needs a dedicated resurrection (details under F14).

---

## Fixed in this PR

### F4 — [High] LTX decode trusts an untrusted `page_num` → OOB panic / silent wrong image — **Fixed**
- `src/ltx.rs:90-100`, `crates/walrust-core/src/ltx.rs:78-99`
- `decode_to_db` indexed `db_data[start..start+page_size]` using a per-page
  `page_num` read from the (untrusted) LTX, with no `1 <= page_num <= commit`
  check, and sized the buffer with an unchecked `num_pages * page_size`. A
  corrupt/crafted LTX panicked (OOB slice) in the binary path and **silently
  dropped** the out-of-range page in the `walrust-core` path (producing a
  wrong byte image that still "verified").
- **Fix:** validate `page_size != 0`, use `checked_mul` for the image buffer,
  and reject any page number outside the valid range with a typed error instead
  of panicking or dropping.

### F1 — [High] `restore` reports success without reaching the target TXID — **Fixed**
- `src/sync/restore.rs:181-188`
- The apply loop set `final_txid` and printed "Restored …" then returned
  `Ok(())` with no check that `final_txid == target_txid`. A missing incremental
  or an end-of-chain gap produced a restore short of the requested point,
  reported as success → silent data loss.
- **Fix:** `ensure!(final_txid == target_txid, …)` after the loop. (Per-file
  pre/post checksum chaining is already verified in `apply_ltx_to_db`; this
  closes the "stopped early" case.)

### F5 — [High] `replicate` silently stalls on a TXID gap — **Fixed**
- `src/sync/replicate.rs:183-191`
- On a gap the loop `continue`d; every later file then also failed the
  contiguity check, so the replica froze forever while `replicate` returned
  `Ok`. The in-code comment even noted "for now just warn and continue".
- **Fix:** a gap is now a hard error that forces a re-bootstrap from the latest
  snapshot rather than skipping frames.

---

## Hardened in the follow-up pass

### F2 — [High] WAL frame checksum chain is never validated → torn tail frame shipped — **Fixed**
- `crates/walrust-core/src/wal.rs` (and `src/wal.rs`)
- The production frame readers parsed `page_number`/`db_size` but never verified
  the SQLite WAL cumulative checksum; the commit boundary was "last frame with
  `db_size > 0`". A torn tail frame whose 24-byte header carried a non-zero
  `db_size` was accepted as a commit.
- **Fix:** implemented the SQLite WAL checksum (`wal_checksum` — the s0/s1
  Fibonacci-weighted sum, big-/little-endian per the WAL magic
  `0x377f0682`/`0x377f0683`), plus `validate_header_checksum` and
  `verify_frame_checksum`. The production reader is now
  `read_frames_as_page_map_checked`, which seeds the chain from the validated
  header checksum (or the caller's running chain mid-WAL), verifies each frame,
  and stops at the first mismatch — a torn tail frame with a bogus non-zero
  `db_size` is no longer treated as a commit. The running chain is threaded
  through `SyncState` / `DbState` so incremental reads keep validating.
  Validation is skipped only for synthetic WALs with a zero header checksum
  (never a real SQLite WAL), so existing hand-built test WALs still parse.
  Golden-vector tests (`test_wal_checksum_golden_vector`) verify the algorithm
  against hand-computed values; torn-tail tests prove valid frames are accepted
  and corrupt ones rejected, in both crates.

### F3 — [High] Generation rollover is size-only; in-place WAL reset (new salt) mis-attributed — **Fixed**
- `crates/walrust-core/src/sync.rs` (all three sync sites), `src/sync/wal_sync.rs`
- Rollover was detected only by `current_size < wal_offset`. SQLite can reset
  the WAL in place with a new salt at the same/larger size; that was missed, so
  new-generation frames were read as a continuation of the old generation and
  the new prefix was skipped.
- **Fix:** threaded the WAL header salt into `SyncState` (`wal_salt`) and
  `DbState`. All three core sync sites now call a shared `read_next_wal_batch`
  helper that triggers rollover on a size shrink OR a salt change, resets the
  offset/generation and re-seeds the checksum chain. The binary sync path does
  the same two-pronged check inline. Salt is persisted in `state.json` and
  tracked even on no-op syncs.

### F13 — [High] `restore_with_snapshot_source` / `pull_incremental` apply with no chain verification — **Fixed**
- `crates/walrust-core/src/sync.rs` — `restore_with_snapshot_source`,
  `pull_incremental`, `pull_incremental_into_sink_inner`
- All three loops applied changesets in seq order with no `verify_chain`, so a
  stale object from a prior lineage at an in-range seq was applied wholesale.
- **Fix:** thread `current_checksum: Option<u64>` through each loop. The first
  changeset establishes the chain (the base isn't HADBP-encoded, so its prior
  checksum is unknown); every subsequent changeset is checked with
  `hadb_changeset::physical::verify_chain(prev, &changeset)` and the loop breaks
  on a chain break rather than applying. The sink path verifies *before*
  routing any pages so a mis-chained changeset is rejected whole.
  `pull_into_sink_stops_on_broken_chain` covers it; the multi-changeset
  lifecycle test was updated to seed properly chained fixtures.

### F10 — [Med] Durable cursor advances before the S3 PUT is durable — **Fixed**
- `src/cache.rs`, `src/uploader.rs`
- The exposed cursor advanced on cache-write / max(txid) before the uploader
  confirmed the PUT; a node reseeded from remote state believed un-uploaded
  TXIDs were restorable.
- **Fix:** added `last_contiguous_uploaded_txid` to the cache manifest — the
  highest TXID with a confirmed durable PUT *and* no gap below it. It advances
  only inside `mark_uploaded` (after a confirmed PUT) across the gap-free
  prefix, never on a mere cache write. The uploader exposes it in
  `UploaderStats.last_contiguous_uploaded_txid`. This is the safe restore
  cursor; `last_uploaded_txid` (max-based) is kept only for observability.

### F9 — [Med] `last_uploaded_txid = max(txid)` hides a permanent gap; uploader returns Ok on failed PUTs — **Fixed**
- `src/cache.rs`, `src/uploader.rs`
- **Fix:** `mark_uploaded` advances the contiguous cursor only across an
  unbroken `1..=T` run. Added `mark_failed` + a `failed_txids` set in the
  manifest; the uploader records every permanently-failed PUT (auth error or
  retries exhausted) so the gap is durable and surfaced via `failed_uploads()`
  / `CacheStats.failed_count` (the upload-failed webhook still fires). The
  contiguous cursor never advances past a failed or missing TXID. Tests cover
  out-of-order uploads, a failed-then-retried gap, and restart persistence.

### F8 — [Med] Cache cleanup can evict the only restorable copy — **Fixed**
- `src/cache.rs`, `src/sync/wal_sync.rs`
- **Fix:** added an `is_snapshot` flag to `CacheEntry` (set via the new
  `write_snapshot_ltx`, used for the initial base in `wal_sync`). Cleanup now
  computes a floor at the latest cached snapshot and never evicts it or any
  TXID at/after it (the restore base + its incremental chain), regardless of
  age or `max_cache_size`. Pending (not-yet-durable) uploads were already never
  evicted. Tests cover keeping a snapshot+chain under aggressive cleanup and
  evicting a superseded older base.

### F7 — [Med] Compaction deletes snapshots with no chain-reachability protection — **Fixed**
- `src/sync/compact.rs`
- **Fix:** before deleting, `compact` discovers the live incremental chain and
  pulls any reachability base out of the delete set: the highest-TXID snapshot
  (current restore base) and the latest snapshot at/below the earliest retained
  incremental's start. Rescued snapshots move to `keep` and their bytes are not
  counted as freed, so a retained incremental chain always has a base.

### F6 — [High] `compact` / `replicate` read a Manifest the watch path never writes — **Fixed**
- `src/sync/compact.rs`, `src/sync/replicate.rs`, `src/sync/manifest.rs`, `src/s3.rs`
- The production watch path discovers by S3 listing and never writes the
  `Manifest`, so `compact` was a silent no-op and `replicate` errored "No LTX
  files found".
- **Fix:** added `discover_snapshots_from_s3` and `discover_all_ltx_from_s3`
  to the manifest module (mirroring how `verify` lists generations). `compact`
  now discovers snapshots from the listing and HEADs each for size/last-modified
  (`s3::head_object_meta`) to build retention entries, deleting full S3 keys and
  no longer reading/writing a manifest. `replicate` discovers all LTX files
  (snapshots + incrementals) from the listing via `DiscoveredLtx`.

### F11 — [Med] `take_snapshot` checkpoints but leaves the WAL cursor untouched — **Fixed**
- `src/sync/wal_sync.rs`
- **Fix:** after the snapshot folds all WAL frames into the base, `take_snapshot`
  now resets `wal_offset` to 0, bumps `wal_generation`, re-reads the WAL header
  salt into `wal_salt`, and clears `wal_checksum_chain` so the next incremental
  read re-seeds from the new header (ties into F3). The snapshot's `db_checksum`
  is the explicit hand-off base for the first incremental.

### F12 — [Med] Shadow segment filename generation width mismatch — **Fixed**
- `src/shadow.rs`, `src/sync/shadow.rs`
- The writer used `{:08x}` (u32 width) for the generation while a test encoder
  used `{:016x}`; lexical order broke for generation `> 0xFFFF_FFFF`.
- **Fix:** one shared `format_segment_name(generation, index)` /
  `SEGMENT_HEX_WIDTH = 16` used by the writer and the test encoder. Parsing was
  already width-agnostic (`u64::from_str_radix`). A test asserts lexical ==
  numeric order past `u32::MAX`.

### F15 — [Low] Three inconsistent "is this a snapshot" definitions — **Fixed**
- `src/sync/manifest.rs`, `verify.rs`
- **Fix:** added one shared `is_snapshot(generation, min_txid, max_txid)` helper
  (`generation > 0 || (min == 1 && max == 1)`) and routed `verify`,
  `discover_snapshots_from_s3`, and `discover_all_ltx_from_s3` through it.

### F14 — [High-for-trust] DST harness does not exercise the faults it claims — **Partial**
- `walrust-dst/src/mock_storage.rs` (+ `chaos.rs`, `invariants.rs`, `properties.rs`)
- Fault-injection honesty fixes landed in `mock_storage.rs`:
  - `PartialWrite` now persists the truncated prefix and then surfaces the
    error, so a torn object is actually observable by readers (was: stored
    nothing).
  - `EventualConsistency` is now gated on a deterministic, seeded operation
    counter (`visible_after_ops`) instead of wall-clock time, so read- and
    list-after-write staleness is reproducible under a fixed seed.
  - `list_objects` honours that same visibility gate, modelling
    list-after-write staleness (was: always consistent).
  - Added tests: truncated-prefix persistence, deterministic EC visibility,
    and list-after-write staleness.
- Not done (reason): the rest of the harness — `chaos.rs`, `invariants.rs`,
  `properties.rs`, and `main.rs` — does not compile against the current crate.
  It imports a removed `walrust::testable` module and the mock implements an
  obsolete `StorageBackend` trait (`upload_bytes`/`download_bytes`/`bucket_name`)
  rather than the current `put`/`get`/`list`/`delete`/`put_if_absent`/`put_if_match`.
  The crate also is not a workspace member and fails a standalone build on a
  `links = "sqlite3"` rusqlite version clash. Resurrecting the `testable` API and
  porting the mock + property modules to the current trait is a dedicated change;
  the `chaos_silent_corruption` / `prop_recovery_under_failure` /
  `prop_point_in_time_restore` rewrites depend on that working harness and are
  deferred. The mock-level fault fixes above are correct against the trait the
  mock implements today.

---

## Test / build notes

- `cargo build` (the `walrust` bin + lib) is green for all Fixed findings.
- `walrust` unit tests pass: WAL checksum golden vectors + torn-tail, the
  restore/pull chain-break test, the cache durable-cursor / failed-gap / floor
  tests, the shadow segment-width test, plus the pre-existing suites.
  `crates/walrust-core` lib tests pass (run in that crate's directory).
- Live-network integration tests (S3-backed) are gated and not exercised here;
  `compact` / `replicate` discovery (F6) is verified by construction against the
  litestream layout, not against a live bucket.
- `walrust-dst` is not a workspace member and does not compile against the
  current crate API (see F14); its mock-storage fault fixes are landed but the
  harness as a whole needs a separate resurrection pass.
