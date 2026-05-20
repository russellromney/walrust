# Adversarial Review — walrust

A bug-hunt of the WAL shipping / shadow / LTX / sync / restore / DST surface.
Each finding lists severity, location, the bug, the fix, and a Status:
**Fixed** (implemented + build green) or **Documented** (verified real; fix
specified for a focused follow-up). Line numbers are approximate against the
reviewed revision; re-locate before editing.

This pass landed the three highest-severity crash / data-loss fixes. The
remaining findings are documented with exact fixes; several are large
(WAL checksum chain, generation-salt rollover, the DST harness) and warrant a
dedicated change with their own focused verification rather than being bundled
in unverified.

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

## Documented (verified real; fix specified)

### F2 — [High] WAL frame checksum chain is never validated → torn tail frame shipped — **Documented**
- `crates/walrust-core/src/wal.rs` (and `src/wal.rs`)
- The production frame readers parse `page_number`/`db_size` but never verify
  the SQLite WAL cumulative checksum; the commit boundary is "last frame with
  `db_size > 0`". A torn tail frame whose 24-byte header carries a non-zero
  `db_size` is accepted as a commit. The salt-aware `read_frames_with_metadata`
  exists but is **only called from tests** (dead in production — confirmed by
  the unused-function warning).
- **Fix:** implement the SQLite WAL checksum (the s0/s1 Fibonacci-weighted sum
  seeded from the two header salt words, summed big- or little-endian per the
  WAL magic `0x377f0682`/`0x377f0683`, over the header words then each frame's
  first 8 header bytes + page body), validate per frame, stop at the first
  mismatch, and wire it onto the production read path. Exacting (a wrong
  checksum rejects valid frames) — own change with golden-vector tests.

### F3 — [High] Generation rollover is size-only; in-place WAL reset (new salt) mis-attributed — **Documented**
- `crates/walrust-core/src/sync.rs:322-331` (+ `~1119`, `~742`), `src/sync/wal_sync.rs:119-135`
- Rollover is detected only by `current_size < wal_offset`. SQLite resets the
  WAL in place with a new salt at same/larger size; that is missed, so
  new-generation frames are read as a continuation of the old generation and
  the new prefix is skipped.
- **Fix:** thread the WAL header salt into `SyncState`; treat any salt change as
  the rollover trigger via the salt-aware reader. Pairs with F2.

### F13 — [High] `restore_with_snapshot_source` / `pull_incremental` apply with no chain verification — **Documented**
- `crates/walrust-core/src/sync.rs:966-1047`, `1225-1297`
- Both loops apply changesets in seq order with no `verify_chain`, so a stale
  object from a prior lineage at an in-range seq is applied wholesale.
- **Fix:** thread `current_checksum` through both loops and call the same
  `verify_chain(prev, &changeset)` `restore()` uses, breaking on mismatch.

### F10 — [Med] Durable cursor advances before the S3 PUT is durable — **Documented**
- `src/sync/wal_sync.rs:336-338,566-569` vs `src/uploader.rs:101-122`
- `current_txid` advances on cache-write before the uploader confirms the PUT;
  a node reseeded from remote `state.json` believes un-uploaded TXIDs are
  restorable.
- **Fix:** advance the exposed/persisted durable cursor only after
  `mark_uploaded` confirms the PUT (or persist a separate `durable_txid`).
  Coordinate with F9.

### F9 — [Med] `last_uploaded_txid = max(txid)` hides a permanent gap; uploader returns Ok on failed PUTs — **Documented**
- `src/cache.rs:252`, `src/uploader.rs:113-115,155-159,294-297`
- **Fix:** track `last_contiguous_uploaded_txid` (advance only when
  `txid == last+1`); surface a non-zero failed count as an error/alarm.

### F8 — [Med] Cache cleanup can evict the only restorable copy — **Documented**
- `src/cache.rs:296-355`
- **Fix:** floor that always retains the latest snapshot + its incremental chain
  regardless of size; never evict an uploaded file whose S3 object is not
  confirmed durable.

### F7 — [Med] Compaction deletes snapshots with no chain-reachability protection — **Documented**
- `src/sync/compact.rs:31-46,98-123`
- **Fix:** before deleting a snapshot, ensure no retained incremental chains from
  it, or delete dependents atomically and advance the floor.

### F6 — [High] `compact` / `replicate` read a Manifest the watch path never writes — **Documented**
- `src/sync/compact.rs:24-29`, `src/sync/replicate.rs:118-121`
- The production watch path discovers by S3 listing and never writes the
  `Manifest`, so `compact` is a silent no-op and `replicate` errors "No LTX
  files found".
- **Fix:** make both discover from S3 listing (mirror `verify.rs` /
  `restore.rs`).

### F11 — [Med] `take_snapshot` checkpoints but leaves the WAL cursor untouched — **Documented**
- `src/sync/wal_sync.rs:664-721`
- **Fix:** after `checkpoint_wal`, re-read the WAL header salt and reset
  `wal_offset` / bump generation (ties into F3); make the snapshot→incremental
  checksum hand-off explicit.

### F12 — [Med] Shadow segment filename generation width mismatch — **Documented**
- `src/shadow.rs:239-243` uses `{:08x}` (u32) while parser/encoder use `u64` and
  tests use `{:016x}`; lexical order breaks for generation `> 0xFFFF_FFFF`.
- **Fix:** one shared 16-hex-digit format constant in writer, parser, tests.

### F15 — [Low] Three inconsistent "is this a snapshot" definitions — **Documented**
- `src/sync/manifest.rs:104-163`, `verify.rs:208-210`
- **Fix:** one shared `is_snapshot(generation, min_txid, max_txid)` helper.

### F14 — [High-for-trust] DST harness does not exercise the faults it claims — **Documented**
- `walrust-dst/src/mock_storage.rs`, `chaos.rs`, `invariants.rs`
- `PartialWrite` stores nothing on overflow; `EventualConsistency` visibility is
  wall-clock (non-deterministic); `list_objects` is always consistent;
  `chaos_silent_corruption` never touches storage; `prop_recovery_under_failure`
  passes vacuously when paths fail; `prop_point_in_time_restore` snapshots after
  every insert so never replays an incremental chain.
- **Fix:** store the truncated prefix on `PartialWrite`; gate EC visibility on
  the seeded RNG; add a list-after-write staleness fault; add a
  corruption-detected restore test; make recovery/PITR properties assert real
  outcomes against an incremental chain.

---

## Test / build notes

- `cargo build --workspace` green; the Fixed cluster compiles.
- Live-network integration tests (S3-backed) are gated and not exercised here.
