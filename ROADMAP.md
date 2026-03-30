# walrust Roadmap

## Vision

**Simple, reliable SQLite backups to S3 with integrity verification.**

Core differentiators:
- LTX format with SHA256 verification
- Lower memory footprint than Litestream
- Built for production: verify, explain, webhook alerting
- Honest about what works (no vaporware)

---

## Current Capabilities (v0.6.0)

**Core features that work:**
- `walrust watch` - Watch and sync multiple databases
- `walrust snapshot` - Take immediate snapshot
- `walrust restore` - Restore database from S3
- `walrust list` - List backups
- `walrust compact` - Clean up old snapshots with GFS retention
- `walrust replicate` - Poll-based read replica
- `walrust explain` - Configuration preview with cost estimation
- `walrust verify` - Backup integrity verification with exit codes
- LTX format with SHA256 verification
- Chained page checksums (O(changed pages) not O(entire DB))
- Point-in-time restore (by TXID or timestamp)
- Multi-database support
- Prometheus metrics + dashboard
- Webhook notifications (corruption, circuit breaker)
- Retry logic with circuit breaker
- Shadow WAL mode
- Constant RSS regardless of write throughput (~23-31MB with streaming + mimalloc)
- Shared infrastructure via hadb-io (S3, retry, webhooks, retention)

---

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

## SnapshotSource trait (turbolite integration)

Pluggable snapshot source for restore/recovery. Instead of downloading an LTX snapshot,
walrust calls a trait method to materialize the base DB. turbolite implements this using
S3 page groups as the snapshot.

- [ ] `SnapshotSource` trait in walrust-core with `materialize()` and `checkpoint_version()`
- [ ] `restore()` accepts optional `SnapshotSource`, falls back to LTX snapshot if absent
- [ ] `pull_and_apply_incrementals()` works with externally-materialized DB files

## Rename to walsync (future)

walrust is Rust-specific. For cross-language composability (Python/Node/Go SDKs), rename to
walsync. The Rust crate stays walsync-core, packages are walsync-python, walsync-node, etc.

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
