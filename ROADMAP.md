# walrust Roadmap

## Vision

**Simple, reliable SQLite backups to S3 with integrity verification.**

Core differentiators:
- LTX format with SHA256 verification
- Lower memory footprint than Litestream
- Built for production: verify, explain, webhook alerting
- Honest about what works (no vaporware)

---

## Current Capabilities (v0.4.0)

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
- Point-in-time restore (by TXID or timestamp)
- Multi-database support
- Prometheus metrics + dashboard
- Webhook notifications (corruption, circuit breaker)
- Retry logic with circuit breaker
- Shadow WAL mode

---

## v0.5.0 — Chained Checksums + Performance

The #1 bottleneck: `compute_expected_post_with_overlay()` reads the entire database from disk and SHA-256 hashes it on every sync cycle (default: 1 second). For a 50MB DB, that's ~100MB I/O per second just for checksumming.

### Chained page checksums
- Switch incremental LTX checksums from full-DB hash to chained page hash
- `post_checksum = SHA-256(pre_checksum || page1_num || page1_data || ...)` — pages sorted by number
- Snapshots keep full-DB hash (data already in memory)
- Eliminates full DB read from hot path entirely
- Removes `wal_page_overlay` HashMap (only existed for full-DB checksum)

### Page clone elimination
- Move frame data instead of cloning during dedup (`for frame in frames` not `&frames`)
- Index-based sorting in `encode_wal_changes()` instead of `pages.to_vec()`

### Result
- Before: 50MB disk read + 50MB hash = ~100MB I/O per sync cycle
- After: 10 dirty pages × 4KB = 40KB hash per sync cycle

---

## Future Considerations (v1.0+)

**Not planning yet, but might be useful:**

### Disk-Based Upload Queue
- Litestream-style disk caching
- Decoupled WAL encoding from S3 uploads
- Crash recovery
- Local cache for fast restore

### Push-Based Read Replicas
- Push-based replication (requires network)
- Lower latency than polling

### Additional Features
- Multi-region replication
- Encryption at rest
- Concurrent S3 uploads in uploader

**Philosophy:** Ship working features, not roadmaps. Only add features when users ask for them.

---

## Completed Features (see CHANGELOG.md)

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
