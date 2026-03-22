# walrust Roadmap

## Vision

**Simple, reliable SQLite backups to S3 with integrity verification.**

Core differentiators:
- LTX format (Litestream-compatible) with SHA256 verification
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
- LTX format (Litestream-compatible)
- Point-in-time restore (by TXID or timestamp)
- Multi-database support
- Prometheus metrics + dashboard
- Webhook notifications (corruption, circuit breaker)
- Retry logic with circuit breaker
- Shadow WAL mode
- 346 tests passing, 0 ignored

---

## Future Considerations (v1.0+)

**Not planning yet, but might be useful:**

### Disk-Based Upload Queue
- Litestream-style disk caching
- Decoupled WAL encoding from S3 uploads
- Crash recovery
- Local cache for fast restore
- **Effort:** ~2 weeks

### Performance Optimization
- Break the 5K w/s throughput ceiling
- Achieve 10K+ w/s at 250 databases
- CPU parallelization
- Batch S3 uploads
- **Effort:** ~1 week

### Read Replicas
- Push-based replication (requires network)
- Lower latency than polling
- **Effort:** ~3 days

### Additional Features
- Multi-region replication
- Encryption at rest
- Python API expansion
- Dashboard improvements

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
