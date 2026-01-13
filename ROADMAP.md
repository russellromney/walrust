# walrust Roadmap

## Vision

Litestream-compatible SQLite sync in Rust. Optimized for multi-tenant deployments (Cinch, Tenement).

**Core differentiators:**
- LTX format (Litestream-compatible) with SHA256 verification on top
- Lower memory footprint (~12MB vs ~33MB)
- Built-in dashboard + Prometheus metrics
- Opinionated defaults (grandfather/father/son retention)

## v0.3 Highlights (Current Alpha)

**Major Achievement:** Full LTX format integration with Litestream compatibility

What works now:
- ✅ **Snapshots as LTX files** - Compressed, checksummed, Litestream-compatible
- ✅ **Point-in-time restore** - By TXID or timestamp with manifest tracking
- ✅ **Binary preservation** - Byte-for-byte identical restore verified
- ✅ **Real S3 testing** - 105 tests including 23 integration tests on Tigris
- ✅ **Multi-database** - Single process handles multiple SQLite databases

What's next:
- ✅ **Compaction & retention** - GFS rotation with configurable retention
- ✅ **Config file support** for multi-DB deployments
- ✅ **Smart sync triggers** (reduce snapshot frequency)
- ✅ **Dashboard & metrics** for observability

---

## Alpha (v0.3) - Target Scope

### Core Commands
```bash
walrust watch <db>... [--config file]   # Watch and sync
walrust snapshot <db>                    # Immediate snapshot
walrust restore <name> -o <output>       # Restore database
walrust list                             # List backups
walrust compact <name> -b <bucket>       # Clean up old snapshots
walrust replicate <source> --local <db>  # Poll-based read replica
walrust explain [--config file]          # Show config summary (dry-run)
walrust verify <name> -b <bucket>        # Verify LTX integrity in S3
```

**Compaction Usage:**
```bash
# Dry-run (default) - show what would be deleted
walrust compact mydb -b s3://my-bucket

# Custom retention policy
walrust compact mydb -b s3://my-bucket --hourly 48 --daily 14

# Actually execute compaction
walrust compact mydb -b s3://my-bucket --force

# Auto-compact in watch mode (after each snapshot)
walrust watch mydb.db -b s3://my-bucket \
  --compact-after-snapshot \
  --retain-hourly 24

# Periodic compaction (every hour)
walrust watch mydb.db -b s3://my-bucket \
  --compact-interval 3600
```

### LTX Format Integration
- [x] Add `litetx` dependency (done)
- [x] Basic encode/decode functions (done)
- [x] Replace raw snapshot uploads with LTX files (done - v0.3)
- [x] Point-in-time restore from LTX files (done - v0.3)
  - [x] Restore by TXID (e.g., `--point-in-time txid:12345`)
  - [x] Restore by timestamp (e.g., `--point-in-time 2024-01-15T10:30:00Z`)
  - [x] Binary data preservation verified with extensive tests
- [x] manifest.json tracking with LtxEntry metadata (done - v0.3)
- [x] Store incremental WAL changes as LTX (done - v0.3)
  - [x] Checksum chaining (pre_apply_checksum → post_apply_checksum)
  - [x] WAL page deduplication (keep only latest version of each page)
  - [x] In-place LTX apply for efficient incremental restore
  - [x] Track db_checksum in state, recompute from db on restart

### Sync Triggers
```toml
[sync]
snapshot_interval = 3600  # Full snapshot every hour (seconds)
max_changes = 100         # Sync after N WAL frames
max_interval = 600        # Or after N seconds (whichever first)
on_idle = 300             # Snapshot after 5 min idle (seconds, 0 = disabled)
on_startup = true         # Snapshot when watch starts
```

### Retention (Grandfather/Father/Son)
```toml
[retention]
hourly = 24            # Keep 24 hourly snapshots
daily = 7              # Keep 7 daily snapshots
weekly = 12            # Keep 12 weekly snapshots
monthly = 12           # Keep 12 monthly snapshots
```

**Status:** ✅ IMPLEMENTED (v0.3)

**Architecture:**
- **Time-based categorization:** Snapshots categorized into hourly/daily/weekly/monthly tiers based on age
- **Bucketing strategy:** Group snapshots by time buckets (hour/day/week/month), keep latest from each bucket
- **Safety guarantees:**
  - Always keep latest snapshot
  - Keep minimum 2 snapshots
  - Dry-run by default (require `--force` to delete)
  - Atomic manifest updates

**Retention Logic:**
```
Snapshot age < 24 hours  → Hourly tier   (keep 24)
Snapshot age < 7 days    → Daily tier    (keep 7)
Snapshot age < 12 weeks  → Weekly tier   (keep 12)
Snapshot age >= 12 weeks → Monthly tier  (keep 12)
```

**Example:** 100 snapshots spanning 6 months:
- Keep: Latest + 24 hourly + 7 daily + 12 weekly + 12 monthly ≈ 56 snapshots
- Delete: 44 oldest snapshots
- Free: ~1.5 GB storage

**Implementation Files:**
- `src/retention.rs` (NEW) - Categorization, bucketing, selection algorithm
- `src/s3.rs` - Add delete_object() and delete_objects()
- `src/sync.rs` - Add compact() orchestration
- `src/main.rs` - Add Compact subcommand + watch flags

### Metrics
- Prometheus `/metrics` endpoint at `--metrics-port` (default: 16767)
- Always on unless `--no-metrics`, localhost-only binding
- Metrics: last_sync, wal_size, next_snapshot, error_count, snapshot_count, current_txid, uptime

### Multi-Database
```toml
[[databases]]
path = "/data/*.db"    # Wildcard support
prefix = "tenant"

[[databases]]
path = "/data/app.db"
prefix = "app"
snapshot_interval = 1800  # Per-DB override (seconds)
```

### Data Integrity
- SHA256 checksum in S3 metadata (existing)
- LTX CRC64 checksum (from litetx)
- Reject partial uploads on restore
- Graceful shutdown: complete in-flight uploads (5s timeout)

---

## Next Steps for Alpha Completion

### Priority 1 - Core Functionality
1. **Compaction & Retention** ✅ COMPLETE
   - [x] Create `src/retention.rs` module with GFS categorization logic
   - [x] Implement retention policy: 24 hourly, 7 daily, 12 weekly, 12 monthly
   - [x] Add S3 delete functions to `src/s3.rs`
   - [x] Add `walrust compact` command with dry-run default
   - [x] Add auto-compact flags to `watch` command
   - [x] Write comprehensive unit and integration tests

2. **Config File Support** ✅ COMPLETE
   - [x] TOML config parsing with `serde`
   - [x] Per-database settings (prefix, snapshot_interval, retention)
   - [x] Wildcard database paths (`/data/*.db`)
   - [x] Config validation and error reporting

3. **Sync Triggers** ✅ COMPLETE
   - [x] `max_changes` - sync after N WAL frames
   - [x] `max_interval` - or after N seconds (whichever first)
   - [x] `on_idle` - snapshot after idle period
   - [x] `on_startup` - snapshot when watch starts

### Priority 2 - Observability
4. **Metrics** ✅ COMPLETE
   - [x] Prometheus `/metrics` endpoint at `--metrics-port` (default: 16767)
   - [x] Localhost-only binding (127.0.0.1), graceful port conflict handling
   - [x] Metrics: last_sync, wal_size, next_snapshot, error_count, snapshot_count, current_txid, uptime

### Priority 3 - Advanced Features
5. **Incremental WAL as LTX** ✅ COMPLETE
   - [x] WAL changes encoded as incremental LTX (not raw segments)
   - [x] Checksum chaining for LTX integrity verification
   - [x] In-place apply_ltx_to_db for efficient restore
   - [x] Comprehensive tests (105 total, all passing)

---

## Post-Alpha Features

### CLI Improvements
- [ ] **Structured exit codes** - Specific exit codes for different error types (S3, database, checksum, etc.)
- [ ] **JSON logging** (maybe) - Structured log output for log aggregation systems

### Read Replicas (Poll-based) ✅ COMPLETE
```bash
walrust replicate s3://bucket/mydb --local replica.db --interval 5s
```
- ✅ Polls S3 for new LTX files at configurable interval
- ✅ Auto-bootstraps from latest snapshot if local db doesn't exist
- ✅ Applies incremental LTX files in-place (efficient page writes)
- ✅ TXID-based tracking with `.db-replica-state` file for resume
- ✅ Gap detection and automatic re-bootstrap when needed
- No network required between primary/replica

### Read Replicas (Push-based) - Future
```bash
# Primary
walrust watch mydb.db --push-to http://replica:8080

# Replica
walrust serve --port 8080 --db replica.db
```
- Lower latency than polling
- Requires network connectivity

### Additional Commands
```bash
walrust verify <name> -b s3://...       # ✅ Verify LTX checksums + TXID continuity
walrust explain [--config file]         # ✅ Show config summary without running
```

---

## Design Decisions

### Single Writer
- Enforced at S3 level (conditional writes)
- No multi-writer support (use orchestration for HA)
- Simpler failure modes

### No Shadow WAL
- Let SQLite checkpoint freely
- Detect WAL reset, take new snapshot
- Trade: more snapshots vs simpler code

### LTX vs Custom Format
- Use `litetx` crate (Superfly/Fly.io maintained)
- Litestream-compatible storage format
- Add SHA256 verification on top of LTX CRC64

### Checkpointing
- Transaction-aware recording (like Litestream v0.5+)
- Don't block SQLite checkpoints
- Re-snapshot when WAL continuity breaks

---

## S3 Layout (LTX-based)

```
s3://bucket/prefix/
├── mydb/
│   ├── 00000001-00000001.ltx     # Snapshot (TXID 1-1)
│   ├── 00000002-00000010.ltx     # Incremental (TXID 2-10)
│   ├── 00000011-00000050.ltx     # Incremental (TXID 11-50)
│   ├── 00000001-00000050.ltx     # Compacted (TXID 1-50)
│   └── manifest.json             # Index of LTX files
└── otherdb/
    └── ...
```

---

## Current Status

### v0.3 (Current - Alpha)
- [x] **LTX Format Integration**
  - [x] Snapshots stored as LTX files (Litestream-compatible)
  - [x] manifest.json tracking with TXID sequencing
  - [x] Point-in-time restore by TXID or timestamp
  - [x] Binary data preservation with extensive test coverage
  - [x] 105 total tests (all passing)
- [x] **Snapshot Compaction & Retention**
  - [x] GFS rotation (hourly/daily/weekly/monthly tiers)
  - [x] `walrust compact` command with dry-run default
  - [x] Auto-compaction in watch mode (--compact-after-snapshot, --compact-interval)
  - [x] Batch S3 delete operations
- [x] **Poll-based Read Replicas**
  - [x] `walrust replicate` command with configurable poll interval
  - [x] Auto-bootstrap from latest snapshot
  - [x] In-place incremental LTX apply
  - [x] TXID tracking with resume capability
- [x] **Operational Commands**
  - [x] `walrust explain` - Show config summary without running
  - [x] `walrust verify` - Verify LTX integrity (checksums, TXID continuity, --fix)
- [x] WAL sync to S3/Tigris as incremental LTX files
- [x] SHA256 checksums in S3 metadata
- [x] Multi-database support (single process)
- [x] Snapshot scheduling (time-based intervals)
- [x] Python bindings

### v0.2 (Previous)
- [x] Basic WAL sync
- [x] Simple snapshot/restore
- [x] S3/Tigris compatibility

---

## References

- [Litestream Revamped](https://fly.io/blog/litestream-revamped/) - LTX format, multi-DB
- [Litestream v0.5.0](https://fly.io/blog/litestream-v050-is-here/) - Compaction levels
- [litetx crate](https://docs.rs/litetx/) - Rust LTX implementation
- [Litestream How It Works](https://litestream.io/how-it-works/) - WAL mechanics
