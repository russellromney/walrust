# walrust Roadmap

## Vision

Litestream-compatible SQLite sync in Rust. Optimized for multi-tenant deployments (Cinch, Tenement).

**Core differentiators:**
- LTX format (Litestream-compatible) with SHA256 verification on top
- Lower memory footprint (~12MB vs ~33MB)
- Built-in dashboard + Prometheus metrics
- Opinionated defaults (grandfather/father/son retention)

## v0.1.8 Plan (Performance Optimization)

**Goal**: Break the 5K w/s throughput ceiling to achieve 10K+ w/s at 250 databases.

### Phase 1: Quick Wins ✅ COMPLETE (2026-01-15)
- [x] Pre-allocate Vec buffers for LTX encoding (2x estimated size)
- [x] Configure S3 client with HyperClientBuilder for connection pooling
- [x] Document 0.5s sync interval option for aggressive batching
- **Result**: Reduced memory allocations, improved S3 concurrency

### Phase 2: CPU Parallelization ✅ COMPLETE (2026-01-15)
- [x] Offload CPU-bound LTX encoding to tokio blocking thread pool
- [x] Added rayon dependency for future parallel expansion
- [x] Applied to all WAL sync functions (standard and shadow modes)
- **Result**: Encoding no longer blocks async I/O, better CPU utilization

### Phase 3: Batch S3 Uploads (PENDING)
- [ ] Batch multiple small LTX files into larger uploads
- [ ] Implement S3 multipart uploads for efficiency
- [ ] Reduce S3 API call rate by 10-50x
- **Status**: Deferred pending Phase 1+2 benchmark results
- **Expected gain**: Additional 30-50% throughput increase

### Success Metrics
**Baseline (before optimizations):**
- 100 dbs: 4,989 w/s (99.8% of 5K target) ✅
- 250 dbs: 4,194 w/s (33.5% of 12.5K target) ❌
- 400 dbs: 2,295 w/s (11.5% of 20K target) ❌

**Target (after Phase 1+2):**
- 100 dbs: 5,000+ w/s (100%+ of target) ✅
- 250 dbs: 10,000+ w/s (80%+ of target) 🎯
- 400 dbs: 15,000+ w/s (75%+ of target) 🎯

**Memory Trade-off:**
- Before: ~20 MB per walrust instance
- After: ~50-100 MB (acceptable for massive throughput gain)

### Next Steps
1. Run comprehensive benchmarks to measure actual gains
2. Document results in CHANGELOG.md
3. Decide if Phase 3 (batch uploads) is needed
4. Plan next features (HA mode, remote orchestrator, etc.)

---

## v0.1.7 Plan (Next)

**Goal**: Production confidence via real S3 testing and improved soak test accuracy.

### Phase 1: Soak Test Warmup Fix ✅ COMPLETE
- [x] Add warmup period (5s default) before memory baseline measurement
- [x] Warmup runs typical operations (writes, syncs, snapshots) to stabilize memory
- [x] Add `--warmup-secs` CLI flag for configurability

### Phase 2: Real S3 Integration Testing ✅ COMPLETE
- [x] `walrust-dst s3-test` command for real Tigris/S3 testing
- [x] Configurable via `S3_TEST_BUCKET` and `AWS_ENDPOINT_URL_S3` env vars
- [x] 12 integration tests covering core functionality, edge cases, and error handling:
  - Basic operations: basic_upload_download, snapshot_restore, incremental_sync
  - Advanced features: point_in_time, concurrent_snapshots
  - Scale testing: large_database (10MB+), binary_data (BLOB), many_incrementals (50+), large_wal (1000+ frames)
  - Error handling: manifest_corruption, corruption_detection, missing_files
- [x] Automatic cleanup after tests (delete test objects)

### Future Considerations (Deferred)
- **Virtual Time**: Deterministic chaos replay with controlled time
- **Checkpoint/Restart**: Resume long soak tests from checkpoint
- **CI Nightly Runs**: Automated 24h soak tests in GitHub Actions

---

## v0.1.6 Highlights (Current)

- ✅ **PITR Bug Fixed** - `testable::restore` now correctly parses point-in-time
  - Supports `txid:N` format (e.g., `txid:12345`)
  - Supports ISO8601 timestamp format
  - Proper snapshot + incrementals selection for target TXID
  - All 7 invariants now tested (un-ignored `test_prop_point_in_time_restore`)
- ✅ **Production Hardening (walrust-dst)**
  - `stress` command: Multi-DB stress testing with 20% fault injection
  - `soak` command: Long-running stability testing with memory trend analysis
  - Resource leak detection (memory and FD tracking)
- ✅ **174 tests** (140 walrust + 34 walrust-dst)

**Known Issues:**
- Soak test memory warning is false positive for short durations (startup overhead)
  - Fix planned for v0.1.7 with warmup period

---

## v0.1.5 Highlights (Previous)

- ✅ **StorageBackend Trait** - Abstraction for S3 operations enabling testability
  - `StorageBackend` trait with `S3Backend` implementation
  - `walrust::testable` module exposing sync functions for DST
- ✅ **DST Framework (walrust-dst)** - Deterministic Simulation Testing
  - `MockStorageBackend` with fault injection (RandomError, Latency, PartialWrite, SilentCorruption, EventualConsistency)
  - Property-based tests (7 properties, 100+ cases each)
  - Real chaos tests calling actual walrust sync functions
  - 22 tests passing
- ✅ **154 tests** - Comprehensive test coverage (132 walrust + 22 walrust-dst)

## v0.1.4 Highlights (Previous)

- ✅ **Monitor Interval** (`monitor_interval`) - File watcher debouncing to reduce CPU usage
- ✅ **Validation Interval** (`validation_interval`) - Automated periodic backup integrity verification
- ✅ **Validation Metrics** - Prometheus metrics for backup validation
- ✅ **132 tests** - Comprehensive test coverage including 20 integration tests on Tigris

## v0.1.3 Highlights (Previous)

**Major Achievement:** Full LTX format integration with Litestream compatibility

- ✅ **Snapshots as LTX files** - Compressed, checksummed, Litestream-compatible
- ✅ **Point-in-time restore** - By TXID or timestamp with manifest tracking
- ✅ **Binary preservation** - Byte-for-byte identical restore verified
- ✅ **Multi-database** - Single process handles multiple SQLite databases
- ✅ **Compaction & retention** - GFS rotation with configurable retention
- ✅ **Config file support** for multi-DB deployments
- ✅ **Smart sync triggers** (reduce snapshot frequency)
- ✅ **Dashboard & metrics** for observability
- ✅ **WAL Checkpoint Controls** - Production-grade WAL management

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
- Graceful shutdown: complete in-flight uploads (5s timeout) ✅ COMPLETE

### Concurrent Database Processing (CRITICAL)
- Current: WAL syncs process databases SEQUENTIALLY in a for loop
- Problem: At 100 DBs, each sync cycle takes 100x longer than needed
- Solution: Use `futures::join_all` or `tokio::spawn` to sync all DBs concurrently
- Location: `src/sync.rs` lines 658-740, the `wal_sync_timer.tick()` branch
- Challenge: Mutable state borrows - need to restructure to:
  1. Clone/collect sync tasks with immutable state reads
  2. Spawn concurrent futures for S3 uploads
  3. Collect results and update state sequentially after
- Benchmark showed: walrust 3896 w/s vs litestream 4956 w/s at 100 DBs x 50 w/s
- Memory was "stable" at 12MB because only 1 DB processed at a time (bug!)

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

### Shadow WAL (v0.1.9 - Complete) ✅

**Status**: Fully integrated. Use `--shadow-wal` flag to enable.

The shadow WAL architecture matches Litestream's approach for better performance at high write rates:

1. **Read Transaction Blocker**: Holds open read transaction to prevent SQLite auto-checkpoint
2. **Shadow Directory**: Copies WAL frames to `.walrust-{dbname}/` as they arrive
3. **Decoupled Uploads**: S3 uploads read from shadow (not active WAL)
4. **Manual Checkpoint**: Walrust triggers checkpoints when ready

**Benefits**:
- No file contention between SQLite writes and S3 uploads
- Checkpoint control (prevents race conditions)
- Preserved history (shadow keeps frames even after checkpoint)
- Better throughput at 100+ databases

**Implementation** (v0.1.9):

- `watch_with_shadow()` in `src/sync.rs` - Full shadow mode implementation
- `ShadowDbState`, `ShadowSyncInput`, `ShadowSyncOutput` - Shadow-specific types
- `sync_shadow_concurrent_with_retry()` - Concurrent shadow sync with retry logic
- WAL notification → `shadow.copy_frames()` (immediate frame copy)
- Sync timer → reads from shadow segments → uploads LTX → cleanup
- Checkpoint timer → `shadow.checkpoint()` (controlled checkpoint)

**Usage**:
```bash
walrust watch mydb.db -b s3://bucket --shadow-wal
```

**Key Files**:
- `src/shadow.rs` - `ShadowWal` struct (checkpoint blocker, frame copier)
- `src/sync.rs` - `watch_with_shadow()` function
- `src/main.rs` - `--shadow-wal` CLI flag routing

### LTX vs Custom Format
- Use `litetx` crate (Superfly/Fly.io maintained)
- Litestream-compatible storage format
- Add SHA256 verification on top of LTX CRC64

### Checkpointing ✅ IMPLEMENTED
- ✅ **WAL Checkpoint Controls** - Production-grade WAL management
  - `checkpoint_interval`: Periodic PASSIVE checkpoint (default: 60s)
  - `min_checkpoint_page_count`: Efficiency threshold (default: 1000 pages, ~4MB)
  - `wal_truncate_threshold_pages`: Emergency TRUNCATE (default: 121359 pages, ~500MB)
  - Configurable via CLI and per-database in `walrust.toml`
  - Non-blocking PASSIVE checkpoints for efficiency
  - Blocking TRUNCATE checkpoints for safety brake
- ✅ Transaction-aware recording (like Litestream v0.5+)
- ✅ Don't block SQLite checkpoints
- ✅ Re-snapshot when WAL continuity breaks

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

### v0.1.4 (Current)
- [x] **Monitor Interval** - File watcher debouncing for high-write workloads
  - [x] Configurable via CLI (`--monitor-interval`) and config file
  - [x] Per-database override support
  - [x] Default: 1 second
- [x] **Validation Interval** - Automated backup integrity verification
  - [x] Periodic LTX checksum and TXID continuity verification
  - [x] Prometheus metrics: `walrust_validation_success_total`, `walrust_validation_failure_total`, `walrust_last_validation_timestamp`
  - [x] Configurable via CLI (`--validation-interval`) and config file
  - [x] Per-database override support
  - [x] Default: 0 (disabled), recommended: 86400 (daily) for production
- [x] 132 total tests (all passing)

### v0.1.3 (Previous)
- [x] **LTX Format Integration**
  - [x] Snapshots stored as LTX files (Litestream-compatible)
  - [x] manifest.json tracking with TXID sequencing
  - [x] Point-in-time restore by TXID or timestamp
  - [x] Binary data preservation with extensive test coverage
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

---

## Battle Testing & DST Framework (Path to v1.0)

**Goal**: Prove walrust won't lose data under ANY failure scenario before v1.0 release.

### Why Battle Testing is Critical

Traditional testing catches ~5% of real bugs in backup systems. Production has:
- Network delays/partitions during S3 uploads
- Crashes mid-WAL-sync with partial writes
- S3 eventual consistency (object appears, disappears, reappears)
- SQLite checkpoint happening mid-backup
- Concurrent writes while backup is reading WAL
- Clock drift, disk full, process SIGKILL

**Battle testing finds these bugs in milliseconds on a laptop that would take months in production.**

### DST Implementation Phases

See [BATTLE_TESTING.md](./BATTLE_TESTING.md) for detailed DST architecture and test scenarios.

#### Phase 1: Basic DST Framework ✅ COMPLETE

**Implemented in `walrust-dst/` crate:**

```bash
walrust-dst/
  src/
    main.rs           # CLI for running DST tests
    mock_storage.rs   # MockStorageBackend with fault injection
    properties.rs     # Property-based tests (7 properties)
    chaos.rs          # Real chaos tests using walrust::testable
  Cargo.toml
```

**Core Components:**

1. **StorageBackend Trait** ✅
   - [x] `StorageBackend` trait in `walrust/src/storage.rs`
   - [x] `S3Backend` implementation for production
   - [x] `walrust::testable` module with `sync_wal`, `take_snapshot`, `restore`

2. **MockStorageBackend** ✅
   - [x] RandomError: Configurable error rate injection
   - [x] Latency: Artificial delays
   - [x] PartialWrite: Simulates incomplete uploads
   - [x] SilentCorruption: Data corruption without errors
   - [x] EventualConsistency: Delayed object visibility

3. **DST Tests** ✅ (22 passing)
   - [x] Property tests (LTX roundtrip, durability, snapshot integrity, etc.)
   - [x] `chaos_silent_corruption` - Tests LTX checksum verification
   - [x] `test_snapshot_with_mock_storage` - Baseline with no faults
   - [x] `chaos_s3_errors` - Documents lack of retry logic (expected failure)
   - [x] `chaos_eventual_consistency` - Observational EC test

**Success Criteria:** ✅ MET
- 22 tests passing
- Real walrust code tested with fault injection
- Silent corruption detection >90%

#### Phase 2: Retry Logic & Webhooks ✅ COMPLETE

**Retry Logic Implementation:**

1. **Exponential Backoff with Jitter** ✅
   - [x] Base delay: 100ms → 200ms → 400ms → 800ms → ...
   - [x] Max delay cap: 30 seconds
   - [x] Full jitter: `random(0, min(cap, base * 2^attempt))`
   - [x] Max retries: configurable (default: 5)

2. **Error Classification** ✅
   - [x] Retryable errors: 500/502/503/504, timeouts, connection errors, "Service unavailable"
   - [x] Non-retryable errors: 400 (client bug), 401/403 (auth), 404 (not found)
   - [x] Circuit breaker: Opens after N consecutive failures (default: 10), half-open after cooldown

3. **Failure Webhooks** ✅
   - [x] POST to configurable URL on persistent failures
   - [x] Event types: `sync_failed`, `auth_failure`, `corruption_detected`, `circuit_breaker_open`
   - [x] Payload: `{ "event": "...", "database": "...", "error": "...", "attempts": N, "timestamp": "..." }`
   - [x] Config: `webhooks: [{ url: "https://...", events: ["sync_failed", "auth_failure"] }]`

4. **Tests** ✅
   - [x] `chaos_s3_errors` passes with retry logic (80%+ success rate under 20% error injection)
   - [x] Auth failure fast-fail verified
   - [x] Circuit breaker behavior tested

**Implementation Files:**
- `src/retry.rs` - RetryPolicy, RetryConfig, exponential backoff, error classification
- `src/webhook.rs` - WebhookConfig, send_webhook, event types
- `src/config.rs` - Added retry and webhook config sections
- `src/sync.rs` - testable module updated to use retry wrapper

**S3 Fault Injection (already implemented in MockStorageBackend):**

1. **S3 Failure Modes**
   - [x] test_partial_upload_recovery() - PartialWrite fault
   - [x] test_s3_eventual_consistency() - EventualConsistency fault
   - [ ] test_s3_500_transient_errors() - Needs retry logic first
   - [x] test_silent_data_corruption() - SilentCorruption fault

2. **WAL Edge Cases**
   - [ ] test_checkpoint_during_sync() - Race condition handling
   - [ ] test_wal_truncate_threshold_reached() - Emergency TRUNCATE checkpoint
   - [ ] test_manifest_corruption_recovery() - Rebuild from S3 scan
   - [ ] test_concurrent_writes_during_backup() - Snapshot consistency

3. **Multi-Database Stress**
   - [ ] test_100_databases_simultaneous_writes()
   - [ ] test_cascading_failures() - One DB failure doesn't affect others
   - [ ] test_resource_exhaustion() - Memory/file descriptor limits

**Success Criteria:**
- All S3 fault scenarios handled gracefully
- Checksum mismatches always detected
- Multi-DB isolation verified

#### Phase 3: Integration into Main Sync Loop ✅ COMPLETE

**Goal**: Integrate retry logic and webhooks from the testable module into the production sync loop.

**Implementation Steps:**

1. **CLI Flags for Retry Config** ✅
   - [x] `--max-retries` - Maximum retry attempts (default: 5)
   - [x] `--base-delay-ms` - Initial backoff delay (default: 100)
   - [x] `--max-delay-ms` - Maximum backoff delay (default: 30000)
   - [x] `--no-circuit-breaker` - Disable circuit breaker
   - [x] `--circuit-breaker-threshold` - Failures before circuit opens (default: 10)

2. **Retry-Wrapped Helper Functions** ✅
   - [x] `sync_wal_with_retry()` - Wraps sync_wal with retry and webhooks
   - [x] `take_snapshot_with_retry()` - Wraps take_snapshot with retry and webhooks
   - [x] Error classification using `retry::classify_error()`
   - [x] Auth errors fast-fail immediately, transient errors retry

3. **Main Sync Loop Integration** ✅
   - [x] All sync_wal calls replaced with retry-wrapped versions
   - [x] All take_snapshot calls replaced with retry-wrapped versions
   - [x] Webhook notifications on: sync_failed, auth_failure
   - [x] watch_with_config initializes RetryPolicy and WebhookSender from config

4. **Error Handling Updates** ✅
   - [x] Classify errors using retry::classify_error()
   - [x] Send webhooks on persistent failures
   - [x] Log retry attempts with structured logging

**Implementation Files:**
- `src/main.rs` - CLI flags for retry config (--max-retries, --base-delay-ms, etc.)
- `src/sync.rs` - sync_wal_with_retry(), take_snapshot_with_retry(), watch_with_config integration
- `src/retry.rs` - Added config() accessor method

**Success Criteria:**
- [x] All S3 operations in watch loop use retry logic
- [x] Webhooks fire on persistent failures
- [x] CLI flags allow runtime configuration
- [x] All existing tests pass (150+ tests)
- [x] Chaos tests demonstrate improved reliability (80%+ success under 20% error injection)

---

#### Phase 4: Continuous Chaos Testing ✅ COMPLETE

**Property-Based Testing with `proptest`:**

1. **Core Properties** ✅
   - [x] Property: Every committed transaction is recoverable from S3
   - [x] Property: Point-in-time restore gives exact state at timestamp T (FIXED in v0.1.6)
   - [x] Property: WAL batching never loses frames
   - [x] Property: Snapshot is atomic (no partial state)
   - [x] Property: GFS compaction preserves recoverability

2. **Chaos Engineering Loop** ✅
   - [x] Run DST suite with random failure injection
   - [x] 10,000+ iterations per property test (configurable via PROPTEST_CASES)
   - [x] Measure MTBF (mean time between failures)
   - [x] Collect failure seeds for regression testing

3. **Performance Under Failure** ✅
   - [x] Measure crash recovery time
   - [x] Verify no memory leaks during repeated crashes
   - [x] Check CPU usage during S3 retry storms
   - [x] Monitor file descriptor leaks

**Implementation Files:**
- `walrust-dst/src/invariants.rs` - Core invariant property tests
- `walrust-dst/src/chaos.rs` - Extended chaos test scenarios
- `walrust-dst/src/metrics.rs` - MTBF tracking and reporting
- `walrust-dst/src/main.rs` - CLI `continuous` command

**Success Criteria:** ✅ MET
- 10,000+ seeds pass all property tests
- Zero data loss in chaos tests
- No resource leaks detected
- Recovery time < 5 seconds for typical workloads

### Critical Invariants to Verify

All DST tests must verify these invariants hold after recovery:

1. **TXID Monotonicity** - No gaps, no duplicates
2. **Checksum Chain Integrity** - pre_apply → post_apply chain valid
3. **Manifest Consistency** - All listed files exist in S3
4. **WAL Frame Count** - Matches S3 LTX frame count
5. **Transaction Atomicity** - No partial transactions (all-or-nothing)
6. **Binary Preservation** - Restored DB byte-identical to source

### Test Scenarios Matrix

| Scenario | Failure Type | Expected Behavior |
|----------|--------------|-------------------|
| Crash during WAL sync | SIGKILL mid-upload | No partial LTX files in S3 |
| S3 500 errors | Transient failures | Retry succeeds, no data loss |
| WAL checkpoint race | SQLite resets WAL while reading | Detect and re-snapshot |
| Eventual consistency | Object appears then disappears | Handle gracefully with retries |
| Clock skew | System clock jumps backward | Snapshot intervals still work |
| Concurrent snapshots | Two snapshots triggered simultaneously | Only one runs (mutex) |
| Restore corruption | Downloaded LTX is corrupted | Detect via checksum, fail safely |
| Disk full | ENOSPC during snapshot | Log error, continue WAL sync |
| Network partition | S3 unreachable for hours | Buffer WAL, resume when network returns |
| Manifest corruption | manifest.json is invalid | Rebuild from S3 object listing |

### Integration with CI/CD

**GitHub Actions:**

```yaml
# .github/workflows/battle-test.yml
name: Battle Test

on:
  push:
    branches: [main]
  pull_request:
  schedule:
    - cron: '0 4 * * *'  # 4 AM UTC daily

jobs:
  smoke:
    runs-on: ubuntu-latest
    steps:
      - name: Smoke tests (basic crash scenarios)
        run: cargo test --test dst_basic

  properties:
    runs-on: ubuntu-latest
    steps:
      - name: Property tests (quick - 100 cases)
        run: cargo test --test dst_properties

      - name: Property tests (extended - 10K cases)
        if: github.event_name == 'schedule'
        run: PROPTEST_CASES=10000 cargo test --test dst_properties

  chaos:
    runs-on: ubuntu-latest
    steps:
      - name: Chaos tests (fault injection)
        run: cargo test --test dst_chaos

  soak:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    timeout-minutes: 120
    steps:
      - name: 2-hour soak test
        run: cargo test --test dst_soak -- --ignored
```

### Success Criteria for v1.0 Release

Before declaring walrust production-ready:

- [ ] **10,000+ seeds pass** all property tests (zero failures)
- [ ] **Zero data loss** in chaos tests (crashes, S3 faults, network partitions)
- [ ] **Litestream compatibility** verified (restore Litestream backups)
- [ ] **100+ database scale** tested without issues
- [ ] **1000 writes/sec/db** sustained (with WAL batching)
- [ ] **24h soak test** passes with no memory leaks
- [ ] **CI runs nightly** for 2+ weeks with zero failures
- [ ] **All critical invariants** verified in every test
- [ ] **Recovery time** < 5 seconds for typical workloads
- [ ] **Documentation** includes failure recovery guide

### Timeline Estimate

- **Phase 1 (Basic DST Framework)**: 1 week
- **Phase 2 (Advanced Scenarios)**: 1 week
- **Phase 3 (Continuous Chaos)**: 1 week
- **CI Integration & Hardening**: 2 weeks nightly runs
- **Total**: ~5 weeks to production-ready v1.0

### Dependencies

**New Crates for DST:**
- `proptest` - Property-based testing
- `tempfile` - Temporary test databases
- `rusqlite` - Direct SQLite access for oracle
- `rand` - Seeded RNG for reproducibility

**Implementation Files:**
- `tests/dst/framework/simulator.rs` - Failure injection
- `tests/dst/framework/oracle.rs` - Reference DB + invariants
- `tests/dst/cases/basic.rs` - Core crash/network tests
- `tests/dst/cases/advanced.rs` - S3/WAL edge cases
- `tests/dst/cases/stress.rs` - Multi-DB, long-running tests

---

## References

- [Litestream Revamped](https://fly.io/blog/litestream-revamped/) - LTX format, multi-DB
- [Litestream v0.5.0](https://fly.io/blog/litestream-v050-is-here/) - Compaction levels
- [litetx crate](https://docs.rs/litetx/) - Rust LTX implementation
- [Litestream How It Works](https://litestream.io/how-it-works/) - WAL mechanics
- [sled simulation guide](https://sled.rs/simulation.html) - DST architecture inspiration
- [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md) - Deterministic simulation testing
- [Jepsen](https://jepsen.io) - Distributed systems testing methodology
