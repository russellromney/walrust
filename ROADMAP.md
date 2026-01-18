# walrust Roadmap

## Vision

Litestream-compatible SQLite sync in Rust. Optimized for multi-tenant deployments (Cinch, Tenement).

**Core differentiators:**
- LTX format (Litestream-compatible) with SHA256 verification on top
- Lower memory footprint (~12MB vs ~33MB)
- Built-in dashboard + Prometheus metrics
- Opinionated defaults (grandfather/father/son retention)

## v0.1.9 Plan (Disk-Based Upload Queue)

**Goal**: Implement litestream-style disk caching for decoupled WAL encoding and S3 uploads, enabling crash recovery and eliminating upload bottlenecks.

### Problem Statement

Current architecture couples WAL encoding directly to S3 uploads:
- Upload blocks next WAL segment encoding
- Failed uploads waste encoding work (must re-read and re-encode WAL)
- No crash recovery (in-flight uploads lost on restart)
- No local cache for fast restore
- Memory concerns if uploads queue in RAM during S3 slowdowns

### Litestream's Solution (Studied Architecture)

Litestream uses a **two-goroutine architecture** with disk-based intermediary:

**Goroutine 1: DB Monitor**
- Watches WAL for changes every 1 second
- Encodes WAL frames to LTX files on disk
- Notifies upload goroutine via channel
- Never blocks on S3

**Goroutine 2: S3 Uploader**
- Runs independently, waits for notification
- Reads LTX files from disk cache
- Uploads to S3 sequentially (TXID ordering preserved)
- Retries from disk on failure (no re-encoding)
- Only deletes cache after successful upload + retention period

**Directory Structure:**
```
/path/to/.app.db-litestream/
  ltx/
    0/                      # Level 0 (incremental)
      00000001-00000001.ltx
      00000002-00000002.ltx
      ...
    1/                      # Level 1 (compacted)
    snapshot/               # Snapshots
```

**Key Benefits Observed:**
- Zero memory buffering (everything on disk)
- Crash recovery (restart reads pending files)
- Fast retry (read from disk, don't re-encode)
- Local cache = fast restore without S3
- Upload failures don't block encoding

### Proposed Walrust Architecture

**Two Independent Tasks per Database:**

**Task 1: WAL Monitor Task** (fast, never blocks)
- Detects WAL changes
- Encodes WAL frames to LTX format
- Writes LTX to disk cache (atomic write via .tmp rename)
- Sends TXID to upload channel
- Updates manifest with pending status
- Continues immediately to next WAL segment

**Task 2: S3 Uploader Task** (slow, independent)
- Receives TXID from channel
- Reads LTX file from disk cache
- Uploads to S3 with retry logic
- Marks as uploaded in manifest
- Preserves file for local cache (retention policy)
- Processes sequentially to preserve TXID ordering

### Directory Structure

```
/path/to/app.db
/path/to/.app.db-walrust/           # Cache directory
  manifest.json                      # Upload state tracking
  ltx/
    00000001.ltx                     # TXID 1 (uploaded)
    00000002.ltx                     # TXID 2 (uploaded)
    00000003.ltx                     # TXID 3 (pending)
    00000004.ltx                     # TXID 4 (pending)
```

**Manifest Format:**
```json
{
  "last_uploaded_txid": 2,
  "pending_txids": [3, 4],
  "cache_size_bytes": 12345,
  "last_cleanup": "2024-01-15T10:30:00Z"
}
```

### Implementation Phases

**Phase 1: Disk Cache Foundation**
- New module: `src/cache.rs`
- `LocalCache` struct with methods:
  - `write_ltx()` - Write LTX to cache (atomic via .tmp)
  - `read_ltx()` - Read LTX from cache
  - `mark_uploaded()` - Update manifest
  - `pending_uploads()` - Get list of pending TXIDs
  - `cleanup()` - Retention policy enforcement
- Manifest persistence in JSON format
- Atomic operations using tempfile + rename
- Thread-safe with Arc<Mutex<>> for concurrent access

**Phase 2: Independent Uploader Task**
- New module: `src/uploader.rs`
- `Uploader` struct managing S3 upload task
- Channel-based communication (mpsc::channel)
- Sequential TXID processing (no out-of-order uploads)
- Integration with existing retry logic
- Webhook notifications on upload failure
- Graceful shutdown (complete pending uploads)

**Phase 3: Main Sync Loop Integration**
- Modify `src/sync.rs`:
  - Replace direct S3 upload with cache write
  - Spawn uploader task per database
  - Send TXID notifications via channel
  - Track upload status in database state

**Phase 4: Startup Recovery**
- On walrust restart:
  - Scan cache directories for pending uploads
  - Resume uploads from where we left off
  - Log recovery progress
  - Verify TXID continuity

**Phase 5: Fast Local Restore**
- Modify `src/restore.rs`:
  - Check local cache before fetching from S3
  - If cache complete, restore locally (no S3 needed)
  - Fall back to S3 if cache incomplete/missing
  - Hybrid approach: cache + S3 for best performance

### Testing Strategy

**Unit Tests (src/cache.rs - 24 tests):**
- ✅ Cache creation and directory structure
- ✅ Write and read LTX files
- ✅ Atomic write verification (tempfile + rename)
- ✅ Mark uploaded status tracking
- ✅ Pending uploads sorted by TXID
- ✅ Sequential upload tracking (TXID ordering)
- ✅ Cleanup with retention policy (time-based and size-based)
- ✅ Never delete pending uploads (safety guarantee)
- ✅ Cache persistence across restarts (manifest recovery)
- ✅ Verify integrity (detect missing files, orphans, size mismatches)
- ✅ Concurrent access safety (thread-safe operations)
- ✅ Manifest corruption detection
- ✅ Empty cache edge cases
- ✅ Large TXID values (u64::MAX)

**Unit Tests (src/uploader.rs - 11 tests):**
- ✅ Basic upload flow (cache → S3)
- ✅ Sequential TXID processing (ordered uploads)
- ✅ Resume pending uploads on startup (crash recovery)
- ✅ Retry on S3 failures (with exponential backoff)
- ✅ Channel buffering (handle bursts)
- ✅ Graceful shutdown (complete pending uploads)
- ✅ Statistics tracking (attempts, successes, failures, bytes)
- ✅ spawn_uploader helper function

**Integration Tests (walrust-dst/disk_queue_tests.rs - 15 tests):**
- ✅ Crash recovery basic (write 10, crash, restart, verify all upload)
- ✅ Crash recovery partial (write 10, upload 5, crash, verify remaining 5)
- ✅ Network disconnect recovery (cache continues, reconnect, upload)
- ✅ Cache continues during S3 failure (100 writes don't block)
- ✅ Fast local restore (restore from cache without S3)
- ✅ Cleanup retention policy (100 files → keep 10 most recent)
- ✅ TXID ordering preserved (out-of-order writes → sequential uploads)
- ✅ Concurrent multi-database (10 DBs × 50 uploads each)
- ✅ Chaos random failures (20% S3 failure rate, retries succeed)
- ✅ Cache verify integrity (detect corruption)
- ✅ Uploader graceful shutdown completes pending
- ✅ Cache persistence across restarts

**Chaos Tests (walrust-dst/chaos.rs - 4 new tests):**
- ✅ Atomic write crashes (verify tempfile+rename atomicity)
- ✅ Crash recovery (N writes, M uploads, crash, verify N-M resume)
- ✅ Manifest corruption (invalid JSON, missing file, inconsistencies)
- ✅ Concurrent multi-database isolation (failures don't cascade)

**Benchmarks (future):**
- [ ] Compare current (direct upload) vs new (cached)
- [ ] Measure WAL encoding latency improvement
- [ ] Measure memory usage under load (bounded by cache)
- [ ] Measure throughput improvement (30%+ expected)

**Test Coverage Summary:**
- **Total Tests**: 54 (24 cache + 11 uploader + 15 integration + 4 chaos)
- **Test Execution**: `cargo test` (unit tests) + `cargo test --package walrust-dst` (integration/chaos)
- **CI Integration**: All tests run on every commit
- **Property Tests**: Chaos tests use seeded RNG for reproducibility

### Benefits

1. **Crash Recovery**: Walrust restarts resume pending uploads automatically
2. **Decoupled Performance**: S3 slowness doesn't block WAL encoding
3. **Efficient Retries**: Failed uploads retry from disk (no re-encoding)
4. **Fast Local Restore**: Recent backups available locally without S3 fetch
5. **Gap Prevention**: Sequential upload preserves TXID ordering
6. **Memory Bounded**: Disk buffering prevents unbounded memory growth
7. **Observability**: Manifest tracks upload status, cache size, pending count

### Configuration

**CLI Flags:**
```bash
--cache-dir <path>              # Override default cache location
--cache-retention <duration>    # How long to keep uploaded files (default: 24h)
--cache-max-size <bytes>        # Max cache size before cleanup
--no-cache                      # Disable caching (direct upload)
```

**Config File:**
```toml
[cache]
enabled = true
retention = "24h"        # Keep uploaded files for 24h
max_size = "10GB"       # Cleanup when cache exceeds 10GB
path = "/custom/cache"  # Override default location
```

### Migration Path

**v1 (Compatible - v0.1.9):**
- Add cache module and uploader task
- Keep direct upload as fallback for errors
- Feature flag: `--enable-cache` (opt-in)

**v2 (Beta - v0.2.0):**
- Make cache default behavior
- Direct upload only if `--no-cache` specified
- Migration guide for existing deployments

**v3 (Stable - v1.0):**
- Remove direct upload code path entirely
- Cache is mandatory
- Cleanup old configuration options

### Success Metrics

**Performance:**
- WAL encoding latency reduced by 50%+ (no upload blocking)
- Throughput increased by 30%+ (especially at high DB counts)
- Memory usage stable (bounded by cache, not upload queue)

**Reliability:**
- 100% upload resume success after crash
- Zero data loss in chaos tests
- Fast local restore (10x faster than S3 fetch)

**Observability:**
- Manifest tracks pending uploads
- Metrics for cache size, upload queue depth
- Logging for recovery operations

### Dependencies

**New Crates:**
- `serde_json` - Manifest persistence (already have serde)
- No additional dependencies needed

**Modified Files:**
- `src/cache.rs` (NEW) - Local cache implementation
- `src/uploader.rs` (NEW) - Independent upload task
- `src/manifest.rs` (NEW) - Upload state tracking
- `src/sync.rs` (MODIFIED) - Integration with cache
- `src/restore.rs` (MODIFIED) - Cache-first restore
- `src/main.rs` (MODIFIED) - CLI flags and initialization
- `src/config.rs` (MODIFIED) - Cache configuration

### Timeline Estimate

- Phase 1 (Cache): 2-3 days
- Phase 2 (Uploader): 2-3 days
- Phase 3 (Integration): 1-2 days
- Phase 4 (Recovery): 1 day
- Phase 5 (Fast Restore): 1-2 days
- Testing & Documentation: 2-3 days
- **Total**: ~2 weeks

### References

- Litestream source: `/Users/russellromney/Documents/Github/litestream`
  - `db.go:1277-1455` - Disk-first sync implementation
  - `replica.go:127-193` - Independent upload goroutine
  - `db.go:252-288` - LTX cache directory structure
- Analysis session: 2026-01-17 (this conversation)

---

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

## Benchmark Framework

### Goal

Measure how successfully walrust/litestream replicate SQLite data to S3, ensuring **minimal data loss** on server crashes, power failures, or disk corruption.

**Success Metric:** All committed SQLite writes appear in S3 with minimal replication lag.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Benchmark Runner (bench/benchmark.py)                          │
│                                                                 │
│  1. Read config (bench/configs/*.yml)                          │
│  2. Create test databases                                      │
│  3. Start tool (walrust/litestream)                            │
│  4. Run workload (DatabaseWriter threads)                      │
│  5. Monitor resources (ResourceMonitor)                        │
│  6. Stop tool                                                  │
│  7. Verify replication (restore from S3 + compare)             │
│  8. Generate report                                            │
└─────────────────────────────────────────────────────────────────┘
```

### Components

#### 1. Workload Generator (`bench/lib/workload.py`)

**Purpose:** Write data to SQLite databases at controlled rates.

```python
class DatabaseWriter:
    """Write data to SQLite with known timestamps."""

    def __init__(self, db_path: Path, writes_per_second: int):
        self.db_path = db_path
        self.writes_per_second = writes_per_second
        self.writes = []  # [(write_id, commit_timestamp), ...]

    def write_loop(self):
        """Write loop with rate limiting."""
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_data (
                id TEXT PRIMARY KEY,
                created_at REAL,
                data BLOB
            )
        """)

        while self.running:
            write_id = str(uuid4())
            commit_ts = time.time()

            conn.execute(
                "INSERT INTO benchmark_data (id, created_at, data) VALUES (?, ?, ?)",
                (write_id, commit_ts, os.urandom(1024))
            )
            conn.commit()

            self.writes.append((write_id, commit_ts))

            time.sleep(1.0 / self.writes_per_second)
```

**No latency tracking** - just recording ground truth: what we wrote and when.

#### 2. Tool Runners (`bench/lib/runners.py`)

**Purpose:** Abstract walrust/litestream process management.

```python
class WalrustRunner:
    """Manage walrust process."""

    def start(self, databases: List[Path], bucket: str, endpoint: str):
        cmd = [
            "walrust", "watch",
            "--bucket", bucket,
            "--endpoint", endpoint,
            "--independent-tasks"  # Multi-DB mode
        ] + [str(db) for db in databases]

        self.proc = subprocess.Popen(cmd, ...)
        time.sleep(2)  # Wait for startup
        return self.proc.pid

class LitestreamRunner:
    """Manage litestream process with single-process multi-DB config."""

    def start(self, databases: List[Path], bucket: str, endpoint: str):
        # Generate litestream.yml
        config = {
            'dbs': [
                {
                    'path': str(db),
                    'replicas': [{
                        'url': f's3://{bucket}/{db.name}',
                        'endpoint': endpoint
                    }]
                }
                for db in databases
            ]
        }

        config_path = Path(tempfile.mktemp(suffix='.yml'))
        config_path.write_text(yaml.dump(config))

        self.proc = subprocess.Popen(
            ["litestream", "replicate", "-config", str(config_path)],
            ...
        )
        time.sleep(2)
        return self.proc.pid
```

#### 3. Resource Monitor (`bench/lib/monitor.py`)

**Purpose:** Track CPU and memory usage.

```python
class ResourceMonitor:
    """Monitor process resources."""

    def __init__(self, pid: int, include_children: bool = False):
        self.pid = pid
        self.include_children = include_children
        self.samples = []  # {'timestamp', 'cpu_percent', 'memory_mb'}

    def monitor_loop(self):
        """Sample every 100ms."""
        while self.running:
            proc = psutil.Process(self.pid)

            if self.include_children:
                # For litestream (may spawn children)
                all_procs = [proc] + proc.children(recursive=True)
                cpu = sum(p.cpu_percent(interval=0.1) for p in all_procs)
                mem = sum(p.memory_info().rss for p in all_procs)
            else:
                # For walrust (single process)
                cpu = proc.cpu_percent(interval=0.1)
                mem = proc.memory_info().rss

            self.samples.append({
                'timestamp': time.time(),
                'cpu_percent': cpu,
                'memory_mb': mem / (1024 * 1024)
            })

            time.sleep(0.1)
```

#### 4. Replication Verifier (`bench/lib/verify.py`)

**Purpose:** Verify all writes made it to S3 and measure replication lag.

**Approach:**
1. Restore database from S3 using `walrust restore` or `litestream restore`
2. Query restored database for all rows
3. Get S3 object metadata (LastModified) for LTX files
4. Compare: commit timestamp (from SQLite row) vs upload timestamp (from S3 metadata)

```python
class ReplicationVerifier:
    """Verify replication completeness and latency."""

    def verify(
        self,
        tool: str,
        db_name: str,
        bucket: str,
        endpoint: str,
        expected_writes: List[Tuple[str, float]]
    ) -> dict:
        """
        Args:
            tool: "walrust" or "litestream"
            db_name: Database name for S3 prefix
            bucket: S3 bucket
            endpoint: S3 endpoint
            expected_writes: [(write_id, commit_timestamp), ...]

        Returns:
            Replication metrics dict
        """

        # 1. Restore database from S3
        restore_path = Path(tempfile.mktemp(suffix='.db'))

        if tool == "walrust":
            subprocess.run([
                "walrust", "restore", db_name,
                "-o", str(restore_path),
                "-b", bucket,
                "--endpoint", endpoint
            ], check=True)
        else:  # litestream
            subprocess.run([
                "litestream", "restore",
                "-o", str(restore_path),
                f"s3://{bucket}/{db_name}"
            ], check=True)

        # 2. Query restored database
        conn = sqlite3.connect(str(restore_path))
        cursor = conn.execute("SELECT id, created_at FROM benchmark_data")
        restored_writes = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()

        # 3. Get S3 metadata for LTX files (upload timestamps)
        s3_client = boto3.client('s3', endpoint_url=endpoint)

        # List all LTX files for this database
        bucket_name = bucket.replace('s3://', '')
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f'{db_name}/'
        )

        # Get upload timestamps (only WAL frames, ignore snapshots)
        ltx_uploads = {}
        for obj in response.get('Contents', []):
            if '-' in obj['Key'] and obj['Key'].endswith('.ltx'):
                # WAL frame file: "00000001-00000100.ltx"
                ltx_uploads[obj['Key']] = obj['LastModified'].timestamp()

        # For simplicity, use the LATEST LTX upload time
        # (In reality, we'd need to map write_id -> specific LTX file)
        latest_upload_ts = max(ltx_uploads.values()) if ltx_uploads else None

        # 4. Compare and compute metrics
        replicated = []
        missing = []
        sync_latencies = []

        for write_id, commit_ts in expected_writes:
            if write_id in restored_writes:
                replicated.append(write_id)

                # Sync latency = S3 upload time - SQLite commit time
                # Using latest upload as approximation
                if latest_upload_ts:
                    latency = latest_upload_ts - commit_ts
                    sync_latencies.append(latency)
            else:
                missing.append(write_id)

        return {
            'total_writes': len(expected_writes),
            'replicated_writes': len(replicated),
            'missing_writes': len(missing),
            'data_loss': len(missing) > 0,
            'sync_latency_p50_ms': percentile(sync_latencies, 0.5) * 1000,
            'sync_latency_p95_ms': percentile(sync_latencies, 0.95) * 1000,
            'sync_latency_p99_ms': percentile(sync_latencies, 0.99) * 1000,
            'sync_latency_max_ms': max(sync_latencies) * 1000 if sync_latencies else 0,
        }
```

**Note:** This is simplified. For accurate per-write latency, we'd need to:
- Parse LTX filename ranges to map write_id → specific LTX file
- Or track TXID in SQLite and map TXID → LTX file

### Metrics

**Replication Metrics:**
- `total_writes` - Total writes performed
- `replicated_writes` - Writes found in restored database
- `missing_writes` - Writes NOT found (data loss!)
- `data_loss` - Boolean: any missing writes?
- `sync_latency_p50_ms` - Median replication lag
- `sync_latency_p95_ms` - 95th percentile replication lag
- `sync_latency_p99_ms` - 99th percentile replication lag
- `sync_latency_max_ms` - Maximum replication lag

**Resource Metrics (from ResourceMonitor):**
- `peak_memory_mb` - Peak process memory
- `avg_memory_mb` - Average process memory
- `min_memory_mb` - Minimum process memory
- `peak_cpu_percent` - Peak CPU usage
- `avg_cpu_percent` - Average CPU usage
- `min_cpu_percent` - Minimum CPU usage

**Workload Metrics (from DatabaseWriter):**
- `duration_seconds` - Actual benchmark duration
- `target_writes_per_second` - Configured write rate
- `actual_writes_per_second` - Achieved write rate

### Configuration Format

**Simple Config (`bench/configs/quick.yml`):**

```yaml
name: "quick-test"

workload:
  type: "rate-limited"
  writes_per_second: 10
  duration_seconds: 30

databases:
  count: 5
  size_kb: 100

tools:
  - walrust
  - litestream

storage:
  bucket: "s3://walrust-bench"
  endpoint: "https://fly.storage.tigris.dev"

metrics:
  resource_sample_interval_ms: 100
```

**Matrix Config (`bench/configs/scalability-matrix.yml`):**

```yaml
name: "scalability-matrix"

matrix:
  databases: [1, 10, 50, 100]
  writes_per_second: [10, 100, 1000]
  duration_seconds: [30]
  tools: [walrust, litestream]

storage:
  bucket: "s3://walrust-bench"
  endpoint: "https://fly.storage.tigris.dev"

metrics:
  resource_sample_interval_ms: 100
```

This generates: 4 × 3 × 1 × 2 = **24 runs**

Each run produces:
```json
{
  "config": {
    "databases": 10,
    "writes_per_second": 100,
    "duration_seconds": 30,
    "tool": "walrust"
  },
  "replication": {
    "total_writes": 3000,
    "replicated_writes": 3000,
    "missing_writes": 0,
    "data_loss": false,
    "sync_latency_p50_ms": 145,
    "sync_latency_p95_ms": 890,
    "sync_latency_p99_ms": 1250,
    "sync_latency_max_ms": 2100
  },
  "resources": {
    "peak_memory_mb": 19.5,
    "avg_memory_mb": 18.2,
    "min_memory_mb": 17.1,
    "peak_cpu_percent": 12.3,
    "avg_cpu_percent": 4.5,
    "min_cpu_percent": 2.1
  },
  "workload": {
    "duration_seconds": 30.2,
    "target_writes_per_second": 100,
    "actual_writes_per_second": 99.3
  }
}
```

### Implementation Status: ✅ Phase 1 Complete (2026-01-17)

1. ✅ Update README with data loss prevention goal
2. ✅ Create `bench/lib/` utilities:
   - ✅ `workload.py` - DatabaseWriter with rate limiting and timestamp tracking
   - ✅ `runners.py` - WalrustRunner, LitestreamRunner for process management
   - ✅ `monitor.py` - ResourceMonitor for CPU/memory sampling
   - ✅ `verify.py` - ReplicationVerifier for S3 restore and data loss detection
   - ✅ `config.py` - BenchmarkConfig with YAML loading and matrix expansion
3. ✅ Create `bench/benchmark.py` - Main CLI orchestrator
4. ✅ Test with simple config (test-minimal.yml: 4 runs completed)
5. ✅ Test with matrix config (2 write rates × 2 tools = 4 configs)
6. ✅ Documentation: `bench/BENCHMARK_FRAMEWORK.md` with complete usage guide
7. ⏳ Pending: Deprecate old `bench/compare.py` and `bench/realworld.py`
8. ⏳ Pending: Delete root experimental scripts

**What Works:**
- End-to-end benchmark execution (config → workload → monitoring → verification → results)
- Matrix expansion (single YAML → multiple test configurations)
- Multi-tool support (walrust and litestream in same framework)
- Resource tracking (CPU/memory percentiles)
- Data loss detection (expected vs replicated writes comparison)
- Sync latency measurement (using S3 upload timestamps)
- JSON output with all metrics

**Next: Phase 2 - Fly.io Integration**
- Integrate with fly-benchmark-engine pool mode
- Run benchmarks on real infrastructure (not localhost)
- Pre-provisioned machines for fast iteration
- Distributed execution for large-scale testing

### Open Questions

1. **Per-write sync latency accuracy** - Current approach uses latest LTX upload time as approximation. For exact per-write latency, we need to:
   - Track SQLite TXID for each write
   - Map TXID → LTX file using filename ranges
   - Use that specific LTX file's upload timestamp

   Should we implement this, or is the approximation good enough?

2. **WAL frame identification** - How do we distinguish WAL frame LTX from snapshot LTX in S3?
   - By filename pattern? (e.g., snapshots are `00000001-00000001.ltx`)
   - By file size?
   - By metadata tag?

3. **Cleanup between runs** - Should we delete S3 data between matrix runs to avoid pollution?

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

## v0.2.0 Plan (Post-Format Completion)

**Goal**: Complete feature parity with litestream for production use.

### Priority 1: Litestream Restore Compatibility Test

**Status**: TEST INFRASTRUCTURE COMPLETE (needs valid S3 credentials to run)

**Goal**: Verify that walrust backups can be restored by litestream CLI and vice versa.

**Why This Matters**:
- Users can migrate between tools without data loss
- Validates our LTX format implementation is truly compatible
- Provides confidence the format is correct before adding more features

**Test Plan**:

1. **Walrust → Litestream Restore**
   ```bash
   # Create test database with known data
   sqlite3 test.db "CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT); INSERT INTO t VALUES(1,'hello'),(2,'world');"

   # Backup with walrust
   walrust watch test.db -b s3://bucket/test --snapshot-interval 5
   # Wait for snapshot + some incrementals

   # Attempt restore with litestream
   litestream restore -o restored.db s3://bucket/test

   # Verify data integrity
   sqlite3 restored.db "SELECT * FROM t;"
   ```

2. **Litestream → Walrust Restore**
   ```bash
   # Create and backup with litestream
   sqlite3 test.db "CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT);"
   litestream replicate test.db s3://bucket/test

   # Attempt restore with walrust
   walrust restore test -o restored.db -b s3://bucket

   # Verify data integrity
   ```

3. **Format Differences to Check**:
   - [ ] Generation folder naming (0000, 0001, etc.)
   - [ ] TXID filename format (16-char hex)
   - [ ] LTX header compatibility
   - [ ] Checksum chaining
   - [ ] Snapshot vs incremental detection

**Implementation**:
- [x] Create `tests/litestream_compat.sh` - Shell script for manual testing
- [ ] Create `walrust-dst/src/litestream_compat.rs` - Automated compatibility tests
- [x] Document any differences in `docs/LITESTREAM_COMPAT.md`

**Success Criteria**:
- [ ] Walrust backup restores successfully with litestream CLI (needs credentials)
- [ ] Litestream backup restores successfully with walrust CLI (needs credentials)
- [ ] All data integrity preserved (byte-for-byte identical)
- [ ] Test script added to CI (optional, requires litestream binary)

---

### Priority 2: Compaction (Merge Incrementals into Snapshots)

**Status**: COMPLETE (v0.2.0) - Core implementation done

**Current State**:
- `retention.rs` has GFS policy for snapshot retention (deleting old snapshots)
- `compact` command exists but only handles snapshot retention
- Incrementals in `0000/` folder are not merged

**Goal**: Implement litestream-style compaction that merges incrementals into new snapshots.

**Litestream Compaction Model**:
```
Before Compaction:
  db/0000/0000000000000001-0000000000000001.ltx  # Snapshot (gen 0)
  db/0000/0000000000000002-0000000000000010.ltx  # Incremental
  db/0000/0000000000000011-0000000000000050.ltx  # Incremental
  db/0000/0000000000000051-0000000000000100.ltx  # Incremental

After Compaction:
  db/0001/0000000000000001-0000000000000100.ltx  # New snapshot (gen 1)
  db/0000/0000000000000101-0000000000000150.ltx  # New incrementals continue
```

**Implementation Phases**:

**Phase 2.1: Compaction Core Logic**
- New function: `compact_generation()` in `src/sync.rs`
- Read all LTX files from generation 0
- Apply them sequentially to create full database state
- Write new snapshot LTX to next generation
- Update generation counter

**Phase 2.2: Incremental Deletion**
- After successful snapshot creation, delete old incrementals
- Keep configurable number of recent incrementals (for PITR)
- Atomic operation: only delete after upload confirmed

**Phase 2.3: Auto-Compaction Triggers**
- `--compact-threshold <N>` - Compact after N incrementals
- `--compact-size <bytes>` - Compact when incrementals exceed size
- `--compact-age <duration>` - Compact incrementals older than duration

**Phase 2.4: Manual Compaction Command**
```bash
# Compact specific database
walrust compact mydb -b s3://bucket --force

# Dry-run (show what would happen)
walrust compact mydb -b s3://bucket

# Compact all databases
walrust compact --all -b s3://bucket --force
```

**Configuration**:
```toml
[compaction]
enabled = true
threshold = 100           # Compact after 100 incrementals
max_incremental_age = "24h"  # Compact incrementals older than 24h
preserve_recent = 10      # Keep 10 most recent incrementals for PITR
```

**Files to Modify**:
- `src/sync.rs` - Add `compact_generation()`, `apply_incrementals_to_snapshot()`
- `src/main.rs` - Enhance `compact` command with new options
- `src/config.rs` - Add compaction configuration
- `src/retention.rs` - Rename to handle both snapshot retention AND incremental compaction

**Tests**:
- [ ] `test_compact_single_generation` - Basic compaction
- [ ] `test_compact_preserves_data` - Restore after compaction works
- [ ] `test_compact_threshold_trigger` - Auto-compact on count
- [ ] `test_compact_size_trigger` - Auto-compact on size
- [ ] `test_compact_with_concurrent_writes` - Safe under load
- [ ] `test_compact_failure_recovery` - No data loss on partial failure

**Success Criteria**:
- [ ] Compaction reduces S3 object count significantly
- [ ] Restore works correctly after compaction
- [ ] PITR still works within preserved window
- [ ] No data loss during compaction
- [ ] Concurrent writes during compaction are safe

---

### Priority 3: WAL Checkpoint Handling Improvements

**Status**: COMPLETE (v0.2.0)

**Current State**:
- Shadow WAL mode exists (`--shadow-wal`)
- Checkpoint interval configurable (`--checkpoint-interval`)
- Min checkpoint pages configurable (`--min-checkpoint-pages`)
- WAL truncate threshold exists (`--wal-truncate-threshold`)

**Remaining Edge Cases**:

**3.1: Checkpoint Detection During Sync**
- **Problem**: SQLite can checkpoint (reset WAL to frame 0) while walrust is reading
- **Current behavior**: May lose frames or get confused
- **Solution**: Detect checkpoint via WAL header generation change, trigger re-snapshot

**Implementation**:
```rust
// In sync_wal():
let current_generation = wal::read_header(&wal_path)?.generation;
if current_generation != state.wal_generation {
    tracing::warn!("WAL checkpoint detected (gen {} -> {}), taking snapshot",
        state.wal_generation, current_generation);
    state.wal_generation = current_generation;
    state.wal_offset = 0;
    take_snapshot(...).await?;
    return Ok(0); // No frames to sync, snapshot handled it
}
```

**3.2: Partial Frame Read Handling**
- **Problem**: Reading frames while SQLite is writing can get partial data
- **Current behavior**: May upload corrupted LTX
- **Solution**: Validate frame checksum before including in LTX

**Implementation**:
```rust
// In wal::read_frames():
for frame in frames {
    if !frame.validate_checksum() {
        // Frame incomplete, stop here and retry later
        break;
    }
    valid_frames.push(frame);
}
```

**3.3: WAL File Truncation Mid-Read**
- **Problem**: WAL file can shrink mid-read due to checkpoint
- **Current behavior**: May get IO error or stale data
- **Solution**: Re-check file size after read, handle ENOENT gracefully

**3.4: Graceful Checkpoint Coordination**
- **Problem**: External checkpoints (from app) can interfere
- **Current behavior**: Reactive detection only
- **Enhancement**: Proactive checkpoint blocking via read transaction (shadow WAL does this)

**Files to Modify**:
- `src/wal.rs` - Add generation tracking, frame validation
- `src/sync.rs` - Checkpoint detection in sync loop
- `src/shadow.rs` - Ensure checkpoint blocking is robust

**Tests**:
- [x] `test_wal_header_salt` - Salt extraction from WAL header
- [x] `test_wal_header_is_different_generation` - Generation comparison
- [x] `test_read_frames_with_metadata_no_wal` - Handle missing WAL
- [x] `test_read_frames_with_metadata_same_salt` - Normal operation
- [x] `test_read_frames_with_metadata_checkpoint_detection` - Detect checkpoint via salt change

**Success Criteria**:
- [x] No data loss when checkpoint occurs during sync (salt-based detection triggers re-snapshot)
- [x] No corrupted LTX files from partial frames (truncation detection)
- [x] Graceful recovery from WAL truncation (WalReadResult.truncated_during_read flag)
- [x] All edge cases have tests (5 new tests added)

---

### Priority 4: Production Hardening

**Status**: COMPLETE (v0.2.0)

**Current State**:
- Dashboard with Prometheus metrics exists (`--metrics-port`)
- Webhook notifications exist (sync_failed, auth_failure, etc.)
- Retry logic with circuit breaker exists
- Basic exit codes exist

**Remaining Work**:

**4.1: Structured Exit Codes**

```rust
// src/errors.rs - enhance ExitStatus
pub enum ExitStatus {
    Success = 0,
    GeneralError = 1,
    ConfigError = 2,
    DatabaseError = 3,
    S3AuthError = 4,
    S3ConnectionError = 5,
    S3BucketNotFound = 6,
    ChecksumMismatch = 7,
    ManifestCorrupt = 8,
    WalCorrupt = 9,
    DiskFull = 10,
    Interrupted = 130,  // SIGINT
}
```

**CLI Documentation**:
```bash
walrust watch --help
# ...
# EXIT CODES:
#   0   Success
#   1   General error
#   2   Configuration error (invalid config file, missing required args)
#   3   Database error (file not found, corrupt, locked)
#   4   S3 authentication error (invalid credentials)
#   5   S3 connection error (network unreachable, timeout)
#   6   S3 bucket not found
#   7   Checksum mismatch (data corruption detected)
#   130 Interrupted (SIGINT/Ctrl+C)
```

**4.2: Graceful Shutdown Enhancement**

Current: Basic signal handling
Enhancement: Complete in-flight operations, flush metrics

```rust
// Enhanced shutdown in src/sync.rs
async fn graceful_shutdown(
    pending_uploads: Vec<PendingUpload>,
    timeout: Duration,
) -> Result<()> {
    tracing::info!("Shutdown requested, completing {} pending uploads...", pending_uploads.len());

    let start = Instant::now();
    for upload in pending_uploads {
        if start.elapsed() > timeout {
            tracing::warn!("Shutdown timeout, {} uploads abandoned", remaining);
            break;
        }
        upload.complete().await?;
    }

    // Flush metrics
    dashboard::flush_metrics().await;

    tracing::info!("Graceful shutdown complete");
    Ok(())
}
```

**4.3: Enhanced Metrics**

New metrics to add:
```
# Compaction metrics
walrust_compaction_total{db="mydb"} 5
walrust_compaction_duration_seconds{db="mydb"} 2.5
walrust_incrementals_merged_total{db="mydb"} 150

# Cache metrics (for disk-based upload queue)
walrust_cache_size_bytes{db="mydb"} 1048576
walrust_cache_pending_uploads{db="mydb"} 3
walrust_cache_upload_retries_total{db="mydb"} 2

# Checkpoint metrics
walrust_checkpoint_total{db="mydb",type="passive"} 100
walrust_checkpoint_total{db="mydb",type="truncate"} 2
walrust_checkpoint_duration_seconds{db="mydb"} 0.5

# Error classification
walrust_errors_total{db="mydb",type="transient"} 5
walrust_errors_total{db="mydb",type="permanent"} 1
```

**4.4: Better Error Messages**

```rust
// Before:
Err(anyhow!("Failed to upload"))

// After:
Err(WalrustError::S3Upload {
    key: key.to_string(),
    bucket: bucket.to_string(),
    source: e,
    suggestion: "Check S3 credentials and bucket permissions",
})
```

**4.5: Health Check Endpoint**

```bash
# GET /health returns:
{
  "status": "healthy",  // or "degraded", "unhealthy"
  "databases": {
    "mydb": {
      "status": "syncing",
      "last_sync": "2024-01-15T10:30:00Z",
      "lag_seconds": 2,
      "errors_last_hour": 0
    }
  },
  "s3": {
    "connected": true,
    "last_successful_upload": "2024-01-15T10:30:00Z"
  },
  "uptime_seconds": 86400
}
```

**Files to Modify**:
- `src/errors.rs` - Enhanced exit codes, error types
- `src/main.rs` - Exit code handling, shutdown handling
- `src/dashboard.rs` - New metrics, health endpoint
- `src/sync.rs` - Graceful shutdown integration

**Tests**:
- [x] `test_exit_codes` - Verify correct exit code for each error type (in errors.rs)
- [x] `test_graceful_shutdown` - Pending uploads complete (test_uploader_graceful_shutdown)
- [x] `test_health_endpoint_healthy` - Returns healthy status
- [x] `test_health_endpoint_degraded` - Returns degraded status on errors
- [x] `test_health_endpoint_unhealthy` - Returns unhealthy on S3 disconnection
- [x] `test_production_metrics` - Checkpoint, retry, sync latency metrics

**Success Criteria**:
- [x] All exit codes documented and consistent (errors.rs has 7 exit codes)
- [x] Graceful shutdown completes pending work (uploader.rs)
- [x] Health endpoint provides actionable status (/health returns JSON)
- [x] Error messages include troubleshooting hints (classify_error patterns)
- [x] All new metrics exported to Prometheus (checkpoint, retry, sync_latency)

---

## v0.2.0 Implementation Order

Recommended execution sequence:

1. **Litestream Compatibility Test** (1-2 days)
   - Quick validation, low risk
   - Identifies any format issues early

2. **WAL Checkpoint Handling** (2-3 days)
   - Fixes edge cases in existing code
   - Improves reliability before adding features

3. **Production Hardening** (2-3 days)
   - Exit codes, shutdown, metrics
   - Makes tool production-ready

4. **Compaction** (3-5 days)
   - Largest feature
   - Benefits from hardened foundation

**Total Estimate**: 8-13 days of focused work

---

## References

- [Litestream Revamped](https://fly.io/blog/litestream-revamped/) - LTX format, multi-DB
- [Litestream v0.5.0](https://fly.io/blog/litestream-v050-is-here/) - Compaction levels
- [litetx crate](https://docs.rs/litetx/) - Rust LTX implementation
- [Litestream How It Works](https://litestream.io/how-it-works/) - WAL mechanics
- [sled simulation guide](https://sled.rs/simulation.html) - DST architecture inspiration
- [TigerBeetle VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md) - Deterministic simulation testing
- [Jepsen](https://jepsen.io) - Distributed systems testing methodology
