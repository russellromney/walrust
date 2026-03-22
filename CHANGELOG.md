# Changelog

All notable changes to walrust will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.2] - 2026-03-22

### Polish & Cleanup (v0.3.3)
- **Test improvements**: All 15 webhook tests now run without `#[ignore]` - created real axum HTTP test servers
- **Code cleanup**: Removed 280+ lines of unused code (RetryOutcome, FrameHeader, CompactionConfig, CompactionStats, compact_incrementals(), should_compact())
- **Clippy fixes**: Fixed 17 clippy warnings (unused imports, variables, doc formatting)

## [0.3.2] - 2026-03-22 (Core Features)

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
