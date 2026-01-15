# Changelog

All notable changes to walrust will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
  - Full integration pending (see ROADMAP.md)

### Changed
- `RetryPolicy` now derives `Clone` for use in concurrent sync futures

### Performance
- Benchmark at 100 DBs x 50 w/s: Sequential processing was the bottleneck
- After concurrent fix: S3 upload latency becomes the limiting factor
- Shadow WAL (when fully integrated) will further decouple uploads from writes

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
  - [CONFIG_VERIFICATION.md](./CONFIG_VERIFICATION.md) - Production readiness assessment
  - [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) - Detailed plan for missing features
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
