# Changelog

All notable changes to walrust will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
