<p align="center">
  <img src="logo.svg" alt="Walrust" width="200">
</p>

# walrust

**Lightweight SQLite replication to S3/Tigris in Rust.**

Like Litestream but with an emphasis on memory footprint and easy of configuration.

> **v0.1.8:** Performance optimization to break 5K w/s ceiling. Pre-allocated buffers, CPU parallelization via spawn_blocking, improved S3 connection pooling. Target: 10K+ w/s at 250 DBs. Memory: ~50-100 MB (up from 20 MB, still 7-14x less than Litestream).

## Installation

### CLI (Rust)
```bash
cargo install walrust
```

### Python Package
```bash
pip install walrust
```

Then use from Python:
```python
from walrust import Walrust

# Create instance
ws = Walrust("s3://my-bucket", endpoint="https://fly.storage.tigris.dev")

# Snapshot a database
ws.snapshot("/path/to/app.db")

# List backed up databases
dbs = ws.list()

# Restore a database
ws.restore("app", "/path/to/restored.db")
```

## Quick Start

```bash
# Watch databases and sync to S3
walrust watch db1.db db2.db -b s3://my-bucket/backups

# With Tigris endpoint
walrust watch app.db -b s3://my-bucket --endpoint https://fly.storage.tigris.dev

# With auto-compaction after each snapshot
walrust watch app.db -b s3://my-bucket --compact-after-snapshot

# Take immediate snapshot
walrust snapshot app.db -b s3://my-bucket

# List backed up databases
walrust list -b s3://my-bucket

# Restore database
walrust restore mydb -o restored.db -b s3://my-bucket

# Clean up old snapshots (dry-run)
walrust compact mydb -b s3://my-bucket

# Actually delete old snapshots
walrust compact mydb -b s3://my-bucket --force
```

## Acknowledgments

Walrust wouldn't exist without [Litestream](https://litestream.io) and the work of [Ben Johnson](https://github.com/benbjohnson). Litestream was the first place I saw WAL-based SQLite replication to cloud storage, and walrust uses the same [LTX file format](https://github.com/superfly/ltx) for efficient compaction and replication.

## How It Works

```
Local:                          S3 (LTX format):
app.db                          /app/00000001-00000001.ltx  (snapshot)
app.db-wal  ────────────────►   /app/00000002-00000010.ltx  (incremental)
           (file watcher)       /app/manifest.json
```

1. **Watch** - Monitor WAL files for changes (inotify/kqueue)
2. **Sync** - Upload new WAL frames as LTX files to S3
3. **Snapshot** - Periodic full database snapshots (configurable interval)
4. **Restore** - Download snapshot + apply incremental LTX files

## Commands

### `walrust watch`

Watch databases and continuously sync WAL changes.

```bash
walrust watch <DATABASES>... -b <BUCKET> [OPTIONS]

Options:
  --snapshot-interval <SECS>       Snapshot interval (default: 3600)
  --wal-sync-interval <SECS>       WAL sync batching interval (default: 1)
  --endpoint <URL>                 S3 endpoint (for Tigris/MinIO)

  # Checkpointing (prevent unbounded WAL growth)
  --checkpoint-interval <SECS>     Checkpoint interval (default: 60)
  --min-checkpoint-pages <N>       Min pages before checkpoint (default: 1000, ~4MB)
  --wal-truncate-threshold <N>     Emergency truncate threshold (default: 121359, ~500MB)

  # Monitoring & Validation
  --monitor-interval <SECS>        File watcher check interval (default: 1)
  --validation-interval <SECS>     Backup validation interval (default: 0, disabled)

  # Compaction
  --compact-after-snapshot         Run compaction after each snapshot
  --compact-interval <SECS>        Compaction interval in seconds (0 = disabled)

  # Retention
  --retain-hourly <N>              Hourly snapshots to keep (default: 24)
  --retain-daily <N>               Daily snapshots to keep (default: 7)
  --retain-weekly <N>              Weekly snapshots to keep (default: 12)
  --retain-monthly <N>             Monthly snapshots to keep (default: 12)
```

### `walrust snapshot`

Take an immediate snapshot.

```bash
walrust snapshot <DATABASE> -b <BUCKET>
```

### `walrust restore`

Restore a database from S3.

```bash
walrust restore <NAME> -o <OUTPUT> -b <BUCKET>

Options:
  --point-in-time <ISO8601>  Restore to specific time
```

### `walrust compact`

Clean up old snapshots using retention policy (GFS rotation).

```bash
walrust compact <NAME> -b <BUCKET> [OPTIONS]

Options:
  --hourly <N>    Hourly snapshots to keep (default: 24)
  --daily <N>     Daily snapshots to keep (default: 7)
  --weekly <N>    Weekly snapshots to keep (default: 12)
  --monthly <N>   Monthly snapshots to keep (default: 12)
  --force         Actually delete files (default: dry-run only)
```

**Example:**
```bash
# Preview what would be deleted
walrust compact mydb -b s3://my-bucket

# Actually delete old snapshots
walrust compact mydb -b s3://my-bucket --force

# Keep more hourly snapshots
walrust compact mydb -b s3://my-bucket --hourly 48 --force
```

### `walrust list`

List backed up databases.

```bash
walrust list -b <BUCKET>
```

### `walrust explain`

Show what the current configuration will do without running.

```bash
walrust explain [--config <CONFIG>]
```

Displays: S3 settings, snapshot triggers, compaction settings, retention policy, and resolved database paths.

### `walrust verify`

Verify integrity of LTX files in S3.

```bash
walrust verify <NAME> -b <BUCKET> [OPTIONS]

Options:
  --endpoint <URL>  S3 endpoint
  --fix             Remove orphaned manifest entries
```

Checks: file existence, header validity, checksums, TXID continuity.

## Exit Codes

Walrust uses structured exit codes for scripting and automation:

| Code | Name | Description |
|------|------|-------------|
| 0 | Success | Operation completed successfully |
| 1 | General | Unknown or uncategorized error |
| 2 | Config | Configuration error (invalid config file, missing CLI args) |
| 3 | Database | Database error (file not found, WAL corruption, SQLite issues) |
| 4 | S3 | S3 error (network, authentication, bucket access) |
| 5 | Integrity | Integrity error (checksum mismatch, LTX verification failed) |
| 6 | Restore | Restore error (no snapshot found, PITR unavailable) |

**Example usage in scripts:**
```bash
walrust verify mydb -b s3://bucket
case $? in
  0) echo "Verification passed" ;;
  5) echo "Integrity error - backup may be corrupted" ;;
  4) echo "S3 error - check credentials/connectivity" ;;
  *) echo "Other error: $?" ;;
esac
```

## Environment Variables

- `AWS_ACCESS_KEY_ID` - AWS/Tigris access key
- `AWS_SECRET_ACCESS_KEY` - AWS/Tigris secret key
- `AWS_ENDPOINT_URL_S3` - S3 endpoint (for Tigris/MinIO)
- `AWS_REGION` - AWS region (default: us-east-1)

## Configuration File

Create `walrust.toml` in your project directory:

```toml
[s3]
bucket = "s3://my-bucket/backups"
endpoint = "https://fly.storage.tigris.dev"

[sync]
snapshot_interval = 3600        # Snapshot every hour
wal_sync_interval = 1           # Batch WAL syncs every 1 second
checkpoint_interval = 60        # Checkpoint every 60 seconds
min_checkpoint_page_count = 1000  # Only checkpoint if WAL >= 1000 pages (~4MB)
wal_truncate_threshold_pages = 121359  # Emergency truncate at 500MB
monitor_interval = 1            # File watcher check interval (debounce)
validation_interval = 86400     # Backup validation every 24 hours (0 = disabled)

max_changes = 1000              # Snapshot after 1000 WAL frames
max_interval = 300              # Snapshot after 5 min of changes
on_idle = 60                    # Snapshot after 60 sec of no activity

compact_after_snapshot = true
compact_interval = 3600

[retention]
hourly = 24
daily = 7
weekly = 12
monthly = 12

# Retry configuration for transient S3 failures
[retry]
max_retries = 5                 # Number of retry attempts
base_delay_ms = 100             # Initial backoff delay
max_delay_ms = 30000            # Maximum backoff cap (30s)
circuit_breaker_enabled = true  # Enable circuit breaker
circuit_breaker_threshold = 10  # Failures before circuit opens
circuit_breaker_cooldown_ms = 60000  # Cooldown before half-open (1 min)

# Webhook notifications for failure events
[[webhooks]]
url = "https://example.com/walrust-webhook"
events = ["sync_failed", "auth_failure", "corruption_detected", "circuit_breaker_open"]
secret = "optional-hmac-secret"  # For X-Walrust-Signature header

[[databases]]
path = "/data/app.db"
prefix = "production"

[[databases]]
path = "/data/analytics.db"
checkpoint_interval = 30        # Override: checkpoint more frequently
wal_truncate_threshold_pages = 50000  # Override: lower emergency threshold
monitor_interval = 5            # Override: debounce every 5 seconds
validation_interval = 3600      # Override: validate hourly for this DB
```

Then run:
```bash
walrust watch  # Auto-discovers walrust.toml
# or
walrust watch --config custom.toml
```

## S3 Layout (LTX Format)

```
s3://bucket/prefix/
├── dbname/
│   ├── 00000001-00000001.ltx     # Snapshot (TXID 1)
│   ├── 00000002-00000010.ltx     # Incremental (TXID 2-10)
│   ├── 00000011-00000050.ltx     # Incremental (TXID 11-50)
│   └── manifest.json             # Index of LTX files
└── otherdb/
    └── ...
```

## Data Integrity

### SHA256 Verification
Every snapshot includes an SHA256 checksum stored in S3 object metadata (`x-amz-meta-sha256`). During restore, checksums are automatically verified:

```
✓ Checksum stored during snapshot
✓ Verified automatically on restore
✓ Fail-fast on corruption detection
✓ Works with existing backups (optional)
```


### Snapshot Compaction

Walrust uses Grandfather/Father/Son (GFS) rotation to manage snapshot retention:

| Tier | Default | Description |
|------|---------|-------------|
| Hourly | 24 | Snapshots from last 24 hours |
| Daily | 7 | One per day for last week |
| Weekly | 12 | One per week for last 12 weeks |
| Monthly | 12 | One per month beyond 12 weeks |

**Safety guarantees:**
- Always keeps latest snapshot
- Minimum 2 snapshots retained
- Dry-run by default (--force required to delete)

**Auto-compaction modes:**
```bash
# After each snapshot
walrust watch app.db -b s3://bucket --compact-after-snapshot

# On interval (every hour)
walrust watch app.db -b s3://bucket --compact-interval 3600
```

### Multi-Database Scalability

| Databases | Litestream | Walrust | Savings |
|-----------|-----------|---------|---------|
| 1 | 33 MB (1 process) | 12 MB (1 process) | **21 MB** |
| 5 | 152 MB (5 processes) | 14 MB (1 process) | **138 MB** |
| 10 | 286 MB (10 processes) | 12 MB (1 process) | **274 MB** |
| 20 | 600 MB (20 processes) | 12 MB (1 process) | **588 MB** |

*Measured on macOS with 100KB test databases. See [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) for full results.*

Single walrust process handles multiple databases with shared S3 connection pooling.

## Testing

173 tests covering:
- ✅ Byte-for-byte data integrity (snapshot → restore → verify)
- ✅ SHA256 checksum storage and verification
- ✅ Multi-database concurrent snapshots
- ✅ WAL file format parsing
- ✅ S3 operations
- ✅ Retry logic with exponential backoff
- ✅ Chaos testing with fault injection (walrust-dst)
- ✅ Property-based testing (7 properties, 100+ cases each)
- ✅ Core invariants (transaction recovery, WAL batching, snapshot atomicity)
- ✅ Continuous chaos testing with MTBF tracking

Run tests: `./run_tests.sh` (requires Tigris credentials in `.env`)

## Use with Tenement/Slum

Perfect for backing up tenant SQLite databases:

```bash
# In your tenement deployment
walrust watch \
  /var/lib/ourfam/romneys/app.db \
  /var/lib/ourfam/smiths/app.db \
  /var/lib/ourfam/jones/app.db \
  -b s3://backups/ourfam \
  --endpoint https://fly.storage.tigris.dev
```

All databases sync with single process, saving ~275MB memory vs Litestream for 10 databases.

## Documentation

- [Docs Site](https://walrust.dev) - Full documentation
- [ROADMAP.md](ROADMAP.md) - Planned features and direction
- [BENCHMARK_RESULTS.md](BENCHMARK_RESULTS.md) - Performance benchmark results
- [TESTING.md](TESTING.md) - Comprehensive testing guide
- [bench/](bench/) - Performance benchmarks (micro, comparison, real-world)

## License

Apache 2.0
