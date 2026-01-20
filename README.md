<p align="center">
  <img src="logo.svg" alt="Walrust" width="200">
</p>

# walrust

**Lightweight SQLite replication to S3/Tigris in Rust.**

Walrust continuously replicates SQLite databases to S3-compatible storage, ensuring **minimal data loss** on server crashes, power failures, or disk corruption. Like Litestream but with an emphasis on memory footprint and ease of configuration.

> **v0.3.0:** Read replicas, disk cache for crash recovery, circuit breaker, and webhook notifications.

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
           (polling)            /app/manifest.json
```

1. **Watch** - Poll WAL files for changes at configurable interval
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

  # Validation
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

### `walrust replicate`

Create a read replica that polls S3 for updates.

```bash
walrust replicate <SOURCE> --local <PATH> [OPTIONS]

Options:
  --interval <DURATION>  Poll interval (default: 5s)
  --endpoint <URL>       S3 endpoint
```

### `walrust pragma`

Output SQLite PRAGMA settings for optimal walrust compatibility.

```bash
walrust pragma [OPTIONS]

Options:
  -o, --output <FILE>    Write to SQL file
  --comments <bool>      Include explanatory comments (default: true)
```

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

| Databases | Litestream | Walrust | Reduction |
|-----------|------------|---------|-----------|
| 1 | 37 MB | 19 MB | 49% |
| 10 | 61 MB | 20 MB | 67% |
| 100 | 228 MB | 19 MB | 92% |

*Measured with 100KB databases on macOS, syncing to Tigris S3. See [bench/BENCHMARK_FRAMEWORK.md](bench/BENCHMARK_FRAMEWORK.md) for methodology.*

Walrust's memory usage remains ~19-20 MB regardless of database count.

## Testing

Test suite includes:
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

## Benchmarking

Walrust includes a comprehensive benchmark framework for measuring data loss prevention and replication lag vs litestream.

Quick start:
```bash
# Simple 2-database test
uv run python bench/benchmark.py --config bench/configs/quick.yml

# Scalability matrix: 4 DB counts × 3 write rates = 24 runs
uv run python bench/benchmark.py --config bench/configs/scalability-matrix.yml
```

**What it measures:**
- Data loss detection (all committed writes in S3?)
- Replication lag (P50/P95/P99 sync latency)
- Resource usage (CPU/memory under load)
- Throughput (writes/sec achieved)

**Architecture:**
- DatabaseWriter threads write to SQLite at controlled rates
- walrust/litestream sync to S3 in background
- Restore from S3 and compare expected vs actual writes
- Report data loss and sync latency percentiles

See [bench/BENCHMARK_FRAMEWORK.md](bench/BENCHMARK_FRAMEWORK.md) for full documentation.

## Use with Tenement/Slum

Back up tenant SQLite databases with a single walrust process:

```bash
# In your tenement deployment
walrust watch \
  /var/lib/ourfam/romneys/app.db \
  /var/lib/ourfam/smiths/app.db \
  /var/lib/ourfam/jones/app.db \
  -b s3://backups/ourfam \
  --endpoint https://fly.storage.tigris.dev
```

Memory usage remains low when watching many databases (see Multi-Database Scalability above).

## Documentation

- [Docs Site](https://walrust.dev) - Full documentation
- [ROADMAP.md](ROADMAP.md) - Planned features and direction
- [bench/BENCHMARK_FRAMEWORK.md](bench/BENCHMARK_FRAMEWORK.md) - Benchmark methodology and results

## License

Apache 2.0
