---
title: CLI Reference
description: Complete CLI reference with options and examples
---

## Overview

```bash
walrust <COMMAND>

Commands:
  snapshot   Take an immediate snapshot
  watch      Watch SQLite databases and sync WAL changes to S3
  restore    Restore a database from S3
  list       List databases in S3 bucket
  compact    Clean up old snapshots using retention policy
  replicate  Run as a read replica, polling S3 for changes
  explain    Show what the current configuration will do
  verify     Verify integrity of LTX files in S3
  help       Print help for a command
```

### Global Options

These options apply to all commands:

| Option | Description |
|--------|-------------|
| `--config <PATH>` | Path to config file (default: `./walrust.toml` if exists) |
| `--version` | Print version |
| `-h, --help` | Print help |

---

## snapshot

Take a one-time snapshot of a database to S3.

```bash
walrust snapshot [OPTIONS] --bucket <BUCKET> <DATABASE>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<DATABASE>` | Path to the SQLite database file |

### Options

| Option | Description |
|--------|-------------|
| `-b, --bucket <BUCKET>` | S3 bucket (required) |
| `--endpoint <ENDPOINT>` | S3 endpoint URL for Tigris/MinIO/etc. Also reads from `AWS_ENDPOINT_URL_S3` |
| `-h, --help` | Print help |

### Examples

```bash
# Snapshot to AWS S3
walrust snapshot myapp.db --bucket my-backups

# Snapshot to Tigris
walrust snapshot myapp.db \
  --bucket my-backups \
  --endpoint https://fly.storage.tigris.dev

# Using environment variable for endpoint
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
walrust snapshot myapp.db --bucket my-backups
```

### Output

```
Snapshotting myapp.db to s3://my-backups/myapp.db/...
✓ Snapshot complete (1.2 MB, 445ms)
  Checksum: a3f2b9c8d4e5f6a7b8c9d0e1f2a3b4c5...
```

---

## watch

Continuously watch one or more databases and sync WAL changes to S3.

```bash
walrust watch [OPTIONS] --bucket <BUCKET> <DATABASES>...
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<DATABASES>...` | One or more database files to watch |

### Options

| Option | Description |
|--------|-------------|
| `-b, --bucket <BUCKET>` | S3 bucket (required) |
| `--snapshot-interval <SECONDS>` | Full snapshot interval in seconds (default: 3600 = 1 hour) |
| `--wal-sync-interval <SECONDS>` | WAL sync batching interval in seconds (default: 1) |
| `--endpoint <ENDPOINT>` | S3 endpoint URL for Tigris/MinIO/etc. Also reads from `AWS_ENDPOINT_URL_S3` |
| `--max-changes <N>` | Take snapshot after N WAL frames (0 = disabled) |
| `--max-interval <SECONDS>` | Maximum seconds between snapshots when changes detected |
| `--on-idle <SECONDS>` | Take snapshot after N seconds of no WAL activity (0 = disabled) |
| `--on-startup <true\|false>` | Take snapshot immediately on watch start |
| `--compact-after-snapshot` | Run compaction after each snapshot |
| `--compact-interval <SECONDS>` | Compaction interval in seconds (0 = disabled) |
| `--checkpoint-interval <SECONDS>` | PASSIVE checkpoint interval (default: 60) |
| `--min-checkpoint-pages <N>` | Min WAL pages before checkpoint (default: 1000, ~4MB) |
| `--wal-truncate-threshold <N>` | Emergency truncate threshold in pages (default: 121359, ~500MB) |
| `--checkpoint-release <local\|remote>` | Checkpoint after durable local HADBP admission (default), or additionally wait for contiguous remote publication |
| `--validation-interval <SECONDS>` | Backup validation interval (default: 0, disabled) |
| `--retain-hourly <N>` | Hourly snapshots to retain (default: 24) |
| `--retain-daily <N>` | Daily snapshots to retain (default: 7) |
| `--retain-weekly <N>` | Weekly snapshots to retain (default: 12) |
| `--retain-monthly <N>` | Monthly snapshots to retain (default: 12) |
| `--metrics-port <PORT>` | Prometheus metrics port (default: 16767) |
| `--no-metrics` | Disable metrics server |
| `-h, --help` | Print help |

### Examples

```bash
# Watch a single database
walrust watch myapp.db --bucket my-backups

# Watch multiple databases (single process!)
walrust watch app.db users.db analytics.db --bucket my-backups

# Custom snapshot interval (every 30 minutes)
walrust watch myapp.db \
  --bucket my-backups \
  --snapshot-interval 1800

# Watch with Tigris endpoint
walrust watch myapp.db \
  --bucket my-backups \
  --endpoint https://fly.storage.tigris.dev

# Auto-compact after each snapshot
walrust watch myapp.db \
  --bucket my-backups \
  --compact-after-snapshot

# Periodic compaction every hour
walrust watch myapp.db \
  --bucket my-backups \
  --compact-interval 3600 \
  --retain-hourly 48
```

### Output

```
Watching 3 database(s)...
  - app.db
  - users.db
  - analytics.db

[2024-01-15 10:30:00] app.db: WAL sync (4 frames, 16KB)
[2024-01-15 10:30:05] users.db: WAL sync (2 frames, 8KB)
[2024-01-15 11:30:00] app.db: Scheduled snapshot (1.2 MB)
```

### Running as a Service

For production, run walrust as a systemd service:

```ini
# /etc/systemd/system/walrust.service
[Unit]
Description=Walrust SQLite backup
After=network.target

[Service]
Type=simple
User=app
Environment=AWS_ACCESS_KEY_ID=your-key
Environment=AWS_SECRET_ACCESS_KEY=your-secret
Environment=AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
ExecStart=/usr/local/bin/walrust watch \
  /var/lib/app/data.db \
  --bucket my-backups
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

---

## restore

Restore a database from S3 backup.

```bash
walrust restore [OPTIONS] --output <OUTPUT> --bucket <BUCKET> <NAME>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Database name as stored in S3 (usually the original filename) |

### Options

| Option | Description |
|--------|-------------|
| `-o, --output <OUTPUT>` | Output path for restored database (required) |
| `-b, --bucket <BUCKET>` | S3 bucket (required) |
| `--endpoint <ENDPOINT>` | S3 endpoint URL for Tigris/MinIO/etc. Also reads from `AWS_ENDPOINT_URL_S3` |
| `--point-in-time <TXID>` | Restore through a specific TXID/sequence number |
| `-h, --help` | Print help |

### Examples

```bash
# Basic restore
walrust restore myapp.db \
  --bucket my-backups \
  --output restored.db

# Restore through a specific TXID/sequence number
walrust restore myapp.db \
  --bucket my-backups \
  --output restored.db \
  --point-in-time 42

# Restore from Tigris
walrust restore myapp.db \
  --bucket my-backups \
  --output restored.db \
  --endpoint https://fly.storage.tigris.dev
```

### Output

```
Restoring myapp.db from s3://my-backups/...
  Downloading snapshot... done (1.2 MB)
  Applying WAL segments... done (47 segments)
  Verifying checksum... ✓ a3f2b9c8d4e5f6a7...
✓ Restored to restored.db
```

---

## compact

Clean up old snapshots using retention policy (Grandfather/Father/Son rotation).

```bash
walrust compact [OPTIONS] --bucket <BUCKET> <NAME>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Database name as stored in S3 |

### Options

| Option | Description |
|--------|-------------|
| `-b, --bucket <BUCKET>` | S3 bucket (required) |
| `--endpoint <ENDPOINT>` | S3 endpoint URL for Tigris/MinIO/etc. Also reads from `AWS_ENDPOINT_URL_S3` |
| `--hourly <N>` | Hourly snapshots to keep (default: 24) |
| `--daily <N>` | Daily snapshots to keep (default: 7) |
| `--weekly <N>` | Weekly snapshots to keep (default: 12) |
| `--monthly <N>` | Monthly snapshots to keep (default: 12) |
| `--force` | Actually delete files (default: dry-run only) |
| `-h, --help` | Print help |

### Retention Policy

Walrust uses Grandfather/Father/Son (GFS) rotation:

| Tier | Default | Description |
|------|---------|-------------|
| Hourly | 24 | Snapshots from last 24 hours |
| Daily | 7 | One per day for last week |
| Weekly | 12 | One per week for last 12 weeks |
| Monthly | 12 | One per month beyond 12 weeks |

**Safety guarantees:**
- Always keeps the latest snapshot
- Minimum 2 snapshots retained
- Dry-run by default (`--force` required to delete)

### Examples

```bash
# Dry-run: preview what would be deleted
walrust compact myapp.db --bucket my-backups

# Actually delete old snapshots
walrust compact myapp.db --bucket my-backups --force

# Keep more hourly snapshots
walrust compact myapp.db \
  --bucket my-backups \
  --hourly 48 \
  --force

# Aggressive retention (fewer snapshots)
walrust compact myapp.db \
  --bucket my-backups \
  --hourly 6 \
  --daily 3 \
  --weekly 4 \
  --monthly 3 \
  --force
```

### Output

```
Compaction plan for 'myapp.db':
  Keep: 45 snapshots, Delete: 55 snapshots, Free: 127.50 MB

Keeping 45 snapshots:
  00000001-00000100.ltx (TXID: 100, 2 hours ago)
  00000001-00000095.ltx (TXID: 95, 5 hours ago)
  ...

Deleting 55 snapshots:
  00000001-00000042.ltx (TXID: 42, 3 months ago)
  00000001-00000038.ltx (TXID: 38, 4 months ago)
  ...

Dry-run mode: no files deleted. Use --force to actually delete.
```

---

## replicate

Run as a read replica, polling S3 for new LTX files and applying them locally.

```bash
walrust replicate [OPTIONS] --local <LOCAL> <SOURCE>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<SOURCE>` | S3 location of the database (e.g., `s3://bucket/mydb`) |

### Options

| Option | Description |
|--------|-------------|
| `--local <LOCAL>` | Local database path for the replica (required) |
| `--interval <INTERVAL>` | Poll interval (default: `5s`). Supports `s`, `m`, `h` suffixes |
| `--endpoint <ENDPOINT>` | S3 endpoint URL for Tigris/MinIO/etc. Also reads from `AWS_ENDPOINT_URL_S3` |
| `-h, --help` | Print help |

### How It Works

1. **Bootstrap**: If the local database doesn't exist, downloads the latest snapshot from S3
2. **Poll**: Checks S3 for new LTX files at the specified interval
3. **Apply**: Downloads and applies incremental LTX files in-place (only changed pages)
4. **Track**: Stores current TXID in `.db-replica-state` file for resume capability

### Examples

```bash
# Basic read replica with 5-second polling
walrust replicate s3://my-bucket/mydb --local replica.db --interval 5s

# Replica with custom endpoint (Tigris)
walrust replicate s3://my-bucket/mydb \
  --local /var/lib/app/replica.db \
  --interval 30s \
  --endpoint https://fly.storage.tigris.dev

# Using environment variable for endpoint
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
walrust replicate s3://my-bucket/prefix/mydb --local replica.db

# Fast polling for near-real-time replication
walrust replicate s3://my-bucket/mydb --local replica.db --interval 1s
```

### Output

```
Replicating s3://my-bucket/mydb -> replica.db
Poll interval: 5s
Press Ctrl+C to stop

Bootstrapped from snapshot: 1024 pages, TXID 100
[10:30:05] Applied 1 LTX file(s), now at TXID 101
[10:30:10] Applied 2 LTX file(s), now at TXID 103
```

### State File

Walrust stores replica progress in a `.db-replica-state` file alongside the database:

```json
{
  "current_txid": 103,
  "last_updated": "2024-01-15T10:30:10Z"
}
```

This allows the replica to resume from where it left off after restart.

### Use Cases

- **Read scaling**: Offload read queries to replicas
- **Disaster recovery**: Keep warm standby databases
- **Analytics**: Run heavy queries against a replica without affecting production
- **Edge caching**: Replicate databases closer to users

---

## list

List databases and snapshots stored in S3.

```bash
walrust list [OPTIONS] --bucket <BUCKET>
```

### Options

| Option | Description |
|--------|-------------|
| `-b, --bucket <BUCKET>` | S3 bucket (required) |
| `--endpoint <ENDPOINT>` | S3 endpoint URL for Tigris/MinIO/etc. Also reads from `AWS_ENDPOINT_URL_S3` |
| `-h, --help` | Print help |

### Examples

```bash
# List all databases
walrust list --bucket my-backups

# List with Tigris endpoint
walrust list \
  --bucket my-backups \
  --endpoint https://fly.storage.tigris.dev
```

### Output

```
Databases in s3://my-backups/:

  myapp.db
    Latest snapshot: 2024-01-15 10:30:00 (1.2 MB)
    WAL segments: 47
    Checksum: a3f2b9c8d4e5...

  users.db
    Latest snapshot: 2024-01-15 10:31:00 (256 KB)
    WAL segments: 12
    Checksum: b4c3d2e1f0a9...
```

---

## explain

Show what the current configuration will do without actually running walrust.

```bash
walrust explain [--config <CONFIG>]
```

### Options

| Option | Description |
|--------|-------------|
| `--config <CONFIG>` | Path to config file (default: ./walrust.toml) |
| `-h, --help` | Print help |

### Output

The explain command displays:
- **S3 Storage**: Bucket and endpoint configuration
- **Snapshot Triggers**: Interval, max_changes, on_idle, on_startup settings
- **Compaction**: Whether auto-compaction is enabled
- **Retention Policy**: GFS tier settings (hourly/daily/weekly/monthly)
- **Databases**: Resolved database paths with any per-database overrides

### Examples

```bash
# Explain default config (./walrust.toml)
walrust explain

# Explain specific config file
walrust explain --config /etc/walrust/production.toml
```

### Output Example

```
Configuration Summary
=====================

S3 Storage:
  Bucket:   s3://my-backups/prod
  Endpoint: https://fly.storage.tigris.dev

Snapshot Triggers (global defaults):
  Interval:    3600 seconds (60 minutes)
  Max changes: 100 WAL frames
  On idle:     60 seconds
  On startup:  yes

Compaction:
  After snapshot: enabled
  Interval:       disabled

Retention Policy (GFS rotation):
  Hourly:  24 snapshots (last 24 hours)
  Daily:   7 snapshots (last 7 days)
  Weekly:  12 snapshots (last 12 weeks)
  Monthly: 12 snapshots (last 12 months)

Databases:
  - /var/lib/app.db -> s3://.../main/*
  - /var/lib/users.db -> s3://.../users/*
    Overrides: interval=1800s, max_changes=50

Summary:
  Max snapshots retained per database: ~55
  Automatic compaction: enabled
```

---

## verify

Verify integrity of all LTX files stored in S3 for a database.

```bash
walrust verify [OPTIONS] --bucket <BUCKET> <NAME>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Database name as stored in S3 |

### Options

| Option | Description |
|--------|-------------|
| `-b, --bucket <BUCKET>` | S3 bucket (required) |
| `--endpoint <ENDPOINT>` | S3 endpoint URL for Tigris/MinIO/etc. Also reads from `AWS_ENDPOINT_URL_S3` |
| `-h, --help` | Print help |

### What It Checks

1. **Snapshot Existence**: Verifies at least one snapshot exists (critical)
2. **File Existence**: Each LTX file in the manifest exists in S3
3. **Header Validity**: LTX headers can be decoded successfully
4. **Checksum Verification**: LTX internal checksums match the data
5. **TXID Continuity**: No gaps in the transaction ID chain

### Examples

```bash
# Verify a database
walrust verify myapp.db --bucket my-backups

# Verify with Tigris endpoint
walrust verify myapp.db \
  --bucket my-backups \
  --endpoint https://fly.storage.tigris.dev
```

### Output

```
Verifying backup: myapp.db in s3://my-backups...

Snapshot: Found generation 1 (TXID 1-1, 4096 bytes)

Incremental files: 47 files
  OK 0000000000000002-0000000000000010.ltx (9 TXIDs, 36KB)
  OK 0000000000000011-0000000000000050.ltx (40 TXIDs, 160KB)
  ...

Verified: 48/48 files (12.34 MB total)
Continuity: No gaps detected (TXID 1-1523)

All checks passed - backup integrity verified
Exit code: 0 (success)
```

### Issue Types

| Type | Description | Resolution |
|------|-------------|------------|
| Missing snapshot | No snapshot found in S3 | Take a new snapshot with `walrust snapshot` |
| Checksum failure | Corrupted LTX file | Restore from backup, investigate cause |
| TXID gap | Missing transactions in the chain | May need point-in-time restore |

---

## pragma

Output recommended SQLite PRAGMA settings for optimal walrust performance.

```bash
walrust pragma [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `-o, --output <FILE>` | Write SQL to file instead of stdout |
| `--comments <true\|false>` | Include explanatory comments (default: true) |
| `-h, --help` | Print help |

### Output

The pragma command outputs SQL statements that:

- Disable auto-checkpointing (walrust manages checkpoints)
- Enable WAL mode
- Optimize settings for replication workloads

### Examples

```bash
# Print to stdout
walrust pragma

# Write to file
walrust pragma -o pragma.sql

# Without comments
walrust pragma --comments false
```

---

## Shadow WAL (Default Mode)

Walrust uses shadow WAL by default. It fsyncs copied frames, encodes native
HADBP into a durable local spool, and only then opens a controlled SQLite
checkpoint window. The exact staged bytes upload asynchronously, so S3 latency
does not enter the default checkpoint path while local capacity is healthy.
`--checkpoint-release remote` additionally gates checkpoints on the contiguous
remote publish cursor; it does not make every SQLite commit synchronously
cloud-durable.

Shadow directories are created at `.<database>-walrust/` next to each database file.

---

## Independent Per-DB Tasks

```bash
walrust watch db1.db db2.db --bucket my-bucket --independent-tasks
```

Each database gets its own task that independently watches for WAL changes and syncs to S3. CPU-bound LTX encoding is distributed across the thread pool.

**When to use:** Multi-database deployments where you want maximum concurrency.

---

## Legacy Disk Cache Compatibility

```bash
walrust watch mydb.db --bucket my-bucket \
  --enable-cache \
  --cache-retention 24h \
  --cache-max-size 5368709120
```

These options retain compatibility with legacy LTX queues. Native HADBP spooling
is always active in default shadow mode and is configured with `[spool]`.

- Crash recovery (resume uploads after restart)
- Decoupled encoding from uploads
- Fast local restore (if files still in cache)

| Option | Description |
|--------|-------------|
| `--enable-cache` | Enable disk cache for uploads |
| `--cache-dir <PATH>` | Override cache directory location |
| `--cache-retention <DURATION>` | Cache retention duration (default: 24h) |
| `--cache-max-size <BYTES>` | Maximum cache size (default: 5GB) |
| `--no-cache` | Disable cache even if enabled in config |

---

## Environment Variables

Walrust reads these environment variables:

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS/S3 access key |
| `AWS_SECRET_ACCESS_KEY` | AWS/S3 secret key |
| `AWS_ENDPOINT_URL_S3` | S3 endpoint URL (for Tigris, MinIO, etc.) |
| `AWS_REGION` | AWS region (optional, defaults to `us-east-1`) |

### Example Setup

```bash
# For Tigris (Fly.io)
export AWS_ACCESS_KEY_ID=tid_xxxxx
export AWS_SECRET_ACCESS_KEY=tsec_xxxxx
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev

# For AWS S3
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1

# For MinIO
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL_S3=http://localhost:9000
```

---

## Exit Codes

| Code | Name | Meaning |
|------|------|---------|
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
  2) echo "Config error - check arguments" ;;
  4) echo "S3 error - check credentials/connectivity" ;;
  5) echo "Integrity error - backup may be corrupted" ;;
  *) echo "Other error: $?" ;;
esac
```
