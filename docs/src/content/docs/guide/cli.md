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
| `--endpoint <ENDPOINT>` | S3 endpoint URL for Tigris/MinIO/etc. Also reads from `AWS_ENDPOINT_URL_S3` |
| `--max-changes <N>` | Take snapshot after N WAL frames (0 = disabled) |
| `--max-interval <SECONDS>` | Maximum seconds between snapshots when changes detected |
| `--on-idle <SECONDS>` | Take snapshot after N seconds of no WAL activity (0 = disabled) |
| `--on-startup <true\|false>` | Take snapshot immediately on watch start |
| `--compact-after-snapshot` | Run compaction after each snapshot |
| `--compact-interval <SECONDS>` | Compaction interval in seconds (0 = disabled) |
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
| `--point-in-time <TIMESTAMP>` | Restore to specific point in time (ISO 8601 format) |
| `-h, --help` | Print help |

### Examples

```bash
# Basic restore
walrust restore myapp.db \
  --bucket my-backups \
  --output restored.db

# Restore to specific point in time
walrust restore myapp.db \
  --bucket my-backups \
  --output restored.db \
  --point-in-time "2024-01-15T10:30:00Z"

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
| `--fix` | Remove orphaned entries from manifest |
| `-h, --help` | Print help |

### What It Checks

1. **File Existence**: Each LTX file in the manifest exists in S3
2. **Header Validity**: LTX headers can be decoded successfully
3. **Checksum Verification**: LTX internal checksums match the data
4. **TXID Continuity**: No gaps in the transaction ID chain
5. **Manifest Consistency**: Header TXIDs match manifest entries

### Examples

```bash
# Verify a database (read-only check)
walrust verify myapp.db --bucket my-backups

# Verify with Tigris endpoint
walrust verify myapp.db \
  --bucket my-backups \
  --endpoint https://fly.storage.tigris.dev

# Fix orphaned manifest entries
walrust verify myapp.db --bucket my-backups --fix
```

### Output

```
Verifying integrity of 'myapp.db' in s3://my-backups/myapp.db...

Found 47 LTX files in manifest
Current TXID: 1523
Page size: 4096 bytes

Verification Results
====================
Verified:  45 files (12.34 MB)
Issues:    2

Issues Found:
  [ORPHAN] 00000100-00000105.ltx: File missing from S3
  [ERROR] 00000200-00000210.ltx: Checksum verification failed

Run with --fix to remove 1 orphaned manifest entries.

Note: 1 non-orphan issues found. These may require manual intervention:
  - Checksum failures indicate corrupted files
  - TXID gaps may require restoring from an earlier snapshot
```

### Issue Types

| Type | Description | Fix |
|------|-------------|-----|
| `[ORPHAN]` | Manifest entry exists but S3 file is missing | Use `--fix` to remove from manifest |
| `[ERROR]` | Checksum failure or corrupted file | Restore from backup, investigate cause |
| `TXID gap` | Missing transactions in the chain | May need point-in-time restore |

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

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (any failure) |
