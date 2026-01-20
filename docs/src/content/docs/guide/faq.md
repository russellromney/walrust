---
title: FAQ
description: Frequently asked questions about walrust
---

Common questions about using walrust.

## General

### What is walrust?

Walrust is a lightweight SQLite replication tool written in Rust. It continuously backs up SQLite databases to S3-compatible storage by watching WAL (Write-Ahead Log) files and uploading changes as LTX files.

### How is walrust different from Litestream?

Both tools solve the same problem, but walrust prioritizes:

- **Lower memory footprint** - ~12 MB baseline vs Litestream's ~33 MB
- **Multi-database efficiency** - Shared S3 client and file watcher across all databases
- **Rust-native** - Better integration with Rust projects, smaller binary
- **Simplicity** - Fewer features, easier to understand and modify

See [Migration from Litestream](/guide/migration-from-litestream/) for a detailed comparison.

### Is walrust production-ready?

Walrust is actively developed and used in production. It has:

- 173+ tests including chaos testing and property-based testing
- Battle-tested LTX format from Litestream
- SHA256 checksums for data integrity
- Comprehensive benchmark suite

That said, always test your backups and have a disaster recovery plan.

### What databases does walrust support?

Walrust works with any SQLite database in WAL mode. This includes:

- Raw SQLite databases
- [Turso](https://turso.tech) local databases
- Python apps using sqlite3
- Node.js apps using better-sqlite3
- Any application using SQLite

### Can I use walrust with non-WAL databases?

No. Walrust requires WAL mode to capture incremental changes. Enable it with:

```sql
PRAGMA journal_mode=WAL;
```

## Setup & Configuration

### Do I need AWS S3?

No. Walrust works with any S3-compatible storage:

- AWS S3
- Tigris (Fly.io's object storage)
- Cloudflare R2
- MinIO (self-hosted)
- Backblaze B2
- DigitalOcean Spaces

See [S3 Providers](/config/s3-providers/) for setup guides.

### How do I configure walrust?

Three ways:

1. **CLI arguments** (quick, one-off commands)
2. **Environment variables** (for credentials)
3. **Config file** (`walrust.toml` for complex setups)

See [Configuration Reference](/config/configuration-reference/) for all options.

### Can I watch multiple databases?

Yes! Pass multiple paths:

```bash
walrust watch app.db users.db analytics.db --bucket my-backups
```

Or use wildcards in a config file:

```toml
[[databases]]
path = "/data/*.db"
```

Walrust uses one process for all databases with minimal memory overhead.

### How often are snapshots taken?

Default: every 3600 seconds (1 hour). Configure with:

```toml
[sync]
snapshot_interval = 1800  # 30 minutes
```

You can also trigger snapshots based on:
- WAL frame count (`max_changes`)
- Idle time (`on_idle`)
- Time since last change (`max_interval`)

## Backups & Restore

### How much data will I lose if my server crashes?

Depends on `wal_sync_interval`:

- Default (1 second): Up to 1 second of data
- Aggressive (0.5 seconds): Up to 0.5 seconds of data

WAL changes are batched and uploaded on this interval. Lower values = less data loss but more S3 API calls.

### How do I restore a database?

```bash
walrust restore mydb --bucket my-backups -o restored.db
```

This downloads the latest snapshot and applies all incremental LTX files.

### Can I restore to a specific point in time?

Yes, using point-in-time recovery (PITR):

```bash
walrust restore mydb \
  --bucket my-backups \
  -o restored.db \
  --point-in-time "2024-01-15T10:30:00Z"
```

Walrust will restore to the closest transaction before that timestamp.

### How do I test my backups?

Run periodic test restores:

```bash
#!/bin/bash
# test-restore.sh
walrust restore mydb --bucket my-backups -o /tmp/test.db
sqlite3 /tmp/test.db "PRAGMA integrity_check;"
if [ $? -eq 0 ]; then
  echo "Backup verified successfully"
  rm /tmp/test.db
else
  echo "Backup verification FAILED"
  exit 1
fi
```

Schedule this with cron or CI.

### How do I verify backup integrity?

Use the verify command:

```bash
walrust verify mydb --bucket my-backups
```

This checks:
- File existence
- SHA256 checksums
- TXID continuity
- LTX header validity

You can also enable automated verification:

```toml
[sync]
validation_interval = 86400  # Verify daily
```

## Storage & Retention

### How much S3 storage will I use?

It depends on:
- Database size
- Write rate
- Retention policy

**Example:** A 100 MB database with moderate writes and default retention (24 hourly + 7 daily + 12 weekly + 12 monthly snapshots) uses roughly:

```
~100 MB (latest snapshot)
+ ~50 MB (hourly incrementals)
+ ~300 MB (older snapshots)
= ~450 MB total
```

### How do I reduce storage costs?

1. **Aggressive retention:**

```toml
[retention]
hourly = 6   # Keep only last 6 hours
daily = 3    # Last 3 days
weekly = 4   # Last 4 weeks
monthly = 3  # Last 3 months
```

2. **Auto-compaction:**

```toml
[sync]
compact_after_snapshot = true
```

3. **Manual compaction:**

```bash
walrust compact mydb --bucket my-backups --force
```

### What happens to old snapshots?

Walrust uses Grandfather-Father-Son (GFS) rotation to keep storage bounded:

| Tier | Default | Keeps |
|------|---------|-------|
| Hourly | 24 | Last 24 hours |
| Daily | 7 | One per day for a week |
| Weekly | 12 | One per week for 12 weeks |
| Monthly | 12 | One per month beyond that |

Run `walrust compact` to delete old snapshots according to this policy.

### Is my data encrypted?

**In transit:** Yes, HTTPS by default.

**At rest:** Depends on your S3 provider:
- AWS S3: Enable server-side encryption (SSE-S3 or SSE-KMS)
- Tigris: Enabled by default
- MinIO: Configure encryption in MinIO settings

Walrust doesn't do client-side encryption (yet). Use your S3 provider's encryption features.

## Performance

### How much memory does walrust use?

- **Baseline:** ~12 MB (single database)
- **Multi-database:** ~12-20 MB (10-500 databases)

Walrust shares S3 clients and file watchers, so adding databases has minimal memory impact.

### How much CPU does it use?

- **Idle:** <1%
- **Active syncing:** 2-5% on modern hardware
- **High write rate (10K+ writes/sec):** 10-20%

If CPU is high, increase `monitor_interval` or `wal_sync_interval`.

### Can walrust keep up with high write rates?

Yes. Benchmarks show walrust handles:

- 10K+ writes/sec with 500 concurrent databases
- 4% average CPU usage
- <1 second sync latency (P95)

See [Benchmark Results](/benchmarks/results/) for details.

### Does walrust slow down my application?

No. Walrust watches the WAL file externally and doesn't interfere with SQLite operations. Your app continues writing normally.

## Read Replicas

### What are read replicas?

Read replicas are local databases that poll S3 for changes and stay in sync with the primary database. Useful for:

- Offloading read queries
- Running analytics without affecting production
- Disaster recovery (warm standby)

### How do I create a read replica?

```bash
walrust replicate s3://my-bucket/mydb --local replica.db --interval 5s
```

This polls S3 every 5 seconds, downloads new LTX files, and applies them to the local database.

### How fresh is replica data?

Freshness = `wal_sync_interval` (primary) + `interval` (replica) + S3 propagation time

**Example:**
- Primary syncs every 1 second
- Replica polls every 5 seconds
- S3 eventual consistency: ~1 second

**Total lag:** ~7 seconds (P95)

For near-real-time replication, use `--interval 1s`.

### Can replicas write data?

No. Replicas are read-only. Walrust will reject writes to replica databases to prevent conflicts.

## Python Integration

### How do I use walrust from Python?

Install via pip:

```bash
pip install walrust
```

Use the Python API:

```python
from walrust import Walrust

# Create instance
ws = Walrust("s3://my-bucket", endpoint="https://fly.storage.tigris.dev")

# Snapshot
ws.snapshot("/path/to/app.db")

# List databases
dbs = ws.list()

# Restore
ws.restore("app", "/path/to/restored.db")
```

See [Python API Reference](/guide/python-api/) for full documentation.

### Can I use walrust in a Jupyter notebook?

Yes! Same Python API works in notebooks:

```python
from walrust import snapshot, restore

# Backup
snapshot("analysis.db", "s3://my-bucket")

# Later... restore
restore("analysis", "analysis-restored.db", "s3://my-bucket")
```

## Deployment

### How do I run walrust in production?

Use systemd, Docker, or Kubernetes. See [Deployment Guide](/config/deployment/) for examples.

**Recommended: systemd**

```ini
[Service]
ExecStart=/usr/local/bin/walrust watch /data/app.db --bucket my-backups
Restart=always
```

### Can I run walrust in Docker?

Yes. Mount your database volume:

```yaml
services:
  walrust:
    image: walrust
    command: watch /data/app.db --bucket my-backups
    volumes:
      - app-data:/data:ro
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
```

### Should walrust run as a separate process or in-app?

**Separate process (recommended):**
- Easier to restart independently
- Simpler deployment
- Works with any language

**In-app (Python only):**
- Fewer moving parts
- Tighter integration
- Good for simple deployments

For production, run walrust as a separate process (sidecar, systemd service, etc.).

## Troubleshooting

### Walrust isn't uploading to S3

1. Check credentials:

```bash
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
```

2. Verify bucket exists:

```bash
walrust list --bucket my-backups
```

3. Enable debug logging:

```bash
export RUST_LOG=walrust=debug
walrust watch app.db --bucket my-backups
```

See [Troubleshooting Guide](/guide/troubleshooting/) for more.

### How do I see what walrust is doing?

Enable logging:

```bash
export RUST_LOG=walrust=info
walrust watch app.db --bucket my-backups
```

Log levels: `error`, `warn`, `info`, `debug`, `trace`

### Backups are failing silently

Check exit codes in your systemd service:

```bash
sudo journalctl -u walrust -n 50
```

Walrust uses structured exit codes (0-6) to indicate different error types. See [Troubleshooting](/guide/troubleshooting/) for details.

## Advanced

### Can I use walrust with Raft or distributed SQLite?

No. Walrust is designed for single-node SQLite databases. For distributed setups, consider:

- [rqlite](https://rqlite.io) (Raft-based distributed SQLite)
- [LiteFS](https://github.com/superfly/litefs) (FUSE-based replication)
- Primary-replica with walrust (one primary, multiple read replicas)

### Does walrust support encryption at rest?

Not built-in. Use your S3 provider's server-side encryption:

- AWS S3: SSE-S3 or SSE-KMS
- Tigris: Enabled by default
- MinIO: Configure via encryption settings

Client-side encryption may be added in a future version.

### Can I contribute?

Yes! Walrust is open source (Apache 2.0). See the [GitHub repo](https://github.com/russellromney/walrust) for:

- Issues and feature requests
- Pull requests
- Development setup

### How do I stay updated?

- Watch the [GitHub repo](https://github.com/russellromney/walrust)
- Check [CHANGELOG.md](https://github.com/russellromney/walrust/blob/main/CHANGELOG.md)
- Follow [@russellromney](https://github.com/russellromney) on GitHub
