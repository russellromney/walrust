---
title: Migration from Litestream
description: How to migrate from Litestream to walrust
---

Walrust and Litestream solve the same problem (SQLite WAL replication to S3) but their backups are not interchangeable. This guide helps you migrate from Litestream to walrust.

## Compatibility

:::caution[Not Interchangeable]
Walrust's LTX files cannot be restored by Litestream (walrust enables checksums that Litestream doesn't expect). This is a one-way migration — once you switch, you can't restore walrust backups with Litestream.
:::

### What's Similar

Both tools use:
- **WAL-based replication** - same underlying mechanism
- **S3 storage layout** - similar directory structure
- **GFS retention** - same retention policy model
- **LTX-derived format** - same origin, but walrust's checksums break Litestream compatibility

### What's Different

| Feature | Litestream | Walrust |
|---------|-----------|---------|
| Memory (1 DB) | 36 MB | 19 MB |
| Memory (100 DBs) | 160 MB | 20 MB |
| Language | Go | Rust |
| Config format | YAML | TOML |
| Metrics | Prometheus | Prometheus |

## Feature Comparison

**Both tools support:** WAL-based continuous replication, S3-compatible storage (AWS, Tigris, R2, MinIO), point-in-time recovery, GFS retention, multiple databases per process, checksum verification, Prometheus metrics.

**Litestream only:** SFTP and Azure Blob storage backends.

**Walrust only:** Python API, read replicas with polling, disk cache for upload queue, webhook notifications, circuit breaker, structured exit codes (0-6).

## Migration Steps

### 1. Install Walrust

```bash
# Rust
cargo install walrust

# Python (optional)
pip install walrust
```

Verify installation:

```bash
walrust --version
```

### 2. Stop Litestream

```bash
# systemd
sudo systemctl stop litestream

# Docker
docker stop litestream
```

### 3. Convert Configuration

Litestream uses YAML, walrust uses TOML.

**Litestream config (litestream.yml):**

```yaml
dbs:
  - path: /data/app.db
    replicas:
      - name: s3
        type: s3
        bucket: my-backups
        path: app.db
        endpoint: https://fly.storage.tigris.dev
        retention:
          hourly: 24
          daily: 7
          weekly: 12
          monthly: 12
        snapshot-interval: 1h
        sync-interval: 1s
```

**Walrust config (walrust.toml):**

```toml
[s3]
bucket = "s3://my-backups"
endpoint = "https://fly.storage.tigris.dev"

[sync]
snapshot_interval = 3600  # 1 hour in seconds
wal_sync_interval = 1     # 1 second

[retention]
hourly = 24
daily = 7
weekly = 12
monthly = 12

[[databases]]
path = "/data/app.db"
prefix = "app.db"  # Matches Litestream's path
```

### 4. Verify S3 Credentials

Both tools use standard AWS environment variables:

```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev  # for Tigris
```

### 5. Take a Fresh Snapshot

Since walrust backups are not compatible with Litestream, start fresh:

```bash
walrust snapshot /data/app.db \
  --bucket my-backups \
  --endpoint https://fly.storage.tigris.dev
```

This creates a new walrust-format snapshot. Your old Litestream backups remain in S3 if you need to roll back.

### 6. Start Walrust

```bash
# CLI
walrust watch /data/app.db \
  --bucket my-backups \
  --endpoint https://fly.storage.tigris.dev

# Or with config file
walrust watch --config walrust.toml
```

### 7. Update systemd Service (if applicable)

**Old (Litestream):**

```ini
[Service]
ExecStart=/usr/local/bin/litestream replicate
```

**New (Walrust):**

```ini
[Service]
ExecStart=/usr/local/bin/walrust watch \
  /data/app.db \
  --bucket my-backups \
  --endpoint https://fly.storage.tigris.dev
```

Reload and restart:

```bash
sudo systemctl daemon-reload
sudo systemctl restart walrust
sudo systemctl status walrust
```

### 8. Monitor for Issues

Watch logs:

```bash
# systemd
sudo journalctl -u walrust -f

# Docker
docker logs -f walrust

# Direct
export RUST_LOG=walrust=info
walrust watch /data/app.db --bucket my-backups
```

Check that snapshots are being uploaded:

```bash
walrust list --bucket my-backups --endpoint https://fly.storage.tigris.dev
```

## Configuration Mapping

### Global Settings

| Litestream | Walrust | Notes |
|-----------|---------|-------|
| `addr` (metrics) | `--metrics-port` | Default: 16767 |
| `log-level` | `RUST_LOG` env | Use `RUST_LOG=walrust=info` |

### Replica Settings

| Litestream | Walrust | Notes |
|-----------|---------|-------|
| `bucket` | `[s3].bucket` | Add `s3://` prefix in walrust |
| `endpoint` | `[s3].endpoint` | Same format |
| `path` | `[[databases]].prefix` | S3 prefix for database |
| `snapshot-interval` | `[sync].snapshot_interval` | Seconds in walrust |
| `sync-interval` | `[sync].wal_sync_interval` | Seconds in walrust |
| `retention` | `[retention]` | Same structure |
| `validation-interval` | `[sync].validation_interval` | Seconds in walrust |

### Database Settings

| Litestream | Walrust | Notes |
|-----------|---------|-------|
| `dbs[].path` | `[[databases]].path` | Same |
| Per-DB snapshot interval | `[[databases]].snapshot_interval` | Override global |
| Per-DB retention | `[[databases]].retention` | Override global |

## Command Mapping

| Litestream Command | Walrust Equivalent |
|-------------------|-------------------|
| `litestream replicate` | `walrust watch` |
| `litestream snapshots <db>` | `walrust list --bucket <bucket>` |
| `litestream restore <db> <path>` | `walrust restore <db> -o <path> --bucket <bucket>` |
| `litestream restore -timestamp <time>` | `walrust restore --point-in-time <time>` |
| `litestream databases` | `walrust list --bucket <bucket>` |
| `litestream version` | `walrust --version` |
| `litestream generations` | (Not implemented) |
| `litestream wal` | (Not implemented) |

## Advanced Migration

### Migrating Multiple Databases

**Litestream:**

```yaml
dbs:
  - path: /data/app.db
  - path: /data/users.db
  - path: /data/analytics.db
```

**Walrust:**

```toml
[[databases]]
path = "/data/app.db"

[[databases]]
path = "/data/users.db"

[[databases]]
path = "/data/analytics.db"
```

Or use wildcards:

```toml
[[databases]]
path = "/data/*.db"
```

### Per-Database Configuration

**Litestream:**

```yaml
dbs:
  - path: /data/critical.db
    replicas:
      - name: s3
        snapshot-interval: 5m  # More frequent
        retention:
          hourly: 48
  - path: /data/logs.db
    replicas:
      - name: s3
        snapshot-interval: 1h  # Less frequent
```

**Walrust:**

```toml
[[databases]]
path = "/data/critical.db"
snapshot_interval = 300  # 5 minutes
retention = { hourly = 48, daily = 7, weekly = 12, monthly = 12 }

[[databases]]
path = "/data/logs.db"
snapshot_interval = 3600  # 1 hour (inherits global retention)
```

### Docker Compose

**Litestream:**

```yaml
services:
  litestream:
    image: litestream/litestream
    command: replicate
    volumes:
      - app-data:/data
      - ./litestream.yml:/etc/litestream.yml
```

**Walrust:**

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
      AWS_ENDPOINT_URL_S3: https://fly.storage.tigris.dev
```

## Rollback Plan

If you need to roll back to Litestream:

1. Stop walrust:

```bash
sudo systemctl stop walrust
```

2. Restart Litestream:

```bash
sudo systemctl start litestream
```

3. Your existing Litestream backups are still in S3 — Litestream will continue from the last TXID

Note: Walrust backups cannot be restored by Litestream, so rolling back means losing any backups taken while using walrust.

## See Also

- [Benchmark Results](/benchmarks/results/) — memory/latency comparison data
- [Configuration Reference](/config/configuration-reference/) — all config options
- [Litestream docs](https://litestream.io/reference/) — if you need to switch back
