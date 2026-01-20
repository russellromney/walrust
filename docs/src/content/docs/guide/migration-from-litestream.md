---
title: Migration from Litestream
description: How to migrate from Litestream to walrust
---

Walrust and Litestream solve the same problem using the same LTX file format. This guide helps you migrate from Litestream to walrust.

## Why Migrate?

Consider walrust if:

- **Memory-constrained environments** - walrust uses ~12 MB vs Litestream's ~33 MB
- **Multi-database setups** - walrust scales better with many databases (500+ databases in 20 MB)
- **Rust ecosystem** - native integration with Rust projects
- **Simpler configuration** - fewer knobs, easier to reason about

Stay with Litestream if:

- You need its mature ecosystem and community
- You use Litestream Cloud
- You need features walrust doesn't have yet (e.g., restore commands with automatic download from cloud)

## Compatibility

### What's Compatible

Both tools use the same:
- **LTX file format** - walrust can read Litestream backups
- **WAL-based replication** - same underlying mechanism
- **S3 storage layout** - similar directory structure
- **GFS retention** - same retention policy model

### What's Different

| Feature | Litestream | Walrust |
|---------|-----------|---------|
| Memory (1 DB) | ~33 MB | ~12 MB |
| Memory (100 DBs) | ~3.3 GB | ~20 MB |
| Language | Go | Rust |
| Config format | YAML | TOML |
| Cloud service | Yes (Litestream Cloud) | No |
| Metrics | Prometheus | Prometheus |
| Restore command | Downloads automatically | Explicit --bucket required |

## Feature Comparison

### Supported in Both

- ✅ WAL-based continuous replication
- ✅ S3-compatible storage (AWS, Tigris, R2, MinIO)
- ✅ Point-in-time recovery (PITR)
- ✅ GFS retention policy
- ✅ Multiple databases per process
- ✅ Checksum verification (SHA256)
- ✅ Prometheus metrics

### Litestream Only

- ❌ Litestream Cloud integration
- ❌ SFTP/Azure Blob storage backends
- ❌ Consul integration
- ❌ PostgreSQL-like streaming replication

### Walrust Only

- ✅ Python API (`pip install walrust`)
- ✅ Read replicas with polling (`walrust replicate`)
- ✅ Disk cache for upload queue (optional)
- ✅ Webhook notifications for failures
- ✅ Circuit breaker for S3 failures
- ✅ Structured exit codes (0-6)

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

### 5. Test Restore (Optional)

Before switching, test that walrust can read your Litestream backups:

```bash
walrust restore app.db \
  --bucket my-backups \
  -o /tmp/test.db \
  --endpoint https://fly.storage.tigris.dev
```

If this works, walrust is compatible with your existing backups.

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

3. Your existing backups are unchanged - Litestream will continue from the last TXID

Both tools use the same LTX format, so they're fully interchangeable.

## Performance Comparison

Based on our benchmarks:

| Metric | Litestream | Walrust |
|--------|-----------|---------|
| Memory (1 DB) | 33 MB | 12 MB |
| Memory (10 DBs) | 330 MB | 16 MB |
| Memory (100 DBs) | ~3.3 GB | ~20 MB |
| CPU (idle) | <1% | <1% |
| CPU (active) | 2-5% | 2-5% |
| Sync latency (P95) | ~1s | ~1s |

**Winner for multi-database:** Walrust (9x-165x less memory)

See [Benchmark Results](/benchmarks/results/) for full details.

## Getting Help

**Litestream resources:**
- [Litestream docs](https://litestream.io/reference/)
- [Litestream Discord](https://discord.gg/Wh5F2RM)

**Walrust resources:**
- [Walrust docs](https://walrust.dev)
- [GitHub Issues](https://github.com/russellromney/walrust/issues)
- [Configuration Reference](/config/configuration-reference/)

## What's Next?

After migrating:

1. **Test your backups** - Run periodic test restores
2. **Set up monitoring** - Use Prometheus metrics endpoint
3. **Enable validation** - Configure `validation_interval` to verify backups
4. **Optimize retention** - Adjust retention policy based on your needs

Welcome to walrust! 🦀
