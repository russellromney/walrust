---
title: Configuration Reference
description: Complete walrust.toml configuration reference
---

Complete reference for the `walrust.toml` configuration file.

## File Location

Walrust looks for configuration in:

1. `--config` CLI argument (if specified)
2. `./walrust.toml` in current directory (if exists)
3. Falls back to CLI arguments

**Example:**

```bash
# Use default location (./walrust.toml)
walrust watch

# Use specific config file
walrust watch --config /etc/walrust/production.toml
```

## Minimal Configuration

The smallest valid config:

```toml
[[databases]]
path = "/data/app.db"
```

CLI arguments are still required:

```bash
walrust watch --config walrust.toml --bucket my-backups
```

## Complete Example

```toml
# S3 storage configuration
[s3]
bucket = "s3://my-backups/production"
endpoint = "https://fly.storage.tigris.dev"

# Global sync/snapshot settings
[sync]
snapshot_interval = 3600
wal_sync_interval = 1
max_changes = 1000
max_interval = 300
on_idle = 60
on_startup = true
compact_after_snapshot = true
compact_interval = 0
checkpoint_interval = 60
min_checkpoint_page_count = 1000
wal_truncate_threshold_pages = 121359

validation_interval = 86400

# Retention policy (GFS)
[retention]
hourly = 24
daily = 7
weekly = 12
monthly = 12

# Disk cache for upload queue
[cache]
enabled = false
retention = "24h"
max_size = 5368709120
path = "/var/cache/walrust"

# Retry configuration
[retry]
max_retries = 5
base_delay_ms = 100
max_delay_ms = 30000
circuit_breaker_enabled = true
circuit_breaker_threshold = 10
circuit_breaker_cooldown_ms = 60000

# Webhook notifications
[[webhooks]]
url = "https://example.com/webhook"
events = ["sync_failed", "auth_failure", "corruption_detected", "circuit_breaker_open"]
secret = "your-hmac-secret"

# Database configurations
[[databases]]
path = "/data/app.db"
prefix = "production"

[[databases]]
path = "/data/analytics.db"
prefix = "analytics"
snapshot_interval = 1800
validation_interval = 3600
```

## Sections

### [s3]

S3 storage configuration.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | string | none | S3 bucket URL (e.g., `"s3://my-bucket"` or `"my-bucket/prefix"`) |
| `endpoint` | string | none | S3 endpoint URL for non-AWS providers (e.g., Tigris, MinIO, R2) |

**Notes:**

- `bucket` can include a prefix: `"s3://my-bucket/backups"`
- `endpoint` is required for Tigris, MinIO, Cloudflare R2
- For AWS S3, `endpoint` can be omitted
- Can be overridden by CLI args (`--bucket`, `--endpoint`)

**Example:**

```toml
[s3]
bucket = "s3://my-backups/production"
endpoint = "https://fly.storage.tigris.dev"
```

---

### [sync]

Global sync and snapshot triggers.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `snapshot_interval` | integer | 3600 | Full snapshot interval in seconds (3600 = 1 hour) |
| `wal_sync_interval` | integer | 1 | WAL sync batching interval in seconds (1 = batch every second) |
| `max_changes` | integer | 0 | Take snapshot after N WAL frames (0 = disabled) |
| `max_interval` | integer | 0 | Max seconds between snapshots when changes detected (0 = disabled) |
| `on_idle` | integer | 0 | Take snapshot after N seconds of no activity (0 = disabled) |
| `on_startup` | boolean | true | Take snapshot immediately when watch starts |
| `compact_after_snapshot` | boolean | false | Run compaction after each snapshot |
| `compact_interval` | integer | 0 | Compaction interval in seconds (0 = disabled) |
| `checkpoint_interval` | integer | 60 | Run PRAGMA wal_checkpoint(PASSIVE) every N seconds |
| `min_checkpoint_page_count` | integer | 1000 | Min WAL pages before checkpoint (1000 pages ≈ 4 MB) |
| `wal_truncate_threshold_pages` | integer | 121359 | Emergency truncate at N WAL pages (121359 ≈ 500 MB) |
| `validation_interval` | integer | 0 | Automated backup verification interval in seconds (0 = disabled) |

**Snapshot Triggers:**

Multiple triggers can be active. Snapshot occurs when ANY trigger fires:

```toml
[sync]
snapshot_interval = 3600   # Every hour
max_changes = 1000          # OR after 1000 WAL frames
on_idle = 60                # OR after 60 seconds of inactivity
```

**Checkpointing:**

Prevents unbounded WAL growth:

```toml
[sync]
checkpoint_interval = 60                    # Check every 60 seconds
min_checkpoint_page_count = 1000            # Only if WAL ≥ 1000 pages
wal_truncate_threshold_pages = 121359       # Emergency truncate at 500 MB
```

**Validation:**

Automated integrity checks:

```toml
[sync]
validation_interval = 86400  # Verify backups daily
```

This runs `walrust verify` automatically and logs errors.

**Example:**

```toml
[sync]
snapshot_interval = 1800    # Snapshot every 30 minutes
wal_sync_interval = 1       # Batch WAL syncs every second
max_changes = 500           # Also snapshot after 500 frames
on_idle = 120               # Also snapshot after 2 minutes idle
on_startup = true
compact_after_snapshot = true
checkpoint_interval = 60
validation_interval = 86400  # Daily validation
```

---

### [retention]

Retention policy using Grandfather-Father-Son (GFS) rotation.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `hourly` | integer | 24 | Keep last N hourly snapshots |
| `daily` | integer | 7 | Keep one snapshot per day for N days |
| `weekly` | integer | 12 | Keep one snapshot per week for N weeks |
| `monthly` | integer | 12 | Keep one snapshot per month for N months |

**Rules:**

- At least one tier must be > 0
- Latest snapshot is always kept
- Minimum 2 snapshots retained

**Example:**

```toml
[retention]
hourly = 24   # Last 24 hours
daily = 7     # Last 7 days (one per day)
weekly = 12   # Last 12 weeks (one per week)
monthly = 12  # Last 12 months (one per month)
```

**Aggressive retention (less storage):**

```toml
[retention]
hourly = 6
daily = 3
weekly = 4
monthly = 3
```

**Relaxed retention (more storage):**

```toml
[retention]
hourly = 48
daily = 14
weekly = 24
monthly = 24
```

---

### [cache]

Disk cache for LTX upload queue (opt-in feature).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | false | Enable disk cache |
| `retention` | string | "24h" | How long to keep uploaded files in cache |
| `max_size` | integer | 5368709120 | Maximum cache size in bytes (5 GB) |
| `path` | string | none | Override default cache location |

**Retention format:**

- `"24h"` - 24 hours
- `"7d"` - 7 days
- `"30m"` - 30 minutes
- `"60s"` - 60 seconds

**Default cache location:**

If `path` is not set, cache is stored at:

```
.<database-name>-walrust/
```

next to the database file.

**Benefits:**

- Crash recovery (resume uploads after restart)
- Decouples encoding from uploads
- Fast local restore (if files still in cache)

**Example:**

```toml
[cache]
enabled = true
retention = "7d"
max_size = 10737418240  # 10 GB
path = "/var/cache/walrust"
```

---

### [retry]

Retry configuration for transient S3 failures.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_retries` | integer | 5 | Number of retry attempts |
| `base_delay_ms` | integer | 100 | Initial backoff delay in milliseconds |
| `max_delay_ms` | integer | 30000 | Maximum backoff cap in milliseconds (30 seconds) |
| `circuit_breaker_enabled` | boolean | true | Enable circuit breaker |
| `circuit_breaker_threshold` | integer | 10 | Failures before circuit opens |
| `circuit_breaker_cooldown_ms` | integer | 60000 | Cooldown before half-open (1 minute) |

**Exponential Backoff:**

Delays between retries grow exponentially:

```
Retry 1: 100ms
Retry 2: 200ms
Retry 3: 400ms
Retry 4: 800ms
Retry 5: 1600ms
```

Capped at `max_delay_ms`.

**Circuit Breaker:**

After `circuit_breaker_threshold` consecutive failures, the circuit "opens" and requests fail fast for `circuit_breaker_cooldown_ms`. Then it enters "half-open" state to test if service recovered.

**Example:**

```toml
[retry]
max_retries = 5
base_delay_ms = 100
max_delay_ms = 30000
circuit_breaker_enabled = true
circuit_breaker_threshold = 10
circuit_breaker_cooldown_ms = 60000
```

---

### [[webhooks]]

Webhook notifications for failure events (array of webhooks).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | (required) | URL to POST notifications to |
| `events` | array | all events | Events to notify on |
| `secret` | string | none | Optional HMAC secret for signature verification |

**Available events:**

- `"sync_failed"` - S3 upload failed
- `"auth_failure"` - S3 authentication failed
- `"corruption_detected"` - Checksum mismatch or integrity error
- `"circuit_breaker_open"` - Circuit breaker opened (too many failures)

**Webhook payload:**

```json
{
  "event": "sync_failed",
  "database": "/data/app.db",
  "timestamp": "2024-01-15T10:30:00Z",
  "error": "Failed to upload to S3: connection timeout",
  "details": {
    "bucket": "s3://my-bucket",
    "file": "00000001-00000010.ltx"
  }
}
```

**HMAC signature:**

If `secret` is provided, walrust sends an `X-Walrust-Signature` header:

```
X-Walrust-Signature: sha256=a1b2c3d4...
```

Verify on receiving end:

```python
import hmac
import hashlib

def verify_signature(payload, signature, secret):
    expected = "sha256=" + hmac.new(
        secret.encode(),
        payload.encode(),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(signature, expected)
```

**Example:**

```toml
[[webhooks]]
url = "https://example.com/walrust-webhook"
events = ["sync_failed", "corruption_detected"]
secret = "your-hmac-secret"

[[webhooks]]
url = "https://backup-webhook.com/notify"
events = ["circuit_breaker_open"]
```

---

### [[databases]]

Per-database configuration (array of databases).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | (required) | Path to database file (supports wildcards: `/data/*.db`) |
| `prefix` | string | filename stem | S3 prefix for this database |
| `snapshot_interval` | integer | (global) | Override snapshot interval for this database |
| `wal_sync_interval` | integer | (global) | Override WAL sync interval |
| `max_changes` | integer | (global) | Override max_changes |
| `max_interval` | integer | (global) | Override max_interval |
| `on_idle` | integer | (global) | Override on_idle |
| `checkpoint_interval` | integer | (global) | Override checkpoint_interval |
| `min_checkpoint_page_count` | integer | (global) | Override min_checkpoint_page_count |
| `wal_truncate_threshold_pages` | integer | (global) | Override wal_truncate_threshold_pages |
| `validation_interval` | integer | (global) | Override validation_interval |
| `retention` | table | (global) | Override retention policy |

**Wildcards:**

Use glob patterns to match multiple databases:

```toml
[[databases]]
path = "/data/*.db"           # All .db files
path = "/data/tenant-*.db"    # Specific pattern
path = "/data/**/*.db"        # Recursive
```

**Per-database overrides:**

```toml
[[databases]]
path = "/data/critical.db"
snapshot_interval = 300              # More frequent (5 min)
validation_interval = 3600           # Validate hourly
retention = { hourly = 48, daily = 14 }

[[databases]]
path = "/data/logs.db"
snapshot_interval = 7200             # Less frequent (2 hours)
checkpoint_interval = 120            # Less frequent checkpoints
```

**Example:**

```toml
# Production database - aggressive backups
[[databases]]
path = "/var/lib/app.db"
prefix = "production"
snapshot_interval = 1800
max_changes = 500
validation_interval = 86400

# Analytics database - relaxed backups
[[databases]]
path = "/var/lib/analytics.db"
prefix = "analytics"
snapshot_interval = 3600
retention = { hourly = 12, daily = 3, weekly = 4, monthly = 3 }

# Tenant databases - dynamic discovery
[[databases]]
path = "/var/lib/tenants/*.db"
prefix = "tenants"
```

---

## Configuration Validation

Walrust validates configuration on load and exits with code 2 if invalid.

**Common validation errors:**

1. **Empty database path:**

```toml
[[databases]]
path = ""  # ERROR: path cannot be empty
```

2. **Zero retention:**

```toml
[retention]
hourly = 0
daily = 0
weekly = 0
monthly = 0  # ERROR: at least one tier must be > 0
```

3. **Invalid bucket:**

```toml
[s3]
bucket = "my bucket with spaces"  # ERROR: bucket cannot contain spaces
```

4. **Unknown fields:**

```toml
[s3]
unknown_field = "value"  # ERROR: unknown field
```

## Environment Variable Overrides

Some settings can be overridden via environment variables:

| Environment Variable | Config Field | Priority |
|---------------------|--------------|----------|
| `AWS_ACCESS_KEY_ID` | (credentials) | Env only |
| `AWS_SECRET_ACCESS_KEY` | (credentials) | Env only |
| `AWS_ENDPOINT_URL_S3` | `[s3].endpoint` | Env > Config > CLI |
| `AWS_REGION` | (region) | Env only |
| `RUST_LOG` | (logging) | Env only |

**Example:**

```bash
export AWS_ACCESS_KEY_ID=tid_xxxxx
export AWS_SECRET_ACCESS_KEY=tsec_xxxxx
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
export RUST_LOG=walrust=info

walrust watch --config walrust.toml
```

## Config File Examples

### Single Database

```toml
[s3]
bucket = "s3://my-backups"
endpoint = "https://fly.storage.tigris.dev"

[sync]
snapshot_interval = 3600
compact_after_snapshot = true

[retention]
hourly = 24
daily = 7

[[databases]]
path = "/data/app.db"
```

### Multi-Database with Overrides

```toml
[s3]
bucket = "s3://backups/prod"
endpoint = "https://fly.storage.tigris.dev"

[sync]
snapshot_interval = 3600
compact_after_snapshot = true

[retention]
hourly = 24
daily = 7
weekly = 12
monthly = 12

# Critical database - snapshot every 5 minutes
[[databases]]
path = "/data/critical.db"
prefix = "critical"
snapshot_interval = 300
validation_interval = 3600

# Regular databases
[[databases]]
path = "/data/app.db"
prefix = "app"

[[databases]]
path = "/data/users.db"
prefix = "users"

# Logs - less frequent backups
[[databases]]
path = "/data/logs.db"
prefix = "logs"
snapshot_interval = 7200
retention = { hourly = 6, daily = 3 }
```

### Multi-Tenant with Wildcards

```toml
[s3]
bucket = "s3://tenant-backups"

[sync]
snapshot_interval = 3600
compact_interval = 86400  # Daily compaction

[retention]
hourly = 12
daily = 7

# All tenant databases
[[databases]]
path = "/var/lib/tenants/*.db"
prefix = "tenants"

# Admin database
[[databases]]
path = "/var/lib/admin.db"
prefix = "admin"
snapshot_interval = 1800  # More frequent
```

### High-Performance Setup

```toml
[s3]
bucket = "s3://backups"
endpoint = "https://fly.storage.tigris.dev"

[sync]
snapshot_interval = 3600
wal_sync_interval = 5  # Higher interval to reduce CPU on high-write workloads
checkpoint_interval = 30
min_checkpoint_page_count = 2000

[cache]
enabled = true
retention = "7d"
max_size = 10737418240

[retry]
max_retries = 5
circuit_breaker_enabled = true

[retention]
hourly = 24
daily = 7

[[databases]]
path = "/data/*.db"
```

## See Also

- [CLI Reference](/guide/cli/) - Command-line options
- [Environment Variables](/config/environment/) - Environment variable reference
- [Deployment](/config/deployment/) - Production deployment examples
- [Troubleshooting](/guide/troubleshooting/) - Configuration errors
