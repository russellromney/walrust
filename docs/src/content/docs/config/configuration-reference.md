---
title: Configuration Reference
description: Native-only walrust.toml configuration
---

Walrust loads the path passed with `--config`; otherwise it checks
`./walrust.toml`. Unknown fields are rejected instead of being silently ignored.

## Complete example

```toml
[s3]
bucket = "s3://my-backups/production"
endpoint = "https://fly.storage.tigris.dev"

[sync]
snapshot_interval = 3600
wal_sync_interval = 1
max_changes = 1000
max_interval = 300
on_idle = 60
on_startup = true
prune_after_snapshot = true
prune_interval = 0
checkpoint_interval = 60
min_checkpoint_page_count = 1000
wal_truncate_threshold_pages = 121359
validation_interval = 86400
checkpoint_release = "local"

[spool]
path = "/var/lib/walrust/spool"
warning_size = 4294967296
max_size = 5368709120
min_free_space = 1073741824
shutdown_drain_seconds = 10

[retention]
hourly = 24
daily = 7
weekly = 12
monthly = 12

[retry]
max_retries = 5
base_delay_ms = 100
max_delay_ms = 30000
circuit_breaker_enabled = true
circuit_breaker_threshold = 10
circuit_breaker_cooldown_ms = 60000

[[webhooks]]
url = "https://example.com/walrust"
events = ["sync_failed", "auth_failure", "corruption_detected", "circuit_breaker_open", "wal_size_exceeded"]
secret = "replace-me"

[[databases]]
path = "/data/app.db"
prefix = "app"

[[databases]]
path = "/data/tenants/*.db"
```

There is no `[cache]` compatibility queue. The native spool is mandatory for
CLI watch and is the only local upload queue.

## Top-level fields

| Field | Default | Meaning |
| --- | --- | --- |
| `allow_empty_globs` | `false` | Permit a database glob that currently matches nothing |

At least one resolved database is required for `watch`.

## `[s3]`

| Field | Default | Meaning |
| --- | --- | --- |
| `bucket` | none | Bucket, optionally with `s3://` and a prefix |
| `endpoint` | environment/provider default | Tigris, MinIO, R2, or other S3-compatible endpoint |

Credentials use the normal AWS environment/provider chain. CLI `--bucket` and
`--endpoint` override configuration where supported.

## `[sync]`

| Field | Default | Meaning |
| --- | --- | --- |
| `snapshot_interval` | `3600` | Periodic full native snapshot, seconds |
| `wal_sync_interval` | `1` | WAL/shadow sync interval, seconds |
| `max_changes` | `0` | Snapshot after N WAL frames; zero disables |
| `max_interval` | `0` | Maximum seconds between snapshots while changing; zero disables |
| `on_idle` | `0` | Snapshot after N idle seconds following changes; zero disables |
| `on_startup` | `true` | Request a startup snapshot |
| `prune_after_snapshot` | `false` | Run native GFS retention after a snapshot |
| `prune_interval` | `0` | Periodic native retention, seconds; zero disables |
| `checkpoint_interval` | `60` | Controlled PASSIVE checkpoint interval |
| `min_checkpoint_page_count` | `1000` | Minimum live WAL pages for periodic checkpoint |
| `wal_truncate_threshold_pages` | `121359` | Emergency bounded TRUNCATE threshold; zero disables |
| `validation_interval` | `0` | Published native-chain validation interval; zero disables |
| `checkpoint_release` | `"local"` | `"local"` or `"remote"` |

`checkpoint_release = "local"` releases the controlled checkpoint only after
the matching HADBP payload and local cursor/journal state are fsynced. It does
not wait for cloud I/O. `"remote"` stages locally first and then also waits for
contiguous remote publication. Neither setting makes every SQLite commit
synchronously cloud-durable.

## `[spool]`

| Field | Default | Meaning |
| --- | --- | --- |
| `path` | database-adjacent `.walrust-spool` | Root for collision-safe per-stream directories |
| `warning_size` | 4 GiB | Warning watermark |
| `max_size` | 5 GiB | Hard capacity; pending objects are never evicted |
| `min_free_space` | 1 GiB | Reserve on the actual spool filesystem |
| `shutdown_drain_seconds` | `10` | Optional bounded cloud drain after local shutdown admission |

Capacity includes installed and temporary HADBP bytes, journal rewrite peaks,
snapshot preparation, shadow/WAL pressure, and the filesystem reserve. At the
hard boundary walrust retains the SQLite blocker and enters a loud
non-checkpointing state.

## `[retention]`

| Field | Default | Meaning |
| --- | --- | --- |
| `hourly` | `24` | Hourly snapshots retained |
| `daily` | `7` | Daily snapshots retained |
| `weekly` | `12` | Weekly snapshots retained |
| `monthly` | `12` | Monthly snapshots retained |

At least one tier must be nonzero. Retention publishes a verified native floor
before deleting older remote objects.

## `[retry]`

| Field | Default | Meaning |
| --- | --- | --- |
| `max_retries` | `5` | Attempts for a retrying remote operation |
| `base_delay_ms` | `100` | Initial backoff |
| `max_delay_ms` | `30000` | Backoff cap |
| `circuit_breaker_enabled` | `true` | Enable repeated-failure protection |
| `circuit_breaker_threshold` | `10` | Failures before opening |
| `circuit_breaker_cooldown_ms` | `60000` | Open-state cooldown |

Uploader retries are reconstructed from the durable spool. A full or dead wake
path cannot block local WAL ingestion.

## `[[webhooks]]`

Each entry accepts `url`, an `events` array, and optional HMAC `secret`. Webhook
delivery is diagnostic; it is not part of the durability state machine.

## `[[databases]]`

| Field | Default | Meaning |
| --- | --- | --- |
| `path` | required | SQLite database path or glob |
| `prefix` | file stem | Remote database identity |
| `snapshot_interval` | global | Per-database override |
| `wal_sync_interval` | global | Per-database override |
| `max_changes` | global | Per-database override |
| `max_interval` | global | Per-database override |
| `on_idle` | global | Per-database override |
| `checkpoint_interval` | global | Per-database override |
| `min_checkpoint_page_count` | global | Per-database override |
| `wal_truncate_threshold_pages` | global | Per-database override |
| `validation_interval` | global | Per-database override |
| `retention` | global | Nested per-database retention override |

Custom spool roots remain isolated across databases by a digest of canonical
path and destination identity; the persisted full identity is still compared
on every open.

## `[compaction]`

The retained compaction engine belongs to the separate public library/owned
native protocol. CLI native-v1 watch rejects `enabled = true`; it uses snapshot
boundaries plus native retention-floor pruning instead. The accepted fields are
`enabled`, `keep_fine_window`, `l1_batch`, and `l2_batch`, with `enabled = false`
by default.

Use `walrust explain --config walrust.toml` to inspect the resolved databases
and effective policy before starting a watcher.
