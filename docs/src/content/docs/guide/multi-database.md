---
title: Multi-Database Sync
description: Backing up many SQLite databases with one process
---

One process watches multiple SQLite databases with minimal memory overhead.

## Command Line

Pass multiple database paths:

```bash
walrust watch \
  /data/users.db \
  /data/orders.db \
  /data/analytics.db \
  -b my-bucket
```

Or use a glob:

```bash
walrust watch /data/tenants/*.db -b my-bucket
```

## Config File

For more control, use a config file:

```toml
[s3]
bucket = "my-bucket"
endpoint = "https://fly.storage.tigris.dev"

[[databases]]
path = "/data/users.db"
prefix = "users"

[[databases]]
path = "/data/orders.db"
prefix = "orders"

[[databases]]
path = "/data/tenants/*.db"
prefix = "tenants"
```

Then:

```bash
walrust watch --config walrust.toml
```

## S3 Layout

Each database gets its own prefix:

```
s3://my-bucket/
├── users/
│   └── native/v1/...
├── orders/
│   └── native/v1/...
└── tenants/
    ├── acme/
    │   └── ...
    └── globex/
        └── ...
```

## Memory Usage

| Databases | Litestream | Walrust | Reduction |
|-----------|------------|---------|-----------|
| 1 | 36 MB | 19 MB | 47% |
| 10 | 55 MB | 19 MB | 65% |
| 100 | 160 MB | 20 MB | 88% |

Walrust shares one S3 client (with connection pooling) across all databases.
Each database has a collision-safe local spool directory and its own durable
journal, lineage, checkpoint blocker, and asynchronous uploader.

## Restoring Individual Databases

Restore any single database without touching the others:

```bash
walrust restore users -o /data/users-restored.db -b my-bucket
walrust restore tenants/acme -o /data/acme-restored.db -b my-bucket
```

## Per-Database Settings

Override settings per database in the config:

```toml
[[databases]]
path = "/data/critical.db"
prefix = "critical"
snapshot_interval = 300  # every 5 minutes

[[databases]]
path = "/data/logs.db"
prefix = "logs"
snapshot_interval = 3600  # every hour is fine
```

## Dynamic Database Discovery

With glob patterns, walrust picks up new databases automatically:

```toml
[[databases]]
path = "/data/tenants/*.db"
prefix = "tenants"
```

New database files matching the pattern are detected and backed up. A restart is required for walrust to discover new files.
