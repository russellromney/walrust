---
title: Multi-Database Sync
description: Backing up many SQLite databases with one process
---

This is the whole point of walrust. One process, many databases, minimal memory.

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
│   ├── 00000001-00000001.ltx
│   └── manifest.json
├── orders/
│   ├── 00000001-00000001.ltx
│   └── manifest.json
└── tenants/
    ├── acme/
    │   └── ...
    └── globex/
        └── ...
```

## Memory Usage

Here's why you're here:

| Databases | Litestream | Walrust |
|-----------|-----------|---------|
| 1 | 33 MB | 12 MB |
| 10 | 330 MB | 12 MB |
| 100 | 3.3 GB | ~15 MB |

Walrust shares one S3 client and one file watcher across all databases. Each database adds ~500KB of state tracking. It's basically free.

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

New tenant shows up? New database file appears? Walrust starts backing it up. No restart needed.

(Okay, you might need to restart. But the config is ready for it.)
