---
title: Why Walrust?
description: Walrust was built for multi-tenant SQLite deployments
---

Walrust exists for multi-tenant SQLite architectures - think a SaaS platform where every customer gets their own database. At scale, that's potentially millions of databases on a single server.

## The Problem

With multi-tenant SQLite, you might have hundreds, thousands, or even millions of databases on a single server. Each one needs backup and replication. Walrust is optimized for lower memory usage in multi-database scenarios:

### Idle Databases

| Databases | Litestream | Walrust | Savings |
|-----------|------------|---------|---------|
| 5 | 40 MB | 13 MB | 27 MB |
| 10 | 50 MB | 14 MB | 36 MB |
| 20 | 62 MB | 17 MB | 45 MB |
| 50 | 71 MB | 17 MB | 54 MB |

### Under Write Load

The difference grows dramatically under active writes:

| DBs | Writes/s/db | Litestream | Walrust | Savings |
|-----|-------------|------------|---------|---------|
| 20 | 10 | 80 MB | 19 MB | 61 MB |
| 20 | 100 | 103 MB | 24 MB | 78 MB |
| 50 | 10 | 266 MB | 21 MB | **245 MB** |
| 50 | 100 | 285 MB | 45 MB | **240 MB** |

Both tools run as a single process watching multiple databases. Walrust scales flat (17-45 MB regardless of DB count) while litestream scales linearly. At 50 databases with active writes, walrust uses **6x less memory**.

## Usage

One walrust process watches all databases:

```bash
walrust watch \
  /var/lib/data/tenant-*.db \
  -b s3://backups
```

Or with a config file:

```toml
[s3]
bucket = "backups"
endpoint = "https://fly.storage.tigris.dev"

[[databases]]
path = "/var/lib/data/*.db"
prefix = "tenants"
```

## Litestream

[Litestream](https://litestream.io) is the inspiration for walrust. It's battle-tested and you should probably use it for most cases. Walrust now uses the same [LTX file format](https://github.com/superfly/ltx) (thanks, it's better than raw WAL pages).

Use walrust when you're running many databases on resource-constrained servers and need the lowest possible memory overhead.

Or just if you're curious. Or if you like to live on the edge. 