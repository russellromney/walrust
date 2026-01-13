---
title: Why Walrust?
description: Walrust was built for multi-tenant SQLite deployments
---

Walrust exists for multi-tenant SQLite architectures - think a SaaS platform where every customer gets their own database. At scale, that's potentially millions of databases on a single server.

## The Problem

With multi-tenant SQLite, you might have hundreds, thousands, or even millions of databases on a single server. Each one needs backup and replication. Running a Litestream process per database didn't use to scale:

| Databases | Litestream (before) | Walrust |
|-----------|-----------|---------|
| 1 | 33 MB | 12 MB |
| 10 | 330 MB | 12 MB |
| 100 | 3.3 GB | 15 MB |
| 1000 | 33 GB | 50 MB |

## The Solution

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