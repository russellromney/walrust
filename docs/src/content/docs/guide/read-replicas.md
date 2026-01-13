---
title: Read Replicas
description: Creating read replicas that poll S3 for updates
---

Walrust can create read replicas that poll S3 for changes. No direct connection to the primary needed - just S3.

## Basic Usage

```bash
walrust replicate s3://my-bucket/mydb --local replica.db --interval 5s
```

This:
1. Checks S3 for the latest LTX files
2. Downloads and applies any new changes
3. Waits 5 seconds
4. Repeats forever

## How It Works

```
┌──────────┐          ┌─────┐          ┌─────────┐
│ Primary  │─────────▶│ S3  │◀─────────│ Replica │
│ (writes) │  upload  │     │  poll    │ (reads) │
└──────────┘          └─────┘          └─────────┘
```

The replica never talks to the primary. It just watches S3 for new LTX files. This means:
- Primary and replica can be in different regions
- Primary going down doesn't affect the replica
- You can have replicas anywhere with S3 access

## Auto-Bootstrap

If the local database doesn't exist, walrust bootstraps it from the latest snapshot:

```
$ walrust replicate s3://my-bucket/mydb --local replica.db --interval 5s
Bootstrapping from snapshot...
Downloaded 00000001-00000001.ltx (2.3 MB)
Applied 1024 pages, TXID 1
Polling for updates...
```

No need to manually restore first.

## Resume Support

Walrust tracks progress in a `.db-replica-state` file next to your database. If you restart, it picks up where it left off:

```
replica.db
replica.db-replica-state  ← tracks TXID
```

## Poll Interval

The `--interval` flag controls how often to check for updates:

```bash
# Near real-time (more S3 requests, more cost)
walrust replicate s3://bucket/db --local replica.db --interval 1s

# Relaxed (fewer requests, more lag)
walrust replicate s3://bucket/db --local replica.db --interval 60s
```

Choose based on your freshness needs and S3 budget.

## Gap Detection

If the replica falls too far behind (missing LTX files), walrust detects the gap and re-bootstraps from the latest snapshot automatically. You don't need to babysit it.

## Use Cases

### Read Scaling

Put replicas in different regions to serve reads locally:

```bash
# US-East replica
walrust replicate s3://bucket/app --local /data/app.db --interval 5s

# EU replica (same S3, different server)
walrust replicate s3://bucket/app --local /data/app.db --interval 5s
```

### Analytics

Keep a replica for analytics queries that won't slow down your primary:

```bash
walrust replicate s3://bucket/app --local /analytics/app.db --interval 60s
```

### Disaster Recovery

A replica in another region is a warm standby. If the primary dies, promote the replica by pointing your app at it.

## Caveats

**Read-only**: The replica database should be opened read-only. Writing to it will break replication.

**Eventual consistency**: There's always some lag. The replica reflects the state as of the last poll, not the current primary state.

**S3 costs**: Frequent polling means frequent S3 LIST and GET requests. At 1-second intervals with many databases, this adds up.
