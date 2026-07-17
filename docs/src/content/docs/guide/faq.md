---
title: FAQ
description: Native HADBP replication, durability, restore, and operations
---

## What does walrust replicate?

Walrust watches a SQLite WAL, fsyncs validated frames into a shadow copy,
encodes native HADBP snapshots/deltas into a durable local spool, and publishes
those exact bytes to S3-compatible storage.

## Is this Litestream-compatible?

No. Walrust does not read or write Litestream LTX. Use a separate bucket/prefix
and start walrust with a fresh native snapshot.

## Does an S3 outage stop application checkpoints?

Not with the default `checkpoint_release = "local"`. A controlled checkpoint
may proceed after the matching local HADBP payload and journal/cursor record are
fsynced. Cloud failures increase `remote_lag`; they do not enter the checkpoint
path while the spool remains healthy.

`checkpoint_release = "remote"` additionally waits for contiguous publication.
It still stages locally first and does not make every SQLite commit synchronous
to S3.

## What happens when the spool fills?

Walrust emits `local_spool_high` before the hard limit. At
`local_spool_full`, it retains pending objects and the SQLite checkpoint blocker
and stops checkpointing. It never evicts an unuploaded object or the only local
snapshot base. Increase capacity/free space or restore cloud publication.

## Can walrust start for the first time while S3 is down?

No. First startup must verify remote absence or an existing matching native
descriptor. Offline continuation is allowed only from a matching local spool
with a locally recorded published snapshot base.

## Is offline multi-host failover safe?

Walrust does not claim that. Publication uses immutable conditional writes,
lineage identity, and a chained visibility cursor, so a reconnecting divergent
writer is rejected as split brain. There is no renewable distributed lease that
would authorize two hosts to write offline concurrently.

## How do I restore latest or PITR?

```bash
walrust restore app --bucket s3://bucket/backups --output restored.db
walrust restore app --bucket s3://bucket/backups --output restored.db --point-in-time 42
```

PITR targets native sequence numbers, not timestamps. Restore uses only the
contiguous published recovery point and verifies the complete selected chain.

## Can I restore from the local spool without S3?

Yes, when it contains a complete matching chain and the watcher does not own it:

```bash
walrust restore app --bucket s3://bucket/backups \
  --output restored.db --spool-dir /var/lib/walrust/spool
```

Ambiguous/mismatched spools and active owners are rejected.

## How do I verify a backup?

```bash
walrust verify app --bucket s3://bucket/backups
```

This verifies descriptor identity, publication continuity, immutable payload
hashes, HADBP decoding and chain checksums, and final SQLite integrity.

## How is retention applied?

```bash
walrust prune app --bucket s3://bucket/backups        # dry run
walrust prune app --bucket s3://bucket/backups --force
```

Forced pruning publishes a native retention floor before deleting old objects.
Watch can run the same GFS policy after snapshots or on an interval.

## Can an application issue `wal_checkpoint(TRUNCATE)`?

The watcher holds a real checkpoint blocker so an application checkpoint cannot
discard frames walrust has not admitted. Walrust's own PASSIVE contention is
nonfatal; emergency TRUNCATE is bounded and observable.

## What SQLite settings are required?

The database must use WAL mode. Run `walrust pragma` for recommended settings.
Keep the database, WAL, and SHM together on a filesystem with normal SQLite
locking semantics. Network filesystems with unreliable locking are unsafe.

## How do read replicas work?

```bash
walrust replicate s3://bucket/backups/app --local replica.db --interval 5s
```

Replicas bootstrap from a published native snapshot and apply contiguous
deltas. Treat the replica as read-only.

## Is there a one-shot snapshot or Python API?

No. One-shot snapshot paths bypass the mandatory local spool/checkpoint state
machine and are not part of the native-only CLI. Embed the native
`walrust-core` Replicator protocol when building a library integration; its
wire layout is separate from CLI native-v1.
