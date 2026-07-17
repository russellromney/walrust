---
title: Troubleshooting
description: Diagnose native spool, checkpoint, publication, and restore failures
---

## Start with the observable state

Run walrust with `RUST_LOG=info` or `RUST_LOG=debug`, record the exact database,
bucket/prefix, spool root, and process exit code, then inspect these independent
signals:

- `remote_lag` objects, bytes, age, and last error
- `local_spool_high` / `local_spool_full`
- live WAL bytes and shadow bytes
- local HADBP stage/fsync duration
- SQLite checkpoint duration
- remote upload duration

A healthy process can still have remote lag; process liveness is not proof of a
current remote recovery point.

## Watch will not start

Common fail-closed causes are:

- first-ever startup cannot contact storage to prove remote absence;
- a remote native stream exists but the matching local identity/base is absent;
- the configured spool resolves to a different canonical database/destination;
- an unproven or divergent local HADBP orphan exists;
- another watcher owns the local spool;
- `[compaction] enabled = true` was set for CLI native-v1.

Do not delete the spool to suppress an identity or orphan error. It may contain
the only complete local recovery chain. Preserve it and investigate the stated
lineage, sequence, checksum, and destination mismatch.

## `remote_lag` keeps increasing

Application WAL ingestion may continue in default local mode while storage is
unavailable. Check credentials, endpoint, DNS, TLS, bucket permissions, and
clock skew. Walrust retries from the on-disk journal with bounded backoff.

When storage returns, publication revalidates the recorded predecessor. A
`split brain/equivocation` error means the remote head changed incompatibly.
Walrust deliberately retains the local spool and refuses to rebase or overwrite.

## `local_spool_high` or `local_spool_full`

The capacity calculation uses the actual spool filesystem and reserves peak
space for WAL/shadow data, snapshot and payload temporaries, installed payloads,
journal rewrites, and `min_free_space`.

At full capacity walrust keeps the blocker and stops checkpointing. Free space
outside the spool only if it is unrelated data, expand the filesystem, increase
the configured cap when real space exists, or repair remote publication. Never
delete pending `.hadbp`, intent, or journal files by hand.

## WAL remains large

Check whether the spool is full, the checkpoint preflight is continuously busy,
a reader pins old WAL frames, or an emergency TRUNCATE is contended. A partial
PASSIVE checkpoint is expected contention and is retried with the blocker
rearmed. Emergency TRUNCATE is bounded; failure leaves a loud degraded state.

## Restore reports no recovery point

Raw object upload is insufficient. Restore requires `stream.json`, a contiguous
chain of `published/<seq>.json` records, and a snapshot base at/before the
target. Check the requested native sequence against the current retention floor.

For local restore, specify the same spool root used by watch. Restore refuses an
active owner, ambiguous candidate spools, mismatched destination identity, or an
incomplete chain.

## Verify exits with integrity status

Do not retry past or delete the reported object. Preserve the bucket and local
spool for investigation. Verify checks immutable SHA-256, HADBP structure,
sequence/lineage, predecessor and ending checksums, database size, and final
SQLite integrity. An existing remote key with different bytes is a hard
equivocation, not an overwrite opportunity.

## Replica reboots from a snapshot

This is expected when retention removed the replica's next required delta. The
replica selects a currently published snapshot and contiguous suffix. Keep the
replica database read-only; local writes make it divergent.

## Graceful shutdown takes time

Shutdown first admits pending local work, then optionally drains cloud for at
most `spool.shutdown_drain_seconds`. Pending work remains on disk whether the
drain succeeds or the process is SIGKILLed. A forced kill is recovered from the
durable spool on restart.

## Collecting a support bundle

Capture configuration with secrets removed, `walrust explain`, relevant logs,
filesystem free space for both database and custom spool mounts, the exact CLI
version, and `walrust verify` output. Do not include credential values or alter
the spool before copying it for analysis.
