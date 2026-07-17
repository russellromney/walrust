---
title: CLI Reference
description: Native HADBP watch, restore, replicate, verify, list, and prune commands
---

Walrust's CLI uses one versioned native HADBP protocol. It does not expose the
retired one-shot snapshot, legacy cache, independent-task, or LTX compaction
commands.

Run `walrust <command> --help` for the authoritative option list.

## watch

```bash
walrust watch /data/app.db \
  --bucket s3://my-bucket/backups \
  --endpoint https://fly.storage.tigris.dev
```

`watch` holds a real SQLite checkpoint blocker, fsyncs WAL frames into a shadow
copy, admits native HADBP bytes and their matching local journal record, then
performs controlled checkpoints. The uploader publishes those exact immutable
bytes asynchronously.

The default release policy is local:

```bash
walrust watch /data/app.db \
  --bucket s3://my-bucket/backups \
  --checkpoint-release local
```

`local` never waits for PUT, LIST, GET, uploader retries, a wake slot, or remote
publication before releasing the controlled checkpoint. `remote` stages the
same bytes locally first, then also waits for their contiguous publish record:

```bash
walrust watch /data/app.db \
  --bucket s3://my-bucket/backups \
  --checkpoint-release remote
```

Neither mode makes every application commit synchronously cloud-durable.

Important watch options include:

| Option | Meaning |
| --- | --- |
| `--snapshot-interval <seconds>` | Periodic full native snapshot |
| `--wal-sync-interval <seconds>` | WAL/shadow admission polling interval |
| `--max-changes <frames>` | Snapshot after this many frames |
| `--max-interval <seconds>` | Maximum time between snapshots while changing |
| `--on-idle <seconds>` | Snapshot after a changed database becomes idle |
| `--on-startup <bool>` | Request a startup snapshot |
| `--checkpoint-interval <seconds>` | Controlled PASSIVE checkpoint interval |
| `--min-checkpoint-pages <pages>` | Minimum live WAL size for periodic checkpoint |
| `--wal-truncate-threshold <pages>` | Emergency bounded TRUNCATE threshold |
| `--checkpoint-release local\|remote` | Checkpoint release boundary |
| `--spool-dir <path>` | Native spool filesystem root |
| `--spool-max-size <bytes>` | Hard spool capacity |
| `--prune-after-snapshot` | Run native retention after snapshots |
| `--prune-interval <seconds>` | Periodic native retention |

The config file also exposes `spool.warning_size`, `spool.min_free_space`, and
`spool.shutdown_drain_seconds`. When capacity is exhausted walrust retains the
blocker and pending objects and stops checkpointing; it does not discard the
queue or exit merely to free the WAL.

## restore

```bash
walrust restore app \
  --bucket s3://my-bucket/backups \
  --output /data/restored.db
```

Restore discovers only the contiguous published native recovery point. A raw
uploaded object beyond a publication gap is invisible. To restore through an
exact native sequence:

```bash
walrust restore app \
  --bucket s3://my-bucket/backups \
  --output /data/restored.db \
  --point-in-time 42
```

Timestamp PITR is not implemented. A complete local spool can be used while S3
is unavailable:

```bash
walrust restore app \
  --bucket s3://my-bucket/backups \
  --output /data/restored.db \
  --spool-dir /var/lib/walrust/spool
```

Local restore refuses an active watcher owner, an ambiguous set of spools, a
destination/lineage mismatch, or an incomplete chain.

## verify

```bash
walrust verify app --bucket s3://my-bucket/backups
```

Verify checks the descriptor, contiguous publish-record chain, immutable object
identity and SHA-256, HADBP decoding, predecessor/ending checksums, database
size, and final SQLite integrity. Integrity failures exit with code 5.

## replicate

```bash
walrust replicate s3://my-bucket/backups/app \
  --local /data/app-replica.db \
  --interval 5s
```

The replica bootstraps from a published snapshot and applies only contiguous
native deltas. Open the replica read-only. Writing to it creates a divergent
database and is unsupported.

## prune

Prune is a dry run unless `--force` is present:

```bash
walrust prune app --bucket s3://my-bucket/backups
walrust prune app --bucket s3://my-bucket/backups --force
```

The GFS knobs are `--hourly`, `--daily`, `--weekly`, and `--monthly`. Forced
prune first publishes a verified native retention floor, preserves the latest
restorable snapshot/suffix, and will not remove a base needed by unpublished
local descendants.

## list

```bash
walrust list --bucket s3://my-bucket/backups
```

Only databases with a valid native-v1 descriptor and visible recovery state
are reported.

## explain and pragma

```bash
walrust explain --config /etc/walrust.toml
walrust pragma
```

`explain` resolves configuration without starting replication. `pragma` prints
recommended SQLite settings.

## Exit behavior

Walrust fails loudly on malformed configuration, missing recovery points,
split-brain/equivocation, checksum failures, and identity mismatches. Temporary
cloud failures after a verified base are reported as `remote_lag` while the
local watcher continues. Do not treat a running process alone as proof that the
remote is current; monitor remote lag objects, bytes, and age.
