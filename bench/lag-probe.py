#!/usr/bin/env python3
"""Replication lag probe used by bench/compare-litestream.sh.

Every --period seconds, inserts a uniquely-valued sentinel row into the
watched database (through its own long-lived WAL connection, like the drill
write driver), then polls the bucket for a NEW object that actually contains
the sentinel bytes, and records commit-to-visible lag.

Containment check tiers, recorded per sample:
  raw  - sentinel bytes found directly in the object (both walrust and
         litestream incremental LTX objects hold page images that are
         raw-searchable; verified empirically against both tools)
  lz4  - found after LZ4-frame decompression (tried at offsets 0 and 100,
         the LTX header size), for snapshot/compacted objects
  timeout - no containing object appeared within --poll-timeout; the lag
         sample is discarded from percentiles and counted separately

The probe's own S3 traffic uses minio-py, whose User-Agent is excluded from
the request-count analysis (bench/report-compare.py), so probing does not
contaminate the per-tool request counts.

Output TSV columns: epoch_commit, sentinel_id, lag_seconds|timeout, method
"""

import argparse
import os
import sqlite3
import sys
import time
import uuid
from urllib.parse import urlparse

from minio import Minio

try:
    import lz4.frame as lz4frame
except ImportError:  # pragma: no cover - lz4 is best-effort
    lz4frame = None


def make_client(endpoint):
    access = os.environ.get("AWS_ACCESS_KEY_ID") or "minioadmin"
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or "minioadmin"
    parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
    return Minio(
        parsed.netloc or parsed.path,
        access_key=access,
        secret_key=secret,
        secure=parsed.scheme == "https",
    )


def list_keys(client, bucket, prefix):
    return {o.object_name for o in client.list_objects(bucket, prefix=prefix, recursive=True)}


def object_contains(client, bucket, key, needle):
    """Return 'raw', 'lz4', or None."""
    resp = client.get_object(bucket, key)
    try:
        data = resp.read()
    finally:
        resp.close()
        resp.release_conn()
    if needle in data:
        return "raw"
    if lz4frame is not None:
        for off in (0, 100):
            try:
                if needle in lz4frame.decompress(data[off:]):
                    return "lz4"
            except Exception:
                pass
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--duration", type=float, required=True)
    ap.add_argument("--period", type=float, default=10.0)
    ap.add_argument("--poll-interval", type=float, default=0.2)
    ap.add_argument("--poll-timeout", type=float, default=30.0)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    client = make_client(args.endpoint)
    conn = sqlite3.connect(args.db, timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")

    seen = list_keys(client, args.bucket, args.prefix)
    end_at = time.monotonic() + args.duration
    i = 0

    with open(args.out, "w", encoding="utf-8") as out:
        while time.monotonic() < end_at:
            i += 1
            sentinel = f"SENTINEL-{i}-{uuid.uuid4().hex}"
            needle = sentinel.encode()
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO items(value) VALUES(?)", (sentinel,))
            conn.execute("COMMIT")
            committed_wall = time.time()
            committed = time.monotonic()

            lag = None
            method = "timeout"
            deadline = committed + args.poll_timeout
            while time.monotonic() < deadline:
                current = list_keys(client, args.bucket, args.prefix)
                new_keys = sorted(current - seen)
                found = False
                for key in new_keys:
                    tag = object_contains(client, args.bucket, key, needle)
                    seen.add(key)
                    if tag:
                        lag = time.monotonic() - committed
                        method = tag
                        found = True
                        break
                if found:
                    break
                time.sleep(args.poll_interval)

            out.write(
                "%f\t%d\t%s\t%s\n"
                % (committed_wall, i, "timeout" if lag is None else f"{lag:.3f}", method)
            )
            out.flush()

            # Pace to the next period boundary (pacing, not synchronization).
            next_at = committed + args.period
            while time.monotonic() < min(next_at, end_at):
                time.sleep(0.2)

    conn.close()


if __name__ == "__main__":
    sys.exit(main())
