#!/usr/bin/env python3
"""
Clean up benchmark data from S3.

Usage:
    uv run bench/cleanup.py           # Clean benchmark prefixes
    uv run bench/cleanup.py --all     # Delete everything
    uv run bench/cleanup.py --prefix db_  # Delete specific prefix
"""

import argparse
import sys

# Add bench/lib to path
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from lib.config import get_config

import boto3


def cleanup_s3(bucket: str, endpoint: str | None, prefix: str | None = None) -> int:
    """Delete objects from S3. Returns count deleted."""
    bucket_name = bucket.replace("s3://", "").split("/")[0]

    kwargs = {}
    if endpoint:
        kwargs["endpoint_url"] = endpoint

    s3 = boto3.client("s3", **kwargs)
    deleted = 0

    paginator = s3.get_paginator("list_objects_v2")
    paginate_kwargs = {"Bucket": bucket_name}
    if prefix:
        paginate_kwargs["Prefix"] = prefix

    for page in paginator.paginate(**paginate_kwargs):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            if objects:
                s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
                deleted += len(objects)
                print(f"  Deleted {len(objects)} objects...")

    return deleted


def main():
    # Load config from .env
    config = get_config()

    parser = argparse.ArgumentParser(description="Clean up benchmark data from S3")
    parser.add_argument("--bucket", default=config.bucket_url, help="S3 bucket URL")
    parser.add_argument("--endpoint", default=config.endpoint, help="S3 endpoint URL")
    parser.add_argument("--all", action="store_true", help="Delete ALL objects in bucket")
    parser.add_argument("--prefix", help="Only delete objects with this prefix")

    args = parser.parse_args()

    if args.all:
        print(f"Deleting ALL objects from {args.bucket}...")
        prefix = None
    elif args.prefix:
        print(f"Deleting objects with prefix '{args.prefix}' from {args.bucket}...")
        prefix = args.prefix
    else:
        # Default: clean up common benchmark prefixes
        print(f"Cleaning up benchmark data from {args.bucket}...")
        prefixes = ["db_", "test_", "stress_test", "sync_test", "recovery_test"]
        total = 0
        for p in prefixes:
            deleted = cleanup_s3(args.bucket, args.endpoint, p)
            if deleted:
                print(f"  {p}*: {deleted} objects")
            total += deleted
        print(f"\nTotal deleted: {total} objects")
        return

    deleted = cleanup_s3(args.bucket, args.endpoint, prefix)
    print(f"\nDeleted {deleted} objects")


if __name__ == "__main__":
    main()
