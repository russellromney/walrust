#!/usr/bin/env python3
"""
Replication Verifier for Walrust Benchmark Framework

Verifies that all committed writes made it to S3 and measures replication lag.
"""

import os
import sqlite3
import subprocess
import tempfile
import statistics
from pathlib import Path
from typing import List, Tuple, Dict, Optional
import boto3
from botocore.exceptions import ClientError


class ReplicationVerifier:
    """Verify replication completeness and measure sync latency.

    Restores database from S3 and compares against expected writes to detect
    data loss and measure replication lag.
    """

    def __init__(self, s3_endpoint: Optional[str] = None):
        """Initialize replication verifier.

        Args:
            s3_endpoint: S3 endpoint URL (default: from AWS_ENDPOINT_URL_S3 env var)
        """
        self.s3_endpoint = s3_endpoint or os.getenv('AWS_ENDPOINT_URL_S3')

    def verify(
        self,
        tool: str,
        db_name: str,
        bucket: str,
        expected_writes: List[Tuple[str, float]],
        walrust_binary: Optional[str] = None,
        litestream_binary: Optional[str] = None
    ) -> Dict:
        """Verify replication for a database.

        Args:
            tool: "walrust" or "litestream"
            db_name: Database name (S3 prefix)
            bucket: S3 bucket name
            expected_writes: List of (write_id, commit_timestamp) tuples
            walrust_binary: Path to walrust binary (default: auto-detect)
            litestream_binary: Path to litestream binary (default: "litestream")

        Returns:
            Dict with replication metrics:
                - total_writes: Total writes performed
                - replicated_writes: Writes found in restored DB
                - missing_writes: Writes NOT found (data loss)
                - missing_write_ids: List of missing write IDs
                - data_loss: Boolean indicating any data loss
                - sync_latency_p50_ms: Median sync latency
                - sync_latency_p95_ms: 95th percentile
                - sync_latency_p99_ms: 99th percentile
                - sync_latency_max_ms: Maximum latency
        """
        # 1. Restore database from S3
        restore_path = self._restore_from_s3(
            tool=tool,
            db_name=db_name,
            bucket=bucket,
            walrust_binary=walrust_binary,
            litestream_binary=litestream_binary
        )

        try:
            # 2. Query restored database
            restored_writes = self._query_restored_db(restore_path)

            # 3. Get S3 metadata for LTX files
            upload_timestamps = self._get_s3_upload_timestamps(bucket, db_name)

            # 4. Compare and compute metrics
            return self._compute_metrics(
                expected_writes,
                restored_writes,
                upload_timestamps
            )

        finally:
            # Cleanup restored database
            if restore_path.exists():
                restore_path.unlink()

    def _restore_from_s3(
        self,
        tool: str,
        db_name: str,
        bucket: str,
        walrust_binary: Optional[str],
        litestream_binary: Optional[str]
    ) -> Path:
        """Restore database from S3 using walrust or litestream.

        Returns:
            Path to restored database file
        """
        restore_path = Path(tempfile.mktemp(suffix='.db', prefix=f'{tool}_restore_'))

        if tool == "walrust":
            # Find walrust binary
            if not walrust_binary:
                walrust_binary = self._find_binary("walrust", ["target/release/walrust"])

            # walrust restore <db_name> -o <output> -b <bucket> --endpoint <endpoint>
            cmd = [
                walrust_binary,
                "restore",
                db_name,
                "-o", str(restore_path),
                "-b", bucket
            ]

            if self.s3_endpoint:
                cmd.extend(["--endpoint", self.s3_endpoint])

        elif tool == "litestream":
            # Find litestream binary
            if not litestream_binary:
                litestream_binary = self._find_binary("litestream")

            # litestream restore -o <output> s3://<bucket>/<db_name>
            # Remove s3:// prefix if present
            bucket_name = bucket.replace('s3://', '')
            s3_url = f"s3://{bucket_name}/{db_name}"
            cmd = [
                litestream_binary,
                "restore",
                "-o", str(restore_path),
                s3_url
            ]

            # Litestream uses AWS_ENDPOINT_URL_S3 env var automatically

        else:
            raise ValueError(f"Unknown tool: {tool}")

        # Run restore command
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=60
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"{tool} restore failed:\n"
                f"Command: {' '.join(cmd)}\n"
                f"stdout: {e.stdout}\n"
                f"stderr: {e.stderr}"
            ) from e
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"{tool} restore timed out after 60s")

        if not restore_path.exists():
            raise RuntimeError(
                f"{tool} restore completed but output file not found: {restore_path}"
            )

        return restore_path

    def _find_binary(self, name: str, local_paths: Optional[List[str]] = None) -> str:
        """Find binary in local paths or PATH."""
        # Check local paths first
        if local_paths:
            for path in local_paths:
                p = Path(path)
                if p.exists():
                    return str(p.absolute())

        # Check PATH
        try:
            result = subprocess.run(
                ["which", name],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            raise FileNotFoundError(
                f"{name} binary not found in local paths or PATH"
            )

    def _query_restored_db(self, db_path: Path) -> Dict[str, float]:
        """Query restored database for benchmark data.

        Returns:
            Dict mapping write_id to created_at timestamp
        """
        conn = sqlite3.connect(str(db_path))

        try:
            cursor = conn.execute(
                "SELECT id, created_at FROM benchmark_data ORDER BY created_at"
            )
            return {row[0]: row[1] for row in cursor.fetchall()}
        except sqlite3.OperationalError as e:
            # Table might not exist if restore failed
            print(f"Warning: Failed to query restored database: {e}")
            return {}
        finally:
            conn.close()

    def _get_s3_upload_timestamps(self, bucket: str, db_name: str) -> Dict[str, float]:
        """Get upload timestamps for LTX files in S3.

        Returns:
            Dict mapping S3 key to upload timestamp (seconds since epoch)
        """
        # Create S3 client
        s3_config = {}
        if self.s3_endpoint:
            s3_config['endpoint_url'] = self.s3_endpoint

        s3_client = boto3.client('s3', **s3_config)

        # List objects with prefix
        upload_timestamps = {}

        try:
            # Remove s3:// prefix if present
            bucket_name = bucket.replace('s3://', '')

            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=f'{db_name}/'
            )

            # Collect upload timestamps for LTX files
            for obj in response.get('Contents', []):
                key = obj['Key']

                # Only track WAL frame files (have dash in filename)
                # Snapshots are: 00000001-00000001.ltx
                # WAL frames are: 00000002-00000100.ltx
                if '-' in key and key.endswith('.ltx'):
                    upload_timestamps[key] = obj['LastModified'].timestamp()

        except ClientError as e:
            print(f"Warning: Failed to list S3 objects: {e}")

        return upload_timestamps

    def _compute_metrics(
        self,
        expected_writes: List[Tuple[str, float]],
        restored_writes: Dict[str, float],
        upload_timestamps: Dict[str, float]
    ) -> Dict:
        """Compute replication metrics.

        Args:
            expected_writes: List of (write_id, commit_timestamp)
            restored_writes: Dict of write_id -> created_at from restored DB
            upload_timestamps: Dict of S3 key -> upload timestamp

        Returns:
            Dict with replication metrics
        """
        replicated = []
        missing = []
        sync_latencies = []

        # Use latest upload timestamp as approximation for sync time
        # (In reality, we'd need to map write_id -> specific LTX file)
        latest_upload_ts = max(upload_timestamps.values()) if upload_timestamps else None

        for write_id, commit_ts in expected_writes:
            if write_id in restored_writes:
                replicated.append(write_id)

                # Compute sync latency (approximation using latest upload)
                if latest_upload_ts:
                    latency_sec = latest_upload_ts - commit_ts
                    # Only count positive latencies (can be negative due to approximation)
                    if latency_sec > 0:
                        sync_latencies.append(latency_sec * 1000)  # Convert to ms
            else:
                missing.append(write_id)

        # Compute percentiles
        if sync_latencies:
            sync_latencies.sort()
            p50 = sync_latencies[int(len(sync_latencies) * 0.50)]
            p95 = sync_latencies[int(len(sync_latencies) * 0.95)]
            p99 = sync_latencies[int(len(sync_latencies) * 0.99)]
            max_lat = max(sync_latencies)
        else:
            p50 = p95 = p99 = max_lat = 0.0

        return {
            'total_writes': len(expected_writes),
            'replicated_writes': len(replicated),
            'missing_writes': len(missing),
            'missing_write_ids': missing[:10],  # First 10 for debugging
            'data_loss': len(missing) > 0,
            'sync_latency_p50_ms': p50,
            'sync_latency_p95_ms': p95,
            'sync_latency_p99_ms': p99,
            'sync_latency_max_ms': max_lat,
        }


if __name__ == "__main__":
    # Simple test (requires S3 credentials and actual data)
    print("ReplicationVerifier implementation complete")
    print("\nTo test:")
    print("1. Run a benchmark that writes to SQLite and syncs to S3")
    print("2. Get the expected_writes list from DatabaseWriter")
    print("3. Call verify() to check replication")
    print("\nExample:")
    print("""
    verifier = ReplicationVerifier()
    metrics = verifier.verify(
        tool="walrust",
        db_name="bench_db_0",
        bucket="s3://walrust-bench",
        expected_writes=[(id1, ts1), (id2, ts2), ...]
    )
    print(f"Data loss: {metrics['data_loss']}")
    print(f"Replicated: {metrics['replicated_writes']}/{metrics['total_writes']}")
    """)
