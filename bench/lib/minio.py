"""
MinIO container management for benchmark reproducibility.

Provides a MinioManager class to start/stop MinIO Docker containers
for local and CI benchmark runs.
"""

import os
import subprocess
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class MinioConfig:
    """MinIO configuration."""
    container_name: str = "walrust-bench-minio"
    image: str = "minio/minio:latest"
    api_port: int = 9000
    console_port: int = 9001
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "walrust-bench"


class MinioManager:
    """
    Manages MinIO Docker container lifecycle.

    Usage:
        with MinioManager() as minio:
            print(f"Endpoint: {minio.endpoint}")
            # Run benchmarks...

    Or manually:
        minio = MinioManager()
        minio.start()
        # ... use minio ...
        minio.stop()
    """

    def __init__(self, config: Optional[MinioConfig] = None):
        self.config = config or MinioConfig()
        self._started_by_us = False

    @property
    def endpoint(self) -> str:
        """Get the MinIO endpoint URL."""
        return f"http://localhost:{self.config.api_port}"

    @property
    def bucket_url(self) -> str:
        """Get the full S3 bucket URL."""
        return f"s3://{self.config.bucket}"

    def is_running(self) -> bool:
        """Check if MinIO container is running."""
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", self.config.container_name],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0 and result.stdout.strip() == "true"

    def is_healthy(self) -> bool:
        """Check if MinIO is responding to health checks."""
        if not self.is_running():
            return False

        try:
            import urllib.request
            req = urllib.request.Request(f"{self.endpoint}/minio/health/live")
            with urllib.request.urlopen(req, timeout=2) as resp:
                return resp.status == 200
        except Exception:
            return False

    def start(self, wait: bool = True, timeout: int = 30) -> None:
        """
        Start MinIO container if not already running.

        Args:
            wait: Wait for MinIO to be healthy before returning
            timeout: Timeout in seconds for health check
        """
        if self.is_running():
            print(f"MinIO container '{self.config.container_name}' already running")
            self._ensure_bucket()
            return

        # Remove any stopped container with same name
        subprocess.run(
            ["docker", "rm", "-f", self.config.container_name],
            capture_output=True,
        )

        # Start container
        cmd = [
            "docker", "run", "-d",
            "--name", self.config.container_name,
            "-p", f"{self.config.api_port}:9000",
            "-p", f"{self.config.console_port}:9001",
            "-e", f"MINIO_ROOT_USER={self.config.access_key}",
            "-e", f"MINIO_ROOT_PASSWORD={self.config.secret_key}",
            self.config.image,
            "server", "/data", "--console-address", ":9001",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to start MinIO: {result.stderr}")

        self._started_by_us = True
        print(f"Started MinIO container '{self.config.container_name}'")

        if wait:
            self._wait_for_healthy(timeout)
            self._ensure_bucket()

    def _wait_for_healthy(self, timeout: int) -> None:
        """Wait for MinIO to be healthy."""
        start = time.time()
        while time.time() - start < timeout:
            if self.is_healthy():
                print("MinIO is healthy")
                return
            time.sleep(0.5)

        raise RuntimeError(f"MinIO did not become healthy within {timeout}s")

    def _ensure_bucket(self) -> None:
        """Create the benchmark bucket if it doesn't exist."""
        try:
            import boto3
            from botocore.exceptions import ClientError

            s3 = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.config.access_key,
                aws_secret_access_key=self.config.secret_key,
            )

            try:
                s3.head_bucket(Bucket=self.config.bucket)
            except ClientError:
                s3.create_bucket(Bucket=self.config.bucket)
                print(f"Created bucket '{self.config.bucket}'")
        except ImportError:
            # Fall back to mc CLI if boto3 not available
            subprocess.run([
                "docker", "exec", self.config.container_name,
                "mc", "alias", "set", "local",
                "http://localhost:9000",
                self.config.access_key,
                self.config.secret_key,
            ], capture_output=True)

            subprocess.run([
                "docker", "exec", self.config.container_name,
                "mc", "mb", f"local/{self.config.bucket}", "--ignore-existing",
            ], capture_output=True)

    def stop(self, force: bool = False) -> None:
        """
        Stop MinIO container.

        Args:
            force: Stop even if we didn't start it
        """
        if not force and not self._started_by_us:
            print("MinIO was not started by us, not stopping")
            return

        if not self.is_running():
            return

        subprocess.run(
            ["docker", "stop", self.config.container_name],
            capture_output=True,
        )
        subprocess.run(
            ["docker", "rm", self.config.container_name],
            capture_output=True,
        )
        print(f"Stopped MinIO container '{self.config.container_name}'")

    def get_env(self) -> dict:
        """Get environment variables for using MinIO."""
        return {
            "AWS_ACCESS_KEY_ID": self.config.access_key,
            "AWS_SECRET_ACCESS_KEY": self.config.secret_key,
            "AWS_ENDPOINT_URL_S3": self.endpoint,
            "WALSYNC_TEST_BUCKET": self.bucket_url,
        }

    def __enter__(self) -> "MinioManager":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


def use_minio_env() -> bool:
    """Check if we should use MinIO based on environment."""
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3", "")
    return "localhost" in endpoint or "127.0.0.1" in endpoint or "minio" in endpoint.lower()


def get_minio_or_env() -> tuple[str, str, dict]:
    """
    Get bucket, endpoint, and env from MinIO or environment.

    Returns:
        (bucket, endpoint, env_updates)
    """
    if use_minio_env():
        config = MinioConfig()
        return (
            f"s3://{config.bucket}",
            f"http://localhost:{config.api_port}",
            {
                "AWS_ACCESS_KEY_ID": config.access_key,
                "AWS_SECRET_ACCESS_KEY": config.secret_key,
            },
        )

    return (
        os.environ.get("WALSYNC_TEST_BUCKET", "s3://walrust-bench"),
        os.environ.get("AWS_ENDPOINT_URL_S3"),
        {},
    )
