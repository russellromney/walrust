#!/usr/bin/env python3
"""
Tool Runners for Walrust Benchmark Framework

Manages walrust and litestream processes for benchmarking.
"""

import os
import time
import subprocess
import tempfile
import yaml
from pathlib import Path
from typing import List, Optional


class WalrustRunner:
    """Manage walrust process for benchmarking.

    Starts walrust in multi-database watch mode and manages the process lifecycle.
    """

    def __init__(self, walrust_binary: Optional[str] = None):
        """Initialize walrust runner.

        Args:
            walrust_binary: Path to walrust binary (default: auto-detect)
        """
        self.walrust_binary = walrust_binary or self._find_walrust()
        self.proc: Optional[subprocess.Popen] = None
        self.databases: List[Path] = []

    def _find_walrust(self) -> str:
        """Find walrust binary (check target/release, then PATH)."""
        # Check local build first
        local_path = Path("target/release/walrust")
        if local_path.exists():
            return str(local_path.absolute())

        # Check PATH
        try:
            result = subprocess.run(
                ["which", "walrust"],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            raise FileNotFoundError(
                "walrust binary not found. Build with 'cargo build --release' "
                "or install to PATH"
            )

    def start(
        self,
        databases: List[Path],
        bucket: str,
        endpoint: str,
        extra_args: Optional[List[str]] = None
    ) -> int:
        """Start walrust process.

        Args:
            databases: List of database paths to watch
            bucket: S3 bucket (e.g., "s3://walrust-bench")
            endpoint: S3 endpoint URL
            extra_args: Additional CLI arguments

        Returns:
            Process PID

        Raises:
            RuntimeError: If walrust fails to start
        """
        if self.proc:
            raise RuntimeError("Walrust process already running")

        self.databases = databases

        # Build command
        cmd = [
            self.walrust_binary,
            "watch",
            "--bucket", bucket,
            "--endpoint", endpoint,
        ]

        # Add databases
        for db in databases:
            cmd.append(str(db))

        # Add extra arguments
        if extra_args:
            cmd.extend(extra_args)

        # Start process
        # Debug: print command and check env
        print(f"  Command: {' '.join(cmd)}")
        print(f"  AWS_ACCESS_KEY_ID: {os.environ.get('AWS_ACCESS_KEY_ID', 'NOT SET')[:20]}...")

        try:
            # Don't capture output to avoid blocking on full pipe buffers
            self.proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # Wait for startup
            time.sleep(2)

            # Check if process is still alive
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"Process died during startup (exit code: {self.proc.returncode})"
                )

            return self.proc.pid

        except Exception as e:
            if self.proc:
                self.proc.kill()
                self.proc = None
            raise RuntimeError(f"Failed to start walrust: {e}") from e

    def stop(self, timeout: float = 5.0):
        """Stop walrust process gracefully.

        Args:
            timeout: Seconds to wait before force-killing
        """
        if not self.proc:
            return

        try:
            # Try graceful termination
            self.proc.terminate()
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Force kill if needed
            self.proc.kill()
            self.proc.wait()
        finally:
            self.proc = None
            self.databases = []

    def is_running(self) -> bool:
        """Check if walrust process is still running."""
        return self.proc is not None and self.proc.poll() is None


class LitestreamRunner:
    """Manage litestream process for benchmarking.

    Generates config file and starts litestream in multi-database mode.
    """

    def __init__(self, litestream_binary: Optional[str] = None):
        """Initialize litestream runner.

        Args:
            litestream_binary: Path to litestream binary (default: use PATH)
        """
        self.litestream_binary = litestream_binary or "litestream"
        self.proc: Optional[subprocess.Popen] = None
        self.config_path: Optional[Path] = None
        self.databases: List[Path] = []

    def start(
        self,
        databases: List[Path],
        bucket: str,
        endpoint: str,
        extra_args: Optional[List[str]] = None
    ) -> int:
        """Start litestream process.

        Args:
            databases: List of database paths to replicate
            bucket: S3 bucket (e.g., "s3://walrust-bench")
            endpoint: S3 endpoint URL
            extra_args: Additional CLI arguments

        Returns:
            Process PID

        Raises:
            RuntimeError: If litestream fails to start
        """
        if self.proc:
            raise RuntimeError("Litestream process already running")

        self.databases = databases

        # Generate config file
        self.config_path = Path(tempfile.mktemp(suffix='.yml', prefix='litestream_'))

        config = {
            'dbs': [
                {
                    'path': str(db.absolute()),
                    'replicas': [{
                        'url': f"{bucket}/{db.name}",
                        'endpoint': endpoint
                    }]
                }
                for db in databases
            ]
        }

        with open(self.config_path, 'w') as f:
            yaml.dump(config, f)

        # Build command
        cmd = [
            self.litestream_binary,
            "replicate",
            "-config", str(self.config_path)
        ]

        # Add extra arguments
        if extra_args:
            cmd.extend(extra_args)

        # Start process
        try:
            # Don't capture output to avoid blocking on full pipe buffers
            self.proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # Wait for startup
            time.sleep(2)

            # Check if process is still alive
            if self.proc.poll() is not None:
                stdout, stderr = self.proc.communicate()
                raise RuntimeError(
                    f"Litestream process died during startup:\n"
                    f"stdout: {stdout}\n"
                    f"stderr: {stderr}"
                )

            return self.proc.pid

        except Exception as e:
            if self.proc:
                self.proc.kill()
                self.proc = None
            self._cleanup_config()
            raise RuntimeError(f"Failed to start litestream: {e}") from e

    def stop(self, timeout: float = 5.0):
        """Stop litestream process gracefully.

        Args:
            timeout: Seconds to wait before force-killing
        """
        if not self.proc:
            return

        try:
            # Try graceful termination
            self.proc.terminate()
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Force kill if needed
            self.proc.kill()
            self.proc.wait()
        finally:
            self.proc = None
            self.databases = []
            self._cleanup_config()

    def _cleanup_config(self):
        """Remove temporary config file."""
        if self.config_path and self.config_path.exists():
            try:
                self.config_path.unlink()
            except Exception:
                pass
            finally:
                self.config_path = None

    def is_running(self) -> bool:
        """Check if litestream process is still running."""
        return self.proc is not None and self.proc.poll() is None


if __name__ == "__main__":
    # Simple test
    import tempfile
    import shutil
    from workload import create_benchmark_databases

    print("Testing runners...")

    # Create temp directory
    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create test databases
        databases = create_benchmark_databases(temp_dir, count=2, prefix="test")
        print(f"Created {len(databases)} test databases")

        # Test WalrustRunner
        print("\nTesting WalrustRunner...")
        runner = WalrustRunner()
        print(f"Found walrust at: {runner.walrust_binary}")

        # Note: This will fail if S3 credentials aren't set up
        # We just test that the runner can find the binary
        print("✓ WalrustRunner initialized successfully")

        # Test LitestreamRunner
        print("\nTesting LitestreamRunner...")
        try:
            subprocess.run(["which", "litestream"], check=True, capture_output=True)
            ls_runner = LitestreamRunner()
            print("✓ LitestreamRunner initialized successfully")
        except subprocess.CalledProcessError:
            print("⚠ Litestream not found in PATH (optional)")

        print("\n✓ All tests passed!")

    finally:
        # Cleanup
        shutil.rmtree(temp_dir)
