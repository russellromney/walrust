"""
Benchmark configuration loader.

Automatically loads environment variables from .env file in the walrust directory.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class BenchConfig:
    """Benchmark configuration."""
    bucket: str
    endpoint: Optional[str]
    access_key: Optional[str]
    secret_key: Optional[str]
    region: Optional[str]

    @property
    def bucket_url(self) -> str:
        """Return bucket as s3:// URL."""
        if self.bucket.startswith("s3://"):
            return self.bucket
        return f"s3://{self.bucket}"


def load_dotenv() -> None:
    """Load environment variables from .env file."""
    # Find .env file relative to this file (bench/lib/config.py -> walrust/.env)
    env_file = Path(__file__).parent.parent.parent / ".env"

    if not env_file.exists():
        return

    with open(env_file) as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue
            # Parse KEY=VALUE
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                # Don't override existing env vars
                if key not in os.environ:
                    os.environ[key] = value


def get_config() -> BenchConfig:
    """Get benchmark configuration from environment."""
    # Load .env first
    load_dotenv()

    bucket = os.environ.get("WALSYNC_TEST_BUCKET", "")
    if not bucket:
        raise ValueError(
            "WALSYNC_TEST_BUCKET not set. Either:\n"
            "  1. Create a .env file with WALSYNC_TEST_BUCKET=your-bucket\n"
            "  2. Run: export WALSYNC_TEST_BUCKET=your-bucket"
        )

    return BenchConfig(
        bucket=bucket,
        endpoint=os.environ.get("AWS_ENDPOINT_URL_S3"),
        access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region=os.environ.get("AWS_REGION"),
    )
