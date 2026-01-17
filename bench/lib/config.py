"""
Benchmark configuration loader.

Automatically loads environment variables from .env file in the walrust directory.
Also supports loading YAML benchmark configs.
"""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any, List


@dataclass
class BenchConfig:
    """Benchmark configuration from environment."""
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


class BenchmarkConfig:
    """YAML-based benchmark configuration."""

    def __init__(self, data: Dict[str, Any]):
        self.data = data
        self.name = data.get('name', 'unnamed')
        self.workload = data.get('workload', {})
        self.databases = data.get('databases', {})
        self.tools = data.get('tools', ['walrust', 'litestream'])
        self.storage = data.get('storage', {})
        self.metrics = data.get('metrics', {})
        self.matrix = data.get('matrix')

    @classmethod
    def from_yaml(cls, path: Path) -> 'BenchmarkConfig':
        """Load config from YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(data)

    def expand_matrix(self) -> List['BenchmarkConfig']:
        """Expand matrix config into individual configs."""
        if not self.matrix:
            return [self]

        # Get matrix dimensions
        databases_list = self.matrix.get('databases', [self.databases.get('count', 1)])
        writes_per_second_list = self.matrix.get('writes_per_second', [self.workload.get('writes_per_second', 10)])
        duration_list = self.matrix.get('duration_seconds', [self.workload.get('duration_seconds', 30)])
        tools_list = self.matrix.get('tools', self.tools)

        # Generate all combinations
        configs = []
        for db_count in databases_list:
            for wps in writes_per_second_list:
                for duration in duration_list:
                    # Create new config for this combination
                    config_data = self.data.copy()
                    config_data['name'] = f"{self.name}_dbs{db_count}_wps{wps}_dur{duration}"
                    config_data['databases'] = {**self.databases, 'count': db_count}
                    config_data['workload'] = {
                        **self.workload,
                        'writes_per_second': wps,
                        'duration_seconds': duration
                    }
                    config_data['tools'] = tools_list
                    # Remove matrix to avoid recursion
                    config_data.pop('matrix', None)

                    configs.append(BenchmarkConfig(config_data))

        return configs


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
