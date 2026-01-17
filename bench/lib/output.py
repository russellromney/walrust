"""
Standardized JSON output schema for benchmark results.

Provides consistent output format across all benchmark scripts
for CI integration and documentation generation.
"""

import json
import os
import platform
import shutil
import subprocess
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class BenchmarkEnvironment:
    """Environment information for benchmark run."""
    platform: str = ""
    platform_version: str = ""
    python_version: str = ""
    walrust_version: str = ""
    litestream_version: str = ""
    storage_backend: str = ""  # "minio", "tigris", "s3", "r2"
    timestamp: str = ""

    @classmethod
    def capture(cls, storage_backend: str = "unknown") -> "BenchmarkEnvironment":
        """Capture current environment."""
        import sys

        return cls(
            platform=platform.system().lower(),
            platform_version=platform.release(),
            python_version=f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            walrust_version=get_walrust_version(),
            litestream_version=get_litestream_version(),
            storage_backend=storage_backend,
            timestamp=datetime.utcnow().isoformat() + "Z",
        )


@dataclass
class BenchmarkResult:
    """
    Top-level benchmark result container.

    Schema:
    {
        "version": "1.0",
        "environment": { ... },
        "benchmarks": {
            "memory_scaling": [...],
            "multi_db_performance": [...],
            ...
        }
    }
    """
    version: str = "1.0"
    environment: Optional[BenchmarkEnvironment] = None
    benchmarks: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "version": self.version,
            "environment": asdict(self.environment) if self.environment else {},
            "benchmarks": self.benchmarks,
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def save(self, path: Path) -> None:
        """Save to JSON file."""
        path.write_text(self.to_json())


def get_walrust_version() -> str:
    """Get walrust version from binary or Cargo.toml."""
    # Try running walrust --version
    walrust_bin = Path(__file__).parent.parent.parent / "target" / "release" / "walrust"
    if walrust_bin.exists():
        result = subprocess.run(
            [str(walrust_bin), "--version"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            # Parse "walrust 0.3.0" -> "0.3.0"
            parts = result.stdout.strip().split()
            if len(parts) >= 2:
                return parts[1]

    # Fall back to Cargo.toml
    cargo_toml = Path(__file__).parent.parent.parent / "Cargo.toml"
    if cargo_toml.exists():
        for line in cargo_toml.read_text().splitlines():
            if line.startswith("version"):
                # version = "0.3.0"
                return line.split('"')[1]

    return "unknown"


def get_litestream_version() -> str:
    """Get litestream version from binary."""
    litestream_bin = shutil.which("litestream")
    if not litestream_bin:
        return "not installed"

    result = subprocess.run(
        [litestream_bin, "version"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        # Parse version from output
        output = result.stdout.strip()
        if output.startswith("v"):
            return output[1:]  # Remove leading 'v'
        return output

    return "unknown"


def detect_storage_backend() -> str:
    """Detect storage backend from environment."""
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3", "")

    if "localhost" in endpoint or "127.0.0.1" in endpoint:
        return "minio"
    elif "tigris" in endpoint.lower():
        return "tigris"
    elif "r2.cloudflarestorage" in endpoint.lower():
        return "r2"
    elif endpoint:
        return "s3-compatible"
    else:
        return "aws-s3"


def write_json_results(
    results: Dict[str, Any],
    output_path: Optional[Path] = None,
    storage_backend: Optional[str] = None,
) -> str:
    """
    Write benchmark results in standardized JSON format.

    Args:
        results: Dictionary of benchmark results
        output_path: Optional path to write JSON file
        storage_backend: Override storage backend detection

    Returns:
        JSON string
    """
    backend = storage_backend or detect_storage_backend()

    result = BenchmarkResult(
        environment=BenchmarkEnvironment.capture(backend),
        benchmarks=results,
    )

    json_str = result.to_json()

    if output_path:
        result.save(output_path)

    return json_str


def generate_markdown_table(
    data: List[Dict],
    columns: List[tuple],  # [(key, header, format_fn), ...]
) -> str:
    """
    Generate a markdown table from benchmark data.

    Args:
        data: List of result dictionaries
        columns: List of (key, header, format_fn) tuples

    Returns:
        Markdown table string
    """
    if not data:
        return ""

    # Header
    headers = [col[1] for col in columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]

    # Rows
    for row in data:
        cells = []
        for key, _, fmt in columns:
            value = row.get(key, "")
            if fmt and value != "":
                value = fmt(value)
            cells.append(str(value))
        lines.append("| " + " | ".join(cells) + " |")

    return "\n".join(lines)
