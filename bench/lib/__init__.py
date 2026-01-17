"""
Walrust benchmark utilities.

Provides shared infrastructure for all benchmark scripts:
- MinIO container management for local/CI reproducibility
- Percentile and statistics calculations
- Standardized JSON output schema
"""

from .minio import MinioManager
from .stats import compute_percentiles, BenchmarkStats, aggregate_samples
from .output import (
    BenchmarkResult,
    BenchmarkEnvironment,
    write_json_results,
    get_walrust_version,
    get_litestream_version,
)

__version__ = "1.0.0"
__all__ = [
    "MinioManager",
    "compute_percentiles",
    "BenchmarkStats",
    "aggregate_samples",
    "BenchmarkResult",
    "BenchmarkEnvironment",
    "write_json_results",
    "get_walrust_version",
    "get_litestream_version",
]
