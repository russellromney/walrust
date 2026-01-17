"""
Statistics utilities for benchmark measurements.

Provides consistent percentile calculations and aggregation
across all benchmark scripts.
"""

import math
import statistics
from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class BenchmarkStats:
    """Aggregated statistics for a set of samples."""
    count: int = 0
    min: float = 0.0
    max: float = 0.0
    mean: float = 0.0
    median: float = 0.0
    stddev: float = 0.0
    p50: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    samples: List[float] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "count": self.count,
            "min": round(self.min, 3),
            "max": round(self.max, 3),
            "mean": round(self.mean, 3),
            "median": round(self.median, 3),
            "stddev": round(self.stddev, 3),
            "p50": round(self.p50, 3),
            "p95": round(self.p95, 3),
            "p99": round(self.p99, 3),
            "samples": [round(s, 3) for s in self.samples[:10]],  # First 10 samples
        }


def compute_percentiles(
    samples: List[float],
    percentiles: List[int] = [50, 95, 99],
) -> Dict[str, float]:
    """
    Compute percentiles for a list of samples.

    Args:
        samples: List of numeric samples
        percentiles: List of percentiles to compute (0-100)

    Returns:
        Dictionary mapping percentile names (e.g., "p50") to values
    """
    if not samples:
        return {f"p{p}": 0.0 for p in percentiles}

    sorted_samples = sorted(samples)
    n = len(sorted_samples)

    result = {}
    for p in percentiles:
        if n == 1:
            result[f"p{p}"] = sorted_samples[0]
        else:
            # Use linear interpolation for better accuracy
            idx = (n - 1) * p / 100
            lower = int(idx)
            upper = min(lower + 1, n - 1)
            frac = idx - lower
            result[f"p{p}"] = sorted_samples[lower] * (1 - frac) + sorted_samples[upper] * frac

    return result


def aggregate_samples(
    samples: List[float],
    keep_samples: int = 10,
) -> BenchmarkStats:
    """
    Aggregate a list of samples into statistics.

    Args:
        samples: List of numeric samples
        keep_samples: Number of raw samples to keep in result

    Returns:
        BenchmarkStats with computed statistics
    """
    if not samples:
        return BenchmarkStats()

    sorted_samples = sorted(samples)
    n = len(sorted_samples)

    # Compute percentiles
    percentiles = compute_percentiles(samples, [50, 95, 99])

    # Compute stddev (requires at least 2 samples)
    if n >= 2:
        stddev = statistics.stdev(samples)
    else:
        stddev = 0.0

    return BenchmarkStats(
        count=n,
        min=sorted_samples[0],
        max=sorted_samples[-1],
        mean=statistics.mean(samples),
        median=statistics.median(samples),
        stddev=stddev,
        p50=percentiles["p50"],
        p95=percentiles["p95"],
        p99=percentiles["p99"],
        samples=samples[:keep_samples],
    )


def format_duration(ms: float) -> str:
    """Format a duration in milliseconds for display."""
    if ms < 1:
        return f"{ms * 1000:.0f}µs"
    elif ms < 1000:
        return f"{ms:.1f}ms"
    else:
        return f"{ms / 1000:.2f}s"


def format_size(bytes: float) -> str:
    """Format a size in bytes for display."""
    if bytes < 1024:
        return f"{bytes:.0f}B"
    elif bytes < 1024 * 1024:
        return f"{bytes / 1024:.1f}KB"
    elif bytes < 1024 * 1024 * 1024:
        return f"{bytes / (1024 * 1024):.1f}MB"
    else:
        return f"{bytes / (1024 * 1024 * 1024):.2f}GB"


def format_rate(per_sec: float, unit: str = "") -> str:
    """Format a rate for display."""
    if per_sec < 1:
        return f"{per_sec * 1000:.1f}m{unit}/s"
    elif per_sec < 1000:
        return f"{per_sec:.1f}{unit}/s"
    elif per_sec < 1000000:
        return f"{per_sec / 1000:.1f}K{unit}/s"
    else:
        return f"{per_sec / 1000000:.1f}M{unit}/s"
