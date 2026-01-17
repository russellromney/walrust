# Walrust Benchmark Framework

Unified framework for benchmarking walrust vs litestream focusing on **data loss prevention** and **replication lag**.

## Goal

Measure how successfully walrust/litestream replicate SQLite data to S3, ensuring **minimal data loss** on server crashes, power failures, or disk corruption.

**Success Metric:** All committed SQLite writes appear in S3 with minimal replication lag.

## Quick Start

### 1. Prerequisites

```bash
# Build walrust
cargo build --release

# Install litestream (optional, for comparison)
brew install litestream  # macOS
# or download from https://litestream.io

# Install Python dependencies
uv pip install pyyaml boto3 psutil
```

### 2. Configure S3

Set up S3 credentials in `.env` file:

```bash
# Tigris S3 (recommended for walrust development)
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
AWS_REGION=auto
WALSYNC_TEST_BUCKET=walrust-bench
```

### 3. Run Quick Test

```bash
# Simple 2-database, 30-second test
uv run python bench/benchmark.py --config bench/configs/quick.yml

# Results saved to benchmark_results.json
```

### 4. Run Scalability Matrix

```bash
# Test across multiple database counts and write rates
# 4 DB counts × 3 write rates = 12 configs × 2 tools = 24 runs
uv run python bench/benchmark.py --config bench/configs/scalability-matrix.yml
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Benchmark Runner (bench/benchmark.py)                       │
│                                                             │
│  1. Read config (bench/configs/*.yml)                       │
│  2. Create test databases (SQLite with WAL mode)            │
│  3. Start tool (walrust/litestream)                         │
│  4. Run workload (DatabaseWriter threads)                   │
│  5. Monitor resources (ResourceMonitor)                     │
│  6. Stop tool gracefully                                    │
│  7. Verify replication (restore from S3 + compare)          │
│  8. Generate report (JSON)                                  │
└─────────────────────────────────────────────────────────────┘
```

## Components

### 1. Workload Generator (`bench/lib/workload.py`)

Writes data to SQLite databases at controlled rates:

```python
from workload import DatabaseWriter

# Create writer
writer = DatabaseWriter(
    db_path=Path("test.db"),
    writes_per_second=10  # 0 = unlimited
)

# Start writing
writer.start()
time.sleep(30)
writer.stop()

# Get all writes for verification
writes = writer.get_writes()  # [(write_id, commit_timestamp), ...]
```

**Key features:**
- Rate-limited writes (configurable w/s)
- Tracks write ID and timestamp for verification
- Thread-safe
- Random BLOB data (1KB per write)

### 2. Tool Runners (`bench/lib/runners.py`)

Manages walrust/litestream processes:

```python
from runners import WalrustRunner, LitestreamRunner

# Start walrust
runner = WalrustRunner()
pid = runner.start(
    databases=[Path("db1.db"), Path("db2.db")],
    bucket="s3://walrust-bench",
    endpoint="https://fly.storage.tigris.dev"
)

# Later...
runner.stop()
```

**Features:**
- Auto-detects binaries (target/release/walrust or PATH)
- Multi-database support
- Graceful shutdown
- Health checks

### 3. Resource Monitor (`bench/lib/monitor.py`)

Tracks CPU and memory usage:

```python
from monitor import ResourceMonitor

monitor = ResourceMonitor(
    pid=process_pid,
    include_children=True,  # For litestream
    sample_interval_ms=100
)

monitor.start()
# ... run benchmark ...
monitor.stop()

stats = monitor.get_stats()
# {
#   'peak_memory_mb': 19.5,
#   'avg_memory_mb': 18.2,
#   'peak_cpu_percent': 12.3,
#   'avg_cpu_percent': 4.5,
#   'sample_count': 300
# }
```

### 4. Replication Verifier (`bench/lib/verify.py`)

Verifies data made it to S3:

```python
from verify import ReplicationVerifier

verifier = ReplicationVerifier(
    s3_endpoint="https://fly.storage.tigris.dev"
)

metrics = verifier.verify(
    tool="walrust",
    db_name="bench_db_0",
    bucket="s3://walrust-bench",
    expected_writes=[(id1, ts1), (id2, ts2), ...]
)

# {
#   'total_writes': 300,
#   'replicated_writes': 300,
#   'missing_writes': 0,
#   'data_loss': False,
#   'sync_latency_p50_ms': 145.3,
#   'sync_latency_p95_ms': 890.2,
#   'sync_latency_p99_ms': 1250.8,
#   'sync_latency_max_ms': 2100.5
# }
```

**How it works:**
1. Restores database from S3 using `walrust restore` or `litestream restore`
2. Queries restored database for all writes
3. Compares expected vs actual writes
4. Gets S3 metadata (LastModified) for sync latency calculation

## Configuration Format

### Simple Config (`bench/configs/quick.yml`)

```yaml
name: "quick-test"

workload:
  type: "rate-limited"
  writes_per_second: 10  # Per database
  duration_seconds: 30

databases:
  count: 2
  size_kb: 100  # Initial padding

tools:
  - walrust
  - litestream

storage:
  bucket: "s3://walrust-bench"
  endpoint: "https://fly.storage.tigris.dev"

metrics:
  resource_sample_interval_ms: 100
```

### Matrix Config (`bench/configs/scalability-matrix.yml`)

Generates all combinations:

```yaml
name: "scalability-matrix"

matrix:
  databases: [1, 5, 10, 20]
  writes_per_second: [10, 50, 100]
  duration_seconds: [30]
  tools: [walrust, litestream]

# ... rest same as simple config
```

This expands to: 4 × 3 × 1 × 2 = **24 benchmark runs**

## Output Format

Results saved to `benchmark_results.json`:

```json
[
  {
    "config_name": "quick-test",
    "tool": "walrust",
    "databases": 2,
    "writes_per_second_per_db": 10,
    "duration_seconds": 30,
    "total_writes": 600,
    "actual_duration_seconds": 30.2,
    "actual_writes_per_second": 19.9,
    "replicated_writes": 600,
    "missing_writes": 0,
    "data_loss": false,
    "sync_latency_p50_ms": 145.3,
    "sync_latency_p95_ms": 890.2,
    "sync_latency_p99_ms": 1250.8,
    "sync_latency_max_ms": 2100.5,
    "peak_memory_mb": 19.5,
    "avg_memory_mb": 18.2,
    "peak_cpu_percent": 12.3,
    "avg_cpu_percent": 4.5
  },
  {
    "config_name": "quick-test",
    "tool": "litestream",
    ...
  }
]
```

## CLI Options

```bash
uv run python bench/benchmark.py \
  --config bench/configs/quick.yml \
  --output results.json \
  --work-dir benchmark_work \
  --tools walrust litestream \
  --no-cleanup  # Keep databases after benchmark
```

## Advanced Usage

### Custom Config

Create your own YAML config:

```yaml
name: "stress-test"

workload:
  writes_per_second: 500  # High write rate
  duration_seconds: 300   # 5 minutes

databases:
  count: 100              # Many databases

tools:
  - walrust               # Only test walrust

storage:
  bucket: "s3://my-bucket"
  endpoint: "https://s3.amazonaws.com"
```

### Programmatic Usage

```python
from pathlib import Path
import sys
sys.path.insert(0, 'bench/lib')

from config import BenchmarkConfig
from workload import DatabaseWriter
from runners import WalrustRunner
# ... etc

# Load config
config = BenchmarkConfig.from_yaml(Path('bench/configs/quick.yml'))

# Run your custom benchmark logic
# ...
```

## Metrics Explained

### Replication Metrics

- **total_writes**: Total writes performed by DatabaseWriter
- **replicated_writes**: Writes found in restored database from S3
- **missing_writes**: Writes NOT found (indicates data loss)
- **data_loss**: Boolean flag - any missing writes?
- **sync_latency_pXX_ms**: Time from SQLite commit to S3 upload

### Resource Metrics

- **peak_memory_mb**: Maximum memory usage (RSS)
- **avg_memory_mb**: Average memory usage
- **peak_cpu_percent**: Maximum CPU usage (can be >100% on multi-core)
- **avg_cpu_percent**: Average CPU usage

### Workload Metrics

- **actual_duration_seconds**: Actual benchmark runtime
- **actual_writes_per_second**: Achieved write rate (may differ from target)

## Troubleshooting

### walrust binary not found

```bash
# Build walrust first
cargo build --release

# Or specify path explicitly
export PATH="$PWD/target/release:$PATH"
```

### litestream not found

```bash
# Install litestream
brew install litestream  # macOS

# Or download from https://litestream.io
# Or run with --tools walrust only
```

### S3 credentials error

```bash
# Check .env file exists
cat .env

# Should contain:
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# AWS_ENDPOINT_URL_S3=...
# WALSYNC_TEST_BUCKET=...
```

### Replication verification fails

This is expected if:
1. S3 bucket doesn't exist
2. Credentials are invalid
3. Tool failed to sync (check tool logs)

The benchmark will still run and collect workload/resource metrics.

## Next Steps

After validating the standalone framework:

1. **Fly.io Integration**: Run benchmarks on real infrastructure (not localhost)
2. **Matrix Testing**: Test across different configurations
3. **Continuous Benchmarking**: Track performance over time
4. **Comparison Reports**: Generate markdown reports comparing walrust vs litestream

See `ROADMAP.md` for the fly-benchmark-engine integration plan.

## Implementation Status

✅ **Phase 1 Complete** - Standalone Benchmark Framework
- [x] Workload generator (DatabaseWriter)
- [x] Tool runners (WalrustRunner, LitestreamRunner)
- [x] Resource monitor (CPU/memory tracking)
- [x] Replication verifier (S3 restore + compare)
- [x] Main orchestrator (benchmark.py)
- [x] Config loading (YAML with matrix support)
- [x] Simple and matrix config examples

⏳ **Phase 2 Planned** - Fly.io Integration
- [ ] fly-benchmark-engine adapter
- [ ] Pool-based orchestration
- [ ] Remote resource monitoring
- [ ] Distributed benchmarking

See the session handoff notes for context on the original benchmark chaos and consolidation effort.
