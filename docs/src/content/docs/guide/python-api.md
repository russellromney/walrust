---
title: Python API
description: Complete Python API reference for walrust
---

Use walrust from Python for programmatic SQLite backups and restores.

## Installation

```bash
pip install walrust
```

## Quick Start

```python
from walrust import Walrust

# Create instance
ws = Walrust("s3://my-bucket", endpoint="https://fly.storage.tigris.dev")

# Take a snapshot
ws.snapshot("/path/to/app.db")

# List backed up databases
dbs = ws.list()
print(dbs)  # ['app', 'users', 'analytics']

# Restore a database
ws.restore("app", "/path/to/restored.db")
```

## Class: Walrust

The main class for interacting with walrust.

### Constructor

```python
Walrust(bucket: str, endpoint: Optional[str] = None)
```

Create a new Walrust instance.

**Parameters:**

- `bucket` (str): S3 bucket URL. Can be:
  - `"s3://my-bucket"` (AWS S3)
  - `"s3://my-bucket/prefix"` (with prefix)
  - `"my-bucket"` (without s3:// prefix)
- `endpoint` (str, optional): S3 endpoint URL for non-AWS providers:
  - Tigris: `"https://fly.storage.tigris.dev"`
  - MinIO: `"http://localhost:9000"`
  - Cloudflare R2: `"https://<account-id>.r2.cloudflarestorage.com"`

**Returns:**

- `Walrust` instance

**Raises:**

- `RuntimeError`: If unable to create runtime or invalid configuration

**Example:**

```python
# AWS S3
ws = Walrust("s3://my-backups")

# Tigris (Fly.io)
ws = Walrust("s3://my-backups", endpoint="https://fly.storage.tigris.dev")

# With prefix
ws = Walrust("s3://my-backups/production")
```

### Methods

#### snapshot()

```python
snapshot(database: str) -> None
```

Take a snapshot of a database and upload to S3.

**Parameters:**

- `database` (str): Path to the SQLite database file

**Returns:**

- None

**Raises:**

- `RuntimeError`: If snapshot fails (database not found, S3 error, etc.)

**Example:**

```python
ws = Walrust("s3://my-bucket")

# Snapshot a single database
ws.snapshot("/data/app.db")

# Snapshot multiple databases in a loop
for db in ["/data/app.db", "/data/users.db"]:
    ws.snapshot(db)
```

**Notes:**

- Database must exist and be readable
- Database should be in WAL mode (walrust will warn if not)
- Creates a full snapshot with all database pages
- Uploads to `s3://<bucket>/<db-name>/00000001-00000001.ltx`

---

#### restore()

```python
restore(name: str, output: str, point_in_time: Optional[str] = None) -> None
```

Restore a database from S3.

**Parameters:**

- `name` (str): Database name as stored in S3 (usually the filename without extension)
- `output` (str): Output path for the restored database
- `point_in_time` (str, optional): TXID/sequence number for point-in-time recovery (e.g., `"42"`)

**Returns:**

- None

**Raises:**

- `RuntimeError`: If restore fails (no snapshot found, S3 error, checksum mismatch, etc.)

**Example:**

```python
ws = Walrust("s3://my-bucket")

# Basic restore
ws.restore("app", "/data/restored.db")

# Point-in-time restore
ws.restore(
    "app",
    "/data/restored.db",
    point_in_time="42"
)
```

**Notes:**

- Downloads the latest snapshot and applies all incremental LTX files
- Verifies checksums during restore
- Point-in-time restore stops at the requested TXID/sequence number
- If `point_in_time` is before the first snapshot, raises `RuntimeError`

---

#### list()

```python
list() -> List[str]
```

List databases stored in the S3 bucket.

**Parameters:**

- None

**Returns:**

- `List[str]`: List of database names (as stored in S3)

**Raises:**

- `RuntimeError`: If listing fails (S3 error, authentication failure, etc.)

**Example:**

```python
ws = Walrust("s3://my-bucket")

# List all databases
dbs = ws.list()
print(dbs)
# Output: ['app', 'users', 'analytics']

# Check if a specific database exists
if 'app' in ws.list():
    print("app.db is backed up")
```

**Notes:**

- Returns database names (prefixes), not full S3 keys
- Empty list if no databases found
- Requires `s3:ListBucket` permission

---

## Module-Level Functions

Convenience functions for one-off operations without creating a `Walrust` instance.

### snapshot()

```python
snapshot(database: str, bucket: str, endpoint: Optional[str] = None) -> None
```

Take a snapshot without creating a Walrust instance.

**Parameters:**

- `database` (str): Path to the SQLite database file
- `bucket` (str): S3 bucket URL
- `endpoint` (str, optional): S3 endpoint URL

**Returns:**

- None

**Raises:**

- `RuntimeError`: If snapshot fails

**Example:**

```python
from walrust import snapshot

# Quick snapshot
snapshot("/data/app.db", "s3://my-bucket")

# With custom endpoint
snapshot(
    "/data/app.db",
    "s3://my-bucket",
    endpoint="https://fly.storage.tigris.dev"
)
```

---

### restore()

```python
restore(
    name: str,
    output: str,
    bucket: str,
    endpoint: Optional[str] = None,
    point_in_time: Optional[str] = None
) -> None
```

Restore a database without creating a Walrust instance.

**Parameters:**

- `name` (str): Database name as stored in S3
- `output` (str): Output path for the restored database
- `bucket` (str): S3 bucket URL
- `endpoint` (str, optional): S3 endpoint URL
- `point_in_time` (str, optional): TXID/sequence number for PITR

**Returns:**

- None

**Raises:**

- `RuntimeError`: If restore fails

**Example:**

```python
from walrust import restore

# Quick restore
restore("app", "/data/restored.db", "s3://my-bucket")

# With point-in-time
restore(
    "app",
    "/data/restored.db",
    "s3://my-bucket",
    point_in_time="42"
)
```

---

### list_databases()

```python
list_databases(bucket: str, endpoint: Optional[str] = None) -> List[str]
```

List databases without creating a Walrust instance.

**Parameters:**

- `bucket` (str): S3 bucket URL
- `endpoint` (str, optional): S3 endpoint URL

**Returns:**

- `List[str]`: List of database names

**Raises:**

- `RuntimeError`: If listing fails

**Example:**

```python
from walrust import list_databases

# Quick list
dbs = list_databases("s3://my-bucket")
print(dbs)

# With custom endpoint
dbs = list_databases(
    "s3://my-bucket",
    endpoint="https://fly.storage.tigris.dev"
)
```

---

## Error Handling

All methods raise `RuntimeError` on failure. Catch exceptions to handle errors gracefully:

```python
from walrust import Walrust

ws = Walrust("s3://my-bucket")

try:
    ws.snapshot("/data/app.db")
except RuntimeError as e:
    print(f"Snapshot failed: {e}")
    # Handle error (retry, alert, etc.)
```

Common error scenarios:

- **Database not found:** File doesn't exist or not readable
- **S3 authentication failure:** Invalid credentials or missing env vars
- **Network error:** Can't reach S3 endpoint
- **Checksum mismatch:** Corrupted backup during restore
- **No snapshot found:** Database name doesn't exist in S3

## Environment Variables

Walrust reads standard AWS environment variables:

```python
import os

# Set credentials
os.environ["AWS_ACCESS_KEY_ID"] = "your-key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "your-secret"
os.environ["AWS_ENDPOINT_URL_S3"] = "https://fly.storage.tigris.dev"

# Now use walrust
from walrust import Walrust
ws = Walrust("s3://my-bucket")
ws.snapshot("/data/app.db")
```

Required environment variables:

- `AWS_ACCESS_KEY_ID` - S3 access key
- `AWS_SECRET_ACCESS_KEY` - S3 secret key
- `AWS_ENDPOINT_URL_S3` - S3 endpoint (optional for AWS S3, required for Tigris/MinIO/R2)
- `AWS_REGION` - AWS region (optional, defaults to `us-east-1`)

## Complete Examples

### Automated Backup Script

```python
#!/usr/bin/env python3
from walrust import Walrust
import sys

DATABASES = [
    "/data/app.db",
    "/data/users.db",
    "/data/analytics.db",
]

def main():
    ws = Walrust("s3://my-bucket", endpoint="https://fly.storage.tigris.dev")

    failed = []
    for db in DATABASES:
        try:
            print(f"Snapshotting {db}...")
            ws.snapshot(db)
            print(f"✓ {db}")
        except RuntimeError as e:
            print(f"✗ {db}: {e}")
            failed.append(db)

    if failed:
        print(f"\nFailed to snapshot {len(failed)} database(s)")
        sys.exit(1)
    else:
        print(f"\n✓ All {len(DATABASES)} databases backed up")
        sys.exit(0)

if __name__ == "__main__":
    main()
```

### Disaster Recovery Script

```python
#!/usr/bin/env python3
from walrust import Walrust
import sys

def restore_all(bucket, endpoint, output_dir):
    """Restore all databases from S3"""
    ws = Walrust(bucket, endpoint=endpoint)

    # List all backed up databases
    dbs = ws.list()
    print(f"Found {len(dbs)} database(s) in {bucket}")

    # Restore each one
    for name in dbs:
        output = f"{output_dir}/{name}.db"
        try:
            print(f"Restoring {name} to {output}...")
            ws.restore(name, output)
            print(f"✓ {name}")
        except RuntimeError as e:
            print(f"✗ {name}: {e}")
            return False

    print(f"\n✓ Restored all {len(dbs)} databases to {output_dir}")
    return True

if __name__ == "__main__":
    success = restore_all(
        bucket="s3://my-bucket",
        endpoint="https://fly.storage.tigris.dev",
        output_dir="/recovery"
    )
    sys.exit(0 if success else 1)
```

### Flask Integration

```python
from flask import Flask
from walrust import Walrust
import os

app = Flask(__name__)

# Initialize walrust
ws = Walrust(
    os.environ["S3_BUCKET"],
    endpoint=os.environ.get("S3_ENDPOINT")
)

@app.route("/backup", methods=["POST"])
def backup():
    """Trigger manual backup"""
    try:
        ws.snapshot("/data/app.db")
        return {"status": "success"}, 200
    except RuntimeError as e:
        return {"status": "error", "message": str(e)}, 500

@app.route("/backups", methods=["GET"])
def list_backups():
    """List available backups"""
    try:
        dbs = ws.list()
        return {"databases": dbs}, 200
    except RuntimeError as e:
        return {"status": "error", "message": str(e)}, 500

if __name__ == "__main__":
    app.run()
```

### Jupyter Notebook Usage

```python
# Cell 1: Setup
from walrust import Walrust
import pandas as pd
import sqlite3

ws = Walrust("s3://my-bucket", endpoint="https://fly.storage.tigris.dev")

# Cell 2: Analyze data
conn = sqlite3.connect("analysis.db")
df = pd.read_sql("SELECT * FROM results", conn)
df.describe()

# Cell 3: Backup results
ws.snapshot("analysis.db")
print("Analysis backed up to S3")

# Cell 4: Later... restore
ws.restore("analysis", "analysis-restored.db")
conn_restored = sqlite3.connect("analysis-restored.db")
df_restored = pd.read_sql("SELECT * FROM results", conn_restored)
assert df.equals(df_restored), "Backup verification failed!"
print("✓ Backup verified")
```

## Best Practices

1. **Reuse Walrust instances** - Creating instances is cheap, but reusing them avoids repeated authentication

2. **Use try/except** - Always wrap walrust calls in error handling

3. **Check WAL mode** - Ensure your database uses WAL mode:

```python
import sqlite3
conn = sqlite3.connect("app.db")
conn.execute("PRAGMA journal_mode=WAL")
conn.close()
```

4. **Validate backups** - Periodically test restores to ensure backups work:

```python
import tempfile
import os

def validate_backup(ws, name):
    """Restore to temp location and verify"""
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        try:
            ws.restore(name, tmp.name)
            # Run integrity check
            conn = sqlite3.connect(tmp.name)
            result = conn.execute("PRAGMA integrity_check").fetchone()
            conn.close()
            return result[0] == "ok"
        finally:
            os.unlink(tmp.name)
```

5. **Set environment variables** - Don't hardcode credentials:

```python
# Good
ws = Walrust(os.environ["S3_BUCKET"])

# Bad
ws = Walrust("s3://my-bucket")  # Credentials in env vars
```

## See Also

- [CLI Reference](/guide/cli/) - Command-line interface
- [Configuration Reference](/config/configuration-reference/) - Config file options
- [Troubleshooting](/guide/troubleshooting/) - Common issues
