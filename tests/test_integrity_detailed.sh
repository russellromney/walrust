#!/bin/bash
set -e

# Export env vars
set -a
source .env
set +a

BUCKET="${WALSYNC_TEST_BUCKET:-empty-cherry-5203}"
ENDPOINT="${AWS_ENDPOINT_URL_S3}"

echo "=== DETAILED Data Integrity Test ==="
echo ""

# Create temp dir
TMPDIR=$(mktemp -d)
trap "kill $SQLITE_PID 2>/dev/null || true; rm -rf $TMPDIR" EXIT

ORIG_DB="$TMPDIR/test.db"
RESTORED_DB="$TMPDIR/restored.db"

# 1. Create test database
echo "1. Creating test database with UNIQUE data..."
sqlite3 "$ORIG_DB" << 'EOSQL'
PRAGMA journal_mode=WAL;
PRAGMA wal_autocheckpoint=0;
CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT, timestamp INTEGER);
INSERT INTO test (data, timestamp) VALUES 
  ('unique_data_row_1', 1234567890),
  ('unique_data_row_2', 1234567891),
  ('unique_data_row_3', 1234567892),
  ('unique_data_row_4', 1234567893),
  ('unique_data_row_5', 1234567894);
EOSQL

# Keep a read lock to prevent WAL checkpoint
sqlite3 "$ORIG_DB" "SELECT 1" > /dev/null &
SQLITE_PID=$!

echo "   Original data:"
sqlite3 "$ORIG_DB" "SELECT * FROM test ORDER BY id"

ORIG_DATA=$(sqlite3 "$ORIG_DB" "SELECT GROUP_CONCAT(data || '|' || timestamp, ',') FROM test ORDER BY id")
ORIG_CHECKSUM=$(echo "$ORIG_DATA" | shasum -a 256 | cut -d' ' -f1)
echo "   Original checksum: $ORIG_CHECKSUM"
echo "   WAL file size: $(stat -f%z "$ORIG_DB-wal" 2>/dev/null || echo "0") bytes"

# 2. Sync to S3
echo ""
echo "2. Syncing to S3..."
gtimeout 15 ./target/release/walrust watch \
    --bucket "s3://$BUCKET/detailed-test/" \
    --endpoint "$ENDPOINT" \
    --independent-tasks \
    --no-metrics \
    "$ORIG_DB" &

WALRUST_PID=$!
sleep 10  # Wait for initial sync
kill $WALRUST_PID 2>/dev/null || true
wait $WALRUST_PID 2>/dev/null || true
echo "   Sync complete"

# 3. Check what's in S3
echo ""
echo "3. Checking S3 contents..."
aws s3 ls "s3://$BUCKET/detailed-test/test/" --endpoint-url=$ENDPOINT --recursive

# 4. Restore from S3
echo ""
echo "4. Restoring from S3..."
./target/release/walrust restore test -o "$RESTORED_DB" \
    --bucket "s3://$BUCKET/detailed-test/" \
    --endpoint "$ENDPOINT"

echo "   Restored database exists: $([ -f "$RESTORED_DB" ] && echo 'yes' || echo 'no')"

# 5. Verify data
echo ""
echo "5. Verifying data..."
echo "   Restored data:"
sqlite3 "$RESTORED_DB" "SELECT * FROM test ORDER BY id"

RESTORED_DATA=$(sqlite3 "$RESTORED_DB" "SELECT GROUP_CONCAT(data || '|' || timestamp, ',') FROM test ORDER BY id")
RESTORED_CHECKSUM=$(echo "$RESTORED_DATA" | shasum -a 256 | cut -d' ' -f1)
echo "   Restored checksum: $RESTORED_CHECKSUM"

echo ""
echo "6. Comparison:"
echo "   Original:  $ORIG_DATA"
echo "   Restored:  $RESTORED_DATA"
echo ""
echo "   Original hash:  $ORIG_CHECKSUM"
echo "   Restored hash:  $RESTORED_CHECKSUM"

if [ "$ORIG_CHECKSUM" = "$RESTORED_CHECKSUM" ]; then
    echo ""
    echo "✅ SUCCESS: Data integrity verified!"
    echo "   All 5 rows with unique data matched perfectly"
    exit 0
else
    echo ""
    echo "❌ FAILED: Data corruption detected!"
    echo "   Checksums don't match!"
    exit 1
fi
