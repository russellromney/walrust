#!/bin/bash
set -e

# Export env vars
set -a
source .env
set +a

BUCKET="${WALSYNC_TEST_BUCKET:-empty-cherry-5203}"
ENDPOINT="${AWS_ENDPOINT_URL_S3}"

echo "=== Simple Data Integrity Test ==="
echo ""

# Create temp dir
TMPDIR=$(mktemp -d)
trap "kill $SQLITE_PID 2>/dev/null || true; rm -rf $TMPDIR" EXIT

ORIG_DB="$TMPDIR/test.db"
RESTORED_DB="$TMPDIR/restored.db"

# 1. Create test database
echo "1. Creating test database..."
sqlite3 "$ORIG_DB" << EOF
PRAGMA journal_mode=WAL;
PRAGMA wal_autocheckpoint=0;
CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT);
INSERT INTO test (data) VALUES ('row1'), ('row2'), ('row3'), ('row4'), ('row5');
EOF

# Keep a read lock to prevent WAL checkpoint
sqlite3 "$ORIG_DB" "SELECT 1" > /dev/null &
SQLITE_PID=$!

ORIG_CHECKSUM=$(sqlite3 "$ORIG_DB" "SELECT GROUP_CONCAT(data, ',') FROM test ORDER BY id" | shasum -a 256 | cut -d' ' -f1)
echo "   Original checksum: ${ORIG_CHECKSUM:0:16}..."
echo "   WAL file exists: $([ -f "$ORIG_DB-wal" ] && echo 'yes' || echo 'no')"

# 2. Sync to S3
echo "2. Syncing to S3..."
if [ -n "$ENDPOINT" ]; then
    gtimeout 15 ./target/release/walrust watch \
        --bucket "s3://$BUCKET/simple-test/" \
        --endpoint "$ENDPOINT" \
        --independent-tasks \
        --no-metrics \
        "$ORIG_DB" &
else
    gtimeout 15 ./target/release/walrust watch \
        --bucket "s3://$BUCKET/simple-test/" \
        --independent-tasks \
        --no-metrics \
        "$ORIG_DB" &
fi

WALRUST_PID=$!
sleep 10  # Wait for initial sync
kill $WALRUST_PID 2>/dev/null || true
wait $WALRUST_PID 2>/dev/null || true
echo "   Sync complete (waited 10s)"

# 3. Restore from S3
echo "3. Restoring from S3..."
if [ -n "$ENDPOINT" ]; then
    ./target/release/walrust restore test -o "$RESTORED_DB" \
        --bucket "s3://$BUCKET/simple-test/" \
        --endpoint "$ENDPOINT"
else
    ./target/release/walrust restore test -o "$RESTORED_DB" \
        --bucket "s3://$BUCKET/simple-test/"
fi

# 4. Verify
echo "4. Verifying data..."
RESTORED_CHECKSUM=$(sqlite3 "$RESTORED_DB" "SELECT GROUP_CONCAT(data, ',') FROM test ORDER BY id" | shasum -a 256 | cut -d' ' -f1)
echo "   Restored checksum: ${RESTORED_CHECKSUM:0:16}..."

if [ "$ORIG_CHECKSUM" = "$RESTORED_CHECKSUM" ]; then
    echo ""
    echo "✅ SUCCESS: Data integrity verified!"
    exit 0
else
    echo ""
    echo "❌ FAILED: Data corruption detected!"
    echo "   Original:  $ORIG_CHECKSUM"
    echo "   Restored:  $RESTORED_CHECKSUM"
    exit 1
fi
