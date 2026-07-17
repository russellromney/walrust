#!/bin/bash
# Litestream Compatibility Test Script
# Tests walrust <-> litestream interoperability
#
# Prerequisites:
# - litestream installed (brew install litestream)
# - walrust built (cargo build --release)
# - S3 credentials configured (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL_S3)
# - BUCKET_NAME environment variable set
#
# Usage:
#   cd walrust
#   source .env  # Load credentials
#   ./tests/litestream_compat.sh

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
WALRUST_BIN="${WALRUST_BIN:-./target/release/walrust}"
TEST_DIR="/tmp/walrust-litestream-compat-$$"
BUCKET="${BUCKET_NAME:-walrust-bench-storage}"
ENDPOINT="${AWS_ENDPOINT_URL_S3:-https://fly.storage.tigris.dev}"
TEST_PREFIX="compat-test-$(date +%s)"

echo "============================================"
echo "Litestream Compatibility Test"
echo "============================================"
echo "Test directory: $TEST_DIR"
echo "Bucket: $BUCKET"
echo "Endpoint: $ENDPOINT"
echo "Test prefix: $TEST_PREFIX"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    rm -rf "$TEST_DIR"
    # Note: S3 cleanup left to user (or add aws s3 rm --recursive)
}
trap cleanup EXIT

# Create test directory
mkdir -p "$TEST_DIR"
cd "$TEST_DIR"

# Check prerequisites
check_prereqs() {
    echo "Checking prerequisites..."

    if ! command -v litestream &> /dev/null; then
        echo -e "${RED}ERROR: litestream not installed${NC}"
        echo "Install with: brew install litestream"
        exit 1
    fi
    echo "  litestream: $(litestream version)"

    if [ ! -x "$WALRUST_BIN" ]; then
        echo -e "${RED}ERROR: walrust binary not found at $WALRUST_BIN${NC}"
        echo "Build with: cargo build --release"
        exit 1
    fi
    echo "  walrust: $($WALRUST_BIN --version 2>/dev/null || echo 'unknown')"

    if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
        echo -e "${RED}ERROR: AWS_ACCESS_KEY_ID not set${NC}"
        exit 1
    fi
    echo "  AWS credentials: configured"

    echo -e "${GREEN}Prerequisites OK${NC}"
    echo ""
}

# Test 1: Walrust backup -> Litestream restore
test_walrust_to_litestream() {
    echo "============================================"
    echo "TEST 1: Walrust backup -> Litestream restore"
    echo "============================================"

    local db_name="walrust-source"
    local db_path="$TEST_DIR/$db_name.db"
    local restore_path="$TEST_DIR/$db_name-restored.db"
    # Note: walrust snapshot automatically appends db_name to the path
    local s3_path="s3://$BUCKET/$TEST_PREFIX"

    # Create test database with data
    echo "Creating test database..."
    sqlite3 "$db_path" <<EOF
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO users (id, name, email) VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com');

CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    title TEXT,
    body BLOB,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
INSERT INTO posts (user_id, title, body) VALUES
    (1, 'Hello World', X'48656C6C6F20576F726C64'),
    (2, 'Test Post', X'54657374696E672031323334');
EOF

    local original_checksum=$(sqlite3 "$db_path" "SELECT COUNT(*) FROM users; SELECT COUNT(*) FROM posts;")
    echo "Original data: $original_checksum"

    # Backup with walrust
    echo "Taking walrust snapshot..."
    "$WALRUST_BIN" snapshot "$db_path" -b "$s3_path" --endpoint "$ENDPOINT"

    # List what walrust created (db_name is appended by walrust)
    echo "Files created by walrust:"
    aws s3 ls "$s3_path/$db_name/" --recursive --endpoint-url "$ENDPOINT" 2>/dev/null || true

    # Restore with litestream
    echo "Restoring with litestream..."
    rm -f "$restore_path"

    # Litestream needs a config file for non-default endpoints
    cat > "$TEST_DIR/litestream-restore.yml" <<YAML
dbs:
  - path: $restore_path
    replicas:
      - type: s3
        bucket: $BUCKET
        path: $TEST_PREFIX/$db_name
        endpoint: $ENDPOINT
        force-path-style: true
YAML

    if litestream restore -config "$TEST_DIR/litestream-restore.yml" "$restore_path" 2>&1; then
        echo -e "${GREEN}Litestream restore succeeded${NC}"

        # Verify data
        local restored_checksum=$(sqlite3 "$restore_path" "SELECT COUNT(*) FROM users; SELECT COUNT(*) FROM posts;")
        echo "Restored data: $restored_checksum"

        if [ "$original_checksum" = "$restored_checksum" ]; then
            echo -e "${GREEN}TEST 1 PASSED: Data integrity verified${NC}"
            return 0
        else
            echo -e "${RED}TEST 1 FAILED: Data mismatch${NC}"
            echo "Expected: $original_checksum"
            echo "Got: $restored_checksum"
            return 1
        fi
    else
        echo -e "${RED}TEST 1 FAILED: Litestream restore failed${NC}"
        return 1
    fi
}

# Test 2: Litestream backup -> Walrust restore
test_litestream_to_walrust() {
    echo ""
    echo "============================================"
    echo "TEST 2: Litestream backup -> Walrust restore"
    echo "============================================"

    local db_name="litestream-source"
    local db_path="$TEST_DIR/$db_name.db"
    local restore_path="$TEST_DIR/$db_name-restored.db"
    local s3_path="s3://$BUCKET/$TEST_PREFIX/$db_name"

    # Create test database
    echo "Creating test database..."
    sqlite3 "$db_path" <<EOF
CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    data BLOB,
    metadata TEXT
);
INSERT INTO items (data, metadata) VALUES
    (X'DEADBEEF', '{"type": "binary"}'),
    (X'CAFEBABE', '{"type": "java"}'),
    (randomblob(1000), '{"type": "random"}');
EOF

    local original_count=$(sqlite3 "$db_path" "SELECT COUNT(*) FROM items;")
    local original_sum=$(sqlite3 "$db_path" "SELECT SUM(LENGTH(data)) FROM items;")
    echo "Original: $original_count items, $original_sum bytes"

    # Create litestream config
    cat > "$TEST_DIR/litestream.yml" <<YAML
dbs:
  - path: $db_path
    replicas:
      - type: s3
        bucket: $BUCKET
        path: $TEST_PREFIX/$db_name
        endpoint: $ENDPOINT
        force-path-style: true
        sync-interval: 1s
YAML

    # Start litestream in background
    echo "Starting litestream replication..."
    litestream replicate -config "$TEST_DIR/litestream.yml" &
    LITESTREAM_PID=$!

    # Wait for initial sync
    sleep 3

    # Add more data to trigger WAL sync
    echo "Adding more data..."
    sqlite3 "$db_path" "INSERT INTO items (data, metadata) VALUES (randomblob(500), '{\"added\": true}');"
    sleep 2

    # Stop litestream
    kill $LITESTREAM_PID 2>/dev/null || true
    wait $LITESTREAM_PID 2>/dev/null || true

    # List what litestream created
    echo "Files created by litestream:"
    aws s3 ls "$s3_path/" --recursive --endpoint-url "$ENDPOINT" 2>/dev/null || true

    # Restore with walrust
    echo "Restoring with walrust..."
    rm -f "$restore_path"

    if "$WALRUST_BIN" restore "$db_name" -o "$restore_path" -b "s3://$BUCKET/$TEST_PREFIX" --endpoint "$ENDPOINT" 2>&1; then
        echo -e "${GREEN}Walrust restore succeeded${NC}"

        # Verify data
        local restored_count=$(sqlite3 "$restore_path" "SELECT COUNT(*) FROM items;")
        local restored_sum=$(sqlite3 "$restore_path" "SELECT SUM(LENGTH(data)) FROM items;")
        echo "Restored: $restored_count items, $restored_sum bytes"

        # Note: count might be higher due to added row
        if [ "$restored_count" -ge "$original_count" ]; then
            echo -e "${GREEN}TEST 2 PASSED: Data restored successfully${NC}"
            return 0
        else
            echo -e "${RED}TEST 2 FAILED: Data mismatch${NC}"
            return 1
        fi
    else
        echo -e "${RED}TEST 2 FAILED: Walrust restore failed${NC}"
        return 1
    fi
}

# Test 3: Format inspection
test_format_inspection() {
    echo ""
    echo "============================================"
    echo "TEST 3: Format Inspection"
    echo "============================================"

    echo "Checking S3 structure..."
    echo ""
    echo "Files from Test 1 (walrust):"
    aws s3 ls "s3://$BUCKET/$TEST_PREFIX/walrust-source/" --recursive --endpoint-url "$ENDPOINT" 2>/dev/null || echo "  (none or error)"

    echo ""
    echo "Files from Test 2 (litestream):"
    aws s3 ls "s3://$BUCKET/$TEST_PREFIX/litestream-source/" --recursive --endpoint-url "$ENDPOINT" 2>/dev/null || echo "  (none or error)"

    echo ""
    echo "Expected litestream format:"
    echo "  db_name/0000/min-max.ltx  (generation 0 = incrementals)"
    echo "  db_name/0001/min-max.ltx  (generation 1+ = snapshots)"
    echo "  TXIDs are 16-char lowercase hex"
    echo ""
}

# Main
main() {
    check_prereqs

    local failed=0

    if ! test_walrust_to_litestream; then
        failed=$((failed + 1))
    fi

    if ! test_litestream_to_walrust; then
        failed=$((failed + 1))
    fi

    test_format_inspection

    echo ""
    echo "============================================"
    echo "SUMMARY"
    echo "============================================"
    if [ $failed -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        exit 0
    else
        echo -e "${RED}$failed test(s) failed${NC}"
        exit 1
    fi
}

main "$@"
