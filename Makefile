.PHONY: build release test basic-e2e drill drill-version-skew clean install dev check fmt lint publish publish-pypi build-python bench bench-compare bench-multidb help

SOUP_PROJECT ?= turbolite
SOUP_ENV ?= development
USE_SOUP ?= 1

TEST_ENV = export AWS_ENDPOINT_URL_S3="$${AWS_ENDPOINT_URL_S3:-$${AWS_ENDPOINT_URL:-}}"; export WALRUST_TEST_BUCKET="$${WALRUST_TEST_BUCKET:-$${TIERED_TEST_BUCKET:-walrust-test-rr-2026}/verify-test}"; export WALRUST_S3_TEST_BUCKET="$${WALRUST_S3_TEST_BUCKET:-$${TIERED_TEST_BUCKET:-sqlces-test}}"

ifeq ($(USE_SOUP),1)
TEST_RUNNER = soup run -p $(SOUP_PROJECT) -e $(SOUP_ENV) --
else
TEST_RUNNER =
endif

# Default target
all: build

# Build debug version
build:
	cargo build

# Build release version
release:
	cargo build --release

# Build Python wheel
build-python:
	maturin build --release

# Run all workspace tests with live storage credentials from Soup.
# nextest runs everything in parallel except the tests pinned to the
# `serial` test-group in .config/nextest.toml (real SIGKILLs, deliberate
# races, split-brain/fenced-follower equivocation, a real chaos process
# kill). nextest doesn't run doctests, so we run those separately.
test:
	$(TEST_RUNNER) sh -c '$(TEST_ENV); cargo nextest run --workspace --profile default && cargo test --workspace --doc'

# Run tests with output (implies serial execution so output doesn't interleave)
test-verbose:
	$(TEST_RUNNER) sh -c '$(TEST_ENV); cargo nextest run --workspace --profile default --no-capture && cargo test --workspace --doc -- --nocapture'

basic-e2e: build
	$(TEST_RUNNER) sh -c '$(TEST_ENV); WALRUST_BIN="$$(pwd)/target/debug/walrust" drills/basic-e2e.sh'

drill: release
	$(TEST_RUNNER) sh -c '$(TEST_ENV); WALRUST_BIN="$$(pwd)/target/release/walrust" drills/run-all.sh'

# Version-skew drill: empirically characterizes an old walrust binary restoring
# a leveled bucket. MANUAL ONLY -- deliberately not part of `make drill` / the
# nightly workflow, because obtaining the old binary needs crates.io network
# access (flaky in CI) and can fall back to an expensive from-source build.
drill-version-skew: release
	$(TEST_RUNNER) sh -c '$(TEST_ENV); WALRUST_BIN="$$(pwd)/target/release/walrust" drills/version-skew.sh'

# Run micro-benchmarks (cargo bench)
bench:
	cargo bench

# Head-to-head vs litestream: request counts, replication lag, RSS.
# Self-contained (starts its own MinIO via docker); see bench/README.md.
bench-compare: release
	WALRUST_BIN="$$(pwd)/target/release/walrust" bench/compare-litestream.sh

# Multi-database RSS scaling (constant vs linear in db count).
bench-multidb: release
	WALRUST_BIN="$$(pwd)/target/release/walrust" bench/multidb-rss.sh

# Clean build artifacts
clean:
	cargo clean
	rm -rf target/ dist/ *.egg-info/

# Install locally
install: release
	cargo install --path .

# Install Python package locally (for development)
install-python:
	maturin develop

# Development mode - watch and rebuild
dev:
	cargo watch -x build

# Check for errors without building
check:
	cargo check --workspace --all-targets

# Format code
fmt:
	cargo fmt --all

# Check formatting
fmt-check:
	cargo fmt --all -- --check

# Run clippy linter
lint:
	cargo clippy --workspace --all-targets

# Publish to crates.io
publish:
	cargo publish

# Publish to PyPI
publish-pypi:
	maturin publish

# Publish to both crates.io and PyPI
publish-all: publish publish-pypi

# Bump version (requires cargo-edit: cargo install cargo-edit)
bump-patch:
	cargo set-version --bump patch

bump-minor:
	cargo set-version --bump minor

bump-major:
	cargo set-version --bump major

help:
	@echo "Available targets:"
	@echo ""
	@echo "  Build:"
	@echo "    make build        - Build debug binary"
	@echo "    make release      - Build release binary"
	@echo "    make build-python - Build Python wheel"
	@echo "    make install      - Install CLI locally"
	@echo "    make install-python - Install Python package for development"
	@echo ""
	@echo "  Test:"
	@echo "    make test           - Run all tests (with S3 credentials via soup)"
	@echo "    make test-verbose   - Run tests with output"
	@echo "    make basic-e2e      - Run the fast basic_e2e drill tier"
	@echo "    make drill          - Run the full drill suite"
	@echo "    make drill-version-skew - Manual-only: old binary vs a leveled bucket"
	@echo ""
	@echo "  Benchmark:"
	@echo "    make bench          - Run micro-benchmarks (cargo bench)"
	@echo "    make bench-compare  - Head-to-head vs litestream (requests, lag, RSS)"
	@echo "    make bench-multidb  - Multi-database RSS scaling (walrust vs litestream)"
	@echo ""
	@echo "  Code Quality:"
	@echo "    make check        - Check for errors"
	@echo "    make fmt          - Format code"
	@echo "    make lint         - Run clippy linter"
	@echo ""
	@echo "  Publish:"
	@echo "    make publish      - Publish to crates.io"
	@echo "    make publish-pypi - Publish to PyPI"
	@echo "    make publish-all  - Publish to both"
	@echo ""
	@echo "  Other:"
	@echo "    make clean        - Remove build artifacts"
	@echo "    make dev          - Watch and rebuild"

.DEFAULT_GOAL := help
