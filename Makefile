.PHONY: build release test clean install dev check fmt lint publish publish-pypi build-python bench bench-compare bench-realworld help

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
test:
	$(TEST_RUNNER) sh -c '$(TEST_ENV); cargo test --workspace'

# Run tests with output
test-verbose:
	$(TEST_RUNNER) sh -c '$(TEST_ENV); cargo test --workspace -- --nocapture'

# Run micro-benchmarks (cargo bench)
bench:
	cargo bench

# Run comparison benchmark (walrust vs litestream)
bench-compare: release
	uv run bench/compare.py

# Run real-world benchmarks (sync latency, restore, multi-DB)
bench-realworld: release
	uv run bench/realworld.py

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
	@echo ""
	@echo "  Benchmark:"
	@echo "    make bench          - Run micro-benchmarks (cargo bench)"
	@echo "    make bench-compare  - Compare walrust vs litestream (memory/CPU)"
	@echo "    make bench-realworld - Real-world benchmarks (sync latency, restore, multi-DB)"
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
