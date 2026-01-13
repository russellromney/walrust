# Walrust Release Guide

This document explains how to release walrust to both Crates.io (Rust) and PyPI (Python).

## Pre-Release Checklist

- [ ] Verify all tests pass: `./run_tests.sh`
- [ ] Update version in `Cargo.toml` and `pyproject.toml`
- [ ] Update `CHANGELOG.md` with changes
- [ ] Commit changes with message: `chore: release v0.X.Y`
- [ ] Create git tag: `git tag v0.X.Y && git push --tags`

## Release to Crates.io (Rust/CLI)

### Manual Release

```bash
# Verify package
cargo publish --dry-run

# Publish to Crates.io
cargo publish
```

### Verify Installation

```bash
cargo install walrust --version 0.X.Y
walrust --version
```

## Release to PyPI (Python)

### Automatic Release (Recommended)

1. **Push tag** - Simply push a tag starting with `v`:
   ```bash
   git tag v0.X.Y
   git push --tags
   ```

2. **GitHub Actions** - Automatically builds wheels and publishes to PyPI
   - Workflow: `.github/workflows/pypi-release.yml`
   - Builds for: Python 3.8-3.12, Linux/macOS (Intel & Apple Silicon)
   - Publishes using PyPI Trusted Publishers

### Manual Release

```bash
# Install maturin and build
pip install maturin build
maturin build --release

# Build wheels for multiple Python versions (requires multiple interpreters)
for python in python3.8 python3.9 python3.10 python3.11 python3.12; do
  maturin build --release -i $python
done

# Build source distribution
python -m build --sdist

# Upload to TestPyPI (test first)
twine upload --repository testpypi dist/*

# Upload to PyPI
twine upload dist/*
```

### Verify Installation

```bash
# Test from PyPI
pip install walrust --upgrade
python -c "from walrust import Walrust; print('OK')"

# Test CLI
pip install walrust[cli]
walrust --version
```

## Release Contents

### Crates.io Release
- ✅ Binary: `walrust` CLI
- ✅ Library: Rust crate

### PyPI Release
- ✅ Python wheel (compiled native extension)
- ✅ Source distribution (for building locally)
- ✅ Supports Python 3.8+
- ✅ Platforms: Linux (x86_64, aarch64), macOS (Intel, Apple Silicon)

## Cargo.toml Configuration

```toml
[package]
name = "walrust"
version = "0.2.0"

# For CLI releases
[[bin]]
name = "walrust"
path = "src/main.rs"

# For library
[lib]
name = "walrust"
crate-type = ["cdylib", "rlib"]
```

## pyproject.toml Configuration

```toml
[project]
name = "walrust"
version = "0.2.0"

[build-system]
requires = ["maturin>=1.4,<2.0"]
build-backend = "maturin"

[tool.maturin]
features = ["python"]
strip = true
profile = "release"
```

## GitHub Actions Workflow

Located in `.github/workflows/pypi-release.yml`:

- **Trigger**: On tags starting with `v` or manual dispatch
- **Matrix**: Python 3.8-3.12 × Linux/macOS (Intel + Apple Silicon)
- **Output**: Wheels + source distribution
- **Publish**: Automatic to PyPI using Trusted Publishers

### Setup PyPI Trusted Publishers

1. Add Environment to GitHub:
   - Go to **Settings → Environments**
   - Create environment: `pypi`
   - Add Trusted Publisher:
     - Publisher type: GitHub Actions
     - Repository: `russellromney/personal-website`
     - Repository owner: `russellromney`
     - Workflow: `pypi-release.yml`
     - Environment: `pypi`

2. Workflow will auto-publish using OIDC token (no API key needed!)

## Version Bumping

Follow semantic versioning:

```
Major.Minor.Patch
  |      |      └─ Bug fixes (0.1.1 → 0.1.2)
  |      └──────── Features (0.1.0 → 0.2.0)
  └────────────── Breaking changes (1.0.0 → 2.0.0)
```

Update both files consistently:

```bash
# Update version
sed -i '' 's/version = "0.1.0"/version = "0.2.0"/' Cargo.toml pyproject.toml

# Verify
grep version Cargo.toml pyproject.toml | head -4
```

## Multi-Platform Wheels

The GitHub Actions workflow automatically builds:

| Platform | Architecture | Python Versions |
|----------|-------------|-----------------|
| Linux | x86_64 | 3.8-3.12 |
| Linux | aarch64 | 3.8-3.12 |
| macOS | Intel (x86_64) | 3.8-3.12 |
| macOS | Apple Silicon (aarch64) | 3.8-3.12 |

Users can install with: `pip install walrust`

## Troubleshooting

### PyPI Upload Fails
- Check that Trusted Publisher is configured
- Verify tag format: `v0.X.Y` (starts with `v`)
- Check GitHub Actions logs for wheel build errors

### Wheel Build Fails on Apple Silicon
- Ensure `macos-14` runner is used (Apple Silicon)
- Check Rust target: `rustup target list`

### Crates.io Upload Fails
- Check you're logged in: `cargo login`
- Verify `Cargo.toml` has correct metadata
- Check for name conflicts at crates.io

## Release Channels

### Stable (Recommended)
```bash
git tag v0.2.0
git push --tags
```

### Pre-release (Beta/RC)
```bash
git tag v0.2.0-rc.1
git push --tags
# Note: PyPI requires `pre` in version, adjust if needed
```

## Support Versions

Walrust supports:
- **Rust**: Edition 2021+
- **Python**: 3.8+
- **Platforms**: Linux, macOS, Windows (via WSL)

## Documentation

After release, update:
- README.md with new features
- CHANGELOG.md with version history
- GitHub releases page with release notes

---

**Current Version**: 0.3.0
- ✅ LTX format (Litestream-compatible) with SHA256 verification
- ✅ Point-in-time restore by TXID or timestamp
- ✅ GFS retention policies (grandfather/father/son compaction)
- ✅ Config file support (TOML) with per-database overrides
- ✅ Poll-based read replicas (`walrust replicate`)
- ✅ `walrust explain` - Preview configuration without running
- ✅ `walrust verify` - Verify LTX integrity in S3
- ✅ Prometheus metrics dashboard
- ✅ Multi-database support (single process)
- ✅ Python bindings
