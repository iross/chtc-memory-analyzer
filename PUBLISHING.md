# Publishing to PyPI

This guide explains how to publish `chtc-memory-analyzer` to PyPI despite having platform-specific dependencies (HTCondor).

## Solution: Optional Dependencies

HTCondor has been made an optional dependency, so:
- **Core package** works on all platforms (macOS, Windows, Linux)
- **HTCondor integration** is available as an extra: `pip install chtc-memory-analyzer[htcondor]`

## Publishing Options

### Option 1: GitHub Actions (Recommended)

The repository includes `.github/workflows/publish.yml` which builds and publishes from Linux.

**Setup:**

1. **Configure PyPI Trusted Publishing** (recommended):
   - Go to https://pypi.org/manage/account/publishing/
   - Add a new publisher:
     - PyPI Project Name: `chtc-memory-analyzer`
     - Owner: `iross`
     - Repository: `chtc-memory-analyzer`
     - Workflow: `publish.yml`
     - Environment: (leave blank)

2. **Create a GitHub Release:**
   ```bash
   git tag v0.3.0
   git push origin v0.3.0
   ```
   - Go to GitHub → Releases → Draft a new release
   - Choose the tag, add release notes, publish
   - GitHub Actions will automatically build and publish to PyPI

**Or manually trigger:**
```bash
# Go to GitHub → Actions → Publish to PyPI → Run workflow
```

### Option 2: Manual Publishing from Linux

If you have access to a Linux machine with HTCondor:

```bash
# Build the package
uv build

# Publish to PyPI
uv publish

# Or publish to TestPyPI first
uv publish --publish-url https://test.pypi.org/legacy/
```

### Option 3: Using Docker (from macOS)

Build and publish using a Linux container:

```bash
# Create a publishing container
docker run -it --rm -v $(pwd):/app -w /app python:3.12-slim bash

# Inside container:
pip install uv
uv build
uv publish
```

## Testing Before Publishing

### Test on TestPyPI

```bash
# Build
uv build

# Publish to TestPyPI
uv publish --publish-url https://test.pypi.org/legacy/

# Test install
pip install --index-url https://test.pypi.org/simple/ chtc-memory-analyzer
```

### Test Local Build

```bash
# Build locally
uv build

# Install from local wheel
pip install dist/chtc_memory_analyzer-0.3.0-py3-none-any.whl

# Test on different platforms
chtc-analyze --help
```

## Version Bumping

Before publishing, update the version in `pyproject.toml`:

```toml
[project]
version = "0.3.0"  # Update this
```

## What Gets Published

The package includes:
- ✅ Core functionality (pandas, click)
- ✅ CSV data source (works everywhere)
- ✅ Analysis modules (works everywhere)
- ✅ Visualization (works everywhere)
- ❌ HTCondor bindings (optional extra)

Users can install:
- `pip install chtc-memory-analyzer` - Core only (works on macOS/Windows)
- `pip install chtc-memory-analyzer[htcondor]` - With HTCondor (Linux only)

## Troubleshooting

### "No matching distribution found for htcondor"

This is expected on macOS/Windows. The package will still install without HTCondor:
```bash
pip install chtc-memory-analyzer  # Without [htcondor] extra
```

### Testing HTCondor integration without Linux

Use the CSV workflow:
1. On Linux: `chtc-analyze --cache-csv jobs.csv`
2. Transfer `jobs.csv` to macOS
3. On macOS: `chtc-analyze --csv jobs.csv`
