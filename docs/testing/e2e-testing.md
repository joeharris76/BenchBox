<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# End-to-End (E2E) Testing Guide

```{tags} contributor, guide, testing, e2e
```

This guide covers BenchBox's end-to-end test suite, which validates complete benchmark workflows through the CLI interface.

## Overview

The E2E test suite (`tests/e2e/`) validates that BenchBox works correctly as an integrated system. Unlike unit tests that test individual components or integration tests that test component interactions, E2E tests exercise the full CLI workflow from command invocation through result generation.

**Test Count**: 125+ test functions across 6 test modules

## Test Structure

```
tests/e2e/
├── conftest.py                 # Fixtures and helpers
├── test_cli_options.py         # CLI option validation (27 tests)
├── test_error_handling.py      # Error handling coverage (25 tests)
├── test_result_validation.py   # Result schema validation (28 tests)
├── test_local_platforms.py     # Local platform tests (14 tests)
├── test_cloud_platforms.py     # Cloud platform dry-run tests (15 tests)
├── test_dataframe_platforms.py # DataFrame platform tests (16 tests)
└── utils/
    ├── platform_detection.py   # Platform availability detection
    └── result_validators.py    # Result JSON validation
```

## Quick Start

### Run All E2E Tests

```bash
# All E2E tests
uv run -- python -m pytest tests/e2e/ -v

# With parallel execution
uv run -- python -m pytest tests/e2e/ -n auto
```

### Run Quick E2E Tests (Dry-Run Mode)

```bash
# Quick tests using dry-run mode (no actual benchmark execution)
make test-e2e-quick
# or
uv run -- python -m pytest -m e2e_quick
```

### Run Local Platform Tests

```bash
# Full execution against local databases
uv run -- python -m pytest -m e2e_local

# Specific platform
uv run -- python -m pytest tests/e2e/test_local_platforms.py -v
```

## Test Categories

### CLI Options Tests (`test_cli_options.py`)

Validates all CLI options work correctly:

| Option | Tests |
|--------|-------|
| `--benchmark` | TPC-H, TPC-DS, SSB, ClickBench, H2ODB selection |
| `--scale` | Scale factor validation and constraints |
| `--phases` | Phase selection (generate, load, power, throughput) |
| `--queries` | Query subset selection (Q1,Q6,Q17 format) |
| `--tuning` | Tuning modes (tuned, notuning, auto) |
| `--compression` | Compression formats (zstd, gzip, none) |
| `--validation` | Validation levels (exact, loose, range, disabled) |
| `--seed` | Deterministic seeding |
| `--dry-run` | Dry-run mode output |

```bash
# Run CLI option tests
uv run -- python -m pytest tests/e2e/test_cli_options.py -v

# Test specific option
uv run -- python -m pytest tests/e2e/test_cli_options.py -k "scale" -v
```

### Error Handling Tests (`test_error_handling.py`)

Validates proper error messages and exit codes:

- Missing required parameters
- Invalid platform/benchmark names
- Invalid scale factors (TPC-DS requires scale ≥ 1)
- Query format violations (max 100, alphanumeric IDs)
- Phase validation errors
- Compression format errors
- Constraint violations

```bash
# Run error handling tests
uv run -- python -m pytest tests/e2e/test_error_handling.py -v
```

### Result Validation Tests (`test_result_validation.py`)

Validates benchmark output files:

- JSON schema compliance
- Required fields present (platform, benchmark, scale_factor)
- Metrics validation (successful_queries, execution_times)
- Result file discovery and parsing
- Manifest file validation

```bash
# Run result validation tests
uv run -- python -m pytest tests/e2e/test_result_validation.py -v
```

### Local Platform Tests (`test_local_platforms.py`)

Full benchmark execution against local databases:

| Platform | Tests |
|----------|-------|
| DuckDB | Full TPC-H execution, query subsets |
| SQLite | Full TPC-H execution |
| DataFusion | Full TPC-H execution |

```bash
# Run local platform tests
uv run -- python -m pytest tests/e2e/test_local_platforms.py -v

# Run DuckDB tests only
uv run -- python -m pytest tests/e2e/test_local_platforms.py -k duckdb -v
```

**Note**: Local platform tests execute actual benchmarks and may take several minutes.

### Cloud Platform Tests (`test_cloud_platforms.py`)

Dry-run tests for cloud platforms (no credentials required):

| Platform | Tests |
|----------|-------|
| Snowflake | Dry-run validation, query generation |
| BigQuery | Dry-run validation, query generation |
| Redshift | Dry-run validation, query generation |
| Athena | Dry-run validation, query generation |
| Databricks | Dry-run validation, query generation |

```bash
# Run cloud platform dry-run tests
uv run -- python -m pytest tests/e2e/test_cloud_platforms.py -v
```

### DataFrame Platform Tests (`test_dataframe_platforms.py`)

Tests for DataFrame-based platforms:

| Platform | Tests |
|----------|-------|
| Polars | Full TPC-H execution |
| Pandas | Full TPC-H execution |
| Dask | Full TPC-H execution |

```bash
# Run DataFrame platform tests
uv run -- python -m pytest tests/e2e/test_dataframe_platforms.py -v
```

## Test Markers

E2E tests use several pytest markers for selective execution:

| Marker | Description |
|--------|-------------|
| `e2e` | All E2E tests |
| `e2e_quick` | Quick dry-run tests |
| `e2e_local` | Local platform tests (full execution) |
| `e2e_cloud` | Cloud platform dry-run tests |
| `e2e_dataframe` | DataFrame platform tests |

```bash
# Examples
uv run -- python -m pytest -m e2e_quick          # Quick tests only
uv run -- python -m pytest -m e2e_local          # Local platforms
uv run -- python -m pytest -m "e2e and not slow" # Fast E2E tests
```

## Fixtures

The E2E test suite provides fixtures in `conftest.py`:

### Directory Fixtures

```python
@pytest.fixture
def results_dir(tmp_path):
    """Temporary directory for benchmark results."""

@pytest.fixture
def dry_run_dir(tmp_path):
    """Temporary directory for dry-run output."""
```

### Platform Configuration Fixtures

```python
@pytest.fixture
def duckdb_config():
    """Default DuckDB configuration."""
    return {"platform": "duckdb", "benchmark": "tpch", "scale": "0.01"}

@pytest.fixture
def snowflake_dry_run_config(dry_run_dir):
    """Snowflake dry-run configuration."""
    return {"platform": "snowflake", "benchmark": "tpch", "dry_run": str(dry_run_dir)}
```

### Helper Functions

```python
def build_cli_args(config, extra_args=None):
    """Build CLI arguments from config dictionary."""

def run_benchmark(config, extra_args=None, env=None, timeout=600):
    """Run a benchmark with configuration."""

def find_result_files(directory, pattern="*.json"):
    """Find result files in directory."""
```

## Writing E2E Tests

### Basic Test Pattern

```python
import pytest
from tests.e2e.conftest import build_cli_args, run_benchmark

def test_basic_benchmark(duckdb_config, results_dir):
    """Test basic benchmark execution."""
    config = {**duckdb_config, "output": str(results_dir)}

    result = run_benchmark(config)

    assert result.returncode == 0
    assert "Benchmark completed" in result.stdout
```

### Testing Error Conditions

```python
def test_invalid_scale_factor():
    """Test that invalid scale factors produce clear errors."""
    config = {"platform": "duckdb", "benchmark": "tpcds", "scale": "0.1"}

    result = run_benchmark(config)

    assert result.returncode != 0
    assert "scale factor" in result.stderr.lower()
```

### Testing Result Files

```python
import json
from tests.e2e.conftest import find_result_files

def test_result_file_schema(duckdb_config, results_dir):
    """Test that result files match expected schema."""
    config = {**duckdb_config, "output": str(results_dir)}
    run_benchmark(config)

    result_files = find_result_files(results_dir / "results")
    assert len(result_files) > 0

    with open(result_files[0]) as f:
        data = json.load(f)

    assert "platform" in data
    assert "benchmark" in data
    assert "successful_queries" in data
```

## CI/CD Integration

### GitHub Actions

```yaml
jobs:
  e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -e .[dev]

      - name: Run E2E quick tests
        run: uv run -- python -m pytest -m e2e_quick -v

      - name: Run E2E local tests
        run: uv run -- python -m pytest -m e2e_local -v
        timeout-minutes: 15
```

### Makefile Targets

```makefile
test-e2e-quick:
	uv run -- python -m pytest -m e2e_quick -v

test-e2e-local:
	uv run -- python -m pytest -m e2e_local -v

test-e2e-all:
	uv run -- python -m pytest tests/e2e/ -v
```

## Troubleshooting

### Tests Timing Out

E2E tests have a 10-minute timeout by default. For slower systems:

```bash
# Increase timeout
uv run -- python -m pytest tests/e2e/ --timeout=900
```

### ClickHouse Tests Without Dependencies

The test suite includes automatic stub generation for ClickHouse dependencies:

```python
@pytest.fixture
def clickhouse_stub_dir(tmp_path):
    """Create minimal chDB and clickhouse_driver stubs."""
    # Stubs are created automatically for dry-run tests
```

### Debugging Test Failures

```bash
# Run with verbose output
uv run -- python -m pytest tests/e2e/test_cli_options.py -v -s

# Run single test with debugging
uv run -- python -m pytest tests/e2e/test_cli_options.py::test_benchmark_selection -v -s --tb=long
```

### Result File Inspection

```bash
# Find generated result files
find /tmp -name "*.json" -path "*/benchmark_results/*" 2>/dev/null

# Validate JSON schema
python -c "import json; json.load(open('result.json'))"
```

## Related Documentation

- [Testing Index](index.md) - Overview of all test categories
- [Live Integration Tests](live-integration-tests.md) - Tests requiring cloud credentials
- [Development Testing Guide](../development/testing.md) - General testing guidance
- [CI/CD Integration](../advanced/ci-cd-integration.md) - Automated workflows
