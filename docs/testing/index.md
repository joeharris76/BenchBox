<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# Testing Documentation

```{tags} contributor, testing
```

Documentation for testing BenchBox functionality, including live integration tests.

## Test Documentation

- [Live Integration Tests](live-integration-tests.md) - Running integration tests against live database platforms

## Test Categories

### Unit Tests
Fast, isolated tests for individual components without external dependencies.

```bash
make test-unit
# or
uv run -- python -m pytest -m unit
```

### Integration Tests
Tests that verify interaction between components, may use embedded databases.

```bash
make test-integration
# or
uv run -- python -m pytest -m "integration and not live_integration"
```

### E2E Tests
End-to-end tests that validate complete benchmark workflows through the CLI.

```bash
# Quick E2E tests (dry-run mode)
make test-e2e-quick
# or
uv run -- python -m pytest -m e2e_quick

# Local platform E2E tests (full execution)
uv run -- python -m pytest -m e2e_local

# All E2E tests
uv run -- python -m pytest tests/e2e/
```

E2E tests cover:
- CLI option validation (`--benchmark`, `--scale`, `--phases`, `--queries`, etc.)
- Error handling for invalid parameters
- Result file validation and schema compliance
- Local platforms (DuckDB, SQLite, DataFusion)
- Cloud platforms (dry-run mode for Snowflake, BigQuery, etc.)
- DataFrame platforms (Polars, Pandas, Dask)

See [E2E Testing Guide](e2e-testing.md) for detailed information.

### Live Integration Tests
Tests that require live database credentials and cloud platforms.

```bash
make test-live
# or
uv run -- python -m pytest -m live_integration
```

See [Live Integration Tests](live-integration-tests.md) for detailed setup instructions.

## Related Documentation

- [Development Guide](../development/development.md) - Development environment setup
- [Testing Guide](../development/testing.md) - Test organization and strategies
- [CI/CD Integration](../advanced/ci-cd-integration.md) - Automated testing workflows

```{toctree}
:maxdepth: 1
:caption: Testing Guides
:hidden:

e2e-testing
live-integration-tests
```
