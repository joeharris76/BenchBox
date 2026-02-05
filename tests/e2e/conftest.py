"""Pytest fixtures for E2E CLI tests."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping, Sequence

# Re-export CLI utilities from integration tests
from tests.integration._cli_e2e_utils import run_cli_command

# Longer timeout for full benchmark runs
E2E_BENCHMARK_TIMEOUT = 600.0  # 10 minutes for full benchmark execution


@pytest.fixture
def results_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for benchmark results."""
    results = tmp_path / "benchmark_results"
    results.mkdir()
    return results


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for generated data."""
    data = tmp_path / "benchmark_data"
    data.mkdir()
    return data


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for CLI output."""
    output = tmp_path / "cli_output"
    output.mkdir()
    return output


@pytest.fixture
def dry_run_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for dry-run output."""
    dry_run = tmp_path / "dry_run"
    dry_run.mkdir()
    return dry_run


@pytest.fixture
def cleanup_results(results_dir: Path) -> Generator[Path, None, None]:
    """Fixture that cleans up results directory after test."""
    yield results_dir
    if results_dir.exists():
        shutil.rmtree(results_dir)


# ============================================================================
# Platform-specific fixtures
# ============================================================================


@pytest.fixture
def duckdb_config() -> dict[str, Any]:
    """Default configuration for DuckDB platform tests."""
    return {
        "platform": "duckdb",
        "benchmark": "tpch",
        "scale": "0.01",
    }


@pytest.fixture
def sqlite_config() -> dict[str, Any]:
    """Default configuration for SQLite platform tests."""
    return {
        "platform": "sqlite",
        "benchmark": "tpch",
        "scale": "0.01",
    }


@pytest.fixture
def datafusion_config() -> dict[str, Any]:
    """Default configuration for DataFusion platform tests."""
    return {
        "platform": "datafusion",
        "benchmark": "tpch",
        "scale": "0.01",
    }


@pytest.fixture
def clickhouse_stub_dir(tmp_path: Path) -> Path:
    """Create minimal chDB and clickhouse_driver stubs for subprocess imports."""
    stub_root = tmp_path / "clickhouse_stubs"

    # Create chDB stub
    chdb_package_dir = stub_root / "chdb"
    session_dir = chdb_package_dir / "session"
    session_dir.mkdir(parents=True, exist_ok=True)

    (chdb_package_dir / "__init__.py").write_text(
        "chdb_version = (0, 10, 0)\n"
        "class _StubConnection:\n"
        "    def query(self, _sql, format='CSV'):\n"
        "        return ''\n"
        "    def close(self):\n"
        "        pass\n"
        "\n"
        "def connect():\n"
        "    return _StubConnection()\n"
        "\n"
        "from .session import Session\n",
        encoding="utf-8",
    )

    (session_dir / "__init__.py").write_text(
        "class Session:\n"
        "    def __init__(self, path=None):\n"
        "        self.path = path\n"
        "    def query(self, _sql, format='CSV'):\n"
        "        return ''\n"
        "    def close(self):\n"
        "        pass\n",
        encoding="utf-8",
    )

    driver_dir = stub_root / "clickhouse_driver"
    driver_dir.mkdir(parents=True, exist_ok=True)
    (driver_dir / "__init__.py").write_text(
        "__version__ = '0.0.0'\n"
        "class Client:\n"
        "    def __init__(self, *args, **kwargs):\n"
        "        self.args = args\n"
        "        self.kwargs = kwargs\n"
        "    def execute(self, _sql):\n"
        "        return []\n"
        "\n"
        "from . import errors\n",
        encoding="utf-8",
    )
    (driver_dir / "errors.py").write_text(
        "class Error(Exception):\n    pass\n",
        encoding="utf-8",
    )

    return stub_root


@pytest.fixture
def clickhouse_env(clickhouse_stub_dir: Path) -> dict[str, str]:
    """Environment variables for ClickHouse stub imports."""
    existing = os.environ.get("PYTHONPATH", "")
    return {"PYTHONPATH": f"{clickhouse_stub_dir}{os.pathsep}{existing}" if existing else str(clickhouse_stub_dir)}


# ============================================================================
# DataFrame platform fixtures
# ============================================================================


@pytest.fixture
def pandas_df_config() -> dict[str, Any]:
    """Default configuration for Pandas DataFrame platform tests."""
    return {
        "platform": "pandas-df",
        "benchmark": "tpch",
        "scale": "0.01",
    }


@pytest.fixture
def polars_df_config() -> dict[str, Any]:
    """Default configuration for Polars DataFrame platform tests."""
    return {
        "platform": "polars-df",
        "benchmark": "tpch",
        "scale": "0.01",
    }


@pytest.fixture
def dask_df_config() -> dict[str, Any]:
    """Default configuration for Dask DataFrame platform tests."""
    return {
        "platform": "dask-df",
        "benchmark": "tpch",
        "scale": "0.01",
    }


# ============================================================================
# Cloud platform fixtures (for dry-run testing)
# ============================================================================


@pytest.fixture
def snowflake_dry_run_config(dry_run_dir: Path) -> dict[str, Any]:
    """Configuration for Snowflake dry-run tests."""
    return {
        "platform": "snowflake",
        "benchmark": "tpch",
        "scale": "0.01",
        "dry_run": str(dry_run_dir),
    }


@pytest.fixture
def bigquery_dry_run_config(dry_run_dir: Path) -> dict[str, Any]:
    """Configuration for BigQuery dry-run tests."""
    return {
        "platform": "bigquery",
        "benchmark": "tpch",
        "scale": "0.01",
        "dry_run": str(dry_run_dir),
    }


@pytest.fixture
def redshift_dry_run_config(dry_run_dir: Path) -> dict[str, Any]:
    """Configuration for Redshift dry-run tests."""
    return {
        "platform": "redshift",
        "benchmark": "tpch",
        "scale": "0.01",
        "dry_run": str(dry_run_dir),
    }


@pytest.fixture
def athena_dry_run_config(dry_run_dir: Path) -> dict[str, Any]:
    """Configuration for Athena dry-run tests."""
    return {
        "platform": "athena",
        "benchmark": "tpch",
        "scale": "0.01",
        "dry_run": str(dry_run_dir),
    }


@pytest.fixture
def databricks_dry_run_config(dry_run_dir: Path) -> dict[str, Any]:
    """Configuration for Databricks dry-run tests."""
    return {
        "platform": "databricks",
        "benchmark": "tpch",
        "scale": "0.01",
        "dry_run": str(dry_run_dir),
    }


# ============================================================================
# CLI runner helpers
# ============================================================================


def build_cli_args(
    config: dict[str, Any],
    *,
    extra_args: Sequence[str] | None = None,
) -> list[str]:
    """Build CLI arguments from a config dictionary.

    Args:
        config: Configuration dictionary with platform, benchmark, scale, etc.
        extra_args: Additional CLI arguments to append

    Returns:
        List of CLI arguments
    """
    args = ["run"]

    if "platform" in config:
        args.extend(["--platform", config["platform"]])
    if "benchmark" in config:
        args.extend(["--benchmark", config["benchmark"]])
    if "scale" in config:
        args.extend(["--scale", str(config["scale"])])
    if "phases" in config:
        args.extend(["--phases", config["phases"]])
    if "queries" in config:
        args.extend(["--queries", config["queries"]])
    if "dry_run" in config:
        args.extend(["--dry-run", config["dry_run"]])
    if "tuning" in config:
        args.extend(["--tuning", config["tuning"]])
    if "compression" in config:
        args.extend(["--compression", config["compression"]])
    if "validation" in config:
        args.extend(["--validation", config["validation"]])
    if "seed" in config:
        args.extend(["--seed", str(config["seed"])])
    if "force" in config:
        args.extend(["--force", config["force"]])
    if config.get("capture_plans"):
        args.append("--capture-plans")

    # Add platform options
    for key, value in config.get("platform_options", {}).items():
        args.extend(["--platform-option", f"{key}={value}"])

    if extra_args:
        args.extend(extra_args)

    return args


def run_benchmark(
    config: dict[str, Any],
    *,
    extra_args: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float = E2E_BENCHMARK_TIMEOUT,
) -> subprocess.CompletedProcess[str]:
    """Run a benchmark with the given configuration.

    Args:
        config: Configuration dictionary
        extra_args: Additional CLI arguments
        env: Environment variables
        timeout: Command timeout in seconds

    Returns:
        CompletedProcess with stdout, stderr, returncode
    """

    args = build_cli_args(config, extra_args=extra_args)
    return run_cli_command(args, env=env, timeout=timeout)


# ============================================================================
# Result file helpers
# ============================================================================


def find_result_files(
    directory: Path,
    *,
    pattern: str = "*.json",
) -> list[Path]:
    """Find result files in a directory.

    Args:
        directory: Directory to search
        pattern: Glob pattern for files

    Returns:
        List of matching file paths sorted by modification time (newest first)
    """
    files = list(directory.glob(pattern))
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def find_latest_result(
    directory: Path,
    *,
    pattern: str = "*.json",
) -> Path | None:
    """Find the most recent result file in a directory.

    Args:
        directory: Directory to search
        pattern: Glob pattern for files

    Returns:
        Path to newest file, or None if no files found
    """
    files = find_result_files(directory, pattern=pattern)
    return files[0] if files else None


# Make imports available at module level
import subprocess  # noqa: E402
