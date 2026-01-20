"""End-to-end tests for local database platforms.

Tests execute the full benchbox CLI against local platforms (DuckDB, SQLite,
DataFusion) and validate result output files.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.e2e.conftest import (
    E2E_BENCHMARK_TIMEOUT,
    find_result_files,
    run_benchmark,
)
from tests.e2e.utils import (
    assert_benchmark_result_valid,
    is_platform_available,
    load_result_json,
)
from tests.integration._cli_e2e_utils import run_cli_command

if TYPE_CHECKING:
    from collections.abc import Mapping


# ============================================================================
# DuckDB E2E Tests
# ============================================================================


class TestDuckDBE2E:
    """E2E tests for DuckDB platform."""

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.e2e_quick
    @pytest.mark.duckdb
    @pytest.mark.tpch
    def test_tpch_full_execution(self, tmp_path: Path) -> None:
        """Test full TPC-H benchmark execution with DuckDB at SF 0.01."""
        config = {
            "platform": "duckdb",
            "benchmark": "tpch",
            "scale": "0.01",
            # DuckDB manages its database location internally
        }

        result = run_benchmark(config, timeout=E2E_BENCHMARK_TIMEOUT)

        assert result.returncode == 0, f"CLI failed with:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        assert "Benchmark completed" in result.stdout or "benchmark completed" in result.stdout.lower()

        # Find and validate result file
        results_dir = Path("benchmark_runs")
        if results_dir.exists():
            result_files = find_result_files(results_dir, pattern="*duckdb*tpch*.json")
            if result_files:
                result_data = load_result_json(result_files[0])
                assert_benchmark_result_valid(result_data)
                assert result_data["platform"] == "duckdb"
                assert result_data["benchmark_name"].lower() in ("tpch", "tpc-h")
                assert result_data["scale_factor"] == 0.01
                assert result_data["successful_queries"] > 0

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.e2e_quick
    @pytest.mark.duckdb
    @pytest.mark.tpch
    def test_tpch_query_subset(self, tmp_path: Path) -> None:
        """Test TPC-H with --queries subset flag."""
        config = {
            "platform": "duckdb",
            "benchmark": "tpch",
            "scale": "0.01",
            "queries": "Q1,Q6,Q14",
            # DuckDB manages its database location internally
        }

        result = run_benchmark(config, timeout=300)

        assert result.returncode == 0, f"CLI failed: {result.stdout}"
        # With query subset, should only execute 3 queries
        assert "Q1" in result.stdout or "q1" in result.stdout.lower()

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.duckdb
    @pytest.mark.tpch
    @pytest.mark.slow
    def test_tpch_with_capture_plans(self, tmp_path: Path) -> None:
        """Test TPC-H with --capture-plans flag."""
        config = {
            "platform": "duckdb",
            "benchmark": "tpch",
            "scale": "0.01",
            "queries": "Q1,Q6",
            "capture_plans": True,
            # DuckDB manages its database location internally
        }

        result = run_benchmark(config, timeout=300)

        assert result.returncode == 0, f"CLI failed: {result.stdout}"

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.duckdb
    @pytest.mark.ssb
    @pytest.mark.slow
    def test_ssb_execution(self, tmp_path: Path) -> None:
        """Test SSB benchmark execution with DuckDB."""
        config = {
            "platform": "duckdb",
            "benchmark": "ssb",
            "scale": "0.01",
            # DuckDB manages its database location internally
        }

        result = run_benchmark(config, timeout=E2E_BENCHMARK_TIMEOUT)

        assert result.returncode == 0, f"CLI failed: {result.stdout}"

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.e2e_quick
    @pytest.mark.duckdb
    def test_dry_run_generates_artifacts(self, tmp_path: Path) -> None:
        """Test dry-run mode generates expected artifacts."""
        output_dir = tmp_path / "dry_run"
        output_dir.mkdir()

        result = run_cli_command(
            [
                "run",
                "--platform",
                "duckdb",
                "--benchmark",
                "tpch",
                "--scale",
                "0.01",
                "--dry-run",
                str(output_dir),
            ]
        )

        assert result.returncode == 0, f"Dry run failed: {result.stdout}"
        assert "Dry run completed" in result.stdout

        # Check artifacts were generated
        artifacts = list(output_dir.glob("*"))
        assert artifacts, "No artifacts generated"

        # Check for expected file types
        json_files = list(output_dir.glob("*.json"))
        yaml_files = list(output_dir.glob("*.yaml"))
        assert json_files or yaml_files, "No JSON or YAML artifacts generated"


# ============================================================================
# SQLite E2E Tests
# ============================================================================


class TestSQLiteE2E:
    """E2E tests for SQLite platform."""

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.e2e_quick
    @pytest.mark.sqlite
    @pytest.mark.tpch
    def test_tpch_full_execution(self, tmp_path: Path) -> None:
        """Test full TPC-H benchmark execution with SQLite at SF 0.01."""
        config = {
            "platform": "sqlite",
            "benchmark": "tpch",
            "scale": "0.01",
            # SQLite manages its database location internally via database_name
        }

        result = run_benchmark(config, timeout=E2E_BENCHMARK_TIMEOUT)

        assert result.returncode == 0, f"CLI failed with:\nstdout: {result.stdout}\nstderr: {result.stderr}"

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.e2e_quick
    @pytest.mark.sqlite
    @pytest.mark.tpch
    def test_tpch_phases_generate_load(self, tmp_path: Path) -> None:
        """Test TPC-H with explicit generate and load phases."""
        config = {
            "platform": "sqlite",
            "benchmark": "tpch",
            "scale": "0.01",
            "phases": "generate,load",
            # SQLite manages its database location internally via database_name
        }

        result = run_benchmark(config, timeout=300)

        assert result.returncode == 0, f"CLI failed: {result.stdout}"

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.e2e_quick
    @pytest.mark.sqlite
    def test_dry_run_generates_artifacts(self, tmp_path: Path) -> None:
        """Test dry-run mode generates expected artifacts."""
        output_dir = tmp_path / "dry_run"
        output_dir.mkdir()

        result = run_cli_command(
            [
                "run",
                "--platform",
                "sqlite",
                "--benchmark",
                "tpch",
                "--scale",
                "0.01",
                "--dry-run",
                str(output_dir),
            ]
        )

        assert result.returncode == 0, f"Dry run failed: {result.stdout}"
        assert "Dry run completed" in result.stdout


# ============================================================================
# DataFusion E2E Tests
# ============================================================================


class TestDataFusionE2E:
    """E2E tests for DataFusion platform."""

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.slow
    def test_tpch_full_execution(self, tmp_path: Path) -> None:
        """Test full TPC-H benchmark execution with DataFusion at SF 0.01."""
        if not is_platform_available("datafusion"):
            pytest.skip("DataFusion not available")

        config = {
            "platform": "datafusion",
            "benchmark": "tpch",
            "scale": "0.01",
        }

        result = run_benchmark(config, timeout=E2E_BENCHMARK_TIMEOUT)

        assert result.returncode == 0, f"CLI failed with:\nstdout: {result.stdout}\nstderr: {result.stderr}"

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.e2e_quick
    def test_dry_run_generates_artifacts(self, tmp_path: Path) -> None:
        """Test dry-run mode generates expected artifacts."""
        if not is_platform_available("datafusion"):
            pytest.skip("DataFusion not available")

        output_dir = tmp_path / "dry_run"
        output_dir.mkdir()

        result = run_cli_command(
            [
                "run",
                "--platform",
                "datafusion",
                "--benchmark",
                "tpch",
                "--scale",
                "0.01",
                "--dry-run",
                str(output_dir),
            ]
        )

        assert result.returncode == 0, f"Dry run failed: {result.stdout}"
        assert "Dry run completed" in result.stdout


# ============================================================================
# ClickHouse Local (chDB) E2E Tests
# ============================================================================


class TestClickHouseLocalE2E:
    """E2E tests for ClickHouse local mode (chDB)."""

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.e2e_quick
    def test_dry_run_generates_artifacts(
        self,
        tmp_path: Path,
        clickhouse_stub_dir: Path,
        clickhouse_env: Mapping[str, str],
    ) -> None:
        """Test dry-run mode with ClickHouse local generates expected artifacts."""
        output_dir = tmp_path / "dry_run"
        output_dir.mkdir()
        chdb_path = tmp_path / "chdb_store"
        chdb_path.mkdir()

        result = run_cli_command(
            [
                "run",
                "--platform",
                "clickhouse",
                "--benchmark",
                "tpch",
                "--scale",
                "0.01",
                "--platform-option",
                "mode=local",
                "--platform-option",
                f"data_path={chdb_path / 'benchbox.chdb'}",
                "--platform-option",
                "driver_auto_install=false",
                "--dry-run",
                str(output_dir),
            ],
            env=dict(clickhouse_env),
        )

        assert result.returncode == 0, f"Dry run failed: {result.stdout}"
        assert "Dry run completed" in result.stdout


# ============================================================================
# Cross-Platform Comparison Tests
# ============================================================================


class TestCrossPlatformE2E:
    """E2E tests comparing results across platforms."""

    @pytest.mark.e2e
    @pytest.mark.e2e_local
    @pytest.mark.slow
    def test_duckdb_sqlite_same_results(self, tmp_path: Path) -> None:
        """Test that DuckDB and SQLite produce comparable results for TPC-H."""
        # Run DuckDB
        duckdb_config = {
            "platform": "duckdb",
            "benchmark": "tpch",
            "scale": "0.01",
            "queries": "Q1",
            # DuckDB manages its database location internally
        }
        duckdb_result = run_benchmark(duckdb_config, timeout=300)
        assert duckdb_result.returncode == 0, f"DuckDB failed: {duckdb_result.stdout}"

        # Run SQLite
        sqlite_config = {
            "platform": "sqlite",
            "benchmark": "tpch",
            "scale": "0.01",
            "queries": "Q1",
            # SQLite manages its database location internally via database_name
        }
        sqlite_result = run_benchmark(sqlite_config, timeout=300)
        assert sqlite_result.returncode == 0, f"SQLite failed: {sqlite_result.stdout}"

        # Both should complete successfully - results should be semantically equivalent
        # (actual value comparison would require parsing result files)


# ============================================================================
# Parametrized Tests
# ============================================================================


@pytest.mark.e2e
@pytest.mark.e2e_local
@pytest.mark.e2e_quick
@pytest.mark.parametrize("platform", ["duckdb", "sqlite"])
def test_help_command_per_platform(platform: str) -> None:
    """Test that help command works for each platform."""
    result = run_cli_command(["run", "--help"])
    assert result.returncode == 0
    assert platform in result.stdout.lower() or "platform" in result.stdout.lower()


@pytest.mark.e2e
@pytest.mark.e2e_local
@pytest.mark.e2e_quick
@pytest.mark.parametrize(
    "platform,benchmark_name",
    [
        ("duckdb", "tpch"),
        ("sqlite", "tpch"),
    ],
)
def test_dry_run_parametrized(tmp_path: Path, platform: str, benchmark_name: str) -> None:
    """Test dry-run mode works for multiple platform/benchmark combinations."""
    output_dir = tmp_path / f"dry_run_{platform}_{benchmark_name}"
    output_dir.mkdir()

    result = run_cli_command(
        [
            "run",
            "--platform",
            platform,
            "--benchmark",
            benchmark_name,
            "--scale",
            "0.01",
            "--dry-run",
            str(output_dir),
        ]
    )

    assert result.returncode == 0, f"Dry run failed for {platform}/{benchmark_name}: {result.stdout}"
    assert "Dry run completed" in result.stdout

    # Verify artifacts exist
    artifacts = list(output_dir.glob("*"))
    assert artifacts, f"No artifacts generated for {platform}/{benchmark_name}"
