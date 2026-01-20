"""Unit tests for DataFrame benchmark runner.

Tests for:
- DataFrame execution mode detection
- DataFramePhases and DataFrameRunOptions
- run_dataframe_benchmark function
- Mode coexistence (SQL vs DataFrame)

Copyright 2026 Joe Harris / BenchBox Project
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from benchbox.core.runner.dataframe_runner import (
    DataFramePhases,
    DataFrameRunOptions,
    get_execution_mode,
    is_dataframe_execution,
    run_dataframe_benchmark,
)

pytestmark = pytest.mark.fast


class TestIsDataFrameExecution:
    """Tests for is_dataframe_execution function."""

    def test_polars_df_is_dataframe(self):
        """Test polars-df is detected as DataFrame mode."""
        config = MagicMock()
        config.type = "polars-df"

        assert is_dataframe_execution(config) is True

    def test_pandas_df_is_dataframe(self):
        """Test pandas-df is detected as DataFrame mode."""
        config = MagicMock()
        config.type = "pandas-df"

        assert is_dataframe_execution(config) is True

    def test_duckdb_is_sql(self):
        """Test duckdb is detected as SQL mode."""
        config = MagicMock()
        config.type = "duckdb"

        assert is_dataframe_execution(config) is False

    def test_polars_without_df_is_sql(self):
        """Test polars (without -df) is SQL mode."""
        config = MagicMock()
        config.type = "polars"

        assert is_dataframe_execution(config) is False

    def test_none_config(self):
        """Test None config returns False."""
        assert is_dataframe_execution(None) is False

    def test_case_insensitive(self):
        """Test case insensitive detection."""
        config = MagicMock()
        config.type = "Polars-DF"

        assert is_dataframe_execution(config) is True


class TestGetExecutionMode:
    """Tests for get_execution_mode function."""

    def test_dataframe_mode(self):
        """Test DataFrame platform returns 'dataframe'."""
        config = MagicMock()
        config.type = "polars-df"

        assert get_execution_mode(config) == "dataframe"

    def test_sql_mode(self):
        """Test SQL platform returns 'sql'."""
        config = MagicMock()
        config.type = "duckdb"

        assert get_execution_mode(config) == "sql"

    def test_none_returns_sql(self):
        """Test None config returns 'sql'."""
        assert get_execution_mode(None) == "sql"


class TestDataFramePhases:
    """Tests for DataFramePhases dataclass."""

    def test_default_values(self):
        """Test default phase values."""
        phases = DataFramePhases()

        assert phases.load is True
        assert phases.execute is True

    def test_custom_values(self):
        """Test custom phase values."""
        phases = DataFramePhases(load=False, execute=True)

        assert phases.load is False
        assert phases.execute is True


class TestDataFrameRunOptions:
    """Tests for DataFrameRunOptions dataclass."""

    def test_default_values(self):
        """Test default option values."""
        options = DataFrameRunOptions()

        assert options.ignore_memory_warnings is False
        assert options.force_regenerate is False
        assert options.prefer_parquet is True
        assert options.cache_dir is None

    def test_custom_values(self):
        """Test custom option values."""
        options = DataFrameRunOptions(
            ignore_memory_warnings=True,
            force_regenerate=True,
            prefer_parquet=False,
            cache_dir=Path("/tmp/cache"),
        )

        assert options.ignore_memory_warnings is True
        assert options.force_regenerate is True
        assert options.prefer_parquet is False
        assert options.cache_dir == Path("/tmp/cache")


class TestRunDataframeBenchmark:
    """Tests for run_dataframe_benchmark function."""

    def test_returns_benchmark_results(self):
        """Test that function returns BenchmarkResults."""
        from benchbox.core.results.models import BenchmarkResults

        # Mock adapter
        adapter = MagicMock()
        adapter.platform_name = "Polars"
        adapter.create_context.return_value = MagicMock()
        adapter.get_platform_info.return_value = {"platform": "Polars"}

        # Mock config
        config = MagicMock()
        config.name = "tpch"
        config.display_name = "TPC-H"
        config.scale_factor = 0.01

        # Execute with load=False, execute=False (minimal execution)
        phases = DataFramePhases(load=False, execute=False)

        result = run_dataframe_benchmark(
            benchmark_config=config,
            adapter=adapter,
            phases=phases,
        )

        assert isinstance(result, BenchmarkResults)
        assert result.benchmark_name == "TPC-H"
        assert "Polars" in result.platform
        assert "DataFrame" in result.platform

    def test_sets_execution_metadata(self):
        """Test that execution_metadata includes mode info."""
        adapter = MagicMock()
        adapter.platform_name = "Pandas"
        adapter.create_context.return_value = MagicMock()
        adapter.get_platform_info.return_value = {"platform": "Pandas"}

        config = MagicMock()
        config.name = "tpch"
        config.display_name = "TPC-H"
        config.scale_factor = 0.01

        phases = DataFramePhases(load=False, execute=False)

        result = run_dataframe_benchmark(
            benchmark_config=config,
            adapter=adapter,
            phases=phases,
        )

        assert result.execution_metadata is not None
        assert result.execution_metadata["execution_mode"] == "dataframe"
        assert result.execution_metadata["dataframe_platform"] == "Pandas"

    def test_handles_load_failure(self):
        """Test graceful handling of load failure."""
        adapter = MagicMock()
        adapter.platform_name = "Polars"
        adapter.create_context.return_value = MagicMock()
        adapter.get_platform_info.return_value = {}

        config = MagicMock()
        config.name = "tpch"
        config.display_name = "TPC-H"
        config.scale_factor = 0.01

        # Load enabled but no data available
        phases = DataFramePhases(load=True, execute=False)

        result = run_dataframe_benchmark(
            benchmark_config=config,
            adapter=adapter,
            phases=phases,
            data_dir=None,  # No data directory
            benchmark_instance=None,  # No benchmark tables
        )

        # Should return failed result, not raise exception
        assert result.validation_status == "FAILED"
        assert result.validation_details is not None
        assert "error" in result.validation_details


class TestModeCoexistence:
    """Tests for SQL and DataFrame mode coexistence."""

    def test_same_benchmark_different_modes(self):
        """Test same benchmark can be identified for different modes."""
        sql_config = MagicMock()
        sql_config.type = "duckdb"

        df_config = MagicMock()
        df_config.type = "polars-df"

        # Same benchmark name
        sql_mode = get_execution_mode(sql_config)
        df_mode = get_execution_mode(df_config)

        assert sql_mode == "sql"
        assert df_mode == "dataframe"
        assert sql_mode != df_mode

    def test_platform_naming_convention(self):
        """Test platform naming convention for mode detection."""
        platforms = {
            "duckdb": "sql",
            "sqlite": "sql",
            "datafusion": "sql",
            "polars": "sql",  # Without -df suffix = SQL mode
            "polars-df": "dataframe",
            "pandas-df": "dataframe",
            "snowflake": "sql",
            "bigquery": "sql",
        }

        for platform, expected_mode in platforms.items():
            config = MagicMock()
            config.type = platform
            actual_mode = get_execution_mode(config)
            assert actual_mode == expected_mode, f"Platform {platform} expected {expected_mode}, got {actual_mode}"


class TestResultSchemaCompatibility:
    """Tests for result schema compatibility between modes."""

    def test_dataframe_result_has_required_fields(self):
        """Test DataFrame results have all required BenchmarkResults fields."""

        adapter = MagicMock()
        adapter.platform_name = "Polars"
        adapter.create_context.return_value = MagicMock()
        adapter.get_platform_info.return_value = {}

        config = MagicMock()
        config.name = "tpch"
        config.display_name = "TPC-H"
        config.scale_factor = 0.01

        phases = DataFramePhases(load=False, execute=False)

        result = run_dataframe_benchmark(
            benchmark_config=config,
            adapter=adapter,
            phases=phases,
        )

        # Check all required BenchmarkResults fields are present
        assert hasattr(result, "benchmark_name")
        assert hasattr(result, "platform")
        assert hasattr(result, "scale_factor")
        assert hasattr(result, "execution_id")
        assert hasattr(result, "timestamp")
        assert hasattr(result, "duration_seconds")
        assert hasattr(result, "total_queries")
        assert hasattr(result, "successful_queries")
        assert hasattr(result, "failed_queries")
        assert hasattr(result, "query_results")
        assert hasattr(result, "validation_status")

    def test_execution_id_format(self):
        """Test execution ID has expected format."""
        adapter = MagicMock()
        adapter.platform_name = "Polars"
        adapter.create_context.return_value = MagicMock()
        adapter.get_platform_info.return_value = {}

        config = MagicMock()
        config.name = "tpch"
        config.display_name = "TPC-H"
        config.scale_factor = 0.01

        phases = DataFramePhases(load=False, execute=False)

        result = run_dataframe_benchmark(
            benchmark_config=config,
            adapter=adapter,
            phases=phases,
        )

        # DataFrame execution IDs are 8-character UUID hex strings
        # (df_ prefix was removed to fix filename redundancy)
        assert len(result.execution_id) == 8
        assert all(c in "0123456789abcdef" for c in result.execution_id)
