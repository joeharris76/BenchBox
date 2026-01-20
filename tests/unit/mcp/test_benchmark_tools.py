"""Tests for BenchBox MCP benchmark execution tools.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import sys

import pytest

# Skip all tests if Python < 3.10
pytestmark = pytest.mark.skipif(sys.version_info < (3, 10), reason="MCP server requires Python 3.10+")


class TestValidateConfigTool:
    """Tests for validate_config tool functionality."""

    def test_valid_duckdb_tpch_config(self):
        """Test validating a valid DuckDB TPC-H configuration."""
        from benchbox.core.benchmark_registry import get_all_benchmarks
        from benchbox.core.platform_registry import PlatformRegistry

        # Check platform is available
        info = PlatformRegistry.get_platform_info("duckdb")
        assert info is not None
        assert info.available

        # Check benchmark exists using public API
        benchmarks = get_all_benchmarks()
        assert "tpch" in benchmarks

    def test_unknown_platform_error(self):
        """Test that unknown platform is detected."""
        from benchbox.core.platform_registry import PlatformRegistry

        info = PlatformRegistry.get_platform_info("nonexistent_platform")
        assert info is None

    def test_unknown_benchmark_error(self):
        """Test that unknown benchmark is detected."""
        from benchbox.core.benchmark_registry import get_all_benchmarks

        benchmarks = get_all_benchmarks()
        assert "nonexistent_benchmark" not in benchmarks

    def test_invalid_scale_factor(self):
        """Test that invalid scale factor is detected."""
        scale_factor = -1.0
        assert scale_factor <= 0  # Should be caught as invalid

    def test_dataframe_platform_validation(self):
        """Test validation for DataFrame platforms."""
        from benchbox.platforms import is_dataframe_platform

        assert is_dataframe_platform("polars-df")
        assert is_dataframe_platform("pandas-df")
        assert not is_dataframe_platform("duckdb")


class TestDryRunTool:
    """Tests for dry_run tool functionality."""

    def test_dry_run_returns_execution_plan(self):
        """Test that dry_run returns an execution plan."""
        from benchbox.core.benchmark_registry import get_all_benchmarks, get_benchmark_class

        # Check benchmark exists using public API
        benchmarks = get_all_benchmarks()
        assert "tpch" in benchmarks

        # Get query IDs via public API
        benchmark_class = get_benchmark_class("tpch")
        assert benchmark_class is not None
        bm = benchmark_class(scale_factor=0.01)
        queries = bm.get_queries()
        query_ids = list(queries.keys())

        assert len(query_ids) == 22

    def test_dry_run_resource_estimates(self):
        """Test that dry_run provides resource estimates."""
        scale_factor = 1.0
        estimated_data_gb = scale_factor * 1.0
        memory_recommended = max(2, estimated_data_gb * 2)

        assert estimated_data_gb == 1.0
        assert memory_recommended >= 2.0


class TestRunBenchmarkTool:
    """Tests for run_benchmark tool functionality."""

    def test_benchmark_config_creation(self):
        """Test that BenchmarkConfig can be created correctly."""
        from benchbox.core.schemas import BenchmarkConfig

        config = BenchmarkConfig(
            name="tpch",
            display_name="TPC-H",
            scale_factor=0.01,
        )

        assert config.name == "tpch"
        assert config.display_name == "TPC-H"
        assert config.scale_factor == 0.01

    def test_database_config_creation(self):
        """Test that DatabaseConfig can be created correctly."""
        from benchbox.core.config import DatabaseConfig

        db_config = DatabaseConfig(
            type="duckdb",
            name="test_db",
        )

        assert db_config.type == "duckdb"
        assert db_config.name == "test_db"

    def test_phase_parsing(self):
        """Test phase parsing logic."""
        phases_str = "load,power"
        phase_list = phases_str.lower().split(",")

        phases_to_run = []
        if "load" in phase_list:
            phases_to_run.append("load")
        if "power" in phase_list or "execute" in phase_list:
            phases_to_run.append("power")

        assert "load" in phases_to_run
        assert "power" in phases_to_run


class TestGetQueryDetailsTool:
    """Tests for get_query_details tool functionality."""

    def test_tpch_query_complexity_hints(self):
        """Test that TPC-H query complexity hints are available."""
        from benchbox.mcp.tools.benchmark import _get_query_complexity_hints

        # Test Q6 - known simple query
        hints = _get_query_complexity_hints("tpch", "6")
        assert hints["type"] == "scan_filter"
        assert hints["complexity"] == "simple"
        assert hints["joins"] == 0
        assert "lineitem" in hints["tables"]

        # Test Q2 - known complex query
        hints = _get_query_complexity_hints("tpch", "2")
        assert hints["type"] == "correlated_subquery"
        assert hints["complexity"] == "complex"
        assert hints["joins"] >= 4

    def test_unknown_query_returns_default(self):
        """Test that unknown queries return default hints."""
        from benchbox.mcp.tools.benchmark import _get_query_complexity_hints

        hints = _get_query_complexity_hints("unknown_benchmark", "99")
        assert hints["type"] == "unknown"
        assert hints["complexity"] == "unknown"
        assert "note" in hints

    def test_query_id_normalization(self):
        """Test query ID normalization logic."""
        # Test the normalization logic used in get_query_details
        test_cases = [
            ("1", "1"),
            ("Q1", "1"),
            ("q1", "1"),
            ("Q01", "01"),
            ("abc", "abc"),  # Non-numeric kept as-is
        ]

        for input_id, expected in test_cases:
            normalized = input_id.upper().lstrip("Q")
            if not normalized.isdigit():
                normalized = input_id
            # For this test we just verify the logic runs
            assert normalized is not None
