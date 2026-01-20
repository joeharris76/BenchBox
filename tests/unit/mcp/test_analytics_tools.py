"""Tests for BenchBox MCP analytics tools.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Skip all tests if Python < 3.10
pytestmark = pytest.mark.skipif(sys.version_info < (3, 10), reason="MCP server requires Python 3.10+")


class TestGetQueryPlanTool:
    """Tests for get_query_plan tool functionality."""

    def test_invalid_format_returns_error(self):
        """Test that invalid format parameter returns validation error."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.analytics import register_analytics_tools

        mcp = FastMCP("test")
        register_analytics_tools(mcp)

        # Get the registered tool function
        tools = list(mcp._tool_manager._tools.values())
        get_query_plan = next(t for t in tools if t.name == "get_query_plan")

        result = get_query_plan.fn(result_file="test.json", query_id="1", format="invalid")

        assert "error" in result
        assert result["error_code"] == "VALIDATION_INVALID_FORMAT"

    def test_valid_formats_accepted(self):
        """Test that valid format parameters are accepted."""
        valid_formats = ["tree", "json", "summary"]
        for fmt in valid_formats:
            # Just validate format is not rejected (file won't exist)
            from mcp.server.fastmcp import FastMCP

            from benchbox.mcp.tools.analytics import register_analytics_tools

            mcp = FastMCP("test")
            register_analytics_tools(mcp)

            tools = list(mcp._tool_manager._tools.values())
            get_query_plan = next(t for t in tools if t.name == "get_query_plan")

            result = get_query_plan.fn(result_file="nonexistent.json", query_id="1", format=fmt)

            # Should fail with file not found, not format validation
            if "error" in result:
                assert result["error_code"] != "VALIDATION_INVALID_FORMAT"

    def test_missing_file_returns_error(self):
        """Test that missing result file returns appropriate error."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.analytics import register_analytics_tools

        mcp = FastMCP("test")
        register_analytics_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        get_query_plan = next(t for t in tools if t.name == "get_query_plan")

        result = get_query_plan.fn(result_file="nonexistent_file.json", query_id="1", format="tree")

        assert "error" in result
        assert result["error_code"] == "RESOURCE_NOT_FOUND"


class TestDetectRegressionsTool:
    """Tests for detect_regressions tool functionality."""

    def test_no_results_directory_returns_no_data(self):
        """Test that missing results directory returns no_data status."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.analytics import DEFAULT_RESULTS_DIR, register_analytics_tools

        mcp = FastMCP("test")
        register_analytics_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        detect_regressions = next(t for t in tools if t.name == "detect_regressions")

        # Mock the results directory to not exist
        with patch.object(Path, "exists", return_value=False):
            result = detect_regressions.fn()

        assert result["status"] in ["no_data", "insufficient_data"]

    def test_threshold_percent_default(self):
        """Test default threshold percent is used."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.analytics import register_analytics_tools

        mcp = FastMCP("test")
        register_analytics_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        detect_regressions = next(t for t in tools if t.name == "detect_regressions")

        # The tool should accept no arguments
        result = detect_regressions.fn()

        # Should not fail on missing arguments
        assert "status" in result


class TestGetPerformanceTrendsTool:
    """Tests for get_performance_trends tool functionality."""

    def test_invalid_metric_returns_error(self):
        """Test that invalid metric parameter returns validation error."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.analytics import register_historical_tools

        mcp = FastMCP("test")
        register_historical_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        get_performance_trends = next(t for t in tools if t.name == "get_performance_trends")

        result = get_performance_trends.fn(metric="invalid_metric")

        assert "error" in result
        assert result["error_code"] == "VALIDATION_ERROR"

    def test_valid_metrics_accepted(self):
        """Test that valid metric parameters are accepted."""
        valid_metrics = ["geometric_mean", "p50", "p95", "p99", "total_time"]

        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.analytics import register_historical_tools

        mcp = FastMCP("test")
        register_historical_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        get_performance_trends = next(t for t in tools if t.name == "get_performance_trends")

        for metric in valid_metrics:
            result = get_performance_trends.fn(metric=metric)

            # Should not fail with metric validation error
            if "error" in result:
                assert result.get("error_code") != "VALIDATION_ERROR"

    def test_no_results_returns_no_data(self):
        """Test that missing results returns appropriate status."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.analytics import register_historical_tools

        mcp = FastMCP("test")
        register_historical_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        get_performance_trends = next(t for t in tools if t.name == "get_performance_trends")

        # Use a non-existent directory
        with patch("benchbox.mcp.tools.analytics.DEFAULT_RESULTS_DIR", Path("/nonexistent")):
            result = get_performance_trends.fn()

        assert result["status"] in ["no_data", "no_matching_data"]


class TestAggregateResultsTool:
    """Tests for aggregate_results tool functionality."""

    def test_invalid_group_by_returns_error(self):
        """Test that invalid group_by parameter returns validation error."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.analytics import register_historical_tools

        mcp = FastMCP("test")
        register_historical_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        aggregate_results = next(t for t in tools if t.name == "aggregate_results")

        result = aggregate_results.fn(group_by="invalid")

        assert "error" in result
        assert result["error_code"] == "VALIDATION_ERROR"

    def test_valid_group_by_accepted(self):
        """Test that valid group_by parameters are accepted."""
        valid_group_by = ["platform", "benchmark", "date"]

        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.analytics import register_historical_tools

        mcp = FastMCP("test")
        register_historical_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        aggregate_results = next(t for t in tools if t.name == "aggregate_results")

        for group in valid_group_by:
            result = aggregate_results.fn(group_by=group)

            # Should not fail with validation error
            if "error" in result:
                assert result.get("error_code") != "VALIDATION_ERROR"


class TestExportResultsTool:
    """Tests for export_results tool functionality."""

    def test_invalid_format_returns_error(self):
        """Test that invalid format parameter returns validation error."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.results import register_results_tools

        mcp = FastMCP("test")
        register_results_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        export_results = next(t for t in tools if t.name == "export_results")

        result = export_results.fn(result_file="test.json", format="invalid")

        assert "error" in result
        assert result["error_code"] == "VALIDATION_INVALID_FORMAT"

    def test_valid_formats_accepted(self):
        """Test that valid format parameters are accepted."""
        valid_formats = ["json", "csv", "html"]

        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.results import register_results_tools

        mcp = FastMCP("test")
        register_results_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        export_results = next(t for t in tools if t.name == "export_results")

        for fmt in valid_formats:
            result = export_results.fn(result_file="nonexistent.json", format=fmt)

            # Should fail with file not found, not format validation
            if "error" in result:
                assert result.get("error_code") != "VALIDATION_INVALID_FORMAT"

    def test_path_traversal_rejected(self):
        """Test that path traversal in output_path is rejected."""
        import json
        import tempfile

        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.results import register_results_tools

        mcp = FastMCP("test")
        register_results_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        export_results = next(t for t in tools if t.name == "export_results")

        # Create a temporary results directory with a test file
        with tempfile.TemporaryDirectory() as tmpdir:
            results_dir = Path(tmpdir) / "benchmark_runs" / "results"
            results_dir.mkdir(parents=True)
            test_file = results_dir / "test.json"
            test_file.write_text(json.dumps({"benchmark": "test", "platform": {"type": "duckdb"}}))

            # Patch the results directory
            with patch("benchbox.mcp.tools.results.DEFAULT_RESULTS_DIR", results_dir):
                with patch("benchbox.mcp.tools.results._get_results_impl") as mock_get:
                    # Mock the results to return valid data
                    mock_get.return_value = {
                        "benchmark": "test",
                        "platform": {"type": "duckdb"},
                        "queries": [],
                    }

                    result = export_results.fn(
                        result_file="test.json", format="json", output_path="../../../etc/passwd"
                    )

                    assert "error" in result
                    assert result["error_code"] == "VALIDATION_ERROR"


class TestCheckDependenciesTool:
    """Tests for check_dependencies tool functionality."""

    def test_returns_dependency_info(self):
        """Test that check_dependencies returns dependency information."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.discovery import register_dependency_tools

        mcp = FastMCP("test")
        register_dependency_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        check_dependencies = next(t for t in tools if t.name == "check_dependencies")

        result = check_dependencies.fn()

        assert "platforms" in result
        assert "summary" in result
        assert "total" in result["summary"]

    def test_unknown_platform_returns_error(self):
        """Test that unknown platform returns error with suggestions."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.discovery import register_dependency_tools

        mcp = FastMCP("test")
        register_dependency_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        check_dependencies = next(t for t in tools if t.name == "check_dependencies")

        result = check_dependencies.fn(platform="nonexistent_platform_xyz")

        assert "error" in result
        assert "available_platforms" in result

    def test_known_platform_returns_status(self):
        """Test that known platform returns dependency status."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.discovery import register_dependency_tools

        mcp = FastMCP("test")
        register_dependency_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        check_dependencies = next(t for t in tools if t.name == "check_dependencies")

        # Test with a known platform
        result = check_dependencies.fn(platform="databricks")

        assert "platforms" in result
        assert "quick_status" in result
        assert result["quick_status"]["platform"] == "databricks"


class TestGenerateDataTool:
    """Tests for generate_data tool functionality."""

    def test_invalid_format_returns_error(self):
        """Test that invalid format parameter returns validation error."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.benchmark import register_datagen_tools

        mcp = FastMCP("test")
        register_datagen_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        generate_data = next(t for t in tools if t.name == "generate_data")

        result = generate_data.fn(benchmark="tpch", format="invalid")

        assert "error" in result
        assert result["error_code"] == "VALIDATION_INVALID_FORMAT"

    def test_unknown_benchmark_returns_error(self):
        """Test that unknown benchmark returns appropriate error."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.benchmark import register_datagen_tools

        mcp = FastMCP("test")
        register_datagen_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        generate_data = next(t for t in tools if t.name == "generate_data")

        result = generate_data.fn(benchmark="nonexistent_benchmark")

        assert "error" in result
        assert result["error_code"] == "RESOURCE_NOT_FOUND"

    def test_invalid_scale_factor_returns_error(self):
        """Test that invalid scale factor returns validation error."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp.tools.benchmark import register_datagen_tools

        mcp = FastMCP("test")
        register_datagen_tools(mcp)

        tools = list(mcp._tool_manager._tools.values())
        generate_data = next(t for t in tools if t.name == "generate_data")

        result = generate_data.fn(benchmark="tpch", scale_factor=-1)

        assert "error" in result
        assert result["error_code"] == "VALIDATION_INVALID_SCALE_FACTOR"


class TestAnalyticsHelperFunctions:
    """Tests for analytics helper functions."""

    def test_percentile_calculation(self):
        """Test percentile calculation helper."""
        from benchbox.mcp.tools.analytics import _percentile

        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        assert _percentile(data, 50) == 5.5
        assert _percentile(data, 0) == 1
        assert _percentile(data, 100) == 10

    def test_percentile_empty_data(self):
        """Test percentile with empty data."""
        from benchbox.mcp.tools.analytics import _percentile

        assert _percentile([], 50) == 0

    def test_std_dev_calculation(self):
        """Test standard deviation calculation helper."""
        from benchbox.mcp.tools.analytics import _std_dev

        data = [2, 4, 4, 4, 5, 5, 7, 9]
        std = _std_dev(data)

        # Sample standard deviation should be approximately 2.14
        assert 2.1 < std < 2.2

    def test_std_dev_single_value(self):
        """Test standard deviation with single value."""
        from benchbox.mcp.tools.analytics import _std_dev

        assert _std_dev([5]) == 0

    def test_calculate_metric_geometric_mean(self):
        """Test geometric mean calculation."""
        from benchbox.mcp.tools.analytics import _calculate_metric

        data = [1, 2, 4, 8]
        result = _calculate_metric(data, "geometric_mean")

        # Geometric mean of 1,2,4,8 = (1*2*4*8)^(1/4) = 64^0.25 ≈ 2.83
        assert 2.8 < result < 2.9

    def test_classify_regression_severity(self):
        """Test regression severity classification."""
        from benchbox.mcp.tools.analytics import _classify_regression_severity

        assert _classify_regression_severity(150) == "critical"
        assert _classify_regression_severity(75) == "high"
        assert _classify_regression_severity(30) == "medium"
        assert _classify_regression_severity(15) == "low"
