"""Tests for MCP resource and prompt registry modules.

Tests that register_all_resources and register_all_prompts actually register
callable functions with the FastMCP server that produce correct output.
"""

import json
import sys
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
    pytest.mark.skipif(sys.version_info < (3, 10), reason="MCP server requires Python 3.10+"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mcp():
    """Create a real FastMCP instance for registration testing."""
    from mcp.server.fastmcp import FastMCP

    return FastMCP("test-benchbox")


# ---------------------------------------------------------------------------
# resources/registry.py
# ---------------------------------------------------------------------------


class TestRegisterAllResources:
    """Test that register_all_resources registers functions on the MCP server."""

    def test_registration_completes(self):
        from benchbox.mcp.resources.registry import register_all_resources

        mcp = _make_mcp()
        # Should not raise
        register_all_resources(mcp, results_dir=Path("benchmark_runs/results"))

    def test_list_benchmarks_resource(self):
        from benchbox.mcp.resources.registry import register_all_resources

        mcp = _make_mcp()
        register_all_resources(mcp, results_dir=Path("benchmark_runs/results"))

        # Access the registered function directly via the resource manager
        # FastMCP stores resources; we can invoke the underlying function
        # by finding it in the registered resources
        resources = mcp._resource_manager._resources
        assert "benchbox://benchmarks" in resources

        # Call the underlying function
        fn = resources["benchbox://benchmarks"].fn
        result = fn()
        data = json.loads(result)
        assert "benchmarks" in data
        assert "count" in data
        assert data["count"] > 0
        # Should include known benchmarks
        names = [b["name"] for b in data["benchmarks"]]
        assert "tpch" in names

    def test_get_benchmark_resource_valid(self):
        from benchbox.mcp.resources.registry import register_all_resources

        mcp = _make_mcp()
        register_all_resources(mcp, results_dir=Path("benchmark_runs/results"))

        templates = mcp._resource_manager._templates
        template_key = "benchbox://benchmarks/{name}"
        assert template_key in templates

        fn = templates[template_key].fn
        result = fn(name="tpch")
        data = json.loads(result)
        assert data["name"] == "tpch"
        assert data["display_name"] == "TPC-H"
        assert data["query_count"] == 22
        assert "scale_factors" in data

    def test_get_benchmark_resource_invalid(self):
        from benchbox.mcp.resources.registry import register_all_resources

        mcp = _make_mcp()
        register_all_resources(mcp, results_dir=Path("benchmark_runs/results"))

        templates = mcp._resource_manager._templates
        fn = templates["benchbox://benchmarks/{name}"].fn
        result = fn(name="nonexistent")
        data = json.loads(result)
        assert "error" in data
        assert "available" in data

    def test_list_platforms_resource(self):
        from benchbox.mcp.resources.registry import register_all_resources

        mcp = _make_mcp()
        register_all_resources(mcp, results_dir=Path("benchmark_runs/results"))

        resources = mcp._resource_manager._resources
        assert "benchbox://platforms" in resources

        fn = resources["benchbox://platforms"].fn
        result = fn()
        data = json.loads(result)
        assert "platforms" in data
        assert data["count"] > 0

    def test_get_platform_resource_valid(self):
        from benchbox.mcp.resources.registry import register_all_resources

        mcp = _make_mcp()
        register_all_resources(mcp, results_dir=Path("benchmark_runs/results"))

        templates = mcp._resource_manager._templates
        fn = templates["benchbox://platforms/{name}"].fn
        result = fn(name="duckdb")
        data = json.loads(result)
        assert data["name"] == "duckdb"
        assert "capabilities" in data

    def test_get_platform_resource_invalid(self):
        from benchbox.mcp.resources.registry import register_all_resources

        mcp = _make_mcp()
        register_all_resources(mcp, results_dir=Path("benchmark_runs/results"))

        templates = mcp._resource_manager._templates
        fn = templates["benchbox://platforms/{name}"].fn
        result = fn(name="nonexistent_platform")
        data = json.loads(result)
        assert "error" in data

    def test_recent_results_no_dir(self, tmp_path):
        from benchbox.mcp.resources.registry import register_all_resources

        mcp = _make_mcp()
        register_all_resources(mcp, results_dir=tmp_path / "nonexistent")

        resources = mcp._resource_manager._resources
        fn = resources["benchbox://results/recent"].fn
        result = fn()
        data = json.loads(result)
        assert data["count"] == 0
        assert data["runs"] == []

    def test_recent_results_with_files(self, tmp_path):
        # Create a mock result file
        result_data = {
            "platform": {"name": "duckdb"},
            "benchmark": {"id": "tpch", "scale_factor": 0.01},
            "run": {"timestamp": "2026-02-07T10:00:00", "id": "test-run-1"},
        }
        result_file = tmp_path / "test_result.json"
        result_file.write_text(json.dumps(result_data))

        from benchbox.mcp.resources.registry import register_all_resources

        mcp = _make_mcp()
        register_all_resources(mcp, results_dir=tmp_path)

        resources = mcp._resource_manager._resources
        fn = resources["benchbox://results/recent"].fn
        result = fn()
        data = json.loads(result)
        assert data["count"] == 1
        assert data["runs"][0]["platform"] == "duckdb"
        assert data["runs"][0]["benchmark"] == "tpch"

    def test_system_profile_resource(self):
        from benchbox.mcp.resources.registry import register_all_resources

        mcp = _make_mcp()
        register_all_resources(mcp, results_dir=Path("benchmark_runs/results"))

        resources = mcp._resource_manager._resources
        assert "benchbox://system/profile" in resources

        fn = resources["benchbox://system/profile"].fn
        result = fn()
        data = json.loads(result)
        assert "cpu" in data
        assert "memory" in data
        assert "packages" in data
        assert data["cpu"]["threads"] > 0


# ---------------------------------------------------------------------------
# prompts/registry.py
# ---------------------------------------------------------------------------


class TestRegisterAllPrompts:
    """Test that register_all_prompts registers prompt functions."""

    def test_registration_completes(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

    def test_analyze_results_prompt(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        assert "analyze_results" in prompts

        fn = prompts["analyze_results"].fn
        result = fn(benchmark="tpch", platform="duckdb", focus="slowest_queries")
        assert len(result) == 1
        assert "TPCH" in result[0].text
        assert "duckdb" in result[0].text
        assert "slowest_queries" in result[0].text

    def test_analyze_results_prompt_no_focus(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["analyze_results"].fn
        result = fn(benchmark="tpcds", platform="polars-df")
        assert len(result) == 1
        assert "TPCDS" in result[0].text
        assert "slowest_queries" not in result[0].text

    def test_compare_platforms_prompt(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["compare_platforms"].fn
        result = fn(benchmark="tpch", platforms="duckdb,polars-df", scale_factor=0.1)
        assert len(result) == 1
        assert "duckdb" in result[0].text
        assert "polars-df" in result[0].text
        assert "0.1" in result[0].text

    def test_identify_regressions_with_runs(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["identify_regressions"].fn
        result = fn(baseline_run="run1.json", comparison_run="run2.json", threshold_percent=15.0)
        assert len(result) == 1
        assert "run1.json" in result[0].text
        assert "run2.json" in result[0].text
        assert "15.0" in result[0].text

    def test_identify_regressions_without_runs(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["identify_regressions"].fn
        result = fn()
        assert len(result) == 1
        assert "recent" in result[0].text.lower()

    def test_benchmark_planning_prompt(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["benchmark_planning"].fn
        result = fn(use_case="production", platforms="duckdb,snowflake", time_budget_minutes=60)
        assert len(result) == 1
        assert "production" in result[0].text
        assert "60 minutes" in result[0].text

    def test_troubleshoot_failure_prompt(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["troubleshoot_failure"].fn
        result = fn(error_message="Connection refused", platform="snowflake", benchmark="tpch")
        assert len(result) == 1
        assert "Connection refused" in result[0].text
        assert "snowflake" in result[0].text

    def test_troubleshoot_failure_no_context(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["troubleshoot_failure"].fn
        result = fn()
        assert len(result) == 1
        assert "No specific context provided" in result[0].text

    def test_benchmark_run_prompt(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["benchmark_run"].fn
        result = fn(platform="duckdb", benchmark="tpch", scale_factor=0.01, queries="Q1,Q6")
        assert len(result) == 1
        assert "duckdb" in result[0].text
        assert "TPCH" in result[0].text
        assert "Q1,Q6" in result[0].text

    def test_benchmark_run_no_queries(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["benchmark_run"].fn
        result = fn(platform="duckdb", benchmark="tpch", scale_factor=0.01)
        assert len(result) == 1
        assert "TPCH" in result[0].text

    def test_platform_tuning_prompt(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["platform_tuning"].fn
        result = fn(platform="duckdb", workload="OLAP heavy joins")
        assert len(result) == 1
        assert "duckdb" in result[0].text
        assert "OLAP heavy joins" in result[0].text

    def test_platform_tuning_no_workload(self):
        from benchbox.mcp.prompts.registry import register_all_prompts

        mcp = _make_mcp()
        register_all_prompts(mcp)

        prompts = mcp._prompt_manager._prompts
        fn = prompts["platform_tuning"].fn
        result = fn(platform="snowflake")
        assert len(result) == 1
        assert "snowflake" in result[0].text
