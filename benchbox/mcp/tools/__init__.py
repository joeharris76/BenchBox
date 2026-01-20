"""MCP Tools for BenchBox.

This package contains tool implementations that expose BenchBox functionality
through the Model Context Protocol.

Tools are organized into modules:
- discovery: Platform and benchmark discovery (list_platforms, list_benchmarks, etc.)
- benchmark: Benchmark execution (run_benchmark, dry_run, validate_config)
- results: Results management (get_results, compare_results, export_results)

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.mcp.tools.benchmark import register_benchmark_tools
from benchbox.mcp.tools.discovery import register_discovery_tools
from benchbox.mcp.tools.results import register_results_tools

__all__ = [
    "register_discovery_tools",
    "register_benchmark_tools",
    "register_results_tools",
]
