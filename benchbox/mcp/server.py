"""BenchBox MCP Server implementation.

This module creates and configures the FastMCP server with all BenchBox
tools, resources, and prompts.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import sys

from mcp.server.fastmcp import FastMCP

from benchbox.mcp.prompts import register_all_prompts
from benchbox.mcp.resources import register_all_resources
from benchbox.mcp.tools.analytics import register_analytics_tools
from benchbox.mcp.tools.benchmark import register_benchmark_tools
from benchbox.mcp.tools.discovery import register_discovery_tools
from benchbox.mcp.tools.results import register_results_tools
from benchbox.mcp.tools.visualization import register_visualization_tools

# Configure logging to stderr (stdout is reserved for MCP JSON-RPC)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)

logger = logging.getLogger(__name__)


def create_benchbox_server() -> FastMCP:
    """Create and configure the BenchBox MCP server.

    The server provides tools for:
    - Discovery: List platforms, benchmarks, and system info
    - Benchmark execution: Run benchmarks, validate configs, dry-run
    - Results: Get results, compare runs, export data

    Returns:
        Configured FastMCP server instance.
    """
    # Suppress all console output - MCP servers must not write to stdout
    # (stdout is reserved exclusively for JSON-RPC messages)
    from benchbox.utils.printing import set_quiet

    set_quiet(True)

    # Create the FastMCP server
    mcp = FastMCP(
        name="benchbox",
        instructions="""BenchBox is a SQL benchmarking framework for OLAP databases.

Use these tools to:
1. **Discover** available platforms and benchmarks (list_available, get_benchmark_info)
2. **Run** benchmarks with specific configurations (run_benchmark with dry_run/validate_only flags)
3. **Analyze** results and compare runs (get_results, analyze_results)
4. **Visualize** results with charts (generate_chart, suggest_charts)

Start by listing available benchmarks or platforms to see what's possible.
For a quick test, try: run_benchmark(platform="duckdb", benchmark="tpch", scale_factor=0.01)
Then visualize: generate_chart(result_files="<result_file>", chart_type="performance_bar")
""",
    )

    # Register all tools
    logger.info("Registering discovery tools...")
    register_discovery_tools(mcp)

    logger.info("Registering benchmark execution tools...")
    register_benchmark_tools(mcp)

    logger.info("Registering results tools...")
    register_results_tools(mcp)

    logger.info("Registering analytics tools...")
    register_analytics_tools(mcp)

    logger.info("Registering visualization tools...")
    register_visualization_tools(mcp)

    logger.info("Registering resources...")
    register_all_resources(mcp)

    logger.info("Registering prompts...")
    register_all_prompts(mcp)

    logger.info("BenchBox MCP server configured successfully")

    return mcp
