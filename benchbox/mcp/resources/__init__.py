"""MCP Resources for BenchBox.

This package contains resource implementations that expose BenchBox metadata
and results through the Model Context Protocol.

Resources are read-only data sources identified by URIs:
- benchbox://benchmarks - List all benchmarks
- benchbox://benchmarks/{name} - Benchmark details
- benchbox://platforms - List all platforms
- benchbox://platforms/{name} - Platform details
- benchbox://results/recent - Recent benchmark runs
- benchbox://system/profile - System information

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.mcp.resources.registry import register_all_resources

__all__ = [
    "register_all_resources",
]
