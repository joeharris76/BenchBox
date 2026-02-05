"""MCP Prompts for BenchBox.

This package contains prompt template implementations that help AI agents
analyze benchmark results and plan benchmark strategies.

Prompts are reusable templates:
- analyze_results - Analyze benchmark results and identify patterns
- compare_platforms - Compare performance across platforms
- identify_regressions - Detect performance regressions between runs
- benchmark_planning - Help plan benchmark strategy

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.mcp.prompts.registry import register_all_prompts

__all__ = [
    "register_all_prompts",
]
