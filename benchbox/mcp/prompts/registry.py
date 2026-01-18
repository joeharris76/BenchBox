"""Prompt registration for BenchBox MCP server.

Provides reusable prompt templates for AI analysis of benchmark results.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging

from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent

logger = logging.getLogger(__name__)


def register_all_prompts(mcp: FastMCP) -> None:
    """Register all MCP prompts with the server.

    Args:
        mcp: The FastMCP server instance to register prompts with.
    """

    @mcp.prompt()
    def analyze_results(
        benchmark: str = "tpch",
        platform: str = "duckdb",
        focus: str | None = None,
    ) -> list[TextContent]:
        """Analyze benchmark results and identify performance patterns.

        Args:
            benchmark: Benchmark name (default: tpch)
            platform: Platform name (default: duckdb)
            focus: Optional focus area (e.g., 'slowest_queries', 'memory', 'io')

        Returns:
            Prompt text for AI analysis.
        """
        focus_text = ""
        if focus:
            focus_text = f"\nPay special attention to {focus} in your analysis."

        prompt = f"""Analyze the {benchmark.upper()} benchmark results on {platform}.

Please use the following tools to gather data:
1. list_recent_runs(platform="{platform}", benchmark="{benchmark}") - Find recent runs
2. get_results(result_file="<filename>") - Get detailed results

Based on the results, provide:
1. **Performance Summary**: Overall execution time and query statistics
2. **Query Analysis**: Identify the 5 slowest queries and potential causes
3. **Pattern Recognition**: Look for patterns (e.g., join-heavy queries, aggregations)
4. **Recommendations**: Specific optimizations for this platform{focus_text}

Format your analysis with clear sections and include specific timing data."""

        return [TextContent(type="text", text=prompt)]

    @mcp.prompt()
    def compare_platforms(
        benchmark: str = "tpch",
        platforms: str = "duckdb,polars-df",
        scale_factor: float = 0.01,
    ) -> list[TextContent]:
        """Compare benchmark performance across multiple platforms.

        Args:
            benchmark: Benchmark to compare (default: tpch)
            platforms: Comma-separated platform names (default: duckdb,polars-df)
            scale_factor: Scale factor for comparison (default: 0.01)

        Returns:
            Prompt text for platform comparison.
        """
        platform_list = [p.strip() for p in platforms.split(",")]

        prompt = f"""Compare {benchmark.upper()} benchmark performance across these platforms:
{", ".join(platform_list)}

Scale Factor: {scale_factor}

Please use these tools:
1. validate_config() for each platform to check availability
2. run_benchmark() for each platform (if results don't exist)
3. list_recent_runs() to find existing results
4. compare_results() to compare two runs at a time

Provide a comprehensive comparison including:
1. **Overall Performance**: Total runtime for each platform
2. **Query-by-Query Analysis**: Which platform wins on each query type
3. **Strengths/Weaknesses**: What each platform does well
4. **Recommendations**: Which platform to use for different use cases

Use tables and specific metrics to support your analysis."""

        return [TextContent(type="text", text=prompt)]

    @mcp.prompt()
    def identify_regressions(
        baseline_run: str | None = None,
        comparison_run: str | None = None,
        threshold_percent: float = 10.0,
    ) -> list[TextContent]:
        """Identify performance regressions between benchmark runs.

        Args:
            baseline_run: Baseline result file (or 'latest' to find automatically)
            comparison_run: Comparison result file
            threshold_percent: Percentage change threshold for regression (default: 10%)

        Returns:
            Prompt text for regression analysis.
        """
        if baseline_run and comparison_run:
            prompt = f"""Analyze performance regressions between two benchmark runs.

Baseline: {baseline_run}
Comparison: {comparison_run}
Regression Threshold: {threshold_percent}%

Use these tools:
1. compare_results(file1="{baseline_run}", file2="{comparison_run}", threshold_percent={threshold_percent})
2. get_results() for detailed query information if needed

Provide analysis including:
1. **Regression Summary**: Number and severity of regressions
2. **Detailed Regressions**: List each regressed query with timing delta
3. **Root Cause Analysis**: Possible reasons for each regression
4. **Improvement Opportunities**: Any queries that got faster (to understand what helped)
5. **Action Items**: Specific steps to investigate/fix regressions"""
        else:
            prompt = f"""Identify performance regressions in recent benchmark runs.

Regression Threshold: {threshold_percent}%

Use these tools:
1. list_recent_runs() - Find the two most recent comparable runs
2. compare_results() - Compare them with threshold {threshold_percent}%
3. get_results() - Get details on any regressed queries

Provide analysis including:
1. **Regression Summary**: Overview of any regressions found
2. **Trend Analysis**: Is performance improving or declining over time?
3. **Action Items**: Recommended next steps for investigation"""

        return [TextContent(type="text", text=prompt)]

    @mcp.prompt()
    def benchmark_planning(
        use_case: str = "testing",
        platforms: str | None = None,
        time_budget_minutes: int = 30,
    ) -> list[TextContent]:
        """Help plan a benchmark strategy for a specific use case.

        Args:
            use_case: Use case type (testing, production, comparison, regression)
            platforms: Optional comma-separated platform list
            time_budget_minutes: Maximum time for benchmark runs (default: 30)

        Returns:
            Prompt text for benchmark planning.
        """
        prompt = f"""Help plan a benchmark strategy for the following requirements:

Use Case: {use_case}
Time Budget: {time_budget_minutes} minutes
Target Platforms: {platforms or "To be determined based on use case"}

Use these tools to gather information:
1. list_platforms() - See available platforms and their capabilities
2. list_benchmarks() - See available benchmarks with query counts
3. system_profile() - Check system resources for scale factor recommendations

Based on this information, provide:
1. **Recommended Benchmark**: Which benchmark(s) to run
2. **Scale Factor**: Appropriate scale factor for time budget and system
3. **Platform Selection**: Which platforms make sense for the use case
4. **Phase Strategy**: Which phases to run (load, power, throughput)
5. **Query Subset**: If time-constrained, which queries to prioritize
6. **Execution Plan**: Step-by-step commands to execute

Consider tradeoffs between:
- Statistical significance (higher scale, more iterations)
- Time constraints
- Resource availability"""

        return [TextContent(type="text", text=prompt)]

    @mcp.prompt()
    def troubleshoot_failure(
        error_message: str | None = None,
        platform: str | None = None,
        benchmark: str | None = None,
    ) -> list[TextContent]:
        """Diagnose and resolve benchmark failures.

        Args:
            error_message: Error message from failed run
            platform: Platform where failure occurred
            benchmark: Benchmark that failed

        Returns:
            Prompt text for troubleshooting.
        """
        context = []
        if error_message:
            context.append(f"Error Message: {error_message}")
        if platform:
            context.append(f"Platform: {platform}")
        if benchmark:
            context.append(f"Benchmark: {benchmark}")

        context_str = "\n".join(context) if context else "No specific context provided"

        prompt = f"""Troubleshoot a benchmark failure with the following context:

{context_str}

Use these tools to diagnose:
1. validate_config() - Check if configuration is valid
2. system_profile() - Check system resources
3. list_platforms() - Verify platform availability

Common issues to check:
1. **Platform Dependencies**: Is the platform properly installed?
2. **Scale Factor**: Is the scale factor valid for this benchmark?
3. **Resource Limits**: Sufficient memory/disk for the scale factor?
4. **Credentials**: For cloud platforms, are credentials configured?
5. **Data Generation**: Does generated data exist and is it valid?

Provide:
1. **Diagnosis**: Most likely cause of the failure
2. **Verification Steps**: How to confirm the cause
3. **Resolution**: Step-by-step fix instructions
4. **Prevention**: How to avoid this issue in the future"""

        return [TextContent(type="text", text=prompt)]

    logger.info("Registered MCP prompts")
