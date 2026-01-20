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
        """Analyze benchmark results. (benchmark=tpch, platform=duckdb, focus=optional)"""
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
        """Compare performance across platforms. (benchmark=tpch, platforms=duckdb,polars-df, scale_factor=0.01)"""
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
        """Find performance regressions between runs. (baseline_run, comparison_run, threshold_percent=10)"""
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
        """Plan a benchmark strategy. (use_case=testing|production|comparison, platforms, time_budget_minutes=30)"""
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
        """Diagnose benchmark failures. (error_message, platform, benchmark)"""
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

    @mcp.prompt()
    def benchmark_run(
        platform: str = "duckdb",
        benchmark: str = "tpch",
        scale_factor: float = 0.01,
        queries: str | None = None,
    ) -> list[TextContent]:
        """Execute a planned benchmark. (platform=duckdb, benchmark=tpch, scale_factor=0.01, queries=optional)"""
        queries_text = ""
        if queries:
            queries_text = f"\nQuery subset: {queries}"

        prompt = f"""Execute a benchmark with the following configuration:

Platform: {platform}
Benchmark: {benchmark.upper()}
Scale Factor: {scale_factor}{queries_text}

Before running, please:
1. validate_config(platform="{platform}", benchmark="{benchmark}", scale_factor={scale_factor}) - Verify the configuration is valid
2. check_dependencies(platform="{platform}") - Ensure dependencies are installed

If validation passes, execute:
3. run_benchmark(platform="{platform}", benchmark="{benchmark}", scale_factor={scale_factor}{f', queries="{queries}"' if queries else ""})

After execution, provide:
1. **Execution Summary**: Overall status and runtime
2. **Performance Highlights**: Fastest and slowest queries
3. **Results Location**: Where to find detailed results
4. **Recommendations**: Any optimizations based on results

If any step fails, provide diagnostic information and suggest fixes."""

        return [TextContent(type="text", text=prompt)]

    @mcp.prompt()
    def platform_tuning(
        platform: str = "duckdb",
        workload: str | None = None,
    ) -> list[TextContent]:
        """Get tuning recommendations for a platform. (platform=duckdb, workload=optional)"""
        workload_text = ""
        if workload:
            workload_text = f"\nWorkload characteristics: {workload}"

        prompt = f"""Provide tuning recommendations for {platform} platform.{workload_text}

Use these tools to gather information:
1. system_profile() - Get system resources (CPU, memory, disk)
2. check_dependencies(platform="{platform}", verbose=True) - Verify platform configuration

Based on the system profile and platform capabilities, provide:

1. **Memory Configuration**
   - Recommended memory limits based on available RAM
   - Buffer pool/cache sizing recommendations

2. **Parallelism Settings**
   - Thread count recommendations based on CPU cores
   - Worker/executor configuration

3. **I/O Optimization**
   - Disk I/O settings if applicable
   - Compression recommendations for data format

4. **Query Optimization**
   - Join strategies for analytical workloads
   - Aggregation optimizations

5. **Platform-Specific Settings**
   - Settings unique to {platform}
   - Best practices for benchmark workloads

Format recommendations as:
- Setting name: recommended value
- Explanation of why this value is optimal for the system

Include example configuration snippets where applicable."""

        return [TextContent(type="text", text=prompt)]

    logger.info("Registered MCP prompts")
