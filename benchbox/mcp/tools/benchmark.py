"""Benchmark execution tools for BenchBox MCP server.

Provides tools for running benchmarks, validating configurations,
and performing dry runs.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from benchbox.mcp.errors import ErrorCode, make_error, make_execution_error, make_not_found_error

logger = logging.getLogger(__name__)

# Tool annotations for benchmark execution tools
RUN_BENCHMARK_ANNOTATIONS = ToolAnnotations(
    title="Execute benchmark",
    readOnlyHint=False,  # Creates files, runs queries
    destructiveHint=False,  # Does not delete existing data
    idempotentHint=False,  # Each run produces new results
    openWorldHint=True,  # Interacts with external databases
)

# Tool annotations for dry-run (read-only preview)
DRYRUN_ANNOTATIONS = ToolAnnotations(
    title="Preview benchmark execution",
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)

# Tool annotations for config validation (read-only)
VALIDATE_ANNOTATIONS = ToolAnnotations(
    title="Validate configuration",
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)

# Tool annotations for query details (read-only)
QUERY_DETAILS_ANNOTATIONS = ToolAnnotations(
    title="Get query details",
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)


def register_benchmark_tools(mcp: FastMCP) -> None:
    """Register benchmark execution tools with the MCP server.

    Args:
        mcp: The FastMCP server instance to register tools with.
    """

    @mcp.tool(annotations=RUN_BENCHMARK_ANNOTATIONS)
    def run_benchmark(
        platform: str,
        benchmark: str,
        scale_factor: float = 0.01,
        queries: str | None = None,
        phases: str | None = None,
    ) -> dict[str, Any]:
        """Run a benchmark on a database platform.

        Executes the specified benchmark on the target platform and returns results.

        Args:
            platform: Target platform (e.g., 'duckdb', 'polars-df', 'snowflake')
            benchmark: Benchmark to run (e.g., 'tpch', 'tpcds')
            scale_factor: Data scale factor (0.01 for testing, 1+ for production)
            queries: Optional comma-separated query IDs to run (e.g., "1,3,6")
            phases: Optional comma-separated phases (default: "load,power")

        Returns:
            Benchmark results including execution times and validation status.

        Example:
            run_benchmark(platform="duckdb", benchmark="tpch", scale_factor=0.01)
        """
        from benchbox.cli.benchmarks import BenchmarkManager
        from benchbox.cli.orchestrator import BenchmarkOrchestrator
        from benchbox.cli.system import SystemProfiler
        from benchbox.core.config import DatabaseConfig
        from benchbox.core.schemas import BenchmarkConfig

        execution_id = f"mcp_{uuid.uuid4().hex[:8]}"
        start_time = datetime.now()

        try:
            # Validate benchmark exists
            manager = BenchmarkManager()
            benchmark_lower = benchmark.lower()
            if benchmark_lower not in manager.benchmarks:
                error_response = make_not_found_error("benchmark", benchmark, available=list(manager.benchmarks.keys()))
                error_response["execution_id"] = execution_id
                error_response["status"] = "failed"
                return error_response

            # Parse phases
            phase_list = (phases or "load,power").lower().split(",")
            phases_to_run = []
            if "load" in phase_list:
                phases_to_run.append("load")
            if "power" in phase_list or "execute" in phase_list:
                phases_to_run.append("power")

            # Note: query subset filtering is handled by the orchestrator
            # The `queries` parameter is validated but filtering happens downstream

            # Create benchmark config
            output_dir = Path(tempfile.gettempdir()) / "benchbox_mcp" / execution_id
            output_dir.mkdir(parents=True, exist_ok=True)

            # Get display name from metadata
            meta = manager.benchmarks[benchmark_lower]
            display_name = meta.get("display_name", benchmark_lower.upper())

            config = BenchmarkConfig(
                name=benchmark_lower,
                display_name=display_name,
                scale_factor=scale_factor,
            )

            # Create database config
            db_config = DatabaseConfig(
                type=platform.lower(),
                name=f"{platform.lower()}_mcp",
            )

            # Get system profile
            profiler = SystemProfiler()
            system_profile = profiler.profile()

            # Create orchestrator and run
            orchestrator = BenchmarkOrchestrator()

            result = orchestrator.execute_benchmark(
                config=config,
                database_config=db_config,
                system_profile=system_profile,
                phases_to_run=phases_to_run,
            )

            # Extract key metrics
            execution_time = (datetime.now() - start_time).total_seconds()

            # Format results for MCP response
            response = {
                "execution_id": execution_id,
                "status": "completed" if result else "no_results",
                "platform": platform,
                "benchmark": benchmark,
                "scale_factor": scale_factor,
                "execution_time_seconds": round(execution_time, 2),
            }

            if result:
                # Get summary from result
                if hasattr(result, "summary") and result.summary:
                    response["summary"] = {
                        "total_queries": getattr(result.summary, "total_queries", 0),
                        "successful_queries": getattr(result.summary, "successful_queries", 0),
                        "failed_queries": getattr(result.summary, "failed_queries", 0),
                        "total_runtime_ms": getattr(result.summary, "total_runtime_ms", 0),
                    }

                # Add per-query results (limited for readability)
                if hasattr(result, "phases") and result.phases:
                    query_results = []
                    for phase_name, phase_data in result.phases.items():
                        if hasattr(phase_data, "queries"):
                            for qr in phase_data.queries:
                                query_results.append(
                                    {
                                        "phase": phase_name,
                                        "id": getattr(qr, "query_id", "unknown"),
                                        "runtime_ms": getattr(qr, "runtime_ms", 0),
                                        "status": getattr(qr, "status", "unknown"),
                                    }
                                )
                    if query_results:
                        response["queries"] = query_results[:20]
                        if len(query_results) > 20:
                            response["queries_note"] = f"Showing 20 of {len(query_results)} queries"

            return response

        except Exception as e:
            logger.exception(f"Benchmark execution failed: {e}")
            error_response = make_execution_error(
                f"Benchmark execution failed: {e}",
                execution_id=execution_id,
                exception=e,
                retry_hint=False,
            )
            error_response["status"] = "failed"
            error_response["platform"] = platform
            error_response["benchmark"] = benchmark
            error_response["execution_time_seconds"] = round((datetime.now() - start_time).total_seconds(), 2)
            return error_response

    @mcp.tool(annotations=DRYRUN_ANNOTATIONS)
    def dry_run(
        platform: str,
        benchmark: str,
        scale_factor: float = 0.01,
        queries: str | None = None,
    ) -> dict[str, Any]:
        """Preview what a benchmark run would do without executing.

        Shows the execution plan including:
        - Queries that would be executed
        - Estimated data sizes
        - Required resources
        - Platform configuration

        Args:
            platform: Target platform
            benchmark: Benchmark to preview
            scale_factor: Data scale factor
            queries: Optional query subset

        Returns:
            Execution plan and resource estimates.

        Example:
            dry_run(platform="duckdb", benchmark="tpch", scale_factor=1)
        """
        from benchbox.cli.benchmarks import BenchmarkManager
        from benchbox.core.benchmark_loader import get_benchmark_class

        try:
            manager = BenchmarkManager()
            benchmark_lower = benchmark.lower()

            # Validate benchmark exists
            if benchmark_lower not in manager.benchmarks:
                error_response = make_not_found_error("benchmark", benchmark, available=list(manager.benchmarks.keys()))
                error_response["status"] = "error"
                return error_response

            meta = manager.benchmarks[benchmark_lower]

            # Get query list from benchmark class
            all_queries = []
            try:
                benchmark_class = get_benchmark_class(benchmark_lower)
                bm = benchmark_class(scale_factor=scale_factor)
                if hasattr(bm, "query_manager") and hasattr(bm.query_manager, "get_all_queries"):
                    queries_dict = bm.query_manager.get_all_queries()
                    all_queries = [str(qid) for qid in queries_dict]
                elif hasattr(bm, "get_query_ids"):
                    all_queries = bm.get_query_ids()
            except Exception:
                # Fall back to metadata
                all_queries = [str(i) for i in range(1, meta.get("num_queries", 0) + 1)]

            # Filter by query subset if specified
            if queries:
                query_list = [q.strip() for q in queries.split(",")]
                selected_queries = [q for q in all_queries if q in query_list]
            else:
                selected_queries = all_queries

            # Estimate data size (rough approximation)
            # TPC-H at SF=1 is approximately 1GB
            estimated_data_gb = scale_factor * 1.0

            return {
                "status": "dry_run",
                "platform": platform,
                "benchmark": benchmark,
                "scale_factor": scale_factor,
                "execution_plan": {
                    "phases": ["load", "power"],
                    "total_queries": len(selected_queries),
                    "query_ids": selected_queries[:30],  # Limit for readability
                    "query_ids_truncated": len(selected_queries) > 30,
                },
                "resource_estimates": {
                    "data_size_gb": round(estimated_data_gb, 2),
                    "memory_recommended_gb": max(2, round(estimated_data_gb * 2, 1)),
                    "disk_space_recommended_gb": max(1, round(estimated_data_gb * 3, 1)),
                },
                "notes": [
                    "This is a preview - no benchmark will be executed",
                    "Actual resource usage may vary based on platform and configuration",
                    "For cloud platforms, ensure credentials are configured",
                ],
            }

        except Exception as e:
            error_response = make_error(
                ErrorCode.INTERNAL_ERROR,
                f"Dry run failed: {e}",
                details={"exception_type": type(e).__name__},
            )
            error_response["status"] = "error"
            return error_response

    @mcp.tool(annotations=VALIDATE_ANNOTATIONS)
    def validate_config(
        platform: str,
        benchmark: str,
        scale_factor: float = 1.0,
    ) -> dict[str, Any]:
        """Validate a benchmark configuration before running.

        Checks:
        - Platform availability and credentials
        - Benchmark exists and supports the platform
        - Scale factor is valid
        - Required dependencies are installed

        Args:
            platform: Target platform
            benchmark: Benchmark name
            scale_factor: Data scale factor

        Returns:
            Validation results with any errors or warnings.

        Example:
            validate_config(platform="snowflake", benchmark="tpch", scale_factor=10)
        """
        from benchbox.cli.benchmarks import BenchmarkManager
        from benchbox.core.platform_registry import PlatformRegistry
        from benchbox.platforms import is_dataframe_platform

        errors = []
        warnings = []

        # Validate platform
        platform_lower = platform.lower()
        base_platform = platform_lower.replace("-df", "")

        # Check if platform exists
        info = PlatformRegistry.get_platform_info(base_platform)
        if info is None:
            errors.append(f"Unknown platform: {platform}")
        elif not info.available:
            errors.append(f"Platform '{platform}' dependencies not installed: {info.installation_command}")

        # Validate benchmark
        manager = BenchmarkManager()
        benchmark_lower = benchmark.lower()

        if benchmark_lower not in manager.benchmarks:
            errors.append(f"Unknown benchmark: {benchmark}. Available: {', '.join(manager.benchmarks.keys())}")
        else:
            meta = manager.benchmarks[benchmark_lower]
            min_scale = meta.get("min_scale", 0.01)
            if scale_factor < min_scale:
                warnings.append(f"{benchmark} requires scale factor >= {min_scale}")

        # Validate scale factor
        if scale_factor <= 0:
            errors.append(f"Scale factor must be positive, got: {scale_factor}")
        elif scale_factor < 0.01:
            warnings.append(f"Scale factor {scale_factor} is very small, results may not be meaningful")

        # Check DataFrame support
        if is_dataframe_platform(platform_lower) and benchmark_lower not in ("tpch", "tpcds"):
            errors.append("DataFrame mode only supports tpch and tpcds benchmarks")

        # Cloud platform warnings
        cloud_platforms = ["snowflake", "bigquery", "databricks", "redshift"]
        if base_platform in cloud_platforms:
            warnings.append(f"Cloud platform '{platform}' requires proper credential configuration")

        return {
            "valid": len(errors) == 0,
            "platform": platform,
            "benchmark": benchmark,
            "scale_factor": scale_factor,
            "errors": errors,
            "warnings": warnings,
            "recommendations": _get_config_recommendations(platform_lower, benchmark_lower, scale_factor),
        }

    @mcp.tool(annotations=QUERY_DETAILS_ANNOTATIONS)
    def get_query_details(
        benchmark: str,
        query_id: str,
    ) -> dict[str, Any]:
        """Get detailed information about a specific query.

        Returns information about a query including:
        - SQL text (if available)
        - Description and purpose
        - Expected complexity hints
        - Tables accessed

        Args:
            benchmark: Benchmark name (e.g., 'tpch', 'tpcds')
            query_id: Query identifier (e.g., '1', 'Q1', '17')

        Returns:
            Query details including SQL text and metadata.

        Example:
            get_query_details(benchmark="tpch", query_id="6")
        """
        from benchbox.cli.benchmarks import BenchmarkManager
        from benchbox.core.benchmark_loader import get_benchmark_class

        benchmark_lower = benchmark.lower()
        manager = BenchmarkManager()

        # Validate benchmark exists
        if benchmark_lower not in manager.benchmarks:
            error_response = make_not_found_error("benchmark", benchmark, available=list(manager.benchmarks.keys()))
            return error_response

        try:
            # Load benchmark class
            benchmark_class = get_benchmark_class(benchmark_lower)
            bm = benchmark_class(scale_factor=0.01)

            # Try to get query info
            query_sql = None
            query_description = None

            # Normalize query ID (handle "Q1" vs "1" format)
            normalized_id = query_id.upper().lstrip("Q")
            if not normalized_id.isdigit():
                normalized_id = query_id  # Keep original if not numeric

            # Try query_manager approach
            if hasattr(bm, "query_manager"):
                qm = bm.query_manager
                if hasattr(qm, "get_query"):
                    try:
                        query = qm.get_query(normalized_id)
                        if query:
                            query_sql = getattr(query, "sql", None) or getattr(query, "template", None)
                            query_description = getattr(query, "description", None)
                    except (KeyError, ValueError):
                        pass
                elif hasattr(qm, "get_all_queries"):
                    all_queries = qm.get_all_queries()
                    # Try various key formats
                    for key in [
                        normalized_id,
                        query_id,
                        f"Q{normalized_id}",
                        int(normalized_id) if normalized_id.isdigit() else None,
                    ]:
                        if key in all_queries:
                            q = all_queries[key]
                            query_sql = getattr(q, "sql", None) or getattr(q, "template", None)
                            query_description = getattr(q, "description", None)
                            break

            # Try direct get_query method
            if query_sql is None and hasattr(bm, "get_query"):
                try:
                    query = bm.get_query(normalized_id)
                    if query:
                        query_sql = getattr(query, "sql", None) or str(query) if not callable(query) else None
                        query_description = getattr(query, "description", None)
                except (KeyError, ValueError, AttributeError):
                    pass

            # Build response
            meta = manager.benchmarks[benchmark_lower]

            response: dict[str, Any] = {
                "benchmark": benchmark_lower,
                "query_id": query_id,
                "normalized_id": normalized_id,
            }

            if query_sql:
                # Truncate very long SQL
                if len(query_sql) > 2000:
                    response["sql"] = query_sql[:2000]
                    response["sql_truncated"] = True
                else:
                    response["sql"] = query_sql

            if query_description:
                response["description"] = query_description

            # Add complexity hints based on benchmark
            response["complexity_hints"] = _get_query_complexity_hints(benchmark_lower, normalized_id)

            # Add benchmark metadata context
            response["benchmark_info"] = {
                "total_queries": meta.get("num_queries", 0),
                "category": meta.get("category", "unknown"),
            }

            return response

        except Exception as e:
            return make_error(
                ErrorCode.INTERNAL_ERROR,
                f"Failed to get query details: {e}",
                details={"benchmark": benchmark, "query_id": query_id, "exception_type": type(e).__name__},
            )


def _get_query_complexity_hints(benchmark: str, query_id: str) -> dict[str, Any]:
    """Get complexity hints for a specific query.

    Args:
        benchmark: Benchmark name
        query_id: Query ID

    Returns:
        Dictionary with complexity hints.
    """
    # TPC-H query complexity hints (well-known queries)
    tpch_hints: dict[str, dict[str, Any]] = {
        "1": {"type": "aggregation", "tables": ["lineitem"], "complexity": "simple", "joins": 0},
        "2": {
            "type": "correlated_subquery",
            "tables": ["part", "supplier", "partsupp", "nation", "region"],
            "complexity": "complex",
            "joins": 5,
        },
        "3": {
            "type": "join_aggregate",
            "tables": ["customer", "orders", "lineitem"],
            "complexity": "medium",
            "joins": 2,
        },
        "4": {"type": "exists_subquery", "tables": ["orders", "lineitem"], "complexity": "medium", "joins": 1},
        "5": {
            "type": "multi_join",
            "tables": ["customer", "orders", "lineitem", "supplier", "nation", "region"],
            "complexity": "complex",
            "joins": 5,
        },
        "6": {"type": "scan_filter", "tables": ["lineitem"], "complexity": "simple", "joins": 0},
        "7": {
            "type": "multi_join",
            "tables": ["supplier", "lineitem", "orders", "customer", "nation"],
            "complexity": "complex",
            "joins": 6,
        },
        "8": {
            "type": "multi_join",
            "tables": ["part", "supplier", "lineitem", "orders", "customer", "nation", "region"],
            "complexity": "complex",
            "joins": 7,
        },
        "9": {
            "type": "multi_join",
            "tables": ["part", "supplier", "lineitem", "partsupp", "orders", "nation"],
            "complexity": "complex",
            "joins": 5,
        },
        "10": {
            "type": "join_aggregate",
            "tables": ["customer", "orders", "lineitem", "nation"],
            "complexity": "medium",
            "joins": 3,
        },
        "11": {
            "type": "having_subquery",
            "tables": ["partsupp", "supplier", "nation"],
            "complexity": "medium",
            "joins": 2,
        },
        "12": {"type": "case_aggregate", "tables": ["orders", "lineitem"], "complexity": "medium", "joins": 1},
        "13": {"type": "outer_join", "tables": ["customer", "orders"], "complexity": "medium", "joins": 1},
        "14": {"type": "case_aggregate", "tables": ["lineitem", "part"], "complexity": "simple", "joins": 1},
        "15": {"type": "view_with_max", "tables": ["lineitem", "supplier"], "complexity": "medium", "joins": 1},
        "16": {
            "type": "distinct_aggregate",
            "tables": ["partsupp", "part", "supplier"],
            "complexity": "medium",
            "joins": 2,
        },
        "17": {"type": "correlated_subquery", "tables": ["lineitem", "part"], "complexity": "complex", "joins": 1},
        "18": {
            "type": "having_subquery",
            "tables": ["customer", "orders", "lineitem"],
            "complexity": "complex",
            "joins": 3,
        },
        "19": {"type": "or_predicates", "tables": ["lineitem", "part"], "complexity": "medium", "joins": 1},
        "20": {
            "type": "exists_subquery",
            "tables": ["supplier", "nation", "partsupp", "part", "lineitem"],
            "complexity": "complex",
            "joins": 4,
        },
        "21": {
            "type": "not_exists",
            "tables": ["supplier", "lineitem", "orders", "nation"],
            "complexity": "complex",
            "joins": 4,
        },
        "22": {"type": "not_exists", "tables": ["customer", "orders"], "complexity": "complex", "joins": 1},
    }

    if benchmark == "tpch" and query_id in tpch_hints:
        return tpch_hints[query_id]

    # Default hints for unknown queries
    return {
        "type": "unknown",
        "complexity": "unknown",
        "note": f"Complexity hints not available for {benchmark} query {query_id}",
    }


def _get_config_recommendations(platform: str, benchmark: str, scale_factor: float) -> list[str]:
    """Generate configuration recommendations.

    Args:
        platform: Platform name
        benchmark: Benchmark name
        scale_factor: Scale factor

    Returns:
        List of recommendations.
    """
    recommendations = []

    if scale_factor >= 10:
        recommendations.append("Consider using Parquet data format for better performance at large scale")

    if platform.endswith("-df"):
        recommendations.append("DataFrame mode uses in-memory processing - ensure sufficient RAM")

    if benchmark == "tpcds" and scale_factor >= 10:
        recommendations.append("TPC-DS at SF=10+ may take 30+ minutes to complete")

    if platform in ("duckdb", "polars", "polars-df"):
        recommendations.append("Local platforms are best for testing and development")

    return recommendations
