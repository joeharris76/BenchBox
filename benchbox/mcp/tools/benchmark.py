"""Benchmark execution tools for BenchBox MCP server.

Provides tools for running benchmarks, validating configurations,
and performing dry runs.

This module uses the public BenchBox API (benchbox.*, benchbox.platforms.*)
and the core benchmark registry for all benchmark operations.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from benchbox.core.benchmark_registry import (
    get_all_benchmarks,
    get_benchmark_class,
)
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


def _get_platform_adapter(platform: str, **config):
    """Get platform adapter from public API.

    Uses benchbox.platforms factory functions.
    """
    from benchbox.platforms import get_dataframe_adapter, get_platform_adapter, is_dataframe_platform

    platform_lower = platform.lower()

    if is_dataframe_platform(platform_lower):
        return get_dataframe_adapter(platform_lower, **config)
    else:
        return get_platform_adapter(platform_lower, **config)


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
        execution_id = f"mcp_{uuid.uuid4().hex[:8]}"
        start_time = datetime.now()

        try:
            # Validate benchmark exists
            benchmark_lower = benchmark.lower()
            all_benchmarks = get_all_benchmarks()

            if benchmark_lower not in all_benchmarks:
                error_response = make_not_found_error("benchmark", benchmark, available=list(all_benchmarks.keys()))
                error_response["execution_id"] = execution_id
                error_response["status"] = "failed"
                return error_response

            # Get benchmark class using public API
            benchmark_class = get_benchmark_class(benchmark_lower)
            if benchmark_class is None:
                error_response = make_error(
                    ErrorCode.DEPENDENCY_MISSING,
                    f"Benchmark '{benchmark}' requires additional dependencies",
                    details={"benchmark": benchmark},
                )
                error_response["execution_id"] = execution_id
                error_response["status"] = "failed"
                return error_response

            # Create benchmark instance using public API
            benchmark_instance = benchmark_class(scale_factor=scale_factor)

            # Get platform adapter using public API
            try:
                adapter = _get_platform_adapter(platform)
            except (ValueError, ImportError) as e:
                error_response = make_error(
                    ErrorCode.VALIDATION_UNSUPPORTED_PLATFORM,
                    str(e),
                    details={"platform": platform},
                )
                error_response["execution_id"] = execution_id
                error_response["status"] = "failed"
                return error_response

            # Parse query subset if provided
            query_subset = None
            if queries:
                query_subset = [q.strip() for q in queries.split(",")]

            # Run benchmark using public API: run_with_platform()
            result = benchmark_instance.run_with_platform(
                adapter,
                query_subset=query_subset,
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
                # Extract summary metrics from BenchmarkResults
                response["summary"] = {
                    "total_queries": getattr(result, "total_queries", 0),
                    "successful_queries": getattr(result, "successful_queries", 0),
                    "failed_queries": getattr(result, "failed_queries", 0),
                    "total_execution_time": getattr(result, "total_execution_time", 0),
                }

                # Add per-query results (limited for readability)
                if hasattr(result, "query_results") and result.query_results:
                    query_results = []
                    for qr in result.query_results[:20]:
                        query_results.append(
                            {
                                "id": qr.get("query_id", "unknown"),
                                "runtime_ms": int(qr.get("execution_time", 0) * 1000),
                                "status": qr.get("status", "unknown"),
                            }
                        )
                    if query_results:
                        response["queries"] = query_results
                        if len(result.query_results) > 20:
                            response["queries_note"] = f"Showing 20 of {len(result.query_results)} queries"

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
        from benchbox.core.config import BenchmarkConfig, DatabaseConfig
        from benchbox.core.dryrun import DryRunExecutor
        from benchbox.core.platform_registry import PlatformRegistry
        from benchbox.core.system import SystemProfiler
        from benchbox.platforms import is_dataframe_platform

        try:
            benchmark_lower = benchmark.lower()
            all_benchmarks = get_all_benchmarks()

            # Validate benchmark exists
            if benchmark_lower not in all_benchmarks:
                error_response = make_not_found_error("benchmark", benchmark, available=list(all_benchmarks.keys()))
                error_response["status"] = "error"
                return error_response

            # Validate platform exists
            platform_lower = platform.lower()
            base_platform = platform_lower.replace("-df", "")
            platform_info = PlatformRegistry.get_platform_info(base_platform)

            warnings: list[str] = []
            if platform_info is None:
                warnings.append(f"Unknown platform: {platform}")
            elif not platform_info.available:
                warnings.append(
                    f"Platform '{platform}' dependencies not installed: {platform_info.installation_command}"
                )

            # Get benchmark metadata for display name
            meta = all_benchmarks[benchmark_lower]
            display_name = meta.get("display_name", benchmark_lower.upper())

            # Build core configs for DryRunExecutor
            benchmark_config = BenchmarkConfig(
                name=benchmark_lower,
                display_name=display_name,
                scale_factor=scale_factor,
                queries=[q.strip() for q in queries.split(",")] if queries else None,
            )

            # Determine execution mode
            execution_mode = "dataframe" if is_dataframe_platform(platform_lower) else "sql"

            database_config = DatabaseConfig(
                type=platform_lower,
                name=f"mcp_dryrun_{platform_lower}",
                execution_mode=execution_mode,
            )

            # Get system profile
            profiler = SystemProfiler()
            system_profile = profiler.get_system_profile()

            # Execute dry run using core executor
            executor = DryRunExecutor(output_dir=None)
            result = executor.execute_dry_run(benchmark_config, system_profile, database_config)

            # Add any warnings from result
            warnings.extend(result.warnings)

            # Build MCP response from DryRunResult
            response: dict[str, Any] = {
                "status": "dry_run",
                "platform": platform,
                "benchmark": benchmark,
                "scale_factor": scale_factor,
                "execution_mode": result.execution_mode,
            }

            # Add execution plan from query_preview
            if result.query_preview:
                query_ids = result.query_preview.get("queries", [])
                response["execution_plan"] = {
                    "phases": ["load", "power"],
                    "total_queries": result.query_preview.get("query_count", 0),
                    "query_ids": query_ids[:30],
                    "query_ids_truncated": len(query_ids) > 30,
                    "test_execution_type": result.query_preview.get("test_execution_type", "standard"),
                    "execution_context": result.query_preview.get("execution_context", ""),
                }

            # Add resource estimates
            if result.estimated_resources:
                data_size_mb = result.estimated_resources.get("estimated_data_size_mb", 0)
                response["resource_estimates"] = {
                    "data_size_gb": round(data_size_mb / 1024, 2),
                    "memory_recommended_gb": max(2, round(data_size_mb / 1024 * 2, 1)),
                    "disk_space_recommended_gb": max(1, round(data_size_mb / 1024 * 3, 1)),
                    "cpu_cores_available": result.estimated_resources.get("cpu_cores_available", 1),
                    "memory_gb_available": result.estimated_resources.get("memory_gb_available", 4),
                }

            # Add notes and warnings
            notes = [
                "This is a preview - no benchmark will be executed",
                "Actual resource usage may vary based on platform and configuration",
            ]
            if platform_info and base_platform in ("snowflake", "bigquery", "databricks", "redshift"):
                notes.append("For cloud platforms, ensure credentials are configured")

            response["notes"] = notes
            if warnings:
                response["warnings"] = warnings

            return response

        except Exception as e:
            logger.exception(f"Dry run failed: {e}")
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
        from benchbox.core.platform_registry import PlatformRegistry
        from benchbox.platforms import is_dataframe_platform

        errors = []
        warnings = []

        # Validate platform using public API
        platform_lower = platform.lower()
        base_platform = platform_lower.replace("-df", "")

        # Check if platform exists
        info = PlatformRegistry.get_platform_info(base_platform)
        if info is None:
            errors.append(f"Unknown platform: {platform}")
        elif not info.available:
            errors.append(f"Platform '{platform}' dependencies not installed: {info.installation_command}")

        # Validate benchmark
        benchmark_lower = benchmark.lower()
        all_benchmarks = get_all_benchmarks()

        if benchmark_lower not in all_benchmarks:
            errors.append(f"Unknown benchmark: {benchmark}. Available: {', '.join(all_benchmarks.keys())}")
        else:
            meta = all_benchmarks[benchmark_lower]
            min_scale = meta.get("min_scale", 0.01)
            if scale_factor < min_scale:
                warnings.append(f"{benchmark} requires scale factor >= {min_scale}")

        # Validate scale factor
        if scale_factor <= 0:
            errors.append(f"Scale factor must be positive, got: {scale_factor}")
        elif scale_factor < 0.01:
            warnings.append(f"Scale factor {scale_factor} is very small, results may not be meaningful")

        # Check DataFrame support
        if benchmark_lower in all_benchmarks:
            meta = all_benchmarks[benchmark_lower]
            if is_dataframe_platform(platform_lower) and not meta.get("supports_dataframe", False):
                errors.append(f"DataFrame mode does not support {benchmark} benchmark")

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
        benchmark_lower = benchmark.lower()
        all_benchmarks = get_all_benchmarks()

        # Validate benchmark exists
        if benchmark_lower not in all_benchmarks:
            error_response = make_not_found_error("benchmark", benchmark, available=list(all_benchmarks.keys()))
            return error_response

        try:
            # Get benchmark class using public API
            benchmark_class = get_benchmark_class(benchmark_lower)
            if benchmark_class is None:
                return make_error(
                    ErrorCode.DEPENDENCY_MISSING,
                    f"Benchmark '{benchmark}' requires additional dependencies",
                    details={"benchmark": benchmark},
                )

            # Create instance with minimal scale factor
            bm = benchmark_class(scale_factor=0.01)

            # Normalize query ID (handle "Q1" vs "1" format)
            normalized_id = query_id.upper().lstrip("Q")
            if not normalized_id.isdigit():
                normalized_id = query_id  # Keep original if not numeric

            # Try to get query using public API
            query_sql = None
            try:
                # Try integer first, then string
                try:
                    query_sql = bm.get_query(int(normalized_id))
                except (ValueError, TypeError):
                    query_sql = bm.get_query(normalized_id)
            except (KeyError, ValueError):
                # Try with original query_id
                import contextlib

                with contextlib.suppress(KeyError, ValueError):
                    query_sql = bm.get_query(query_id)

            # Build response
            meta = all_benchmarks[benchmark_lower]

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

            # Add complexity hints based on benchmark
            response["complexity_hints"] = _get_query_complexity_hints(benchmark_lower, normalized_id)

            # Add benchmark metadata context
            response["benchmark_info"] = {
                "display_name": meta.get("display_name", benchmark_lower),
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


# Tool annotations for data generation (creates files)
DATAGEN_ANNOTATIONS = ToolAnnotations(
    title="Generate benchmark data",
    readOnlyHint=False,  # Creates data files
    destructiveHint=False,  # Does not delete existing data
    idempotentHint=True,  # Same params = same data
    openWorldHint=False,  # Local operation
)


def register_datagen_tools(mcp: FastMCP) -> None:
    """Register data generation tools with the MCP server.

    Args:
        mcp: The FastMCP server instance to register tools with.
    """
    from pathlib import Path

    @mcp.tool(annotations=DATAGEN_ANNOTATIONS)
    def generate_data(
        benchmark: str,
        scale_factor: float = 0.01,
        format: str = "parquet",
        force: bool = False,
    ) -> dict[str, Any]:
        """Generate benchmark data without running queries.

        Creates benchmark data files that can be reused across multiple runs.
        Data is stored in the standard benchmark_runs/data directory.

        Args:
            benchmark: Benchmark name (e.g., 'tpch', 'tpcds', 'clickbench')
            scale_factor: Data scale factor (0.01 for testing, 1+ for production)
            format: Data format ('parquet' or 'csv')
            force: Force regeneration even if data exists

        Returns:
            Data generation status with location and statistics.

        Example:
            generate_data(benchmark="tpch", scale_factor=0.1)
            generate_data(benchmark="tpcds", scale_factor=1, force=True)
        """
        # Validate format
        valid_formats = ["parquet", "csv"]
        format_lower = format.lower()
        if format_lower not in valid_formats:
            return make_error(
                ErrorCode.VALIDATION_INVALID_FORMAT,
                f"Invalid format: {format}",
                details={"valid_formats": valid_formats},
                suggestion=f"Use one of: {', '.join(valid_formats)}",
            )

        # Validate benchmark
        benchmark_lower = benchmark.lower()
        all_benchmarks = get_all_benchmarks()

        if benchmark_lower not in all_benchmarks:
            return make_not_found_error("benchmark", benchmark, available=list(all_benchmarks.keys()))

        # Validate scale factor
        if scale_factor <= 0:
            return make_error(
                ErrorCode.VALIDATION_INVALID_SCALE_FACTOR,
                f"Scale factor must be positive: {scale_factor}",
                details={"scale_factor": scale_factor},
            )

        meta = all_benchmarks[benchmark_lower]
        min_scale = meta.get("min_scale", 0.01)
        if scale_factor < min_scale:
            return make_error(
                ErrorCode.VALIDATION_INVALID_SCALE_FACTOR,
                f"{benchmark} requires scale factor >= {min_scale}",
                details={"scale_factor": scale_factor, "minimum": min_scale},
            )

        try:
            # Get benchmark class
            benchmark_class = get_benchmark_class(benchmark_lower)
            if benchmark_class is None:
                return make_error(
                    ErrorCode.DEPENDENCY_MISSING,
                    f"Benchmark '{benchmark}' requires additional dependencies",
                    details={"benchmark": benchmark},
                )

            # Determine data directory
            data_dir = Path("benchmark_runs/data") / benchmark_lower / f"sf{scale_factor}"

            # Check if data already exists
            if data_dir.exists() and not force:
                # Count existing files
                existing_files = list(data_dir.glob(f"*.{format_lower}"))
                if existing_files:
                    total_size = sum(f.stat().st_size for f in existing_files)
                    return {
                        "status": "exists",
                        "benchmark": benchmark_lower,
                        "scale_factor": scale_factor,
                        "format": format_lower,
                        "data_path": str(data_dir),
                        "file_count": len(existing_files),
                        "total_size_mb": round(total_size / (1024 * 1024), 2),
                        "message": "Data already exists. Use force=True to regenerate.",
                    }

            # Create benchmark instance
            bm = benchmark_class(scale_factor=scale_factor)

            # Generate data using the benchmark's generate method
            data_dir.mkdir(parents=True, exist_ok=True)

            # Most benchmarks use generate_data() method
            if hasattr(bm, "generate_data"):
                bm.generate_data(output_dir=data_dir, format=format_lower)
            elif hasattr(bm, "generate"):
                bm.generate(output_dir=data_dir)
            else:
                return make_error(
                    ErrorCode.INTERNAL_ERROR,
                    f"Benchmark '{benchmark}' does not support standalone data generation",
                    details={"benchmark": benchmark},
                    suggestion="Use run_benchmark with phases=['generate', 'load'] instead",
                )

            # Get statistics on generated files
            generated_files = list(data_dir.glob(f"*.{format_lower}"))
            if not generated_files:
                # Try without format filter in case different extension
                generated_files = list(data_dir.glob("*.*"))

            total_size = sum(f.stat().st_size for f in generated_files if f.is_file())

            return {
                "status": "generated",
                "benchmark": benchmark_lower,
                "scale_factor": scale_factor,
                "format": format_lower,
                "data_path": str(data_dir),
                "file_count": len(generated_files),
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "files": [f.name for f in generated_files[:20]],
                "files_truncated": len(generated_files) > 20,
            }

        except Exception as e:
            logger.exception(f"Data generation failed: {e}")
            return make_error(
                ErrorCode.BENCHMARK_DATA_GENERATION_FAILED,
                f"Data generation failed: {e}",
                details={
                    "benchmark": benchmark,
                    "scale_factor": scale_factor,
                    "exception_type": type(e).__name__,
                },
            )
