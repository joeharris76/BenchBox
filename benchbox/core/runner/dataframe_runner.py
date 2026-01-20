"""DataFrame benchmark lifecycle runner.

This module provides the execution lifecycle for DataFrame mode benchmarks,
separate from the SQL mode runner. It handles:
- Data loading into DataFrames (CSV/TBL/Parquet)
- Query execution via DataFrame operations
- Result collection and formatting

Unlike SQL mode which uses database connections, DataFrame mode works
entirely in-memory with platform-specific DataFrame libraries.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from benchbox.core.config import BenchmarkConfig, SystemProfile
from benchbox.core.constants import (
    GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS,
    GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS,
)
from benchbox.core.dataframe import (
    check_sufficient_memory,
    format_memory_warning,
    validate_scale_factor,
)
from benchbox.core.dataframe.data_loader import DataFrameDataLoader, get_tpch_column_names
from benchbox.core.results.models import (
    BenchmarkResults,
    DataLoadingPhase,
    ExecutionPhases,
    SetupPhase,
    TableLoadingStats,
)
from benchbox.monitoring import PerformanceMonitor
from benchbox.utils.printing import quiet_console
from benchbox.utils.verbosity import VerbositySettings

logger = logging.getLogger(__name__)

console = quiet_console


@dataclass
class DataFramePhases:
    """Phases for DataFrame benchmark execution."""

    load: bool = True
    execute: bool = True


@dataclass
class DataFrameRunOptions:
    """Options for DataFrame benchmark execution."""

    ignore_memory_warnings: bool = False
    force_regenerate: bool = False
    prefer_parquet: bool = True
    cache_dir: Path | None = None


def run_dataframe_benchmark(
    benchmark_config: BenchmarkConfig,
    adapter: Any,
    system_profile: SystemProfile | None = None,
    *,
    data_dir: Path | None = None,
    phases: DataFramePhases | None = None,
    options: DataFrameRunOptions | None = None,
    benchmark_instance: Any | None = None,
    verbosity: VerbositySettings | None = None,
    monitor: PerformanceMonitor | None = None,
) -> BenchmarkResults:
    """Run a benchmark using DataFrame mode execution.

    Args:
        benchmark_config: Benchmark configuration
        adapter: DataFrame platform adapter (PolarsDataFrameAdapter, PandasDataFrameAdapter)
        system_profile: System profile for resource information
        data_dir: Directory containing benchmark data files
        phases: Which phases to execute
        options: DataFrame-specific execution options
        benchmark_instance: Pre-constructed benchmark instance
        verbosity: Verbosity settings
        monitor: Performance monitor

    Returns:
        BenchmarkResults with execution details
    """
    phases = phases or DataFramePhases()
    options = options or DataFrameRunOptions()

    start_time = time.time()
    execution_id = uuid.uuid4().hex[:8]

    platform_name = adapter.platform_name
    scale_factor = getattr(benchmark_config, "scale_factor", 1.0)

    logger.info(f"Starting DataFrame benchmark: {benchmark_config.name}")
    logger.info(f"Platform: {platform_name}, Scale Factor: {scale_factor}")

    # Initialize result tracking
    query_results: list[dict[str, Any]] = []
    table_stats: dict[str, int] = {}
    load_time = 0.0
    setup_phase = None

    try:
        # Pre-execution memory check
        if not options.ignore_memory_warnings:
            memory_check = check_sufficient_memory(benchmark_config.name, scale_factor, platform_name.lower())
            if not memory_check.is_safe:
                warning = format_memory_warning(memory_check)
                logger.warning(warning)
                # Continue with warning - let the user handle OOM if it happens

        # Scale factor validation
        is_valid, sf_warning = validate_scale_factor(scale_factor, platform_name.lower())
        if sf_warning:
            logger.warning(sf_warning)

        # Create execution context
        ctx = adapter.create_context()

        # Load phase
        if phases.load:
            load_start = time.time()
            table_stats, per_table_stats = _load_dataframe_data(
                adapter=adapter,
                ctx=ctx,
                benchmark_config=benchmark_config,
                benchmark_instance=benchmark_instance,
                data_dir=data_dir,
                options=options,
            )
            load_time = time.time() - load_start
            logger.info(f"Data loading completed in {load_time:.2f}s")

            setup_phase = SetupPhase(
                data_loading=DataLoadingPhase(
                    duration_ms=int(load_time * 1000),
                    status="SUCCESS",
                    total_rows_loaded=sum(table_stats.values()),
                    tables_loaded=len(table_stats),
                    per_table_stats=per_table_stats,
                )
            )

        # Execute phase
        if phases.execute:
            query_results = _execute_dataframe_queries(
                adapter=adapter,
                ctx=ctx,
                benchmark_config=benchmark_config,
                benchmark_instance=benchmark_instance,
                monitor=monitor,
            )

        # Compute summary metrics
        total_time = time.time() - start_time
        successful = sum(1 for r in query_results if r.get("status") == "SUCCESS")
        failed = len(query_results) - successful

        total_exec_time = sum(r.get("execution_time", 0) for r in query_results)
        avg_time = total_exec_time / len(query_results) if query_results else 0.0

        return BenchmarkResults(
            benchmark_name=getattr(benchmark_config, "display_name", benchmark_config.name),
            platform=f"{platform_name} (DataFrame)",
            scale_factor=scale_factor,
            execution_id=execution_id,
            timestamp=datetime.now(),
            duration_seconds=total_time,
            total_queries=len(query_results),
            successful_queries=successful,
            failed_queries=failed,
            query_results=query_results,
            total_execution_time=total_exec_time,
            average_query_time=avg_time,
            data_loading_time=load_time,
            total_rows_loaded=sum(table_stats.values()),
            table_statistics=table_stats,
            execution_phases=ExecutionPhases(setup=setup_phase) if setup_phase else None,
            execution_metadata={
                "execution_mode": "dataframe",
                "dataframe_platform": platform_name,
                "adapter_info": adapter.get_platform_info() if hasattr(adapter, "get_platform_info") else {},
            },
            validation_status="PASSED" if failed == 0 else "FAILED",
            platform_info=adapter.get_platform_info() if hasattr(adapter, "get_platform_info") else {},
            test_execution_type=getattr(benchmark_config, "test_execution_type", "standard"),
        )

    except Exception as e:
        logger.error(f"DataFrame benchmark failed: {e}")
        return BenchmarkResults(
            benchmark_name=getattr(benchmark_config, "display_name", benchmark_config.name),
            platform=f"{platform_name} (DataFrame)",
            scale_factor=scale_factor,
            execution_id=execution_id,
            timestamp=datetime.now(),
            duration_seconds=time.time() - start_time,
            total_queries=0,
            successful_queries=0,
            failed_queries=0,
            query_results=[],
            validation_status="FAILED",
            validation_details={"error": str(e), "error_type": type(e).__name__},
            execution_metadata={
                "execution_mode": "dataframe",
                "dataframe_platform": platform_name,
                "error": str(e),
            },
        )


def _load_dataframe_data(
    adapter: Any,
    ctx: Any,
    benchmark_config: BenchmarkConfig,
    benchmark_instance: Any | None,
    data_dir: Path | None,
    options: DataFrameRunOptions,
) -> tuple[dict[str, int], dict[str, TableLoadingStats]]:
    """Load benchmark data into DataFrame context.

    Returns:
        Tuple of (table_stats, per_table_loading_stats)
    """
    platform_name = adapter.platform_name.lower()

    # Create data loader
    loader = DataFrameDataLoader(
        platform=platform_name,
        cache_dir=options.cache_dir,
        prefer_parquet=options.prefer_parquet,
        force_regenerate=options.force_regenerate,
    )

    # Get data paths
    if benchmark_instance and hasattr(benchmark_instance, "tables") and benchmark_instance.tables:
        # Use benchmark's pre-generated data
        data_paths = {
            name: Path(path) if not isinstance(path, Path) else path for name, path in benchmark_instance.tables.items()
        }
        logger.info(f"Using benchmark-provided data: {len(data_paths)} tables")
    elif data_dir and data_dir.exists():
        # Use provided data directory
        data_paths = loader._discover_files(data_dir)
        logger.info(f"Discovered data in {data_dir}: {len(data_paths)} tables")
    else:
        raise ValueError("No data source available. Either provide data_dir or generate data first.")

    # Get schema info for column names
    benchmark_name = benchmark_config.name.lower()
    column_names_map = get_tpch_column_names() if "tpch" in benchmark_name or "tpc-h" in benchmark_name else {}

    # Load tables
    table_stats: dict[str, int] = {}
    per_table_stats: dict[str, TableLoadingStats] = {}

    for table_name, file_path in data_paths.items():
        load_start = time.time()
        try:
            column_names = column_names_map.get(table_name.lower())
            row_count = adapter.load_table(ctx, table_name.lower(), [file_path], column_names=column_names)
            load_time_ms = int((time.time() - load_start) * 1000)

            table_stats[table_name] = row_count
            per_table_stats[table_name] = TableLoadingStats(
                rows=row_count,
                load_time_ms=load_time_ms,
                status="SUCCESS",
            )
            logger.debug(f"Loaded {table_name}: {row_count:,} rows in {load_time_ms}ms")

        except Exception as e:
            load_time_ms = int((time.time() - load_start) * 1000)
            per_table_stats[table_name] = TableLoadingStats(
                rows=0,
                load_time_ms=load_time_ms,
                status="FAILED",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            logger.error(f"Failed to load {table_name}: {e}")

    return table_stats, per_table_stats


def _execute_dataframe_queries(
    adapter: Any,
    ctx: Any,
    benchmark_config: BenchmarkConfig,
    benchmark_instance: Any | None,
    monitor: PerformanceMonitor | None,
) -> list[dict[str, Any]]:
    """Execute DataFrame queries with warmup and measurement iterations.

    Follows TPC power test methodology:
    1. Warmup iterations (default 1): Prime caches, not included in results
    2. Measurement iterations (default 3): Timed runs, results aggregated

    Each iteration uses a different stream_id for query permutation, matching
    the SQL runner behavior. This ensures each iteration executes queries in
    a different order per TPC-H/TPC-DS specification.

    Returns:
        List of query result dictionaries with timing from measurement iterations
    """
    # Get warmup and iteration counts from config options
    options = getattr(benchmark_config, "options", {}) or {}
    warmup_iterations = int(
        options.get("power_warmup_iterations", GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS)
        or GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS
    )
    measurement_iterations = int(
        options.get("power_iterations", GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS)
        or GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS
    )

    # Get query subset filter if specified
    query_subset = getattr(benchmark_config, "queries", None)
    query_filter: set[str] | None = None
    if query_subset:
        query_filter = {str(q) for q in query_subset}

    # Get initial query set to determine total count
    initial_queries = _get_queries_for_benchmark(benchmark_config, benchmark_instance, stream_id=0)
    if query_filter:
        initial_queries = [q for q in initial_queries if q.query_id in query_filter]

    if not initial_queries:
        logger.warning("No queries found for execution")
        return []

    total_queries = len(initial_queries)
    logger.info(f"Executing {total_queries} queries")
    logger.info(f"Warmup iterations: {warmup_iterations}, Measurement iterations: {measurement_iterations}")

    # Execute warmup iterations (not timed, not included in results)
    # Each warmup uses a different stream_id (0, 1, 2, ...)
    if warmup_iterations > 0:
        console.print(f"\n[yellow]Running {warmup_iterations} warmup iteration(s)...[/yellow]")
        for warmup_iter in range(warmup_iterations):
            warmup_stream_id = warmup_iter
            warmup_queries = _get_queries_for_benchmark(
                benchmark_config, benchmark_instance, stream_id=warmup_stream_id
            )
            if query_filter:
                warmup_queries = [q for q in warmup_queries if q.query_id in query_filter]

            console.print(f"[dim]Warmup {warmup_iter + 1}/{warmup_iterations} (stream {warmup_stream_id})[/dim]")
            for query in warmup_queries:
                try:
                    adapter.execute_query(ctx, query)
                except Exception as e:
                    logger.warning(f"Warmup query {query.query_id} failed: {e}")

    # Execute measurement iterations
    # Each measurement iteration uses a different stream_id (continues from warmup)
    console.print(
        f"\n[cyan]Running {measurement_iterations} measurement iteration(s) with {total_queries} queries each...[/cyan]"
    )

    # Collect results across all measurement iterations
    all_iteration_results: dict[str, list[dict[str, Any]]] = {}
    for q in initial_queries:
        all_iteration_results[q.query_id] = []

    for iteration in range(measurement_iterations):
        # Each measurement iteration uses a unique stream_id
        # Stream IDs continue from where warmup left off
        measurement_stream_id = warmup_iterations + iteration
        measurement_queries = _get_queries_for_benchmark(
            benchmark_config, benchmark_instance, stream_id=measurement_stream_id
        )
        if query_filter:
            measurement_queries = [q for q in measurement_queries if q.query_id in query_filter]

        console.print(
            f"\n[green]Iteration {iteration + 1}/{measurement_iterations} (stream {measurement_stream_id})[/green]"
        )

        for i, query in enumerate(measurement_queries, 1):
            console.print(f"[blue]Executing query {i}/{total_queries}: {query.query_id}[/blue]")

            def execute_single_query(q=query):
                """Execute a single query and return the result."""
                try:
                    result = adapter.execute_query(ctx, q)
                    all_iteration_results[q.query_id].append(result)
                except Exception as e:
                    logger.error(f"Query {q.query_id} failed: {e}")
                    all_iteration_results[q.query_id].append(
                        {
                            "query_id": q.query_id,
                            "status": "FAILED",
                            "error": str(e),
                            "execution_time_seconds": 0.0,
                        }
                    )

            # Use time_operation context manager for consistent monitoring
            if monitor:
                with monitor.time_operation(f"query_{query.query_id}_iter{iteration + 1}"):
                    execute_single_query()
            else:
                execute_single_query()

    # Aggregate results: use minimum time across iterations (TPC methodology)
    query_results: list[dict[str, Any]] = []
    for query in initial_queries:
        iteration_results = all_iteration_results[query.query_id]
        if not iteration_results:
            continue

        # Find the best (fastest successful) run
        successful_runs = [r for r in iteration_results if r.get("status") == "SUCCESS"]

        if successful_runs:
            # Use minimum execution time (TPC standard)
            best_run = min(successful_runs, key=lambda r: r.get("execution_time_seconds", float("inf")))
            best_run = dict(best_run)  # Copy to avoid modifying original

            # Add iteration metadata
            times = [r.get("execution_time_seconds", 0) for r in successful_runs]
            best_run["iterations"] = measurement_iterations
            best_run["iteration_times"] = times
            best_run["min_time"] = min(times) if times else 0
            best_run["max_time"] = max(times) if times else 0
            best_run["avg_time"] = sum(times) / len(times) if times else 0

            query_results.append(best_run)
        else:
            # All iterations failed - report the last failure
            query_results.append(iteration_results[-1])

    return query_results


def _get_queries_for_benchmark(
    benchmark_config: BenchmarkConfig,
    benchmark_instance: Any | None,
    stream_id: int | None = None,
) -> list[Any]:
    """Get DataFrame queries for a benchmark in proper execution order.

    For TPC-H power tests, queries are returned in the TPC-H specification's
    permutation order based on stream_id. This is required for TPC-H compliance.

    Args:
        benchmark_config: Benchmark configuration
        benchmark_instance: Optional benchmark instance
        stream_id: Stream ID for query permutation (default: from config or 0).
                   Each stream ID produces a different query execution order
                   per TPC-H/TPC-DS specification.

    Returns:
        List of DataFrameQuery objects in execution order for the given stream.
    """
    benchmark_name = benchmark_config.name.lower()

    # Use provided stream_id, or fall back to config, or default to 0
    if stream_id is None:
        stream_id = getattr(benchmark_config, "stream_id", 0)

    # Check registry for pre-defined queries
    if "tpch" in benchmark_name or "tpc-h" in benchmark_name:
        from benchbox.core.tpch.dataframe_queries import TPCH_DATAFRAME_QUERIES
        from benchbox.core.tpch.streams import TPCHStreams

        # Get query permutation for this stream (TPC-H specification)
        query_permutation = TPCHStreams.PERMUTATION_MATRIX[stream_id % len(TPCHStreams.PERMUTATION_MATRIX)]

        # Return queries in permuted order
        queries = []
        for query_num in query_permutation:
            query_id = f"Q{query_num}"
            query = TPCH_DATAFRAME_QUERIES.get(query_id)
            if query:
                queries.append(query)
            else:
                logger.warning(f"Query {query_id} not found in TPC-H DataFrame registry")
        return queries

    elif "tpcds" in benchmark_name or "tpc-ds" in benchmark_name:
        from benchbox.core.tpcds.dataframe_queries import TPCDS_DATAFRAME_QUERIES
        from benchbox.core.tpcds.streams import PermutationMode, TPCDSPermutationGenerator

        # Get available query IDs from the registry
        available_query_ids = [int(qid[1:]) for qid in TPCDS_DATAFRAME_QUERIES.get_query_ids()]

        # Generate permutation using TPC-DS standard algorithm with stream-based seed
        generator = TPCDSPermutationGenerator(seed=42 + stream_id)
        query_permutation = generator.generate_permutation(available_query_ids, PermutationMode.TPCDS_STANDARD)

        # Return queries in permuted order
        queries = []
        for query_num in query_permutation:
            query_id = f"Q{query_num}"
            query = TPCDS_DATAFRAME_QUERIES.get(query_id)
            if query:
                queries.append(query)
            else:
                logger.warning(f"Query {query_id} not found in TPC-DS DataFrame registry")
        return queries

    # Try benchmark instance
    if benchmark_instance and hasattr(benchmark_instance, "get_dataframe_queries"):
        return benchmark_instance.get_dataframe_queries()

    return []


def is_dataframe_execution(database_config: Any) -> bool:
    """Check if execution should use DataFrame mode.

    DataFrame mode is determined by platform naming convention:
    - polars-df: DataFrame mode
    - pandas-df: DataFrame mode
    - polars: SQL mode (if exists)
    - duckdb: SQL mode

    Args:
        database_config: Database configuration with platform type

    Returns:
        True if DataFrame mode should be used
    """
    if database_config is None:
        return False

    platform_type = getattr(database_config, "type", "").lower()

    # DataFrame platforms end with -df
    return platform_type.endswith("-df")


def get_execution_mode(database_config: Any) -> str:
    """Get the execution mode for a platform.

    Args:
        database_config: Database configuration

    Returns:
        "dataframe" or "sql"
    """
    if is_dataframe_execution(database_config):
        return "dataframe"
    return "sql"
