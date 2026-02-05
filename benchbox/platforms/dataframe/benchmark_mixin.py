"""Benchmark execution mixin for DataFrame adapters.

This module provides the BenchmarkExecutionMixin that adds run_benchmark()
capability to DataFrame adapters, enabling unified interface with SQL adapters.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import time
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

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
from benchbox.core.results import (
    BenchmarkInfoInput,
    ResultBuilder,
    build_platform_info,
    normalize_query_result,
)
from benchbox.core.results.builder import RunConfigInput, normalize_benchmark_id
from benchbox.core.results.models import (
    BenchmarkResults,
    TableLoadingStats,
)
from benchbox.utils.printing import quiet_console

if TYPE_CHECKING:
    from benchbox.core.dataframe.query import DataFrameQuery

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
    verbose: bool = False
    very_verbose: bool = False


class DataLoadingError(RuntimeError):
    """Raised when DataFrame data loading fails critically."""

    def __init__(
        self,
        message: str,
        table_stats: dict[str, int] | None = None,
        per_table_stats: dict[str, TableLoadingStats] | None = None,
    ) -> None:
        super().__init__(message)
        self.table_stats = table_stats or {}
        self.per_table_stats = per_table_stats or {}


class BenchmarkExecutionMixin:
    """Mixin providing run_benchmark() for DataFrame adapters.

    This mixin adds unified benchmark execution capability to DataFrame
    adapters, enabling them to be used interchangeably with SQL adapters.

    The adapter must provide:
    - platform_name: str property
    - family: str property ("expression" or "pandas")
    - create_context() method
    - load_table() method
    - execute_query() method
    - get_platform_info() method (optional)
    """

    # These must be provided by the adapter class
    platform_name: str
    family: str

    @abstractmethod
    def create_context(self) -> Any:
        """Create execution context."""

    @abstractmethod
    def load_table(
        self,
        ctx: Any,
        table_name: str,
        file_paths: list[Path],
        column_names: list[str] | None = None,
    ) -> int:
        """Load a table into context."""

    @abstractmethod
    def execute_query(
        self,
        ctx: Any,
        query: DataFrameQuery,
        query_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute a query."""

    def get_platform_info(self) -> dict[str, Any]:
        """Get platform information. Override in subclass for details."""
        return {"platform": self.platform_name, "family": self.family}

    def run_benchmark(
        self,
        benchmark: Any,
        *,
        benchmark_config: BenchmarkConfig | None = None,
        system_profile: SystemProfile | None = None,
        data_dir: Path | None = None,
        phases: DataFramePhases | None = None,
        options: DataFrameRunOptions | None = None,
        monitor: Any | None = None,
        **run_config: Any,
    ) -> BenchmarkResults:
        """Run a benchmark using DataFrame mode execution.

        This method provides a unified interface matching SQL adapters,
        allowing DataFrame adapters to be used interchangeably.

        Args:
            benchmark: Benchmark instance with query definitions
            benchmark_config: Optional explicit benchmark configuration.
                             If not provided, extracted from benchmark instance.
            system_profile: System profile for resource information
            data_dir: Directory containing benchmark data files
            phases: Which phases to execute (default: load and execute)
            options: DataFrame-specific execution options
            monitor: Performance monitor for tracking
            **run_config: Additional configuration options

        Returns:
            BenchmarkResults with execution details
        """
        phases = phases or DataFramePhases()
        if isinstance(options, dict):
            options = DataFrameRunOptions(**options)
        else:
            options = options or DataFrameRunOptions()

        # Extract benchmark_config from benchmark if not provided
        if benchmark_config is None:
            benchmark_config = self._extract_benchmark_config(benchmark, run_config)

        platform_name = self.platform_name
        scale_factor = getattr(benchmark_config, "scale_factor", 1.0)

        logger.info(f"Starting DataFrame benchmark: {benchmark_config.name}")
        logger.info(f"Platform: {platform_name}, Scale Factor: {scale_factor}")

        # Build platform info using standardized extractor
        platform_info = build_platform_info(self, execution_mode="dataframe")

        # Create result builder
        builder = ResultBuilder(
            benchmark=BenchmarkInfoInput(
                name=benchmark_config.name,
                scale_factor=scale_factor,
                test_type=getattr(benchmark_config, "test_execution_type", "power"),
                benchmark_id=normalize_benchmark_id(benchmark_config.name),
                display_name=getattr(benchmark_config, "display_name", benchmark_config.name),
            ),
            platform=platform_info,
        )
        builder.mark_started()

        options_map = getattr(benchmark_config, "options", {}) or {}
        builder.set_run_config(
            RunConfigInput(
                compression_type=options_map.get("compression_type"),
                compression_level=options_map.get("compression_level"),
                seed=options_map.get("seed"),
                phases=options_map.get("phases"),
                query_subset=getattr(benchmark_config, "queries", None),
                tuning_mode=options_map.get("tuning_mode"),
                tuning_config=options_map.get("df_tuning_config"),
            )
        )

        # Initialize result tracking
        query_results: list[dict[str, Any]] = []
        table_stats: dict[str, int] = {}
        load_time = 0.0

        try:
            # Pre-execution memory check
            if not options.ignore_memory_warnings:
                memory_check = check_sufficient_memory(benchmark_config.name, scale_factor, platform_name.lower())
                if not memory_check.is_safe:
                    warning = format_memory_warning(memory_check)
                    logger.warning(warning)

            # Scale factor validation
            is_valid, sf_warning = validate_scale_factor(scale_factor, platform_name.lower())
            if sf_warning:
                logger.warning(sf_warning)

            # Create execution context
            ctx = self.create_context()

            # Load phase
            if phases.load:
                load_start = time.time()
                try:
                    table_stats, per_table_stats = self._load_data_phase(
                        ctx=ctx,
                        benchmark_config=benchmark_config,
                        benchmark_instance=benchmark,
                        data_dir=data_dir,
                        options=options,
                    )
                    load_time = time.time() - load_start
                    logger.info(f"Data loading completed in {load_time:.2f}s")

                    # Add table stats to builder
                    for table_name, stats in per_table_stats.items():
                        builder.add_table_stats(
                            table_name,
                            stats.rows,
                            load_time_ms=stats.load_time_ms,
                            status=stats.status,
                            error_message=stats.error_message,
                        )
                    builder.set_loading_time(load_time * 1000)
                    builder.set_phase_status("data_loading", "COMPLETED", load_time * 1000)

                except DataLoadingError as load_err:
                    load_time = time.time() - load_start

                    # Add failed table stats to builder
                    for table_name, stats in load_err.per_table_stats.items():
                        builder.add_table_stats(
                            table_name,
                            stats.rows,
                            load_time_ms=stats.load_time_ms,
                            status=stats.status,
                            error_message=stats.error_message,
                        )
                    builder.set_loading_time(load_time * 1000)
                    builder.set_validation_status(
                        "FAILED",
                        {"error": str(load_err), "phase": "data_loading"},
                    )
                    builder.set_phase_status("data_loading", "FAILED")
                    builder.mark_completed()
                    return builder.build()

            # Execute phase
            if phases.execute:
                query_results = self._execute_queries_phase(
                    ctx=ctx,
                    benchmark_config=benchmark_config,
                    benchmark_instance=benchmark,
                    monitor=monitor,
                    run_options=options,
                )

                # Add query results to builder
                for qr in query_results:
                    builder.add_query_result(normalize_query_result(qr))
                builder.set_phase_status("power_test", "COMPLETED")

            builder.mark_completed()
            return builder.build()

        except Exception as e:
            logger.error(f"DataFrame benchmark failed: {e}")
            builder.set_validation_status(
                "FAILED",
                {"error": str(e), "error_type": type(e).__name__},
            )
            builder.add_execution_metadata("error", str(e))
            builder.mark_completed()
            return builder.build()

    def _extract_benchmark_config(self, benchmark: Any, run_config: dict[str, Any]) -> BenchmarkConfig:
        """Extract BenchmarkConfig from benchmark instance or run_config."""
        # Check if benchmark has config attribute
        if hasattr(benchmark, "config") and isinstance(benchmark.config, BenchmarkConfig):
            return benchmark.config

        # Build from benchmark attributes and run_config
        name = getattr(benchmark, "_name", getattr(benchmark, "name", type(benchmark).__name__.lower()))
        display_name = getattr(benchmark, "display_name", name.upper())
        scale_factor = run_config.get("scale_factor", getattr(benchmark, "scale_factor", 1.0))
        queries = run_config.get("query_subset", run_config.get("queries"))

        return BenchmarkConfig(
            name=name,
            display_name=display_name,
            scale_factor=scale_factor,
            queries=queries,
        )

    def _load_data_phase(
        self,
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
        platform_name = self.platform_name.lower()

        # Create data loader
        loader = DataFrameDataLoader(
            platform=platform_name,
            cache_dir=options.cache_dir,
            prefer_parquet=options.prefer_parquet,
            force_regenerate=options.force_regenerate,
        )

        def _normalize_table_paths(tables: dict[str, Any]) -> dict[str, Path | list[Path]]:
            normalized: dict[str, Path | list[Path]] = {}
            for name, path in tables.items():
                if isinstance(path, list):
                    normalized[name] = [Path(p) if not isinstance(p, Path) else p for p in path]
                elif isinstance(path, Path):
                    normalized[name] = path
                else:
                    normalized[name] = Path(path)
            return normalized

        def _generate_data() -> dict[str, Path | list[Path]]:
            logger.info("Generating benchmark data...")
            benchmark_instance.generate_data()  # type: ignore[union-attr]
            if hasattr(benchmark_instance, "tables") and benchmark_instance.tables:
                return _normalize_table_paths(benchmark_instance.tables)
            return {}

        def _has_list_paths(paths: dict[str, Path | list[Path]]) -> bool:
            return any(isinstance(value, list) for value in paths.values())

        def _benchmark_tables_has_list() -> bool:
            if not benchmark_instance or not hasattr(benchmark_instance, "tables") or not benchmark_instance.tables:
                return False
            return any(isinstance(value, list) for value in benchmark_instance.tables.values())

        def _find_missing_paths(paths: dict[str, Path | list[Path]]) -> dict[str, list[Path]]:
            missing: dict[str, list[Path]] = {}
            for table_name, file_path in paths.items():
                file_list = file_path if isinstance(file_path, list) else [file_path]
                for entry in file_list:
                    entry_path = entry if isinstance(entry, Path) else Path(entry)
                    if not entry_path.exists():
                        missing.setdefault(table_name, []).append(entry_path)
            return missing

        # Get data paths - try multiple sources in order
        data_paths: dict[str, Path | list[Path]] = {}

        if options.force_regenerate and benchmark_instance and hasattr(benchmark_instance, "generate_data"):
            data_paths = _generate_data()

        if benchmark_instance and hasattr(benchmark_instance, "tables") and benchmark_instance.tables:
            # Use benchmark's pre-generated data
            # Note: tables can be dict[str, Path | list[Path]]
            data_paths = {}

            # Check for parquet directory if prefer_parquet is True
            parquet_dir = None
            if options.prefer_parquet:
                # Look for parquet files in the same directory as the TBL files
                first_path = next(iter(benchmark_instance.tables.values()))
                if isinstance(first_path, list):
                    first_path = first_path[0]
                first_path = Path(first_path)
                parent_dir = first_path.parent

                # Check for parquet subdirectory (common pattern: tpch_sf001_*.parquet/)
                for pq_candidate in parent_dir.glob("*.parquet"):
                    if pq_candidate.is_dir():
                        parquet_dir = pq_candidate
                        logger.info(f"Found parquet directory: {parquet_dir}")
                        break

            if parquet_dir and parquet_dir.is_dir():
                # Use parquet files for proper schema handling
                for pq_file in parquet_dir.glob("*.parquet"):
                    table_name = pq_file.stem
                    data_paths[table_name] = pq_file
                logger.info(f"Using parquet data: {len(data_paths)} tables from {parquet_dir}")
            else:
                # Fall back to benchmark's TBL files
                data_paths = _normalize_table_paths(benchmark_instance.tables)
                logger.info(f"Using benchmark-provided data: {len(data_paths)} tables")
        elif data_dir and data_dir.exists():
            # Use provided data directory
            data_paths = loader._discover_files(data_dir)
            logger.info(f"Discovered data in {data_dir}: {len(data_paths)} tables")
            if not data_paths and benchmark_instance and hasattr(benchmark_instance, "generate_data"):
                data_paths = _generate_data()
        elif benchmark_instance and hasattr(benchmark_instance, "generate_data"):
            # No data available - try to generate it
            try:
                data_paths = _generate_data()
                if data_paths:
                    logger.info(f"Generated data: {len(data_paths)} tables")
            except Exception as gen_err:
                logger.warning(f"Data generation failed: {gen_err}")

        if not data_paths:
            raise ValueError("No data source available. Either provide data_dir or generate data first.")

        if options.prefer_parquet and not _benchmark_tables_has_list() and benchmark_instance is not None:
            try:
                prepared_paths = loader.prepare_benchmark_data(
                    benchmark_instance,
                    scale_factor=getattr(benchmark_config, "scale_factor", 1.0),
                    data_dir=data_dir,
                )
                if prepared_paths:
                    data_paths = prepared_paths
            except Exception as prep_err:
                logger.warning(f"Data preparation failed: {prep_err}. Using source files.")

        missing_paths = _find_missing_paths(data_paths)
        if missing_paths and benchmark_instance and hasattr(benchmark_instance, "generate_data"):
            logger.info("Missing data files detected, regenerating benchmark data...")
            data_paths = _generate_data()
            if options.prefer_parquet and not _benchmark_tables_has_list() and benchmark_instance is not None:
                try:
                    prepared_paths = loader.prepare_benchmark_data(
                        benchmark_instance,
                        scale_factor=getattr(benchmark_config, "scale_factor", 1.0),
                        data_dir=data_dir,
                    )
                    if prepared_paths:
                        data_paths = prepared_paths
                except Exception as prep_err:
                    logger.warning(f"Data preparation failed: {prep_err}. Using source files.")
            missing_paths = _find_missing_paths(data_paths)

        if missing_paths:
            per_table_stats: dict[str, TableLoadingStats] = {}
            for table_name, paths in missing_paths.items():
                missing_list = ", ".join(str(p) for p in paths[:5])
                more = "..." if len(paths) > 5 else ""
                per_table_stats[table_name] = TableLoadingStats(
                    rows=0,
                    load_time_ms=0,
                    status="FAILED",
                    error_type="MissingDataError",
                    error_message=f"Missing data files: {missing_list}{more}",
                )
            missing_summary = "; ".join(f"{t}: {len(p)} file(s)" for t, p in missing_paths.items())
            raise DataLoadingError(
                "Data files not found. Missing: "
                f"{missing_summary}. Run with --phases generate,load or --force-regenerate to recreate data.",
                per_table_stats=per_table_stats,
            )

        # Get schema info for column names
        benchmark_id = normalize_benchmark_id(benchmark_config.name)
        column_names_map = get_tpch_column_names() if benchmark_id == "tpch" else {}

        # Load tables
        table_stats: dict[str, int] = {}
        per_table_stats: dict[str, TableLoadingStats] = {}

        for table_name, file_path in data_paths.items():
            load_start = time.time()
            try:
                column_names = column_names_map.get(table_name.lower())
                # Ensure file_path is a list
                file_paths = [file_path] if not isinstance(file_path, list) else file_path
                row_count = self.load_table(ctx, table_name.lower(), file_paths, column_names=column_names)
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

        failed_tables = [name for name, stats in per_table_stats.items() if stats.status == "FAILED"]
        if failed_tables:
            raise DataLoadingError(
                f"Failed to load {len(failed_tables)} table(s): {', '.join(failed_tables)}.",
                table_stats=table_stats,
                per_table_stats=per_table_stats,
            )

        return table_stats, per_table_stats

    def _execute_queries_phase(
        self,
        ctx: Any,
        benchmark_config: BenchmarkConfig,
        benchmark_instance: Any | None,
        monitor: Any | None,
        run_options: DataFrameRunOptions | None = None,
    ) -> list[dict[str, Any]]:
        """Execute DataFrame queries with warmup and measurement iterations.

        Follows TPC power test methodology:
        1. Warmup iterations (default 1): Prime caches, not included in results
        2. Measurement iterations (default 3): Timed runs, results aggregated
        """
        # Get warmup and iteration counts from config options
        config_options = getattr(benchmark_config, "options", {}) or {}
        warmup_iterations = int(
            config_options.get("power_warmup_iterations", GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS)
            or GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS
        )
        measurement_iterations = int(
            config_options.get("power_iterations", GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS)
            or GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS
        )

        # Get query subset filter if specified
        query_subset = getattr(benchmark_config, "queries", None)
        query_filter: set[str] | None = None
        if query_subset:
            # Normalize query IDs - handle both "1" and "Q1" formats
            normalized = set()
            for q in query_subset:
                q_str = str(q).strip().upper()
                # Add both forms for matching: "Q1" and "1"
                if q_str.startswith("Q"):
                    normalized.add(q_str)  # Q1
                    normalized.add(q_str[1:])  # 1
                else:
                    normalized.add(q_str)  # 1
                    normalized.add(f"Q{q_str}")  # Q1
            query_filter = normalized

        # Get initial query set to determine total count
        initial_queries = self._get_queries_for_benchmark(benchmark_config, benchmark_instance, stream_id=0)
        if query_filter:
            initial_queries = [q for q in initial_queries if q.query_id.upper() in query_filter]

        if not initial_queries:
            logger.warning("No queries found for execution")
            return []

        total_queries = len(initial_queries)
        logger.info(f"Executing {total_queries} queries")
        logger.info(f"Warmup iterations: {warmup_iterations}, Measurement iterations: {measurement_iterations}")

        query_results: list[dict[str, Any]] = []

        # Execute warmup iterations (emit results)
        if warmup_iterations > 0:
            console.print(f"\n[yellow]Running {warmup_iterations} warmup iteration(s)...[/yellow]")
            for warmup_iter in range(warmup_iterations):
                warmup_queries = self._get_queries_for_benchmark(benchmark_config, benchmark_instance, stream_id=0)
                if query_filter:
                    warmup_queries = [q for q in warmup_queries if q.query_id.upper() in query_filter]

                console.print(f"[dim]Warmup {warmup_iter + 1}/{warmup_iterations} (stream 0)[/dim]")
                for query in warmup_queries:
                    try:
                        result = self.execute_query(ctx, query)
                        result = dict(result)
                    except Exception as e:
                        logger.warning(f"Warmup query {query.query_id} failed: {e}")
                        result = {
                            "query_id": query.query_id,
                            "status": "FAILED",
                            "error": str(e),
                            "execution_time_seconds": 0.0,
                        }
                    result["iteration"] = 0
                    result["stream_id"] = 0
                    result["run_type"] = "warmup"
                    query_results.append(result)

        # Execute measurement iterations
        console.print(
            f"\n[cyan]Running {measurement_iterations} measurement iteration(s) with {total_queries} queries each...[/cyan]"
        )

        for iteration in range(measurement_iterations):
            measurement_stream_id = warmup_iterations + iteration
            measurement_queries = self._get_queries_for_benchmark(
                benchmark_config, benchmark_instance, stream_id=measurement_stream_id
            )
            if query_filter:
                measurement_queries = [q for q in measurement_queries if q.query_id.upper() in query_filter]

            console.print(
                f"\n[green]Iteration {iteration + 1}/{measurement_iterations} (stream {measurement_stream_id})[/green]"
            )

            iteration_results: list[dict[str, Any]] = []
            for i, query in enumerate(measurement_queries, 1):
                if run_options and (run_options.verbose or run_options.very_verbose):
                    console.print(f"[blue]Executing query {i}/{total_queries}: {query.query_id}[/blue]")

                def execute_single_query(q=query):
                    try:
                        result = self.execute_query(ctx, q)
                        result = dict(result)
                    except Exception as e:
                        logger.error(f"Query {q.query_id} failed: {e}")
                        result = {
                            "query_id": q.query_id,
                            "status": "FAILED",
                            "error": str(e),
                            "execution_time_seconds": 0.0,
                        }
                    result["iteration"] = iteration + 1
                    result["stream_id"] = measurement_stream_id
                    result["run_type"] = "measurement"
                    query_results.append(result)
                    iteration_results.append(result)

                if monitor:
                    with monitor.time_operation(f"query_{query.query_id}_iter{iteration + 1}"):
                        execute_single_query()
                else:
                    execute_single_query()

            self._print_iteration_summary(
                iteration + 1,
                measurement_iterations,
                iteration_results,
                benchmark_config,
            )

        return query_results

    def _print_iteration_summary(
        self,
        iteration: int,
        total_iterations: int,
        iteration_results: list[dict[str, Any]],
        benchmark_config: BenchmarkConfig,
    ) -> None:
        """Print summary for a completed measurement iteration."""
        from benchbox.core.results.metrics import TPCMetricsCalculator

        total_queries = len(iteration_results)
        successful = [r for r in iteration_results if str(r.get("status", "")).upper() == "SUCCESS"]
        successful_count = len(successful)

        exec_times: list[float] = []
        for result in successful:
            exec_time = result.get("execution_time_seconds")
            if exec_time is None:
                exec_time = result.get("execution_time", 0.0)
            if exec_time:
                exec_times.append(float(exec_time))

        total_time = sum(exec_times)
        scale_factor = getattr(benchmark_config, "scale_factor", 1.0)
        benchmark_id = normalize_benchmark_id(benchmark_config.name)

        if benchmark_id == "tpch":
            display_name = "TPC-H"
        elif benchmark_id == "tpcds":
            display_name = "TPC-DS"
        else:
            display_name = str(benchmark_config.name).upper()

        power_at_size = None
        if successful_count == total_queries and exec_times:
            power_at_size = TPCMetricsCalculator.calculate_power_at_size(exec_times, scale_factor)

        test_name = f"{display_name} Power Test"
        console.print(f"\n[dim]Iteration {iteration}/{total_iterations} summary:[/dim]")
        if successful_count == total_queries:
            if power_at_size and power_at_size > 0:
                console.print(f"[green]{test_name} completed: Power@Size = {power_at_size:.2f}[/green]")
            else:
                console.print(f"[green]{test_name} completed[/green]")
            console.print(f"  Queries executed: {total_queries}, Successful: {successful_count}")
            success_rate = successful_count / max(total_queries, 1)
            if display_name == "TPC-H":
                console.print(f"  Success rate: {success_rate:.1%} (TPC-H requires >=95%)")
            else:
                console.print(f"  Success rate: {success_rate:.1%}")
            console.print(f"  Total execution time: {total_time:.2f}s")
        else:
            console.print(f"[yellow]{test_name} completed with failures[/yellow]")
            console.print(
                f"  Queries: {total_queries}, Successful: {successful_count}, "
                f"Failed: {total_queries - successful_count}"
            )

    def _get_queries_for_benchmark(
        self,
        benchmark_config: BenchmarkConfig,
        benchmark_instance: Any | None,
        stream_id: int | None = None,
    ) -> list[DataFrameQuery]:
        """Get DataFrame queries for a benchmark in proper execution order."""
        benchmark_id = normalize_benchmark_id(benchmark_config.name)

        if stream_id is None:
            stream_id = getattr(benchmark_config, "stream_id", 0)

        # Check registry for pre-defined queries
        if benchmark_id == "tpch":
            from benchbox.core.tpch.dataframe_queries import TPCH_DATAFRAME_QUERIES
            from benchbox.core.tpch.streams import TPCHStreams

            query_permutation = TPCHStreams.PERMUTATION_MATRIX[stream_id % len(TPCHStreams.PERMUTATION_MATRIX)]

            queries = []
            for query_num in query_permutation:
                query_id = f"Q{query_num}"
                query = TPCH_DATAFRAME_QUERIES.get(query_id)
                if query:
                    queries.append(query)
                else:
                    logger.warning(f"Query {query_id} not found in TPC-H DataFrame registry")
            return queries

        elif benchmark_id == "tpcds":
            from benchbox.core.tpcds.dataframe_queries import TPCDS_DATAFRAME_QUERIES
            from benchbox.core.tpcds.streams import PermutationMode, TPCDSPermutationGenerator

            available_query_ids = [int(qid[1:]) for qid in TPCDS_DATAFRAME_QUERIES.get_query_ids()]
            generator = TPCDSPermutationGenerator(seed=42 + stream_id)
            query_permutation = generator.generate_permutation(available_query_ids, PermutationMode.TPCDS_STANDARD)

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
