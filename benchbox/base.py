"""Base class for all benchmarks.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import logging
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from benchbox.core.connection import DatabaseConnection
from benchbox.utils.cloud_storage import create_path_handler
from benchbox.utils.scale_factor import format_scale_factor
from benchbox.utils.verbosity import VerbosityMixin, compute_verbosity

if TYPE_CHECKING:
    import sqlglot

    from benchbox.core.results.models import BenchmarkResults
else:
    try:
        import sqlglot
    except ImportError:
        sqlglot = None  # type: ignore


class BaseBenchmark(VerbosityMixin, ABC):
    """Base class for all benchmarks.

    All benchmarks inherit from this class.
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize a benchmark.

        Args:
            scale_factor: Scale factor (1.0 = standard size)
            output_dir: Data output directory
            **kwargs: Additional options
        """
        if scale_factor <= 0:
            raise ValueError("Scale factor must be positive")

        # Validate that scale factors >= 1 are whole integers
        if scale_factor >= 1 and scale_factor != int(scale_factor):
            raise ValueError(
                f"Scale factors >= 1 must be whole integers. Got: {scale_factor}. "
                f"Use values like 1, 2, 10, etc. for large scale factors. "
                f"Use values like 0.1, 0.01, 0.001, etc. for small scale factors."
            )

        self.scale_factor = scale_factor

        if output_dir is None:
            # Use CLI-compatible default path: benchmark_runs/datagen/{benchmark}_{sf}
            benchmark_name = self._get_benchmark_name().lower()
            sf_str = format_scale_factor(scale_factor)
            self.output_dir = Path.cwd() / "benchmark_runs" / "datagen" / f"{benchmark_name}_{sf_str}"
        else:
            # Support both local and cloud storage paths
            self.output_dir = create_path_handler(output_dir)

        # Verbosity and quiet handling (normalize bool/int)
        verbose_value = kwargs.pop("verbose", 0)
        quiet_value = kwargs.pop("quiet", False)
        verbosity_settings = compute_verbosity(verbose_value, quiet_value)
        self.apply_verbosity(verbosity_settings)

        # Logger for core benchmarks
        self.logger = logging.getLogger(f"benchbox.core.{self._get_benchmark_name()}")

        # Store remaining kwargs as attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _validate_scale_factor_type(self, scale_factor: float) -> None:
        """Validate scale factor is a number (int or float).

        Args:
            scale_factor: Scale factor to validate

        Raises:
            TypeError: If scale_factor is not a number
        """
        if not isinstance(scale_factor, (int, float)):
            raise TypeError(f"scale_factor must be a number, got {type(scale_factor).__name__}")

    def _initialize_benchmark_implementation(
        self,
        implementation_class,
        scale_factor: float,
        output_dir: Optional[Union[str, Path]],
        **kwargs,
    ):
        """Common initialization pattern for benchmark implementations.

        Args:
            implementation_class: The benchmark implementation class to instantiate
            scale_factor: Scale factor for the benchmark
            output_dir: Directory to output generated data files
            **kwargs: Additional implementation-specific options
        """
        # Extract verbose and force_regenerate from kwargs to avoid passing them twice
        verbose = kwargs.pop("verbose", False)
        force_regenerate = kwargs.pop("force_regenerate", False)

        self._impl = implementation_class(
            scale_factor=scale_factor,
            output_dir=output_dir,
            verbose=verbose,
            force_regenerate=force_regenerate,
            **kwargs,
        )

    def _get_benchmark_name(self) -> str:
        """Derive benchmark name from class name.

        Maps class names to benchmark names:
        - TPCH -> tpch
        - TPCDS -> tpcds
        - ClickBench -> clickbench
        - AMPLab -> amplab
        - H2ODB -> h2odb
        - JoinOrder -> joinorder
        - TPCHavoc -> tpchavoc
        - NYCTaxi -> nyctaxi
        - etc.

        Returns:
            Lowercase benchmark name suitable for directory paths
        """
        class_name = self.__class__.__name__

        # Handle special cases first
        special_mappings = {
            "ClickBench": "clickbench",
            "AMPLab": "amplab",
            "AMPLabBenchmark": "amplab",
            "H2ODB": "h2odb",
            "H2OBenchmark": "h2odb",
            "JoinOrder": "joinorder",
            "JoinOrderBenchmark": "joinorder",
            "TPCHavoc": "tpchavoc",
            "TPCHBenchmark": "tpch",
            "TPCDSBenchmark": "tpcds",
            "TPCDIBenchmark": "tpcdi",
            "SSBBenchmark": "ssb",
            "PrimitivesBenchmark": "primitives",
            "MergeBenchmark": "merge",
            "CoffeeShopBenchmark": "coffeeshop",
            "NYCTaxi": "nyctaxi",
            "NYCTaxiBenchmark": "nyctaxi",
            "TSBSDevOps": "tsbs_devops",
            "TSBSDevOpsBenchmark": "tsbs_devops",
        }

        if class_name in special_mappings:
            return special_mappings[class_name]

        # For standard cases, just lowercase
        # TPCH -> tpch, TPCDS -> tpcds, etc.
        return class_name.lower()

    def get_data_source_benchmark(self) -> Optional[str]:
        """Return the canonical source benchmark when data is shared.

        Benchmarks that reuse data generated by another benchmark (for example,
        ``Primitives`` reusing ``TPC-H`` datasets) should override this method and
        return the lower-case identifier of the source benchmark. Benchmarks that
        produce their own data should return ``None`` (default).
        """

        return None

    @abstractmethod
    def generate_data(self) -> list[Union[str, Path]]:
        """Generate benchmark data.

        Returns:
            List of data file paths
        """

    @abstractmethod
    def get_queries(self) -> dict[str, str]:
        """Get all benchmark queries.

        Returns:
            Dictionary mapping query IDs to query strings
        """

    @abstractmethod
    def get_query(self, query_id: Union[int, str], *, params: Optional[dict[str, Any]] = None) -> str:
        """Get a benchmark query.

        Args:
            query_id: Query ID
            params: Optional parameters

        Returns:
            Query string with parameters resolved

        Raises:
            ValueError: If query_id is invalid
        """

    def _load_data(self, connection: DatabaseConnection) -> None:
        """Load benchmark data into database.

        Each benchmark implements this method for its data loading.

        Args:
            connection: Database connection

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        # Default implementation - benchmarks should override
        # Non-abstract for backward compatibility
        # Raises NotImplementedError if not overridden
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _load_data() method to support database execution functionality"
        )

    def setup_database(self, connection: DatabaseConnection) -> None:
        """Set up database with schema and data.

        Creates necessary database schema and loads
        benchmark data into the database.

        Args:
            connection: Database connection to set up

        Raises:
            ValueError: If data generation fails
            Exception: If database setup fails
        """
        logger = logging.getLogger(__name__)

        try:
            logger.info("Setting up database schema and loading data...")
            start_time = time.time()

            # Generate data if not already generated
            if not hasattr(self, "_data_generated") or not self._data_generated:
                logger.info("Generating benchmark data...")
                self.generate_data()
                self._data_generated = True

            # Load data into database
            self._load_data(connection)

            setup_time = time.time() - start_time
            logger.info(f"Database setup completed in {setup_time:.2f} seconds")

        except Exception as e:
            logger.error(f"Database setup failed: {str(e)}")
            raise

    def run_query(
        self,
        query_id: Union[int, str],
        connection: DatabaseConnection,
        params: Optional[dict[str, Any]] = None,
        fetch_results: bool = False,
    ) -> dict[str, Any]:
        """Execute single query and return timing and results.

        Args:
            query_id: ID of the query to execute
            connection: Database connection to execute query on
            params: Optional parameters for query customization
            fetch_results: Whether to fetch and return query results

        Returns:
            Dictionary containing:
                - query_id: Executed query ID
                - execution_time: Time taken to execute query in seconds
                - query_text: Executed query text
                - results: Query results if fetch_results=True, otherwise None
                - row_count: Number of rows returned (if results fetched)

        Raises:
            ValueError: If query_id is invalid
            Exception: If query execution fails
        """
        logger = logging.getLogger(__name__)

        try:
            # Get the query text
            query_text = self.get_query(query_id, params=params)

            logger.debug(f"Executing query {query_id}")
            start_time = time.time()

            # Execute the query
            cursor = connection.execute(query_text)

            # Fetch results if requested
            results = None
            row_count = 0
            if fetch_results:
                results = connection.fetchall(cursor)
                row_count = len(results) if results else 0

            execution_time = time.time() - start_time

            logger.info(f"Query {query_id} completed in {execution_time:.3f} seconds")
            if fetch_results:
                logger.debug(f"Query {query_id} returned {row_count} rows")

            return {
                "query_id": query_id,
                "execution_time": execution_time,
                "query_text": query_text,
                "results": results,
                "row_count": row_count,
            }

        except Exception as e:
            logger.error(f"Query {query_id} execution failed: {str(e)}")
            raise

    def run_benchmark(
        self,
        connection: DatabaseConnection,
        query_ids: Optional[list[Union[int, str]]] = None,
        fetch_results: bool = False,
        setup_database: bool = True,
    ) -> dict[str, Any]:
        """Run the complete benchmark suite.

        Args:
            connection: Database connection to execute queries on
            query_ids: Optional list of specific query IDs to run (defaults to all)
            fetch_results: Whether to fetch and return query results
            setup_database: Whether to set up the database first

        Returns:
            Dictionary containing:
                - benchmark_name: Name of the benchmark
                - total_execution_time: Total time for all queries
                - total_queries: Number of queries executed
                - successful_queries: Number of queries that succeeded
                - failed_queries: Number of queries that failed
                - query_results: List of individual query results
                - setup_time: Time taken for database setup (if performed)

        Raises:
            Exception: If benchmark execution fails
        """
        logger = logging.getLogger(__name__)

        benchmark_start_time = time.time()
        setup_time = 0.0

        try:
            # Set up database if requested
            if setup_database:
                logger.info("Setting up database for benchmark...")
                setup_start_time = time.time()
                self.setup_database(connection)
                setup_time = time.time() - setup_start_time

            # Determine which queries to run
            if query_ids is None:
                all_queries = self.get_queries()
                query_ids = list(all_queries.keys())

            logger.info(f"Running benchmark with {len(query_ids)} queries...")

            # Execute all queries
            query_results = []
            successful_queries = 0
            failed_queries = 0

            for query_id in query_ids:
                try:
                    result = self.run_query(query_id, connection, fetch_results=fetch_results)
                    query_results.append(result)
                    successful_queries += 1

                except Exception as e:
                    logger.error(f"Query {query_id} failed: {str(e)}")
                    failed_queries += 1
                    # Include failed query in results
                    query_results.append(
                        {
                            "query_id": query_id,
                            "execution_time": 0.0,
                            "query_text": None,
                            "results": None,
                            "row_count": 0,
                            "error": str(e),
                        }
                    )

            total_execution_time = time.time() - benchmark_start_time - setup_time

            # Calculate summary statistics
            query_times = [r["execution_time"] for r in query_results if "error" not in r]

            benchmark_result = {
                "benchmark_name": self.__class__.__name__,
                "total_execution_time": total_execution_time,
                "total_queries": len(query_ids),
                "successful_queries": successful_queries,
                "failed_queries": failed_queries,
                "query_results": query_results,
                "setup_time": setup_time,
                "average_query_time": sum(query_times) / len(query_times) if query_times else 0.0,
                "min_query_time": min(query_times) if query_times else 0.0,
                "max_query_time": max(query_times) if query_times else 0.0,
            }

            logger.info(f"Benchmark completed: {successful_queries}/{len(query_ids)} queries successful")
            logger.info(f"Total execution time: {total_execution_time:.2f} seconds")

            return benchmark_result

        except Exception as e:
            logger.error(f"Benchmark execution failed: {str(e)}")
            raise

    def run_with_platform(self, platform_adapter, **run_config):
        """Run complete benchmark using platform-specific optimizations.

        This method provides a unified interface for running benchmarks
        using database platform adapters that handle connection management,
        data loading optimizations, and query execution.

        This is the standard method that all benchmarks should support for
        integration with the CLI and other orchestration tools.

        Args:
            platform_adapter: Platform adapter instance (e.g., DuckDBAdapter)
            **run_config: Configuration options:
                - categories: List of query categories to run (if benchmark supports)
                - query_subset: List of specific query IDs to run
                - connection: Connection configuration
                - benchmark_type: Type hint for optimizations ('olap', 'oltp', etc.)

        Returns:
            BenchmarkResults object with execution results

        Example:
            from benchbox.platforms import DuckDBAdapter

            benchmark = SomeBenchmark(scale_factor=0.1)
            adapter = DuckDBAdapter()
            results = benchmark.run_with_platform(adapter)
        """
        # Set default benchmark type based on benchmark characteristics
        default_benchmark_type = self._get_default_benchmark_type()
        run_config.setdefault("benchmark_type", default_benchmark_type)

        # Execute using the platform adapter
        return platform_adapter.run_benchmark(self, **run_config)

    def _get_default_benchmark_type(self) -> str:
        """Get the default benchmark type for platform optimizations.

        Subclasses can override this to specify their workload characteristics.

        Returns:
            Default benchmark type ('olap', 'oltp', 'mixed', 'analytics')
        """
        # Most benchmarks in BenchBox are OLAP-focused
        return "olap"

    def format_results(self, benchmark_result: dict[str, Any]) -> str:
        """Format benchmark results for display.

        Args:
            benchmark_result: Result dictionary from run_benchmark()

        Returns:
            Formatted string representation of the results
        """
        lines = []
        lines.append(f"Benchmark: {benchmark_result['benchmark_name']}")
        lines.append("=" * 50)

        lines.append(f"Total Queries: {benchmark_result['total_queries']}")
        lines.append(f"Successful: {benchmark_result['successful_queries']}")
        lines.append(f"Failed: {benchmark_result['failed_queries']}")

        if benchmark_result["setup_time"] > 0:
            lines.append(f"Setup Time: {benchmark_result['setup_time']:.2f}s")

        lines.append(f"Total Execution Time: {benchmark_result['total_execution_time']:.2f}s")
        lines.append(f"Average Query Time: {benchmark_result['average_query_time']:.3f}s")
        lines.append(f"Min Query Time: {benchmark_result['min_query_time']:.3f}s")
        lines.append(f"Max Query Time: {benchmark_result['max_query_time']:.3f}s")

        lines.append("\nQuery Details:")
        lines.append("-" * 30)

        for result in benchmark_result["query_results"]:
            query_id = result["query_id"]
            if "error" in result:
                lines.append(f"Query {query_id}: FAILED - {result['error']}")
            else:
                exec_time = result["execution_time"]
                row_count = result["row_count"]
                lines.append(f"Query {query_id}: {exec_time:.3f}s ({row_count} rows)")

        return "\n".join(lines)

    def _format_time(self, seconds: float) -> str:
        """Format execution time for display.

        Args:
            seconds: Time in seconds

        Returns:
            Formatted time string
        """
        if seconds < 1:
            return f"{seconds * 1000:.1f}ms"
        elif seconds < 60:
            return f"{seconds:.2f}s"
        else:
            minutes = int(seconds // 60)
            remaining_seconds = seconds % 60
            return f"{minutes}m {remaining_seconds:.1f}s"

    def translate_query(self, query_id: Union[int, str], dialect: str) -> str:
        """Translate a query to a specific SQL dialect.

        Args:
            query_id: The ID of the query to translate
            dialect: The target SQL dialect

        Returns:
            The translated query string

        Raises:
            ValueError: If the query_id is invalid
            ImportError: If sqlglot is not installed
            ValueError: If the dialect is not supported
        """
        if sqlglot is None:
            raise ImportError("sqlglot is required for query translation. Install it with `pip install sqlglot`.")

        from benchbox.utils.dialect_utils import normalize_dialect_for_sqlglot

        query = self.get_query(query_id)

        # Normalize dialect for SQLGlot compatibility
        normalized_dialect = normalize_dialect_for_sqlglot(dialect)

        # Apply translation for specific SQL syntax
        try:
            # Use identify=True to quote identifiers and prevent reserved keyword conflicts
            translated = sqlglot.transpile(  # type: ignore[attr-defined]
                query, read="postgres", write=normalized_dialect, identify=True
            )[0]
            return translated
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Error translating to dialect '{dialect}': {e}")

    @property
    def benchmark_name(self) -> str:
        """Get the human-readable benchmark name."""
        # Try to get from implementation first, then fallback to class name
        if hasattr(self, "_impl"):
            if hasattr(self._impl, "_name"):
                return self._impl._name
            elif hasattr(self._impl, "benchmark_name"):
                return self._impl.benchmark_name
        # For classes without _impl (like core implementation classes)
        return getattr(self, "_name", type(self).__name__)

    def create_enhanced_benchmark_result(
        self,
        platform: str,
        query_results: list[dict[str, Any]],
        execution_metadata: Optional[dict[str, Any]] = None,
        phases: Optional[dict[str, dict[str, Any]]] = None,
        resource_utilization: Optional[dict[str, Any]] = None,
        performance_characteristics: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> "BenchmarkResults":
        """Create a BenchmarkResults object with standardized fields.

        This centralizes the logic for creating benchmark results that was previously
        duplicated across platform adapters and CLI orchestrator.

        Args:
            platform: Platform name (e.g., "DuckDB", "ClickHouse")
            query_results: List of query execution results
            execution_metadata: Optional execution metadata
            phases: Optional phase tracking information
            resource_utilization: Optional resource usage metrics
            performance_characteristics: Optional performance analysis
            **kwargs: Additional fields to override defaults

        Returns:
            Fully configured BenchmarkResults object
        """
        # Delegate to implementation's create method if available, otherwise use our own
        if hasattr(self, "_impl") and hasattr(self._impl, "create_enhanced_benchmark_result"):
            return self._impl.create_enhanced_benchmark_result(
                platform=platform,
                query_results=query_results,
                execution_metadata=execution_metadata,
                phases=phases,
                resource_utilization=resource_utilization,
                performance_characteristics=performance_characteristics,
                **kwargs,
            )

        # Fallback implementation for wrapper class
        import time
        import uuid
        from datetime import datetime

        # Calculate basic metrics from query results
        total_queries = len(query_results)
        successful_queries = len([r for r in query_results if r.get("status") == "SUCCESS"])
        failed_queries = total_queries - successful_queries

        # Calculate timing metrics
        successful_results = [r for r in query_results if r.get("status") == "SUCCESS"]
        total_execution_time = sum(r.get("execution_time", 0.0) for r in successful_results)
        average_query_time = total_execution_time / max(successful_queries, 1)

        # Generate standard identifiers
        execution_id = kwargs.get(
            "execution_id",
            f"{self.benchmark_name.lower().replace(' ', '_')}_{int(time.time())}",
        )
        timestamp = kwargs.get("timestamp", datetime.now())

        # Import required classes for creating proper results (from core)
        from benchbox.core.results.models import (
            BenchmarkResults,
            DataGenerationPhase,
            DataLoadingPhase,
            ExecutionPhases,
            QueryDefinition,
            SchemaCreationPhase,
            SetupPhase,
            ValidationPhase,
        )

        # Create query definitions from query results
        query_definitions = {}
        if hasattr(self, "get_queries"):
            queries = self.get_queries()
            for query_id, sql in queries.items():
                query_definitions.setdefault("stream_1", {})[query_id] = QueryDefinition(sql=sql, parameters=None)

        # Create basic execution phases with proper structure and timing
        data_gen_time = kwargs.get("data_generation_time", 0.0)
        schema_time = kwargs.get("schema_creation_time", 0.0)
        loading_time = kwargs.get("data_loading_time", 0.0)
        validation_time = kwargs.get("validation_time", 0.0)

        setup_phase = SetupPhase(
            data_generation=DataGenerationPhase(
                duration_ms=int(data_gen_time * 1000),
                status="SUCCESS",
                tables_generated=kwargs.get("tables_generated", 0),
                total_rows_generated=kwargs.get("total_rows_generated", 0),
                total_data_size_bytes=kwargs.get("total_data_size_bytes", 0),
                per_table_stats={},
            ),
            schema_creation=SchemaCreationPhase(
                duration_ms=int(schema_time * 1000),
                status="SUCCESS",
                tables_created=kwargs.get("tables_created", 0),
                constraints_applied=kwargs.get("constraints_applied", 0),
                indexes_created=kwargs.get("indexes_created", 0),
                per_table_creation={},
            ),
            data_loading=DataLoadingPhase(
                duration_ms=int(loading_time * 1000),
                status="SUCCESS",
                total_rows_loaded=kwargs.get("total_rows_loaded", 0),
                tables_loaded=kwargs.get("tables_loaded", 0),
                per_table_stats={},
            ),
            validation=ValidationPhase(
                duration_ms=int(validation_time * 1000),
                row_count_validation=kwargs.get("row_count_validation", "SKIPPED"),
                schema_validation=kwargs.get("schema_validation", "SKIPPED"),
                data_integrity_checks=kwargs.get("data_integrity_checks", "SKIPPED"),
                validation_details=kwargs.get("validation_details", {}),
            ),
        )

        # Use provided execution phases if available, otherwise create basic setup phase
        provided_phases = phases or kwargs.get("execution_phases") or kwargs.get("phases")
        if provided_phases and hasattr(provided_phases, "power_test"):
            # Type checker doesn't understand the hasattr check narrows the type
            assert isinstance(provided_phases, ExecutionPhases)
            execution_phases = provided_phases
        else:
            execution_phases = ExecutionPhases(setup=setup_phase)

        # Create result with proper structured data
        result = BenchmarkResults(
            # Core benchmark info
            benchmark_name=self.benchmark_name,
            platform=platform,
            scale_factor=self.scale_factor,
            execution_id=execution_id,
            timestamp=timestamp,
            duration_seconds=kwargs.get("duration_seconds", total_execution_time),
            # Required structured data
            query_results=query_results,
            query_definitions=query_definitions,
            execution_phases=execution_phases,
            # Summary metrics (calculated from phases)
            total_queries=total_queries,
            successful_queries=successful_queries,
            failed_queries=failed_queries,
            total_execution_time=total_execution_time,
            average_query_time=average_query_time,
            # Setup metrics (from execution phases)
            data_loading_time=loading_time,
            schema_creation_time=schema_time,
            total_rows_loaded=kwargs.get("total_rows_loaded", 0),
            data_size_mb=kwargs.get("total_data_size_bytes", 0) / (1024 * 1024),
            table_statistics=kwargs.get("table_statistics", {}),
            # Optional fields with defaults
            test_execution_type=kwargs.get("test_execution_type", "standard"),
            validation_status=kwargs.get("validation_status", "UNKNOWN"),
            validation_details=kwargs.get("validation_details", {}),
            platform_info=kwargs.get("platform_info"),
            tunings_applied=kwargs.get("tunings_applied", {}),
            tuning_validation_status=kwargs.get("tuning_validation_status", "NOT_APPLIED"),
            tuning_metadata_saved=kwargs.get("tuning_metadata_saved", False),
            system_profile=kwargs.get("system_profile", {}),
            anonymous_machine_id=kwargs.get("anonymous_machine_id", str(uuid.uuid4())[:8]),
            execution_metadata=execution_metadata or {},
        )

        if performance_characteristics is not None:
            result.performance_characteristics = performance_characteristics
        else:
            result.performance_characteristics = {}

        snapshot_payload = kwargs.get("performance_snapshot")
        if snapshot_payload is not None:
            try:
                from benchbox.monitoring.performance import PerformanceSnapshot, attach_snapshot_to_result

                if isinstance(snapshot_payload, PerformanceSnapshot):
                    attach_snapshot_to_result(result, snapshot_payload)
                elif isinstance(snapshot_payload, dict):
                    result.performance_summary = dict(snapshot_payload)
                    if not result.performance_characteristics:
                        result.performance_characteristics = dict(snapshot_payload)
            except ImportError:
                if isinstance(snapshot_payload, dict):
                    result.performance_summary = dict(snapshot_payload)
                    if not result.performance_characteristics:
                        result.performance_characteristics = dict(snapshot_payload)
        elif performance_characteristics and not result.performance_summary:
            result.performance_summary = dict(performance_characteristics)

        return result
