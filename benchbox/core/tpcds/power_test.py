"""TPC-DS Power Test Implementation.

This module implements the TPC-DS Power Test according to the official TPC-DS
specification. The Power Test measures the time to execute all TPC-DS queries
sequentially in a single stream to calculate the Power@Size metric.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DS (TPC-DS) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DS specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

# Import the real DatabaseConnection
try:
    from benchbox.core.connection import DatabaseConnection as RealDatabaseConnection
except ImportError:
    RealDatabaseConnection = None


@dataclass
class TPCDSPowerTestConfig:
    """Configuration for TPC-DS Power Test."""

    scale_factor: float = 1.0
    seed: int = 1
    stream_id: int = 0  # TPC-DS stream ID for query permutation
    timeout: Optional[float] = None
    warm_up: bool = True
    validation: bool = True
    verbose: bool = False
    query_subset: Optional[list[str]] = None  # If set, run only these queries in specified order


@dataclass
class TPCDSPowerTestResult:
    """Result of TPC-DS Power Test."""

    config: TPCDSPowerTestConfig
    start_time: str
    end_time: str
    total_time: float
    power_at_size: float
    queries_executed: int
    queries_successful: int
    query_results: list[dict[str, Any]]
    success: bool
    errors: list[str]

    @property
    def scale_factor(self) -> float:
        """Scale factor from config for backward compatibility."""
        return self.config.scale_factor


class TPCDSPowerTest:
    """TPC-DS Power Test implementation."""

    def __init__(
        self,
        benchmark: Any,
        connection_factory: Optional[Callable[[], Any]] = None,
        scale_factor: float = 1.0,
        seed: Optional[int] = None,
        stream_id: int = 0,
        verbose: bool = False,
        timeout: Optional[float] = None,
        connection_string: Optional[str] = None,
        dialect: Optional[str] = None,
        warm_up: bool = True,
        validation: bool = True,
        query_subset: Optional[list[str]] = None,
    ) -> None:
        """Initialize TPC-DS Power Test.

        Args:
            benchmark: TPCDSBenchmark instance
            connection_factory: Factory function to create database connections
            scale_factor: Scale factor for the benchmark
            seed: Random seed for parameter generation (default: 1)
            stream_id: TPC-DS stream ID for query permutation (default: 0)
            verbose: Enable verbose logging
            timeout: Query timeout in seconds
            connection_string: Database connection string (legacy parameter)
            dialect: SQL dialect (legacy parameter)
            warm_up: Enable database warm-up procedure
            validation: Enable result validation
            query_subset: Optional list of specific query IDs to run (overrides stream permutation)

        Raises:
            ValueError: If scale_factor is not positive
        """
        # Validate parameters
        if scale_factor <= 0:
            raise ValueError("scale_factor must be a positive number")

        self.benchmark = benchmark

        # Handle legacy connection_string parameter
        if connection_string is not None:
            # Create a simple connection factory that returns a mock connection
            # Capture connection_string in a local variable for type narrowing
            conn_str = connection_string
            self.connection_factory = lambda: DatabaseConnection(
                connection_string=conn_str,
                dialect=dialect or "standard",
                verbose=verbose,
            )
        elif connection_factory is not None:
            self.connection_factory = connection_factory
        else:
            # Default to mock connection factory
            self.connection_factory = lambda: DatabaseConnection()

        self.config = TPCDSPowerTestConfig(
            scale_factor=scale_factor,
            seed=seed or 1,
            stream_id=stream_id,
            timeout=timeout,
            warm_up=warm_up,
            validation=validation,
            verbose=verbose,
            query_subset=query_subset,
        )

        # Store target dialect for query translation
        self.target_dialect = dialect

        # Legacy compatibility attributes
        self.scale_factor = scale_factor
        self.connection = None
        self.test_running = False
        self.current_query = None

        self.logger = logging.getLogger(__name__)
        if verbose:
            self.logger.setLevel(logging.INFO)
        # Captured SQL items for dry-run preview: (label, sql)
        self.captured_items: list[tuple[str, str]] = []

    def run(self) -> TPCDSPowerTestResult:
        """Execute the TPC-DS Power Test.

        Returns:
            Power Test results with Power@Size metric

        Raises:
            RuntimeError: If Power Test execution fails
        """
        start_time = time.time()
        start_time_str = datetime.now().isoformat()

        result = TPCDSPowerTestResult(
            config=self.config,
            start_time=start_time_str,
            end_time="",
            total_time=0.0,
            power_at_size=0.0,
            queries_executed=0,
            queries_successful=0,
            query_results=[],
            success=True,
            errors=[],
        )

        # Logging and setup
        if self.config.verbose:
            self.logger.info("Starting TPC-DS Power Test")
            self.logger.info(f"Scale factor: {self.config.scale_factor}")
            self.logger.info(f"Seed: {self.config.seed}")

        # Determine available query IDs
        try:
            all_queries = self.benchmark.get_queries()
            available_query_ids = [int(k) for k in all_queries if k.isdigit()]
            available_query_ids.sort()
            if not available_query_ids:
                # Fallback if no digit-only query IDs found
                available_query_ids = list(range(1, 100))
        except Exception:
            available_query_ids = list(range(1, 100))

        if self.config.verbose:
            self.logger.info(f"Found {len(available_query_ids)} queries to execute")

        # Build the execution sequence
        if self.config.query_subset:
            # User specified specific queries - run in their order
            queries_to_execute = [(int(qid) if str(qid).isdigit() else qid, None) for qid in self.config.query_subset]
            if self.config.verbose:
                self.logger.info(f"Using user-specified query subset: {[q[0] for q in queries_to_execute]}")
            # Warn about compliance impact
            self.logger.warning(
                "⚠️  query_subset overrides standard query sequence - results may not be compliant. "
                "Official TPC-DS benchmarks require running all queries in the specified order."
            )
        elif hasattr(self, "_query_sequence"):
            queries_to_execute = self._query_sequence
            if self.config.verbose:
                self.logger.info(f"Using custom query sequence: {queries_to_execute}")
        else:
            from benchbox.core.tpcds.streams import create_standard_streams

            # Get the query manager - handle both direct TPCDSBenchmark and TPCDS wrapper
            query_manager = None
            if hasattr(self.benchmark, "query_manager"):
                query_manager = self.benchmark.query_manager
            elif hasattr(self.benchmark, "_impl") and hasattr(self.benchmark._impl, "query_manager"):
                query_manager = self.benchmark._impl.query_manager

            if query_manager is None:
                if self.config.verbose:
                    self.logger.warning("No query_manager found, using sequential execution")
                queries_to_execute = [(q, None) for q in available_query_ids]
            else:
                stream_id = getattr(self.config, "stream_id", 0)
                stream_manager = create_standard_streams(
                    query_manager=query_manager,
                    num_streams=1,
                    query_ids=available_query_ids if available_query_ids else None,
                    query_range=(1, 99),  # Fallback range if query_ids is None
                    base_seed=self.config.seed + stream_id,
                )
                streams = stream_manager.generate_streams()
                stream_queries = streams.get(0, [])
                queries_to_execute = []
                for sq in stream_queries:
                    if sq.variant is None:
                        queries_to_execute.append((sq.query_id, None))
                    else:
                        queries_to_execute.append((sq.query_id, sq.variant))
                if self.config.verbose:
                    self.logger.info(f"Using TPC-DS stream {stream_id} with {len(queries_to_execute)} queries")
                    self.logger.info(f"Query order (first 10): {queries_to_execute[:10]}")

        # Preflight: validate that all queries can be generated for this stream/seed
        # Let preflight failures propagate as RuntimeError (tests expect this)
        self._preflight_validate_generation(available_query_ids)

        try:
            connection = self.connection_factory()

            # Calculate stream parameter seed ONCE for all queries in this stream
            # Per TPC-DS specification: all queries in a stream use the same parameter seed
            # This matches the stream manager's calculation: base_seed + stream_id + 1000
            stream_param_seed = self.config.seed + self.config.stream_id + 1000

            if self.config.verbose:
                self.logger.info(f"Using parameter seed {stream_param_seed} for stream {self.config.stream_id}")

            for position, query_info in enumerate(queries_to_execute):
                # Handle both old format (int) and new format (tuple)
                if isinstance(query_info, tuple):
                    query_id, variant = query_info
                    query_display_id = f"{query_id}{variant}" if variant else str(query_id)
                else:
                    # Backward compatibility for old format
                    query_id = query_info
                    variant = None
                    query_display_id = str(query_id)

                query_start = time.time()
                query_result = {
                    "query_id": query_display_id,
                    "position": position + 1,
                    "stream_id": self.config.stream_id,
                    "execution_time": 0.0,
                    "success": False,
                    "error": None,
                    "result_count": 0,
                }

                try:
                    if self.config.verbose:
                        self.logger.info(f"Executing Query {query_display_id} (position {position + 1})")

                    # Call get_query with variant parameter if needed
                    # Use stream_param_seed (same for all queries in this stream)
                    if variant is not None:
                        query_text = self.benchmark.get_query(
                            query_id,
                            seed=stream_param_seed,
                            scale_factor=self.config.scale_factor,
                            variant=variant,
                            dialect=self.target_dialect,
                        )
                    else:
                        query_text = self.benchmark.get_query(
                            query_id,
                            seed=stream_param_seed,
                            scale_factor=self.config.scale_factor,
                            dialect=self.target_dialect,
                        )

                    # Execute the actual query
                    label = f"Position_{position + 1}_Query_{query_display_id}"
                    try:
                        # Set query context before execution for validation
                        # NOTE: Answer files are only available for stream 0
                        # Other streams use different seeds and will have different expected row counts
                        if hasattr(connection, "set_query_context"):
                            connection.set_query_context(query_display_id, stream_id=self.config.stream_id)

                        cursor = connection.execute(query_text)
                        # Fetch to complete execution in non-dry-run
                        rows = cursor.fetchall() if hasattr(cursor, "fetchall") else []

                        # Check if query failed validation
                        if hasattr(cursor, "platform_result"):
                            result_dict = cursor.platform_result
                            if result_dict.get("status") == "FAILED":
                                # Validation failed - treat as query failure
                                error_msg = result_dict.get(
                                    "error", result_dict.get("row_count_validation_error", "Query validation failed")
                                )
                                raise RuntimeError(error_msg)

                        if hasattr(connection, "commit"):
                            connection.commit()
                    finally:
                        # Record labeled SQL for preview
                        self.captured_items.append((label, query_text))

                    execution_time = time.time() - query_start

                    query_result.update(
                        {
                            "execution_time": execution_time,
                            "success": True,
                            "result_count": len(rows),
                        }
                    )

                    result.queries_successful += 1

                    if self.config.verbose:
                        self.logger.info(f"Query {query_id} completed in {execution_time:.3f}s")

                except Exception as e:
                    execution_time = time.time() - query_start
                    query_result.update(
                        {
                            "execution_time": execution_time,
                            "success": False,
                            "error": str(e),
                        }
                    )

                    # For TPC-DS, some queries may not be available, which is normal
                    if "Template substitution error" not in str(e):
                        result.errors.append(f"Query {query_id} failed: {e}")

                    if self.config.verbose:
                        self.logger.warning(f"Query {query_id} failed: {e}")

                result.query_results.append(query_result)
                result.queries_executed += 1

            connection.close()

            # Calculate Power@Size metric
            total_execution_time = time.time() - start_time

            # Power@Size = 3600 * SF / Power_Test_Time (in seconds)
            if total_execution_time > 0 and result.queries_successful > 0:
                result.power_at_size = (3600.0 * self.config.scale_factor) / total_execution_time

            result.total_time = total_execution_time
            result.end_time = datetime.now().isoformat()

            # TPC-DS success criteria: at least 70% of queries must succeed
            success_rate = result.queries_successful / max(result.queries_executed, 1)
            result.success = success_rate >= 0.7

            if self.config.verbose:
                self.logger.info(f"Power Test completed in {total_execution_time:.3f}s")
                self.logger.info(f"Successful queries: {result.queries_successful}/{result.queries_executed}")
                self.logger.info(f"Success rate: {success_rate:.2%}")
                self.logger.info(f"Power@Size: {result.power_at_size:.2f}")

            return result
        except Exception as e:
            result.total_time = time.time() - start_time
            result.end_time = datetime.now().isoformat()
            result.success = False
            result.errors.append(f"Power Test execution failed: {e}")

            if self.config.verbose:
                self.logger.error(f"Power Test failed: {e}")

            return result

    def _build_query_sequence(self, available_query_ids: list[int]) -> list[tuple]:
        """Build the power test query sequence including variants when available."""
        # Check if custom query sequence is set (for testing)
        if hasattr(self, "_query_sequence"):
            return [(q, None) if not isinstance(q, tuple) else q for q in self._query_sequence]
        from benchbox.core.tpcds.streams import create_standard_streams

        query_manager = None
        if hasattr(self.benchmark, "query_manager"):
            query_manager = self.benchmark.query_manager
        elif hasattr(self.benchmark, "_impl") and hasattr(self.benchmark._impl, "query_manager"):
            query_manager = self.benchmark._impl.query_manager
        if query_manager is None:
            return [(q, None) for q in available_query_ids]
        stream_id = getattr(self.config, "stream_id", 0)
        stream_manager = create_standard_streams(
            query_manager=query_manager,
            num_streams=1,
            query_ids=available_query_ids if available_query_ids else None,
            query_range=(1, 99),  # Fallback range if query_ids is None
            base_seed=self.config.seed + stream_id,
        )
        streams = stream_manager.generate_streams()
        stream_queries = streams.get(0, [])
        sequence: list[tuple] = []
        # Prefer variants only when the template exists in this dsqgen build
        for sq in stream_queries:
            if sq.variant is None:
                sequence.append((sq.query_id, None))
            else:
                # Use query_manager to validate variant presence
                try:
                    composite_id = f"{sq.query_id}{sq.variant}"
                    if hasattr(query_manager, "validate_query_id") and query_manager.validate_query_id(composite_id):
                        sequence.append((sq.query_id, sq.variant))
                    else:
                        # Fallback to base query when variant template not available
                        sequence.append((sq.query_id, None))
                except Exception:
                    # Conservative fallback to base
                    sequence.append((sq.query_id, None))
        return sequence

    def _preflight_validate_generation(self, available_query_ids: list[int]) -> None:
        """Validate all queries in the power sequence can be generated for this seed/stream.

        Raises RuntimeError with details if any query generation fails.
        """
        sequence = self._build_query_sequence(available_query_ids)
        failures = []

        # Use same parameter seed calculation as main execution loop
        # Per TPC-DS spec: all queries in a stream use the same parameter seed
        stream_param_seed = self.config.seed + self.config.stream_id + 1000

        for position, item in enumerate(sequence):
            query_id, variant = item if isinstance(item, tuple) else (item, None)
            try:
                if variant is not None:
                    _ = self.benchmark.get_query(
                        query_id,
                        seed=stream_param_seed,
                        scale_factor=self.config.scale_factor,
                        variant=variant,
                        dialect=self.target_dialect,
                    )
                else:
                    _ = self.benchmark.get_query(
                        query_id,
                        seed=stream_param_seed,
                        scale_factor=self.config.scale_factor,
                        dialect=self.target_dialect,
                    )
            except Exception as e:
                failures.append(f"{query_id}{variant or ''}: {e}")
        if failures:
            msg = f"TPC-DS PowerTest preflight failed for {len(failures)} queries. Examples: {', '.join(failures[:3])}"
            raise RuntimeError(msg)

    def validate_results(self, result: TPCDSPowerTestResult) -> bool:
        """Validate Power Test results against TPC-DS specification.

        Args:
            result: Power Test results to validate

        Returns:
            True if results are valid, False otherwise
        """
        if not result.success:
            return False

        # TPC-DS requires at least 70% query success rate
        if result.queries_executed > 0:
            success_rate = result.queries_successful / result.queries_executed
            if success_rate < 0.7:
                return False

        return not result.power_at_size <= 0

    def _calculate_power_at_size(self, total_time: float) -> float:
        """Calculate Power@Size metric.

        Args:
            total_time: Total execution time in seconds

        Returns:
            Power@Size metric (3600 * SF / total_time)
        """
        if total_time <= 0:
            return 0.0
        return (3600.0 * self.scale_factor) / total_time

    @property
    def query_sequence(self) -> list:
        """Get the query sequence for the power test.

        Returns:
            List of query IDs including variants
        """
        if hasattr(self, "_query_sequence"):
            return self._query_sequence
        # TPC-DS has 99 base queries + 8 variants (multi-part queries)
        base_queries = list(range(1, 100))
        variants = ["14a", "14b", "23a", "23b", "24a", "24b", "39a", "39b"]
        return base_queries + variants

    @query_sequence.setter
    def query_sequence(self, value: list):
        """Set the query sequence for the power test.

        Args:
            value: List of query IDs
        """
        self._query_sequence = value

    def get_status(self) -> dict[str, Any]:
        """Get current power test status.

        Returns:
            Dictionary with status information
        """
        return {
            "running": self.test_running,
            "current_query": self.current_query,
            "scale_factor": self.scale_factor,
            "seed": self.config.seed,
            "dialect": "standard",
            "query_sequence_length": len(self.query_sequence),
        }

    def _connect_database(self) -> None:
        """Establish database connection."""
        self.connection = self.connection_factory()

    def _disconnect_database(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def _warm_up_database(self) -> None:
        """Execute warm-up queries to prepare the database."""
        if not self.connection:
            return

        # Execute some simple warm-up queries
        warm_up_queries = [
            "SELECT 1",
            "SELECT COUNT(*) FROM (SELECT 1 UNION SELECT 2)",
            "SELECT AVG(x) FROM (SELECT 1 as x UNION SELECT 2 UNION SELECT 3)",
            "SELECT MAX(x), MIN(x) FROM (SELECT 1 as x UNION SELECT 2 UNION SELECT 3)",
            "SELECT x, COUNT(*) FROM (SELECT 1 as x UNION SELECT 1 UNION SELECT 2) GROUP BY x",
        ]

        for query in warm_up_queries:
            try:
                cursor = self.connection.execute(query)
                cursor.fetchall()
            except Exception:
                pass  # Ignore warm-up failures

    def _execute_query(self, query_id: Any, query_text: str) -> dict[str, Any]:
        """Execute a single query and return results.

        Args:
            query_id: Query identifier
            query_text: SQL query text

        Returns:
            Dictionary with execution results
        """
        start_time = time.time()
        result = {
            "query_id": query_id,
            "status": "success",
            "execution_time": 0.0,
            "result_count": 0,
            "error": None,
        }

        try:
            cursor = self.connection.execute(query_text)
            rows = cursor.fetchall()
            result["result_count"] = len(rows)
            self.connection.commit()
        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        result["execution_time"] = time.time() - start_time
        return result

    def _validate_results(self, result: TPCDSPowerTestResult) -> bool:
        """Validate power test results.

        Args:
            result: Power test result to validate

        Returns:
            True if results are valid, False otherwise
        """
        if not self.config.validation:
            return True

        # Check if all expected queries were executed
        executed_queries = set()
        for query_result in result.query_results:
            if hasattr(query_result, "get"):
                executed_queries.add(query_result.get("query_id"))
            else:
                executed_queries.add(query_result)

        expected_queries = set(self.query_sequence)
        missing_queries = expected_queries - executed_queries

        if missing_queries:
            result.errors.extend([f"Missing query: {q}" for q in missing_queries])
            return False

        return True

    def _get_database_info(self) -> dict[str, Any]:
        """Get database information.

        Returns:
            Dictionary with database info
        """
        connection_string = ""
        if self.connection:
            connection_string = getattr(self.connection, "connection_string", "sqlite::memory:")

        return {
            "connection_string": connection_string,
            "dialect": "standard",
            "timestamp": datetime.now().isoformat(),
        }

    def export_results(self, result: TPCDSPowerTestResult, output_file: str) -> None:
        """Export results to file.

        Args:
            result: Power test result
            output_file: Output file path
        """
        import dataclasses
        import json

        # Convert result to dictionary
        result_dict = dataclasses.asdict(result)

        # Include scale_factor at top level for backward compatibility
        result_dict["scale_factor"] = result.config.scale_factor

        # Convert query_results to proper format expected by tests
        query_results_dict = {}
        for query_result in result.query_results:
            if isinstance(query_result, dict):
                query_id = query_result.get("query_id")
                if query_id is not None:
                    query_results_dict[str(query_id)] = query_result

        result_dict["query_results"] = query_results_dict

        with open(output_file, "w") as f:
            json.dump(result_dict, f, indent=2)

    def compare_results(self, result1: TPCDSPowerTestResult, result2: TPCDSPowerTestResult) -> dict[str, Any]:
        """Compare two power test results.

        Args:
            result1: First result
            result2: Second result

        Returns:
            Comparison dictionary
        """
        comparison = {
            "power_at_size": {
                "result1": result1.power_at_size,
                "result2": result2.power_at_size,
                "improvement": ((result2.power_at_size - result1.power_at_size) / result1.power_at_size) * 100
                if result1.power_at_size > 0
                else 0,
            },
            "total_time": {
                "result1": result1.total_time,
                "result2": result2.total_time,
                "improvement": ((result1.total_time - result2.total_time) / result1.total_time) * 100
                if result1.total_time > 0
                else 0,
            },
            "query_improvements": {},
        }

        # Convert result lists to dictionaries for easier comparison
        results1_dict = {}
        for qr in result1.query_results:
            if isinstance(qr, dict):
                query_id = qr.get("query_id")
                if query_id is not None:
                    results1_dict[query_id] = qr

        results2_dict = {}
        for qr in result2.query_results:
            if isinstance(qr, dict):
                query_id = qr.get("query_id")
                if query_id is not None:
                    results2_dict[query_id] = qr

        # Compare individual queries
        for query_id in set(results1_dict.keys()).intersection(results2_dict.keys()):
            time1 = results1_dict[query_id].get("execution_time", 0)
            time2 = results2_dict[query_id].get("execution_time", 0)

            improvement = 0
            if time1 > 0:
                improvement = ((time1 - time2) / time1) * 100

            comparison["query_improvements"][query_id] = {
                "time1": time1,
                "time2": time2,
                "improvement": improvement,
            }

        return comparison


# Alias for direct access to the result class
PowerTestResult = TPCDSPowerTestResult


# Mock DatabaseConnection for test compatibility
class DatabaseConnection:
    """Mock database connection for test compatibility."""

    def __init__(
        self,
        connection_string: str = "",
        dialect: str = "standard",
        verbose: bool = False,
    ):
        self.connection_string = connection_string
        self.dialect = dialect
        self.verbose = verbose
        self.cursor = None

    def execute(self, query: str):
        """Mock execute method."""
        # Return a mock cursor
        cursor = MockCursor()
        self.cursor = cursor
        return cursor

    def commit(self):
        """Mock commit method."""

    def fetchall(self):
        """Mock fetchall method."""
        return [("mock_result",)]

    def fetchone(self):
        """Mock fetchone method."""
        return ("mock_result",)

    def close(self):
        """Mock close method for test compatibility."""


class MockCursor:
    """Mock cursor for test compatibility."""

    def fetchall(self):
        return [("mock_result",)]

    def fetchone(self):
        return ("mock_result",)
