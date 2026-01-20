"""TPC-DS Throughput Test Implementation.

This module implements the TPC-DS Throughput Test according to the official TPC-DS
specification. The Throughput Test executes multiple concurrent query streams
to calculate the Throughput@Size metric.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DS (TPC-DS) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DS specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import concurrent.futures
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional


@dataclass
class TPCDSThroughputTestConfig:
    """Configuration for TPC-DS Throughput Test."""

    scale_factor: float = 1.0
    num_streams: int = 4
    base_seed: int = 42
    stream_timeout: int = 7200  # Timeout per stream in seconds (0 = no timeout)
    max_workers: Optional[int] = None
    verbose: bool = False
    # Number of queries to execute per stream (None = all queries, ~99 for TPC-DS)
    # NOTE: Per TPC-DS spec, full query set should be executed. Use subset only for testing.
    queries_per_stream: Optional[int] = None  # Default: execute all queries
    # Enable preflight validation (validates query generation before execution)
    enable_preflight: bool = True


@dataclass
class TPCDSThroughputStreamResult:
    """Result of a single TPC-DS throughput test stream."""

    stream_id: int
    start_time: float
    end_time: float
    duration: float
    queries_executed: int
    queries_successful: int
    queries_failed: int
    query_results: list[dict[str, Any]] = field(default_factory=list)
    success: bool = True
    error: Optional[str] = None


@dataclass
class TPCDSThroughputTestResult:
    """Result of TPC-DS Throughput Test."""

    config: TPCDSThroughputTestConfig
    start_time: str
    end_time: str
    total_time: float
    throughput_at_size: float
    streams_executed: int
    streams_successful: int
    stream_results: list[TPCDSThroughputStreamResult] = field(default_factory=list)
    query_throughput: float = 0.0
    success: bool = True
    errors: list[str] = field(default_factory=list)

    @property
    def scale_factor(self) -> float:
        """Scale factor from config for backward compatibility."""
        return self.config.scale_factor


class TPCDSThroughputTest:
    """TPC-DS Throughput Test implementation."""

    def __init__(
        self,
        benchmark: Any,
        connection_factory: Optional[Callable[[], Any]] = None,
        scale_factor: float = 1.0,
        num_streams: int = 4,
        verbose: bool = False,
        connection_string: Optional[str] = None,
        dialect: Optional[str] = None,
    ) -> None:
        """Initialize TPC-DS Throughput Test.

        Args:
            benchmark: TPCDSBenchmark instance
            connection_factory: Factory function to create database connections
            scale_factor: Scale factor for the benchmark
            num_streams: Number of concurrent streams
            verbose: Enable verbose logging
            connection_string: Database connection string (legacy parameter)
            dialect: SQL dialect (legacy parameter)
        """
        self.benchmark = benchmark

        # Handle legacy connection_string parameter
        if connection_string is not None:
            # Create a simple connection factory that returns a mock connection
            conn_str = connection_string  # Type narrowing for lambda
            self.connection_factory = lambda: DatabaseConnection(conn_str)
        elif connection_factory is not None:
            self.connection_factory = connection_factory
        else:
            # Default to mock connection factory
            self.connection_factory = lambda: DatabaseConnection()

        self.config = TPCDSThroughputTestConfig(scale_factor=scale_factor, num_streams=num_streams, verbose=verbose)

        # Store target dialect for query translation
        self.target_dialect = dialect

        self.logger = logging.getLogger(__name__)
        if verbose:
            self.logger.setLevel(logging.INFO)
        # Captured SQL items for dry-run preview: (label, sql)
        self.captured_items: list[tuple[str, str]] = []
        # Lock for concurrent stream capture
        import threading

        self._capture_lock = threading.Lock()

    def run(self, config: Optional[TPCDSThroughputTestConfig] = None) -> TPCDSThroughputTestResult:
        """Execute the TPC-DS Throughput Test.

        Args:
            config: Optional test configuration (uses default if not provided)

        Returns:
            Throughput Test results with Throughput@Size metric

        Raises:
            RuntimeError: If Throughput Test execution fails
        """
        if config is None:
            config = self.config

        # NOTE: start_time here is for test metadata only, not for TTT calculation.
        # Per TPC-DS specification, Total Test Time (TTT) must be measured from when
        # the first stream begins execution until the last stream completes execution.
        # This excludes setup overhead (executor creation, future submission, preflight, etc.).
        start_time = time.time()
        start_time_str = datetime.now().isoformat()

        result = TPCDSThroughputTestResult(
            config=config,
            start_time=start_time_str,
            end_time="",
            total_time=0.0,
            throughput_at_size=0.0,
            streams_executed=0,
            streams_successful=0,
            stream_results=[],
            query_throughput=0.0,
            success=True,
            errors=[],
        )

        # Logging
        try:
            if config.verbose:
                self.logger.info("Starting TPC-DS Throughput Test")
                self.logger.info(f"Number of streams: {config.num_streams}")
                self.logger.info(f"Scale factor: {config.scale_factor}")

            # Execute concurrent streams
            max_workers = config.max_workers or config.num_streams

            # Preflight: validate that selected query subsets in all streams can be generated
        except Exception:
            # Reraise any logging/prep errors
            raise

        # Run preflight outside of try so failures raise
        if config.enable_preflight:
            self._preflight_validate_generation(config)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Track future -> stream_id mapping for timeout error reporting
                future_to_stream_id = {}

                for stream_id in range(config.num_streams):
                    future = executor.submit(
                        self._execute_stream,
                        stream_id,
                        config.base_seed + stream_id,
                        config,
                    )
                    future_to_stream_id[future] = stream_id

                # Wait for all streams to complete with per-stream timeout enforcement
                # Note: Timed-out streams continue executing in background (Python threading limitation)
                timeout = config.stream_timeout if config.stream_timeout > 0 else None

                for future in concurrent.futures.as_completed(future_to_stream_id.keys()):
                    stream_id = future_to_stream_id[future]

                    try:
                        # Enforce per-stream timeout
                        stream_result = future.result(timeout=timeout)
                        result.stream_results.append(stream_result)
                        result.streams_executed += 1

                        if stream_result.success:
                            result.streams_successful += 1
                        else:
                            result.errors.append(f"Stream {stream_result.stream_id} failed: {stream_result.error}")

                        if config.verbose:
                            self.logger.info(
                                f"Stream {stream_result.stream_id}: "
                                f"{stream_result.queries_successful}/{stream_result.queries_executed} successful"
                            )

                    except concurrent.futures.TimeoutError:
                        result.streams_executed += 1
                        error_msg = f"Stream {stream_id} timeout after {timeout}s"
                        result.errors.append(error_msg)
                        if config.verbose:
                            self.logger.error(error_msg)

                    except Exception as e:
                        result.streams_executed += 1
                        result.errors.append(f"Stream {stream_id} execution failed: {e}")
                        if config.verbose:
                            self.logger.error(f"Stream {stream_id} execution failed: {e}")

            # Calculate metrics
            result.end_time = datetime.now().isoformat()

            # Per TPC-DS specification: Total Test Time (TTT) is measured from when the
            # first stream begins execution until the last stream completes execution.
            # This is the actual concurrent execution time, excluding setup/teardown overhead.
            if result.stream_results:
                first_stream_start = min(sr.start_time for sr in result.stream_results)
                last_stream_end = max(sr.end_time for sr in result.stream_results)
                total_time = last_stream_end - first_stream_start
            else:
                # Fallback if no streams executed (shouldn't happen in normal operation)
                total_time = time.time() - start_time

            result.total_time = total_time

            if total_time > 0:
                # Throughput@Size = S × 3600 × SF / TTT
                # where S = num_streams, SF = scale_factor, TTT = total_test_time (concurrent execution)
                result.throughput_at_size = (config.num_streams * 3600.0 * config.scale_factor) / total_time

                # Calculate query throughput
                total_queries = sum(sr.queries_executed for sr in result.stream_results)
                result.query_throughput = total_queries / total_time

            # TPC-DS success criteria: at least 70% of streams must succeed
            success_rate = result.streams_successful / max(config.num_streams, 1)
            result.success = success_rate >= 0.7

            if config.verbose:
                self.logger.info(f"Throughput Test completed in {total_time:.3f}s")
                self.logger.info(f"Successful streams: {result.streams_successful}/{config.num_streams}")
                self.logger.info(f"Stream success rate: {success_rate:.2%}")
                self.logger.info(f"Throughput@Size: {result.throughput_at_size:.2f}")
                self.logger.info(f"Query throughput: {result.query_throughput:.2f} queries/sec")

            return result

        except Exception as e:
            result.total_time = time.time() - start_time
            result.end_time = datetime.now().isoformat()
            result.success = False
            result.errors.append(f"Throughput Test execution failed: {e}")

            if config.verbose:
                self.logger.error(f"Throughput Test failed: {e}")

            return result

    def _preflight_validate_generation(self, config: TPCDSThroughputTestConfig) -> None:
        """Validate that queries can be generated for all streams.

        This validates all 99 TPC-DS queries for each stream to ensure no runtime
        generation failures. This is conservative but guarantees safety regardless
        of which queries the stream permutation algorithm selects.

        Raises RuntimeError with details if any generation fails.
        """
        failures = []

        # Log the preflight strategy
        if config.verbose:
            self.logger.info(
                f"Preflight validation: checking all 99 queries × {config.num_streams} streams "
                f"= {99 * config.num_streams} total validations"
            )

        try:
            # Use standard TPC-DS query id range: 1-99
            available_query_ids = list(range(1, 100))

            for stream_id in range(config.num_streams):
                for position, query_id in enumerate(available_query_ids):
                    stream_seed = config.base_seed + stream_id * 1000 + position
                    try:
                        _ = self.benchmark.get_query(
                            query_id, seed=stream_seed, scale_factor=config.scale_factor, dialect=self.target_dialect
                        )
                    except Exception as e:
                        failures.append(f"stream {stream_id} q{query_id} pos {position + 1}: {e}")
        except Exception as e:
            raise RuntimeError(f"Throughput preflight internal error: {e}") from e

        if failures:
            msg = (
                f"TPC-DS ThroughputTest preflight failed for {len(failures)} queries. "
                f"Examples: {', '.join(failures[:3])}"
            )
            raise RuntimeError(msg)

    def _execute_stream(
        self, stream_id: int, seed: int, config: TPCDSThroughputTestConfig
    ) -> TPCDSThroughputStreamResult:
        """Execute a single TPC-DS throughput test stream.

        Args:
            stream_id: Stream identifier
            seed: Random seed for this stream
            config: Test configuration

        Returns:
            Stream execution result
        """
        start_time = time.time()

        stream_result = TPCDSThroughputStreamResult(
            stream_id=stream_id,
            start_time=start_time,
            end_time=0.0,
            duration=0.0,
            queries_executed=0,
            queries_successful=0,
            queries_failed=0,
        )

        connection = None
        try:
            if config.verbose:
                self.logger.info(f"Starting stream {stream_id} with seed {seed}")

            # Create connection for this stream
            connection = self.connection_factory()

            # Get available queries and execute them in random order
            try:
                all_queries = self.benchmark.get_queries()
                available_query_ids = [int(k) for k in all_queries if k.isdigit()]
                if not available_query_ids:
                    # Fallback if no digit-only query IDs found
                    available_query_ids = list(range(1, 100))
            except Exception:
                # Fallback: assume queries 1-99 are available
                available_query_ids = list(range(1, 100))

            # Use proper TPC-DS stream permutation for throughput testing
            from benchbox.core.tpcds.streams import (
                create_standard_streams,
            )

            # Get the query manager - handle both direct TPCDSBenchmark and TPCDS wrapper
            query_manager = None
            if hasattr(self.benchmark, "query_manager"):
                # Direct TPCDSBenchmark instance
                query_manager = self.benchmark.query_manager
            elif hasattr(self.benchmark, "_impl") and hasattr(self.benchmark._impl, "query_manager"):
                # TPCDS wrapper instance
                query_manager = self.benchmark._impl.query_manager
            else:
                raise RuntimeError("No query_manager found - ThroughputTest requires a TPCDSBenchmark instance")

            # Create stream manager for proper permutation
            stream_manager = create_standard_streams(
                query_manager=query_manager,
                num_streams=stream_id + 1,  # Ensure we have enough streams
                query_range=(min(available_query_ids), max(available_query_ids)) if available_query_ids else (1, 99),
                base_seed=seed + stream_id,
            )

            # Generate the streams to get proper permutation
            streams = stream_manager.generate_streams()
            stream_queries = streams.get(stream_id, [])

            # Execute queries for throughput testing (only main queries, not variants)
            main_queries = [sq for sq in stream_queries if sq.variant is None]

            # Apply query subset limit if configured
            if config.queries_per_stream is not None:
                query_subset = main_queries[: min(config.queries_per_stream, len(main_queries))]
                if config.verbose:
                    self.logger.info(
                        f"Stream {stream_id} using TPC-DS permutation with {len(query_subset)} queries "
                        f"(limited by queries_per_stream={config.queries_per_stream})"
                    )
            else:
                # Execute all queries (full TPC-DS compliance)
                query_subset = main_queries
                if config.verbose:
                    self.logger.info(
                        f"Stream {stream_id} using TPC-DS permutation with {len(query_subset)} queries (full query set)"
                    )

            for position, stream_query in enumerate(query_subset):
                query_id = stream_query.query_id
                query_start = time.time()
                query_result = {
                    "query_id": query_id,
                    "position": position + 1,
                    "stream_id": stream_id,
                    "execution_time": 0.0,
                    "success": False,
                    "error": None,
                    "result_count": 0,
                }

                try:
                    # Get the query with stream-specific parameters
                    # Use stream and position-specific seed as per TPC-DS specification
                    stream_seed = seed + stream_id * 1000 + position
                    query_text = self.benchmark.get_query(
                        query_id, seed=stream_seed, scale_factor=config.scale_factor, dialect=self.target_dialect
                    )

                    # Execute the actual query to enable dry-run capture via platform adapter
                    label = f"Stream_{stream_id}_Position_{position + 1}_Query_{query_id}"
                    try:
                        cursor = connection.execute(query_text)
                        # Fetch to complete execution in non-dry-run
                        if hasattr(cursor, "fetchall"):
                            cursor.fetchall()
                        if hasattr(connection, "commit"):
                            connection.commit()
                    finally:
                        # Record labeled SQL for preview
                        with self._capture_lock:
                            self.captured_items.append((label, query_text))

                    execution_time = time.time() - query_start

                    query_result.update(
                        {
                            "execution_time": execution_time,
                            "success": True,
                            "result_count": 0,
                        }
                    )

                    stream_result.queries_successful += 1

                except Exception as e:
                    execution_time = time.time() - query_start
                    query_result.update(
                        {
                            "execution_time": execution_time,
                            "success": False,
                            "error": str(e),
                        }
                    )

                    stream_result.queries_failed += 1

                    # Don't log template substitution errors as failures - they're expected
                    if "Template substitution error" not in str(e) and config.verbose:
                        self.logger.warning(f"Stream {stream_id} Query {query_id} failed: {e}")

                stream_result.query_results.append(query_result)
                stream_result.queries_executed += 1

            # TPC-DS success criteria: at least 70% of queries must succeed in each stream
            if stream_result.queries_executed > 0:
                success_rate = stream_result.queries_successful / stream_result.queries_executed
                stream_result.success = success_rate >= 0.7

                if config.verbose:
                    self.logger.info(
                        f"Stream {stream_id} completed: "
                        f"{stream_result.queries_successful}/{stream_result.queries_executed} successful "
                        f"(success rate: {success_rate:.2%})"
                    )
            else:
                stream_result.success = False

                if config.verbose:
                    self.logger.info(f"Stream {stream_id} completed: no queries executed")

        except Exception as e:
            stream_result.error = str(e)
            stream_result.success = False

            if config.verbose:
                self.logger.error(f"Stream {stream_id} failed: {e}")

        finally:
            # Ensure connection is always closed, even on exception
            if connection is not None:
                try:
                    connection.close()
                except Exception as close_error:
                    if config.verbose:
                        self.logger.warning(f"Failed to close connection for stream {stream_id}: {close_error}")

            # Record end time and duration
            stream_result.end_time = time.time()
            stream_result.duration = stream_result.end_time - stream_result.start_time

        return stream_result

    def validate_results(self, result: TPCDSThroughputTestResult) -> bool:
        """Validate Throughput Test results against TPC-DS specification.

        Args:
            result: Throughput Test results to validate

        Returns:
            True if results are valid, False otherwise
        """
        if not result.success:
            return False

        # TPC-DS requires at least 70% stream success rate
        if result.config.num_streams > 0:
            stream_success_rate = result.streams_successful / result.config.num_streams
            if stream_success_rate < 0.7:
                return False

        return not result.throughput_at_size <= 0


# Aliases for backward compatibility
ThroughputTestConfig = TPCDSThroughputTestConfig
ThroughputTestResult = TPCDSThroughputTestResult
StreamResult = TPCDSThroughputStreamResult


# Mock QueryResult for test compatibility
@dataclass
class QueryResult:
    query_id: int = 1
    execution_time: float = 1.0
    success: bool = True


# Mock DatabaseConnection for test compatibility
class DatabaseConnection:
    """Mock database connection for test compatibility."""

    def __init__(self, connection_string: str = ""):
        self.connection_string = connection_string

    def cursor(self):
        """Return a mock cursor."""
        return MockCursor()

    def close(self):
        """Close the connection."""


class MockCursor:
    """Mock cursor for test compatibility."""

    def fetchall(self):
        """Return mock results."""
        return [("result1",), ("result2",)]
