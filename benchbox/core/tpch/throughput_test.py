"""TPC-H Throughput Test Implementation.

This module implements the TPC-H Throughput Test according to the official TPC-H
specification. The Throughput Test executes multiple concurrent query streams
to calculate the Throughput@Size metric.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-H specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import concurrent.futures
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional


@dataclass
class TPCHThroughputTestConfig:
    """Configuration for TPC-H Throughput Test."""

    scale_factor: float = 1.0
    num_streams: int = 2
    base_seed: int = 42
    stream_timeout: int = 3600  # Timeout per stream in seconds (0 = no timeout)
    max_workers: Optional[int] = None
    verbose: bool = False
    # Minimum success rate for streams (0.0-1.0). Default 0.99 = 99% must succeed.
    # TPC-H spec allows up to 1% query failures in production environments.
    min_success_rate: float = 0.99


@dataclass
class TPCHThroughputStreamResult:
    """Result of a single throughput test stream."""

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
class TPCHThroughputTestResult:
    """Result of TPC-H Throughput Test."""

    config: TPCHThroughputTestConfig
    start_time: str
    end_time: str
    total_time: float
    throughput_at_size: float
    streams_executed: int
    streams_successful: int
    stream_results: list[TPCHThroughputStreamResult] = field(default_factory=list)
    query_throughput: float = 0.0
    success: bool = True
    errors: list[str] = field(default_factory=list)

    @property
    def scale_factor(self) -> float:
        """Get scale factor from config."""
        return self.config.scale_factor


class TPCHThroughputTest:
    """TPC-H Throughput Test implementation."""

    def __init__(
        self,
        benchmark: Any,
        connection_factory: Callable[[], Any],
        scale_factor: float = 1.0,
        num_streams: int = 2,
        verbose: bool = False,
    ) -> None:
        """Initialize TPC-H Throughput Test.

        Args:
            benchmark: TPCHBenchmark instance
            connection_factory: Factory function to create database connections
            scale_factor: Scale factor for the benchmark
            num_streams: Number of concurrent streams
            verbose: Enable verbose logging
        """
        self.benchmark = benchmark
        self.connection_factory = connection_factory
        self.config = TPCHThroughputTestConfig(scale_factor=scale_factor, num_streams=num_streams, verbose=verbose)

        self.logger = logging.getLogger(__name__)
        if verbose:
            self.logger.setLevel(logging.INFO)

        # Initialize captured items for dry-run SQL preview
        self.captured_items: list[tuple[str, str]] = []

    def run(self, config: Optional[TPCHThroughputTestConfig] = None) -> TPCHThroughputTestResult:
        """Execute the TPC-H Throughput Test.

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
        # Per TPC-H specification, Total Test Time (TTT) must be measured from when
        # the first stream begins execution until the last stream completes execution.
        # This excludes setup overhead (executor creation, future submission, etc.).
        start_time = time.time()
        start_time_str = datetime.now().isoformat()

        result = TPCHThroughputTestResult(
            config=config,
            start_time=start_time_str,
            end_time="",
            total_time=0.0,
            throughput_at_size=0.0,
            streams_executed=0,
            streams_successful=0,
            query_throughput=0.0,
        )

        try:
            if config.verbose:
                self.logger.info("Starting TPC-H Throughput Test")
                self.logger.info(f"Number of streams: {config.num_streams}")
                self.logger.info(f"Scale factor: {config.scale_factor}")

            # Execute concurrent streams
            max_workers = config.max_workers or config.num_streams

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

            # Per TPC-H specification: Total Test Time (TTT) is measured from when the
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

            # TPC-H success criteria: configurable stream success rate
            # Default is 99% (allows up to 1% failures per TPC-H spec)
            if config.num_streams > 0:
                success_rate = result.streams_successful / config.num_streams
                result.success = success_rate >= config.min_success_rate
            else:
                result.success = False

            if config.verbose:
                self.logger.info(f"Throughput Test completed in {total_time:.3f}s")
                self.logger.info(f"Successful streams: {result.streams_successful}/{config.num_streams}")
                if config.num_streams > 0:
                    success_rate = result.streams_successful / config.num_streams
                    self.logger.info(
                        f"Stream success rate: {success_rate:.2%} (threshold: {config.min_success_rate:.2%})"
                    )
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

    def _execute_stream(
        self, stream_id: int, seed: int, config: TPCHThroughputTestConfig
    ) -> TPCHThroughputStreamResult:
        """Execute a single throughput test stream.

        Args:
            stream_id: Stream identifier
            seed: Random seed for this stream
            config: Test configuration

        Returns:
            Stream execution result
        """
        start_time = time.time()

        stream_result = TPCHThroughputStreamResult(
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

            # Execute all 22 TPC-H queries in proper TPC-H permutation order for this stream
            from benchbox.core.tpch.streams import TPCHStreams

            # Use stream-specific permutation from TPC-H specification
            query_permutation = TPCHStreams.PERMUTATION_MATRIX[stream_id % len(TPCHStreams.PERMUTATION_MATRIX)]

            if config.verbose:
                self.logger.info(f"Stream {stream_id} using TPC-H permutation: {query_permutation}")

            for position, query_id in enumerate(query_permutation):
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
                    # Use stream and position-specific seed as per TPC-H specification
                    stream_seed = seed + stream_id * 1000 + position
                    query_text = self.benchmark.get_query(
                        query_id,
                        seed=stream_seed,
                        stream_id=stream_id,
                        scale_factor=config.scale_factor,
                    )

                    # Execute the actual query against the database
                    label = f"Stream_{stream_id}_Position_{position + 1}_Query_{query_id}"
                    try:
                        # Set query context for validation
                        if hasattr(connection, "set_query_context"):
                            connection.set_query_context(query_id)

                        cursor = connection.execute(query_text)
                        rows = cursor.fetchall() if hasattr(cursor, "fetchall") else []

                        # Check for validation failures from platform adapter
                        if hasattr(cursor, "platform_result"):
                            result_dict = cursor.platform_result
                            if result_dict.get("status") == "FAILED":
                                error_msg = result_dict.get(
                                    "error", result_dict.get("row_count_validation_error", "Query validation failed")
                                )
                                raise RuntimeError(error_msg)

                        if hasattr(connection, "commit"):
                            connection.commit()
                    finally:
                        # Capture labeled SQL for dry-run preview
                        if hasattr(self, "captured_items"):
                            self.captured_items.append((label, query_text))

                    execution_time = time.time() - query_start

                    query_result.update(
                        {
                            "execution_time": execution_time,
                            "success": True,
                            "result_count": len(rows),
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

                    if config.verbose:
                        self.logger.error(f"Stream {stream_id} Query {query_id} failed: {e}")

                stream_result.query_results.append(query_result)
                stream_result.queries_executed += 1

            stream_result.success = stream_result.queries_failed == 0

            if config.verbose:
                self.logger.info(
                    f"Stream {stream_id} completed: "
                    f"{stream_result.queries_successful}/{stream_result.queries_executed} successful"
                )

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

    def validate_results(self, result: TPCHThroughputTestResult) -> bool:
        """Validate Throughput Test results against TPC-H specification.

        Args:
            result: Throughput Test results to validate

        Returns:
            True if results are valid, False otherwise
        """
        if not result.success:
            return False

        if result.streams_successful != result.config.num_streams:
            return False

        if result.throughput_at_size <= 0:
            return False

        # Ensure all streams executed all 22 queries
        return all(stream_result.queries_executed == 22 for stream_result in result.stream_results)
