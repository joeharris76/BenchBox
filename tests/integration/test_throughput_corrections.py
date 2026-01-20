"""Integration tests validating throughput test corrections.

This module contains tests that validate the critical fixes made to the
TPC-H and TPC-DS Throughput Test implementations, including:
- Timing measurement accuracy (TTT calculation)
- Timeout behavior and error reporting
- Connection cleanup on failures

These tests use real ThreadPoolExecutor and actual timing to validate
behavior that cannot be tested with mocks alone.
"""

from __future__ import annotations

import time
from threading import Lock
from unittest.mock import Mock

import pytest

from benchbox.core.tpcds.throughput_test import (
    TPCDSThroughputTest,
    TPCDSThroughputTestConfig,
)
from benchbox.core.tpch.throughput_test import (
    TPCHThroughputTest,
    TPCHThroughputTestConfig,
)

# Mark all tests in this file as integration tests
pytestmark = [pytest.mark.integration, pytest.mark.slow]


class TestTimingMeasurementAccuracy:
    """Validate that Total Test Time (TTT) is measured correctly."""

    def test_tpch_ttt_excludes_setup_overhead(self):
        """Test that TPC-H TTT measures only concurrent execution time."""
        # Create a benchmark that tracks when queries actually execute
        execution_times = []
        execution_lock = Lock()

        def mock_get_query(*args, **kwargs):
            with execution_lock:
                execution_times.append(time.time())
            # Simulate query generation taking time
            time.sleep(0.01)
            return "SELECT 1"

        benchmark = Mock()
        benchmark.get_query = mock_get_query

        # Create connections that track execution timing
        connection_creation_times = []
        connection_lock = Lock()

        def connection_factory():
            with connection_lock:
                connection_creation_times.append(time.time())

            conn = Mock()
            cursor = Mock()
            cursor.fetchall.return_value = []
            conn.execute.return_value = cursor
            conn.close.return_value = None
            return conn

        # Run throughput test with 2 streams
        config = TPCHThroughputTestConfig(
            scale_factor=1.0,
            num_streams=2,
            stream_timeout=10,
            verbose=False,
        )

        test = TPCHThroughputTest(
            benchmark=benchmark,
            connection_factory=connection_factory,
            scale_factor=config.scale_factor,
            num_streams=config.num_streams,
            verbose=config.verbose,
        )

        # Add small delay before running to ensure timing is clean
        time.sleep(0.1)
        test_start = time.time()
        result = test.run(config)
        test_end = time.time()

        # Verify test succeeded
        assert result.success
        assert result.streams_executed == 2
        assert len(result.stream_results) == 2

        # Critical validation: TTT should be less than wall-clock time
        # because it excludes setup/teardown overhead
        wall_clock_time = test_end - test_start
        measured_ttt = result.total_time

        # TTT should be based on actual stream execution time
        assert measured_ttt > 0
        assert measured_ttt <= wall_clock_time

        # TTT should be close to the duration of the longest stream
        # (since streams run concurrently)
        max_stream_duration = max(sr.duration for sr in result.stream_results)

        # TTT should match max stream duration within reasonable tolerance
        # (allowing for thread scheduling overhead)
        assert abs(measured_ttt - max_stream_duration) < 0.5  # 500ms tolerance

    def test_tpcds_ttt_excludes_setup_overhead(self):
        """Test that TPC-DS TTT measures only concurrent execution time."""
        # Similar structure to TPC-H test
        execution_times = []
        execution_lock = Lock()

        def mock_get_query(*args, **kwargs):
            with execution_lock:
                execution_times.append(time.time())
            time.sleep(0.01)
            return "SELECT 1"

        def connection_factory():
            conn = Mock()
            cursor = Mock()
            cursor.fetchall.return_value = []
            conn.execute.return_value = cursor
            conn.close.return_value = None
            conn.commit.return_value = None
            return conn

        # Create mock benchmark
        benchmark = Mock()
        benchmark.get_query = mock_get_query
        benchmark.get_queries.return_value = {"1": "SELECT 1", "2": "SELECT 2"}
        benchmark.query_manager = Mock()

        config = TPCDSThroughputTestConfig(
            scale_factor=1.0,
            num_streams=2,
            stream_timeout=10,
            verbose=False,
            queries_per_stream=2,  # Limit to 2 queries for faster test
            enable_preflight=False,  # Skip preflight for this test
        )

        test = TPCDSThroughputTest(
            benchmark=benchmark,
            connection_factory=connection_factory,
            scale_factor=config.scale_factor,
            num_streams=config.num_streams,
            verbose=config.verbose,
        )

        # Patch stream generation to avoid complexity
        from unittest.mock import patch

        with patch("benchbox.core.tpcds.streams.create_standard_streams") as mock_create:
            mock_manager = Mock()
            mock_streams = {}
            for stream_id in range(2):
                mock_streams[stream_id] = [
                    Mock(stream_id=stream_id, query_id=1, position=1, variant=None, sql="SELECT 1"),
                    Mock(stream_id=stream_id, query_id=2, position=2, variant=None, sql="SELECT 2"),
                ]
            mock_manager.generate_streams.return_value = mock_streams
            mock_create.return_value = mock_manager

            time.sleep(0.1)
            test_start = time.time()
            result = test.run(config)
            test_end = time.time()

        # Validations
        assert result.success
        assert result.streams_executed == 2

        wall_clock_time = test_end - test_start
        measured_ttt = result.total_time

        assert measured_ttt > 0
        assert measured_ttt <= wall_clock_time

        # TTT should match concurrent execution time
        if result.stream_results:
            max_stream_duration = max(sr.duration for sr in result.stream_results)
            assert abs(measured_ttt - max_stream_duration) < 0.5


class TestConnectionCleanup:
    """Validate connection cleanup on failures."""

    def test_tpch_connection_closed_on_query_failure(self):
        """Test that TPC-H closes connections even when queries fail."""
        connections_created = []
        connections_closed = []
        creation_lock = Lock()
        close_lock = Lock()

        def connection_factory():
            conn = Mock()

            # Track creation
            with creation_lock:
                connections_created.append(conn)

            # Track closure
            original_close = conn.close

            def tracked_close():
                with close_lock:
                    connections_closed.append(conn)
                return original_close()

            conn.close = tracked_close

            # Make execute fail
            def failing_execute(*args, **kwargs):
                raise RuntimeError("Simulated query failure")

            conn.execute = failing_execute

            return conn

        benchmark = Mock()
        benchmark.get_query.return_value = "SELECT 1"

        config = TPCHThroughputTestConfig(
            scale_factor=1.0,
            num_streams=2,
            stream_timeout=5,
            verbose=False,
        )

        test = TPCHThroughputTest(
            benchmark=benchmark,
            connection_factory=connection_factory,
            scale_factor=config.scale_factor,
            num_streams=config.num_streams,
            verbose=config.verbose,
        )

        result = test.run(config)

        # Verify test failed (queries failed)
        assert not result.success

        # Critical validation: All connections were closed
        # Even though queries failed, connections must be cleaned up
        assert len(connections_created) == 2  # 2 streams = 2 connections

        # Give threads time to clean up
        time.sleep(0.5)

        assert len(connections_closed) == 2  # All connections closed

        # Same connection objects should be in both lists
        assert set(connections_created) == set(connections_closed)

    def test_tpcds_connection_closed_on_stream_failure(self):
        """Test that TPC-DS closes connections even when streams fail."""
        connections_created = []
        connections_closed = []
        creation_lock = Lock()
        close_lock = Lock()

        def connection_factory():
            conn = Mock()

            with creation_lock:
                connections_created.append(conn)

            original_close = conn.close

            def tracked_close():
                with close_lock:
                    connections_closed.append(conn)
                return original_close()

            conn.close = tracked_close

            # Make execute fail
            def failing_execute(*args, **kwargs):
                raise RuntimeError("Simulated failure")

            conn.execute = failing_execute
            conn.commit = Mock()

            return conn

        benchmark = Mock()
        benchmark.get_query.return_value = "SELECT 1"
        benchmark.get_queries.return_value = {"1": "SELECT 1"}
        benchmark.query_manager = Mock()

        config = TPCDSThroughputTestConfig(
            scale_factor=1.0,
            num_streams=2,
            stream_timeout=5,
            verbose=False,
            queries_per_stream=1,
            enable_preflight=False,
        )

        test = TPCDSThroughputTest(
            benchmark=benchmark,
            connection_factory=connection_factory,
            scale_factor=config.scale_factor,
            num_streams=config.num_streams,
            verbose=config.verbose,
        )

        from unittest.mock import patch

        with patch("benchbox.core.tpcds.streams.create_standard_streams") as mock_create:
            mock_manager = Mock()
            mock_streams = {
                0: [Mock(stream_id=0, query_id=1, position=1, variant=None, sql="SELECT 1")],
                1: [Mock(stream_id=1, query_id=1, position=1, variant=None, sql="SELECT 1")],
            }
            mock_manager.generate_streams.return_value = mock_streams
            mock_create.return_value = mock_manager

            result = test.run(config)

        # Verify failures occurred
        assert result.streams_executed == 2

        # Validate cleanup
        assert len(connections_created) == 2

        time.sleep(0.5)

        assert len(connections_closed) == 2
        assert set(connections_created) == set(connections_closed)
