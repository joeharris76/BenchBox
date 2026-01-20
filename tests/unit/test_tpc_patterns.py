"""Unit tests for TPC patterns module.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Any, Union
from unittest.mock import Mock, patch

import pytest

from benchbox.core.tpc_patterns import (
    BenchmarkTestRunner,
    ErrorHandler,
    # Enums
    ExecutionStatus,
    ParameterManager,
    # Data classes
    PermutationConfig,
    ProgressTracker,
    # Core classes
    QueryPermutator,
    QueryResult,
    ResultAggregator,
    RetryPolicy,
    StreamExecutor,
    StreamResult,
    TransactionManager,
    create_basic_query_executor,
    # Utility functions
    create_tpc_stream_configs,
    create_validation_function,
)

# Mark all tests in this file as unit tests
pytestmark = [pytest.mark.unit, pytest.mark.fast]


class MockDatabaseConnection:
    """Mock database connection for testing."""

    def __init__(self, fail_on_query=None):
        self.queries_executed = []
        self.fail_on_query = fail_on_query
        self.closed = False

    def execute(self, query: str, params: list[Any] | None = None):
        if self.fail_on_query and self.fail_on_query in query:
            raise Exception(f"Mock failure for query: {query}")
        self.queries_executed.append(query)
        return f"cursor_{len(self.queries_executed)}"

    def fetchall(self, cursor):
        return [("row1", "data1"), ("row2", "data2")]

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.closed = True


class MockBenchmark:
    """Mock benchmark for testing."""

    def __init__(self, queries=None):
        self.queries = queries or {
            1: "SELECT * FROM table1",
            2: "SELECT * FROM table2",
            3: "SELECT * FROM table3",
        }

    def get_query(self, query_id: Union[int, str], params: dict[str, Any] | None = None) -> str:
        return self.queries.get(int(query_id), f"SELECT * FROM unknown_table_{query_id}")


class TestQueryPermutator:
    """Test QueryPermutator class."""

    def test_sequential_permutation(self):
        """Test sequential permutation mode."""
        config = PermutationConfig(mode="sequential")
        permutator = QueryPermutator(config)

        query_ids = [3, 1, 4, 2, 5]
        result = permutator.generate_permutation(query_ids)

        assert result == [1, 2, 3, 4, 5]
        assert permutator.validate_permutation(query_ids, result)

    def test_random_permutation_with_seed(self):
        """Test random permutation with seed for reproducibility."""
        config = PermutationConfig(mode="random", seed=42)
        permutator = QueryPermutator(config)

        query_ids = [1, 2, 3, 4, 5]
        result1 = permutator.generate_permutation(query_ids)
        result2 = permutator.generate_permutation(query_ids)

        assert result1 == result2  # Same seed should produce same result
        assert permutator.validate_permutation(query_ids, result1)

    def test_tpc_standard_permutation(self):
        """Test TPC standard permutation mode."""
        config = PermutationConfig(mode="tpc_standard", seed=42)
        permutator = QueryPermutator(config)

        query_ids = [1, 2, 3, 4, 5]
        result = permutator.generate_permutation(query_ids)

        assert len(result) == len(query_ids)
        assert set(result) == set(query_ids)
        assert permutator.validate_permutation(query_ids, result)

    def test_invalid_permutation_mode(self):
        """Test invalid permutation mode raises error."""
        config = PermutationConfig(mode="invalid_mode")
        permutator = QueryPermutator(config)

        with pytest.raises(ValueError, match="Unknown permutation mode"):
            permutator.generate_permutation([1, 2, 3])

    def test_validation_failures(self):
        """Test permutation validation failures."""
        config = PermutationConfig(mode="sequential")
        permutator = QueryPermutator(config)

        original = [1, 2, 3]

        # Different lengths
        assert not permutator.validate_permutation(original, [1, 2])

        # Different elements
        assert not permutator.validate_permutation(original, [1, 2, 4])

        # Duplicates with ensure_unique=True
        config.ensure_unique = True
        permutator = QueryPermutator(config)
        assert not permutator.validate_permutation(original, [1, 1, 2])


class TestParameterManager:
    """Test ParameterManager class."""

    def test_deterministic_parameter_generation(self):
        """Test that parameters are generated deterministically."""
        manager = ParameterManager(base_seed=42)

        params1 = manager.generate_parameters(query_id=1, stream_id=0, scale_factor=1.0)
        params2 = manager.generate_parameters(query_id=1, stream_id=0, scale_factor=1.0)

        assert params1 == params2
        assert "scale_factor" in params1
        assert "query_id" in params1
        assert "random_seed" in params1

    def test_different_parameters_for_different_contexts(self):
        """Test that different query/stream combinations produce different parameters."""
        manager = ParameterManager(base_seed=42)

        params1 = manager.generate_parameters(query_id=1, stream_id=0)
        params2 = manager.generate_parameters(query_id=2, stream_id=0)
        params3 = manager.generate_parameters(query_id=1, stream_id=1)

        assert params1 != params2
        assert params1 != params3
        assert params2 != params3

    def test_custom_seed_override(self):
        """Test that custom seed overrides computed seed."""
        manager = ParameterManager(base_seed=42)

        params1 = manager.generate_parameters(query_id=1, stream_id=0, custom_seed=123)
        params2 = manager.generate_parameters(query_id=2, stream_id=1, custom_seed=123)

        # Same custom seed should produce same random_seed
        assert params1["random_seed"] == params2["random_seed"]

    def test_cache_functionality(self):
        """Test parameter caching."""
        manager = ParameterManager(base_seed=42)

        # First call should compute parameters
        params1 = manager.generate_parameters(query_id=1, stream_id=0)

        # Second call should use cache
        params2 = manager.generate_parameters(query_id=1, stream_id=0)

        assert params1 == params2
        assert len(manager._parameter_cache) == 1

        # Clear cache
        manager.clear_cache()
        assert len(manager._parameter_cache) == 0


class TestProgressTracker:
    """Test ProgressTracker class."""

    def test_progress_tracking(self):
        """Test basic progress tracking."""
        tracker = ProgressTracker(total_items=10, description="Test")

        # Set progress
        tracker.update(completed=3)
        tracker.update(completed=2)  # Should add to existing completed
        tracker.update(failed=2)

        summary = tracker.get_summary()
        assert summary["total_items"] == 10
        assert summary["completed_items"] == 6
        assert summary["failed_items"] == 2
        assert summary["total_processed"] == 8
        assert summary["success_rate"] == 6 / 8

    def test_progress_logging(self):
        """Test progress logging functionality."""
        with patch("logging.getLogger") as mock_logger:
            mock_log = Mock()
            mock_logger.return_value = mock_log

            tracker = ProgressTracker(total_items=10, description="Test")

            # Set progress to trigger logging
            tracker.update(completed=10)

            # Check that logging was called
            mock_log.info.assert_called()


class TestResultAggregator:
    """Test ResultAggregator class."""

    def test_query_result_aggregation(self):
        """Test aggregation of query results."""
        aggregator = ResultAggregator()

        # Include query results
        result1 = QueryResult(query_id=1, execution_time=0.1, status=ExecutionStatus.COMPLETED)
        result2 = QueryResult(
            query_id=2,
            execution_time=0.2,
            status=ExecutionStatus.FAILED,
            error="Test error",
        )

        aggregator.add_query_result(result1)
        aggregator.add_query_result(result2)

        results = aggregator.get_aggregated_results()
        assert results["total_queries"] == 2
        assert results["successful_queries"] == 1
        assert results["failed_queries"] == 1
        assert results["success_rate"] == 0.5
        assert results["total_execution_time"] == 0.1

    def test_stream_result_aggregation(self):
        """Test aggregation of stream results."""
        aggregator = ResultAggregator()

        # stream result
        query_results = [
            QueryResult(query_id=1, execution_time=0.1, status=ExecutionStatus.COMPLETED),
            QueryResult(query_id=2, execution_time=0.2, status=ExecutionStatus.COMPLETED),
        ]

        stream_result = StreamResult(
            stream_id=0,
            queries=query_results,
            total_execution_time=0.3,
            status=ExecutionStatus.COMPLETED,
        )

        aggregator.add_stream_result(stream_result)

        results = aggregator.get_aggregated_results()
        assert results["total_streams"] == 1
        assert results["total_queries"] == 2
        assert results["successful_queries"] == 2
        assert results["failed_queries"] == 0
        assert results["success_rate"] == 1.0

    def test_empty_results(self):
        """Test aggregation with no results."""
        aggregator = ResultAggregator()
        results = aggregator.get_aggregated_results()

        assert results["total_queries"] == 0
        assert results["successful_queries"] == 0
        assert results["failed_queries"] == 0
        assert results["success_rate"] == 0.0


class TestErrorHandler:
    """Test ErrorHandler class."""

    def test_retryable_error_detection(self):
        """Test detection of retryable errors - optimized version."""
        handler = ErrorHandler(max_retries=3)

        # Mock time.sleep to avoid actual delays in tests
        with patch("time.sleep"):
            # Test retryable errors
            retryable_errors = [
                Exception("Connection timeout"),
                Exception("Network error"),
                Exception("Temporary failure"),
                Exception("Database lock detected"),
            ]

            for error in retryable_errors:
                context = {"retry_count": 0}
                assert handler.handle_error(error, context)

    def test_non_retryable_error(self):
        """Test handling of non-retryable errors."""
        handler = ErrorHandler(max_retries=3)

        error = Exception("Syntax error in query")
        context = {"retry_count": 0}

        assert not handler.handle_error(error, context)

    def test_max_retries_exceeded(self):
        """Test that max retries are respected."""
        handler = ErrorHandler(max_retries=2)

        error = Exception("Connection timeout")
        context = {"retry_count": 2}  # Already at max retries

        assert not handler.handle_error(error, context)

    def test_delay_calculations(self):
        """Test different retry delay calculations."""
        handler = ErrorHandler(retry_policy=RetryPolicy.EXPONENTIAL_BACKOFF)

        # Test exponential backoff
        assert handler._calculate_delay(0) == 1.0
        assert handler._calculate_delay(1) == 2.0
        assert handler._calculate_delay(2) == 4.0
        assert handler._calculate_delay(10) == 60.0  # Capped at 60 seconds

        # Test fixed delay
        handler.retry_policy = RetryPolicy.FIXED_DELAY
        assert handler._calculate_delay(0) == 1.0
        assert handler._calculate_delay(5) == 1.0

        # Test linear backoff
        handler.retry_policy = RetryPolicy.LINEAR_BACKOFF
        assert handler._calculate_delay(0) == 1.0
        assert handler._calculate_delay(1) == 2.0
        assert handler._calculate_delay(2) == 3.0


class TestTransactionManager:
    """Test TransactionManager class."""

    def test_successful_transaction(self):
        """Test successful transaction execution."""
        connection = MockDatabaseConnection()
        manager = TransactionManager(connection)

        with manager.transaction():
            connection.execute("SELECT 1")

        assert "SELECT 1" in connection.queries_executed

    def test_transaction_rollback_on_error(self):
        """Test transaction rollback on error."""
        connection = MockDatabaseConnection()
        manager = TransactionManager(connection)

        with pytest.raises(Exception), manager.transaction():
            raise Exception("Test error")

    def test_nested_transaction_error(self):
        """Test that nested transactions raise error."""
        connection = MockDatabaseConnection()
        manager = TransactionManager(connection)

        with manager.transaction(), pytest.raises(RuntimeError, match="Transaction already active"):
            with manager.transaction():
                pass


class TestBenchmarkTestRunner:
    """Test BenchmarkTestRunner class."""

    def test_single_query_execution(self):
        """Test single query execution."""

        def connection_factory():
            return MockDatabaseConnection()

        runner = BenchmarkTestRunner(connection_factory)
        benchmark = MockBenchmark()
        query_executor = create_basic_query_executor(benchmark)

        result = runner.run_single_query_test(1, query_executor)

        assert result.query_id == 1
        assert result.status == ExecutionStatus.COMPLETED
        assert result.row_count == 2

    def test_sequential_execution(self):
        """Test sequential query execution."""

        def connection_factory():
            return MockDatabaseConnection()

        runner = BenchmarkTestRunner(connection_factory)
        benchmark = MockBenchmark()
        query_executor = create_basic_query_executor(benchmark)

        results = runner.run_sequential_test([1, 2, 3], query_executor)

        assert results["total_queries"] == 3
        assert results["successful_queries"] == 3
        assert results["failed_queries"] == 0

    def test_validation_test(self):
        """Test validation test execution."""

        def connection_factory():
            return MockDatabaseConnection()

        runner = BenchmarkTestRunner(connection_factory)
        benchmark = MockBenchmark()
        query_executor = create_basic_query_executor(benchmark)

        # validator that expects 2 rows
        expected_results = {1: {"row_count": 2}, 2: {"row_count": 2}}
        validator = create_validation_function(expected_results)

        results = runner.run_validation_test([1, 2], query_executor, validator)

        assert results["total_queries"] == 2
        assert results["validation"]["valid_results"] == 2
        assert results["validation"]["validation_rate"] == 1.0


class TestUtilityFunctions:
    """Test utility functions."""

    def test_create_tpc_stream_configs(self):
        """Test TPC stream configuration creation."""
        configs = create_tpc_stream_configs(
            num_streams=3,
            query_ids=[1, 2, 3, 4, 5],
            base_seed=42,
            permutation_mode="random",
        )

        assert len(configs) == 3
        for i, config in enumerate(configs):
            assert config.stream_id == i
            assert config.query_ids == [1, 2, 3, 4, 5]
            assert config.parameter_seed == 42 + i + 1000

    def test_create_basic_query_executor(self):
        """Test basic query executor creation."""
        benchmark = MockBenchmark()
        query_executor = create_basic_query_executor(benchmark)

        connection = MockDatabaseConnection()
        result = query_executor(1, connection, {})

        assert result.query_id == 1
        assert result.status == ExecutionStatus.COMPLETED
        assert result.row_count == 2

    def test_create_validation_function(self):
        """Test validation function creation."""
        expected_results = {1: {"row_count": 2}}
        validator = create_validation_function(expected_results)

        # Test valid result
        valid_result = QueryResult(
            query_id=1,
            status=ExecutionStatus.COMPLETED,
            results=[("a", "b"), ("c", "d")],
        )
        assert validator(valid_result)

        # Test invalid result
        invalid_result = QueryResult(query_id=1, status=ExecutionStatus.FAILED, error="Test error")
        assert not validator(invalid_result)


class TestStreamExecutor:
    """Test StreamExecutor class."""

    def test_stream_execution(self):
        """Test basic stream execution."""

        def connection_factory():
            return MockDatabaseConnection()

        executor = StreamExecutor(connection_factory, max_concurrent_streams=2)

        # stream configs
        configs = create_tpc_stream_configs(num_streams=2, query_ids=[1, 2, 3], base_seed=42)

        # query executor
        benchmark = MockBenchmark()
        query_executor = create_basic_query_executor(benchmark)

        # Execute streams
        results = executor.execute_streams(configs, query_executor)

        assert results["total_streams"] == 2
        assert results["total_queries"] == 6  # 2 streams * 3 queries each
        assert results["successful_queries"] == 6
        assert results["failed_queries"] == 0

    def test_stream_execution_with_failures(self):
        """Test stream execution with query failures."""

        def connection_factory():
            return MockDatabaseConnection(fail_on_query="table2")

        executor = StreamExecutor(connection_factory, max_concurrent_streams=1)

        # stream config
        configs = create_tpc_stream_configs(num_streams=1, query_ids=[1, 2, 3], base_seed=42)

        # query executor
        benchmark = MockBenchmark()
        query_executor = create_basic_query_executor(benchmark)

        # Execute streams
        results = executor.execute_streams(configs, query_executor)

        assert results["total_streams"] == 1
        assert results["total_queries"] == 3
        assert results["successful_queries"] == 2  # Query 2 should fail
        assert results["failed_queries"] == 1


# Integration tests
class TestIntegration:
    """Integration tests for TPC patterns."""

    def test_end_to_end_execution(self):
        """Test end-to-end execution flow."""

        def connection_factory():
            return MockDatabaseConnection()

        # benchmark
        benchmark = MockBenchmark(
            {
                1: "SELECT * FROM customers",
                2: "SELECT * FROM orders",
                3: "SELECT * FROM products",
            }
        )

        # query executor
        query_executor = create_basic_query_executor(benchmark)

        # stream configurations
        stream_configs = create_tpc_stream_configs(
            num_streams=2,
            query_ids=[1, 2, 3],
            base_seed=42,
            permutation_mode="tpc_standard",
        )

        # Execute streams
        executor = StreamExecutor(connection_factory, max_concurrent_streams=2)
        results = executor.execute_streams(stream_configs, query_executor)

        # Verify results
        assert results["total_streams"] == 2
        assert results["total_queries"] == 6
        assert results["success_rate"] == 1.0
        assert len(results["query_results"]) == 6

        # Verify stream statistics
        assert len(results["stream_statistics"]) == 2
        for _stream_id, stats in results["stream_statistics"].items():
            assert stats["total_queries"] == 3
            assert stats["successful_queries"] == 3
            assert stats["failed_queries"] == 0
            assert stats["status"] == "completed"
