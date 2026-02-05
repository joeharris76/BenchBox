"""Comprehensive tests for the BaseBenchmark class.

Tests BaseBenchmark class execution methods, error handling,
timing functionality, result formatting, and database operations.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import logging
from pathlib import Path
from typing import Any, Optional, Union
from unittest.mock import Mock, patch

import pytest

from benchbox.base import BaseBenchmark
from benchbox.core.connection import DatabaseConnection, DatabaseError

# Mark all tests in this file as unit tests
pytestmark = [pytest.mark.unit, pytest.mark.fast]


class MockBaseBenchmark(BaseBenchmark):
    """A concrete implementation of BaseBenchmark for testing."""

    def __init__(self, scale_factor: float = 1.0, **kwargs: Any) -> None:
        super().__init__(scale_factor=scale_factor, **kwargs)
        self._queries: dict[Union[int, str], str] = {
            1: "SELECT * FROM table1",
            2: "SELECT COUNT(*) FROM table2",
            "complex": "SELECT t1.id, t2.name FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id",
            "error_query": "SELECT * FROM nonexistent_table",
            "slow_query": "SELECT * FROM table1 WHERE id IN (SELECT id FROM table2)",
        }
        self._data_generated = False
        self._load_data_called = False

    def generate_data(self) -> list[Union[str, Path]]:
        """Generate mock data files."""
        self._data_generated = True
        return [Path("table1.csv"), Path("table2.csv"), Path("table3.csv")]

    def get_queries(self) -> dict[int | str, str]:
        """Get all queries."""
        return self._queries

    def get_query(self, query_id: Union[int, str], *, params: Optional[dict[str, Any]] = None) -> str:
        """Get a specific query."""
        if query_id not in self._queries:
            raise ValueError(f"Invalid query ID: {query_id}")

        query = self._queries[query_id]

        # Simple parameter substitution for testing
        if params:
            for key, value in params.items():
                query = query.replace(f"${key}", str(value))

        return query

    def _load_data(self, connection: DatabaseConnection) -> None:
        """Load data into the database."""
        self._load_data_called = True
        # Simulate loading data by executing CREATE TABLE statements
        connection.execute("CREATE TABLE IF NOT EXISTS table1 (id INT, name VARCHAR(50))")
        connection.execute("CREATE TABLE IF NOT EXISTS table2 (id INT, value DECIMAL(10,2))")
        connection.execute("INSERT INTO table1 VALUES (1, 'test1'), (2, 'test2')")
        connection.execute("INSERT INTO table2 VALUES (1, 10.5), (2, 20.5)")


class FailingBenchmark(BaseBenchmark):
    """A benchmark that fails in various ways for testing error handling."""

    def __init__(self, fail_mode: str = "generate_data", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.fail_mode = fail_mode

    def generate_data(self) -> list[Union[str, Path]]:
        if self.fail_mode == "generate_data":
            raise RuntimeError("Data generation failed")
        return [Path("test.csv")]

    def get_queries(self) -> dict[str, str]:
        if self.fail_mode == "get_queries":
            raise RuntimeError("Query retrieval failed")
        return {"1": "SELECT 1"}

    def get_query(self, query_id: Union[int, str], *, params: Optional[dict[str, Any]] = None) -> str:
        if self.fail_mode == "get_query":
            raise RuntimeError("Query retrieval failed")
        return "SELECT 1"

    def _load_data(self, connection: DatabaseConnection) -> None:
        if self.fail_mode == "load_data":
            raise RuntimeError("Data loading failed")


class MockBaseBenchmarkInitialization:
    """Test benchmark initialization and configuration."""

    def test_basic_initialization(self):
        """Test basic benchmark initialization."""
        benchmark = MockBaseBenchmark(scale_factor=1.0)
        assert benchmark.scale_factor == 1.0
        assert benchmark.output_dir == Path.cwd() / "benchmark_runs" / "datagen" / "testbasebenchmark_sf1"
        assert not benchmark._data_generated
        assert not benchmark._load_data_called

    def test_custom_scale_factor(self):
        """Test initialization with custom scale factor."""
        benchmark = MockBaseBenchmark(scale_factor=2)
        assert benchmark.scale_factor == 2

    def test_custom_output_dir(self):
        """Test initialization with custom output directory."""
        custom_dir = Path("/tmp/test_output")
        benchmark = MockBaseBenchmark(output_dir=custom_dir)
        assert benchmark.output_dir == custom_dir

    def test_string_output_dir(self):
        """Test initialization with string output directory."""
        benchmark = MockBaseBenchmark(output_dir="/tmp/test_output")
        assert benchmark.output_dir == Path("/tmp/test_output")

    def test_custom_kwargs(self):
        """Test initialization with custom keyword arguments."""
        benchmark = MockBaseBenchmark(scale_factor=1.0, custom_param="test_value", another_param=42)
        assert benchmark.custom_param == "test_value"
        assert benchmark.another_param == 42


class MockBaseBenchmarkAbstractMethods:
    """Test abstract method requirements and implementations."""

    def test_cannot_instantiate_abstract_class(self):
        """Test that BaseBenchmark cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseBenchmark()

    def test_concrete_class_implements_required_methods(self):
        """Test that concrete class implements all required abstract methods."""
        benchmark = MockBaseBenchmark()

        # Test that all abstract methods are implemented
        assert hasattr(benchmark, "generate_data")
        assert hasattr(benchmark, "get_queries")
        assert hasattr(benchmark, "get_query")
        assert callable(benchmark.generate_data)
        assert callable(benchmark.get_queries)
        assert callable(benchmark.get_query)

    def test_unimplemented_load_data_raises_error(self):
        """Test that _load_data raises NotImplementedError if not implemented."""

        class UnimplementedBenchmark(BaseBenchmark):
            def generate_data(self) -> list[Union[str, Path]]:
                return []

            def get_queries(self) -> dict[str, str]:
                return {}

            def get_query(
                self,
                query_id: Union[int, str],
                *,
                params: Optional[dict[str, Any]] = None,
            ) -> str:
                return "SELECT 1"

        benchmark = UnimplementedBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)

        with pytest.raises(NotImplementedError, match="must implement _load_data"):
            benchmark._load_data(mock_connection)


class MockBaseBenchmarkDatabaseSetup:
    """Test database setup functionality."""

    def test_setup_database_success(self):
        """Test successful database setup."""
        benchmark = MockBaseBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)

        with patch("time.time", side_effect=[0.0, 1.0, 2.0]):  # start, after generate, end
            benchmark.setup_database(mock_connection)

        assert benchmark._data_generated
        assert benchmark._load_data_called
        assert mock_connection.execute.call_count >= 2  # At least CREATE TABLE calls

    def test_setup_database_skips_data_generation_if_already_generated(self):
        """Test that setup skips data generation if already done."""
        benchmark = MockBaseBenchmark()
        benchmark._data_generated = True
        mock_connection = Mock(spec=DatabaseConnection)

        with patch.object(benchmark, "generate_data") as mock_generate:
            benchmark.setup_database(mock_connection)
            mock_generate.assert_not_called()

        assert benchmark._load_data_called

    def test_setup_database_handles_data_generation_failure(self):
        """Test setup handles data generation failure."""
        benchmark = FailingBenchmark(fail_mode="generate_data")
        mock_connection = Mock(spec=DatabaseConnection)

        with pytest.raises(RuntimeError, match="Data generation failed"):
            benchmark.setup_database(mock_connection)

    def test_setup_database_handles_data_loading_failure(self):
        """Test setup handles data loading failure."""
        benchmark = FailingBenchmark(fail_mode="load_data")
        mock_connection = Mock(spec=DatabaseConnection)

        with pytest.raises(RuntimeError, match="Data loading failed"):
            benchmark.setup_database(mock_connection)

    def test_setup_database_logs_timing_info(self, caplog):
        """Test that setup logs timing information."""
        benchmark = MockBaseBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)

        # Capture logs from the benchbox.base logger specifically
        with caplog.at_level(logging.INFO, logger="benchbox.base"):
            benchmark.setup_database(mock_connection)

        assert "Setting up database schema and loading data" in caplog.text
        assert "Database setup completed" in caplog.text


class MockBaseBenchmarkQueryExecution:
    """Test query execution functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.benchmark = MockBaseBenchmark()
        self.mock_connection = Mock(spec=DatabaseConnection)
        self.mock_cursor = Mock()
        self.mock_connection.execute.return_value = self.mock_cursor
        self.mock_connection.fetchall.return_value = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"},
        ]

    def test_run_query_success_without_results(self):
        """Test successful query execution without fetching results."""
        with patch("time.time", side_effect=[0.0, 1.5]):  # start, end
            result = self.benchmark.run_query(1, self.mock_connection)

        assert result["query_id"] == 1
        assert result["execution_time"] == 1.5
        assert result["query_text"] == "SELECT * FROM table1"
        assert result["results"] is None
        assert result["row_count"] == 0

        self.mock_connection.execute.assert_called_once_with("SELECT * FROM table1")
        self.mock_connection.fetchall.assert_not_called()

    def test_run_query_success_with_results(self):
        """Test successful query execution with fetching results."""
        with patch("time.time", side_effect=[0.0, 1.5]):  # start, end
            result = self.benchmark.run_query(1, self.mock_connection, fetch_results=True)

        assert result["query_id"] == 1
        assert result["execution_time"] == 1.5
        assert result["query_text"] == "SELECT * FROM table1"
        assert result["results"] == [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"},
        ]
        assert result["row_count"] == 2

        self.mock_connection.execute.assert_called_once_with("SELECT * FROM table1")
        self.mock_connection.fetchall.assert_called_once()

    def test_run_query_with_parameters(self):
        """Test query execution with parameters."""
        params = {"limit": "10", "filter": "name"}
        query_with_params = "SELECT * FROM table1 WHERE $filter IS NOT NULL LIMIT $limit"

        # Mock the query to include parameters
        self.benchmark._queries[1] = query_with_params

        result = self.benchmark.run_query(1, self.mock_connection, params=params)

        expected_query = "SELECT * FROM table1 WHERE name IS NOT NULL LIMIT 10"
        assert result["query_text"] == expected_query
        self.mock_connection.execute.assert_called_once_with(expected_query)

    def test_run_query_invalid_query_id(self):
        """Test query execution with invalid query ID."""
        with pytest.raises(ValueError, match="Invalid query ID"):
            self.benchmark.run_query(999, self.mock_connection)

    def test_run_query_database_error(self):
        """Test query execution with database error."""
        self.mock_connection.execute.side_effect = DatabaseError("Database connection failed")

        with pytest.raises(DatabaseError, match="Database connection failed"):
            self.benchmark.run_query(1, self.mock_connection)

    def test_run_query_logs_execution_info(self, caplog):
        """Test that query execution logs appropriate information."""
        with caplog.at_level(logging.INFO, logger="benchbox.base"):
            self.benchmark.run_query(1, self.mock_connection)

        assert "Query 1 completed" in caplog.text

    def test_run_query_logs_results_info_when_fetched(self, caplog):
        """Test that query execution logs result information when fetched."""
        with caplog.at_level(logging.DEBUG, logger="benchbox.base"):
            self.benchmark.run_query(1, self.mock_connection, fetch_results=True)

        assert "Query 1 returned 2 rows" in caplog.text

    def test_run_query_handles_empty_results(self):
        """Test query execution with empty results."""
        self.mock_connection.fetchall.return_value = []

        result = self.benchmark.run_query(1, self.mock_connection, fetch_results=True)

        assert result["results"] == []
        assert result["row_count"] == 0

    def test_run_query_handles_none_results(self):
        """Test query execution with None results."""
        self.mock_connection.fetchall.return_value = None

        result = self.benchmark.run_query(1, self.mock_connection, fetch_results=True)

        assert result["results"] is None
        assert result["row_count"] == 0


class MockBaseBenchmarkBenchmarkExecution:
    """Test full benchmark execution functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.benchmark = MockBaseBenchmark()
        self.mock_connection = Mock(spec=DatabaseConnection)
        self.mock_cursor = Mock()
        self.mock_connection.execute.return_value = self.mock_cursor
        self.mock_connection.fetchall.return_value = [{"result": "success"}]

    def test_run_benchmark_success_with_setup(self):
        """Test successful benchmark execution with database setup."""
        with patch("time.time", side_effect=[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]):
            # benchmark_start, setup_start, setup_end, query1_start, query1_end, query2_start, query2_end, final_time
            result = self.benchmark.run_benchmark(self.mock_connection, query_ids=[1, 2], setup_database=True)

        assert result["benchmark_name"] == "MockBaseBenchmark"
        assert result["total_queries"] == 2
        assert result["successful_queries"] == 2
        assert result["failed_queries"] == 0
        assert result["setup_time"] == 3.0  # setup_end - setup_start = 4.0 - 1.0 = 3.0
        assert len(result["query_results"]) == 2
        assert "average_query_time" in result
        assert "min_query_time" in result
        assert "max_query_time" in result

    def test_run_benchmark_success_without_setup(self):
        """Test successful benchmark execution without database setup."""
        with patch("time.time", side_effect=[0.0, 1.0, 2.0, 3.0, 4.0, 5.0]):
            result = self.benchmark.run_benchmark(self.mock_connection, query_ids=[1], setup_database=False)

        assert result["setup_time"] == 0.0
        assert result["successful_queries"] == 1

    def test_run_benchmark_all_queries_default(self):
        """Test benchmark execution with all queries (default)."""
        with patch("time.time", side_effect=list(range(20))):  # Plenty of timestamps
            result = self.benchmark.run_benchmark(self.mock_connection, setup_database=False)

        # Should run all queries in the benchmark
        assert result["total_queries"] == len(self.benchmark.get_queries())
        assert result["successful_queries"] == len(self.benchmark.get_queries())

    def test_run_benchmark_partial_failure(self):
        """Test benchmark execution with some query failures."""

        # Mock connection to fail on specific queries
        def mock_execute(query):
            if "nonexistent_table" in query:
                raise DatabaseError("Table does not exist")
            return self.mock_cursor

        self.mock_connection.execute.side_effect = mock_execute

        with patch("time.time", side_effect=list(range(20))):
            result = self.benchmark.run_benchmark(
                self.mock_connection, query_ids=[1, "error_query"], setup_database=False
            )

        assert result["total_queries"] == 2
        assert result["successful_queries"] == 1
        assert result["failed_queries"] == 1

        # Check that failed query has error info
        failed_query = next(r for r in result["query_results"] if "error" in r)
        assert failed_query["query_id"] == "error_query"
        assert "Table does not exist" in failed_query["error"]

    def test_run_benchmark_setup_failure(self):
        """Test benchmark execution with setup failure."""
        benchmark = FailingBenchmark(fail_mode="load_data")

        with pytest.raises(RuntimeError, match="Data loading failed"):
            benchmark.run_benchmark(self.mock_connection, query_ids=[1], setup_database=True)

    def test_run_benchmark_with_fetch_results(self):
        """Test benchmark execution with result fetching."""
        with patch("time.time", side_effect=list(range(10))):
            result = self.benchmark.run_benchmark(
                self.mock_connection,
                query_ids=[1],
                fetch_results=True,
                setup_database=False,
            )

        query_result = result["query_results"][0]
        assert query_result["results"] == [{"result": "success"}]
        assert query_result["row_count"] == 1

    def test_run_benchmark_logs_progress(self, caplog):
        """Test that benchmark execution logs progress."""
        with caplog.at_level(logging.INFO, logger="benchbox.base"):
            self.benchmark.run_benchmark(self.mock_connection, query_ids=[1], setup_database=False)

        assert "Running benchmark with 1 queries" in caplog.text
        assert "Benchmark completed" in caplog.text

    def test_run_benchmark_calculates_timing_statistics(self):
        """Test that benchmark calculates timing statistics correctly."""
        # Mock query execution times: each query takes 1.0s
        # benchmark_start, q1_start, q1_end, q2_start, q2_end, q3_start, q3_end, benchmark_end
        execution_times = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]

        with patch("time.time", side_effect=execution_times):
            result = self.benchmark.run_benchmark(
                self.mock_connection, query_ids=[1, 2, "complex"], setup_database=False
            )

        # Each query took 1.0s (end - start for each query)
        assert result["average_query_time"] == 1.0
        assert result["min_query_time"] == 1.0
        assert result["max_query_time"] == 1.0

    def test_run_benchmark_handles_empty_query_list(self):
        """Test benchmark execution with empty query list."""
        with patch("time.time", side_effect=[0.0, 1.0]):
            result = self.benchmark.run_benchmark(self.mock_connection, query_ids=[], setup_database=False)

        assert result["total_queries"] == 0
        assert result["successful_queries"] == 0
        assert result["failed_queries"] == 0
        assert result["query_results"] == []
        assert result["average_query_time"] == 0.0


class MockBaseBenchmarkResultFormatting:
    """Test result formatting functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.benchmark = MockBaseBenchmark()
        self.sample_result = {
            "benchmark_name": "TestBenchmark",
            "total_queries": 3,
            "successful_queries": 2,
            "failed_queries": 1,
            "setup_time": 1.5,
            "total_execution_time": 5.0,
            "average_query_time": 2.0,
            "min_query_time": 1.0,
            "max_query_time": 3.0,
            "query_results": [
                {"query_id": 1, "execution_time": 1.0, "row_count": 10},
                {"query_id": 2, "execution_time": 3.0, "row_count": 5},
                {"query_id": 3, "error": "Database connection failed"},
            ],
        }

    def test_format_results_basic_structure(self):
        """Test basic structure of formatted results."""
        formatted = self.benchmark.format_results(self.sample_result)

        assert "Benchmark: TestBenchmark" in formatted
        assert "Total Queries: 3" in formatted
        assert "Successful: 2" in formatted
        assert "Failed: 1" in formatted
        assert "Setup Time: 1.50s" in formatted
        assert "Total Execution Time: 5.00s" in formatted
        assert "Average Query Time: 2.000s" in formatted
        assert "Min Query Time: 1.000s" in formatted
        assert "Max Query Time: 3.000s" in formatted

    def test_format_results_query_details(self):
        """Test formatting of individual query details."""
        formatted = self.benchmark.format_results(self.sample_result)

        assert "Query Details:" in formatted
        assert "Query 1: 1.000s (10 rows)" in formatted
        assert "Query 2: 3.000s (5 rows)" in formatted
        assert "Query 3: FAILED - Database connection failed" in formatted

    def test_format_results_no_setup_time(self):
        """Test formatting when setup time is zero."""
        result = self.sample_result.copy()
        result["setup_time"] = 0.0

        formatted = self.benchmark.format_results(result)

        assert "Setup Time:" not in formatted

    def test_format_results_no_failures(self):
        """Test formatting when all queries succeed."""
        result = {
            "benchmark_name": "TestBenchmark",
            "total_queries": 2,
            "successful_queries": 2,
            "failed_queries": 0,
            "setup_time": 0.0,
            "total_execution_time": 3.0,
            "average_query_time": 1.5,
            "min_query_time": 1.0,
            "max_query_time": 2.0,
            "query_results": [
                {"query_id": 1, "execution_time": 1.0, "row_count": 10},
                {"query_id": 2, "execution_time": 2.0, "row_count": 5},
            ],
        }

        formatted = self.benchmark.format_results(result)

        assert "Failed: 0" in formatted
        assert "FAILED" not in formatted

    def test_format_time_utility(self):
        """Test the _format_time utility method."""
        # Test milliseconds
        assert self.benchmark._format_time(0.05) == "50.0ms"
        assert self.benchmark._format_time(0.5) == "500.0ms"

        # Test seconds
        assert self.benchmark._format_time(1.0) == "1.00s"
        assert self.benchmark._format_time(30.5) == "30.50s"

        # Test minutes
        assert self.benchmark._format_time(60.0) == "1m 0.0s"
        assert self.benchmark._format_time(125.5) == "2m 5.5s"


class MockBaseBenchmarkQueryTranslation:
    """Test query translation functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.benchmark = MockBaseBenchmark()

    def test_translate_query_success(self):
        """Test successful query translation."""
        with patch("benchbox.base.sqlglot") as mock_sqlglot:
            with patch("benchbox.utils.dialect_utils.normalize_dialect_for_sqlglot") as mock_normalize:
                mock_normalize.return_value = "mysql"
                mock_sqlglot.transpile.return_value = ["SELECT * FROM table1"]

                result = self.benchmark.translate_query(1, "mysql")

                assert result == "SELECT * FROM table1"
                mock_normalize.assert_called_once_with("mysql")
                mock_sqlglot.transpile.assert_called_once_with(
                    "SELECT * FROM table1", read="postgres", write="mysql", identify=True
                )

    def test_translate_query_invalid_query_id(self):
        """Test translation with invalid query ID."""
        with pytest.raises(ValueError, match="Invalid query ID"):
            self.benchmark.translate_query(999, "mysql")

    def test_translate_query_no_sqlglot(self):
        """Test translation when sqlglot is not available."""
        with patch("benchbox.base.sqlglot", None), pytest.raises(ImportError, match="sqlglot is required"):
            self.benchmark.translate_query(1, "mysql")

    def test_translate_query_unsupported_dialect(self):
        """Test translation with unsupported dialect."""
        with patch("benchbox.base.sqlglot") as mock_sqlglot:
            mock_sqlglot.transpile.side_effect = ValueError("Unsupported dialect")

            with pytest.raises(ValueError, match="Error translating to dialect"):
                self.benchmark.translate_query(1, "unsupported")

    def test_translate_query_with_parameters(self):
        """Test translation with query parameters."""
        with patch("benchbox.base.sqlglot") as mock_sqlglot:
            mock_sqlglot.transpile.return_value = ["SELECT * FROM table1 WHERE name = 'test'"]

            result = self.benchmark.translate_query(1, "mysql")

            # Should get the query first, then translate it
            mock_sqlglot.transpile.assert_called_once()
            assert result == "SELECT * FROM table1 WHERE name = 'test'"


class MockBaseBenchmarkBackwardCompatibility:
    """Test backward compatibility with existing benchmarks."""

    def test_existing_benchmark_pattern_still_works(self):
        """Test existing benchmark patterns work."""
        # Test the base class with existing code
        benchmark = MockBaseBenchmark()

        # Test basic functionality that existing benchmarks rely on
        assert benchmark.scale_factor == 1.0
        assert benchmark.output_dir == Path.cwd() / "benchmark_runs" / "datagen" / "testbasebenchmark_sf1"

        # Test query methods
        queries = benchmark.get_queries()
        assert isinstance(queries, dict)
        assert len(queries) > 0

        query = benchmark.get_query(1)
        assert isinstance(query, str)
        assert len(query) > 0

    def test_custom_attributes_preserved(self):
        """Test that custom attributes are preserved."""
        benchmark = MockBaseBenchmark(custom_param="test_value", another_param=42)

        assert benchmark.custom_param == "test_value"
        assert benchmark.another_param == 42

    def test_subclass_overrides_work(self):
        """Test that subclass method overrides work properly."""

        class CustomBenchmark(MockBaseBenchmark):
            def get_queries(self) -> dict[str, str]:
                return {"custom": "SELECT 'custom' as result"}

        benchmark = CustomBenchmark()
        queries = benchmark.get_queries()

        assert queries == {"custom": "SELECT 'custom' as result"}

    def test_load_data_override_works(self):
        """Test that _load_data can be overridden properly."""

        class CustomLoadBenchmark(MockBaseBenchmark):
            def _load_data(self, connection: DatabaseConnection) -> None:
                connection.execute("CREATE TABLE custom_table (id INT)")
                self.custom_load_called = True

        benchmark = CustomLoadBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)

        benchmark._load_data(mock_connection)

        assert benchmark.custom_load_called
        mock_connection.execute.assert_called_with("CREATE TABLE custom_table (id INT)")


class MockBaseBenchmarkErrorHandling:
    """Test comprehensive error handling scenarios."""

    def test_database_connection_error(self):
        """Test handling of database connection errors."""
        benchmark = MockBaseBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)
        mock_connection.execute.side_effect = DatabaseError("Connection failed")

        with pytest.raises(DatabaseError, match="Connection failed"):
            benchmark.run_query(1, mock_connection)

    def test_query_execution_timeout(self):
        """Test handling of query execution timeouts."""
        benchmark = MockBaseBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)
        mock_connection.execute.side_effect = TimeoutError("Query timed out")

        with pytest.raises(TimeoutError, match="Query timed out"):
            benchmark.run_query(1, mock_connection)

    def test_result_fetching_error(self):
        """Test handling of result fetching errors."""
        benchmark = MockBaseBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)
        mock_cursor = Mock()
        mock_connection.execute.return_value = mock_cursor
        mock_connection.fetchall.side_effect = DatabaseError("Failed to fetch results")

        with pytest.raises(DatabaseError, match="Failed to fetch results"):
            benchmark.run_query(1, mock_connection, fetch_results=True)

    def test_setup_logging_error_handling(self, caplog):
        """Test error handling in setup with logging."""
        benchmark = FailingBenchmark(fail_mode="generate_data")
        mock_connection = Mock(spec=DatabaseConnection)

        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError):
            benchmark.setup_database(mock_connection)

        assert "Database setup failed" in caplog.text

    def test_query_execution_logging_error_handling(self, caplog):
        """Test error handling in query execution with logging."""
        benchmark = MockBaseBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)
        mock_connection.execute.side_effect = DatabaseError("Query failed")

        with caplog.at_level(logging.ERROR), pytest.raises(DatabaseError):
            benchmark.run_query(1, mock_connection)

        assert "Query 1 execution failed" in caplog.text


class MockBaseBenchmarkIntegration:
    """Integration tests for the enhanced BaseBenchmark class."""

    def test_full_benchmark_workflow(self):
        """Test complete benchmark workflow from setup to results."""
        benchmark = MockBaseBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)
        mock_cursor = Mock()
        mock_connection.execute.return_value = mock_cursor
        mock_connection.fetchall.return_value = [{"id": 1, "value": "test"}]

        # Run complete benchmark
        with patch("time.time", side_effect=list(range(20))):
            result = benchmark.run_benchmark(
                mock_connection,
                query_ids=[1, 2],
                fetch_results=True,
                setup_database=True,
            )

        # Verify complete workflow
        assert benchmark._data_generated
        assert benchmark._load_data_called
        assert result["successful_queries"] == 2
        assert result["failed_queries"] == 0
        assert len(result["query_results"]) == 2

        # Verify results are formatted correctly
        formatted = benchmark.format_results(result)
        assert "Benchmark: MockBaseBenchmark" in formatted
        assert "Successful: 2" in formatted
        assert "Failed: 0" in formatted

    def test_benchmark_with_mixed_success_failure(self):
        """Test benchmark with both successful and failed queries."""
        benchmark = MockBaseBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)
        mock_cursor = Mock()

        # Configure mock to fail on specific query
        def mock_execute(query):
            if "nonexistent_table" in query:
                raise DatabaseError("Table does not exist")
            return mock_cursor

        mock_connection.execute.side_effect = mock_execute
        mock_connection.fetchall.return_value = [{"result": "success"}]

        with patch("time.time", side_effect=list(range(20))):
            result = benchmark.run_benchmark(
                mock_connection,
                query_ids=[1, "error_query", 2],
                fetch_results=True,
                setup_database=True,
            )

        assert result["total_queries"] == 3
        assert result["successful_queries"] == 2
        assert result["failed_queries"] == 1

        # Check that error information is preserved
        error_result = next(r for r in result["query_results"] if "error" in r)
        assert error_result["query_id"] == "error_query"
        assert "Table does not exist" in error_result["error"]

    def test_benchmark_performance_metrics(self):
        """Test that performance metrics are calculated correctly."""
        benchmark = MockBaseBenchmark()
        mock_connection = Mock(spec=DatabaseConnection)
        mock_cursor = Mock()
        mock_connection.execute.return_value = mock_cursor
        mock_connection.fetchall.return_value = []

        # Mock specific execution times: each query takes 1.0s
        query_times = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]  # 1s each

        with patch("time.time", side_effect=query_times):
            result = benchmark.run_benchmark(mock_connection, query_ids=[1, 2, "complex"], setup_database=False)

        # Verify timing calculations
        assert result["average_query_time"] == 1.0
        assert result["min_query_time"] == 1.0
        assert result["max_query_time"] == 1.0
        assert result["total_execution_time"] == 7.0


# Test markers for pytest
pytestmark = [pytest.mark.unit, pytest.mark.fast]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
