"""Unit tests for Write Primitives benchmark execution.

Tests for:
- WritePrimitivesBenchmark initialization
- OperationResult dataclass
- SQL identifier quoting and placeholder replacement
- Operation management methods
- Benchmark information retrieval

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from benchbox.core.write_primitives.benchmark import OperationResult, WritePrimitivesBenchmark

pytestmark = pytest.mark.fast


class TestOperationResult:
    """Tests for OperationResult dataclass."""

    def test_operation_result_creation(self):
        """Test creating an OperationResult instance."""
        result = OperationResult(
            operation_id="INSERT_001",
            success=True,
            write_duration_ms=50.5,
            rows_affected=100,
            validation_duration_ms=10.2,
            validation_passed=True,
            validation_results=[{"query_id": "V1", "passed": True}],
            cleanup_duration_ms=5.0,
            cleanup_success=True,
        )

        assert result.operation_id == "INSERT_001"
        assert result.success is True
        assert result.write_duration_ms == 50.5
        assert result.rows_affected == 100
        assert result.validation_passed is True
        assert result.cleanup_success is True
        assert result.error is None
        assert result.cleanup_warning is None

    def test_operation_result_with_error(self):
        """Test creating an OperationResult with error."""
        result = OperationResult(
            operation_id="FAILED_OP",
            success=False,
            write_duration_ms=0.0,
            rows_affected=0,
            validation_duration_ms=0.0,
            validation_passed=False,
            validation_results=[],
            cleanup_duration_ms=0.0,
            cleanup_success=False,
            error="Database error: connection lost",
            cleanup_warning="Cleanup failed",
        )

        assert result.success is False
        assert result.error == "Database error: connection lost"
        assert result.cleanup_warning == "Cleanup failed"


class TestWritePrimitivesBenchmarkInit:
    """Tests for WritePrimitivesBenchmark initialization."""

    def test_default_initialization(self, tmp_path):
        """Test benchmark initializes with defaults."""
        with patch("benchbox.core.write_primitives.benchmark.get_benchmark_runs_datagen_path") as mock_path:
            mock_path.return_value = tmp_path
            wp_benchmark = WritePrimitivesBenchmark()

            assert wp_benchmark._name == "Write Primitives Benchmark"
            assert wp_benchmark._version == "1.0"
            assert wp_benchmark.scale_factor == 1.0
            assert wp_benchmark.tables == {}

    def test_custom_scale_factor(self, tmp_path):
        """Test benchmark with custom scale factor."""
        with patch("benchbox.core.write_primitives.benchmark.get_benchmark_runs_datagen_path") as mock_path:
            mock_path.return_value = tmp_path
            wp_benchmark = WritePrimitivesBenchmark(scale_factor=0.1)

            assert wp_benchmark.scale_factor == 0.1

    def test_custom_output_dir(self, tmp_path):
        """Test benchmark with custom output directory."""
        wp_benchmark = WritePrimitivesBenchmark(output_dir=tmp_path)

        # output_dir property should return a path-like object
        assert str(wp_benchmark.output_dir) == str(tmp_path)

    def test_data_source_benchmark(self, tmp_path):
        """Test get_data_source_benchmark returns tpch."""
        wp_benchmark = WritePrimitivesBenchmark(output_dir=tmp_path)
        assert wp_benchmark.get_data_source_benchmark() == "tpch"


class TestQuoteIdentifier:
    """Tests for SQL identifier quoting."""

    @pytest.fixture
    def wp_benchmark(self, tmp_path):
        """Create a benchmark instance for testing."""
        return WritePrimitivesBenchmark(output_dir=tmp_path)

    def test_valid_identifier(self, wp_benchmark):
        """Test quoting valid identifiers."""
        assert wp_benchmark._quote_identifier("table_name") == '"table_name"'
        assert wp_benchmark._quote_identifier("Orders") == '"Orders"'
        assert wp_benchmark._quote_identifier("_private") == '"_private"'

    def test_identifier_with_numbers(self, wp_benchmark):
        """Test quoting identifiers with numbers."""
        assert wp_benchmark._quote_identifier("table123") == '"table123"'
        assert wp_benchmark._quote_identifier("t1") == '"t1"'

    def test_invalid_identifier_with_spaces(self, wp_benchmark):
        """Test that identifiers with spaces raise ValueError."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            wp_benchmark._quote_identifier("table name")

    def test_invalid_identifier_with_semicolon(self, wp_benchmark):
        """Test that identifiers with semicolons raise ValueError."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            wp_benchmark._quote_identifier("table;DROP TABLE users")

    def test_invalid_identifier_starting_with_number(self, wp_benchmark):
        """Test that identifiers starting with numbers raise ValueError."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            wp_benchmark._quote_identifier("123table")

    def test_invalid_identifier_with_special_chars(self, wp_benchmark):
        """Test that identifiers with special characters raise ValueError."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            wp_benchmark._quote_identifier("table-name")
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            wp_benchmark._quote_identifier("table.name")


class TestReplacePlaceholders:
    """Tests for placeholder replacement in SQL."""

    @pytest.fixture
    def wp_benchmark(self, tmp_path):
        """Create a benchmark instance for testing."""
        return WritePrimitivesBenchmark(output_dir=tmp_path)

    def test_file_path_placeholder(self, wp_benchmark):
        """Test replacing {file_path} placeholder."""
        sql = "COPY table FROM '{file_path}/data.csv'"
        result = wp_benchmark._replace_placeholders(sql)

        assert "{file_path}" not in result
        assert "write_primitives_auxiliary" in result

    def test_no_placeholder(self, wp_benchmark):
        """Test SQL without placeholders passes through unchanged."""
        sql = "SELECT * FROM orders"
        result = wp_benchmark._replace_placeholders(sql)
        assert result == sql


class TestTableExists:
    """Tests for table existence checking."""

    @pytest.fixture
    def wp_benchmark(self, tmp_path):
        """Create a benchmark instance for testing."""
        return WritePrimitivesBenchmark(output_dir=tmp_path)

    def test_table_exists_success(self, wp_benchmark):
        """Test that existing table returns True."""
        mock_conn = Mock()
        mock_conn.execute.return_value = None  # Query succeeds

        result = wp_benchmark._table_exists(mock_conn, "orders")
        assert result is True

    def test_table_does_not_exist(self, wp_benchmark):
        """Test that nonexistent table returns False."""
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Table 'test' does not exist")

        result = wp_benchmark._table_exists(mock_conn, "test")
        assert result is False

    def test_table_exists_invalid_name(self, wp_benchmark):
        """Test that invalid table name returns False."""
        mock_conn = Mock()

        result = wp_benchmark._table_exists(mock_conn, "invalid;DROP TABLE")
        assert result is False
        mock_conn.execute.assert_not_called()

    def test_table_exists_unexpected_error(self, wp_benchmark):
        """Test that unexpected errors return False."""
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Connection timeout")

        result = wp_benchmark._table_exists(mock_conn, "orders")
        assert result is False


class TestOperationManagement:
    """Tests for operation management methods."""

    @pytest.fixture
    def wp_benchmark(self, tmp_path):
        """Create a benchmark instance for testing."""
        return WritePrimitivesBenchmark(output_dir=tmp_path)

    def test_get_operation_categories(self, wp_benchmark):
        """Test getting operation categories."""
        categories = wp_benchmark.get_operation_categories()

        assert isinstance(categories, list)
        assert len(categories) > 0
        # Common categories
        assert "insert" in categories or "INSERT" in [c.upper() for c in categories]

    def test_get_all_operations(self, wp_benchmark):
        """Test getting all operations."""
        operations = wp_benchmark.get_all_operations()

        assert isinstance(operations, dict)
        assert len(operations) > 0

    def test_get_operation(self, wp_benchmark):
        """Test getting a specific operation."""
        operations = wp_benchmark.get_all_operations()
        if operations:
            first_op_id = next(iter(operations.keys()))
            operation = wp_benchmark.get_operation(first_op_id)
            assert operation is not None
            assert hasattr(operation, "write_sql")

    def test_get_operation_invalid_id(self, wp_benchmark):
        """Test getting operation with invalid ID raises error."""
        with pytest.raises((ValueError, KeyError)):
            wp_benchmark.get_operation("NONEXISTENT_OPERATION_12345")

    def test_get_queries(self, wp_benchmark):
        """Test getting all queries."""
        queries = wp_benchmark.get_queries()

        assert isinstance(queries, dict)
        # Each value should be SQL string
        for sql in queries.values():
            assert isinstance(sql, str)

    def test_get_queries_by_category(self, wp_benchmark):
        """Test getting queries by category."""
        categories = wp_benchmark.get_operation_categories()
        if categories:
            category = categories[0]
            queries = wp_benchmark.get_queries_by_category(category)
            assert isinstance(queries, dict)


class TestBenchmarkInfo:
    """Tests for benchmark information methods."""

    @pytest.fixture
    def wp_benchmark(self, tmp_path):
        """Create a benchmark instance for testing."""
        return WritePrimitivesBenchmark(output_dir=tmp_path)

    def test_get_benchmark_info(self, wp_benchmark):
        """Test getting benchmark information."""
        info = wp_benchmark.get_benchmark_info()

        assert isinstance(info, dict)
        assert "name" in info
        assert "version" in info
        assert "scale_factor" in info
        assert "total_operations" in info
        assert "categories" in info
        assert "data_source" in info
        assert info["data_source"] == "tpch"

    def test_get_schema(self, wp_benchmark):
        """Test getting schema definitions."""
        schema = wp_benchmark.get_schema()

        assert isinstance(schema, dict)
        assert len(schema) > 0

    def test_get_create_tables_sql(self, wp_benchmark):
        """Test getting CREATE TABLE SQL."""
        sql = wp_benchmark.get_create_tables_sql()

        assert isinstance(sql, str)
        assert "CREATE TABLE" in sql
        # Should include both TPC-H and staging tables
        assert "Write Primitives Staging Tables" in sql

    def test_get_create_tables_sql_with_tuning(self, wp_benchmark):
        """Test getting CREATE TABLE SQL with tuning config."""
        mock_tuning = Mock()
        mock_tuning.primary_keys = Mock(enabled=True)
        mock_tuning.foreign_keys = Mock(enabled=False)

        sql = wp_benchmark.get_create_tables_sql(tuning_config=mock_tuning)

        assert isinstance(sql, str)
        assert "CREATE TABLE" in sql

    def test_get_query(self, wp_benchmark):
        """Test getting a single query."""
        operations = wp_benchmark.get_all_operations()
        if operations:
            first_op_id = next(iter(operations.keys()))
            query = wp_benchmark.get_query(first_op_id)
            assert isinstance(query, str)


class TestExecuteOperation:
    """Tests for execute_operation method."""

    @pytest.fixture
    def wp_benchmark(self, tmp_path):
        """Create a benchmark instance for testing."""
        return WritePrimitivesBenchmark(output_dir=tmp_path)

    def test_execute_operation_no_connection(self, wp_benchmark):
        """Test that None connection raises ValueError."""
        with pytest.raises(ValueError, match="Connection is None"):
            wp_benchmark.execute_operation("INSERT_001", None)

    def test_execute_operation_invalid_connection(self, wp_benchmark):
        """Test that invalid connection type raises ValueError."""
        invalid_conn = "not a connection"
        with pytest.raises(ValueError, match="Invalid connection type"):
            wp_benchmark.execute_operation("INSERT_001", invalid_conn)


class TestRunBenchmark:
    """Tests for run_benchmark method."""

    @pytest.fixture
    def wp_benchmark(self, tmp_path):
        """Create a benchmark instance for testing."""
        return WritePrimitivesBenchmark(output_dir=tmp_path)

    def test_run_benchmark_with_mock(self, wp_benchmark):
        """Test run_benchmark with mocked execute_operation."""
        mock_conn = Mock()
        mock_conn.execute.return_value = Mock(rowcount=10)

        # Get a real operation ID from the benchmark
        operations = wp_benchmark.get_all_operations()
        real_op_id = next(iter(operations.keys()))

        # Mock the staging tables as already set up
        with patch.object(wp_benchmark, "is_setup", return_value=True):
            with patch.object(wp_benchmark, "execute_operation") as mock_execute:
                # Create a successful result
                mock_execute.return_value = OperationResult(
                    operation_id=real_op_id,
                    success=True,
                    write_duration_ms=10.0,
                    rows_affected=10,
                    validation_duration_ms=5.0,
                    validation_passed=True,
                    validation_results=[],
                    cleanup_duration_ms=2.0,
                    cleanup_success=True,
                )

                results = wp_benchmark.run_benchmark(mock_conn, operation_ids=[real_op_id])

                assert len(results) == 1
                assert results[0].success is True

    def test_run_benchmark_by_category(self, wp_benchmark):
        """Test run_benchmark filtered by category."""
        mock_conn = Mock()
        mock_conn.execute.return_value = Mock(rowcount=10)

        with patch.object(wp_benchmark, "is_setup", return_value=True):
            with patch.object(wp_benchmark, "execute_operation") as mock_execute:
                mock_execute.return_value = OperationResult(
                    operation_id="cat_op",
                    success=True,
                    write_duration_ms=10.0,
                    rows_affected=10,
                    validation_duration_ms=5.0,
                    validation_passed=True,
                    validation_results=[],
                    cleanup_duration_ms=2.0,
                    cleanup_success=True,
                )

                categories = wp_benchmark.get_operation_categories()
                if categories:
                    results = wp_benchmark.run_benchmark(mock_conn, categories=[categories[0]])
                    assert isinstance(results, list)


class TestSetupAndTeardown:
    """Tests for setup and teardown methods."""

    @pytest.fixture
    def wp_benchmark(self, tmp_path):
        """Create a benchmark instance for testing."""
        return WritePrimitivesBenchmark(output_dir=tmp_path)

    def test_teardown_drops_tables(self, wp_benchmark):
        """Test that teardown drops staging tables."""
        mock_conn = Mock()

        wp_benchmark.teardown(mock_conn)

        # Should have called execute for each staging table
        assert mock_conn.execute.call_count > 0
        # Check some DROP TABLE calls were made
        drop_calls = [call for call in mock_conn.execute.call_args_list if "DROP TABLE" in str(call)]
        assert len(drop_calls) > 0

    def test_is_setup_false_when_tables_missing(self, wp_benchmark):
        """Test is_setup returns False when tables are missing."""
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Table not found")

        result = wp_benchmark.is_setup(mock_conn)
        assert result is False

    def test_is_setup_false_when_tables_empty(self, wp_benchmark):
        """Test is_setup returns False when tables are empty."""
        mock_conn = Mock()
        # Return 0 rows
        mock_conn.execute.return_value.fetchone.return_value = (0,)

        result = wp_benchmark.is_setup(mock_conn)
        assert result is False


class TestCleanupAuxiliaryFiles:
    """Tests for auxiliary file cleanup."""

    @pytest.fixture
    def wp_benchmark(self, tmp_path):
        """Create a benchmark instance with a real output dir."""
        return WritePrimitivesBenchmark(output_dir=tmp_path)

    def test_cleanup_removes_directory(self, wp_benchmark, tmp_path):
        """Test that cleanup_auxiliary_files removes the directory."""
        # Create the auxiliary directory
        aux_dir = tmp_path / "write_primitives_auxiliary"
        aux_dir.mkdir()
        (aux_dir / "test.csv").write_text("data")

        # Set up data generator's files_dir
        wp_benchmark.data_generator.files_dir = aux_dir

        wp_benchmark.cleanup_auxiliary_files()

        assert not aux_dir.exists()

    def test_cleanup_handles_nonexistent_dir(self, wp_benchmark, tmp_path):
        """Test that cleanup handles nonexistent directory gracefully."""
        aux_dir = tmp_path / "nonexistent"
        wp_benchmark.data_generator.files_dir = aux_dir

        # Should not raise an error
        wp_benchmark.cleanup_auxiliary_files()
