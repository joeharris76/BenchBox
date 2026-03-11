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
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest

from benchbox.core.write_primitives.benchmark import OperationResult, WritePrimitivesBenchmark

pytestmark = [
    pytest.mark.unit,
    pytest.mark.medium,
]


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

    def test_execute_operation_skips_unsupported_datafusion_category(self, wp_benchmark, monkeypatch):
        """DataFusion should mark unsupported operation categories as SKIPPED via platform_overrides."""
        mock_conn = Mock()
        mock_conn.execute = Mock()

        monkeypatch.setattr(wp_benchmark, "is_setup", lambda conn: True)
        result = wp_benchmark.execute_operation("update_single_row_pk", mock_conn, platform_key="datafusion")

        assert result.status == "SKIPPED"
        assert result.success is True
        assert result.validation_passed is True
        mock_conn.execute.assert_not_called()

    def test_execute_operation_uses_platform_override_when_available(self, wp_benchmark, monkeypatch):
        """Platform override SQL should be used instead of base write_sql."""
        operation = wp_benchmark.get_operation("insert_on_conflict_ignore")

        # DuckDB override is present in catalog for this operation.
        assert operation.platform_overrides.get("duckdb")

        class _Result:
            rowcount = 1

            def fetchall(self):
                return [(1,)]

        sql_calls: list[str] = []

        def _execute(sql):
            sql_calls.append(sql)
            return _Result()

        mock_conn = Mock()
        mock_conn.execute = _execute
        monkeypatch.setattr(wp_benchmark, "is_setup", lambda conn: True)

        result = wp_benchmark.execute_operation("insert_on_conflict_ignore", mock_conn, platform_key="duckdb")

        assert result.status == "SUCCESS"
        assert any("INSERT OR IGNORE" in sql for sql in sql_calls)


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

    def test_acquire_setup_lock_skips_lock_table_for_datafusion(self, wp_benchmark):
        """DataFusion setup lock should bypass SQL lock-table DDL/PK usage."""
        mock_conn = Mock()

        assert wp_benchmark._acquire_setup_lock(mock_conn, dialect="datafusion") is True
        mock_conn.execute.assert_not_called()

    def test_setup_uses_datafusion_dialect_for_staging_tables(self, wp_benchmark, monkeypatch):
        """setup() should request DataFusion dialect staging DDL when dialect='datafusion'."""

        class _Result:
            def __init__(self, row=(1,)):
                self._row = row

            def fetchone(self):
                return self._row

        mock_conn = Mock()
        mock_conn.execute.return_value = _Result((1,))
        monkeypatch.setattr(wp_benchmark, "_table_exists", lambda conn, table_name: False)

        requested_dialects: list[str] = []

        def _fake_create_sql(table_name, dialect="standard", if_not_exists=False):
            requested_dialects.append(dialect)
            if_not_exists_clause = " IF NOT EXISTS" if if_not_exists else ""
            return f"CREATE TABLE{if_not_exists_clause} {table_name} (id INTEGER)"

        monkeypatch.setattr("benchbox.core.write_primitives.benchmark.get_create_table_sql", _fake_create_sql)

        result = wp_benchmark.setup(mock_conn, force=False, dialect="datafusion")

        assert result["success"] is True
        assert requested_dialects
        assert set(requested_dialects) == {"datafusion"}


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


class TestDataFrameSqlParity:
    """Tests for DataFrame/SQL parity behavior in Write Primitives execution."""

    @pytest.fixture
    def wp_benchmark(self, tmp_path):
        """Create benchmark instance for parity tests."""
        return WritePrimitivesBenchmark(output_dir=tmp_path)

    def test_dataframe_workload_matches_sql_status_mapping_for_same_operations(self, wp_benchmark, monkeypatch):
        """DataFrame workload should map operation outcomes identically to SQL runner semantics."""
        operation_ids = ["insert_single_row", "update_single_row_pk", "ddl_create_table_simple"]

        monkeypatch.setattr(
            wp_benchmark,
            "_select_dataframe_operation_ids",
            lambda query_filter=None: operation_ids,  # noqa: ARG005
        )

        # Avoid touching real DuckDB/data loading in this unit test.
        monkeypatch.setattr("benchbox.platforms.duckdb.DuckDBAdapter.create_connection", lambda self, **_: object())
        monkeypatch.setattr(
            "benchbox.platforms.duckdb.DuckDBAdapter.create_schema",
            lambda self, benchmark, connection: 0.0,  # noqa: ARG005
        )
        monkeypatch.setattr(
            "benchbox.platforms.duckdb.DuckDBAdapter.load_data",
            lambda self, benchmark, connection, data_dir: ({}, 0.0, None),  # noqa: ARG005
        )
        monkeypatch.setattr(
            "benchbox.platforms.duckdb.DuckDBAdapter.close_connection",
            lambda self, connection: None,  # noqa: ARG005
        )

        results_by_id = {
            "insert_single_row": OperationResult(
                operation_id="insert_single_row",
                success=True,
                write_duration_ms=1.0,
                rows_affected=1,
                validation_duration_ms=0.1,
                validation_passed=True,
                validation_results=[],
                cleanup_duration_ms=0.1,
                cleanup_success=True,
            ),
            "update_single_row_pk": OperationResult(
                operation_id="update_single_row_pk",
                success=False,
                write_duration_ms=2.0,
                rows_affected=0,
                validation_duration_ms=0.1,
                validation_passed=False,
                validation_results=[],
                cleanup_duration_ms=0.0,
                cleanup_success=False,
                error="forced update failure",
            ),
            "ddl_create_table_simple": OperationResult(
                operation_id="ddl_create_table_simple",
                success=True,
                write_duration_ms=0.5,
                rows_affected=-1,
                validation_duration_ms=0.1,
                validation_passed=False,
                validation_results=[],
                cleanup_duration_ms=0.0,
                cleanup_success=True,
            ),
        }

        monkeypatch.setattr(
            wp_benchmark,
            "execute_operation",
            lambda op_id, connection: results_by_id[op_id],  # noqa: ARG005
        )

        sql_results = wp_benchmark.run_benchmark(connection=object(), operation_ids=operation_ids)
        expected_status = {
            result.operation_id: ("SUCCESS" if result.success and result.validation_passed else "FAILED")
            for result in sql_results
        }

        class DummyAdapter:
            platform_name = "polars-df"

        dataframe_rows = wp_benchmark.execute_dataframe_workload(
            ctx=None,
            adapter=DummyAdapter(),
            benchmark_config=SimpleNamespace(options={}),
            query_filter=None,
            monitor=None,
            run_options=None,
        )

        # Default parity shape: 1 warmup + 3 measurements for each operation ID.
        assert len(dataframe_rows) == len(operation_ids) * 4

        seen_status: dict[str, set[str]] = {op_id: set() for op_id in operation_ids}
        for row in dataframe_rows:
            query_id = row["query_id"]
            assert query_id in expected_status
            seen_status[query_id].add(row["status"])
            assert row["run_type"] in {"warmup", "measurement"}
            assert row["iteration"] in {0, 1, 2, 3}

        for op_id in operation_ids:
            assert seen_status[op_id] == {expected_status[op_id]}

    def test_dataframe_workload_query_filter_keeps_sql_subset(self, wp_benchmark, monkeypatch):
        """Query filter should constrain DataFrame parity execution to SQL-equivalent subset."""
        monkeypatch.setattr("benchbox.platforms.duckdb.DuckDBAdapter.create_connection", lambda self, **_: object())
        monkeypatch.setattr(
            "benchbox.platforms.duckdb.DuckDBAdapter.create_schema",
            lambda self, benchmark, connection: 0.0,  # noqa: ARG005
        )
        monkeypatch.setattr(
            "benchbox.platforms.duckdb.DuckDBAdapter.load_data",
            lambda self, benchmark, connection, data_dir: ({}, 0.0, None),  # noqa: ARG005
        )
        monkeypatch.setattr(
            "benchbox.platforms.duckdb.DuckDBAdapter.close_connection",
            lambda self, connection: None,  # noqa: ARG005
        )

        monkeypatch.setattr(
            wp_benchmark,
            "execute_operation",
            lambda op_id, connection: OperationResult(  # noqa: ARG005
                operation_id=op_id,
                success=True,
                write_duration_ms=1.0,
                rows_affected=1,
                validation_duration_ms=0.1,
                validation_passed=True,
                validation_results=[],
                cleanup_duration_ms=0.1,
                cleanup_success=True,
            ),
        )

        class DummyAdapter:
            platform_name = "polars-df"

        rows = wp_benchmark.execute_dataframe_workload(
            ctx=None,
            adapter=DummyAdapter(),
            benchmark_config=SimpleNamespace(options={}),
            query_filter={"INSERT_SINGLE_ROW"},
            monitor=None,
            run_options=None,
        )

        assert rows
        assert {row["query_id"] for row in rows} == {"insert_single_row"}
