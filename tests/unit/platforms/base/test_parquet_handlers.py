"""Tests for Parquet file handlers."""

import builtins
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from benchbox.platforms.base.data_loading import (
    DuckDBParquetHandler,
    FileFormatRegistry,
    ParquetFileHandler,
)

pytestmark = pytest.mark.fast


class TestParquetFileHandler:
    """Tests for generic ParquetFileHandler."""

    @pytest.fixture
    def handler(self):
        """Create handler instance."""
        return ParquetFileHandler()

    @pytest.fixture
    def temp_parquet_file(self, tmp_path):
        """Create a temporary Parquet file for testing."""
        # Create test data
        data = {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "amount": [100.50, 200.75, 300.00],
        }
        table = pa.table(data)

        # Write to Parquet
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, parquet_file)

        return parquet_file

    def test_get_delimiter(self, handler):
        """Test that Parquet handler returns empty delimiter."""
        assert handler.get_delimiter() == ""

    def test_load_table_basic(self, handler, temp_parquet_file):
        """Test loading a basic Parquet file."""
        # Mock connection
        connection = Mock()
        connection.executemany = Mock()

        # Mock logger and benchmark
        logger = Mock()
        benchmark = Mock()

        # Load table
        row_count = handler.load_table(
            "test_table",
            temp_parquet_file,
            connection,
            benchmark,
            logger,
        )

        # Verify row count
        assert row_count == 3

        # Verify executemany was called
        assert connection.executemany.called

        # Check the INSERT statement
        call_args = connection.executemany.call_args[0]
        insert_sql = call_args[0]
        assert "INSERT INTO test_table" in insert_sql
        assert "(id,name,amount)" in insert_sql
        assert "VALUES (?,?,?)" in insert_sql

        # Check data was passed correctly
        data_tuples = call_args[1]
        assert len(data_tuples) == 3
        assert data_tuples[0] == (1, "Alice", 100.50)
        assert data_tuples[1] == (2, "Bob", 200.75)
        assert data_tuples[2] == (3, "Charlie", 300.00)

    def test_load_table_empty_file(self, handler, tmp_path):
        """Test loading an empty Parquet file."""
        # Create empty Parquet file
        empty_data = {"id": [], "name": []}
        table = pa.table(empty_data)
        parquet_file = tmp_path / "empty.parquet"
        pq.write_table(table, parquet_file)

        # Mock connection
        connection = Mock()
        logger = Mock()
        benchmark = Mock()

        # Load table
        row_count = handler.load_table(
            "test_table",
            parquet_file,
            connection,
            benchmark,
            logger,
        )

        # Verify no rows loaded
        assert row_count == 0
        assert not connection.executemany.called

    def test_load_table_large_file(self, handler, tmp_path):
        """Test loading a large Parquet file with batching."""
        # Create large dataset (2500 rows to test batching with batch_size=1000)
        data = {
            "id": list(range(2500)),
            "value": [f"value_{i}" for i in range(2500)],
        }
        table = pa.table(data)
        parquet_file = tmp_path / "large.parquet"
        pq.write_table(table, parquet_file)

        # Mock connection
        connection = Mock()
        connection.executemany = Mock()
        logger = Mock()
        benchmark = Mock()

        # Load table
        row_count = handler.load_table(
            "test_table",
            parquet_file,
            connection,
            benchmark,
            logger,
        )

        # Verify row count
        assert row_count == 2500

        # Verify batching - should be called 3 times (1000 + 1000 + 500)
        assert connection.executemany.call_count == 3

        # Verify batch sizes
        calls = connection.executemany.call_args_list
        assert len(calls[0][0][1]) == 1000  # First batch
        assert len(calls[1][0][1]) == 1000  # Second batch
        assert len(calls[2][0][1]) == 500  # Last batch

    def test_load_table_pyarrow_not_installed(self, handler, tmp_path, monkeypatch):
        """Test error handling when PyArrow is not installed."""
        real_import = builtins.__import__

        def mock_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name.startswith("pyarrow"):
                raise ImportError("pyarrow not found")
            return real_import(name, globals, locals, fromlist, level)

        monkeypatch.setattr("builtins.__import__", mock_import)

        # Attempt to load
        connection = Mock()
        logger = Mock()
        benchmark = Mock()
        parquet_file = tmp_path / "test.parquet"

        with pytest.raises(RuntimeError, match="pyarrow is required"):
            # Create new handler instance to trigger import
            handler_new = ParquetFileHandler()
            handler_new.load_table("test_table", parquet_file, connection, benchmark, logger)


class TestDuckDBParquetHandler:
    """Tests for DuckDB-optimized Parquet handler."""

    @pytest.fixture
    def adapter(self):
        """Create mock adapter."""
        adapter = Mock()
        adapter.dry_run_mode = False
        return adapter

    @pytest.fixture
    def handler(self, adapter):
        """Create handler instance."""
        return DuckDBParquetHandler(adapter)

    def test_get_delimiter(self, handler):
        """Test that DuckDB Parquet handler returns empty delimiter."""
        assert handler.get_delimiter() == ""

    def test_load_table_normal_mode(self, handler, tmp_path):
        """Test loading Parquet file in normal mode."""
        parquet_file = tmp_path / "test.parquet"
        parquet_file.touch()

        # Mock connection
        connection = Mock()
        connection.execute = Mock()
        connection.execute.return_value.fetchone.return_value = (1000,)

        # Mock logger and benchmark
        logger = Mock()
        benchmark = Mock()

        # Load table
        row_count = handler.load_table(
            "customer",
            parquet_file,
            connection,
            benchmark,
            logger,
        )

        # Verify row count
        assert row_count == 1000

        # Verify execute was called with correct SQL
        assert connection.execute.call_count == 2  # INSERT + COUNT

        # Check INSERT statement
        insert_call = connection.execute.call_args_list[0]
        insert_sql = insert_call[0][0]
        assert "INSERT INTO customer" in insert_sql
        assert f"read_parquet('{parquet_file}')" in insert_sql

    def test_load_table_dry_run_mode(self, handler, adapter, tmp_path):
        """Test loading Parquet file in dry-run mode."""
        adapter.dry_run_mode = True
        adapter.capture_sql = Mock()

        parquet_file = tmp_path / "test.parquet"
        parquet_file.touch()

        # Mock connection
        connection = Mock()
        logger = Mock()
        benchmark = Mock()

        # Load table
        row_count = handler.load_table(
            "customer",
            parquet_file,
            connection,
            benchmark,
            logger,
        )

        # Verify placeholder row count
        assert row_count == 1000

        # Verify SQL was captured
        assert adapter.capture_sql.called
        call_args = adapter.capture_sql.call_args
        assert "INSERT INTO customer" in call_args[0][0]
        assert f"read_parquet('{parquet_file}')" in call_args[0][0]
        assert call_args[0][1] == "load_data"
        assert call_args[0][2] == "customer"

        # Verify connection was not used
        assert not connection.execute.called


class TestFileFormatRegistry:
    """Tests for Parquet format registration."""

    def test_parquet_format_registered(self):
        """Test that .parquet extension is registered."""
        handler = FileFormatRegistry.get_handler(Path("test.parquet"))
        assert handler is not None
        assert isinstance(handler, ParquetFileHandler)

    def test_get_base_data_extension_parquet(self):
        """Test base extension detection for Parquet files."""
        # Simple parquet file
        assert FileFormatRegistry.get_base_data_extension(Path("customer.parquet")) == ".parquet"

        # Parquet with compression (Parquet handles compression internally)
        # Note: Parquet files typically don't have .gz/.zst extensions as they have internal compression
        assert FileFormatRegistry.get_base_data_extension(Path("customer.parquet")) == ".parquet"

    def test_parquet_handler_returned_for_parquet_files(self):
        """Test that ParquetFileHandler is returned for .parquet files."""
        handler = FileFormatRegistry.get_handler(Path("/data/customer.parquet"))
        assert isinstance(handler, ParquetFileHandler)
        assert handler.get_delimiter() == ""


class TestParquetIntegration:
    """Integration tests for Parquet loading."""

    def test_end_to_end_parquet_loading(self, tmp_path):
        """Test complete flow from Parquet file to database."""
        # Create test Parquet file
        data = {
            "c_custkey": [1, 2, 3, 4, 5],
            "c_name": ["Customer#1", "Customer#2", "Customer#3", "Customer#4", "Customer#5"],
            "c_acctbal": [100.50, 200.75, 300.00, 400.25, 500.50],
        }
        table = pa.table(data)
        parquet_file = tmp_path / "customer.parquet"
        pq.write_table(table, parquet_file)

        # Get handler from registry
        handler = FileFormatRegistry.get_handler(parquet_file)
        assert isinstance(handler, ParquetFileHandler)

        # Mock connection
        connection = Mock()
        connection.executemany = Mock()
        logger = Mock()
        benchmark = Mock()

        # Load table
        row_count = handler.load_table(
            "customer",
            parquet_file,
            connection,
            benchmark,
            logger,
        )

        # Verify results
        assert row_count == 5
        assert connection.executemany.called

        # Verify data integrity
        call_args = connection.executemany.call_args[0]
        data_tuples = call_args[1]
        assert len(data_tuples) == 5
        assert data_tuples[0] == (1, "Customer#1", 100.50)
        assert data_tuples[4] == (5, "Customer#5", 500.50)
