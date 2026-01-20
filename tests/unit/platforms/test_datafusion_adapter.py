"""Tests for DataFusion platform adapter.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from benchbox.platforms.datafusion import DataFusionAdapter

pytestmark = pytest.mark.fast


class TestDataFusionAdapter:
    """Test DataFusion platform adapter functionality."""

    def test_initialization_success(self):
        """Test successful adapter initialization."""
        with patch("benchbox.platforms.datafusion.SessionContext"), tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(
                working_dir=tmpdir,
                memory_limit="8G",
                target_partitions=4,
                data_format="parquet",
                batch_size=16384,
            )
            assert adapter.platform_name == "DataFusion"
            assert adapter.get_target_dialect() == "postgres"
            assert str(adapter.working_dir) == tmpdir
            assert adapter.memory_limit == "8G"
            assert adapter.target_partitions == 4
            assert adapter.data_format == "parquet"
            assert adapter.batch_size == 16384

    def test_initialization_with_defaults(self):
        """Test initialization with default configuration."""
        with patch("benchbox.platforms.datafusion.SessionContext"), tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)
            assert adapter.working_dir == Path(tmpdir)
            assert adapter.memory_limit == "16G"
            assert adapter.target_partitions > 0  # Should default to CPU count
            assert adapter.data_format == "parquet"
            assert adapter.batch_size == 8192

    def test_initialization_missing_driver(self):
        """Test initialization when DataFusion driver is not available."""
        with patch("benchbox.platforms.datafusion.SessionContext", None):
            with pytest.raises(ImportError, match="DataFusion not installed"):
                DataFusionAdapter()

    def test_get_platform_info(self):
        """Test platform information retrieval."""
        with patch("benchbox.platforms.datafusion.SessionContext"), tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir, memory_limit="8G")

            # Mock the datafusion module import inside get_platform_info
            mock_df_module = Mock()
            mock_df_module.__version__ = "50.1.0"

            with patch(
                "builtins.__import__",
                side_effect=lambda name, *args: mock_df_module if name == "datafusion" else __import__(name, *args),
            ):
                info = adapter.get_platform_info()

            assert info["platform_type"] == "datafusion"
            assert info["platform_name"] == "DataFusion"
            assert info["connection_mode"] == "in-memory"
            assert info["configuration"]["memory_limit"] == "8G"
            assert info["client_library_version"] == "50.1.0"

    def test_create_connection(self):
        """Test connection creation with SessionContext."""

        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup comprehensive mocking - mock the entire datafusion module
            mock_config = Mock()
            mock_config.with_target_partitions.return_value = mock_config
            mock_config.with_parquet_pruning.return_value = mock_config
            mock_config.with_repartition_joins.return_value = mock_config
            mock_config.with_repartition_aggregations.return_value = mock_config
            mock_config.with_repartition_windows.return_value = mock_config
            mock_config.with_information_schema.return_value = mock_config
            mock_config.with_batch_size.return_value = mock_config
            # Make set() raise an exception to simulate Rust panic - we catch this in the code
            mock_config.set.side_effect = Exception("Config not supported")

            mock_ctx = Mock()

            # Mock RuntimeEnv with build method
            mock_runtime_builder = Mock()
            mock_runtime_builder.build.return_value = Mock()

            with patch("benchbox.platforms.datafusion.SessionConfig", return_value=mock_config):
                with patch("benchbox.platforms.datafusion.SessionContext", return_value=mock_ctx):
                    with patch("benchbox.platforms.datafusion.RuntimeEnv", return_value=mock_runtime_builder):
                        adapter = DataFusionAdapter(
                            working_dir=tmpdir,
                            memory_limit="8G",
                            target_partitions=4,
                            batch_size=16384,
                        )

                        with patch.object(adapter, "handle_existing_database"):
                            connection = adapter.create_connection()

                        assert connection == mock_ctx

                        # Verify configuration was applied
                        mock_config.with_target_partitions.assert_called_once_with(4)
                        mock_config.with_parquet_pruning.assert_called_once_with(True)
                        mock_config.with_repartition_joins.assert_called_once_with(True)
                        mock_config.with_batch_size.assert_called_once_with(16384)

    def test_parse_memory_limit(self):
        """Test memory limit parsing."""
        with patch("benchbox.platforms.datafusion.SessionContext"), tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            # Test GB parsing
            assert adapter._parse_memory_limit("16G") == str(16 * 1024 * 1024 * 1024)
            assert adapter._parse_memory_limit("16GB") == str(16 * 1024 * 1024 * 1024)

            # Test MB parsing
            assert adapter._parse_memory_limit("4096M") == str(4096 * 1024 * 1024)
            assert adapter._parse_memory_limit("4096MB") == str(4096 * 1024 * 1024)

            # Test KB parsing
            assert adapter._parse_memory_limit("1024K") == str(1024 * 1024)
            assert adapter._parse_memory_limit("1024KB") == str(1024 * 1024)

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_create_schema(self, mock_session_context):
        """Test schema creation."""
        mock_benchmark = Mock()
        mock_benchmark.__class__.__name__ = "TPCHBenchmark"
        mock_benchmark.get_schema.return_value = {
            "test": {
                "name": "test",
                "columns": [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "VARCHAR(100)"}],
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            # Mock the constraint configuration
            with patch.object(adapter, "_get_constraint_configuration") as mock_constraints:
                with patch.object(adapter, "_log_constraint_configuration"):
                    mock_constraints.return_value = (False, False)

                    mock_connection = Mock()
                    duration = adapter.create_schema(mock_benchmark, mock_connection)

                    assert duration >= 0
                    assert hasattr(adapter, "_table_schemas")
                    assert isinstance(adapter._table_schemas, dict)
                    assert "test" in adapter._table_schemas
                    assert len(adapter._table_schemas["test"]["columns"]) == 2

    def test_get_benchmark_schema(self):
        """Test getting structured schema from benchmark."""
        with patch("benchbox.platforms.datafusion.SessionContext"), tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            # Mock benchmark with get_schema() method
            mock_benchmark = Mock()
            mock_benchmark.get_schema.return_value = {
                "customer": {
                    "name": "customer",
                    "columns": [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "VARCHAR(100)"}],
                },
                "orders": {
                    "name": "orders",
                    "columns": [{"name": "id", "type": "INTEGER"}, {"name": "customer_id", "type": "INTEGER"}],
                },
            }

            table_schemas = adapter._get_benchmark_schema(mock_benchmark)
            assert len(table_schemas) == 2
            assert "customer" in table_schemas
            assert "orders" in table_schemas
            assert len(table_schemas["customer"]["columns"]) == 2
            assert table_schemas["customer"]["columns"][0]["name"] == "id"

    def test_get_benchmark_schema_fallback(self):
        """Test fallback when benchmark doesn't have get_schema()."""
        with patch("benchbox.platforms.datafusion.SessionContext"), tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            # Mock benchmark without get_schema() method
            mock_benchmark = Mock(spec=[])  # Empty spec means no methods
            mock_benchmark.__class__.__name__ = "CustomBenchmark"

            table_schemas = adapter._get_benchmark_schema(mock_benchmark)
            # Should return empty dict when get_schema() is not available
            assert table_schemas == {}

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_load_table_csv(self, mock_session_context):
        """Test loading table from CSV files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test CSV file
            csv_file = Path(tmpdir) / "test.csv"
            csv_file.write_text("1,test\n2,data\n")

            adapter = DataFusionAdapter(working_dir=tmpdir, data_format="csv")

            # Mock connection
            mock_conn = Mock()
            mock_result = Mock()
            mock_batch = Mock()
            mock_batch.column.return_value = [2]  # Row count
            mock_result.collect.return_value = [mock_batch]
            mock_conn.sql.return_value = mock_result

            row_count = adapter._load_table_csv(mock_conn, "test_table", [csv_file], Path(tmpdir))

            assert row_count == 2
            # Verify CREATE EXTERNAL TABLE was called
            assert mock_conn.sql.called

    @patch("benchbox.platforms.datafusion.SessionContext")
    @patch("pyarrow.csv.read_csv")
    @patch("pyarrow.parquet.write_table")
    @patch("pyarrow.concat_tables")
    def test_load_table_parquet(self, mock_concat_tables, mock_write_table, mock_read_csv, mock_session_context):
        """Test loading table by converting CSV to Parquet."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test CSV file
            csv_file = Path(tmpdir) / "test.csv"
            csv_file.write_text("1,test\n2,data\n")

            adapter = DataFusionAdapter(working_dir=tmpdir, data_format="parquet")

            # Mock PyArrow objects
            mock_table = Mock()
            mock_table.num_rows = 2
            mock_read_csv.return_value = mock_table
            mock_concat_tables.return_value = mock_table

            # Mock connection
            mock_conn = Mock()

            row_count = adapter._load_table_parquet(mock_conn, "test_table", [csv_file], Path(tmpdir))

            assert row_count == 2
            # Verify Parquet file was written
            mock_write_table.assert_called_once()
            # Verify table was registered
            mock_conn.register_parquet.assert_called_once()

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_execute_query_success(self, mock_session_context):
        """Test successful query execution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            # Mock connection and result
            mock_conn = Mock()
            mock_df = Mock()
            mock_batch = Mock()
            mock_batch.num_rows = 10
            mock_batch.num_columns = 2
            mock_batch.column.side_effect = lambda i: [f"value_{i}"]
            mock_df.collect.return_value = [mock_batch]
            mock_conn.sql.return_value = mock_df

            result = adapter.execute_query(
                mock_conn,
                "SELECT * FROM test",
                "query_1",
                benchmark_type="tpch",
                scale_factor=1.0,
                validate_row_count=False,
            )

            assert result["query_id"] == "query_1"
            assert result["status"] == "SUCCESS"
            assert result["rows_returned"] == 10
            assert result["execution_time"] >= 0

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_execute_query_failure(self, mock_session_context):
        """Test query execution failure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            # Mock connection to raise error
            mock_conn = Mock()
            mock_conn.sql.side_effect = Exception("Query failed")

            result = adapter.execute_query(
                mock_conn,
                "SELECT * FROM nonexistent",
                "query_1",
            )

            assert result["query_id"] == "query_1"
            assert result["status"] == "FAILED"
            assert "Query failed" in result["error"]

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_execute_query_dry_run(self, mock_session_context):
        """Test query execution in dry-run mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)
            adapter.dry_run_mode = True

            mock_conn = Mock()

            with patch.object(adapter, "capture_sql"):
                result = adapter.execute_query(
                    mock_conn,
                    "SELECT * FROM test",
                    "query_1",
                )

            assert result["query_id"] == "query_1"
            assert result["status"] == "DRY_RUN"
            assert result["dry_run"] is True

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_configure_for_benchmark(self, mock_session_context):
        """Test benchmark-specific configuration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            mock_conn = Mock()
            # Should not raise
            adapter.configure_for_benchmark(mock_conn, "tpch")

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_apply_platform_optimizations(self, mock_session_context):
        """Test platform optimization application."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            mock_config = Mock()
            mock_conn = Mock()

            # Should not raise
            adapter.apply_platform_optimizations(mock_config, mock_conn)

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_apply_constraint_configuration(self, mock_session_context):
        """Test constraint configuration application."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            mock_pk_config = Mock()
            mock_pk_config.enabled = True
            mock_fk_config = Mock()
            mock_fk_config.enabled = True
            mock_conn = Mock()

            # Should not raise (DataFusion doesn't enforce constraints)
            adapter.apply_constraint_configuration(mock_pk_config, mock_fk_config, mock_conn)

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_check_database_exists(self, mock_session_context):
        """Test database existence checking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            # Should not exist initially (no parquet files)
            assert not adapter.check_database_exists()

            # Create a parquet file in the working directory
            (Path(tmpdir) / "test.parquet").touch()

            # Should exist now
            assert adapter.check_database_exists()

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_drop_database(self, mock_session_context):
        """Test database dropping."""
        with tempfile.TemporaryDirectory() as tmpdir:
            working_dir = Path(tmpdir) / "datafusion_working"
            working_dir.mkdir()
            (working_dir / "test.txt").touch()

            adapter = DataFusionAdapter(working_dir=str(working_dir))

            assert working_dir.exists()
            adapter.drop_database()
            assert not working_dir.exists()

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_validate_platform_capabilities(self, mock_session_context):
        """Test platform capability validation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir)

            result = adapter.validate_platform_capabilities("tpch")

            assert result.is_valid
            assert len(result.errors) == 0
            # Just verify it doesn't crash and validates successfully when mocked

    @patch("benchbox.platforms.datafusion.SessionContext", None)
    def test_validate_platform_capabilities_missing_driver(self):
        """Test platform validation when driver is missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Can't create adapter without driver
            with pytest.raises(ImportError):
                DataFusionAdapter(working_dir=tmpdir)

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_from_config(self, mock_session_context):
        """Test adapter creation from configuration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "benchmark": "tpch",
                "scale_factor": 1.0,
                "output_dir": tmpdir,
                "memory_limit": "8G",
                "partitions": 8,
                "format": "parquet",
                "batch_size": 16384,
            }

            adapter = DataFusionAdapter.from_config(config)

            assert adapter.memory_limit == "8G"
            assert adapter.target_partitions == 8
            assert adapter.data_format == "parquet"
            assert adapter.batch_size == 16384

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_add_cli_arguments(self, mock_session_context):
        """Test CLI argument addition."""
        import argparse

        parser = argparse.ArgumentParser()
        DataFusionAdapter.add_cli_arguments(parser)

        # Verify datafusion arguments were added
        args = parser.parse_args(
            [
                "--datafusion-memory-limit",
                "8G",
                "--datafusion-partitions",
                "4",
                "--datafusion-format",
                "parquet",
            ]
        )

        assert args.datafusion_memory_limit == "8G"
        assert args.datafusion_partitions == 4
        assert args.datafusion_format == "parquet"


class TestDataFusionColumnTypeMapping:
    """Test column type mapping for CSV to Parquet conversion.

    These tests verify that the fix for TPC-DS queries Q8, Q15, Q19, Q45
    correctly maps schema column types to PyArrow types to prevent incorrect
    type inference (e.g., ca_zip being inferred as int64 instead of string).
    """

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_load_table_parquet_with_schema_types(self, mock_session_context):
        """Test that schema types are correctly passed to PyArrow ConvertOptions."""
        import pyarrow as pa

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test CSV file with numeric-looking string data
            csv_file = Path(tmpdir) / "customer_address.csv"
            csv_file.write_text("1|89436|123 Main St\n2|07001|456 Oak Ave\n")

            adapter = DataFusionAdapter(working_dir=tmpdir, data_format="parquet")

            # Set up schema with VARCHAR type for ca_zip
            adapter._table_schemas = {
                "customer_address": {
                    "name": "customer_address",
                    "columns": [
                        {"name": "ca_address_sk", "type": "INTEGER"},
                        {"name": "ca_zip", "type": "CHAR(10)"},
                        {"name": "ca_street", "type": "VARCHAR(60)"},
                    ],
                }
            }

            # Mock PyArrow functions to capture column_types
            captured_convert_options = {}

            def capture_read_csv(file_path, read_options=None, parse_options=None, convert_options=None):
                # Capture the convert_options for verification
                if convert_options:
                    captured_convert_options["column_types"] = convert_options.column_types
                # Return a mock table
                mock_table = Mock()
                mock_table.num_rows = 2
                return mock_table

            mock_conn = Mock()

            with patch("pyarrow.csv.read_csv", side_effect=capture_read_csv):
                with patch("pyarrow.concat_tables") as mock_concat:
                    mock_combined = Mock()
                    mock_combined.num_rows = 2
                    mock_concat.return_value = mock_combined
                    with patch("pyarrow.parquet.write_table"):
                        adapter._load_table_parquet(mock_conn, "customer_address", [csv_file], Path(tmpdir))

            # Verify column types were correctly mapped
            assert "column_types" in captured_convert_options
            col_types = captured_convert_options["column_types"]

            # ca_zip should be string (CHAR type)
            assert "ca_zip" in col_types
            assert col_types["ca_zip"] == pa.string()

            # ca_street should be string (VARCHAR type)
            assert "ca_street" in col_types
            assert col_types["ca_street"] == pa.string()

            # ca_address_sk (INTEGER) should not be in column_types - uses auto-inference
            assert "ca_address_sk" not in col_types

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_column_type_mapping_char_variants(self, mock_session_context):
        """Test that all CHAR/VARCHAR variants map to PyArrow string."""
        import pyarrow as pa

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir, data_format="parquet")

            # Test various string type declarations
            adapter._table_schemas = {
                "test_table": {
                    "name": "test_table",
                    "columns": [
                        {"name": "col_char", "type": "CHAR(5)"},
                        {"name": "col_char_no_len", "type": "CHAR"},
                        {"name": "col_varchar", "type": "VARCHAR(100)"},
                        {"name": "col_varchar_no_len", "type": "VARCHAR"},
                        {"name": "col_text", "type": "TEXT"},
                        {"name": "col_string", "type": "STRING"},
                    ],
                }
            }

            captured_types = {}

            def capture_read_csv(file_path, read_options=None, parse_options=None, convert_options=None):
                if convert_options and convert_options.column_types:
                    captured_types.update(convert_options.column_types)
                mock_table = Mock()
                mock_table.num_rows = 1
                return mock_table

            mock_conn = Mock()
            csv_file = Path(tmpdir) / "test.csv"
            csv_file.write_text("a|b|c|d|e|f\n")

            with patch("pyarrow.csv.read_csv", side_effect=capture_read_csv):
                with patch("pyarrow.concat_tables") as mock_concat:
                    mock_concat.return_value = Mock(num_rows=1)
                    with patch("pyarrow.parquet.write_table"):
                        adapter._load_table_parquet(mock_conn, "test_table", [csv_file], Path(tmpdir))

            # All string-type columns should map to pa.string()
            for col_name in [
                "col_char",
                "col_char_no_len",
                "col_varchar",
                "col_varchar_no_len",
                "col_text",
                "col_string",
            ]:
                assert col_name in captured_types, f"{col_name} should be in column_types"
                assert captured_types[col_name] == pa.string(), f"{col_name} should be pa.string()"

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_column_type_mapping_date(self, mock_session_context):
        """Test that DATE types map to PyArrow date32."""
        import pyarrow as pa

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir, data_format="parquet")

            adapter._table_schemas = {
                "test_table": {
                    "name": "test_table",
                    "columns": [
                        {"name": "order_date", "type": "DATE"},
                        {"name": "ship_date", "type": "DATE"},
                    ],
                }
            }

            captured_types = {}

            def capture_read_csv(file_path, read_options=None, parse_options=None, convert_options=None):
                if convert_options and convert_options.column_types:
                    captured_types.update(convert_options.column_types)
                mock_table = Mock()
                mock_table.num_rows = 1
                return mock_table

            mock_conn = Mock()
            csv_file = Path(tmpdir) / "test.csv"
            csv_file.write_text("2024-01-01|2024-01-05\n")

            with patch("pyarrow.csv.read_csv", side_effect=capture_read_csv):
                with patch("pyarrow.concat_tables") as mock_concat:
                    mock_concat.return_value = Mock(num_rows=1)
                    with patch("pyarrow.parquet.write_table"):
                        adapter._load_table_parquet(mock_conn, "test_table", [csv_file], Path(tmpdir))

            assert "order_date" in captured_types
            assert captured_types["order_date"] == pa.date32()
            assert "ship_date" in captured_types
            assert captured_types["ship_date"] == pa.date32()

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_column_type_mapping_decimal(self, mock_session_context):
        """Test that DECIMAL types map to PyArrow float64."""
        import pyarrow as pa

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir, data_format="parquet")

            adapter._table_schemas = {
                "test_table": {
                    "name": "test_table",
                    "columns": [
                        {"name": "price", "type": "DECIMAL(10,2)"},
                        {"name": "tax", "type": "DECIMAL"},
                    ],
                }
            }

            captured_types = {}

            def capture_read_csv(file_path, read_options=None, parse_options=None, convert_options=None):
                if convert_options and convert_options.column_types:
                    captured_types.update(convert_options.column_types)
                mock_table = Mock()
                mock_table.num_rows = 1
                return mock_table

            mock_conn = Mock()
            csv_file = Path(tmpdir) / "test.csv"
            csv_file.write_text("99.99|5.50\n")

            with patch("pyarrow.csv.read_csv", side_effect=capture_read_csv):
                with patch("pyarrow.concat_tables") as mock_concat:
                    mock_concat.return_value = Mock(num_rows=1)
                    with patch("pyarrow.parquet.write_table"):
                        adapter._load_table_parquet(mock_conn, "test_table", [csv_file], Path(tmpdir))

            assert "price" in captured_types
            assert captured_types["price"] == pa.float64()
            assert "tax" in captured_types
            assert captured_types["tax"] == pa.float64()

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_column_type_mapping_integer_uses_inference(self, mock_session_context):
        """Test that INTEGER types are not explicitly mapped (use PyArrow inference)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir, data_format="parquet")

            adapter._table_schemas = {
                "test_table": {
                    "name": "test_table",
                    "columns": [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "quantity", "type": "BIGINT"},
                        {"name": "small_num", "type": "SMALLINT"},
                    ],
                }
            }

            captured_types = {}

            def capture_read_csv(file_path, read_options=None, parse_options=None, convert_options=None):
                if convert_options and convert_options.column_types:
                    captured_types.update(convert_options.column_types)
                mock_table = Mock()
                mock_table.num_rows = 1
                return mock_table

            mock_conn = Mock()
            csv_file = Path(tmpdir) / "test.csv"
            csv_file.write_text("1|100|5\n")

            with patch("pyarrow.csv.read_csv", side_effect=capture_read_csv):
                with patch("pyarrow.concat_tables") as mock_concat:
                    mock_concat.return_value = Mock(num_rows=1)
                    with patch("pyarrow.parquet.write_table"):
                        adapter._load_table_parquet(mock_conn, "test_table", [csv_file], Path(tmpdir))

            # Integer types should NOT be in column_types - they use auto-inference
            assert "id" not in captured_types
            assert "quantity" not in captured_types
            assert "small_num" not in captured_types

    @patch("benchbox.platforms.datafusion.SessionContext")
    def test_no_schema_uses_no_column_types(self, mock_session_context):
        """Test that missing schema means no explicit column types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = DataFusionAdapter(working_dir=tmpdir, data_format="parquet")

            # No schema set
            adapter._table_schemas = {}

            captured_types = {"called": False}

            def capture_read_csv(file_path, read_options=None, parse_options=None, convert_options=None):
                captured_types["called"] = True
                if convert_options:
                    captured_types["column_types"] = convert_options.column_types
                mock_table = Mock()
                mock_table.num_rows = 1
                return mock_table

            mock_conn = Mock()
            csv_file = Path(tmpdir) / "test.csv"
            csv_file.write_text("1|test|2024-01-01\n")

            with patch("pyarrow.csv.read_csv", side_effect=capture_read_csv):
                with patch("pyarrow.concat_tables") as mock_concat:
                    mock_concat.return_value = Mock(num_rows=1)
                    with patch("pyarrow.parquet.write_table"):
                        adapter._load_table_parquet(mock_conn, "unknown_table", [csv_file], Path(tmpdir))

            assert captured_types["called"]
            # column_types should be None or empty when no schema
            col_types = captured_types.get("column_types")
            assert col_types is None or len(col_types) == 0
