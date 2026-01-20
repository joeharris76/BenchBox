"""Tests for Polars platform adapter.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Check if Polars is available
try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False
    pl = None


pytestmark = pytest.mark.skipif(not POLARS_AVAILABLE, reason="Polars not installed")


class TestPolarsAdapterBasics:
    """Tests for basic PolarsAdapter functionality."""

    def test_initialization_success(self):
        """Test successful adapter initialization."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)

            assert adapter.platform_name == "Polars"
            assert adapter.execution_mode == "lazy"
            assert adapter.streaming is False
            assert adapter.rechunk is True

    def test_initialization_custom_config(self):
        """Test initialization with custom configuration."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(
                working_dir=tmpdir,
                execution_mode="eager",
                streaming=True,
                n_rows=1000,
                rechunk=False,
            )

            assert adapter.execution_mode == "eager"
            assert adapter.streaming is True
            assert adapter.n_rows == 1000
            assert adapter.rechunk is False

    def test_platform_name(self):
        """Test platform_name property."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            assert adapter.platform_name == "Polars"

    def test_target_dialect(self):
        """Test get_target_dialect returns dataframe (SQL mode not supported)."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            assert adapter.get_target_dialect() == "dataframe"


class TestPolarsAdapterFromConfig:
    """Tests for from_config class method."""

    def test_from_config_basic(self):
        """Test creating adapter from configuration dict."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "working_dir": tmpdir,
                "benchmark": "tpch",
                "scale_factor": 1.0,
            }

            adapter = PolarsAdapter.from_config(config)

            assert adapter.platform_name == "Polars"
            assert adapter.execution_mode == "lazy"

    def test_from_config_with_execution_mode(self):
        """Test from_config with custom execution mode."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            config = {
                "working_dir": tmpdir,
                "benchmark": "tpch",
                "scale_factor": 1.0,
                "execution_mode": "eager",
                "streaming": True,
            }

            adapter = PolarsAdapter.from_config(config)

            assert adapter.execution_mode == "eager"
            assert adapter.streaming is True


class TestPolarsDataFrameContext:
    """Tests for PolarsDataFrameContext wrapper."""

    def test_dataframe_context_creation(self):
        """Test DataFrame context wrapper creation."""
        from benchbox.platforms.polars_platform import PolarsAdapter, PolarsDataFrameContext

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            ctx = PolarsDataFrameContext(adapter)

            assert ctx is not None
            assert ctx.get_tables() == []

    def test_register_table(self):
        """Test registering a table in DataFrame context."""
        from benchbox.platforms.polars_platform import PolarsAdapter, PolarsDataFrameContext

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            ctx = PolarsDataFrameContext(adapter)

            # Create a simple DataFrame
            df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

            # Register it
            ctx.register_table("test_table", df)

            assert "test_table" in ctx.get_tables()

    def test_get_table(self):
        """Test getting a table by name."""
        from benchbox.platforms.polars_platform import PolarsAdapter, PolarsDataFrameContext

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            ctx = PolarsDataFrameContext(adapter)

            # Create and register a test table
            df = pl.DataFrame(
                {
                    "id": [1, 2, 3, 4, 5],
                    "value": [10, 20, 30, 40, 50],
                }
            )
            ctx.register_table("numbers", df)

            # Get table
            result = ctx.get_table("numbers")

            assert result is not None
            # It should be a LazyFrame
            assert isinstance(result, pl.LazyFrame)

    def test_get_nonexistent_table(self):
        """Test getting a nonexistent table returns None."""
        from benchbox.platforms.polars_platform import PolarsAdapter, PolarsDataFrameContext

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            ctx = PolarsDataFrameContext(adapter)

            # Get nonexistent table
            result = ctx.get_table("nonexistent")

            assert result is None

    def test_unregister_table(self):
        """Test unregistering a table."""
        from benchbox.platforms.polars_platform import PolarsAdapter, PolarsDataFrameContext

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            ctx = PolarsDataFrameContext(adapter)

            # Create and register a table
            df = pl.DataFrame({"id": [1, 2, 3]})
            ctx.register_table("test_table", df)
            assert "test_table" in ctx.get_tables()

            # Unregister it
            ctx.unregister_table("test_table")
            assert "test_table" not in ctx.get_tables()


class TestPolarsAdapterConnection:
    """Tests for connection creation."""

    def test_create_connection(self):
        """Test creating a Polars DataFrame context."""
        from benchbox.platforms.polars_platform import PolarsAdapter, PolarsDataFrameContext

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            conn = adapter.create_connection()

            assert isinstance(conn, PolarsDataFrameContext)

    def test_get_platform_info(self):
        """Test getting platform information."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            info = adapter.get_platform_info()

            assert info["platform_type"] == "polars"
            assert info["platform_name"] == "Polars"
            assert info["connection_mode"] == "in-memory"
            assert "client_library_version" in info
            assert info["client_library_version"] == pl.__version__


class TestPolarsAdapterSchemaCreation:
    """Tests for schema creation."""

    def test_create_schema_basic(self):
        """Test basic schema creation."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            conn = adapter.create_connection()

            # Create mock benchmark
            mock_benchmark = MagicMock()
            mock_benchmark.get_schema.return_value = {
                "orders": {
                    "name": "orders",
                    "columns": [
                        {"name": "order_id", "type": "INTEGER"},
                        {"name": "customer_id", "type": "INTEGER"},
                        {"name": "order_date", "type": "DATE"},
                    ],
                },
            }

            duration = adapter.create_schema(mock_benchmark, conn)

            assert duration >= 0
            assert "orders" in adapter._table_schemas


class TestPolarsAdapterQueryExecution:
    """Tests for query execution - SQL mode not supported."""

    def test_execute_query_raises_not_implemented(self):
        """Test that execute_query raises NotImplementedError (SQL mode not supported)."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)
            conn = adapter.create_connection()

            # Register test data
            df = pl.DataFrame(
                {
                    "id": [1, 2, 3, 4, 5],
                    "value": [100, 200, 300, 400, 500],
                }
            )
            conn.register_table("test", df)

            # Execute query should raise NotImplementedError
            with pytest.raises(NotImplementedError) as exc_info:
                adapter.execute_query(
                    connection=conn,
                    query="SELECT * FROM test WHERE value > 200",
                    query_id="Q1",
                    validate_row_count=False,
                )

            assert "SQL mode is not supported" in str(exc_info.value)
            assert "polars-df" in str(exc_info.value)


class TestPolarsAdapterDataLoading:
    """Tests for data loading functionality."""

    def test_detect_csv_format(self):
        """Test CSV format detection."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)

            # Test TBL files (TPC format)
            # BenchBox generates TPC files WITHOUT trailing delimiters (dbgen default)
            format_type, delim, trailing = adapter._detect_file_format([Path("orders.tbl")])
            assert format_type == "csv"
            assert delim == "|"
            assert trailing is False  # BenchBox TPC files don't have trailing delimiters

            # Test CSV files
            format_type, delim, trailing = adapter._detect_file_format([Path("data.csv")])
            assert format_type == "csv"
            assert delim == ","
            assert trailing is False

            # Test Parquet files
            format_type, delim, trailing = adapter._detect_file_format([Path("data.parquet")])
            assert format_type == "parquet"

    def test_load_csv_data(self):
        """Test loading CSV data."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            adapter = PolarsAdapter(working_dir=tmpdir)

            # Create a test CSV file
            csv_file = tmpdir_path / "test.csv"
            csv_file.write_text("1,Alice,100\n2,Bob,200\n3,Charlie,300\n")

            # Load the data
            lf = adapter._load_csv(
                file_paths=[csv_file],
                delimiter=",",
                column_names=["id", "name", "value"],
                has_trailing_delimiter=False,
            )

            df = lf.collect()
            assert len(df) == 3
            assert df.columns == ["id", "name", "value"]


class TestPolarsAdapterValidation:
    """Tests for validation functionality."""

    def test_validate_platform_capabilities(self):
        """Test platform capability validation."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)

            result = adapter.validate_platform_capabilities("tpch")

            assert result.is_valid is True
            assert "polars" in result.details["platform"].lower()
            assert result.details["polars_available"] is True

    def test_validate_platform_capabilities_sql_mode_warning(self):
        """Test that validation warns SQL mode is not available."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)

            result = adapter.validate_platform_capabilities("tpch")

            # Should have a warning about SQL mode not being available
            assert len(result.warnings) > 0
            assert any("SQL mode is not available" in w for w in result.warnings)
            assert result.details.get("sql_mode") is False

    def test_check_database_exists_empty(self):
        """Test checking for database when directory is empty."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)

            assert adapter.check_database_exists() is False

    def test_check_database_exists_with_parquet(self):
        """Test checking for database with parquet files."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            adapter = PolarsAdapter(working_dir=tmpdir)

            # Create a parquet file
            df = pl.DataFrame({"id": [1, 2, 3]})
            df.write_parquet(tmpdir_path / "test.parquet")

            assert adapter.check_database_exists() is True


class TestPolarsAdapterDatabaseOperations:
    """Tests for database operations."""

    def test_drop_database(self):
        """Test dropping database (removing working directory)."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as parent:
            working_dir = Path(parent) / "polars_data"
            working_dir.mkdir()

            # Create some files
            (working_dir / "test.parquet").touch()
            (working_dir / "data.csv").touch()

            adapter = PolarsAdapter(working_dir=str(working_dir))
            adapter.drop_database()

            assert not working_dir.exists()


class TestPolarsAdapterCLIArguments:
    """Tests for CLI argument handling."""

    def test_add_cli_arguments(self):
        """Test adding CLI arguments."""
        from argparse import ArgumentParser

        from benchbox.platforms.polars_platform import PolarsAdapter

        parser = ArgumentParser()
        PolarsAdapter.add_cli_arguments(parser)

        # Parse with Polars-specific args
        args = parser.parse_args(
            [
                "--polars-execution-mode",
                "eager",
                "--polars-streaming",
            ]
        )

        assert args.polars_execution_mode == "eager"
        assert args.polars_streaming is True


class TestPolarsAdapterIntegration:
    """Integration tests for the Polars adapter."""

    def test_full_workflow_data_loading(self):
        """Test complete workflow: connect, load data (SQL execution not supported)."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir)

            # Create connection
            conn = adapter.create_connection()

            # Register test data directly (simulating loaded data)
            df = pl.DataFrame(
                {
                    "l_orderkey": [1, 1, 2, 2, 3],
                    "l_partkey": [100, 101, 102, 103, 104],
                    "l_quantity": [10, 20, 30, 40, 50],
                    "l_extendedprice": [1000.0, 2000.0, 3000.0, 4000.0, 5000.0],
                }
            )
            conn.register_table("lineitem", df)

            # Verify table was registered
            assert "lineitem" in conn.get_tables()

            # Get the table and verify contents
            lf = conn.get_table("lineitem")
            assert lf is not None
            result_df = lf.collect()
            assert len(result_df) == 5

    def test_lazy_execution_mode(self):
        """Test lazy execution mode with DataFrame context."""
        from benchbox.platforms.polars_platform import PolarsAdapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = PolarsAdapter(working_dir=tmpdir, execution_mode="lazy")
            conn = adapter.create_connection()

            # Register LazyFrame directly
            lf = pl.LazyFrame(
                {
                    "id": range(1000),
                    "value": range(1000),
                }
            )
            conn.register_table("large_table", lf)

            # Get table and perform DataFrame operations
            table = conn.get_table("large_table")
            assert table is not None

            # Perform aggregation using DataFrame API
            result = table.select(pl.len().alias("cnt")).collect()
            assert result["cnt"][0] == 1000


class TestPolarsAdapterRegistration:
    """Tests for adapter registration in the platforms module."""

    def test_adapter_in_platform_mapping(self):
        """Test that Polars is in the platform mapping."""
        from benchbox.platforms import get_platform_adapter

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = get_platform_adapter(
                "polars",
                working_dir=tmpdir,
                benchmark="tpch",
                scale_factor=1.0,
            )
            assert adapter.platform_name == "Polars"

    def test_list_available_platforms(self):
        """Test that Polars appears in available platforms."""
        from benchbox.platforms import list_available_platforms

        platforms = list_available_platforms()
        assert "polars" in platforms
        assert platforms["polars"] is True

    def test_get_platform_requirements(self):
        """Test getting Polars requirements."""
        from benchbox.platforms import get_platform_requirements

        req = get_platform_requirements("polars")
        assert "polars" in req.lower()
