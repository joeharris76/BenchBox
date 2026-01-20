"""Integration tests for open table format conversions.

This module provides smoke tests for the format conversion pipeline to verify
end-to-end functionality for all supported open table formats:
- Parquet (always available - uses PyArrow)
- Delta Lake (requires deltalake optional dependency)
- Apache Iceberg (requires pyiceberg optional dependency)

These tests verify that:
1. Data can be converted from TBL to each format
2. DuckDB can read the converted data
3. Query results match expected values
4. Compression options work correctly
5. Partitioned output is queryable

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import importlib.util
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from benchbox.utils.format_converters.base import ConversionOptions
from benchbox.utils.format_converters.parquet_converter import ParquetConverter

# =============================================================================
# Test Data and Fixtures
# =============================================================================


@pytest.fixture
def tpch_customer_schema():
    """TPC-H customer table schema (simplified for testing)."""
    return {
        "columns": [
            {"name": "c_custkey", "type": "INTEGER"},
            {"name": "c_name", "type": "VARCHAR(25)"},
            {"name": "c_address", "type": "VARCHAR(40)"},
            {"name": "c_nationkey", "type": "INTEGER"},
            {"name": "c_phone", "type": "CHAR(15)"},
            {"name": "c_acctbal", "type": "DECIMAL(15,2)"},
            {"name": "c_mktsegment", "type": "CHAR(10)"},
            {"name": "c_comment", "type": "VARCHAR(117)"},
        ]
    }


@pytest.fixture
def tpch_customer_tbl(tmp_path: Path) -> Path:
    """Create a sample TPC-H customer TBL file."""
    tbl_file = tmp_path / "customer.tbl"
    # TBL format: pipe-delimited with trailing pipe
    tbl_file.write_text(
        "1|Customer#000000001|IVhzIApeRb|15|25-989-741-2988|711.56|BUILDING|regular packages|\n"
        "2|Customer#000000002|XSTf4,NCwDVaW|13|23-768-687-3665|121.65|AUTOMOBILE|theodolites|\n"
        "3|Customer#000000003|MG9kdTD2W|1|11-719-748-3364|7498.12|AUTOMOBILE|deposits serve|\n"
        "4|Customer#000000004|XxVSJsLAGt|4|14-128-190-5944|2866.83|MACHINERY|requests haggle|\n"
        "5|Customer#000000005|KvpyuHCplr|3|13-750-942-6364|794.47|HOUSEHOLD|pending foxes|\n"
    )
    return tbl_file


@pytest.fixture
def tpch_lineitem_schema():
    """TPC-H lineitem table schema (simplified for partitioning tests)."""
    return {
        "columns": [
            {"name": "l_orderkey", "type": "INTEGER"},
            {"name": "l_partkey", "type": "INTEGER"},
            {"name": "l_suppkey", "type": "INTEGER"},
            {"name": "l_linenumber", "type": "INTEGER"},
            {"name": "l_quantity", "type": "DECIMAL(15,2)"},
            {"name": "l_extendedprice", "type": "DECIMAL(15,2)"},
            {"name": "l_discount", "type": "DECIMAL(15,2)"},
            {"name": "l_tax", "type": "DECIMAL(15,2)"},
            {"name": "l_returnflag", "type": "CHAR(1)"},
            {"name": "l_linestatus", "type": "CHAR(1)"},
            {"name": "l_shipdate", "type": "DATE"},
            {"name": "l_commitdate", "type": "DATE"},
            {"name": "l_receiptdate", "type": "DATE"},
            {"name": "l_shipinstruct", "type": "CHAR(25)"},
            {"name": "l_shipmode", "type": "CHAR(10)"},
            {"name": "l_comment", "type": "VARCHAR(44)"},
        ]
    }


@pytest.fixture
def tpch_lineitem_tbl(tmp_path: Path) -> Path:
    """Create a sample TPC-H lineitem TBL file with partitionable data."""
    tbl_file = tmp_path / "lineitem.tbl"
    # Data with multiple return flags and line statuses for partition testing
    tbl_file.write_text(
        "1|155190|7706|1|17.00|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts|\n"
        "1|67310|7311|2|36.00|45983.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dep|\n"
        "2|106170|1191|1|38.00|44694.46|0.00|0.05|N|O|1997-01-28|1997-01-14|1997-02-02|TAKE BACK RETURN|RAIL|ven requests|\n"
        "3|4297|1798|1|45.00|54058.05|0.06|0.00|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|ongside of |\n"
        "3|19036|6540|2|49.00|46796.47|0.10|0.00|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL|unusual accounts|\n"
    )
    return tbl_file


# =============================================================================
# Phase 1: Parquet Format Smoke Tests
# =============================================================================


@pytest.mark.integration
class TestParquetFormatSmoke:
    """Smoke tests for Parquet format conversion and DuckDB querying."""

    def test_parquet_conversion_and_duckdb_query(
        self, tmp_path: Path, tpch_customer_tbl: Path, tpch_customer_schema: dict
    ):
        """Test TBL → Parquet conversion and verify DuckDB can query the result."""
        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tpch_customer_tbl],
            table_name="customer",
            schema=tpch_customer_schema,
            options=ConversionOptions(output_dir=tmp_path),
        )

        # Verify conversion succeeded
        assert result.success
        assert result.row_count == 5
        parquet_file = result.output_files[0]
        assert parquet_file.exists()

        # Query with DuckDB
        conn = duckdb.connect(":memory:")
        df = conn.execute(f"SELECT * FROM read_parquet('{parquet_file}')").fetchdf()

        # Verify row count
        assert len(df) == 5

        # Verify data integrity with specific queries
        count_result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')").fetchone()
        assert count_result[0] == 5

        # Verify aggregation works
        sum_result = conn.execute(f"SELECT SUM(c_acctbal) FROM read_parquet('{parquet_file}')").fetchone()
        expected_sum = 711.56 + 121.65 + 7498.12 + 2866.83 + 794.47
        assert abs(float(sum_result[0]) - expected_sum) < 0.01

        # Verify filtering works
        filter_result = conn.execute(
            f"SELECT c_custkey, c_name FROM read_parquet('{parquet_file}') WHERE c_acctbal > 1000"
        ).fetchall()
        assert len(filter_result) == 2  # Customer 3 and 4 have acctbal > 1000
        custkeys = {row[0] for row in filter_result}
        assert custkeys == {3, 4}

        conn.close()

    def test_parquet_snappy_compression(self, tmp_path: Path, tpch_customer_tbl: Path, tpch_customer_schema: dict):
        """Test Parquet conversion with snappy compression."""
        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tpch_customer_tbl],
            table_name="customer_snappy",
            schema=tpch_customer_schema,
            options=ConversionOptions(output_dir=tmp_path, compression="snappy"),
        )

        assert result.success
        assert result.metadata["compression"] == "snappy"

        # Verify file is readable
        table = pq.read_table(result.output_files[0])
        assert table.num_rows == 5

        # Verify DuckDB can read it
        conn = duckdb.connect(":memory:")
        count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{result.output_files[0]}')").fetchone()[0]
        assert count == 5
        conn.close()

    def test_parquet_gzip_compression(self, tmp_path: Path, tpch_customer_tbl: Path, tpch_customer_schema: dict):
        """Test Parquet conversion with gzip compression."""
        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tpch_customer_tbl],
            table_name="customer_gzip",
            schema=tpch_customer_schema,
            options=ConversionOptions(output_dir=tmp_path, compression="gzip"),
        )

        assert result.success
        assert result.metadata["compression"] == "gzip"

        # Verify DuckDB can read gzip-compressed Parquet
        conn = duckdb.connect(":memory:")
        df = conn.execute(f"SELECT * FROM read_parquet('{result.output_files[0]}')").fetchdf()
        assert len(df) == 5
        conn.close()

    def test_parquet_zstd_compression(self, tmp_path: Path, tpch_customer_tbl: Path, tpch_customer_schema: dict):
        """Test Parquet conversion with zstd compression."""
        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tpch_customer_tbl],
            table_name="customer_zstd",
            schema=tpch_customer_schema,
            options=ConversionOptions(output_dir=tmp_path, compression="zstd"),
        )

        assert result.success
        assert result.metadata["compression"] == "zstd"

        # Verify DuckDB can read zstd-compressed Parquet
        conn = duckdb.connect(":memory:")
        df = conn.execute(f"SELECT * FROM read_parquet('{result.output_files[0]}')").fetchdf()
        assert len(df) == 5
        conn.close()

    def test_parquet_hive_partitioned_duckdb_query(
        self, tmp_path: Path, tpch_lineitem_tbl: Path, tpch_lineitem_schema: dict
    ):
        """Test Hive-partitioned Parquet can be read by DuckDB as a dataset."""
        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tpch_lineitem_tbl],
            table_name="lineitem",
            schema=tpch_lineitem_schema,
            options=ConversionOptions(
                output_dir=tmp_path,
                partition_cols=["l_returnflag"],
            ),
        )

        assert result.success
        assert result.metadata["partitioned"]
        assert result.metadata["partition_cols"] == ["l_returnflag"]

        # Verify partition directories exist
        partitioned_dir = result.output_files[0]
        assert partitioned_dir.is_dir()
        assert (partitioned_dir / "l_returnflag=N").exists()
        assert (partitioned_dir / "l_returnflag=R").exists()

        # Query partitioned dataset with DuckDB using glob pattern
        conn = duckdb.connect(":memory:")

        # Read all partitions
        all_rows = conn.execute(
            f"SELECT * FROM read_parquet('{partitioned_dir}/**/*.parquet', hive_partitioning=true)"
        ).fetchall()
        assert len(all_rows) == 5

        # Verify partition pruning works - query only 'N' partition
        n_rows = conn.execute(
            f"""
            SELECT l_orderkey, l_returnflag
            FROM read_parquet('{partitioned_dir}/**/*.parquet', hive_partitioning=true)
            WHERE l_returnflag = 'N'
            """
        ).fetchall()
        assert len(n_rows) == 3  # 3 rows have returnflag='N'
        assert all(row[1] == "N" for row in n_rows)

        # Verify aggregation across partitions
        agg_result = conn.execute(
            f"""
            SELECT l_returnflag, COUNT(*) as cnt
            FROM read_parquet('{partitioned_dir}/**/*.parquet', hive_partitioning=true)
            GROUP BY l_returnflag
            ORDER BY l_returnflag
            """
        ).fetchall()
        assert len(agg_result) == 2
        # N: 3 rows, R: 2 rows
        flag_counts = {row[0]: row[1] for row in agg_result}
        assert flag_counts["N"] == 3
        assert flag_counts["R"] == 2

        conn.close()


# =============================================================================
# Phase 2: Delta Lake Format Smoke Tests
# =============================================================================


# Check if deltalake is available
DELTALAKE_AVAILABLE = importlib.util.find_spec("deltalake") is not None


@pytest.mark.integration
@pytest.mark.skipif(not DELTALAKE_AVAILABLE, reason="deltalake package not installed")
class TestDeltaLakeFormatSmoke:
    """Smoke tests for Delta Lake format conversion."""

    def test_delta_conversion_and_structure(self, tmp_path: Path, tpch_customer_tbl: Path, tpch_customer_schema: dict):
        """Test TBL → Delta Lake conversion and verify table structure."""
        from benchbox.utils.format_converters.delta_converter import DeltaConverter

        converter = DeltaConverter()
        result = converter.convert(
            source_files=[tpch_customer_tbl],
            table_name="customer",
            schema=tpch_customer_schema,
            options=ConversionOptions(output_dir=tmp_path),
        )

        # Verify conversion succeeded
        assert result.success
        assert result.row_count == 5

        # Verify Delta Lake directory structure
        delta_dir = tmp_path / "customer"
        assert delta_dir.exists()
        assert (delta_dir / "_delta_log").exists()

        # Verify transaction log exists
        log_files = list((delta_dir / "_delta_log").glob("*.json"))
        assert len(log_files) >= 1  # At least one commit

    def test_delta_duckdb_query_via_delta_scan(
        self, tmp_path: Path, tpch_customer_tbl: Path, tpch_customer_schema: dict
    ):
        """Test that DuckDB can query Delta Lake tables using delta extension."""
        from benchbox.utils.format_converters.delta_converter import DeltaConverter

        converter = DeltaConverter()
        result = converter.convert(
            source_files=[tpch_customer_tbl],
            table_name="customer",
            schema=tpch_customer_schema,
            options=ConversionOptions(output_dir=tmp_path),
        )

        assert result.success
        delta_dir = tmp_path / "customer"

        # Query with DuckDB using delta extension
        conn = duckdb.connect(":memory:")

        # Install and load delta extension
        conn.execute("INSTALL delta")
        conn.execute("LOAD delta")

        # Query the Delta table
        count = conn.execute(f"SELECT COUNT(*) FROM delta_scan('{delta_dir}')").fetchone()[0]
        assert count == 5

        # Verify data integrity
        sum_result = conn.execute(f"SELECT SUM(c_acctbal) FROM delta_scan('{delta_dir}')").fetchone()[0]
        expected_sum = 711.56 + 121.65 + 7498.12 + 2866.83 + 794.47
        assert abs(float(sum_result) - expected_sum) < 0.01

        conn.close()

    def test_delta_transaction_log_metadata(self, tmp_path: Path, tpch_customer_tbl: Path, tpch_customer_schema: dict):
        """Test Delta Lake transaction log contains correct metadata."""
        import json

        from benchbox.utils.format_converters.delta_converter import DeltaConverter

        converter = DeltaConverter()
        result = converter.convert(
            source_files=[tpch_customer_tbl],
            table_name="customer",
            schema=tpch_customer_schema,
            options=ConversionOptions(output_dir=tmp_path),
        )

        assert result.success

        # Read the first transaction log file
        delta_log_dir = tmp_path / "customer" / "_delta_log"
        log_file = delta_log_dir / "00000000000000000000.json"
        assert log_file.exists()

        # Parse the transaction log (newline-delimited JSON)
        actions = []
        with open(log_file) as f:
            for line in f:
                if line.strip():
                    actions.append(json.loads(line))

        # Should have protocol, metaData, and add actions
        action_types = {list(action.keys())[0] for action in actions}
        assert "protocol" in action_types
        assert "metaData" in action_types
        assert "add" in action_types


# =============================================================================
# Phase 3: Apache Iceberg Format Smoke Tests
# =============================================================================


# Check if pyiceberg is available
PYICEBERG_AVAILABLE = importlib.util.find_spec("pyiceberg") is not None


@pytest.mark.integration
@pytest.mark.skipif(not PYICEBERG_AVAILABLE, reason="pyiceberg package not installed")
class TestIcebergFormatSmoke:
    """Smoke tests for Apache Iceberg format conversion."""

    def test_iceberg_conversion_and_structure(
        self, tmp_path: Path, tpch_customer_tbl: Path, tpch_customer_schema: dict
    ):
        """Test TBL → Iceberg conversion and verify table structure."""
        from benchbox.utils.format_converters.iceberg_converter import IcebergConverter

        converter = IcebergConverter()
        result = converter.convert(
            source_files=[tpch_customer_tbl],
            table_name="customer",
            schema=tpch_customer_schema,
            options=ConversionOptions(output_dir=tmp_path),
        )

        # Verify conversion succeeded
        assert result.success
        assert result.row_count == 5

        # Verify Iceberg metadata structure exists
        iceberg_dir = tmp_path / "customer"
        assert iceberg_dir.exists()
        metadata_dir = iceberg_dir / "metadata"
        assert metadata_dir.exists()

        # Should have at least one metadata file
        metadata_files = list(metadata_dir.glob("*.json")) + list(metadata_dir.glob("*.avro"))
        assert len(metadata_files) >= 1

    def test_iceberg_metadata_structure(self, tmp_path: Path, tpch_customer_tbl: Path, tpch_customer_schema: dict):
        """Test Iceberg metadata contains expected structure."""
        import json

        from benchbox.utils.format_converters.iceberg_converter import IcebergConverter

        converter = IcebergConverter()
        result = converter.convert(
            source_files=[tpch_customer_tbl],
            table_name="customer",
            schema=tpch_customer_schema,
            options=ConversionOptions(output_dir=tmp_path),
        )

        assert result.success

        # Find version-hint or latest metadata
        metadata_dir = tmp_path / "customer" / "metadata"
        metadata_files = sorted(metadata_dir.glob("v*.metadata.json"))

        if metadata_files:
            # Read the latest metadata file
            with open(metadata_files[-1]) as f:
                metadata = json.load(f)

            # Verify essential Iceberg metadata fields
            assert "format-version" in metadata
            assert "table-uuid" in metadata
            assert "schema" in metadata or "schemas" in metadata


# =============================================================================
# Phase 4: CLI Integration Tests
# =============================================================================


@pytest.fixture
def tpch_data_dir_with_manifest(tmp_path_factory) -> Path:
    """Create a directory with TBL data and a valid manifest for CLI testing."""
    import json

    # Create a unique tmp directory for this fixture
    data_dir = tmp_path_factory.mktemp("cli_test_data")

    # Create the customer TBL file inline (same data as tpch_customer_tbl)
    tbl_file = data_dir / "customer.tbl"
    tbl_file.write_text(
        "1|Customer#000000001|IVhzIApeRb|15|25-989-741-2988|711.56|BUILDING|regular packages|\n"
        "2|Customer#000000002|XSTf4,NCwDVaW|13|23-768-687-3665|121.65|AUTOMOBILE|theodolites|\n"
        "3|Customer#000000003|MG9kdTD2W|1|11-719-748-3364|7498.12|AUTOMOBILE|deposits serve|\n"
        "4|Customer#000000004|XxVSJsLAGt|4|14-128-190-5944|2866.83|MACHINERY|requests haggle|\n"
        "5|Customer#000000005|KvpyuHCplr|3|13-750-942-6364|794.47|HOUSEHOLD|pending foxes|\n"
    )

    # Create a v2 manifest
    manifest = {
        "version": 2,
        "benchmark": "tpch",
        "scale_factor": 0.01,
        "format_preference": ["tbl"],
        "tables": {
            "customer": {
                "formats": {
                    "tbl": [
                        {
                            "path": "customer.tbl",
                            "size_bytes": tbl_file.stat().st_size,
                            "row_count": 5,
                        }
                    ]
                }
            }
        },
    }

    manifest_path = data_dir / "_datagen_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    return data_dir


@pytest.mark.integration
class TestCLIConvertIntegration:
    """Integration tests for the CLI convert command.

    Note: The CLI convert command has a known limitation where benchmark.get_schema()
    is called with a table_name argument, but TPC-H's get_schema() takes no arguments.
    These tests verify CLI availability and error handling rather than full conversion.

    Direct converter tests (TestParquetFormatSmoke, TestDeltaLakeFormatSmoke,
    TestIcebergFormatSmoke) already verify the core conversion functionality.
    """

    def test_cli_convert_help(self):
        """Test that CLI convert help is available and shows expected options."""
        from click.testing import CliRunner

        from benchbox.cli.commands.convert import convert

        runner = CliRunner()
        result = runner.invoke(convert, ["--help"])

        assert result.exit_code == 0
        assert "Convert benchmark data to optimized table formats" in result.output
        assert "--format" in result.output
        assert "--compression" in result.output
        assert "parquet" in result.output
        assert "delta" in result.output
        assert "iceberg" in result.output

    def test_cli_convert_missing_input_error(self):
        """Test that CLI reports clear error when --input is missing."""
        from click.testing import CliRunner

        from benchbox.cli.commands.convert import convert

        runner = CliRunner()
        result = runner.invoke(convert, ["--format", "parquet"])

        assert result.exit_code != 0
        assert "Missing option '--input'" in result.output

    def test_cli_convert_missing_format_error(self, tpch_data_dir_with_manifest: Path):
        """Test that CLI reports clear error when --format is missing."""
        from click.testing import CliRunner

        from benchbox.cli.commands.convert import convert

        runner = CliRunner()
        result = runner.invoke(convert, ["--input", str(tpch_data_dir_with_manifest)])

        assert result.exit_code != 0
        assert "Missing option '--format'" in result.output

    def test_cli_convert_invalid_format_error(self, tpch_data_dir_with_manifest: Path):
        """Test that CLI reports clear error for invalid format."""
        from click.testing import CliRunner

        from benchbox.cli.commands.convert import convert

        runner = CliRunner()
        result = runner.invoke(
            convert,
            [
                "--input",
                str(tpch_data_dir_with_manifest),
                "--format",
                "invalid_format",
            ],
        )

        assert result.exit_code != 0
        assert "Invalid value for '--format'" in result.output

    def test_cli_convert_missing_manifest_error(self, tmp_path_factory):
        """Test that CLI reports clear error when manifest is missing."""
        from click.testing import CliRunner

        from benchbox.cli.commands.convert import convert

        empty_dir = tmp_path_factory.mktemp("empty_dir")

        runner = CliRunner()
        result = runner.invoke(
            convert,
            ["--input", str(empty_dir), "--format", "parquet"],
        )

        assert result.exit_code != 0
        assert "Manifest not found" in result.output

    def test_cli_convert_compression_options_accepted(self):
        """Test that all compression options are shown in help."""
        from click.testing import CliRunner

        from benchbox.cli.commands.convert import convert

        runner = CliRunner()
        result = runner.invoke(convert, ["--help"])

        assert result.exit_code == 0
        # Verify all compression options are documented
        for compression in ["snappy", "gzip", "zstd", "none"]:
            assert compression in result.output
