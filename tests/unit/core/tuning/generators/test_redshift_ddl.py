"""Unit tests for Redshift DDL Generator.

Tests the RedshiftDDLGenerator class for:
- Distribution style (DISTSTYLE ALL/KEY/EVEN/AUTO) generation
- Distribution key (DISTKEY) generation
- Sort key generation (COMPOUND and INTERLEAVED SORTKEY)
- Column encoding (ENCODE) recommendations
- Full CREATE TABLE DDL generation

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from unittest.mock import patch

from benchbox.core.tuning import TuningColumn
from benchbox.core.tuning.ddl_generator import ColumnDefinition, ColumnNullability
from benchbox.core.tuning.generators.redshift import (
    ColumnEncoding,
    DistStyle,
    RedshiftDDLGenerator,
    SortStyle,
    recommend_encoding,
)
from benchbox.core.tuning.interface import TableTuning


class TestColumnEncodingRecommendation:
    """Tests for encoding recommendation logic."""

    def test_numeric_types_get_az64(self) -> None:
        """Test that numeric types get AZ64 encoding."""
        assert recommend_encoding("BIGINT") == ColumnEncoding.AZ64
        assert recommend_encoding("INTEGER") == ColumnEncoding.AZ64
        assert recommend_encoding("SMALLINT") == ColumnEncoding.AZ64
        assert recommend_encoding("DECIMAL(15,2)") == ColumnEncoding.AZ64
        assert recommend_encoding("NUMERIC(10,0)") == ColumnEncoding.AZ64
        assert recommend_encoding("DOUBLE PRECISION") == ColumnEncoding.AZ64
        assert recommend_encoding("FLOAT") == ColumnEncoding.AZ64
        assert recommend_encoding("REAL") == ColumnEncoding.AZ64

    def test_date_types_get_az64(self) -> None:
        """Test that date/time types get AZ64 encoding."""
        assert recommend_encoding("DATE") == ColumnEncoding.AZ64
        assert recommend_encoding("TIMESTAMP") == ColumnEncoding.AZ64
        assert recommend_encoding("TIMESTAMPTZ") == ColumnEncoding.AZ64
        assert recommend_encoding("TIME") == ColumnEncoding.AZ64

    def test_varchar_types_get_lzo(self) -> None:
        """Test that VARCHAR types get LZO encoding."""
        assert recommend_encoding("VARCHAR(100)") == ColumnEncoding.LZO
        assert recommend_encoding("VARCHAR(MAX)") == ColumnEncoding.LZO
        assert recommend_encoding("CHAR(10)") == ColumnEncoding.LZO
        assert recommend_encoding("TEXT") == ColumnEncoding.LZO

    def test_boolean_gets_runlength(self) -> None:
        """Test that BOOLEAN gets RUNLENGTH encoding."""
        assert recommend_encoding("BOOLEAN") == ColumnEncoding.RUNLENGTH
        assert recommend_encoding("BOOL") == ColumnEncoding.RUNLENGTH

    def test_unknown_types_get_auto(self) -> None:
        """Test that unknown types get AUTO encoding."""
        assert recommend_encoding("SUPER") == ColumnEncoding.AUTO
        assert recommend_encoding("GEOMETRY") == ColumnEncoding.AUTO


class TestRedshiftDDLGeneratorBasics:
    """Tests for RedshiftDDLGenerator basic properties."""

    def test_platform_name(self) -> None:
        """Test platform name property."""
        generator = RedshiftDDLGenerator()
        assert generator.platform_name == "redshift"

    def test_supported_tuning_types(self) -> None:
        """Test supported tuning types."""
        generator = RedshiftDDLGenerator()
        assert generator.supports_tuning_type("distribution")
        assert generator.supports_tuning_type("sorting")
        assert generator.supports_tuning_type("clustering")
        assert not generator.supports_tuning_type("partitioning")


class TestDistributionGeneration:
    """Tests for distribution style and key generation."""

    def test_single_distribution_column_creates_distkey(self) -> None:
        """Test that single distribution column creates DISTSTYLE KEY + DISTKEY."""
        generator = RedshiftDDLGenerator()
        table_tuning = TableTuning(
            table_name="lineitem",
            distribution=[TuningColumn(name="l_orderkey", type="BIGINT", order=1)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)
        assert "DISTSTYLE KEY" in clauses.distribute_by
        assert "DISTKEY (l_orderkey)" in clauses.distribute_by

    def test_multiple_distribution_columns_uses_first(self) -> None:
        """Test that multiple distribution columns uses first with warning."""
        generator = RedshiftDDLGenerator()
        table_tuning = TableTuning(
            table_name="orders",
            distribution=[
                TuningColumn(name="o_orderkey", type="BIGINT", order=1),
                TuningColumn(name="o_custkey", type="BIGINT", order=2),
            ],
        )

        with patch("benchbox.core.tuning.generators.redshift.logger") as mock_logger:
            clauses = generator.generate_tuning_clauses(table_tuning)
            mock_logger.warning.assert_called_once()
            assert "single-column" in mock_logger.warning.call_args[0][0]

        assert "DISTKEY (o_orderkey)" in clauses.distribute_by

    def test_diststyle_all_for_small_tables(self) -> None:
        """Test DISTSTYLE ALL generation."""
        generator = RedshiftDDLGenerator(default_dist_style=DistStyle.ALL)
        table_tuning = TableTuning(table_name="region")

        clauses = generator.generate_tuning_clauses(table_tuning)
        assert clauses.distribute_by == "DISTSTYLE ALL"

    def test_diststyle_even(self) -> None:
        """Test DISTSTYLE EVEN generation."""
        generator = RedshiftDDLGenerator(default_dist_style=DistStyle.EVEN)
        table_tuning = TableTuning(table_name="staging")

        clauses = generator.generate_tuning_clauses(table_tuning)
        assert clauses.distribute_by == "DISTSTYLE EVEN"

    def test_diststyle_auto_produces_no_clause(self) -> None:
        """Test that DISTSTYLE AUTO produces no explicit clause."""
        generator = RedshiftDDLGenerator(default_dist_style=DistStyle.AUTO)
        table_tuning = TableTuning(table_name="test")

        clauses = generator.generate_tuning_clauses(table_tuning)
        # AUTO is the default, so no clause is generated
        assert clauses.distribute_by is None


class TestSortKeyGeneration:
    """Tests for sort key generation."""

    def test_compound_sortkey_default(self) -> None:
        """Test COMPOUND SORTKEY generation (default)."""
        generator = RedshiftDDLGenerator()
        table_tuning = TableTuning(
            table_name="lineitem",
            sorting=[
                TuningColumn(name="l_shipdate", type="DATE", order=1),
                TuningColumn(name="l_orderkey", type="BIGINT", order=2),
            ],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)
        assert clauses.sort_by == "COMPOUND SORTKEY (l_shipdate, l_orderkey)"

    def test_interleaved_sortkey(self) -> None:
        """Test INTERLEAVED SORTKEY generation."""
        generator = RedshiftDDLGenerator(default_sort_style=SortStyle.INTERLEAVED)
        table_tuning = TableTuning(
            table_name="sales",
            sorting=[
                TuningColumn(name="sale_date", type="DATE", order=1),
                TuningColumn(name="customer_id", type="BIGINT", order=2),
                TuningColumn(name="product_id", type="BIGINT", order=3),
            ],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)
        assert clauses.sort_by == "INTERLEAVED SORTKEY (sale_date, customer_id, product_id)"

    def test_sort_columns_respects_order(self) -> None:
        """Test that sort columns are sorted by their order property."""
        generator = RedshiftDDLGenerator()
        table_tuning = TableTuning(
            table_name="orders",
            sorting=[
                TuningColumn(name="o_orderkey", type="BIGINT", order=3),
                TuningColumn(name="o_orderdate", type="DATE", order=1),
                TuningColumn(name="o_custkey", type="BIGINT", order=2),
            ],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)
        assert clauses.sort_by == "COMPOUND SORTKEY (o_orderdate, o_custkey, o_orderkey)"

    def test_clustering_maps_to_sortkey(self) -> None:
        """Test that clustering columns are treated as sort columns."""
        generator = RedshiftDDLGenerator()
        table_tuning = TableTuning(
            table_name="events",
            clustering=[TuningColumn(name="event_date", type="DATE", order=1)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)
        assert clauses.sort_by == "COMPOUND SORTKEY (event_date)"

    def test_combined_sorting_and_clustering(self) -> None:
        """Test that sorting and clustering columns are combined."""
        generator = RedshiftDDLGenerator()
        table_tuning = TableTuning(
            table_name="events",
            sorting=[TuningColumn(name="event_date", type="DATE", order=1)],
            clustering=[TuningColumn(name="user_id", type="BIGINT", order=2)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)
        assert clauses.sort_by == "COMPOUND SORTKEY (event_date, user_id)"


class TestPartitioningWarning:
    """Tests for partitioning warning (not supported in Redshift)."""

    def test_partitioning_logs_info(self) -> None:
        """Test that partitioning columns trigger an info message."""
        generator = RedshiftDDLGenerator()
        table_tuning = TableTuning(
            table_name="orders",
            partitioning=[TuningColumn(name="order_date", type="DATE", order=1)],
        )

        with patch("benchbox.core.tuning.generators.redshift.logger") as mock_logger:
            generator.generate_tuning_clauses(table_tuning)
            mock_logger.info.assert_called_once()
            assert "doesn't support native partitioning" in mock_logger.info.call_args[0][0]


class TestColumnDefinitionGeneration:
    """Tests for column definition with ENCODE clause."""

    def test_column_with_auto_encoding(self) -> None:
        """Test column definition with automatic encoding."""
        generator = RedshiftDDLGenerator(auto_encoding=True)
        col = ColumnDefinition("l_orderkey", "BIGINT", ColumnNullability.NOT_NULL)
        definition = generator.generate_column_definition(col)
        assert definition == "l_orderkey BIGINT NOT NULL ENCODE az64"

    def test_column_without_encoding(self) -> None:
        """Test column definition without encoding."""
        generator = RedshiftDDLGenerator(auto_encoding=False)
        col = ColumnDefinition("l_orderkey", "BIGINT", ColumnNullability.NOT_NULL)
        definition = generator.generate_column_definition(col)
        assert definition == "l_orderkey BIGINT NOT NULL"
        assert "ENCODE" not in definition

    def test_varchar_column_gets_lzo(self) -> None:
        """Test VARCHAR column gets LZO encoding."""
        generator = RedshiftDDLGenerator(auto_encoding=True)
        col = ColumnDefinition("l_comment", "VARCHAR(44)")
        definition = generator.generate_column_definition(col)
        assert "ENCODE lzo" in definition


class TestCreateTableDDL:
    """Tests for full CREATE TABLE DDL generation."""

    def test_basic_create_table(self) -> None:
        """Test basic CREATE TABLE without tuning."""
        generator = RedshiftDDLGenerator(auto_encoding=False)
        columns = [
            ColumnDefinition("id", "BIGINT", ColumnNullability.NOT_NULL),
            ColumnDefinition("name", "VARCHAR(100)"),
        ]
        ddl = generator.generate_create_table_ddl("customers", columns)
        assert "CREATE TABLE customers" in ddl
        assert "id BIGINT NOT NULL" in ddl
        assert "name VARCHAR(100)" in ddl
        assert ddl.endswith(";")

    def test_create_table_with_encoding(self) -> None:
        """Test CREATE TABLE with column encoding."""
        generator = RedshiftDDLGenerator(auto_encoding=True)
        columns = [
            ColumnDefinition("id", "BIGINT", ColumnNullability.NOT_NULL),
            ColumnDefinition("name", "VARCHAR(100)"),
        ]
        ddl = generator.generate_create_table_ddl("customers", columns)
        assert "ENCODE az64" in ddl
        assert "ENCODE lzo" in ddl

    def test_create_table_with_distribution(self) -> None:
        """Test CREATE TABLE with DISTSTYLE KEY and DISTKEY."""
        generator = RedshiftDDLGenerator(auto_encoding=False)
        columns = [
            ColumnDefinition("l_orderkey", "BIGINT", ColumnNullability.NOT_NULL),
            ColumnDefinition("l_shipdate", "DATE"),
        ]
        tuning = generator.generate_tuning_clauses(
            TableTuning(
                table_name="lineitem",
                distribution=[TuningColumn(name="l_orderkey", type="BIGINT", order=1)],
            )
        )
        ddl = generator.generate_create_table_ddl("lineitem", columns, tuning=tuning)
        assert "CREATE TABLE lineitem" in ddl
        assert "DISTSTYLE KEY" in ddl
        assert "DISTKEY (l_orderkey)" in ddl

    def test_create_table_with_sortkey(self) -> None:
        """Test CREATE TABLE with SORTKEY."""
        generator = RedshiftDDLGenerator(auto_encoding=False)
        columns = [
            ColumnDefinition("l_orderkey", "BIGINT"),
            ColumnDefinition("l_shipdate", "DATE"),
        ]
        tuning = generator.generate_tuning_clauses(
            TableTuning(
                table_name="lineitem",
                sorting=[
                    TuningColumn(name="l_shipdate", type="DATE", order=1),
                    TuningColumn(name="l_orderkey", type="BIGINT", order=2),
                ],
            )
        )
        ddl = generator.generate_create_table_ddl("lineitem", columns, tuning=tuning)
        assert "COMPOUND SORTKEY (l_shipdate, l_orderkey)" in ddl

    def test_create_table_with_all_tuning(self) -> None:
        """Test CREATE TABLE with distribution and sort key."""
        generator = RedshiftDDLGenerator(auto_encoding=False)
        columns = [
            ColumnDefinition("l_orderkey", "BIGINT", ColumnNullability.NOT_NULL),
            ColumnDefinition("l_shipdate", "DATE"),
            ColumnDefinition("l_quantity", "DECIMAL(15,2)"),
        ]
        tuning = generator.generate_tuning_clauses(
            TableTuning(
                table_name="lineitem",
                distribution=[TuningColumn(name="l_orderkey", type="BIGINT", order=1)],
                sorting=[
                    TuningColumn(name="l_shipdate", type="DATE", order=1),
                    TuningColumn(name="l_orderkey", type="BIGINT", order=2),
                ],
            )
        )
        ddl = generator.generate_create_table_ddl("lineitem", columns, tuning=tuning)

        assert "CREATE TABLE lineitem" in ddl
        assert "DISTSTYLE KEY" in ddl
        assert "DISTKEY (l_orderkey)" in ddl
        assert "COMPOUND SORTKEY (l_shipdate, l_orderkey)" in ddl
        assert ddl.endswith(";")

    def test_create_table_if_not_exists(self) -> None:
        """Test CREATE TABLE IF NOT EXISTS."""
        generator = RedshiftDDLGenerator(auto_encoding=False)
        columns = [ColumnDefinition("id", "BIGINT")]
        ddl = generator.generate_create_table_ddl("test", columns, if_not_exists=True)
        assert "CREATE TABLE IF NOT EXISTS test" in ddl

    def test_create_table_with_schema(self) -> None:
        """Test CREATE TABLE with schema prefix."""
        generator = RedshiftDDLGenerator(auto_encoding=False)
        columns = [ColumnDefinition("id", "BIGINT")]
        ddl = generator.generate_create_table_ddl("orders", columns, schema="tpch")
        assert "CREATE TABLE tpch.orders" in ddl


class TestCopyCommand:
    """Tests for COPY command generation."""

    def test_basic_copy_command(self) -> None:
        """Test basic COPY command for loading from S3."""
        generator = RedshiftDDLGenerator()
        copy_sql = generator.generate_copy_command(
            table_name="lineitem",
            s3_path="s3://my-bucket/tpch/lineitem/",
            iam_role="arn:aws:iam::123456789:role/RedshiftLoadRole",
        )
        assert "COPY lineitem" in copy_sql
        assert "FROM 's3://my-bucket/tpch/lineitem/'" in copy_sql
        assert "IAM_ROLE 'arn:aws:iam::123456789:role/RedshiftLoadRole'" in copy_sql
        assert "FORMAT AS PARQUET" in copy_sql
        assert copy_sql.endswith(";")

    def test_copy_command_with_schema(self) -> None:
        """Test COPY command with schema-qualified table."""
        generator = RedshiftDDLGenerator()
        copy_sql = generator.generate_copy_command(
            table_name="lineitem",
            s3_path="s3://bucket/data/",
            iam_role="arn:aws:iam::123:role/Role",
            schema="tpch",
        )
        assert "COPY tpch.lineitem" in copy_sql

    def test_copy_command_csv_format(self) -> None:
        """Test COPY command with CSV format."""
        generator = RedshiftDDLGenerator()
        copy_sql = generator.generate_copy_command(
            table_name="data",
            s3_path="s3://bucket/csv/",
            iam_role="arn:aws:iam::123:role/Role",
            file_format="CSV",
        )
        assert "FORMAT AS CSV" in copy_sql


class TestEnums:
    """Tests for enum definitions."""

    def test_dist_style_values(self) -> None:
        """Test DistStyle enum values."""
        assert DistStyle.ALL.value == "ALL"
        assert DistStyle.KEY.value == "KEY"
        assert DistStyle.EVEN.value == "EVEN"
        assert DistStyle.AUTO.value == "AUTO"

    def test_sort_style_values(self) -> None:
        """Test SortStyle enum values."""
        assert SortStyle.COMPOUND.value == "COMPOUND"
        assert SortStyle.INTERLEAVED.value == "INTERLEAVED"

    def test_encoding_values(self) -> None:
        """Test ColumnEncoding enum values."""
        assert ColumnEncoding.AZ64.value == "az64"
        assert ColumnEncoding.LZO.value == "lzo"
        assert ColumnEncoding.ZSTD.value == "zstd"
        assert ColumnEncoding.AUTO.value == "AUTO"
