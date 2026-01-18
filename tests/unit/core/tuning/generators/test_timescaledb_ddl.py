"""Unit tests for TimescaleDB DDL Generator.

Tests the TimescaleDBDDLGenerator class for:
- create_hypertable() generation
- Chunk interval configuration
- Compression settings and policies

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from benchbox.core.tuning import TuningColumn
from benchbox.core.tuning.ddl_generator import ColumnDefinition, ColumnNullability
from benchbox.core.tuning.generators.timescaledb import TimescaleDBDDLGenerator
from benchbox.core.tuning.interface import TableTuning


class TestTimescaleDBDDLGeneratorBasics:
    """Tests for TimescaleDBDDLGenerator basic properties."""

    def test_platform_name(self) -> None:
        """Test platform name property."""
        generator = TimescaleDBDDLGenerator()
        assert generator.platform_name == "timescaledb"

    def test_inherits_postgresql_tuning_types(self) -> None:
        """Test that PostgreSQL tuning types are inherited."""
        generator = TimescaleDBDDLGenerator()
        assert generator.supports_tuning_type("partitioning")
        assert generator.supports_tuning_type("clustering")
        assert generator.supports_tuning_type("sorting")


class TestHypertableGeneration:
    """Tests for create_hypertable() generation."""

    def test_basic_hypertable(self) -> None:
        """Test basic hypertable creation."""
        generator = TimescaleDBDDLGenerator()
        table_tuning = TableTuning(
            table_name="metrics",
            partitioning=[TuningColumn(name="time", type="TIMESTAMPTZ", order=1)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)

        assert len(clauses.post_create_statements) == 1
        stmt = clauses.post_create_statements[0]
        assert "create_hypertable('{table_name}', 'time'" in stmt
        assert "chunk_time_interval" in stmt

    def test_custom_chunk_interval(self) -> None:
        """Test custom chunk interval."""
        generator = TimescaleDBDDLGenerator(default_chunk_interval="INTERVAL '1 week'")
        table_tuning = TableTuning(
            table_name="events",
            partitioning=[TuningColumn(name="event_time", type="TIMESTAMPTZ", order=1)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)

        assert "INTERVAL '1 week'" in clauses.post_create_statements[0]

    def test_space_partitioning(self) -> None:
        """Test space partitioning with distribution columns."""
        generator = TimescaleDBDDLGenerator()
        table_tuning = TableTuning(
            table_name="metrics",
            partitioning=[TuningColumn(name="time", type="TIMESTAMPTZ", order=1)],
            distribution=[TuningColumn(name="device_id", type="INTEGER", order=1)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)

        stmt = clauses.post_create_statements[0]
        assert "partitioning_column => 'device_id'" in stmt
        assert "number_partitions => 4" in stmt

    def test_no_hypertable_without_partitioning(self) -> None:
        """Test that no hypertable is created without partitioning."""
        generator = TimescaleDBDDLGenerator()
        table_tuning = TableTuning(
            table_name="metrics",
            clustering=[TuningColumn(name="device_id", type="INTEGER", order=1)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)

        assert len(clauses.post_create_statements) == 0


class TestCompressionGeneration:
    """Tests for compression configuration generation."""

    def test_compression_disabled_by_default(self) -> None:
        """Test that compression is disabled by default."""
        generator = TimescaleDBDDLGenerator()
        table_tuning = TableTuning(
            table_name="metrics",
            partitioning=[TuningColumn(name="time", type="TIMESTAMPTZ", order=1)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)

        # Only hypertable creation, no compression
        assert len(clauses.post_create_statements) == 1
        assert "compress" not in clauses.post_create_statements[0].lower()

    def test_compression_enabled(self) -> None:
        """Test compression when enabled."""
        generator = TimescaleDBDDLGenerator(enable_compression=True)
        table_tuning = TableTuning(
            table_name="metrics",
            partitioning=[TuningColumn(name="time", type="TIMESTAMPTZ", order=1)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)

        # Hypertable + compression settings + compression policy
        assert len(clauses.post_create_statements) == 3
        assert "timescaledb.compress" in clauses.post_create_statements[1]
        assert "add_compression_policy" in clauses.post_create_statements[2]

    def test_compression_with_segmentby(self) -> None:
        """Test compression with segmentby columns."""
        generator = TimescaleDBDDLGenerator(enable_compression=True)
        table_tuning = TableTuning(
            table_name="metrics",
            partitioning=[TuningColumn(name="time", type="TIMESTAMPTZ", order=1)],
            distribution=[TuningColumn(name="device_id", type="INTEGER", order=1)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)

        alter_stmt = clauses.post_create_statements[1]
        assert "compress_segmentby = 'device_id'" in alter_stmt

    def test_compression_with_orderby(self) -> None:
        """Test compression with orderby columns."""
        generator = TimescaleDBDDLGenerator(enable_compression=True)
        table_tuning = TableTuning(
            table_name="metrics",
            partitioning=[TuningColumn(name="time", type="TIMESTAMPTZ", order=1)],
            sorting=[
                TuningColumn(name="value", type="DOUBLE PRECISION", order=1, sort_order="DESC"),
            ],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)

        alter_stmt = clauses.post_create_statements[1]
        assert "compress_orderby = 'value DESC'" in alter_stmt

    def test_custom_compression_after(self) -> None:
        """Test custom compression policy interval."""
        generator = TimescaleDBDDLGenerator(
            enable_compression=True,
            compression_after="INTERVAL '30 days'",
        )
        table_tuning = TableTuning(
            table_name="metrics",
            partitioning=[TuningColumn(name="time", type="TIMESTAMPTZ", order=1)],
        )
        clauses = generator.generate_tuning_clauses(table_tuning)

        policy_stmt = clauses.post_create_statements[2]
        assert "INTERVAL '30 days'" in policy_stmt


class TestPartitionChildren:
    """Tests for partition children (not used in TimescaleDB)."""

    def test_no_partition_children(self) -> None:
        """Test that TimescaleDB doesn't generate partition children."""
        generator = TimescaleDBDDLGenerator()
        table_tuning = TableTuning(
            table_name="metrics",
            partitioning=[TuningColumn(name="time", type="TIMESTAMPTZ", order=1)],
        )
        columns = [ColumnDefinition("time", "TIMESTAMPTZ")]
        tuning = generator.generate_tuning_clauses(table_tuning)

        children = generator.generate_partition_children("metrics", columns, tuning, table_tuning)

        assert len(children) == 0


class TestCreateTableDDL:
    """Tests for CREATE TABLE DDL generation."""

    def test_basic_create_table(self) -> None:
        """Test basic CREATE TABLE (inherited from PostgreSQL)."""
        generator = TimescaleDBDDLGenerator()
        columns = [
            ColumnDefinition("time", "TIMESTAMPTZ", ColumnNullability.NOT_NULL),
            ColumnDefinition("device_id", "INTEGER", ColumnNullability.NOT_NULL),
            ColumnDefinition("temperature", "DOUBLE PRECISION"),
        ]
        ddl = generator.generate_create_table_ddl("metrics", columns)
        assert "CREATE TABLE metrics" in ddl
        assert "time TIMESTAMPTZ NOT NULL" in ddl
        assert ddl.endswith(";")

    def test_full_workflow(self) -> None:
        """Test complete workflow: CREATE TABLE + hypertable statements."""
        generator = TimescaleDBDDLGenerator(
            default_chunk_interval="INTERVAL '1 day'",
            enable_compression=True,
        )
        columns = [
            ColumnDefinition("time", "TIMESTAMPTZ", ColumnNullability.NOT_NULL),
            ColumnDefinition("device_id", "INTEGER"),
            ColumnDefinition("value", "DOUBLE PRECISION"),
        ]
        table_tuning = TableTuning(
            table_name="metrics",
            partitioning=[TuningColumn(name="time", type="TIMESTAMPTZ", order=1)],
            distribution=[TuningColumn(name="device_id", type="INTEGER", order=1)],
        )

        # Get DDL and tuning
        tuning = generator.generate_tuning_clauses(table_tuning)
        ddl = generator.generate_create_table_ddl("metrics", columns)

        # Verify CREATE TABLE
        assert "CREATE TABLE metrics" in ddl

        # Verify post-create statements
        assert len(tuning.post_create_statements) == 3

        # Hypertable with space partitioning
        assert "create_hypertable" in tuning.post_create_statements[0]
        assert "device_id" in tuning.post_create_statements[0]

        # Compression
        assert "timescaledb.compress" in tuning.post_create_statements[1]
        assert "add_compression_policy" in tuning.post_create_statements[2]
