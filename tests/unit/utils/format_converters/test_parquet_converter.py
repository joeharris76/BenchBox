"""Tests for Parquet format converter."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from benchbox.utils.format_converters.base import (
    ConversionError,
    ConversionOptions,
    SchemaError,
)
from benchbox.utils.format_converters.parquet_converter import ParquetConverter

pytestmark = pytest.mark.fast


class TestParquetConverterBasics:
    """Tests for ParquetConverter basic functionality."""

    def test_get_file_extension(self):
        """Test file extension for Parquet format."""
        converter = ParquetConverter()
        assert converter.get_file_extension() == ".parquet"

    def test_get_format_name(self):
        """Test human-readable format name."""
        converter = ParquetConverter()
        assert converter.get_format_name() == "Parquet"


class TestParquetSchemaValidation:
    """Tests for schema validation."""

    def test_valid_schema(self):
        """Test validation of a valid schema."""
        converter = ParquetConverter()
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR(100)"},
            ]
        }
        assert converter.validate_schema(schema) is True

    def test_empty_schema(self):
        """Test validation of empty schema."""
        converter = ParquetConverter()
        with pytest.raises(SchemaError, match="Schema is empty or None"):
            converter.validate_schema({})

    def test_none_schema(self):
        """Test validation of None schema."""
        converter = ParquetConverter()
        with pytest.raises(SchemaError, match="Schema is empty or None"):
            converter.validate_schema(None)

    def test_schema_missing_columns(self):
        """Test validation of schema missing columns field."""
        converter = ParquetConverter()
        schema = {"tables": []}
        with pytest.raises(SchemaError, match="Schema missing 'columns' field"):
            converter.validate_schema(schema)

    def test_schema_empty_columns(self):
        """Test validation of schema with empty columns list."""
        converter = ParquetConverter()
        schema = {"columns": []}
        with pytest.raises(SchemaError, match="'columns' must be a non-empty list"):
            converter.validate_schema(schema)

    def test_schema_columns_not_list(self):
        """Test validation of schema with non-list columns."""
        converter = ParquetConverter()
        schema = {"columns": "not a list"}
        with pytest.raises(SchemaError, match="'columns' must be a non-empty list"):
            converter.validate_schema(schema)

    def test_column_not_dict(self):
        """Test validation of column that is not a dict."""
        converter = ParquetConverter()
        schema = {"columns": ["not a dict"]}
        with pytest.raises(SchemaError, match="Column 0 is not a dictionary"):
            converter.validate_schema(schema)

    def test_column_missing_name(self):
        """Test validation of column missing name field."""
        converter = ParquetConverter()
        schema = {"columns": [{"type": "INTEGER"}]}
        with pytest.raises(SchemaError, match="Column 0 missing 'name' field"):
            converter.validate_schema(schema)

    def test_column_missing_type(self):
        """Test validation of column missing type field."""
        converter = ParquetConverter()
        schema = {"columns": [{"name": "id"}]}
        with pytest.raises(SchemaError, match="Column 0 \\(id\\) missing 'type' field"):
            converter.validate_schema(schema)


class TestSqlTypeMapping:
    """Tests for SQL type to Arrow type mapping."""

    def test_integer_mapping(self):
        """Test INTEGER type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("INTEGER")
        assert arrow_type == pa.int64()

    def test_int_mapping(self):
        """Test INT type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("INT")
        assert arrow_type == pa.int64()

    def test_bigint_mapping(self):
        """Test BIGINT type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("BIGINT")
        assert arrow_type == pa.int64()

    def test_decimal_with_params_mapping(self):
        """Test DECIMAL(15,2) type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("DECIMAL(15,2)")
        assert arrow_type == pa.decimal128(15, 2)

    def test_decimal_with_one_param_mapping(self):
        """Test DECIMAL(10) type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("DECIMAL(10)")
        assert arrow_type == pa.decimal128(10, 0)

    def test_decimal_without_params_mapping(self):
        """Test DECIMAL type mapping without parameters."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("DECIMAL")
        assert arrow_type == pa.decimal128(15, 2)

    def test_date_mapping(self):
        """Test DATE type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("DATE")
        assert arrow_type == pa.date32()

    def test_varchar_mapping(self):
        """Test VARCHAR type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("VARCHAR(100)")
        assert arrow_type == pa.string()

    def test_varchar_no_size_mapping(self):
        """Test VARCHAR type mapping without size."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("VARCHAR")
        assert arrow_type == pa.string()

    def test_char_mapping(self):
        """Test CHAR type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("CHAR(10)")
        assert arrow_type == pa.string()

    def test_float_mapping(self):
        """Test FLOAT type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("FLOAT")
        assert arrow_type == pa.float32()

    def test_real_mapping(self):
        """Test REAL type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("REAL")
        assert arrow_type == pa.float32()

    def test_double_mapping(self):
        """Test DOUBLE type mapping."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("DOUBLE")
        assert arrow_type == pa.float64()

    def test_unknown_type_defaults_to_string(self):
        """Test unknown type defaults to string."""
        converter = ParquetConverter()
        arrow_type = converter._map_sql_type_to_arrow("UNKNOWN_TYPE")
        assert arrow_type == pa.string()

    def test_case_insensitive_mapping(self):
        """Test that type mapping is case insensitive."""
        converter = ParquetConverter()
        assert converter._map_sql_type_to_arrow("integer") == pa.int64()
        assert converter._map_sql_type_to_arrow("Integer") == pa.int64()
        assert converter._map_sql_type_to_arrow("INTEGER") == pa.int64()


class TestArrowSchemaBuilding:
    """Tests for building PyArrow schemas."""

    def test_build_basic_schema(self):
        """Test building a basic Arrow schema."""
        converter = ParquetConverter()
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR(100)"},
            ]
        }
        arrow_schema = converter._build_arrow_schema(schema)
        assert len(arrow_schema) == 2
        assert arrow_schema.field("id").type == pa.int64()
        assert arrow_schema.field("name").type == pa.string()

    def test_build_schema_with_nullable(self):
        """Test building schema with nullable fields."""
        converter = ParquetConverter()
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "VARCHAR(100)", "nullable": True},
            ]
        }
        arrow_schema = converter._build_arrow_schema(schema)
        assert arrow_schema.field("id").nullable is False
        assert arrow_schema.field("name").nullable is True

    def test_build_schema_default_nullable(self):
        """Test building schema with default nullable (True)."""
        converter = ParquetConverter()
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
            ]
        }
        arrow_schema = converter._build_arrow_schema(schema)
        assert arrow_schema.field("id").nullable is True

    def test_build_complex_schema(self):
        """Test building a complex schema with various types."""
        converter = ParquetConverter()
        schema = {
            "columns": [
                {"name": "c_custkey", "type": "INTEGER"},
                {"name": "c_name", "type": "VARCHAR(25)"},
                {"name": "c_acctbal", "type": "DECIMAL(15,2)"},
                {"name": "c_mktsegment", "type": "CHAR(10)"},
            ]
        }
        arrow_schema = converter._build_arrow_schema(schema)
        assert len(arrow_schema) == 4
        assert arrow_schema.field("c_custkey").type == pa.int64()
        assert arrow_schema.field("c_name").type == pa.string()
        assert arrow_schema.field("c_acctbal").type == pa.decimal128(15, 2)
        assert arrow_schema.field("c_mktsegment").type == pa.string()


class TestParquetConversion:
    """Tests for TBL to Parquet conversion."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for test files."""
        return tmp_path

    @pytest.fixture
    def sample_schema(self):
        """Sample schema for testing."""
        return {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR(25)"},
                {"name": "amount", "type": "DECIMAL(15,2)"},
            ]
        }

    def test_convert_single_file(self, temp_dir, sample_schema):
        """Test converting a single TBL file to Parquet."""
        # Create a sample TBL file
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|100.50|\n2|Bob|200.75|\n3|Charlie|300.00|\n")

        # Convert to Parquet
        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        # Verify result
        assert result.success is True
        assert result.row_count == 3
        assert len(result.output_files) == 1
        assert result.output_files[0].exists()
        assert result.output_files[0].suffix == ".parquet"

        # Verify Parquet file contents
        table = pq.read_table(result.output_files[0])
        assert table.num_rows == 3
        assert table.num_columns == 3

        # Verify data values
        data = table.to_pydict()
        assert data["id"] == [1, 2, 3]
        assert data["name"] == ["Alice", "Bob", "Charlie"]

    def test_convert_sharded_files(self, temp_dir, sample_schema):
        """Test converting sharded TBL files to Parquet."""
        # Create sharded TBL files
        tbl_file1 = temp_dir / "test.tbl.1"
        tbl_file1.write_text("1|Alice|100.50|\n2|Bob|200.75|\n")

        tbl_file2 = temp_dir / "test.tbl.2"
        tbl_file2.write_text("3|Charlie|300.00|\n4|David|400.25|\n")

        # Convert to Parquet
        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tbl_file1, tbl_file2],
            table_name="test",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir, merge_shards=True),
        )

        # Verify result
        assert result.success is True
        assert result.row_count == 4
        assert len(result.output_files) == 1

        # Verify Parquet file contains all rows
        table = pq.read_table(result.output_files[0])
        assert table.num_rows == 4
        data = table.to_pydict()
        assert data["id"] == [1, 2, 3, 4]

    def test_convert_with_compression_options(self, temp_dir, sample_schema):
        """Test conversion with different compression options."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|100.50|\n2|Bob|200.75|\n")

        converter = ParquetConverter()

        # Test snappy compression
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test_snappy",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir, compression="snappy"),
        )
        assert result.success is True
        assert result.metadata["compression"] == "snappy"

        # Test gzip compression
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test_gzip",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir, compression="gzip"),
        )
        assert result.success is True
        assert result.metadata["compression"] == "gzip"

        # Test zstd compression
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test_zstd",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir, compression="zstd"),
        )
        assert result.success is True
        assert result.metadata["compression"] == "zstd"

    def test_convert_with_empty_strings_as_null(self, temp_dir):
        """Test that empty strings are treated as NULL values."""
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR(25)", "nullable": True},
            ]
        }

        tbl_file = temp_dir / "test.tbl"
        # Second row has empty string for name
        tbl_file.write_text("1|Alice|\n2||\n")

        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        # Verify that empty string was converted to NULL
        table = pq.read_table(result.output_files[0])
        data = table.to_pydict()
        assert data["id"] == [1, 2]
        assert data["name"][0] == "Alice"
        assert data["name"][1] is None  # Empty string should be NULL

    def test_convert_missing_source_file(self, temp_dir, sample_schema):
        """Test error handling for missing source files."""
        missing_file = temp_dir / "missing.tbl"

        converter = ParquetConverter()
        with pytest.raises(ConversionError, match="Source file not found"):
            converter.convert(
                source_files=[missing_file],
                table_name="test",
                schema=sample_schema,
            )

    def test_convert_empty_source_list(self, sample_schema):
        """Test error handling for empty source file list."""
        converter = ParquetConverter()
        with pytest.raises(ConversionError, match="No source files provided"):
            converter.convert(
                source_files=[],
                table_name="test",
                schema=sample_schema,
            )

    def test_convert_invalid_schema(self, temp_dir):
        """Test error handling for invalid schema."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|\n")

        converter = ParquetConverter()
        with pytest.raises(SchemaError):
            converter.convert(
                source_files=[tbl_file],
                table_name="test",
                schema={},  # Invalid schema
            )

    def test_conversion_result_metadata(self, temp_dir, sample_schema):
        """Test that conversion result includes proper metadata."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|100.50|\n2|Bob|200.75|\n")

        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        # Check metadata
        assert "row_groups" in result.metadata
        assert "compression" in result.metadata
        assert "num_columns" in result.metadata
        assert result.metadata["num_columns"] == 3

        # Check size metrics
        assert result.source_size_bytes > 0
        assert result.output_size_bytes > 0
        assert result.compression_ratio > 0

    def test_progress_callback(self, temp_dir, sample_schema):
        """Test that progress callback is called during conversion."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|100.50|\n")

        progress_updates = []

        def progress_callback(message: str, progress: float):
            progress_updates.append((message, progress))

        converter = ParquetConverter()
        converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir),
            progress_callback=progress_callback,
        )

        # Verify progress callbacks were made
        assert len(progress_updates) > 0
        # Should have start, read, write, and complete callbacks
        assert any("Starting" in msg for msg, _ in progress_updates)
        assert any("Reading" in msg for msg, _ in progress_updates)
        assert any("Writing" in msg for msg, _ in progress_updates)
        assert any("complete" in msg for msg, _ in progress_updates)

    def test_get_output_path(self, temp_dir):
        """Test output path generation."""
        converter = ParquetConverter()

        # Test default output path
        output_path = converter.get_output_path("customer", temp_dir)
        assert output_path == temp_dir / "customer.parquet"

        # Test with custom output dir
        custom_dir = temp_dir / "custom"
        options = ConversionOptions(output_dir=custom_dir)
        output_path = converter.get_output_path("customer", temp_dir, options)
        assert output_path == custom_dir / "customer.parquet"


class TestParquetPartitioning:
    """Tests for Parquet Hive-style partitioning."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory for test files."""
        return tmp_path

    @pytest.fixture
    def partitioned_schema(self):
        """Schema with partition-suitable columns."""
        return {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR(25)"},
                {"name": "region", "type": "VARCHAR(10)"},
                {"name": "year", "type": "INTEGER"},
            ]
        }

    def test_convert_with_single_partition_column(self, temp_dir, partitioned_schema):
        """Test partitioning by a single column."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|East|2023|\n2|Bob|West|2023|\n3|Charlie|East|2024|\n4|David|West|2024|\n")

        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=partitioned_schema,
            options=ConversionOptions(
                output_dir=temp_dir,
                partition_cols=["region"],
            ),
        )

        # Verify result
        assert result.success is True
        assert result.row_count == 4
        assert result.metadata["partitioned"] is True
        assert result.metadata["partition_cols"] == ["region"]

        # Verify directory structure (Hive-style)
        partitioned_dir = temp_dir / "test"
        assert partitioned_dir.exists()
        assert partitioned_dir.is_dir()

        # Check partition directories exist
        east_dir = partitioned_dir / "region=East"
        west_dir = partitioned_dir / "region=West"
        assert east_dir.exists()
        assert west_dir.exists()

        # Check parquet files exist in partition directories
        east_files = list(east_dir.glob("*.parquet"))
        west_files = list(west_dir.glob("*.parquet"))
        assert len(east_files) >= 1
        assert len(west_files) >= 1

    def test_convert_with_multiple_partition_columns(self, temp_dir, partitioned_schema):
        """Test partitioning by multiple columns."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|East|2023|\n2|Bob|West|2023|\n3|Charlie|East|2024|\n4|David|West|2024|\n")

        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=partitioned_schema,
            options=ConversionOptions(
                output_dir=temp_dir,
                partition_cols=["region", "year"],
            ),
        )

        # Verify result
        assert result.success is True
        assert result.metadata["partitioned"] is True
        assert result.metadata["partition_cols"] == ["region", "year"]

        # Verify nested partition structure
        partitioned_dir = temp_dir / "test"
        assert (partitioned_dir / "region=East" / "year=2023").exists()
        assert (partitioned_dir / "region=East" / "year=2024").exists()
        assert (partitioned_dir / "region=West" / "year=2023").exists()
        assert (partitioned_dir / "region=West" / "year=2024").exists()

    def test_partitioned_data_can_be_read_as_dataset(self, temp_dir, partitioned_schema):
        """Test that partitioned data can be read back using PyArrow dataset."""
        import pyarrow.dataset as ds

        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|East|2023|\n2|Bob|West|2023|\n3|Charlie|East|2024|\n")

        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=partitioned_schema,
            options=ConversionOptions(
                output_dir=temp_dir,
                partition_cols=["region"],
            ),
        )

        # Read partitioned dataset
        partitioned_dir = result.output_files[0]
        dataset = ds.dataset(partitioned_dir, format="parquet", partitioning="hive")
        table = dataset.to_table()

        # Verify all rows are present
        assert table.num_rows == 3

        # Verify partition column is recovered
        data = table.to_pydict()
        assert set(data["region"]) == {"East", "West"}

    def test_partition_column_validation_invalid_column(self, temp_dir, partitioned_schema):
        """Test that invalid partition column raises error."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|East|2023|\n")

        converter = ParquetConverter()
        with pytest.raises(ConversionError, match="Partition column 'invalid_col' not found"):
            converter.convert(
                source_files=[tbl_file],
                table_name="test",
                schema=partitioned_schema,
                options=ConversionOptions(
                    output_dir=temp_dir,
                    partition_cols=["invalid_col"],
                ),
            )

    def test_partition_counts_in_metadata(self, temp_dir, partitioned_schema):
        """Test that partition counts are included in metadata."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|East|2023|\n2|Bob|West|2023|\n3|Charlie|East|2024|\n4|David|North|2024|\n")

        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=partitioned_schema,
            options=ConversionOptions(
                output_dir=temp_dir,
                partition_cols=["region"],
            ),
        )

        # Verify partition counts
        assert "partition_counts" in result.metadata
        assert result.metadata["partition_counts"]["region"] == 3  # East, West, North

    def test_partitioned_with_compression(self, temp_dir, partitioned_schema):
        """Test partitioned output with compression."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|East|2023|\n2|Bob|West|2023|\n")

        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=partitioned_schema,
            options=ConversionOptions(
                output_dir=temp_dir,
                partition_cols=["region"],
                compression="gzip",
            ),
        )

        assert result.success is True
        assert result.metadata["compression"] == "gzip"

    def test_non_partitioned_output_has_partitioned_false(self, temp_dir, partitioned_schema):
        """Test that non-partitioned output has partitioned=False in metadata."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|East|2023|\n")

        converter = ParquetConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=partitioned_schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        assert result.metadata.get("partitioned") is False
