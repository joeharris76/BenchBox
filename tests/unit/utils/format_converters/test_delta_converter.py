"""Tests for Delta Lake format converter."""

import pyarrow as pa
import pytest

from benchbox.utils.format_converters.base import (
    ConversionError,
    ConversionOptions,
    SchemaError,
)
from benchbox.utils.format_converters.delta_converter import DeltaConverter

# Import Delta Lake library (optional dependency)
try:
    from deltalake import DeltaTable

    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False


pytestmark = pytest.mark.skipif(not DELTA_AVAILABLE, reason="Delta Lake library not installed")


class TestDeltaConverterBasics:
    """Tests for DeltaConverter basic functionality."""

    def test_get_file_extension(self):
        """Test file extension for Delta format (directory-based)."""
        converter = DeltaConverter()
        assert converter.get_file_extension() == ""

    def test_get_format_name(self):
        """Test human-readable format name."""
        converter = DeltaConverter()
        assert converter.get_format_name() == "Delta Lake"


class TestDeltaSchemaValidation:
    """Tests for schema validation."""

    def test_valid_schema(self):
        """Test validation of a valid schema."""
        converter = DeltaConverter()
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR(100)"},
            ]
        }
        assert converter.validate_schema(schema) is True

    def test_empty_schema(self):
        """Test validation of empty schema."""
        converter = DeltaConverter()
        with pytest.raises(SchemaError, match="Schema is empty or None"):
            converter.validate_schema({})

    def test_none_schema(self):
        """Test validation of None schema."""
        converter = DeltaConverter()
        with pytest.raises(SchemaError, match="Schema is empty or None"):
            converter.validate_schema(None)

    def test_schema_missing_columns(self):
        """Test validation of schema missing columns field."""
        converter = DeltaConverter()
        schema = {"tables": []}
        with pytest.raises(SchemaError, match="Schema missing 'columns' field"):
            converter.validate_schema(schema)

    def test_schema_empty_columns(self):
        """Test validation of schema with empty columns list."""
        converter = DeltaConverter()
        schema = {"columns": []}
        with pytest.raises(SchemaError, match="'columns' must be a non-empty list"):
            converter.validate_schema(schema)

    def test_schema_columns_not_list(self):
        """Test validation of schema with non-list columns."""
        converter = DeltaConverter()
        schema = {"columns": "not a list"}
        with pytest.raises(SchemaError, match="'columns' must be a non-empty list"):
            converter.validate_schema(schema)

    def test_column_not_dict(self):
        """Test validation of column that is not a dict."""
        converter = DeltaConverter()
        schema = {"columns": ["not a dict"]}
        with pytest.raises(SchemaError, match="Column 0 is not a dictionary"):
            converter.validate_schema(schema)

    def test_column_missing_name(self):
        """Test validation of column missing name field."""
        converter = DeltaConverter()
        schema = {"columns": [{"type": "INTEGER"}]}
        with pytest.raises(SchemaError, match="Column 0 missing 'name' field"):
            converter.validate_schema(schema)

    def test_column_missing_type(self):
        """Test validation of column missing type field."""
        converter = DeltaConverter()
        schema = {"columns": [{"name": "id"}]}
        with pytest.raises(SchemaError, match="Column 0 \\(id\\) missing 'type' field"):
            converter.validate_schema(schema)


class TestSqlTypeMapping:
    """Tests for SQL type to Arrow type mapping."""

    def test_integer_mapping(self):
        """Test INTEGER type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("INTEGER")
        assert arrow_type == pa.int64()

    def test_int_mapping(self):
        """Test INT type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("INT")
        assert arrow_type == pa.int64()

    def test_bigint_mapping(self):
        """Test BIGINT type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("BIGINT")
        assert arrow_type == pa.int64()

    def test_decimal_with_params_mapping(self):
        """Test DECIMAL(15,2) type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("DECIMAL(15,2)")
        assert arrow_type == pa.decimal128(15, 2)

    def test_decimal_with_one_param_mapping(self):
        """Test DECIMAL(10) type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("DECIMAL(10)")
        assert arrow_type == pa.decimal128(10, 0)

    def test_decimal_without_params_mapping(self):
        """Test DECIMAL type mapping without parameters."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("DECIMAL")
        assert arrow_type == pa.decimal128(15, 2)

    def test_date_mapping(self):
        """Test DATE type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("DATE")
        assert arrow_type == pa.date32()

    def test_varchar_mapping(self):
        """Test VARCHAR type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("VARCHAR(100)")
        assert arrow_type == pa.string()

    def test_varchar_no_size_mapping(self):
        """Test VARCHAR type mapping without size."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("VARCHAR")
        assert arrow_type == pa.string()

    def test_char_mapping(self):
        """Test CHAR type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("CHAR(10)")
        assert arrow_type == pa.string()

    def test_float_mapping(self):
        """Test FLOAT type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("FLOAT")
        assert arrow_type == pa.float32()

    def test_real_mapping(self):
        """Test REAL type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("REAL")
        assert arrow_type == pa.float32()

    def test_double_mapping(self):
        """Test DOUBLE type mapping."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("DOUBLE")
        assert arrow_type == pa.float64()

    def test_unknown_type_defaults_to_string(self):
        """Test unknown type defaults to string."""
        converter = DeltaConverter()
        arrow_type = converter._map_sql_type_to_arrow("UNKNOWN_TYPE")
        assert arrow_type == pa.string()

    def test_case_insensitive_mapping(self):
        """Test that type mapping is case insensitive."""
        converter = DeltaConverter()
        assert converter._map_sql_type_to_arrow("integer") == pa.int64()
        assert converter._map_sql_type_to_arrow("Integer") == pa.int64()
        assert converter._map_sql_type_to_arrow("INTEGER") == pa.int64()


class TestArrowSchemaBuilding:
    """Tests for building PyArrow schemas."""

    def test_build_basic_schema(self):
        """Test building a basic Arrow schema."""
        converter = DeltaConverter()
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
        converter = DeltaConverter()
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
        converter = DeltaConverter()
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
            ]
        }
        arrow_schema = converter._build_arrow_schema(schema)
        assert arrow_schema.field("id").nullable is True

    def test_build_complex_schema(self):
        """Test building a complex schema with various types."""
        converter = DeltaConverter()
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


class TestDeltaConversion:
    """Tests for TBL to Delta Lake conversion."""

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
        """Test converting a single TBL file to Delta Lake."""
        # Create a sample TBL file
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|100.50|\n2|Bob|200.75|\n3|Charlie|300.00|\n")

        # Convert to Delta Lake
        converter = DeltaConverter()
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
        assert result.output_files[0].is_dir()  # Delta Lake uses directories

        # Verify Delta Lake table
        delta_table = DeltaTable(str(result.output_files[0]))
        df = delta_table.to_pyarrow_table()
        assert df.num_rows == 3
        assert df.num_columns == 3

        # Verify data values
        data = df.to_pydict()
        assert data["id"] == [1, 2, 3]
        assert data["name"] == ["Alice", "Bob", "Charlie"]

    def test_convert_sharded_files(self, temp_dir, sample_schema):
        """Test converting sharded TBL files to Delta Lake."""
        # Create sharded TBL files
        tbl_file1 = temp_dir / "test.tbl.1"
        tbl_file1.write_text("1|Alice|100.50|\n2|Bob|200.75|\n")

        tbl_file2 = temp_dir / "test.tbl.2"
        tbl_file2.write_text("3|Charlie|300.00|\n4|David|400.25|\n")

        # Convert to Delta Lake
        converter = DeltaConverter()
        result = converter.convert(
            source_files=[tbl_file1, tbl_file2],
            table_name="test",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        # Verify result
        assert result.success is True
        assert result.row_count == 4
        assert len(result.output_files) == 1

        # Verify Delta Lake table contains all rows
        delta_table = DeltaTable(str(result.output_files[0]))
        df = delta_table.to_pyarrow_table()
        assert df.num_rows == 4
        data = df.to_pydict()
        # Delta Lake doesn't guarantee row order, so sort before comparing
        assert sorted(data["id"]) == [1, 2, 3, 4]

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

        converter = DeltaConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        # Verify that empty string was converted to NULL
        delta_table = DeltaTable(str(result.output_files[0]))
        df = delta_table.to_pyarrow_table()
        data = df.to_pydict()
        assert data["id"] == [1, 2]
        assert data["name"][0] == "Alice"
        assert data["name"][1] is None  # Empty string should be NULL

    def test_convert_missing_source_file(self, temp_dir, sample_schema):
        """Test error handling for missing source files."""
        missing_file = temp_dir / "missing.tbl"

        converter = DeltaConverter()
        with pytest.raises(ConversionError, match="Source file not found"):
            converter.convert(
                source_files=[missing_file],
                table_name="test",
                schema=sample_schema,
            )

    def test_convert_empty_source_list(self, sample_schema):
        """Test error handling for empty source file list."""
        converter = DeltaConverter()
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

        converter = DeltaConverter()
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

        converter = DeltaConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        # Check metadata
        assert "format" in result.metadata
        assert result.metadata["format"] == "delta"
        assert "partition_cols" in result.metadata
        assert "num_columns" in result.metadata
        assert result.metadata["num_columns"] == 3
        assert "table_path" in result.metadata

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

        converter = DeltaConverter()
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

    def test_delta_table_metadata(self, temp_dir, sample_schema):
        """Test that Delta Lake table has proper metadata."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|100.50|\n")

        converter = DeltaConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test_table",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        # Verify Delta Lake table metadata
        delta_table = DeltaTable(str(result.output_files[0]))
        metadata = delta_table.metadata()

        # Check that table has correct name
        assert metadata.name == "test_table"
        assert metadata.description == "TPC benchmark table: test_table"

    def test_delta_transaction_log(self, temp_dir, sample_schema):
        """Test that Delta Lake creates transaction log."""
        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|Alice|100.50|\n")

        converter = DeltaConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        # Verify transaction log directory exists
        delta_log_dir = result.output_files[0] / "_delta_log"
        assert delta_log_dir.exists()
        assert delta_log_dir.is_dir()

        # Verify transaction log files exist
        log_files = list(delta_log_dir.glob("*.json"))
        assert len(log_files) > 0

    def test_convert_with_partitioning(self, temp_dir):
        """Test conversion with partitioning."""
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "category", "type": "VARCHAR(25)"},
                {"name": "amount", "type": "DECIMAL(15,2)"},
            ]
        }

        tbl_file = temp_dir / "test.tbl"
        tbl_file.write_text("1|A|100.50|\n2|B|200.75|\n3|A|300.00|\n")

        converter = DeltaConverter()
        result = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=schema,
            options=ConversionOptions(output_dir=temp_dir, partition_cols=["category"]),
        )

        # Verify partitioning metadata
        assert result.metadata["partition_cols"] == ["category"]

        # Verify partition directories exist
        delta_table = DeltaTable(str(result.output_files[0]))
        df = delta_table.to_pyarrow_table()
        assert df.num_rows == 3

        # Check that partitioned data is correctly written
        data = df.to_pydict()
        assert sorted(set(data["category"])) == ["A", "B"]

    def test_overwrite_existing_table(self, temp_dir, sample_schema):
        """Test that conversion overwrites existing Delta table."""
        tbl_file = temp_dir / "test.tbl"

        # Create initial table
        tbl_file.write_text("1|Alice|100.50|\n")
        converter = DeltaConverter()
        converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        # Overwrite with new data
        tbl_file.write_text("2|Bob|200.75|\n3|Charlie|300.00|\n")
        result2 = converter.convert(
            source_files=[tbl_file],
            table_name="test",
            schema=sample_schema,
            options=ConversionOptions(output_dir=temp_dir),
        )

        # Verify table was overwritten (has 2 rows, not 3)
        delta_table = DeltaTable(str(result2.output_files[0]))
        df = delta_table.to_pyarrow_table()
        assert df.num_rows == 2
        data = df.to_pydict()
        assert data["id"] == [2, 3]
