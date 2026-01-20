"""Tests for format converter base classes."""

import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

from benchbox.utils.format_converters.base import (
    ArrowTypeMapper,
    BaseFormatConverter,
    ConversionError,
    ConversionOptions,
    ConversionResult,
    SchemaError,
)

pytestmark = pytest.mark.fast


class TestConversionOptions:
    """Tests for ConversionOptions dataclass."""

    def test_default_options(self):
        """Test default conversion options."""
        opts = ConversionOptions()
        assert opts.compression == "snappy"
        assert opts.row_group_size == 128 * 1024 * 1024
        assert opts.partition_cols == []
        assert opts.merge_shards is True
        assert opts.output_dir is None
        assert opts.preserve_source is True
        assert opts.validate_row_count is True
        assert opts.strict_schema is False

    def test_custom_options(self):
        """Test custom conversion options."""
        opts = ConversionOptions(
            compression="gzip",
            row_group_size=64 * 1024 * 1024,
            partition_cols=["date"],
            merge_shards=False,
        )
        assert opts.compression == "gzip"
        assert opts.row_group_size == 64 * 1024 * 1024
        assert opts.partition_cols == ["date"]
        assert opts.merge_shards is False

    def test_strict_schema_option(self):
        """Test strict_schema option can be enabled."""
        opts = ConversionOptions(strict_schema=True)
        assert opts.strict_schema is True

    def test_invalid_compression(self):
        """Test that invalid compression raises error."""
        with pytest.raises(ValueError, match="Invalid compression"):
            ConversionOptions(compression="invalid")

    def test_invalid_row_group_size(self):
        """Test that invalid row group size raises error."""
        with pytest.raises(ValueError, match="row_group_size must be positive"):
            ConversionOptions(row_group_size=0)

        with pytest.raises(ValueError, match="row_group_size must be positive"):
            ConversionOptions(row_group_size=-100)


class TestConversionResult:
    """Tests for ConversionResult dataclass."""

    def test_basic_result(self):
        """Test basic conversion result."""
        result = ConversionResult(
            output_files=[Path("/tmp/test.parquet")],
            row_count=1000,
            source_size_bytes=10000,
            output_size_bytes=2000,
        )
        assert len(result.output_files) == 1
        assert result.row_count == 1000
        assert result.success is True

    def test_compression_ratio(self):
        """Test compression ratio calculation."""
        result = ConversionResult(
            output_files=[Path("/tmp/test.parquet")],
            row_count=1000,
            source_size_bytes=10000,
            output_size_bytes=2000,
        )
        assert result.compression_ratio == 5.0

    def test_compression_ratio_zero_output(self):
        """Test compression ratio with zero output size."""
        result = ConversionResult(
            output_files=[],
            row_count=0,
            source_size_bytes=0,
            output_size_bytes=0,
        )
        assert result.compression_ratio == 0.0

    def test_result_with_errors(self):
        """Test result with errors."""
        result = ConversionResult(
            output_files=[],
            row_count=0,
            source_size_bytes=0,
            output_size_bytes=0,
            errors=["File not found", "Schema mismatch"],
        )
        assert result.success is False
        assert len(result.errors) == 2


class TestConversionErrors:
    """Tests for conversion error classes."""

    def test_conversion_error(self):
        """Test ConversionError exception."""
        with pytest.raises(ConversionError, match="Test error"):
            raise ConversionError("Test error")

    def test_schema_error(self):
        """Test SchemaError exception."""
        with pytest.raises(SchemaError, match="Invalid schema"):
            raise SchemaError("Invalid schema")

    def test_schema_error_is_conversion_error(self):
        """Test that SchemaError is subclass of ConversionError."""
        assert issubclass(SchemaError, ConversionError)


class TestArrowTypeMapper:
    """Tests for ArrowTypeMapper SQL-to-Arrow type mapping."""

    def test_integer_mapping(self):
        """Test INTEGER type mapping."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("INTEGER") == pa.int64()
        assert ArrowTypeMapper.map_sql_type_to_arrow("INT") == pa.int64()

    def test_bigint_mapping(self):
        """Test BIGINT type mapping."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("BIGINT") == pa.int64()

    def test_float_mapping(self):
        """Test FLOAT/REAL type mapping."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("FLOAT") == pa.float32()
        assert ArrowTypeMapper.map_sql_type_to_arrow("REAL") == pa.float32()

    def test_double_mapping(self):
        """Test DOUBLE type mapping."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("DOUBLE") == pa.float64()

    def test_varchar_mapping(self):
        """Test VARCHAR type mapping."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("VARCHAR(100)") == pa.string()
        assert ArrowTypeMapper.map_sql_type_to_arrow("VARCHAR") == pa.string()

    def test_char_mapping(self):
        """Test CHAR type mapping."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("CHAR(10)") == pa.string()
        assert ArrowTypeMapper.map_sql_type_to_arrow("CHAR") == pa.string()

    def test_date_mapping(self):
        """Test DATE type mapping."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("DATE") == pa.date32()

    def test_timestamp_mapping(self):
        """Test TIMESTAMP type mapping."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("TIMESTAMP") == pa.timestamp("us")

    def test_boolean_mapping(self):
        """Test BOOLEAN/BOOL type mapping."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("BOOLEAN") == pa.bool_()
        assert ArrowTypeMapper.map_sql_type_to_arrow("BOOL") == pa.bool_()

    def test_text_mapping(self):
        """Test TEXT type mapping (PostgreSQL/SQLite)."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("TEXT") == pa.string()

    def test_decimal_with_params_mapping(self):
        """Test DECIMAL type with precision and scale."""
        result = ArrowTypeMapper.map_sql_type_to_arrow("DECIMAL(15,2)")
        assert result == pa.decimal128(15, 2)

    def test_decimal_with_precision_only(self):
        """Test DECIMAL type with precision only (scale defaults to 0)."""
        result = ArrowTypeMapper.map_sql_type_to_arrow("DECIMAL(10)")
        assert result == pa.decimal128(10, 0)

    def test_decimal_without_params(self):
        """Test DECIMAL type without parameters (uses defaults)."""
        result = ArrowTypeMapper.map_sql_type_to_arrow("DECIMAL")
        assert result == pa.decimal128(15, 2)

    def test_malformed_decimal_raises_error(self):
        """Test that malformed DECIMAL specification raises SchemaError."""
        with pytest.raises(SchemaError, match="Invalid DECIMAL"):
            ArrowTypeMapper.map_sql_type_to_arrow("DECIMAL(abc,def)")

    def test_malformed_decimal_non_numeric_precision(self):
        """Test that non-numeric precision in DECIMAL raises error."""
        with pytest.raises(SchemaError, match="Invalid DECIMAL"):
            ArrowTypeMapper.map_sql_type_to_arrow("DECIMAL(fifteen,two)")

    def test_case_insensitive_mapping(self):
        """Test that type mapping is case-insensitive."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("integer") == pa.int64()
        assert ArrowTypeMapper.map_sql_type_to_arrow("Integer") == pa.int64()
        assert ArrowTypeMapper.map_sql_type_to_arrow("INTEGER") == pa.int64()

    def test_unknown_type_defaults_to_string(self):
        """Test that unknown types default to string."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("CUSTOM_TYPE") == pa.string()

    def test_unknown_type_raises_error_in_strict_mode(self):
        """Test that unknown types raise SchemaError in strict mode."""
        with pytest.raises(SchemaError, match="Unknown SQL type.*strict mode"):
            ArrowTypeMapper.map_sql_type_to_arrow("CUSTOM_TYPE", strict=True)

    def test_unknown_type_error_message_contains_type_name(self):
        """Test that error message includes the unknown type name."""
        with pytest.raises(SchemaError, match="WEIRD_TYPE"):
            ArrowTypeMapper.map_sql_type_to_arrow("WEIRD_TYPE", strict=True)

    def test_known_types_work_in_strict_mode(self):
        """Test that known types work correctly in strict mode."""
        assert ArrowTypeMapper.map_sql_type_to_arrow("INTEGER", strict=True) == pa.int64()
        assert ArrowTypeMapper.map_sql_type_to_arrow("VARCHAR(50)", strict=True) == pa.string()
        assert ArrowTypeMapper.map_sql_type_to_arrow("DECIMAL(10,2)", strict=True) == pa.decimal128(10, 2)

    def test_build_arrow_schema_basic(self):
        """Test building Arrow schema from column definitions."""
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR(100)"},
            ]
        }
        arrow_schema = ArrowTypeMapper.build_arrow_schema(schema)
        assert len(arrow_schema) == 2
        assert arrow_schema.field("id").type == pa.int64()
        assert arrow_schema.field("name").type == pa.string()

    def test_build_arrow_schema_with_nullable(self):
        """Test building Arrow schema with nullable fields."""
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "VARCHAR(100)", "nullable": True},
            ]
        }
        arrow_schema = ArrowTypeMapper.build_arrow_schema(schema)
        assert arrow_schema.field("id").nullable is False
        assert arrow_schema.field("name").nullable is True

    def test_build_arrow_schema_missing_columns(self):
        """Test that missing columns field raises SchemaError."""
        with pytest.raises(SchemaError, match="columns"):
            ArrowTypeMapper.build_arrow_schema({})

    def test_build_arrow_schema_empty(self):
        """Test that empty schema raises SchemaError."""
        with pytest.raises(SchemaError, match="columns"):
            ArrowTypeMapper.build_arrow_schema(None)

    def test_build_arrow_schema_strict_mode_unknown_type(self):
        """Test that build_arrow_schema raises SchemaError for unknown types in strict mode."""
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "data", "type": "CUSTOM_TYPE"},
            ]
        }
        with pytest.raises(SchemaError, match="Unknown SQL type"):
            ArrowTypeMapper.build_arrow_schema(schema, strict=True)

    def test_build_arrow_schema_non_strict_mode_unknown_type(self):
        """Test that build_arrow_schema defaults unknown types to string in non-strict mode."""
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "data", "type": "CUSTOM_TYPE"},
            ]
        }
        arrow_schema = ArrowTypeMapper.build_arrow_schema(schema, strict=False)
        assert arrow_schema.field("id").type == pa.int64()
        assert arrow_schema.field("data").type == pa.string()

    def test_build_arrow_schema_strict_mode_known_types(self):
        """Test that build_arrow_schema works with known types in strict mode."""
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "VARCHAR(100)"},
                {"name": "amount", "type": "DECIMAL(15,2)"},
            ]
        }
        arrow_schema = ArrowTypeMapper.build_arrow_schema(schema, strict=True)
        assert arrow_schema.field("id").type == pa.int64()
        assert arrow_schema.field("name").type == pa.string()
        assert arrow_schema.field("amount").type == pa.decimal128(15, 2)


class TestBaseFormatConverterValidation:
    """Tests for BaseFormatConverter validation methods."""

    @pytest.fixture
    def converter(self):
        """Create a concrete converter for testing base class methods."""
        from benchbox.utils.format_converters.parquet_converter import ParquetConverter

        return ParquetConverter()

    def test_validate_row_count_match(self, converter):
        """Test row count validation passes when counts match."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create a TBL file with 3 rows
            tbl_file = temp_path / "test.tbl"
            tbl_file.write_text("1|Alice|\n2|Bob|\n3|Charlie|\n")

            # Validation should pass when row counts match
            # (no exception means pass)
            converter.validate_row_count([tbl_file], 3, "test")

    def test_validate_row_count_mismatch_single_file(self, converter):
        """Test row count validation fails when counts don't match."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create a TBL file with 3 rows
            tbl_file = temp_path / "test.tbl"
            tbl_file.write_text("1|Alice|\n2|Bob|\n3|Charlie|\n")

            # Validation should fail when output has fewer rows
            with pytest.raises(ConversionError, match="Row count mismatch"):
                converter.validate_row_count([tbl_file], 2, "test")

    def test_validate_row_count_mismatch_merged_shards(self, converter):
        """Test row count validation catches mismatch in merged shards.

        This test ensures that when multiple shard files are merged,
        the validation correctly detects data loss.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create multiple shard files
            shard1 = temp_path / "test.tbl.1"
            shard1.write_text("1|Alice|\n2|Bob|\n")  # 2 rows

            shard2 = temp_path / "test.tbl.2"
            shard2.write_text("3|Charlie|\n4|David|\n5|Eve|\n")  # 3 rows

            shard3 = temp_path / "test.tbl.3"
            shard3.write_text("6|Frank|\n")  # 1 row

            # Total: 6 rows across 3 shards
            source_files = [shard1, shard2, shard3]

            # Validation should pass with correct count
            converter.validate_row_count(source_files, 6, "test")

            # Validation should fail with wrong count (simulating data loss)
            with pytest.raises(ConversionError, match="Row count mismatch.*input=6.*output=5"):
                converter.validate_row_count(source_files, 5, "test")

            with pytest.raises(ConversionError, match="Row count mismatch.*input=6.*output=4"):
                converter.validate_row_count(source_files, 4, "test")

    def test_validate_row_count_error_message_contains_details(self, converter):
        """Test that row count mismatch error contains useful details."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            tbl_file = temp_path / "customer.tbl"
            tbl_file.write_text("1|Alice|\n2|Bob|\n3|Charlie|\n")

            with pytest.raises(ConversionError) as exc_info:
                converter.validate_row_count([tbl_file], 1, "customer")

            error_msg = str(exc_info.value)
            # Error should contain table name
            assert "customer" in error_msg
            # Error should contain input row count
            assert "input=3" in error_msg
            # Error should contain output row count
            assert "output=1" in error_msg
            # Error should mention TPC compliance
            assert "TPC compliance" in error_msg

    def test_validate_row_count_skips_empty_lines(self, converter):
        """Test that empty lines in source files are not counted."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create file with empty lines
            tbl_file = temp_path / "test.tbl"
            tbl_file.write_text("1|Alice|\n\n2|Bob|\n\n\n3|Charlie|\n")

            # Only 3 non-empty rows should be counted
            converter.validate_row_count([tbl_file], 3, "test")

    def test_count_rows_across_shards(self, converter):
        """Test counting rows across multiple shard files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            shard1 = temp_path / "test.tbl.1"
            shard1.write_text("1|Alice|\n2|Bob|\n")

            shard2 = temp_path / "test.tbl.2"
            shard2.write_text("3|Charlie|\n")

            count = converter.count_rows([shard1, shard2])
            assert count == 3

    def test_detect_source_format_tbl(self, converter):
        """Test source format detection for TBL files."""
        assert converter._detect_source_format([Path("customer.tbl")]) == "tbl"
        assert converter._detect_source_format([Path("customer.tbl.1")]) == "tbl"

    def test_detect_source_format_dat(self, converter):
        """Test source format detection for DAT files (TPC-DS)."""
        assert converter._detect_source_format([Path("store_sales.dat")]) == "tbl"

    def test_detect_source_format_csv(self, converter):
        """Test source format detection for CSV files."""
        assert converter._detect_source_format([Path("data.csv")]) == "csv"

    def test_detect_source_format_parquet(self, converter):
        """Test source format detection for Parquet files."""
        assert converter._detect_source_format([Path("data.parquet")]) == "parquet"

    def test_detect_source_format_default(self, converter):
        """Test source format detection defaults to TBL for unknown."""
        assert converter._detect_source_format([Path("data.unknown")]) == "tbl"
        assert converter._detect_source_format([]) == "tbl"

    def test_get_current_timestamp(self):
        """Test timestamp generation."""
        timestamp = BaseFormatConverter.get_current_timestamp()
        # Should be ISO 8601 format
        assert "T" in timestamp
        # Should contain timezone info
        assert "+" in timestamp or "Z" in timestamp
