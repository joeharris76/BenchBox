"""Base classes and interfaces for format converters.

This module defines the abstract base class for format converters and common
data structures used across all converter implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.csv as csv


@dataclass
class ConversionOptions:
    """Configuration options for format conversion.

    Attributes:
        compression: Compression algorithm ('snappy', 'gzip', 'zstd', 'none')
        row_group_size: Target row group size in bytes (Parquet-specific)
        partition_cols: Columns to partition by (Hive-style partitioning)
        merge_shards: Whether to merge sharded files into single output
        output_dir: Directory for converted files (if None, uses source dir)
        preserve_source: Whether to keep source files after conversion
        strict_schema: Raise SchemaError for unknown SQL types (default: False)
        metadata: Additional format-specific options
    """

    compression: str = "snappy"
    row_group_size: int = 128 * 1024 * 1024  # 128MB default
    partition_cols: list[str] = field(default_factory=list)
    merge_shards: bool = True
    output_dir: Path | None = None
    preserve_source: bool = True
    validate_row_count: bool = True
    strict_schema: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate options after initialization."""
        valid_compressions = {"snappy", "gzip", "zstd", "none", None}
        if self.compression not in valid_compressions:
            raise ValueError(f"Invalid compression: {self.compression}. Must be one of {valid_compressions}")

        if self.row_group_size <= 0:
            raise ValueError(f"row_group_size must be positive, got {self.row_group_size}")

        # Normalise boolean flags
        self.validate_row_count = bool(self.validate_row_count)


@dataclass
class ConversionResult:
    """Result of a format conversion operation.

    Attributes:
        output_files: List of paths to created files
        row_count: Total number of rows converted
        source_size_bytes: Total size of source files in bytes
        output_size_bytes: Total size of output files in bytes
        compression_ratio: Ratio of source_size / output_size
        metadata: Format-specific metadata (e.g., Parquet row groups, Delta version)
        errors: List of error messages if conversion partially failed
        source_format: Format of the source files (e.g., 'tbl', 'csv')
        converted_at: ISO 8601 timestamp of when conversion occurred
        conversion_options: Options used during conversion
    """

    output_files: list[Path]
    row_count: int
    source_size_bytes: int
    output_size_bytes: int
    metadata: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    source_format: str = "tbl"
    converted_at: str | None = None
    conversion_options: dict[str, Any] = field(default_factory=dict)

    @property
    def compression_ratio(self) -> float:
        """Calculate compression ratio (source_size / output_size)."""
        if self.output_size_bytes == 0:
            return 0.0
        return self.source_size_bytes / self.output_size_bytes

    @property
    def success(self) -> bool:
        """Check if conversion was successful (no errors)."""
        return len(self.errors) == 0


class ConversionError(Exception):
    """Exception raised during format conversion."""


class SchemaError(ConversionError):
    """Exception raised when schema mapping or validation fails."""


class ArrowTypeMapper:
    """Centralized SQL type to PyArrow type mapping.

    This class provides a single source of truth for SQL→Arrow type conversions,
    eliminating code duplication across all format converters.
    """

    @staticmethod
    def map_sql_type_to_arrow(sql_type: str, *, strict: bool = False) -> pa.DataType:
        """Map SQL type to PyArrow data type.

        Args:
            sql_type: SQL type string (e.g., 'INTEGER', 'VARCHAR(100)', 'DECIMAL(15,2)')
            strict: If True, raise SchemaError for unknown types instead of defaulting to string

        Returns:
            PyArrow data type

        Raises:
            SchemaError: If SQL type cannot be mapped (always for malformed types, or in strict mode for unknown types)
        """
        sql_type_upper = sql_type.upper().strip()

        # INTEGER types
        if sql_type_upper in ("INTEGER", "INT"):
            return pa.int64()  # Use int64 for safety with large values

        # BIGINT
        if sql_type_upper == "BIGINT":
            return pa.int64()

        # DECIMAL types - extract precision and scale
        if sql_type_upper.startswith("DECIMAL"):
            if "(" in sql_type_upper:
                try:
                    params = sql_type_upper[sql_type_upper.index("(") + 1 : sql_type_upper.index(")")].split(",")
                    precision = int(params[0].strip())
                    scale = int(params[1].strip()) if len(params) > 1 else 0
                    return pa.decimal128(precision, scale)
                except (ValueError, IndexError) as e:
                    raise SchemaError(f"Invalid DECIMAL type specification: {sql_type}") from e
            else:
                # Default DECIMAL without parameters
                return pa.decimal128(15, 2)

        # DATE type
        if sql_type_upper == "DATE":
            return pa.date32()

        # TIMESTAMP types
        if sql_type_upper == "TIMESTAMP":
            return pa.timestamp("us")  # Microsecond precision

        # VARCHAR and CHAR types - both map to string
        if sql_type_upper.startswith(("VARCHAR", "CHAR")):
            return pa.string()

        # FLOAT/REAL/DOUBLE
        if sql_type_upper in ("FLOAT", "REAL"):
            return pa.float32()
        if sql_type_upper == "DOUBLE":
            return pa.float64()

        # BOOLEAN
        if sql_type_upper in ("BOOLEAN", "BOOL"):
            return pa.bool_()

        # TEXT (PostgreSQL, SQLite)
        if sql_type_upper == "TEXT":
            return pa.string()

        # Unknown type handling
        if strict:
            raise SchemaError(f"Unknown SQL type '{sql_type}' cannot be mapped to Arrow type in strict mode")
        # Default to string for backward compatibility
        return pa.string()

    @staticmethod
    def build_arrow_schema(schema: dict[str, Any], *, strict: bool = False) -> pa.Schema:
        """Build PyArrow schema from benchmark schema definition.

        Args:
            schema: Benchmark table schema with 'columns' field
            strict: If True, raise SchemaError for unknown types instead of defaulting to string

        Returns:
            PyArrow schema

        Raises:
            SchemaError: If schema cannot be converted
        """
        if not schema or "columns" not in schema:
            raise SchemaError("Schema missing 'columns' field")

        fields = []
        columns = schema["columns"]

        for col in columns:
            col_name = col["name"]
            sql_type = col["type"]
            nullable = col.get("nullable", True)

            try:
                arrow_type = ArrowTypeMapper.map_sql_type_to_arrow(sql_type, strict=strict)
                field = pa.field(col_name, arrow_type, nullable=nullable)
                fields.append(field)
            except Exception as e:
                raise SchemaError(f"Failed to map column '{col_name}' with type '{sql_type}': {e}") from e

        return pa.schema(fields)


class FormatConverter(ABC):
    """Abstract base class for table format converters.

    This class defines the interface that all format converters must implement.
    Converters transform data from one format (typically TBL) to another format
    (Parquet, Delta Lake, Iceberg).
    """

    @abstractmethod
    def convert(
        self,
        source_files: list[Path],
        table_name: str,
        schema: dict[str, Any],
        options: ConversionOptions | None = None,
        progress_callback: Callable[[str, float], None] | None = None,
    ) -> ConversionResult:
        """Convert source files to target format.

        Args:
            source_files: List of source file paths to convert
            table_name: Name of the table being converted
            schema: Table schema definition (benchmark-specific format)
            options: Conversion options (uses defaults if None)
            progress_callback: Optional callback for progress updates (message, percent_complete)

        Returns:
            ConversionResult with details about the conversion

        Raises:
            ConversionError: If conversion fails
            SchemaError: If schema is invalid or cannot be mapped
        """
        ...

    @abstractmethod
    def validate_schema(self, schema: dict[str, Any]) -> bool:
        """Validate that schema can be converted to target format.

        Args:
            schema: Table schema definition

        Returns:
            True if schema is valid and can be converted

        Raises:
            SchemaError: If schema is invalid with explanation
        """
        ...

    def get_output_path(
        self,
        table_name: str,
        source_dir: Path,
        options: ConversionOptions | None = None,
    ) -> Path:
        """Determine output file path for converted data.

        Args:
            table_name: Name of table being converted
            source_dir: Directory containing source files
            options: Conversion options (uses defaults if None)

        Returns:
            Path where converted file should be written
        """
        opts = options or ConversionOptions()
        output_dir = opts.output_dir or source_dir

        # Get format-specific extension
        extension = self.get_file_extension()

        return output_dir / f"{table_name}{extension}"

    @abstractmethod
    def get_file_extension(self) -> str:
        """Get file extension for this format (e.g., '.parquet', '.delta').

        Returns:
            File extension including leading dot
        """
        ...

    @abstractmethod
    def get_format_name(self) -> str:
        """Get human-readable format name (e.g., 'Parquet', 'Delta Lake').

        Returns:
            Format name
        """
        ...


class BaseFormatConverter(FormatConverter):
    """Base implementation with common utilities for format converters.

    This class provides default implementations of common methods and utilities
    that can be shared across different format converters.
    """

    def validate_source_files(self, source_files: list[Path]) -> None:
        """Validate that source files exist and are readable.

        Args:
            source_files: List of source file paths

        Raises:
            ConversionError: If any file is missing or unreadable
        """
        if not source_files:
            raise ConversionError("No source files provided")

        for file_path in source_files:
            if not file_path.exists():
                raise ConversionError(f"Source file not found: {file_path}")
            if not file_path.is_file():
                raise ConversionError(f"Source path is not a file: {file_path}")

    def validate_schema(self, schema: dict[str, Any] | None) -> bool:
        """Validate that schema can be converted to target format.

        This is the common schema validation logic shared across all converters.

        Args:
            schema: Table schema definition from benchmark

        Returns:
            True if schema is valid

        Raises:
            SchemaError: If schema is invalid or missing required fields
        """
        if not schema:
            raise SchemaError("Schema is empty or None")

        if "columns" not in schema:
            raise SchemaError("Schema missing 'columns' field")

        columns = schema["columns"]
        if not columns or not isinstance(columns, list):
            raise SchemaError("Schema 'columns' must be a non-empty list")

        # Validate each column has required fields
        for i, col in enumerate(columns):
            if not isinstance(col, dict):
                raise SchemaError(f"Column {i} is not a dictionary")

            if "name" not in col:
                raise SchemaError(f"Column {i} missing 'name' field")

            if "type" not in col:
                raise SchemaError(f"Column {i} ({col.get('name', 'unknown')}) missing 'type' field")

        return True

    def _map_sql_type_to_arrow(self, sql_type: str, *, strict: bool = False) -> pa.DataType:
        """Map SQL type to PyArrow data type.

        Delegates to centralized ArrowTypeMapper.

        Args:
            sql_type: SQL type string (e.g., 'INTEGER', 'VARCHAR(100)', 'DECIMAL(15,2)')
            strict: If True, raise SchemaError for unknown types instead of defaulting to string

        Returns:
            PyArrow data type
        """
        return ArrowTypeMapper.map_sql_type_to_arrow(sql_type, strict=strict)

    def _build_arrow_schema(self, schema: dict[str, Any], *, strict: bool = False) -> pa.Schema:
        """Build PyArrow schema from benchmark schema.

        Delegates to centralized ArrowTypeMapper for type mapping.

        Args:
            schema: Benchmark table schema
            strict: If True, raise SchemaError for unknown types instead of defaulting to string

        Returns:
            PyArrow schema

        Raises:
            SchemaError: If schema cannot be converted
        """
        return ArrowTypeMapper.build_arrow_schema(schema, strict=strict)

    def read_tbl_files(
        self,
        source_files: list[Path],
        schema: dict[str, Any],
        progress_callback: Callable[[str, float], None] | None = None,
        progress_start: float = 0.0,
        progress_end: float = 0.8,
    ) -> pa.Table:
        """Read TBL files and return combined Arrow table.

        This is the common TBL file reading logic shared across all converters.
        TBL files are pipe-delimited with a trailing pipe on each line.

        Args:
            source_files: List of source TBL file paths
            schema: Table schema definition
            progress_callback: Optional callback for progress updates
            progress_start: Progress percentage at start of reading
            progress_end: Progress percentage at end of reading

        Returns:
            Combined PyArrow table

        Raises:
            ConversionError: If reading or concatenation fails
        """
        arrow_schema = self._build_arrow_schema(schema)
        column_names = [col["name"] for col in schema["columns"]]

        # TBL files have trailing pipe delimiter, which creates an extra empty column
        column_names_with_trailing = column_names + ["_trailing_delimiter"]

        tables = []
        total_files = len(source_files)
        progress_range = progress_end - progress_start

        try:
            for i, file_path in enumerate(source_files):
                if progress_callback:
                    progress = progress_start + (i / total_files) * progress_range
                    progress_callback(f"Reading {file_path.name}", progress)

                # Configure CSV read options
                read_options = csv.ReadOptions(
                    column_names=column_names_with_trailing,
                    autogenerate_column_names=False,
                )

                parse_options = csv.ParseOptions(
                    delimiter="|",
                    quote_char='"',
                    escape_char="\\",
                )

                convert_options = csv.ConvertOptions(
                    column_types=arrow_schema,
                    null_values=[""],
                    strings_can_be_null=True,
                    include_columns=column_names,  # Exclude trailing delimiter
                )

                # Read TBL file with PyArrow
                table = csv.read_csv(
                    file_path,
                    read_options=read_options,
                    parse_options=parse_options,
                    convert_options=convert_options,
                )

                tables.append(table)

        except Exception as e:
            raise ConversionError(f"Failed to read TBL files: {e}") from e

        # Concatenate all tables if multiple files
        try:
            if len(tables) > 1:
                if progress_callback:
                    progress_callback("Merging sharded files", progress_end)
                return pa.concat_tables(tables)
            else:
                return tables[0]
        except Exception as e:
            raise ConversionError(f"Failed to concatenate tables: {e}") from e

    def calculate_file_size(self, file_paths: list[Path]) -> int:
        """Calculate total size of files in bytes.

        Args:
            file_paths: List of file paths

        Returns:
            Total size in bytes
        """
        return sum(f.stat().st_size for f in file_paths if f.exists())

    def count_rows(self, file_paths: list[Path], delimiter: str = "|") -> int:
        """Count total rows across all files.

        Args:
            file_paths: List of file paths
            delimiter: Field delimiter (for validation)

        Returns:
            Total row count
        """
        total_rows = 0
        for file_path in file_paths:
            with open(file_path) as f:
                for line in f:
                    if line.strip():  # Skip empty lines
                        total_rows += 1
        return total_rows

    def validate_row_count(
        self,
        source_files: list[Path],
        output_row_count: int,
        table_name: str,
    ) -> None:
        """Validate that output row count matches input row count.

        This is critical for TPC compliance and data integrity. Any mismatch
        indicates data loss during conversion.

        Args:
            source_files: List of source TBL file paths
            output_row_count: Number of rows in converted output
            table_name: Name of table being converted (for error messages)

        Raises:
            ConversionError: If row counts don't match, indicating data loss
        """
        # Count rows in source files
        input_row_count = self.count_rows(source_files)

        if input_row_count != output_row_count:
            raise ConversionError(
                f"Row count mismatch for table '{table_name}': "
                f"input={input_row_count:,} rows, output={output_row_count:,} rows. "
                f"Data loss detected during conversion! "
                f"This violates TPC compliance and indicates a serious conversion error."
            )

    def _detect_source_format(self, source_files: list[Path]) -> str:
        """Detect source format from file extensions.

        Args:
            source_files: List of source file paths

        Returns:
            Format name (tbl, csv, parquet, etc.)
        """
        if not source_files:
            return "tbl"

        # Get first file's suffixes (handles cases like .tbl.1 for sharding)
        first_file = source_files[0]
        suffixes = first_file.suffixes
        name_lower = first_file.name.lower()

        # Check suffixes properly (not substring matching)
        if ".parquet" in suffixes:
            return "parquet"
        elif ".tbl" in suffixes or name_lower.endswith(".tbl"):
            return "tbl"
        elif ".dat" in suffixes or name_lower.endswith(".dat"):
            return "tbl"  # TPC-DS uses .dat but it's same format as TBL
        elif ".csv" in suffixes or name_lower.endswith(".csv"):
            return "csv"
        else:
            # Default to TBL for unknown formats
            return "tbl"

    @staticmethod
    def get_current_timestamp() -> str:
        """Get current UTC timestamp in ISO 8601 format.

        Returns:
            ISO 8601 formatted timestamp string
        """
        return datetime.now(timezone.utc).isoformat()
