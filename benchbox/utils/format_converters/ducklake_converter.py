"""DuckLake format converter for BenchBox.

Converts TPC benchmark data from TBL (pipe-delimited) format to DuckLake format.

DuckLake is DuckDB's native open table format that provides:
- ACID transactions
- Time travel and snapshot isolation
- Schema evolution
- Native DuckDB integration with optimal performance
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Callable

from benchbox.utils.format_converters.base import (
    BaseFormatConverter,
    ConversionError,
    ConversionOptions,
    ConversionResult,
    SchemaError,
)


class DuckLakeConverter(BaseFormatConverter):
    """Converter for TBL → DuckLake format.

    DuckLake is DuckDB's native open table format that provides ACID transactions
    on top of Parquet files with metadata stored in a DuckDB database. This converter:
    1. Reads TBL files using PyArrow CSV reader
    2. Converts to Arrow tables
    3. Writes to DuckLake using DuckDB with the ducklake extension

    DuckLake structure:
    - Metadata catalog: A DuckDB database file storing table metadata
    - Data directory: Parquet files containing the actual data
    """

    def get_file_extension(self) -> str:
        """Get file extension for DuckLake format (directory-based)."""
        return ""  # DuckLake uses directories, not file extensions

    def get_format_name(self) -> str:
        """Get human-readable format name."""
        return "DuckLake"

    def convert(
        self,
        source_files: list[Path],
        table_name: str,
        schema: dict[str, Any],
        options: ConversionOptions | None = None,
        progress_callback: Callable[[str, float], None] | None = None,
    ) -> ConversionResult:
        """Convert TBL files to DuckLake format.

        Args:
            source_files: List of source TBL file paths (may be sharded)
            table_name: Name of the table being converted
            schema: Table schema definition
            options: Conversion options (uses defaults if None)
            progress_callback: Optional callback for progress updates

        Returns:
            ConversionResult with details about the conversion

        Raises:
            ConversionError: If conversion fails
            SchemaError: If schema is invalid
        """
        try:
            import duckdb
        except ImportError as e:
            raise ConversionError("DuckLake support requires DuckDB. Install it with: uv add duckdb") from e

        opts = options or ConversionOptions()

        # Validate inputs
        self.validate_source_files(source_files)
        self.validate_schema(schema)

        if progress_callback:
            progress_callback(f"Starting DuckLake conversion for {table_name}", 0.0)

        # Build PyArrow schema (validates schema can be converted)
        try:
            self._build_arrow_schema(schema)
        except SchemaError:
            raise
        except Exception as e:
            raise SchemaError(f"Failed to build Arrow schema: {e}") from e

        # Read TBL files using shared method
        combined_table = self.read_tbl_files(
            source_files, schema, progress_callback, progress_start=0.0, progress_end=0.6
        )

        # Get column names for metadata
        column_names = [col["name"] for col in schema["columns"]]

        # Determine output path - DuckLake uses a directory
        source_dir = source_files[0].parent
        output_dir = opts.output_dir if opts.output_dir else source_dir
        ducklake_table_path = output_dir / table_name

        # Clean up existing table directory if it exists
        if ducklake_table_path.exists():
            shutil.rmtree(ducklake_table_path, ignore_errors=True)

        ducklake_table_path.mkdir(parents=True, exist_ok=True)

        conn = None
        try:
            if progress_callback:
                progress_callback("Installing DuckLake extension", 0.65)

            # Create an in-memory connection to work with the data
            conn = duckdb.connect(":memory:")

            # Install and load the ducklake extension
            try:
                conn.execute("INSTALL ducklake")
                conn.execute("LOAD ducklake")
            except Exception as e:
                raise ConversionError(
                    f"Failed to load DuckLake extension. Ensure DuckDB >= 1.2.0 is installed. Error: {e}"
                ) from e

            if progress_callback:
                progress_callback("Creating DuckLake catalog", 0.7)

            # DuckLake stores metadata in a metadata file and data in a data directory
            metadata_path = ducklake_table_path / "metadata.ducklake"
            data_path = ducklake_table_path / "data"
            data_path.mkdir(parents=True, exist_ok=True)

            # Attach DuckLake database with DATA_PATH option
            conn.execute(f"ATTACH 'ducklake:{metadata_path}' AS ducklake_db (DATA_PATH '{data_path}')")

            if progress_callback:
                progress_callback("Writing DuckLake table", 0.8)

            # Register the Arrow table as a view
            conn.register("source_data", combined_table)

            # Create the schema (namespace) if it doesn't exist
            conn.execute("CREATE SCHEMA IF NOT EXISTS ducklake_db.main")

            # Create table in DuckLake catalog
            conn.execute(f"CREATE OR REPLACE TABLE ducklake_db.main.{table_name} AS SELECT * FROM source_data")

            if progress_callback:
                progress_callback("Finalizing DuckLake table", 0.9)

            # Get row count for verification
            row_count = combined_table.num_rows

            # Close connection to ensure all data is flushed
            conn.close()
            conn = None

        except ConversionError:
            raise
        except Exception as e:
            # Clean up partial output on failure
            if ducklake_table_path.exists():
                shutil.rmtree(ducklake_table_path, ignore_errors=True)
            raise ConversionError(f"Failed to write DuckLake table: {e}") from e
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

        # Calculate metrics
        source_size = self.calculate_file_size(source_files)
        row_count = combined_table.num_rows

        # Validate row count integrity (critical for TPC compliance)
        # If validation fails, clean up output before raising
        if opts.validate_row_count:
            try:
                self.validate_row_count(source_files, row_count, table_name)
            except ConversionError:
                # Clean up output directory since validation failed
                if ducklake_table_path.exists():
                    shutil.rmtree(ducklake_table_path, ignore_errors=True)
                raise

        # Calculate DuckLake table size (data files + catalog)
        output_size = sum(f.stat().st_size for f in ducklake_table_path.rglob("*") if f.is_file())

        # Build metadata
        metadata = {
            "format": "ducklake",
            "partition_cols": opts.partition_cols if opts.partition_cols else [],
            "num_columns": len(column_names),
            "table_path": str(ducklake_table_path),
            "metadata_path": str(metadata_path),
            "data_path": str(data_path),
            "compression": opts.compression,
        }

        if progress_callback:
            progress_callback(f"Conversion complete: {row_count:,} rows", 1.0)

        # Detect source format from file extensions
        source_format = self._detect_source_format(source_files)

        return ConversionResult(
            output_files=[ducklake_table_path],
            row_count=row_count,
            source_size_bytes=source_size,
            output_size_bytes=output_size,
            metadata=metadata,
            source_format=source_format,
            converted_at=self.get_current_timestamp(),
            conversion_options={
                "compression": opts.compression,
                "merge_shards": opts.merge_shards,
                "partition_cols": opts.partition_cols,
                "validate_row_count": opts.validate_row_count,
            },
        )
