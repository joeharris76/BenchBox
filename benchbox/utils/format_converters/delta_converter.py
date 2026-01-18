"""Delta Lake format converter for BenchBox.

Converts TPC benchmark data from TBL (pipe-delimited) format to Delta Lake format.
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


class DeltaConverter(BaseFormatConverter):
    """Converter for TBL → Delta Lake format.

    Delta Lake is a directory-based format that provides ACID transactions
    on top of Parquet files. This converter:
    1. Reads TBL files using PyArrow CSV reader
    2. Converts to Arrow tables
    3. Writes to Delta Lake using the deltalake library
    """

    def get_file_extension(self) -> str:
        """Get file extension for Delta format (directory-based)."""
        return ""  # Delta Lake uses directories, not file extensions

    def get_format_name(self) -> str:
        """Get human-readable format name."""
        return "Delta Lake"

    def convert(
        self,
        source_files: list[Path],
        table_name: str,
        schema: dict[str, Any],
        options: ConversionOptions | None = None,
        progress_callback: Callable[[str, float], None] | None = None,
    ) -> ConversionResult:
        """Convert TBL files to Delta Lake format.

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
            from deltalake import write_deltalake
        except ImportError as e:
            raise ConversionError(
                "Delta Lake support requires the 'deltalake' package. "
                "Install it with: uv add deltalake --optional table-formats"
            ) from e

        opts = options or ConversionOptions()

        # Validate inputs
        self.validate_source_files(source_files)
        self.validate_schema(schema)

        if progress_callback:
            progress_callback(f"Starting Delta Lake conversion for {table_name}", 0.0)

        # Build PyArrow schema (validates schema can be converted)
        try:
            self._build_arrow_schema(schema)
        except SchemaError:
            raise
        except Exception as e:
            raise SchemaError(f"Failed to build Arrow schema: {e}") from e

        # Read TBL files using shared method
        combined_table = self.read_tbl_files(
            source_files, schema, progress_callback, progress_start=0.0, progress_end=0.7
        )

        # Get column names for metadata
        column_names = [col["name"] for col in schema["columns"]]

        # Determine output path - Delta Lake uses a directory
        source_dir = source_files[0].parent
        output_dir = opts.output_dir if opts.output_dir else source_dir
        delta_table_path = output_dir / table_name
        delta_table_path.mkdir(parents=True, exist_ok=True)

        # Write to Delta Lake
        try:
            from deltalake.writer import WriterProperties

            if progress_callback:
                progress_callback("Writing Delta Lake table", 0.9)

            # Prepare partition columns if specified
            partition_by = opts.partition_cols if opts.partition_cols else None

            # Map compression to deltalake WriterProperties format (uppercase)
            compression_map = {
                "snappy": "SNAPPY",
                "gzip": "GZIP",
                "zstd": "ZSTD",
                "none": "UNCOMPRESSED",
                None: "SNAPPY",
            }
            compression_codec = compression_map.get(opts.compression, "SNAPPY")

            # Create writer properties for compression
            writer_properties = WriterProperties(compression=compression_codec)

            # Write the Delta table
            write_deltalake(
                str(delta_table_path),
                combined_table,
                mode="overwrite",
                partition_by=partition_by,
                name=table_name,
                description=f"TPC benchmark table: {table_name}",
                writer_properties=writer_properties,
            )

        except Exception as e:
            # Clean up partial output on failure
            if delta_table_path.exists():
                shutil.rmtree(delta_table_path, ignore_errors=True)
            raise ConversionError(f"Failed to write Delta Lake table: {e}") from e

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
                if delta_table_path.exists():
                    shutil.rmtree(delta_table_path, ignore_errors=True)
                raise

        # Calculate Delta table size (data files + transaction log)
        output_size = sum(f.stat().st_size for f in delta_table_path.rglob("*") if f.is_file())

        # Build metadata
        metadata = {
            "format": "delta",
            "partition_cols": opts.partition_cols if opts.partition_cols else [],
            "num_columns": len(column_names),
            "table_path": str(delta_table_path),
            "compression": opts.compression,
        }

        if progress_callback:
            progress_callback(f"Conversion complete: {row_count:,} rows", 1.0)

        # Detect source format from file extensions
        source_format = self._detect_source_format(source_files)

        return ConversionResult(
            output_files=[delta_table_path],
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
