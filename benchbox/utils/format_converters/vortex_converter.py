"""Vortex format converter for BenchBox.

Converts TPC benchmark data from TBL (pipe-delimited) format to Vortex columnar format.
Vortex is a high-performance columnar file format optimized for analytics workloads.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any, Callable

from benchbox.utils.format_converters.base import (
    BaseFormatConverter,
    ConversionError,
    ConversionOptions,
    ConversionResult,
    SchemaError,
)


class VortexConverter(BaseFormatConverter):
    """Converter for TBL -> Vortex format.

    Converts pipe-delimited TPC benchmark data files to Vortex columnar format.
    Uses the vortex Python package for efficient columnar storage with compression.
    """

    def get_file_extension(self) -> str:
        """Get file extension for Vortex format."""
        return ".vortex"

    def get_format_name(self) -> str:
        """Get human-readable format name."""
        return "Vortex"

    def _get_vortex_module(self):
        """Get the vortex module, raising helpful error if not installed.

        Returns:
            The vortex module

        Raises:
            ConversionError: If vortex is not installed
        """
        try:
            import vortex

            return vortex
        except ImportError as e:
            raise ConversionError(
                "Vortex format support requires the 'vortex' package. "
                "Install it with: uv add vortex --optional table-formats"
            ) from e

    def convert(
        self,
        source_files: list[Path],
        table_name: str,
        schema: dict[str, Any],
        options: ConversionOptions | None = None,
        progress_callback: Callable[[str, float], None] | None = None,
    ) -> ConversionResult:
        """Convert TBL files to Vortex format.

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
        # Get vortex module (will raise if not installed)
        vortex = self._get_vortex_module()

        opts = options or ConversionOptions()

        # Validate inputs
        self.validate_source_files(source_files)
        self.validate_schema(schema)

        if progress_callback:
            progress_callback(f"Starting Vortex conversion for {table_name}", 0.0)

        # Build PyArrow schema
        try:
            self._build_arrow_schema(schema)
        except SchemaError:
            raise
        except Exception as e:
            raise SchemaError(f"Failed to build Arrow schema: {e}") from e

        # Read TBL files using shared method
        combined_table = self.read_tbl_files(
            source_files, schema, progress_callback, progress_start=0.0, progress_end=0.8
        )

        # Get column names for metadata
        column_names = [col["name"] for col in schema["columns"]]

        # Determine output path
        source_dir = source_files[0].parent
        output_dir = opts.output_dir if opts.output_dir else source_dir
        output_path = output_dir / f"{table_name}.vortex"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if progress_callback:
                progress_callback("Writing Vortex file", 0.9)

            # Convert PyArrow table to Vortex array and write
            vortex_array = vortex.encoding.array(combined_table)
            vortex.io.write(vortex_array, output_path)

        except Exception as e:
            # Clean up partial file if write failed
            if output_path.exists():
                with contextlib.suppress(Exception):
                    output_path.unlink()
            raise ConversionError(f"Failed to write Vortex file: {e}") from e

        # Calculate metrics
        source_size = self.calculate_file_size(source_files)
        output_size = output_path.stat().st_size
        row_count = combined_table.num_rows

        # Validate row count integrity
        if opts.validate_row_count:
            try:
                self.validate_row_count(source_files, row_count, table_name)
            except ConversionError:
                if output_path.exists():
                    with contextlib.suppress(Exception):
                        output_path.unlink()
                raise

        # Build metadata
        metadata = {
            "compression": opts.compression,
            "num_columns": len(column_names),
        }

        if progress_callback:
            progress_callback(f"Conversion complete: {row_count:,} rows", 1.0)

        source_format = self._detect_source_format(source_files)

        return ConversionResult(
            output_files=[output_path],
            row_count=row_count,
            source_size_bytes=source_size,
            output_size_bytes=output_size,
            metadata=metadata,
            source_format=source_format,
            converted_at=self.get_current_timestamp(),
            conversion_options={
                "compression": opts.compression,
                "merge_shards": opts.merge_shards,
                "validate_row_count": opts.validate_row_count,
            },
        )
