"""Parquet format converter for BenchBox.

Converts TPC benchmark data from TBL (pipe-delimited) format to Apache Parquet format.
Supports Hive-style partitioning for optimized query performance.
"""

from __future__ import annotations

import contextlib
import shutil
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from benchbox.utils.format_converters.base import (
    BaseFormatConverter,
    ConversionError,
    ConversionOptions,
    ConversionResult,
    SchemaError,
)


class ParquetConverter(BaseFormatConverter):
    """Converter for TBL → Parquet format.

    Converts pipe-delimited TPC benchmark data files to Apache Parquet format
    using PyArrow for efficient columnar storage. Supports Hive-style partitioning
    for optimized query performance on selective queries.
    """

    def get_file_extension(self) -> str:
        """Get file extension for Parquet format."""
        return ".parquet"

    def get_format_name(self) -> str:
        """Get human-readable format name."""
        return "Parquet"

    def _validate_partition_columns(self, partition_cols: list[str], schema: dict[str, Any]) -> None:
        """Validate that partition columns exist in the schema.

        Args:
            partition_cols: Columns to partition by
            schema: Table schema definition

        Raises:
            ConversionError: If any partition column is not in the schema
        """
        available_columns = {col["name"] for col in schema["columns"]}
        for col in partition_cols:
            if col not in available_columns:
                raise ConversionError(
                    f"Partition column '{col}' not found in schema. Available columns: {sorted(available_columns)}"
                )

    def convert(
        self,
        source_files: list[Path],
        table_name: str,
        schema: dict[str, Any],
        options: ConversionOptions | None = None,
        progress_callback: Callable[[str, float], None] | None = None,
    ) -> ConversionResult:
        """Convert TBL files to Parquet format.

        Supports both single-file output and Hive-style partitioned output.
        Partitioning creates a directory structure like:
            table_name/partition_col=value1/part-0.parquet
            table_name/partition_col=value2/part-0.parquet

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
        opts = options or ConversionOptions()

        # Validate inputs
        self.validate_source_files(source_files)
        self.validate_schema(schema)

        # Validate partition columns if specified
        if opts.partition_cols:
            self._validate_partition_columns(opts.partition_cols, schema)

        if progress_callback:
            progress_callback(f"Starting Parquet conversion for {table_name}", 0.0)

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

        # Determine output path/directory
        source_dir = source_files[0].parent
        output_dir = opts.output_dir if opts.output_dir else source_dir

        # Set up compression
        compression_map = {
            "snappy": "SNAPPY",
            "gzip": "GZIP",
            "zstd": "ZSTD",
            "none": None,
            None: None,
        }
        compression = compression_map.get(opts.compression, "SNAPPY")

        # Route to partitioned or single-file output
        if opts.partition_cols:
            return self._write_partitioned(
                combined_table=combined_table,
                table_name=table_name,
                output_dir=output_dir,
                source_files=source_files,
                column_names=column_names,
                opts=opts,
                compression=compression,
                progress_callback=progress_callback,
            )
        else:
            return self._write_single_file(
                combined_table=combined_table,
                table_name=table_name,
                output_dir=output_dir,
                source_files=source_files,
                column_names=column_names,
                opts=opts,
                compression=compression,
                progress_callback=progress_callback,
            )

    def _write_single_file(
        self,
        combined_table: pa.Table,
        table_name: str,
        output_dir: Path,
        source_files: list[Path],
        column_names: list[str],
        opts: ConversionOptions,
        compression: str | None,
        progress_callback: Callable[[str, float], None] | None,
    ) -> ConversionResult:
        """Write non-partitioned single Parquet file.

        Args:
            combined_table: PyArrow table to write
            table_name: Name of the table
            output_dir: Directory for output
            source_files: Original source files
            column_names: List of column names
            opts: Conversion options
            compression: Compression codec
            progress_callback: Optional progress callback

        Returns:
            ConversionResult
        """
        output_path = output_dir / f"{table_name}.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if progress_callback:
                progress_callback("Writing Parquet file", 0.9)

            pq.write_table(
                combined_table,
                output_path,
                compression=compression,
                use_dictionary=True,
                write_statistics=True,
                row_group_size=opts.row_group_size,
            )

        except Exception as e:
            # Clean up partial file if write failed
            if output_path.exists():
                with contextlib.suppress(Exception):
                    output_path.unlink()
            raise ConversionError(f"Failed to write Parquet file: {e}") from e

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
            "row_groups": pq.read_metadata(output_path).num_row_groups,
            "compression": opts.compression,
            "num_columns": len(column_names),
            "partitioned": False,
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
                "row_group_size": opts.row_group_size,
                "merge_shards": opts.merge_shards,
                "partition_cols": opts.partition_cols,
                "validate_row_count": opts.validate_row_count,
            },
        )

    def _write_partitioned(
        self,
        combined_table: pa.Table,
        table_name: str,
        output_dir: Path,
        source_files: list[Path],
        column_names: list[str],
        opts: ConversionOptions,
        compression: str | None,
        progress_callback: Callable[[str, float], None] | None,
    ) -> ConversionResult:
        """Write Hive-style partitioned Parquet dataset.

        Creates a directory structure with partition columns:
            table_name/partition_col=value1/part-0.parquet
            table_name/partition_col=value2/part-0.parquet

        Args:
            combined_table: PyArrow table to write
            table_name: Name of the table
            output_dir: Directory for output
            source_files: Original source files
            column_names: List of column names
            opts: Conversion options
            compression: Compression codec
            progress_callback: Optional progress callback

        Returns:
            ConversionResult
        """
        # Partitioned output uses a directory named after the table
        partitioned_dir = output_dir / table_name
        partitioned_dir.mkdir(parents=True, exist_ok=True)

        try:
            if progress_callback:
                progress_callback(
                    f"Writing partitioned Parquet (by {', '.join(opts.partition_cols)})",
                    0.9,
                )

            # Build partitioning schema from the table's actual schema
            partition_fields = [combined_table.schema.field(col) for col in opts.partition_cols]
            partitioning = ds.partitioning(pa.schema(partition_fields), flavor="hive")

            # Write partitioned dataset
            ds.write_dataset(
                combined_table,
                partitioned_dir,
                format="parquet",
                partitioning=partitioning,
                basename_template="part-{i}.parquet",
                existing_data_behavior="overwrite_or_ignore",
                file_options=ds.ParquetFileFormat().make_write_options(
                    compression=compression,
                    write_statistics=True,
                ),
            )

        except Exception as e:
            # Clean up partial output on failure
            if partitioned_dir.exists():
                shutil.rmtree(partitioned_dir, ignore_errors=True)
            raise ConversionError(f"Failed to write partitioned Parquet: {e}") from e

        # Gather all output files
        output_files = sorted(partitioned_dir.rglob("*.parquet"))

        # Calculate metrics
        source_size = self.calculate_file_size(source_files)
        output_size = sum(f.stat().st_size for f in output_files)
        row_count = combined_table.num_rows

        # Validate row count integrity
        if opts.validate_row_count:
            try:
                self.validate_row_count(source_files, row_count, table_name)
            except ConversionError:
                if partitioned_dir.exists():
                    shutil.rmtree(partitioned_dir, ignore_errors=True)
                raise

        # Count distinct partition values
        partition_counts = {}
        for col in opts.partition_cols:
            partition_counts[col] = len(combined_table.column(col).unique())

        # Build metadata
        metadata = {
            "compression": opts.compression,
            "num_columns": len(column_names),
            "partitioned": True,
            "partition_cols": opts.partition_cols,
            "partition_counts": partition_counts,
            "num_files": len(output_files),
            "table_path": str(partitioned_dir),
        }

        if progress_callback:
            progress_callback(
                f"Conversion complete: {row_count:,} rows across {len(output_files)} files",
                1.0,
            )

        source_format = self._detect_source_format(source_files)

        return ConversionResult(
            output_files=[partitioned_dir],  # Return directory for partitioned output
            row_count=row_count,
            source_size_bytes=source_size,
            output_size_bytes=output_size,
            metadata=metadata,
            source_format=source_format,
            converted_at=self.get_current_timestamp(),
            conversion_options={
                "compression": opts.compression,
                "row_group_size": opts.row_group_size,
                "merge_shards": opts.merge_shards,
                "partition_cols": opts.partition_cols,
                "validate_row_count": opts.validate_row_count,
            },
        )
