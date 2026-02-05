"""Apache Iceberg format converter for BenchBox.

Converts TPC benchmark data from TBL (pipe-delimited) format to Apache Iceberg format.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable

from benchbox.utils.format_converters.base import (
    BaseFormatConverter,
    ConversionError,
    ConversionOptions,
    ConversionResult,
    SchemaError,
)


class IcebergConverter(BaseFormatConverter):
    """Converter for TBL → Apache Iceberg format.

    Apache Iceberg is a table format with metadata/manifest files on top of Parquet.
    This converter:
    1. Reads TBL files using PyArrow CSV reader
    2. Converts to Arrow tables
    3. Writes to Iceberg using the pyiceberg library with a filesystem catalog
    4. Supports identity partitioning for partition columns
    """

    def get_file_extension(self) -> str:
        """Get file extension for Iceberg format (directory-based)."""
        return ""  # Iceberg uses directories, not file extensions

    def get_format_name(self) -> str:
        """Get human-readable format name."""
        return "Apache Iceberg"

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

    def _build_partition_spec(self, iceberg_schema: Any, partition_cols: list[str]) -> Any:
        """Build an Iceberg PartitionSpec from partition columns.

        Args:
            iceberg_schema: PyIceberg schema
            partition_cols: List of column names to partition by

        Returns:
            PartitionSpec with identity transforms for each column
        """
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import IdentityTransform

        partition_fields = []
        for idx, col_name in enumerate(partition_cols):
            # Find the source field ID from the schema
            source_field = iceberg_schema.find_field(col_name)
            partition_fields.append(
                PartitionField(
                    source_id=source_field.field_id,
                    field_id=1000 + idx,  # Partition field IDs start at 1000 by convention
                    transform=IdentityTransform(),
                    name=col_name,
                )
            )

        return PartitionSpec(*partition_fields)

    def convert(
        self,
        source_files: list[Path],
        table_name: str,
        schema: dict[str, Any],
        options: ConversionOptions | None = None,
        progress_callback: Callable[[str, float], None] | None = None,
    ) -> ConversionResult:
        """Convert TBL files to Iceberg format.

        Supports identity partitioning for specified columns, creating a
        directory structure managed by Iceberg metadata.

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
            from pyiceberg.catalog.sql import SqlCatalog
        except ImportError as e:
            raise ConversionError(
                "Iceberg support requires the 'pyiceberg' package. "
                "Install it with: uv add pyiceberg --optional table-formats"
            ) from e

        opts = options or ConversionOptions()

        # Validate inputs
        self.validate_source_files(source_files)
        self.validate_schema(schema)

        # Validate partition columns if specified
        if opts.partition_cols:
            self._validate_partition_columns(opts.partition_cols, schema)

        if progress_callback:
            progress_callback(f"Starting Iceberg conversion for {table_name}", 0.0)

        # Build PyArrow schema (validates schema can be converted)
        try:
            arrow_schema = self._build_arrow_schema(schema)
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

        # Determine output path - Iceberg uses a directory
        source_dir = source_files[0].parent
        output_dir = opts.output_dir if opts.output_dir else source_dir
        iceberg_table_path = output_dir / table_name
        iceberg_table_path.mkdir(parents=True, exist_ok=True)

        # Create catalog and table
        catalog_fd = None
        catalog_db = None
        try:
            if progress_callback:
                progress_callback("Creating Iceberg catalog", 0.7)

            # Use SQL catalog with SQLite backend for filesystem-based Iceberg tables
            catalog_fd, catalog_db = tempfile.mkstemp(suffix=".db", prefix="benchbox_iceberg_")
            os.close(catalog_fd)
            catalog_fd = None

            warehouse_path = str(output_dir)

            catalog = SqlCatalog(
                "benchbox_catalog",
                uri=f"sqlite:///{catalog_db}",
                warehouse=warehouse_path,
            )

            # Create namespace if it doesn't exist
            catalog.create_namespace_if_not_exists("benchbox")

            if progress_callback:
                progress_callback("Writing Iceberg table", 0.8)

            # Create or replace the table
            table_identifier = ("benchbox", table_name)

            # Try to drop existing table if it exists
            try:
                catalog.drop_table(table_identifier)
            except Exception:
                pass  # Table doesn't exist, that's fine

            # Create table with PyArrow schema
            compression_map = {
                "snappy": "snappy",
                "gzip": "gzip",
                "zstd": "zstd",
                "none": "uncompressed",
                None: "snappy",
            }
            compression_codec = compression_map.get(opts.compression, "snappy")

            # Build partition spec if partition columns specified
            partition_spec = None
            iceberg_schema = None
            if opts.partition_cols:
                from pyiceberg.io.pyarrow import (
                    _ConvertToIcebergWithoutIDs,
                    visit_pyarrow,
                )
                from pyiceberg.schema import assign_fresh_schema_ids

                # Convert Arrow schema to Iceberg schema with proper field IDs
                # We need to pass the Iceberg schema (not Arrow) when using partition spec
                # to ensure field IDs match between schema and partition spec
                visitor = _ConvertToIcebergWithoutIDs()
                iceberg_schema_no_ids = visit_pyarrow(arrow_schema, visitor)
                iceberg_schema = assign_fresh_schema_ids(iceberg_schema_no_ids)
                partition_spec = self._build_partition_spec(iceberg_schema, opts.partition_cols)

            # Prepare create_table kwargs
            # Use Iceberg schema if we have a partition spec, otherwise Arrow schema is fine
            # Use as_uri() for cross-platform compatibility (Windows paths need forward slashes)
            create_kwargs = {
                "identifier": table_identifier,
                "schema": iceberg_schema if iceberg_schema else arrow_schema,
                "location": iceberg_table_path.as_uri(),
                "properties": {
                    "format-version": "2",
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": compression_codec,
                },
            }
            if partition_spec:
                create_kwargs["partition_spec"] = partition_spec

            iceberg_table = catalog.create_table(**create_kwargs)

            # Write data to Iceberg table
            iceberg_table.overwrite(combined_table)

        except ConversionError:
            raise
        except Exception as e:
            # Clean up partial output on failure
            if iceberg_table_path.exists():
                shutil.rmtree(iceberg_table_path, ignore_errors=True)
            raise ConversionError(f"Failed to write Iceberg table: {e}") from e
        finally:
            # Clean up temporary catalog database
            if catalog_fd is not None:
                try:
                    os.close(catalog_fd)
                except Exception:
                    pass
            if catalog_db and os.path.exists(catalog_db):
                try:
                    os.unlink(catalog_db)
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
                if iceberg_table_path.exists():
                    shutil.rmtree(iceberg_table_path, ignore_errors=True)
                raise

        # Calculate Iceberg table size (data files + metadata)
        output_size = sum(f.stat().st_size for f in iceberg_table_path.rglob("*") if f.is_file())

        # Count partition values if partitioned
        partition_counts = {}
        if opts.partition_cols:
            for col in opts.partition_cols:
                partition_counts[col] = len(combined_table.column(col).unique())

        # Build metadata
        metadata = {
            "format": "iceberg",
            "partitioned": bool(opts.partition_cols),
            "partition_cols": opts.partition_cols if opts.partition_cols else [],
            "partition_counts": partition_counts if partition_counts else None,
            "num_columns": len(column_names),
            "table_path": str(iceberg_table_path),
            "format_version": 2,
            "compression": opts.compression,
        }

        if progress_callback:
            progress_callback(f"Conversion complete: {row_count:,} rows", 1.0)

        # Detect source format from file extensions
        source_format = self._detect_source_format(source_files)

        return ConversionResult(
            output_files=[iceberg_table_path],
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
