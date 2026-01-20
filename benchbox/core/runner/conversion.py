"""Format conversion orchestration for benchmark data.

This module provides orchestration for converting benchmark data between
different table formats (TBL, Parquet, Delta Lake, Apache Iceberg).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from benchbox.core.manifest import (
    ConvertedFileEntry,
    ManifestV2,
    TableFormats,
    load_manifest,
    upgrade_v1_to_v2,
    write_manifest,
)
from benchbox.utils.format_converters import (
    ConversionOptions,
    ConversionResult,
    DeltaConverter,
    IcebergConverter,
    ParquetConverter,
)

logger = logging.getLogger(__name__)


class FormatConversionOrchestrator:
    """Orchestrate table format conversion during benchmark runs."""

    def __init__(self) -> None:
        """Initialize the orchestrator with available converters."""
        self._converters = {
            "parquet": ParquetConverter(),
            "delta": DeltaConverter(),
            "iceberg": IcebergConverter(),
        }

    def convert_benchmark_tables(
        self,
        manifest_path: Path,
        output_dir: Path,
        target_format: str,
        schemas: dict[str, dict[str, Any]],
        options: ConversionOptions | None = None,
    ) -> dict[str, ConversionResult]:
        """Convert all tables in manifest to target format.

        Args:
            manifest_path: Path to source manifest file
            output_dir: Directory containing data files
            target_format: Target format (parquet, delta, iceberg)
            schemas: Dict mapping table_name -> schema dict with {"columns": [...]}
            options: Conversion options (compression, partitioning, etc.)

        Returns:
            Mapping of table_name → ConversionResult

        Raises:
            ValueError: If target format is unknown
            RuntimeError: If conversion fails
        """
        # 1. Load manifest (v1 or v2)
        logger.info(f"Loading manifest: {manifest_path}")
        manifest = load_manifest(manifest_path)

        # 2. Upgrade to v2 if needed
        if not isinstance(manifest, ManifestV2):
            logger.info("Upgrading manifest v1 to v2")
            manifest = upgrade_v1_to_v2(manifest)

        # 3. Get converter
        converter = self._get_converter(target_format)

        # 4. Convert each table
        results = {}
        tables_list = list(manifest.tables.keys())
        total_tables = len(tables_list)

        logger.info(f"Converting {total_tables} tables to {target_format}")

        for idx, table_name in enumerate(tables_list, 1):
            logger.info(f"[{idx}/{total_tables}] Converting table: {table_name}")

            # Get source files from manifest
            source_files = self._get_source_files(manifest, table_name, output_dir)

            if not source_files:
                logger.warning(f"No source files found for table: {table_name}")
                continue

            # Verify source files exist
            missing_files = [f for f in source_files if not f.exists()]
            if missing_files:
                logger.error(f"Missing source files for {table_name}: {missing_files}")
                continue

            # Get schema
            schema = schemas.get(table_name)
            if not schema:
                logger.warning(f"No schema found for table: {table_name}, skipping")
                continue

            # Convert
            try:
                result = converter.convert(
                    source_files=source_files,
                    table_name=table_name,
                    schema=schema,
                    options=options or ConversionOptions(),
                )
                results[table_name] = result

                # Log success
                compression_info = f"{result.compression_ratio:.2f}x" if result.compression_ratio > 0 else "N/A"
                logger.info(
                    f"[{idx}/{total_tables}] ✓ Converted {table_name}: "
                    f"{result.row_count:,} rows, "
                    f"compression: {compression_info}"
                )

            except Exception as e:
                logger.error(f"[{idx}/{total_tables}] ✗ Failed to convert {table_name}: {e}")
                raise RuntimeError(f"Conversion failed for table {table_name}") from e

        # 5. Update manifest with conversion results
        if results:
            logger.info("Updating manifest with conversion results")
            self._update_manifest_with_results(manifest, results, target_format, output_dir)

            # 6. Write manifest v2
            write_manifest(manifest, manifest_path)
            logger.info(f"Updated manifest: {manifest_path}")

        return results

    def _get_converter(self, format_name: str):
        """Get converter instance for format.

        Args:
            format_name: Format name (parquet, delta, iceberg)

        Returns:
            FormatConverter instance

        Raises:
            ValueError: If format is unknown
        """
        converter = self._converters.get(format_name.lower())
        if not converter:
            available = ", ".join(self._converters.keys())
            raise ValueError(f"Unknown format: {format_name}. Available formats: {available}")
        return converter

    def _get_source_files(self, manifest: ManifestV2, table_name: str, output_dir: Path) -> list[Path]:
        """Get source files for a table from manifest.

        Args:
            manifest: ManifestV2 instance
            table_name: Name of table
            output_dir: Base directory for data files

        Returns:
            List of absolute paths to source files
        """
        table_formats = manifest.tables.get(table_name)
        if not table_formats:
            return []

        # Try tbl format first (most common source for conversion)
        if "tbl" in table_formats.formats:
            files = table_formats.formats["tbl"]
            return [output_dir / f.path for f in files]

        # Fallback to first available format
        for format_name, files in table_formats.formats.items():
            logger.info(f"Using {format_name} files as source for {table_name}")
            return [output_dir / f.path for f in files]

        return []

    def _update_manifest_with_results(
        self,
        manifest: ManifestV2,
        results: dict[str, ConversionResult],
        target_format: str,
        output_dir: Path,
    ) -> None:
        """Update manifest with conversion results.

        Args:
            manifest: ManifestV2 instance to update
            results: Conversion results by table name
            target_format: Target format name
            output_dir: Base directory for data files
        """
        for table_name, result in results.items():
            table_formats = manifest.tables.get(table_name)
            if not table_formats:
                # Create new entry if table doesn't exist
                table_formats = TableFormats(formats={})
                manifest.tables[table_name] = table_formats

            # Add converted files to manifest
            converted_files = []
            for output_file in result.output_files:
                # Make path relative to output_dir
                try:
                    relative_path = output_file.relative_to(output_dir)
                except ValueError:
                    # If file is not under output_dir, use name only
                    relative_path = output_file.name

                file_entry = ConvertedFileEntry(
                    path=str(relative_path),
                    size_bytes=result.output_size_bytes,
                    row_count=result.row_count,
                    converted_from=result.source_format,
                    converted_at=result.converted_at,
                    compression=result.conversion_options.get("compression"),
                    row_groups=result.metadata.get("row_groups"),
                    conversion_options=result.conversion_options,
                )
                converted_files.append(file_entry)

            # Update format in manifest
            table_formats.formats[target_format] = converted_files

        # Update format preference to prefer converted format
        if target_format not in manifest.format_preference:
            # Insert at beginning to make it the preferred format
            manifest.format_preference.insert(0, target_format)
