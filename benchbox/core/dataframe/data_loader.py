"""DataFrame Data Loading Strategy and Format Management.

This module provides comprehensive data loading infrastructure for DataFrame
benchmarking, including:
- Format-aware loading (CSV, TBL, Parquet, Arrow)
- CSV-to-Parquet conversion for optimal performance
- Schema mapping from TPC-H/TPC-DS to DataFrame types
- Cache directory management
- Integration with TPC data generators

Architecture:
1. DataFrameDataLoader orchestrates data preparation
2. SchemaMapper converts TPC schemas to platform-specific dtypes
3. FormatConverter handles CSV→Parquet conversion
4. DataCache manages cached/converted files

Usage:
    from benchbox.core.dataframe.data_loader import DataFrameDataLoader

    loader = DataFrameDataLoader(platform="polars", data_dir=Path("./data"))

    # Prepare data for benchmark
    paths = loader.prepare_benchmark_data(benchmark, scale_factor=1.0)

    # Load into platform-specific DataFrames
    tables = loader.load_tables(paths, adapter)

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchbox.core.dataframe.capabilities import (
    DataFormat,
    get_platform_capabilities,
)
from benchbox.core.dataframe.tuning.write_config import (
    DataFrameWriteConfiguration,
)

if TYPE_CHECKING:
    from benchbox.core.tpch.schema import Table

logger = logging.getLogger(__name__)

# Default cache directory (can be overridden via environment)
DEFAULT_CACHE_DIR = Path.home() / ".benchbox" / "dataframe-data"


class ConversionStatus(Enum):
    """Status of format conversion."""

    NOT_NEEDED = "not_needed"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class LoadedTable:
    """Information about a loaded table."""

    table_name: str
    file_path: Path
    format: DataFormat
    row_count: int | None = None
    size_bytes: int | None = None
    load_time_seconds: float | None = None


@dataclass
class DataLoadResult:
    """Result of data loading operation."""

    tables: dict[str, LoadedTable] = field(default_factory=dict)
    total_load_time_seconds: float = 0.0
    source_format: DataFormat = DataFormat.CSV
    target_format: DataFormat = DataFormat.PARQUET
    conversion_performed: bool = False
    cache_hit: bool = False
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """Check if load was successful."""
        return len(self.errors) == 0 and len(self.tables) > 0


@dataclass
class CacheManifest:
    """Manifest tracking cached data files."""

    benchmark: str
    scale_factor: float
    format: str
    created_at: str
    source_hash: str
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "benchmark": self.benchmark,
            "scale_factor": self.scale_factor,
            "format": self.format,
            "created_at": self.created_at,
            "source_hash": self.source_hash,
            "tables": self.tables,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CacheManifest:
        """Create from dictionary."""
        return cls(
            benchmark=data["benchmark"],
            scale_factor=data["scale_factor"],
            format=data["format"],
            created_at=data["created_at"],
            source_hash=data["source_hash"],
            tables=data.get("tables", {}),
        )


class SchemaMapper:
    """Maps TPC benchmark schemas to DataFrame-specific types.

    This class converts BenchBox schema definitions (Column, Table) to
    platform-specific DataFrame column types.
    """

    # Mapping from TPC DataType enum values to Polars types
    POLARS_TYPE_MAP: dict[str, str] = {
        "INTEGER": "Int64",
        "DECIMAL(15,2)": "Float64",  # Polars doesn't have native Decimal
        "VARCHAR": "Utf8",
        "CHAR": "Utf8",
        "DATE": "Date",
    }

    # Mapping from TPC DataType enum values to Pandas types
    PANDAS_TYPE_MAP: dict[str, str] = {
        "INTEGER": "int64",
        "DECIMAL(15,2)": "float64",
        "VARCHAR": "object",
        "CHAR": "object",
        "DATE": "datetime64[ns]",
    }

    # Mapping for PyArrow types (used in Parquet conversion)
    PYARROW_TYPE_MAP: dict[str, str] = {
        "INTEGER": "int64",
        "DECIMAL(15,2)": "float64",
        "VARCHAR": "string",
        "CHAR": "string",
        "DATE": "date32",
    }

    @classmethod
    def get_column_names(cls, table: Table) -> list[str]:
        """Extract column names from a Table schema.

        Args:
            table: TPC schema Table object

        Returns:
            List of column names
        """
        return [col.name for col in table.columns]

    @classmethod
    def get_polars_schema(cls, table: Table) -> dict[str, str]:
        """Convert Table schema to Polars column types.

        Args:
            table: TPC schema Table object

        Returns:
            Dictionary mapping column name to Polars type string
        """
        schema = {}
        for col in table.columns:
            dtype_value = col.data_type.value
            polars_type = cls.POLARS_TYPE_MAP.get(dtype_value, "Utf8")
            schema[col.name] = polars_type
        return schema

    @classmethod
    def get_pandas_schema(cls, table: Table) -> dict[str, str]:
        """Convert Table schema to Pandas column types.

        Args:
            table: TPC schema Table object

        Returns:
            Dictionary mapping column name to Pandas dtype string
        """
        schema = {}
        for col in table.columns:
            dtype_value = col.data_type.value
            pandas_type = cls.PANDAS_TYPE_MAP.get(dtype_value, "object")
            schema[col.name] = pandas_type
        return schema

    @classmethod
    def get_pyarrow_schema(cls, table: Table) -> dict[str, str]:
        """Convert Table schema to PyArrow column types.

        Args:
            table: TPC schema Table object

        Returns:
            Dictionary mapping column name to PyArrow type string
        """
        schema = {}
        for col in table.columns:
            dtype_value = col.data_type.value
            arrow_type = cls.PYARROW_TYPE_MAP.get(dtype_value, "string")
            schema[col.name] = arrow_type
        return schema


class FormatConverter:
    """Converts data between file formats.

    Primarily handles CSV/TBL → Parquet conversion for optimal DataFrame
    performance.
    """

    @staticmethod
    def convert_csv_to_parquet(
        source_path: Path,
        target_path: Path,
        column_names: list[str] | None = None,
        delimiter: str = "|",
        compression: str = "zstd",
        write_config: DataFrameWriteConfiguration | None = None,
    ) -> tuple[ConversionStatus, int]:
        """Convert CSV/TBL file to Parquet format.

        Uses PyArrow for efficient conversion with proper type inference
        and compression. Supports physical layout options via write_config.

        Args:
            source_path: Path to source CSV/TBL file
            target_path: Path for output Parquet file
            column_names: Optional column names (for headerless files)
            delimiter: Field delimiter (default "|" for TBL files)
            compression: Parquet compression codec (zstd, snappy, gzip)
            write_config: Optional write configuration for physical layout

        Returns:
            Tuple of (conversion status, row count)
        """
        try:
            import pyarrow.csv as pv
            import pyarrow.parquet as pq
        except ImportError as e:
            logger.error(f"PyArrow not installed: {e}")
            return ConversionStatus.FAILED, 0

        try:
            # Handle TBL files with trailing delimiter (TPC spec)
            # Add a dummy column name to account for the trailing delimiter
            is_tbl_file = str(source_path).endswith(".tbl")
            actual_column_names = column_names
            if is_tbl_file and column_names:
                # TBL files have trailing delimiter, add dummy column
                actual_column_names = column_names + ["_trailing_"]

            # Read options for CSV
            read_options = pv.ReadOptions(
                column_names=actual_column_names if actual_column_names else None,
            )

            parse_options = pv.ParseOptions(delimiter=delimiter)

            # Convert options for optimal Parquet output
            convert_options = pv.ConvertOptions(
                auto_dict_encode=True,
                strings_can_be_null=True,
            )

            # Read the CSV file
            logger.debug(f"Reading {source_path}")
            table = pv.read_csv(
                source_path,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options,
            )

            # Drop trailing column for TBL files
            if is_tbl_file and column_names:
                # Select only the original columns (exclude _trailing_)
                table = table.select(column_names)

            # Apply physical layout options from write_config
            if write_config:
                table = FormatConverter._apply_write_config(table, write_config)
                # Use compression from write_config if specified and different from default
                if write_config.compression != "zstd":
                    compression = write_config.compression

            # Ensure target directory exists
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Build Parquet write options
            write_kwargs: dict[str, Any] = {
                "compression": compression,
                "use_dictionary": True,
            }

            # Apply write_config options to Parquet writer
            if write_config:
                # Row group size
                if write_config.row_group_size is not None:
                    write_kwargs["row_group_size"] = write_config.row_group_size

                # Compression level (PyArrow uses compression_level kwarg)
                if write_config.compression_level is not None:
                    write_kwargs["compression_level"] = write_config.compression_level

                # Dictionary columns (use_dictionary can be list of column names)
                if write_config.dictionary_columns:
                    write_kwargs["use_dictionary"] = write_config.dictionary_columns
                elif write_config.skip_dictionary_columns:
                    # Exclude specific columns from dictionary encoding
                    all_cols = set(table.column_names)
                    dict_cols = list(all_cols - set(write_config.skip_dictionary_columns))
                    write_kwargs["use_dictionary"] = dict_cols

            # Write Parquet
            logger.debug(f"Writing {target_path}")
            pq.write_table(table, target_path, **write_kwargs)

            row_count = table.num_rows
            logger.info(f"Converted {source_path.name} → {target_path.name}: {row_count:,} rows")

            return ConversionStatus.SUCCESS, row_count

        except Exception as e:
            logger.error(f"Conversion failed for {source_path}: {e}")
            return ConversionStatus.FAILED, 0

    @staticmethod
    def _apply_write_config(
        table: Any,  # PyArrow Table
        write_config: DataFrameWriteConfiguration,
    ) -> Any:
        """Apply write configuration transformations to a PyArrow table.

        This includes sorting the table based on sort_by columns.

        Args:
            table: PyArrow Table to transform
            write_config: Write configuration specifying transformations

        Returns:
            Transformed PyArrow Table
        """
        import pyarrow.compute as pc

        # Apply sorting if specified
        if write_config.sort_by:
            # Build sort keys from write_config
            sort_keys = []
            for sort_col in write_config.sort_by:
                # Validate column exists
                if sort_col.name not in table.column_names:
                    logger.warning(f"Sort column '{sort_col.name}' not found in table, skipping")
                    continue
                # PyArrow sort_indices uses "ascending" or "descending"
                order = "ascending" if sort_col.order == "asc" else "descending"
                sort_keys.append((sort_col.name, order))

            if sort_keys:
                logger.debug(f"Sorting table by {sort_keys}")
                # Get sort indices
                indices = pc.sort_indices(table, sort_keys=sort_keys)
                # Reorder table using indices
                table = table.take(indices)

        return table


class DataCache:
    """Manages cached DataFrame data files.

    Cache structure:
        ~/.benchbox/dataframe-data/
          tpch/
            sf_1.0/
              parquet/
                _manifest.json
                customer.parquet
                lineitem.parquet
                ...
            sf_0.01/
              parquet/
                ...
          tpcds/
            ...
    """

    def __init__(self, cache_dir: Path | None = None):
        """Initialize the data cache.

        Args:
            cache_dir: Custom cache directory (default: ~/.benchbox/dataframe-data/)
        """
        self.cache_dir = cache_dir or Path(os.environ.get("BENCHBOX_CACHE_DIR", str(DEFAULT_CACHE_DIR)))

    def get_cache_path(self, benchmark: str, scale_factor: float, format: DataFormat) -> Path:
        """Get the cache path for a benchmark/SF/format combination.

        Args:
            benchmark: Benchmark name (tpch, tpcds)
            scale_factor: Scale factor
            format: Target format

        Returns:
            Path to cache directory
        """
        sf_str = f"sf_{scale_factor}"
        return self.cache_dir / benchmark / sf_str / format.value

    def get_manifest_path(self, benchmark: str, scale_factor: float, format: DataFormat) -> Path:
        """Get the manifest file path.

        Args:
            benchmark: Benchmark name
            scale_factor: Scale factor
            format: Target format

        Returns:
            Path to manifest JSON file
        """
        cache_path = self.get_cache_path(benchmark, scale_factor, format)
        return cache_path / "_manifest.json"

    def has_cached_data(
        self,
        benchmark: str,
        scale_factor: float,
        format: DataFormat,
        source_hash: str | None = None,
    ) -> bool:
        """Check if cached data exists and is valid.

        Args:
            benchmark: Benchmark name
            scale_factor: Scale factor
            format: Target format
            source_hash: Optional hash to validate against

        Returns:
            True if valid cached data exists
        """
        manifest_path = self.get_manifest_path(benchmark, scale_factor, format)

        if not manifest_path.exists():
            return False

        try:
            with open(manifest_path) as f:
                manifest = CacheManifest.from_dict(json.load(f))

            # Validate source hash if provided
            if source_hash and manifest.source_hash != source_hash:
                logger.debug(f"Cache hash mismatch: {manifest.source_hash} != {source_hash}")
                return False

            # Verify all table files exist
            cache_dir = manifest_path.parent
            for table_name, table_info in manifest.tables.items():
                file_path = cache_dir / table_info["file"]
                if not file_path.exists():
                    logger.debug(f"Cache file missing: {file_path}")
                    return False

            return True

        except (json.JSONDecodeError, KeyError) as e:
            logger.debug(f"Invalid cache manifest: {e}")
            return False

    def get_cached_files(self, benchmark: str, scale_factor: float, format: DataFormat) -> dict[str, Path] | None:
        """Get cached data file paths.

        Args:
            benchmark: Benchmark name
            scale_factor: Scale factor
            format: Target format

        Returns:
            Dictionary mapping table name to file path, or None if not cached
        """
        manifest_path = self.get_manifest_path(benchmark, scale_factor, format)

        if not manifest_path.exists():
            return None

        try:
            with open(manifest_path) as f:
                manifest = CacheManifest.from_dict(json.load(f))

            cache_dir = manifest_path.parent
            return {table_name: cache_dir / table_info["file"] for table_name, table_info in manifest.tables.items()}

        except (json.JSONDecodeError, KeyError):
            return None

    def save_manifest(
        self,
        benchmark: str,
        scale_factor: float,
        format: DataFormat,
        source_hash: str,
        tables: dict[str, dict[str, Any]],
    ) -> None:
        """Save a cache manifest.

        Args:
            benchmark: Benchmark name
            scale_factor: Scale factor
            format: Target format
            source_hash: Hash of source files
            tables: Table metadata (file paths, row counts)
        """
        manifest = CacheManifest(
            benchmark=benchmark,
            scale_factor=scale_factor,
            format=format.value,
            created_at=datetime.now().astimezone().isoformat(),
            source_hash=source_hash,
            tables=tables,
        )

        manifest_path = self.get_manifest_path(benchmark, scale_factor, format)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(manifest_path, "w") as f:
            json.dump(manifest.to_dict(), f, indent=2)

    def clear_cache(
        self,
        benchmark: str | None = None,
        scale_factor: float | None = None,
        format: DataFormat | None = None,
    ) -> int:
        """Clear cached data.

        Args:
            benchmark: Optional benchmark to clear (clears all if None)
            scale_factor: Optional SF to clear
            format: Optional format to clear

        Returns:
            Number of files removed
        """
        if not self.cache_dir.exists():
            return 0

        removed = 0

        if benchmark is None:
            # Clear entire cache
            shutil.rmtree(self.cache_dir, ignore_errors=True)
            removed = -1  # Indicate full clear
        elif scale_factor is None:
            # Clear specific benchmark
            benchmark_dir = self.cache_dir / benchmark
            if benchmark_dir.exists():
                shutil.rmtree(benchmark_dir, ignore_errors=True)
                removed = -1
        elif format is None:
            # Clear specific SF
            sf_dir = self.cache_dir / benchmark / f"sf_{scale_factor}"
            if sf_dir.exists():
                shutil.rmtree(sf_dir, ignore_errors=True)
                removed = -1
        else:
            # Clear specific format
            cache_path = self.get_cache_path(benchmark, scale_factor, format)
            if cache_path.exists():
                for f in cache_path.iterdir():
                    f.unlink()
                    removed += 1
                cache_path.rmdir()

        return removed


def _compute_source_hash(source_dir: Path, tables: dict[str, Path]) -> str:
    """Compute hash of source files for cache validation.

    Uses file modification times for fast validation.

    Args:
        source_dir: Source data directory
        tables: Dictionary of table name to file path

    Returns:
        Hash string
    """
    hash_data = []
    for table_name in sorted(tables.keys()):
        file_path = tables[table_name]
        if file_path.exists():
            stat = file_path.stat()
            hash_data.append(f"{table_name}:{stat.st_mtime}:{stat.st_size}")

    combined = "|".join(hash_data)
    return hashlib.md5(combined.encode()).hexdigest()[:12]


class DataFrameDataLoader:
    """Main orchestrator for DataFrame data loading.

    Coordinates data preparation for DataFrame benchmarking:
    1. Determines optimal format for target platform
    2. Checks cache for existing converted data
    3. Converts source data if needed
    4. Returns paths to ready-to-load files

    Usage:
        loader = DataFrameDataLoader(platform="polars")

        # Prepare data (converts to Parquet if needed)
        paths = loader.prepare_benchmark_data(benchmark, scale_factor=1.0)

        # paths = {"customer": Path(".../customer.parquet"), ...}
    """

    def __init__(
        self,
        platform: str = "polars",
        cache_dir: Path | None = None,
        prefer_parquet: bool = True,
        force_regenerate: bool = False,
        write_config: DataFrameWriteConfiguration | None = None,
    ):
        """Initialize the data loader.

        Args:
            platform: Target DataFrame platform (polars, pandas, etc.)
            cache_dir: Custom cache directory
            prefer_parquet: Prefer Parquet format for performance
            force_regenerate: Force regeneration even if cached
            write_config: Optional write configuration for physical layout
        """
        self.platform = platform.lower().replace("-df", "")
        self.cache = DataCache(cache_dir)
        self.prefer_parquet = prefer_parquet
        self.force_regenerate = force_regenerate
        self.write_config = write_config

        # Get platform capabilities
        try:
            self.capabilities = get_platform_capabilities(self.platform)
        except ValueError:
            # Unknown platform - use defaults
            self.capabilities = None
            logger.warning(f"Unknown platform '{platform}', using default settings")

    def get_optimal_format(self, scale_factor: float) -> DataFormat:
        """Determine optimal data format for platform and scale factor.

        Args:
            scale_factor: Data scale factor

        Returns:
            Recommended DataFormat
        """
        if not self.prefer_parquet:
            return DataFormat.CSV

        # Platform-specific recommendations
        if self.capabilities:
            recommended = self.capabilities.recommended_data_format
            # recommended is already a DataFormat enum
            if isinstance(recommended, DataFormat):
                return recommended
            # Handle string comparison for backwards compatibility
            elif recommended == "parquet":
                return DataFormat.PARQUET
            elif recommended == "arrow":
                return DataFormat.ARROW
            else:
                return DataFormat.CSV

        # Default: Parquet for larger datasets
        if scale_factor >= 0.1:
            return DataFormat.PARQUET
        return DataFormat.CSV

    def prepare_benchmark_data(
        self,
        benchmark: Any,
        scale_factor: float,
        data_dir: Path | None = None,
        write_config: DataFrameWriteConfiguration | None = None,
    ) -> dict[str, Path]:
        """Prepare benchmark data files for DataFrame loading.

        This method:
        1. Locates source data files (from benchmark.tables or data_dir)
        2. Determines optimal format for the target platform
        3. Converts to optimal format if needed (using cache)
        4. Returns paths to ready-to-load files

        Args:
            benchmark: Benchmark instance with schema info
            scale_factor: Data scale factor
            data_dir: Optional data directory (overrides benchmark.tables)
            write_config: Optional write configuration for physical layout
                (overrides self.write_config if provided)

        Returns:
            Dictionary mapping table name to data file path
        """
        # Get benchmark name
        benchmark_name = getattr(benchmark, "name", "unknown").lower()

        # Determine source files
        source_files = self._get_source_files(benchmark, data_dir)
        if not source_files:
            raise ValueError("No source data files found")

        # Determine target format
        target_format = self.get_optimal_format(scale_factor)

        # Check if source is already in target format
        source_format = self._detect_source_format(source_files)
        if source_format == target_format:
            logger.info(f"Source data already in {target_format.value} format")
            return source_files

        # Use provided write_config or fall back to instance config
        effective_write_config = write_config or self.write_config

        # Check cache
        source_hash = _compute_source_hash(
            data_dir or Path(list(source_files.values())[0]).parent,
            source_files,
        )

        # Include write_config in cache key if it affects output
        if effective_write_config and not effective_write_config.is_default():
            # Add write config hash to source hash to differentiate cached outputs
            import json

            config_str = json.dumps(effective_write_config.to_dict(), sort_keys=True)
            source_hash = hashlib.md5(f"{source_hash}:{config_str}".encode()).hexdigest()[:12]

        if not self.force_regenerate and self.cache.has_cached_data(
            benchmark_name, scale_factor, target_format, source_hash
        ):
            cached = self.cache.get_cached_files(benchmark_name, scale_factor, target_format)
            if cached:
                logger.info(f"Using cached {target_format.value} data")
                return cached

        # Convert to target format
        return self._convert_data(
            benchmark=benchmark,
            benchmark_name=benchmark_name,
            scale_factor=scale_factor,
            source_files=source_files,
            source_format=source_format,
            target_format=target_format,
            source_hash=source_hash,
            write_config=effective_write_config,
        )

    def _get_source_files(self, benchmark: Any, data_dir: Path | None) -> dict[str, Path]:
        """Get source data file paths.

        Args:
            benchmark: Benchmark instance
            data_dir: Optional data directory

        Returns:
            Dictionary mapping table name to file path
        """
        # First try benchmark.tables attribute
        if hasattr(benchmark, "tables") and benchmark.tables:
            tables = benchmark.tables
            if isinstance(tables, dict):
                return {name: Path(path) if not isinstance(path, Path) else path for name, path in tables.items()}

        # Then try data_dir
        if data_dir and data_dir.exists():
            return self._discover_files(data_dir)

        # Finally try benchmark._impl.tables
        if hasattr(benchmark, "_impl") and hasattr(benchmark._impl, "tables"):
            tables = benchmark._impl.tables
            if isinstance(tables, dict):
                return {name: Path(path) if not isinstance(path, Path) else path for name, path in tables.items()}

        return {}

    def _discover_files(self, data_dir: Path) -> dict[str, Path]:
        """Discover data files in a directory.

        Args:
            data_dir: Directory to scan

        Returns:
            Dictionary mapping table name to file path
        """
        files = {}

        for pattern in ["*.tbl", "*.csv", "*.parquet"]:
            for file_path in data_dir.glob(pattern):
                # Skip directories (e.g., *.parquet directories used for database output)
                if file_path.is_dir():
                    # Check if this is a parquet directory containing table files
                    # (e.g., tpch_sf001.parquet/ containing lineitem.parquet, etc.)
                    parquet_files = list(file_path.glob("*.parquet"))
                    if parquet_files:
                        # This is a valid parquet output directory - recurse into it
                        for pq_file in parquet_files:
                            if pq_file.is_file():
                                table_name = pq_file.stem.lower()
                                if table_name not in files:
                                    files[table_name] = pq_file
                    continue

                # Table name is stem of filename
                table_name = file_path.stem.lower()
                # Prefer existing entry if already found (TBL > CSV priority)
                if table_name not in files:
                    files[table_name] = file_path

        return files

    def _detect_source_format(self, source_files: dict[str, Path]) -> DataFormat:
        """Detect the format of source files.

        Args:
            source_files: Dictionary of table name to file path

        Returns:
            Detected DataFormat
        """
        for path in source_files.values():
            suffix = path.suffix.lower()
            if suffix == ".parquet":
                return DataFormat.PARQUET
            elif suffix == ".tbl":
                return DataFormat.CSV  # TBL is CSV with different delimiter
            elif suffix == ".arrow":
                return DataFormat.ARROW

        return DataFormat.CSV

    def _convert_data(
        self,
        benchmark: Any,
        benchmark_name: str,
        scale_factor: float,
        source_files: dict[str, Path],
        source_format: DataFormat,
        target_format: DataFormat,
        source_hash: str,
        write_config: DataFrameWriteConfiguration | None = None,
    ) -> dict[str, Path]:
        """Convert data files to target format.

        Args:
            benchmark: Benchmark instance for schema info
            benchmark_name: Name of benchmark
            scale_factor: Scale factor
            source_files: Source file paths
            source_format: Source format
            target_format: Target format
            source_hash: Hash of source files
            write_config: Optional write configuration for physical layout

        Returns:
            Dictionary mapping table name to converted file path
        """
        if target_format != DataFormat.PARQUET:
            # Currently only support CSV→Parquet conversion
            logger.warning(f"Conversion to {target_format.value} not supported, using source")
            return source_files

        # Get cache path for output
        cache_path = self.cache.get_cache_path(benchmark_name, scale_factor, target_format)
        cache_path.mkdir(parents=True, exist_ok=True)

        # Log write config if provided
        if write_config and not write_config.is_default():
            enabled_types = write_config.get_enabled_types()
            logger.info(
                f"Converting {len(source_files)} tables from CSV/TBL to Parquet "
                f"with write tuning: {[t.value for t in enabled_types]}"
            )
        else:
            logger.info(f"Converting {len(source_files)} tables from CSV/TBL to Parquet")

        # Get schema info for column names
        schema_info = self._get_schema_info(benchmark)

        converted_files = {}
        table_metadata = {}

        for table_name, source_path in source_files.items():
            target_path = cache_path / f"{table_name}.parquet"

            # Get column names from schema
            column_names = schema_info.get(table_name)

            # Determine delimiter
            delimiter = "|" if source_path.suffix.lower() == ".tbl" else ","

            # Get table-specific write config (filter sort columns that exist in this table)
            table_write_config = self._get_table_write_config(write_config, table_name, column_names)

            # Convert
            status, row_count = FormatConverter.convert_csv_to_parquet(
                source_path=source_path,
                target_path=target_path,
                column_names=column_names,
                delimiter=delimiter,
                write_config=table_write_config,
            )

            if status == ConversionStatus.SUCCESS:
                converted_files[table_name] = target_path
                table_metadata[table_name] = {
                    "file": f"{table_name}.parquet",
                    "row_count": row_count,
                    "source": source_path.name,
                }
                # Record write config if applied
                if table_write_config and not table_write_config.is_default():
                    table_metadata[table_name]["write_config"] = table_write_config.to_dict()
            else:
                logger.error(f"Failed to convert {table_name}, using source file")
                converted_files[table_name] = source_path

        # Save manifest
        self.cache.save_manifest(
            benchmark=benchmark_name,
            scale_factor=scale_factor,
            format=target_format,
            source_hash=source_hash,
            tables=table_metadata,
        )

        return converted_files

    def _get_table_write_config(
        self,
        write_config: DataFrameWriteConfiguration | None,
        table_name: str,
        column_names: list[str] | None,
    ) -> DataFrameWriteConfiguration | None:
        """Get write configuration filtered for a specific table.

        Filters out sort columns that don't exist in this table's schema.

        Args:
            write_config: Base write configuration
            table_name: Name of the table
            column_names: Column names in the table

        Returns:
            Filtered write configuration or None
        """
        if write_config is None or write_config.is_default():
            return write_config

        if column_names is None:
            # Can't filter without column names
            return write_config

        # Filter sort_by columns to only include those that exist in this table
        valid_columns = set(column_names)
        filtered_sort_by = [s for s in write_config.sort_by if s.name in valid_columns]

        # Filter partition_by columns similarly
        filtered_partition_by = [p for p in write_config.partition_by if p.name in valid_columns]

        # Filter dictionary columns
        filtered_dict_cols = [c for c in write_config.dictionary_columns if c in valid_columns]
        filtered_skip_dict_cols = [c for c in write_config.skip_dictionary_columns if c in valid_columns]

        # Return a new config with filtered columns
        return DataFrameWriteConfiguration(
            partition_by=filtered_partition_by,
            sort_by=filtered_sort_by,
            row_group_size=write_config.row_group_size,
            target_file_size_mb=write_config.target_file_size_mb,
            repartition_count=write_config.repartition_count,
            compression=write_config.compression,
            compression_level=write_config.compression_level,
            dictionary_columns=filtered_dict_cols,
            skip_dictionary_columns=filtered_skip_dict_cols,
        )

    def _get_schema_info(self, benchmark: Any) -> dict[str, list[str]]:
        """Extract column names from benchmark schema.

        Args:
            benchmark: Benchmark instance

        Returns:
            Dictionary mapping table name to list of column names
        """
        schema_info: dict[str, list[str]] = {}

        # Try get_schema() method
        if hasattr(benchmark, "get_schema"):
            try:
                schema = benchmark.get_schema()
                for table_name, table_schema in schema.items():
                    columns = table_schema.get("columns", [])
                    schema_info[table_name.lower()] = [c["name"] for c in columns]
            except Exception:
                pass

        # Try schema module for TPC-H
        if not schema_info:
            try:
                from benchbox.core.tpch.schema import TABLES

                for table in TABLES:
                    schema_info[table.name.lower()] = [col.name for col in table.columns]
            except ImportError:
                pass

        return schema_info


def get_tpch_column_names() -> dict[str, list[str]]:
    """Get column names for all TPC-H tables.

    Returns:
        Dictionary mapping table name to column names
    """
    try:
        from benchbox.core.tpch.schema import TABLES

        return {table.name.lower(): [col.name for col in table.columns] for table in TABLES}
    except ImportError:
        # Fallback hardcoded columns (TPC-H spec)
        return {
            "region": ["r_regionkey", "r_name", "r_comment"],
            "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
            "supplier": ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"],
            "part": [
                "p_partkey",
                "p_name",
                "p_mfgr",
                "p_brand",
                "p_type",
                "p_size",
                "p_container",
                "p_retailprice",
                "p_comment",
            ],
            "partsupp": ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"],
            "customer": [
                "c_custkey",
                "c_name",
                "c_address",
                "c_nationkey",
                "c_phone",
                "c_acctbal",
                "c_mktsegment",
                "c_comment",
            ],
            "orders": [
                "o_orderkey",
                "o_custkey",
                "o_orderstatus",
                "o_totalprice",
                "o_orderdate",
                "o_orderpriority",
                "o_clerk",
                "o_shippriority",
                "o_comment",
            ],
            "lineitem": [
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "l_linenumber",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_tax",
                "l_returnflag",
                "l_linestatus",
                "l_shipdate",
                "l_commitdate",
                "l_receiptdate",
                "l_shipinstruct",
                "l_shipmode",
                "l_comment",
            ],
        }


def get_tpcds_column_names() -> dict[str, list[str]]:
    """Get column names for TPC-DS tables.

    Returns:
        Dictionary mapping table name to column names
    """
    try:
        from benchbox.core.tpcds.schema import TABLES

        return {table.name.lower(): [col.name for col in table.columns] for table in TABLES}
    except ImportError:
        # TPC-DS has ~24 tables, return empty and let schema inference handle it
        return {}
