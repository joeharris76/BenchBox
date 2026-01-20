"""DataFusion platform adapter with data loading and query execution.

Provides Apache DataFusion-specific optimizations for in-memory OLAP workloads,
supporting both CSV and Parquet formats with automatic conversion options.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

try:
    from datafusion import SessionConfig, SessionContext

    try:
        from datafusion import RuntimeEnv
    except ImportError:
        # Newer versions use RuntimeEnvBuilder
        from datafusion import RuntimeEnvBuilder as RuntimeEnv
except ImportError:
    SessionContext = None  # type: ignore[assignment, misc]
    SessionConfig = None  # type: ignore[assignment, misc]
    RuntimeEnv = None  # type: ignore[assignment, misc]

from benchbox.platforms.base import PlatformAdapter

logger = logging.getLogger(__name__)


class DataFusionAdapter(PlatformAdapter):
    """Apache DataFusion platform adapter with optimized bulk loading and execution."""

    @property
    def platform_name(self) -> str:
        return "DataFusion"

    def get_target_dialect(self) -> str:
        """Get the target SQL dialect for DataFusion.

        DataFusion uses PostgreSQL-compatible SQL dialect.
        """
        return "postgres"

    @staticmethod
    def add_cli_arguments(parser) -> None:
        """Add DataFusion-specific CLI arguments."""
        datafusion_group = parser.add_argument_group("DataFusion Arguments")
        datafusion_group.add_argument(
            "--datafusion-memory-limit",
            type=str,
            default="16G",
            help="DataFusion memory limit (e.g., '16G', '8GB', '4096MB')",
        )
        datafusion_group.add_argument(
            "--datafusion-partitions",
            type=int,
            default=None,
            help="Number of parallel partitions (default: CPU count)",
        )
        datafusion_group.add_argument(
            "--datafusion-format",
            type=str,
            choices=["csv", "parquet"],
            default="parquet",
            help="Data format to use (parquet recommended for performance)",
        )
        datafusion_group.add_argument(
            "--datafusion-temp-dir",
            type=str,
            default=None,
            help="Temporary directory for disk spilling",
        )
        datafusion_group.add_argument(
            "--datafusion-batch-size",
            type=int,
            default=8192,
            help="RecordBatch size for query execution",
        )
        datafusion_group.add_argument(
            "--datafusion-working-dir",
            type=str,
            help="Working directory for DataFusion tables and data",
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]):
        """Create DataFusion adapter from unified configuration."""
        from pathlib import Path

        from benchbox.utils.database_naming import generate_database_filename
        from benchbox.utils.scale_factor import format_benchmark_name

        # Extract DataFusion-specific configuration
        adapter_config = {}

        # Working directory handling (similar to database path for file-based DBs)
        if config.get("working_dir"):
            adapter_config["working_dir"] = config["working_dir"]
        else:
            # Generate database directory path using standard naming convention
            # DataFusion stores data in Parquet format (.parquet extension determined by platform)
            from benchbox.utils.path_utils import get_benchmark_runs_datagen_path

            # Use output_dir if provided, otherwise use canonical benchmark_runs/datagen path
            if config.get("output_dir"):
                data_dir = Path(config["output_dir"]) / format_benchmark_name(
                    config["benchmark"], config["scale_factor"]
                )
            else:
                data_dir = get_benchmark_runs_datagen_path(config["benchmark"], config["scale_factor"])

            db_filename = generate_database_filename(
                benchmark_name=config["benchmark"],
                scale_factor=config["scale_factor"],
                platform="datafusion",
                tuning_config=config.get("tuning_config"),
            )

            # Full path to DataFusion working directory
            working_dir = data_dir / db_filename
            adapter_config["working_dir"] = str(working_dir)
            working_dir.mkdir(parents=True, exist_ok=True)

        # Memory limit
        adapter_config["memory_limit"] = config.get("memory_limit", "16G")

        # Parallelism (default to CPU count)
        adapter_config["target_partitions"] = config.get("partitions") or os.cpu_count()

        # Data format
        adapter_config["data_format"] = config.get("format", "parquet")

        # Temp directory for spilling
        adapter_config["temp_dir"] = config.get("temp_dir")

        # Batch size
        adapter_config["batch_size"] = config.get("batch_size", 8192)

        # Force recreate
        adapter_config["force_recreate"] = config.get("force", False)

        # Pass through other relevant config
        for key in ["tuning_config", "verbose_enabled", "very_verbose"]:
            if key in config:
                adapter_config[key] = config[key]

        return cls(**adapter_config)

    def __init__(self, **config):
        super().__init__(**config)
        if SessionContext is None:
            raise ImportError("DataFusion not installed. Install with: pip install datafusion")

        # DataFusion configuration
        self.working_dir = Path(config.get("working_dir", "./datafusion_working"))
        self.memory_limit = config.get("memory_limit", "16G")
        self.target_partitions = config.get("target_partitions", os.cpu_count())
        self.data_format = config.get("data_format", "parquet")
        self.temp_dir = config.get("temp_dir")
        self.batch_size = config.get("batch_size", 8192)

        # Schema tracking (populated during create_schema)
        self._table_schemas = {}

        # Create working directory
        self.working_dir.mkdir(parents=True, exist_ok=True)

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get DataFusion platform information."""
        platform_info = {
            "platform_type": "datafusion",
            "platform_name": "DataFusion",
            "connection_mode": "in-memory",
            "configuration": {
                "working_dir": str(self.working_dir),
                "memory_limit": self.memory_limit,
                "target_partitions": self.target_partitions,
                "data_format": self.data_format,
                "temp_dir": self.temp_dir,
                "batch_size": self.batch_size,
                "result_cache_enabled": False,
            },
        }

        # Get DataFusion version
        try:
            import datafusion as df_module

            platform_info["client_library_version"] = df_module.__version__
            platform_info["platform_version"] = df_module.__version__
        except (ImportError, AttributeError):
            platform_info["client_library_version"] = None
            platform_info["platform_version"] = None

        return platform_info

    def create_connection(self, **connection_config) -> Any:
        """Create DataFusion SessionContext with optimized configuration."""
        self.log_operation_start("DataFusion connection")

        # Handle existing database using base class method
        self.handle_existing_database(**connection_config)

        # Configure runtime environment for disk spilling and memory management
        # Note: RuntimeEnv/RuntimeEnvBuilder API varies by version
        runtime = None
        if RuntimeEnv is not None:
            try:
                # Check if this is RuntimeEnvBuilder (newer API)
                if hasattr(RuntimeEnv, "build"):
                    # RuntimeEnvBuilder API
                    builder = RuntimeEnv()

                    # Configure memory pool using fair spill pool
                    # This replaces the invalid config.set("memory_pool_size") approach
                    if self.memory_limit:
                        memory_bytes = int(self._parse_memory_limit(self.memory_limit))
                        builder = builder.with_fair_spill_pool(memory_bytes)
                        self.log_very_verbose(
                            f"Configured fair spill pool: {self.memory_limit} ({memory_bytes:,} bytes)"
                        )

                    # Configure disk manager for spilling
                    builder = builder.with_disk_manager_os()
                    if self.temp_dir:
                        self.log_very_verbose(f"Enabled disk spilling (temp dir: {self.temp_dir})")
                    else:
                        self.log_very_verbose("Enabled disk spilling (using system temp dir)")

                    runtime = builder.build()
                else:
                    # Old RuntimeEnv API (fallback)
                    runtime = RuntimeEnv()
                    self.log_very_verbose("Using default RuntimeEnv (memory configuration not available in old API)")
            except Exception as e:
                self.log_very_verbose(f"Could not configure RuntimeEnv: {e}, using defaults")
                runtime = None

        # Create session configuration
        config = SessionConfig()

        # Set parallelism
        config = config.with_target_partitions(self.target_partitions)

        # Enable optimizations
        config = config.with_parquet_pruning(True)
        config = config.with_repartition_joins(True)
        config = config.with_repartition_aggregations(True)
        config = config.with_repartition_windows(True)
        config = config.with_information_schema(True)

        # Set batch size
        config = config.with_batch_size(self.batch_size)

        # Track applied configuration for logging
        config_applied = [
            f"target_partitions={self.target_partitions}",
            f"batch_size={self.batch_size}",
            "parquet_pruning=enabled",
            "repartitioning=enabled",
        ]

        # Add runtime configuration to tracking
        if runtime is not None:
            if self.memory_limit:
                config_applied.append(f"memory_pool={self.memory_limit}")
            config_applied.append("disk_spilling=enabled")

        # Note: Memory configuration now handled via RuntimeEnvBuilder above
        # The invalid config.set("memory_pool_size") approach has been removed

        # Note: Parquet optimizations already configured via with_parquet_pruning(True)
        # Redundant config.set() calls have been removed

        # Try to normalize identifiers to lowercase (for TPC benchmark compatibility)
        # Note: This setting is undocumented in DataFusion 50.x and may not be necessary
        # as DataFusion already handles identifier case according to PostgreSQL semantics
        try:
            config = config.set("datafusion.sql_parser.enable_ident_normalization", "true")
            self.log_very_verbose("Enabled SQL identifier normalization (if supported)")
            config_applied.append("ident_normalization=enabled")
        except BaseException as e:
            # Not critical - DataFusion's PostgreSQL semantics handle TPC naming correctly
            self.log_very_verbose(f"Identifier normalization not available (using PostgreSQL defaults): {e}")

        # Create SessionContext with runtime environment if available
        if runtime is not None:
            try:
                ctx = SessionContext(config, runtime)
                self.log_very_verbose("SessionContext created with RuntimeEnv")
            except TypeError:
                # Older versions may not accept runtime parameter
                ctx = SessionContext(config)
                self.log_very_verbose("SessionContext created without RuntimeEnv (not supported in this version)")
        else:
            ctx = SessionContext(config)

        self.log_operation_complete("DataFusion connection", details=f"Applied: {', '.join(config_applied)}")

        return ctx

    def _parse_memory_limit(self, memory_limit: str) -> str:
        """Parse memory limit string to bytes.

        Args:
            memory_limit: Memory limit string (e.g., "16G", "8GB", "4096MB")

        Returns:
            Memory limit in bytes as string
        """
        memory_str = memory_limit.upper().strip()

        # Remove 'B' suffix if present
        if memory_str.endswith("B"):
            memory_str = memory_str[:-1]

        # Parse numeric value and unit
        if memory_str.endswith("G"):
            return str(int(float(memory_str[:-1]) * 1024 * 1024 * 1024))
        elif memory_str.endswith("M"):
            return str(int(float(memory_str[:-1]) * 1024 * 1024))
        elif memory_str.endswith("K"):
            return str(int(float(memory_str[:-1]) * 1024))
        else:
            # Assume already in bytes
            return memory_str

    def create_schema(self, benchmark, connection: Any) -> float:
        """Create schema using DataFusion.

        Note: For DataFusion, actual table creation happens during load_data() via
        CREATE EXTERNAL TABLE. This method validates the schema is available.
        """
        start_time = time.time()
        self.log_operation_start("Schema creation", f"benchmark: {benchmark.__class__.__name__}")

        # Get constraint settings from tuning configuration
        enable_primary_keys, enable_foreign_keys = self._get_constraint_configuration()
        self._log_constraint_configuration(enable_primary_keys, enable_foreign_keys)

        # Note: DataFusion doesn't enforce constraints, so we log but don't apply them
        if enable_primary_keys or enable_foreign_keys:
            self.log_verbose(
                "DataFusion does not enforce PRIMARY KEY or FOREIGN KEY constraints - schema will be created without constraints"
            )

        # Get structured schema directly from benchmark
        # This is cleaner than parsing SQL and provides type-safe access
        self._table_schemas = self._get_benchmark_schema(benchmark)

        duration = time.time() - start_time
        self.log_operation_complete(
            "Schema creation", duration, f"Schema validated for {len(self._table_schemas)} tables"
        )
        return duration

    def _get_benchmark_schema(self, benchmark) -> dict[str, dict]:
        """Get structured schema directly from benchmark.

        Returns:
            Dict mapping table_name -> {'columns': [...]}
            where each column is {'name': str, 'type': str}
        """
        schemas = {}

        # Try to get schema from benchmark's get_schema() method
        try:
            benchmark_schema = benchmark.get_schema()
        except (AttributeError, TypeError):
            # Fallback: some benchmarks might not have get_schema()
            self.log_verbose(
                f"Benchmark {benchmark.__class__.__name__} does not provide get_schema() method, "
                "will rely on schema inference during data loading"
            )
            return {}

        if not benchmark_schema:
            self.log_verbose("Benchmark returned empty schema, will rely on schema inference")
            return {}

        # Convert benchmark schema format to our internal format
        # Benchmark schema: {table_name: {'name': str, 'columns': [{'name': str, 'type': str, ...}]}}
        for table_name_key, table_def in benchmark_schema.items():
            # Normalize table name to lowercase
            table_name = table_name_key.lower()

            # Extract column information
            columns = []
            if isinstance(table_def, dict) and "columns" in table_def:
                for col in table_def["columns"]:
                    if isinstance(col, dict) and "name" in col:
                        # Get type from the column definition
                        col_type = col.get("type", "VARCHAR")
                        # Handle both string types and potentially nested type info
                        if not isinstance(col_type, str):
                            col_type = "VARCHAR"

                        columns.append({"name": col["name"], "type": col_type})
                    else:
                        self.log_very_verbose(f"Skipping invalid column definition in {table_name}: {col}")

            if columns:
                schemas[table_name] = {"columns": columns}
                self.log_very_verbose(f"Extracted schema for {table_name}: {len(columns)} columns")
            else:
                self.log_verbose(f"Warning: No valid columns found for table {table_name}")

        return schemas

    def load_data(
        self, benchmark, connection: Any, data_dir: Path
    ) -> tuple[dict[str, int], float, dict[str, Any] | None]:
        """Load data into DataFusion using CSV or Parquet format.

        DataFusion supports two loading modes:
        1. CSV mode: Direct loading of CSV files via CREATE EXTERNAL TABLE
        2. Parquet mode: Convert CSV to Parquet first for 10-50x query performance
        """
        from benchbox.platforms.base.data_loading import DataSourceResolver

        start_time = time.time()
        self.log_operation_start("Data loading", f"format: {self.data_format}")

        # Resolve data source
        resolver = DataSourceResolver()
        data_source = resolver.resolve(benchmark, data_dir)

        if not data_source or not data_source.tables:
            raise ValueError(f"No data files found in {data_dir}")

        table_stats = {}
        per_table_timings = {}

        # Load each table
        for table_name, file_paths in data_source.tables.items():
            table_start = time.time()

            # Ensure file_paths is a list
            if not isinstance(file_paths, list):
                file_paths = [file_paths]

            # Normalize table name to lowercase
            table_name_lower = table_name.lower()

            if self.data_format == "parquet":
                # Convert CSV to Parquet and load
                row_count = self._load_table_parquet(connection, table_name_lower, file_paths, data_dir)
            else:
                # Load CSV directly
                row_count = self._load_table_csv(connection, table_name_lower, file_paths, data_dir)

            table_duration = time.time() - table_start
            table_stats[table_name_lower] = row_count
            per_table_timings[table_name_lower] = {"total_ms": table_duration * 1000}

            self.log_verbose(f"Loaded table {table_name_lower}: {row_count:,} rows in {table_duration:.2f}s")

        total_duration = time.time() - start_time
        total_rows = sum(table_stats.values())

        self.log_operation_complete(
            "Data loading",
            total_duration,
            f"{total_rows:,} rows across {len(table_stats)} tables",
        )

        return table_stats, total_duration, per_table_timings

    def _detect_csv_format(self, file_paths: list[Path]) -> tuple[str, bool]:
        """Detect CSV delimiter and format from file extension.

        Returns:
            Tuple of (delimiter, has_trailing_delimiter)
        """
        if file_paths:
            first_file = str(file_paths[0])
            if ".tbl" in first_file or ".dat" in first_file:
                # TPC benchmark format uses pipe delimiter with trailing delimiter
                return "|", True
            else:
                return ",", False
        return ",", False

    def _load_table_csv(self, connection: Any, table_name: str, file_paths: list[Path], data_dir: Path) -> int:
        """Load table from CSV files using CREATE EXTERNAL TABLE.

        Handles TPC benchmark format with trailing pipe delimiters and
        uses glob patterns for multiple files.
        """
        # Detect delimiter and format
        delimiter, has_trailing_delimiter = self._detect_csv_format(file_paths)

        # Get schema information for proper column names
        schema_info = self._table_schemas.get(table_name, {})
        columns = schema_info.get("columns", [])

        # Build column schema for CREATE EXTERNAL TABLE
        if columns:
            # Use actual column names and types from schema
            schema_clause = ", ".join([f"{col['name']} {self._map_to_arrow_type(col['type'])}" for col in columns])
            schema_clause = f"({schema_clause})"
        else:
            # No schema available - let DataFusion infer
            schema_clause = ""
            self.log_verbose(f"Warning: No schema found for {table_name}, using schema inference")

        # Use glob pattern for multiple files or single file
        if len(file_paths) > 1:
            # Check if files are in same directory and can use glob
            parent_dir = file_paths[0].parent
            if all(f.parent == parent_dir for f in file_paths):
                # Use glob pattern based on actual file extension
                # E.g., table.tbl.1, table.tbl.2 -> table.tbl.*
                # This preserves the extension to avoid matching unintended files
                first_file_name = file_paths[0].name
                # Find the position of the first numeric extension or just use table_name
                if "." in first_file_name:
                    # Keep everything up to the last numeric part
                    base_pattern = (
                        first_file_name.rsplit(".", 1)[0]
                        if first_file_name.split(".")[-1].isdigit()
                        else first_file_name
                    )
                    location = str(parent_dir / f"{base_pattern}*")
                else:
                    location = str(parent_dir / f"{table_name}*")
                self.log_very_verbose(f"Using glob pattern for {table_name}: {location}")
            else:
                # Files in different directories - fall back to first file with warning
                location = str(file_paths[0])
                self.log_verbose(
                    f"Warning: Multiple files in different directories for {table_name}, using first file only"
                )
        else:
            location = str(file_paths[0])

        # Build CREATE EXTERNAL TABLE statement
        # Note: DataFusion's CSV reader doesn't have a direct "ignore trailing delimiter" option
        # We need to handle this via schema definition with exact column count
        options = [
            "'has_header' 'false'",
            f"'delimiter' '{delimiter}'",
        ]

        # Add format-specific options
        if has_trailing_delimiter and columns:
            # For TPC files with trailing delimiters, we rely on explicit schema
            # to prevent extra empty column
            self.log_very_verbose(f"Handling TPC format with trailing delimiter for {table_name}")

        options_clause = ", ".join(options)

        create_sql = f"""
            CREATE EXTERNAL TABLE {table_name} {schema_clause}
            STORED AS CSV
            LOCATION '{location}'
            OPTIONS ({options_clause})
        """

        try:
            connection.sql(create_sql)
            self.log_very_verbose(f"Created external table: {table_name}")
        except Exception as e:
            self.log_verbose(f"Error creating external table {table_name}: {e}")
            raise RuntimeError(f"Failed to create external table {table_name}: {e}") from e

        # Count rows
        try:
            result = connection.sql(f"SELECT COUNT(*) FROM {table_name}").collect()
            row_count = int(result[0].column(0)[0])
            return row_count
        except Exception as e:
            self.log_verbose(f"Error counting rows in {table_name}: {e}")
            raise RuntimeError(f"Failed to count rows in {table_name}: {e}") from e

    def _map_to_arrow_type(self, sql_type: str) -> str:
        """Map SQL types to Arrow/DataFusion types.

        This mapping is used when creating external CSV tables with explicit schemas.
        For Parquet tables, PyArrow infers types automatically during CSV parsing.
        """
        sql_type_upper = sql_type.upper()

        # Map common SQL types to DataFusion types
        type_mapping = {
            "INTEGER": "INT",
            "BIGINT": "BIGINT",
            "DECIMAL": "DECIMAL",
            "DOUBLE": "DOUBLE",
            "FLOAT": "FLOAT",
            "VARCHAR": "VARCHAR",
            "CHAR": "VARCHAR",
            "TEXT": "VARCHAR",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "BOOLEAN": "BOOLEAN",
        }

        # Check for parameterized types like DECIMAL(10,2) or VARCHAR(100)
        base_type = sql_type_upper.split("(")[0]

        if base_type in type_mapping:
            # For parameterized types, preserve the parameters
            if "(" in sql_type_upper:
                return f"{type_mapping[base_type]}{sql_type_upper[len(base_type) :]}"
            return type_mapping[base_type]

        # Return as-is if not in mapping (assume it's already valid)
        return sql_type

    def _load_table_parquet(self, connection: Any, table_name: str, file_paths: list[Path], data_dir: Path) -> int:
        """Load table by converting CSV to Parquet first, then loading.

        Preserves column names from schema. PyArrow handles trailing delimiters correctly.
        """
        import pyarrow as pa
        import pyarrow.csv as csv
        import pyarrow.parquet as pq

        # Store parquet files directly in working directory
        parquet_dir = self.working_dir
        parquet_dir.mkdir(exist_ok=True)

        parquet_file = parquet_dir / f"{table_name}.parquet"

        # Detect delimiter - PyArrow's CSV reader handles trailing delimiters automatically
        delimiter, _ = self._detect_csv_format(file_paths)

        # Get schema information for proper column names and types
        schema_info = self._table_schemas.get(table_name, {})
        columns = schema_info.get("columns", [])

        # Build column names list from schema
        # PyArrow's CSV reader handles trailing delimiters correctly - it doesn't create extra columns
        column_names = None
        column_types = None
        if columns:
            column_names = [col["name"] for col in columns]
            # Build PyArrow column types from schema to prevent incorrect type inference
            # e.g., ca_zip "89436" should be string, not int64
            column_types = {}
            for col in columns:
                col_name = col["name"]
                col_type = col.get("type", "VARCHAR").upper()
                # Map schema types to PyArrow types - string types must be explicit
                # to prevent PyArrow from inferring numeric types for zip codes etc.
                if col_type.startswith(("CHAR", "VARCHAR", "TEXT", "STRING")):
                    column_types[col_name] = pa.string()
                elif col_type.startswith("DATE"):
                    column_types[col_name] = pa.date32()
                elif col_type.startswith("DECIMAL"):
                    # Use float64 for decimal to avoid precision issues
                    column_types[col_name] = pa.float64()
                # Other types (INT, BIGINT, etc.) can use auto-inference
            self.log_very_verbose(f"Using {len(column_names)} columns from schema for {table_name}: {column_names}")
        else:
            self.log_verbose(f"Warning: No schema found for {table_name}, using auto-generated column names")

        # Convert CSV to Parquet
        self.log_very_verbose(f"Converting {len(file_paths)} CSV file(s) to Parquet for {table_name}")

        # Read all CSV files and combine
        # Note: PyArrow automatically detects and handles compressed files (.gz, .bz2, etc.)
        tables = []
        for file_path in file_paths:
            try:
                # Configure CSV read options
                read_options = csv.ReadOptions(
                    column_names=column_names,
                    autogenerate_column_names=(column_names is None),
                )

                parse_options = csv.ParseOptions(
                    delimiter=delimiter,
                    quote_char='"',  # Standard quote character
                    escape_char="\\",  # Standard escape character
                )

                convert_options = csv.ConvertOptions(
                    null_values=[""],
                    strings_can_be_null=True,
                    column_types=column_types,  # Explicit types to prevent incorrect inference
                )

                # Read CSV with PyArrow (handles Path objects and compression automatically)
                table = csv.read_csv(
                    file_path,
                    read_options=read_options,
                    parse_options=parse_options,
                    convert_options=convert_options,
                )

                tables.append(table)

            except Exception as e:
                self.log_verbose(f"Error reading CSV file {file_path}: {e}")
                raise RuntimeError(f"Failed to read CSV file {file_path}: {e}") from e

        # Concatenate all tables
        try:
            combined_table = pa.concat_tables(tables)
        except Exception as e:
            self.log_verbose(f"Error concatenating tables for {table_name}: {e}")
            raise RuntimeError(f"Failed to concatenate CSV data for {table_name}: {e}") from e

        # Write to Parquet with compression
        try:
            pq.write_table(
                combined_table,
                parquet_file,
                compression="snappy",  # Fast compression, good balance
            )
            self.log_very_verbose(f"Created Parquet file: {parquet_file} ({combined_table.num_rows:,} rows)")
        except Exception as e:
            self.log_verbose(f"Error writing Parquet file for {table_name}: {e}")
            raise RuntimeError(f"Failed to write Parquet file for {table_name}: {e}") from e

        # Register Parquet table in DataFusion
        try:
            connection.register_parquet(table_name, str(parquet_file))
        except Exception as e:
            # Clean up the Parquet file if registration fails
            try:
                if parquet_file.exists():
                    parquet_file.unlink()
                    self.log_very_verbose(f"Cleaned up orphaned Parquet file: {parquet_file}")
            except Exception as cleanup_error:
                self.log_very_verbose(f"Could not clean up Parquet file: {cleanup_error}")

            self.log_verbose(f"Error registering Parquet table {table_name}: {e}")
            raise RuntimeError(f"Failed to register Parquet table {table_name}: {e}") from e

        return combined_table.num_rows

    def configure_for_benchmark(self, connection: Any, benchmark_type: str) -> None:
        """Apply DataFusion-specific optimizations based on benchmark type."""
        # DataFusion optimizations are set during connection creation
        # Additional benchmark-specific settings can be added here
        self.log_verbose(f"DataFusion configured for {benchmark_type} benchmark")

    def execute_query(
        self,
        connection: Any,
        query: str,
        query_id: str,
        benchmark_type: str | None = None,
        scale_factor: float | None = None,
        validate_row_count: bool = True,
        stream_id: int | None = None,
    ) -> dict[str, Any]:
        """Execute query with detailed timing and result collection."""
        self.log_verbose(f"Executing query {query_id}")
        self.log_very_verbose(f"Query SQL (first 200 chars): {query[:200]}{'...' if len(query) > 200 else ''}")

        # In dry-run mode, capture SQL instead of executing
        if self.dry_run_mode:
            self.capture_sql(query, "query", None)
            self.log_very_verbose(f"Captured query {query_id} for dry-run")

            return {
                "query_id": query_id,
                "status": "DRY_RUN",
                "execution_time": 0.0,
                "rows_returned": 0,
                "first_row": None,
                "error": None,
                "dry_run": True,
            }

        start_time = time.time()

        try:
            # Execute the query
            df = connection.sql(query)

            # Collect results
            result_batches = df.collect()

            # Calculate total rows
            actual_row_count = sum(batch.num_rows for batch in result_batches)

            # Get first row if results exist
            first_row = None
            if result_batches and result_batches[0].num_rows > 0:
                # Convert first row to tuple
                first_batch = result_batches[0]
                first_row = tuple(
                    first_batch.column(i)[0].as_py()
                    if hasattr(first_batch.column(i)[0], "as_py")
                    else first_batch.column(i)[0]
                    for i in range(first_batch.num_columns)
                )

            execution_time = time.time() - start_time
            logger.debug(f"Query {query_id} completed in {execution_time:.3f}s, returned {actual_row_count} rows")

            # Validate row count if enabled
            validation_result = None
            if validate_row_count and benchmark_type:
                from benchbox.core.validation.query_validation import QueryValidator

                validator = QueryValidator()
                validation_result = validator.validate_query_result(
                    benchmark_type=benchmark_type,
                    query_id=query_id,
                    actual_row_count=actual_row_count,
                    scale_factor=scale_factor,
                    stream_id=stream_id,
                )

                # Log validation result
                if validation_result.warning_message:
                    self.log_verbose(f"Row count validation: {validation_result.warning_message}")
                elif not validation_result.is_valid:
                    self.log_verbose(f"Row count validation FAILED: {validation_result.error_message}")
                else:
                    self.log_very_verbose(
                        f"Row count validation PASSED: {actual_row_count} rows "
                        f"(expected: {validation_result.expected_row_count})"
                    )

            # Use centralized helper to build result with validation
            return self._build_query_result_with_validation(
                query_id=query_id,
                execution_time=execution_time,
                actual_row_count=actual_row_count,
                first_row=first_row,
                validation_result=validation_result,
            )

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"Query {query_id} failed after {execution_time:.3f}s: {e}",
                exc_info=True,
            )

            return {
                "query_id": query_id,
                "status": "FAILED",
                "execution_time": execution_time,
                "rows_returned": 0,
                "error": str(e),
                "error_type": type(e).__name__,
            }

    def apply_platform_optimizations(self, platform_config, connection: Any) -> None:
        """Apply DataFusion-specific platform optimizations.

        DataFusion optimizations are primarily configured at SessionContext creation.
        This method logs the optimization configuration.
        """
        if not platform_config:
            self.log_verbose("No platform optimizations configured")
            return

        self.log_verbose("DataFusion optimizations configured at session creation")

        # DataFusion doesn't support most traditional database optimizations
        # like Z-ordering, auto-optimize, bloom filters, etc.
        # These are handled through file organization (Parquet partitioning)

    def apply_constraint_configuration(
        self,
        primary_key_config,
        foreign_key_config,
        connection: Any,
    ) -> None:
        """Apply constraint configuration.

        Note: DataFusion does not enforce PRIMARY KEY or FOREIGN KEY constraints.
        This method logs the configuration but does not apply constraints.
        """
        if primary_key_config and primary_key_config.enabled:
            self.log_verbose(
                "DataFusion does not enforce PRIMARY KEY constraints - configuration noted but not applied"
            )

        if foreign_key_config and foreign_key_config.enabled:
            self.log_verbose(
                "DataFusion does not enforce FOREIGN KEY constraints - configuration noted but not applied"
            )

    def check_database_exists(self, **connection_config) -> bool:
        """Check if DataFusion working directory exists with data."""
        working_dir = Path(connection_config.get("working_dir", self.working_dir))

        if not working_dir.exists():
            return False

        # Check if working directory has parquet files
        if any(working_dir.glob("*.parquet")):
            return True

        return False

    def drop_database(self, **connection_config) -> None:
        """Drop DataFusion working directory and all data."""
        import shutil

        working_dir = Path(connection_config.get("working_dir", self.working_dir))

        if working_dir.exists():
            self.log_verbose(f"Removing DataFusion working directory: {working_dir}")
            shutil.rmtree(working_dir)
            self.log_verbose("DataFusion working directory removed")

    def validate_platform_capabilities(self, benchmark_type: str):
        """Validate DataFusion-specific capabilities for the benchmark."""
        from benchbox.core.validation import ValidationResult

        errors = []
        warnings = []

        # Check if DataFusion is available
        if SessionContext is None:
            errors.append("DataFusion library not available - install with 'pip install datafusion'")
        else:
            # Check DataFusion version
            try:
                import datafusion as df_module

                version = df_module.__version__
                self.log_very_verbose(f"DataFusion version: {version}")
            except (ImportError, AttributeError):
                warnings.append("Could not determine DataFusion version")

        # Warn about TPC-DS limitations
        if benchmark_type.lower() == "tpcds":
            warnings.append("Some TPC-DS queries may fail due to DataFusion SQL feature limitations")

        # Check memory configuration
        if self.memory_limit:
            try:
                memory_bytes = int(self._parse_memory_limit(self.memory_limit))
                memory_gb = memory_bytes / (1024**3)

                if memory_gb < 2.0:
                    warnings.append(f"Memory limit ({self.memory_limit}) may be insufficient for larger scale factors")
            except (ValueError, TypeError):
                warnings.append(f"Could not parse memory limit: {self.memory_limit}")

        platform_info = {
            "platform": self.platform_name,
            "benchmark_type": benchmark_type,
            "dry_run_mode": self.dry_run_mode,
            "datafusion_available": SessionContext is not None,
            "working_dir": str(self.working_dir),
            "memory_limit": self.memory_limit,
            "target_partitions": self.target_partitions,
            "data_format": self.data_format,
        }

        if SessionContext:
            try:
                import datafusion as df_module

                platform_info["datafusion_version"] = df_module.__version__
            except (ImportError, AttributeError):
                # Version attribute not available in this DataFusion version
                pass

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=platform_info,
        )

    def _get_existing_tables(self, connection) -> list[str]:
        """Get list of existing tables in DataFusion SessionContext.

        Override base class method to use DataFusion's catalog API instead of
        information_schema queries.
        """
        try:
            # DataFusion SessionContext has a catalog() method to list tables
            # Get all registered tables
            tables = []

            # DataFusion stores tables in a catalog structure
            # Use SHOW TABLES to get list of tables
            result = connection.sql("SHOW TABLES")
            rows = result.collect()

            # Extract table names from the result
            # SHOW TABLES returns a DataFrame with columns:
            # [table_catalog, table_schema, table_name, table_type]
            for batch in rows:
                # Convert to pydict to get table names
                data = batch.to_pydict()
                # Get the 'table_name' column specifically
                if data and "table_name" in data:
                    tables.extend([name.lower() for name in data["table_name"]])

            return tables
        except Exception as e:
            self.log_verbose(f"Error getting existing tables: {e}")
            # Fallback - return empty list if query fails
            return []

    def _validate_data_integrity(
        self, benchmark, connection, table_stats: dict[str, int]
    ) -> tuple[str, dict[str, Any]]:
        """Validate basic data integrity using DataFusion SessionContext API.

        Override base class method to use DataFusion's ctx.sql() instead of
        DB-API 2.0 cursor interface.
        """
        validation_details = {}

        try:
            # DataFusion connection is a SessionContext, not a DB-API 2.0 connection
            # Use ctx.sql() to validate table accessibility
            accessible_tables = []
            inaccessible_tables = []

            for table_name in table_stats:
                try:
                    # Try a simple SELECT to verify table is accessible
                    # Use SessionContext.sql() which returns a DataFrame
                    result = connection.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
                    # Execute the query to verify it works
                    result.collect()
                    accessible_tables.append(table_name)
                except Exception as e:
                    self.log_verbose(f"Table {table_name} inaccessible: {e}")
                    inaccessible_tables.append(table_name)

            if inaccessible_tables:
                validation_details["inaccessible_tables"] = inaccessible_tables
                validation_details["constraints_enabled"] = False
                return "FAILED", validation_details
            else:
                validation_details["accessible_tables"] = accessible_tables
                validation_details["constraints_enabled"] = True
                return "PASSED", validation_details

        except Exception as e:
            validation_details["error"] = str(e)
            return "FAILED", validation_details
