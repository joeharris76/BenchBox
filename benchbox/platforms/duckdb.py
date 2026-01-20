"""DuckDB platform adapter with data loading and query execution.

Provides DuckDB-specific optimizations including fast bulk data loading
using DuckDB's native CSV reading capabilities.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError:
    duckdb = None

from benchbox.core.errors import PlanCaptureError
from benchbox.utils.cloud_storage import get_cloud_path_info, is_cloud_path

from .base import PlatformAdapter

logger = logging.getLogger(__name__)


class DuckDBConnectionWrapper:
    """Wrapper for DuckDB connection that supports dry-run mode."""

    def __init__(self, connection, platform_adapter):
        self._connection = connection
        self._platform_adapter = platform_adapter

    def execute(self, query: str, parameters=None):
        """Execute query, capturing SQL in dry-run mode."""
        if self._platform_adapter.dry_run_mode:
            # Capture SQL instead of executing
            self._platform_adapter.capture_sql(query, "query", None)
            # Return mock cursor-like object
            return DuckDBCursorWrapper([], self._platform_adapter)
        else:
            # Normal execution
            return self._connection.execute(query, parameters)

    def commit(self):
        """Commit transaction (no-op in dry-run mode)."""
        if not self._platform_adapter.dry_run_mode and hasattr(self._connection, "commit"):
            self._connection.commit()

    def close(self):
        """Close connection (no-op in dry-run mode)."""
        if not self._platform_adapter.dry_run_mode:
            self._connection.close()

    def __getattr__(self, name):
        """Delegate other attributes to the real connection."""
        return getattr(self._connection, name)


class DuckDBCursorWrapper:
    """Mock cursor for dry-run mode."""

    def __init__(self, rows, platform_adapter):
        self._rows = rows
        self._platform_adapter = platform_adapter

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return None if not self._rows else self._rows[0]

    def fetchmany(self, size=None):
        return self._rows[:size] if size else self._rows


class DuckDBAdapter(PlatformAdapter):
    """DuckDB platform adapter with optimized bulk loading and execution."""

    @property
    def platform_name(self) -> str:
        return "DuckDB"

    @staticmethod
    def add_cli_arguments(parser) -> None:
        """Add DuckDB-specific CLI arguments."""
        duckdb_group = parser.add_argument_group("DuckDB Arguments")
        duckdb_group.add_argument("--duckdb-database-path", type=str, help="Path to DuckDB database file")
        duckdb_group.add_argument("--memory-limit", type=str, default="4GB", help="DuckDB memory limit")

    @classmethod
    def from_config(cls, config: dict[str, Any]):
        """Create DuckDB adapter from unified configuration."""
        from pathlib import Path

        from benchbox.utils.database_naming import generate_database_filename
        from benchbox.utils.scale_factor import format_benchmark_name

        # Extract DuckDB-specific configuration
        adapter_config = {}

        # Database path handling
        if config.get("database_path"):
            adapter_config["database_path"] = config["database_path"]
        else:
            # Generate database path using naming utilities
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
                platform="duckdb",
                tuning_config=config.get("tuning_config"),
            )
            adapter_config["database_path"] = str(data_dir / db_filename)
            data_dir.mkdir(parents=True, exist_ok=True)

        # Memory limit
        adapter_config["memory_limit"] = config.get("memory_limit", "4GB")

        # Force recreate
        adapter_config["force_recreate"] = config.get("force", False)

        # Pass through other relevant config
        for key in ["tuning_config", "verbose_enabled", "very_verbose"]:
            if key in config:
                adapter_config[key] = config[key]

        return cls(**adapter_config)

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get DuckDB platform information."""
        platform_info = {
            "platform_type": "duckdb",
            "platform_name": "DuckDB",
            "connection_mode": "memory" if self.database_path == ":memory:" else "file",
            "configuration": {
                "database_path": self.database_path,
                "memory_limit": self.memory_limit,
                "thread_limit": self.thread_limit,
                "max_temp_directory_size": self.max_temp_directory_size,
                "enable_progress_bar": self.enable_progress_bar,
                "result_cache_enabled": False,  # DuckDB has no persistent query result cache
            },
        }

        # Get DuckDB version and client library version
        try:
            import duckdb

            platform_info["client_library_version"] = duckdb.__version__
            platform_info["platform_version"] = duckdb.__version__
        except (ImportError, AttributeError):
            platform_info["client_library_version"] = None
            platform_info["platform_version"] = None

        return platform_info

    def __init__(self, **config):
        super().__init__(**config)
        if duckdb is None:
            raise ImportError("DuckDB not installed. Install with: pip install duckdb")
        # DuckDB configuration
        self.database_path = config.get("database_path", ":memory:")
        self.memory_limit = config.get("memory_limit", "4GB")
        self.max_temp_directory_size = config.get("max_temp_directory_size")
        self.thread_limit = config.get("thread_limit")
        self.enable_progress_bar = config.get("progress_bar", False)

    def get_database_path(self, **connection_config) -> str:
        """Get the database file path for DuckDB.

        Priority:
        1. connection_config["database_path"] if provided and not None
        2. self.database_path (set during from_config)
        3. ":memory:" as final fallback
        """
        # Use connection_config path if explicitly provided and not None
        db_path = connection_config.get("database_path")
        if db_path is not None:
            return db_path
        # Fall back to instance database_path (set during from_config)
        if self.database_path is not None:
            return self.database_path
        # Final fallback to in-memory database
        return ":memory:"

    def create_connection(self, **connection_config) -> Any:
        """Create optimized DuckDB connection."""
        self.log_operation_start("DuckDB connection")

        # Handle existing database using base class method
        self.handle_existing_database(**connection_config)

        db_path = self.get_database_path(**connection_config)
        self.log_very_verbose(f"DuckDB database path: {db_path}")

        # Create connection
        conn = duckdb.connect(db_path)
        self.log_very_verbose("DuckDB connection established")

        # Apply DuckDB settings
        config_applied = []
        if self.memory_limit:
            conn.execute(f"SET memory_limit = '{self.memory_limit}'")
            config_applied.append(f"memory_limit={self.memory_limit}")
            self.log_very_verbose(f"DuckDB memory limit set to: {self.memory_limit}")

        if self.max_temp_directory_size:
            conn.execute(f"SET max_temp_directory_size = '{self.max_temp_directory_size}'")
            config_applied.append(f"max_temp_directory_size={self.max_temp_directory_size}")
            self.log_very_verbose(f"DuckDB max temp directory size set to: {self.max_temp_directory_size}")

        if self.thread_limit:
            conn.execute(f"SET threads TO {self.thread_limit}")
            config_applied.append(f"threads={self.thread_limit}")
            self.log_very_verbose(f"DuckDB thread limit set to: {self.thread_limit}")

        if self.enable_progress_bar:
            conn.execute("SET enable_progress_bar = true")
            config_applied.append("progress_bar=enabled")
            self.log_very_verbose("DuckDB progress bar enabled")

        # Optimize for OLAP workloads
        conn.execute("SET default_order = 'ASC'")
        config_applied.append("OLAP optimizations")
        self.log_very_verbose("DuckDB OLAP optimizations applied")
        # Note: enable_optimizer setting not available in current DuckDB versions

        # Enable profiling only if requested
        if self.show_query_plans:
            conn.execute("SET enable_profiling = 'query_tree_optimizer'")
            config_applied.append("query profiling")
            self.log_very_verbose("DuckDB query profiling enabled")

        self.log_operation_complete("DuckDB connection", details=f"Applied: {', '.join(config_applied)}")

        # Return wrapped connection for dry-run interception only when enabled
        if self.dry_run_mode:
            return DuckDBConnectionWrapper(conn, self)
        return conn

    def create_schema(self, benchmark, connection: Any) -> float:
        """Create schema using benchmark's SQL definitions."""
        start_time = time.time()
        self.log_operation_start("Schema creation", f"benchmark: {benchmark.__class__.__name__}")

        # Get constraint settings from tuning configuration
        enable_primary_keys, enable_foreign_keys = self._get_constraint_configuration()
        self._log_constraint_configuration(enable_primary_keys, enable_foreign_keys)
        self.log_verbose(
            f"Schema constraints - Primary keys: {enable_primary_keys}, Foreign keys: {enable_foreign_keys}"
        )

        # Use common schema creation helper (no translation needed for DuckDB)
        schema_sql = self._create_schema_with_tuning(benchmark, source_dialect="duckdb")

        # For TPC-DS, remove foreign key constraints to avoid constraint violations during parallel loading
        benchmark_name = getattr(benchmark, "_name", "") or benchmark.__class__.__name__
        if "TPC-DS" in str(benchmark_name) or "TPCDS" in str(benchmark_name):
            # Strip REFERENCES clauses from schema to avoid foreign key constraint violations
            import re

            schema_sql = re.sub(
                r",\s*FOREIGN KEY[^,)]*\([^)]*\)\s*REFERENCES[^,)]*\([^)]*\)",
                "",
                schema_sql,
                flags=re.IGNORECASE | re.MULTILINE,
            )
            schema_sql = re.sub(r"REFERENCES\s+\w+\s*\([^)]*\)", "", schema_sql, flags=re.IGNORECASE)

        # Split schema into individual CREATE TABLE statements for better compatibility
        # This handles foreign key constraints and complex multi-table schemas
        statements = []
        current_statement = []

        for line in schema_sql.split("\n"):
            if line.strip().startswith("CREATE TABLE") and current_statement:
                statements.append("\n".join(current_statement))
                current_statement = [line]
            else:
                current_statement.append(line)

        # Include the last statement
        if current_statement:
            statements.append("\n".join(current_statement))

        # Execute each CREATE TABLE statement separately
        tables_created = 0
        for statement in statements:
            if statement.strip():
                # Extract table name
                import re

                table_name = "unknown"
                match = re.search(r"CREATE TABLE\s+(\w+)", statement, re.IGNORECASE)
                if match:
                    table_name = match.group(1)

                if self.dry_run_mode:
                    self.capture_sql(statement, "create_table", table_name)
                    self.log_very_verbose(f"Captured CREATE TABLE statement for {table_name}")
                else:
                    try:
                        connection.execute(statement)
                        tables_created += 1
                        self.log_very_verbose(f"Created table: {table_name}")
                    except Exception as e:
                        raise Exception(f"Failed to create table {table_name}: {e}")

        duration = time.time() - start_time
        self.log_operation_complete("Schema creation", duration, f"{tables_created} tables created")
        return duration

    def load_data(
        self, benchmark, connection: Any, data_dir: Path
    ) -> tuple[dict[str, int], float, dict[str, Any] | None]:
        """Load data using DuckDB's optimized CSV reading capabilities."""
        from benchbox.platforms.base.data_loading import DataLoader

        # Check if using cloud storage and log
        if is_cloud_path(str(data_dir)):
            path_info = get_cloud_path_info(str(data_dir))
            self.log_verbose(f"Loading data from cloud storage: {path_info['provider']} bucket '{path_info['bucket']}'")
            print(f"  Loading data from {path_info['provider']} cloud storage")

        # Create DuckDB-specific handler factory
        def duckdb_handler_factory(file_path, adapter, benchmark_instance):
            from benchbox.platforms.base.data_loading import (
                DuckDBDeltaHandler,
                DuckDBNativeHandler,
                DuckDBParquetHandler,
                FileFormatRegistry,
            )

            # Check if this is a Delta Lake table directory
            if file_path.is_dir():
                delta_log_dir = file_path / "_delta_log"
                if delta_log_dir.exists() and delta_log_dir.is_dir():
                    return DuckDBDeltaHandler(adapter)

            # Determine the true base extension (handles names like *.tbl.1.zst)
            base_ext = FileFormatRegistry.get_base_data_extension(file_path)

            # Create DuckDB native handler for supported formats
            if base_ext in [".tbl", ".dat"]:
                return DuckDBNativeHandler("|", adapter, benchmark_instance)
            elif base_ext == ".csv":
                return DuckDBNativeHandler(",", adapter, benchmark_instance)
            elif base_ext == ".parquet":
                return DuckDBParquetHandler(adapter)
            return None  # Fall back to generic handler

        # Use DataLoader with DuckDB-specific handler
        loader = DataLoader(
            adapter=self,
            benchmark=benchmark,
            connection=connection,
            data_dir=data_dir,
            handler_factory=duckdb_handler_factory,
        )
        table_stats, loading_time = loader.load()
        # DataLoader doesn't provide per-table timings yet
        return table_stats, loading_time, None

    def configure_for_benchmark(self, connection: Any, benchmark_type: str) -> None:
        """Apply DuckDB-specific optimizations based on benchmark type."""
        # Only apply profiling when explicitly requested
        if self.show_query_plans:
            connection.execute("SET enable_profiling = 'query_tree'")

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
        """Execute query with detailed timing and profiling."""
        self.log_verbose(f"Executing query {query_id}")
        self.log_very_verbose(f"Query SQL (first 200 chars): {query[:200]}{'...' if len(query) > 200 else ''}")

        # In dry-run mode, capture SQL instead of executing
        if self.dry_run_mode:
            self.capture_sql(query, "query", None)
            self.log_very_verbose(f"Captured query {query_id} for dry-run")

            # Return mock result for dry-run
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
            # Enable profiling only if query plans are requested
            if self.show_query_plans:
                connection.execute("PRAGMA enable_profiling = 'query_tree'")
                logger.debug("DuckDB query profiling enabled for this query")

            # Execute the query
            result = connection.execute(query)
            rows = result.fetchall()

            execution_time = time.time() - start_time
            actual_row_count = len(rows)
            logger.debug(f"Query {query_id} completed in {execution_time:.3f}s, returned {actual_row_count} rows")

            # Display query plan if enabled
            self.display_query_plan_if_enabled(connection, query, query_id)

            # Capture structured query plan if enabled
            query_plan = None
            plan_fingerprint = None
            plan_capture_time_ms = None
            if self.capture_plans:
                query_plan, plan_capture_time_ms = self.capture_query_plan(connection, query, query_id)
                if query_plan:
                    plan_fingerprint = query_plan.plan_fingerprint

            # Validate row count if enabled and benchmark type is provided
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

            # Use centralized helper to build result with consistent validation field mapping
            result = self._build_query_result_with_validation(
                query_id=query_id,
                execution_time=execution_time,
                actual_row_count=actual_row_count,
                first_row=rows[0] if rows else None,
                validation_result=validation_result,
            )

            # Add query plan to result if captured
            if query_plan:
                result["query_plan"] = query_plan
                result["plan_fingerprint"] = plan_fingerprint
            if plan_capture_time_ms is not None:
                result["plan_capture_time_ms"] = plan_capture_time_ms

            return result

        except PlanCaptureError:
            raise
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
        finally:
            # Disable profiling if it was enabled
            if self.show_query_plans:
                connection.execute("PRAGMA disable_profiling")

    def get_query_plan(self, connection: Any, query: str) -> str | None:
        """Get DuckDB query execution plan."""
        try:
            # Get profiling information from DuckDB
            profile_info = connection.execute("PRAGMA show_profiling_info").fetchall()
            if profile_info:
                # Format the profiling information into a readable string
                plan_parts = []
                for row in profile_info:
                    if len(row) >= 2:
                        plan_parts.append(str(row[1]))  # The plan text is usually in the second column
                return "\n".join(plan_parts)
        except Exception:
            # Fallback to EXPLAIN if profiling isn't available
            try:
                explain_result = connection.execute(f"EXPLAIN {query}").fetchall()
                return "\n".join([str(row[1]) for row in explain_result if len(row) > 1])
            except Exception:
                pass
        return None

    def get_query_plan_parser(self):
        """Get DuckDB query plan parser."""
        from benchbox.core.query_plans.parsers.duckdb import DuckDBQueryPlanParser

        return DuckDBQueryPlanParser()

    def _get_platform_metadata(self, connection: Any) -> dict[str, Any]:
        """Get DuckDB-specific metadata and system information."""
        metadata = {
            "platform": self.platform_name,
            "duckdb_version": duckdb.__version__,
            "result_cache_enabled": False,  # DuckDB has no persistent query result cache
        }

        try:
            # Get DuckDB settings
            settings = connection.execute("PRAGMA show_all_settings").fetchall()
            metadata["settings"] = {setting[0]: setting[1] for setting in settings}

            # Get memory usage
            memory_info = connection.execute("PRAGMA database_size").fetchall()
            metadata["database_size"] = memory_info

        except Exception as e:
            metadata["metadata_error"] = str(e)

        return metadata

    def analyze_tables(self, connection: Any) -> None:
        """Run ANALYZE on all tables for better query optimization."""
        try:
            # Get all table names
            tables = connection.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            """).fetchall()

            for (table_name,) in tables:
                connection.execute(f"ANALYZE {table_name}")

            print(f"✅ Analyzed {len(tables)} tables for query optimization")

        except Exception as e:
            print(f"⚠️️  Could not analyze tables: {e}")

    def get_target_dialect(self) -> str:
        """Get the target SQL dialect for this platform."""
        return "duckdb"

    def supports_tuning_type(self, tuning_type) -> bool:
        """Check if DuckDB supports a specific tuning type.

        DuckDB supports:
        - SORTING: Via ORDER BY in table definition (DuckDB 0.10+)
        - PARTITIONING: Limited support, mainly through file-based partitions

        Args:
            tuning_type: The type of tuning to check support for

        Returns:
            True if the tuning type is supported by DuckDB
        """
        # Import here to avoid circular imports
        try:
            from benchbox.core.tuning.interface import TuningType

            return tuning_type in {TuningType.SORTING, TuningType.PARTITIONING}
        except ImportError:
            return False

    def generate_tuning_clause(self, table_tuning) -> str:
        """Generate DuckDB-specific tuning clauses for CREATE TABLE statements.

        DuckDB supports:
        - Sorting optimization hints (no explicit syntax in CREATE TABLE)
        - Partitioning through file organization (handled at data loading level)

        Args:
            table_tuning: The tuning configuration for the table

        Returns:
            SQL clause string to be appended to CREATE TABLE statement
        """
        if not table_tuning:
            return ""

        clauses = []

        # DuckDB doesn't have explicit CREATE TABLE tuning clauses
        # Sorting and partitioning are handled at query/loading time
        # We'll return empty string and handle optimization in apply_table_tunings

        return " ".join(clauses) if clauses else ""

    def apply_table_tunings(self, table_name: str, table_tuning, connection: Any) -> None:
        """Apply tuning configurations to a DuckDB table.

        DuckDB tuning approach:
        - SORTING: Create indexes on sort columns for query optimization
        - PARTITIONING: Log partitioning strategy (handled at data loading level)
        - CLUSTERING: Treat as secondary sorting for optimization hints
        - DISTRIBUTION: Not applicable for single-node DuckDB

        Args:
            table_tuning: The tuning configuration to apply
            connection: DuckDB connection

        Raises:
            ValueError: If the tuning configuration is invalid for DuckDB
        """
        if not table_tuning or not table_tuning.has_any_tuning():
            return

        table_name_upper = table_name.upper()
        self.logger.info(f"Applying DuckDB tunings for table: {table_name_upper}")

        try:
            # Import here to avoid circular imports
            from benchbox.core.tuning.interface import TuningType

            # Handle sorting optimization through indexes
            sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)
            if sort_columns:
                # Sort columns by their order and create index
                sorted_cols = sorted(sort_columns, key=lambda col: col.order)
                column_names = [col.name for col in sorted_cols]

                # Create index for sort optimization
                index_name = f"idx_{table_name_upper.lower()}_sort"
                index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name_upper} ({', '.join(column_names)})"

                try:
                    connection.execute(index_sql)
                    self.logger.info(f"Created sort index on {table_name_upper}: {', '.join(column_names)}")
                except Exception as e:
                    self.logger.warning(f"Failed to create sort index on {table_name_upper}: {e}")

            # Handle clustering as additional index optimization
            cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)
            if cluster_columns:
                # Sort columns by their order and create secondary index
                sorted_cols = sorted(cluster_columns, key=lambda col: col.order)
                column_names = [col.name for col in sorted_cols]

                # Create index for clustering optimization
                index_name = f"idx_{table_name_upper.lower()}_cluster"
                index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name_upper} ({', '.join(column_names)})"

                try:
                    connection.execute(index_sql)
                    self.logger.info(f"Created cluster index on {table_name_upper}: {', '.join(column_names)}")
                except Exception as e:
                    self.logger.warning(f"Failed to create cluster index on {table_name_upper}: {e}")

            # Handle partitioning - log strategy but implementation depends on data loading
            partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
            if partition_columns:
                sorted_cols = sorted(partition_columns, key=lambda col: col.order)
                column_names = [col.name for col in sorted_cols]
                self.logger.info(
                    f"Partitioning strategy for {table_name_upper}: {', '.join(column_names)} (handled at data loading level)"
                )

            # Distribution not applicable for DuckDB
            distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
            if distribution_columns:
                self.logger.warning(
                    f"Distribution tuning not applicable for single-node DuckDB on table: {table_name_upper}"
                )

        except ImportError:
            self.logger.warning("Tuning interface not available - skipping tuning application")
        except Exception as e:
            raise ValueError(f"Failed to apply tunings to DuckDB table {table_name_upper}: {e}")

    def apply_unified_tuning(self, tuning_config, connection) -> None:
        """Apply unified tuning configuration to DuckDB.

        Args:
            tuning_config: UnifiedTuningConfiguration instance
            connection: DuckDB connection
        """
        try:
            # Apply table-level tunings for each table configuration
            for table_name, table_tuning in tuning_config.table_tunings.items():
                self.logger.info(f"Applying unified tuning to table: {table_name}")
                self.apply_table_tunings(table_name, table_tuning, connection)

            # Apply platform-specific optimizations
            self.apply_platform_optimizations(tuning_config, connection)

            # Apply constraint configurations (already handled in schema creation)
            self.logger.info("Constraint configuration applied during schema creation")

        except Exception as e:
            self.logger.error(f"Failed to apply unified tuning configuration: {e}")
            raise

    def apply_platform_optimizations(self, tuning_config, connection) -> None:
        """Apply DuckDB-specific platform optimizations.

        Args:
            tuning_config: UnifiedTuningConfiguration instance
            connection: DuckDB connection
        """
        try:
            from benchbox.core.tuning.interface import TuningType

            # Check if platform optimizations are configured
            if not tuning_config.platform_optimizations:
                self.logger.info("No platform optimizations configured")
                return

            # DuckDB-specific optimizations can be applied through platform_optimizations
            # For now, just log that platform optimization validation was completed
            self.logger.info("Platform optimization validation completed for DuckDB")

            # Note: DuckDB doesn't support platform-specific tunings like Z_ORDERING, AUTO_OPTIMIZE
            # These are Databricks/cloud-specific features that don't apply to DuckDB
            unsupported_types = {
                TuningType.Z_ORDERING,
                TuningType.AUTO_OPTIMIZE,
                TuningType.BLOOM_FILTERS,
                TuningType.AUTO_COMPACT,
                TuningType.MATERIALIZED_VIEWS,
            }

            for table_name, table_tuning in tuning_config.table_tunings.items():
                # Check all tuning types for unsupported configurations
                all_tuning_types = [
                    TuningType.PARTITIONING,
                    TuningType.CLUSTERING,
                    TuningType.DISTRIBUTION,
                    TuningType.SORTING,
                ]
                for tuning_type in all_tuning_types:
                    if tuning_type in unsupported_types:
                        columns = table_tuning.get_columns_by_type(tuning_type)
                        if columns:
                            self.logger.warning(
                                f"Tuning type {tuning_type.value} not supported on DuckDB for table {table_name}"
                            )

        except Exception as e:
            self.logger.error(f"Failed to apply platform optimizations: {e}")
            raise

    def apply_constraint_configuration(self, tuning_config, table_name: str, connection) -> None:
        """Apply constraint configuration for a specific table.

        Note: Constraints are applied during schema creation in DuckDB,
        so this method primarily validates the configuration.

        Args:
            tuning_config: UnifiedTuningConfiguration instance
            table_name: Name of the table
            connection: DuckDB connection
        """
        try:
            # Primary keys - validate configuration
            if not tuning_config.primary_keys.enabled:
                self.logger.info(f"Primary keys disabled for table {table_name}")

            # Foreign keys - validate configuration
            if not tuning_config.foreign_keys.enabled:
                self.logger.info(f"Foreign keys disabled for table {table_name}")

            # Unique constraints
            if not tuning_config.unique_constraints.enabled:
                self.logger.info(f"Unique constraints disabled for table {table_name}")

            # Check constraints
            if not tuning_config.check_constraints.enabled:
                self.logger.info(f"Check constraints disabled for table {table_name}")

        except Exception as e:
            self.logger.error(f"Failed to apply constraint configuration for table {table_name}: {e}")
            raise

    def _get_existing_tables(self, connection) -> list[str]:
        """Get list of existing tables in the DuckDB database.

        Override the base class implementation with DuckDB-specific query.
        DuckDB doesn't support the standard information_schema query used in base class.

        Args:
            connection: DuckDB connection

        Returns:
            List of table names (lowercase)
        """
        try:
            # Use DuckDB's SHOW TABLES command which is more reliable
            result = connection.execute("SHOW TABLES").fetchall()
            # Convert to lowercase for consistent comparison
            return [row[0].lower() for row in result]
        except Exception as e:
            self.logger.debug(f"Failed to get existing tables: {e}")
            return []

    def validate_platform_capabilities(self, benchmark_type: str):
        """Validate DuckDB-specific capabilities for the benchmark.

        Args:
            benchmark_type: Type of benchmark (e.g., 'tpcds', 'tpch')

        Returns:
            ValidationResult with DuckDB capability validation status
        """
        errors = []
        warnings = []

        # Check if DuckDB is available
        if duckdb is None:
            errors.append("DuckDB library not available - install with 'pip install duckdb'")
        else:
            # Check DuckDB version compatibility
            try:
                version = duckdb.__version__
                # Warn if using very old versions
                if version.startswith(("0.8", "0.9")):
                    warnings.append(f"DuckDB version {version} is older - consider upgrading for better performance")
            except AttributeError:
                warnings.append("Could not determine DuckDB version")

        # Check memory configuration
        if hasattr(self, "memory_limit") and self.memory_limit:
            try:
                # Parse memory limit if it's a string (e.g., "4GB")
                if isinstance(self.memory_limit, str):
                    if self.memory_limit.lower().endswith("gb"):
                        memory_gb = float(self.memory_limit[:-2])
                    elif self.memory_limit.lower().endswith("mb"):
                        memory_gb = float(self.memory_limit[:-2]) / 1024
                    else:
                        memory_gb = float(self.memory_limit) / (1024**3)  # Assume bytes
                else:
                    memory_gb = float(self.memory_limit) / (1024**3)  # Assume bytes

                if memory_gb < 1.0:
                    warnings.append(f"Memory limit ({self.memory_limit}) may be insufficient for larger scale factors")
            except (ValueError, TypeError):
                warnings.append(f"Could not parse memory limit: {self.memory_limit}")

        # Platform-specific details
        platform_info = {
            "platform": self.platform_name,
            "benchmark_type": benchmark_type,
            "dry_run_mode": self.dry_run_mode,
            "duckdb_available": duckdb is not None,
            "database_path": getattr(self, "database_path", None),
            "memory_limit": getattr(self, "memory_limit", None),
            "thread_limit": getattr(self, "thread_limit", None),
        }

        if duckdb:
            platform_info["duckdb_version"] = getattr(duckdb, "__version__", "unknown")

        # Import ValidationResult here to avoid circular imports
        try:
            from benchbox.core.validation import ValidationResult

            return ValidationResult(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                details=platform_info,
            )
        except ImportError:
            # Fallback if validation module not available
            return None

    def validate_connection_health(self, connection: Any):
        """Validate DuckDB connection health and capabilities.

        Args:
            connection: DuckDB connection object

        Returns:
            ValidationResult with connection health status
        """
        errors = []
        warnings = []
        connection_info = {}

        try:
            # Test basic query execution
            result = connection.execute("SELECT 1 as test_value").fetchone()
            if result[0] != 1:
                errors.append("Basic query execution test failed")
            else:
                connection_info["basic_query_test"] = "passed"

            # Check available memory settings
            try:
                memory_result = connection.execute("PRAGMA memory_limit").fetchone()
                if memory_result:
                    connection_info["memory_limit_setting"] = memory_result[0]
            except Exception:
                warnings.append("Could not query memory limit setting")

            # Check available threads
            try:
                threads_result = connection.execute("PRAGMA threads").fetchone()
                if threads_result:
                    connection_info["threads_setting"] = threads_result[0]
            except Exception:
                warnings.append("Could not query threads setting")

        except Exception as e:
            errors.append(f"Connection health check failed: {str(e)}")

        # Import ValidationResult here to avoid circular imports
        try:
            from benchbox.core.validation import ValidationResult

            return ValidationResult(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                details={
                    "platform": self.platform_name,
                    "connection_type": type(connection).__name__,
                    **connection_info,
                },
            )
        except ImportError:
            # Fallback if validation module not available
            return None
