"""pg_mooncake platform adapter for BenchBox benchmarking.

Extends PostgreSQL adapter with pg_mooncake-specific functionality:
- Columnstore table access method (USING columnstore) with Parquet/Iceberg storage
- DuckDB-powered vectorized execution on columnar data
- Object storage backend support (S3/GCS/Azure)

pg_mooncake is a PostgreSQL extension that adds native columnstore tables
with DuckDB-powered vectorized execution. Data is stored in Parquet format
with Iceberg metadata, providing 5-20x columnar compression and top-10
ClickBench performance.

Deployment modes:
- self-hosted: Self-hosted PostgreSQL with pg_mooncake extension (default)

Storage modes:
- local: Data stored on local disk (default)
- s3: Data stored in S3 bucket

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from .postgresql import POSTGRES_DIALECT, PostgreSQLAdapter

logger = logging.getLogger(__name__)

try:
    import psycopg2
except ImportError:
    psycopg2 = None


class PgMooncakeAdapter(PostgreSQLAdapter):
    """pg_mooncake platform adapter with columnstore tables and DuckDB execution.

    Extends PostgreSQLAdapter with pg_mooncake-specific features:
    - Columnstore table access method (USING columnstore)
    - DuckDB-powered vectorized execution on Parquet data
    - Object storage backend configuration (S3/GCS)

    Requires PostgreSQL 15+ with pg_mooncake extension installed.
    """

    @property
    def platform_name(self) -> str:
        return "pg_mooncake"

    def get_target_dialect(self) -> str:
        """Return the target SQL dialect for pg_mooncake (PostgreSQL-compatible)."""
        return POSTGRES_DIALECT

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> PgMooncakeAdapter:
        """Create pg_mooncake adapter from unified configuration."""
        adapter_config = {}

        # Connection parameters (inherited from PostgreSQL)
        adapter_config["host"] = config.get("host", "localhost")
        adapter_config["port"] = config.get("port", 5432)
        adapter_config["username"] = config.get("username", "postgres")
        adapter_config["password"] = config.get("password")
        adapter_config["schema"] = config.get("schema", "public")
        adapter_config["sslmode"] = config.get("sslmode", "prefer")

        # Database name - use provided or generate from benchmark config
        if config.get("database"):
            adapter_config["database"] = config["database"]
        elif config.get("benchmark") and config.get("scale_factor") is not None:
            from benchbox.utils.scale_factor import format_benchmark_name

            benchmark_name = format_benchmark_name(config["benchmark"], config["scale_factor"])
            adapter_config["database"] = f"benchbox_{benchmark_name}".lower().replace("-", "_")
        else:
            adapter_config["database"] = "benchbox"

        # Admin database for CREATE/DROP DATABASE operations
        adapter_config["admin_database"] = config.get("admin_database", "postgres")

        # Performance settings (inherited from PostgreSQL)
        adapter_config["work_mem"] = config.get("work_mem", "256MB")
        adapter_config["maintenance_work_mem"] = config.get("maintenance_work_mem", "512MB")
        adapter_config["effective_cache_size"] = config.get("effective_cache_size", "1GB")
        adapter_config["max_parallel_workers_per_gather"] = config.get("max_parallel_workers_per_gather", 2)

        # Connection pool settings
        adapter_config["connect_timeout"] = config.get("connect_timeout", 10)
        adapter_config["statement_timeout"] = config.get("statement_timeout", 0)

        # pg_mooncake-specific settings
        adapter_config["storage_mode"] = config.get("storage_mode", "local")
        adapter_config["mooncake_bucket"] = config.get("mooncake_bucket")

        # Force recreate
        adapter_config["force_recreate"] = config.get("force", False)

        # Pass through other config
        for key in ["tuning_config", "verbose_enabled", "very_verbose"]:
            if key in config:
                adapter_config[key] = config[key]

        return cls(**adapter_config)

    def __init__(self, **config):
        super().__init__(**config)

        # pg_mooncake-specific configuration
        self.storage_mode = config.get("storage_mode", "local")

        # Validate storage mode
        valid_storage_modes = {"local", "s3"}
        if self.storage_mode not in valid_storage_modes:
            raise ValueError(
                f"Invalid pg_mooncake storage mode '{self.storage_mode}'. "
                f"Valid modes: {', '.join(sorted(valid_storage_modes))}"
            )

        # Object storage settings
        self.mooncake_bucket = config.get("mooncake_bucket") or os.environ.get("MOONCAKE_S3_BUCKET")

        if self.storage_mode == "s3" and not self.mooncake_bucket:
            raise ValueError(
                "S3 storage mode requires bucket configuration.\n"
                "Provide via --platform-option mooncake_bucket=s3://bucket/path or "
                "set MOONCAKE_S3_BUCKET environment variable."
            )

    def create_connection(self, **connection_config) -> Any:
        """Create PostgreSQL connection and configure pg_mooncake extension."""
        conn = super().create_connection(**connection_config)

        cursor = conn.cursor()
        try:
            # Verify pg_mooncake extension is installed
            cursor.execute("SELECT extversion FROM pg_extension WHERE extname = 'pg_mooncake'")
            result = cursor.fetchone()
            if result:
                self.logger.info(f"pg_mooncake extension version: {result[0]}")
            else:
                # Try to create the extension
                self.logger.info("pg_mooncake extension not found, attempting to create...")
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_mooncake")
                conn.commit()
                cursor.execute("SELECT extversion FROM pg_extension WHERE extname = 'pg_mooncake'")
                result = cursor.fetchone()
                if result:
                    self.logger.info(f"Created pg_mooncake extension version: {result[0]}")
                else:
                    raise RuntimeError(
                        "pg_mooncake extension is not available on this PostgreSQL server. "
                        "Install pg_mooncake (https://github.com/Mooncake-Labs/pg_mooncake) or use "
                        "the 'postgresql' or 'duckdb' platform instead."
                    )

            # Configure object storage if in S3 mode
            if self.storage_mode == "s3" and self.mooncake_bucket:
                # Use psycopg2 escaping to prevent SQL injection via bucket URL
                escaped_bucket = cursor.mogrify("%s", (self.mooncake_bucket,)).decode()
                cursor.execute(f"SET mooncake.default_bucket = {escaped_bucket}")
                self.logger.info(f"Set mooncake.default_bucket = {self.mooncake_bucket}")

            conn.commit()

        except RuntimeError:
            cursor.close()
            raise
        except Exception as e:
            self.logger.error(f"Failed to configure pg_mooncake extension: {e}")
            cursor.close()
            raise RuntimeError(f"pg_mooncake configuration failed: {e}") from e
        finally:
            if not cursor.closed:
                cursor.close()

        return conn

    def create_schema(self, connection: Any, schema_ddl: list[str], **kwargs) -> None:
        """Create schema with columnstore access method for all tables.

        Overrides PostgreSQL's create_schema to modify CREATE TABLE
        statements to use the columnstore access method. This is the
        critical differentiator — all benchmark tables use columnstore
        storage for Parquet-based columnar data.
        """
        modified_ddl = []
        for stmt in schema_ddl:
            modified_stmt = self._add_columnstore_access_method(stmt)
            modified_ddl.append(modified_stmt)

        # Delegate to parent with modified DDL
        super().create_schema(connection, modified_ddl, **kwargs)

    def _add_columnstore_access_method(self, ddl_statement: str) -> str:
        """Add USING columnstore to CREATE TABLE statements.

        Transforms:
            CREATE TABLE foo (col1 INT, col2 TEXT);
        Into:
            CREATE TABLE foo (col1 INT, col2 TEXT) USING columnstore;

        Only modifies CREATE TABLE statements. Other DDL (CREATE INDEX,
        ALTER TABLE, etc.) is passed through unchanged.
        """
        stripped = ddl_statement.strip()
        upper = stripped.upper()

        # Only modify CREATE TABLE statements
        if not upper.startswith("CREATE TABLE"):
            return ddl_statement

        # Don't double-add if already has USING columnstore
        if "USING COLUMNSTORE" in upper:
            return ddl_statement

        # Find the closing parenthesis of the column definitions
        # and insert USING columnstore before the semicolon
        if stripped.endswith(";"):
            return stripped[:-1] + " USING columnstore;"
        else:
            return stripped + " USING columnstore"

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get pg_mooncake platform information."""
        platform_info = super().get_platform_info(connection)

        # Override platform type and name
        platform_info["platform_type"] = "pg_mooncake"
        platform_info["platform_name"] = "pg_mooncake"

        # Add pg_mooncake-specific configuration
        platform_info["configuration"]["storage_mode"] = self.storage_mode
        if self.mooncake_bucket:
            platform_info["configuration"]["mooncake_bucket"] = self.mooncake_bucket

        if connection:
            try:
                cursor = connection.cursor()

                # Get pg_mooncake version
                cursor.execute("SELECT extversion FROM pg_extension WHERE extname = 'pg_mooncake'")
                result = cursor.fetchone()
                if result:
                    platform_info["pg_mooncake_version"] = result[0]

                cursor.close()
            except Exception as e:
                self.logger.debug(f"Error getting pg_mooncake info: {e}")

        return platform_info

    def supports_tuning_type(self, tuning_type: Any) -> bool:
        """Check if pg_mooncake supports a specific tuning type.

        pg_mooncake columnstore tables have different tuning characteristics
        than PostgreSQL heap tables:
        - No B-tree indexes on columnstore tables
        - No CLUSTER support (Parquet-based storage)
        - Partitioning handled at the Iceberg/Parquet level
        """
        try:
            from benchbox.core.tuning.interface import TuningType

            supported = {
                TuningType.PARTITIONING: False,  # Columnstore handles its own partitioning
                TuningType.SORTING: False,  # No native sort keys on columnstore
                TuningType.DISTRIBUTION: False,  # Not distributed
                TuningType.CLUSTERING: False,  # No CLUSTER on columnstore tables
                TuningType.PRIMARY_KEYS: False,  # Columnstore tables don't support constraints
                TuningType.FOREIGN_KEYS: False,  # Columnstore tables don't support constraints
            }
            return supported.get(tuning_type, False)
        except ImportError:
            return False


def _build_pg_mooncake_config(benchmark_config: dict, platform_options: dict) -> dict:
    """Build pg_mooncake configuration from benchmark and platform options.

    This function is registered with PlatformHookRegistry to provide
    pg_mooncake-specific configuration handling.
    """
    config = {
        "host": platform_options.get("host", "localhost"),
        "port": platform_options.get("port", 5432),
        "username": platform_options.get("username", "postgres"),
        "password": platform_options.get("password"),
        "schema": platform_options.get("schema", "public"),
        "database": platform_options.get("database"),
        "admin_database": platform_options.get("admin_database", "postgres"),
        "sslmode": platform_options.get("sslmode", "prefer"),
        "work_mem": platform_options.get("work_mem", "256MB"),
        "maintenance_work_mem": platform_options.get("maintenance_work_mem", "512MB"),
        "effective_cache_size": platform_options.get("effective_cache_size", "1GB"),
        "max_parallel_workers_per_gather": platform_options.get("max_parallel_workers_per_gather", 2),
        # pg_mooncake-specific
        "storage_mode": platform_options.get("storage_mode", "local"),
        "mooncake_bucket": platform_options.get("mooncake_bucket"),
    }

    # Merge benchmark configuration
    config.update(benchmark_config)

    return config
