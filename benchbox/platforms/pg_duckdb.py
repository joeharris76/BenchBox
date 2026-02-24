"""pg_duckdb platform adapter for BenchBox benchmarking.

Extends PostgreSQL adapter with pg_duckdb-specific functionality:
- DuckDB vectorized execution engine on PostgreSQL heap tables
- GUC parameter configuration (duckdb.force_execution, thread tuning)
- MotherDuck deployment mode for hybrid cloud queries

pg_duckdb is a PostgreSQL extension that embeds DuckDB's columnar-vectorized
analytics engine inside PostgreSQL, accelerating OLAP queries without requiring
SQL changes or data migration.

Deployment modes:
- self-hosted: Self-hosted PostgreSQL with pg_duckdb extension (default)
- motherduck: MotherDuck-connected mode for hybrid local+cloud queries

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


class PgDuckDBAdapter(PostgreSQLAdapter):
    """pg_duckdb platform adapter with DuckDB-accelerated query execution.

    Extends PostgreSQLAdapter with pg_duckdb-specific features:
    - DuckDB vectorized execution for analytical queries
    - Configurable force_execution and thread tuning
    - MotherDuck hybrid cloud mode

    Requires PostgreSQL 14+ with pg_duckdb 1.0+ extension installed.
    """

    @property
    def platform_name(self) -> str:
        return "pg_duckdb"

    def get_target_dialect(self) -> str:
        """Return the target SQL dialect for pg_duckdb (PostgreSQL-compatible)."""
        return POSTGRES_DIALECT

    @staticmethod
    def add_cli_arguments(parser) -> None:
        """Add pg_duckdb-specific CLI arguments."""
        if not hasattr(parser, "add_argument"):
            return
        try:
            # Inherit PostgreSQL connection arguments
            parser.add_argument(
                "--pgduckdb-host",
                dest="host",
                default="localhost",
                help="PostgreSQL server hostname (with pg_duckdb installed)",
            )
            parser.add_argument(
                "--pgduckdb-port",
                dest="port",
                type=int,
                default=5432,
                help="PostgreSQL server port",
            )
            parser.add_argument(
                "--pgduckdb-database",
                dest="database",
                help="PostgreSQL database name (auto-generated if not specified)",
            )
            parser.add_argument(
                "--pgduckdb-username",
                dest="username",
                default="postgres",
                help="PostgreSQL username",
            )
            parser.add_argument(
                "--pgduckdb-password",
                dest="password",
                help="PostgreSQL password",
            )
            parser.add_argument(
                "--pgduckdb-schema",
                dest="schema",
                default="public",
                help="PostgreSQL schema name",
            )
            # pg_duckdb-specific options
            parser.add_argument(
                "--pgduckdb-force-execution",
                dest="force_execution",
                action="store_true",
                default=True,
                help="Force DuckDB execution engine for all queries (default: True)",
            )
            parser.add_argument(
                "--pgduckdb-threads",
                dest="postgres_scan_threads",
                type=int,
                default=0,
                help="Threads for PostgreSQL table scanning (0 = auto)",
            )
        except Exception:
            pass

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> PgDuckDBAdapter:
        """Create pg_duckdb adapter from unified configuration."""
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

        # pg_duckdb-specific settings
        adapter_config["force_execution"] = config.get("force_execution", True)
        adapter_config["postgres_scan_threads"] = config.get("postgres_scan_threads", 0)

        # Deployment mode and MotherDuck support
        adapter_config["deployment_mode"] = config.get("deployment_mode", "self-hosted")
        if config.get("motherduck_token"):
            adapter_config["motherduck_token"] = config["motherduck_token"]

        # Force recreate
        adapter_config["force_recreate"] = config.get("force", False)

        # Pass through other config
        for key in ["tuning_config", "verbose_enabled", "very_verbose"]:
            if key in config:
                adapter_config[key] = config[key]

        return cls(**adapter_config)

    def __init__(self, **config):
        # Determine deployment mode with priority:
        # 1. deployment_mode (from factory via colon syntax: pg-duckdb:motherduck)
        # 2. Default to 'self-hosted'
        deployment_mode = config.get("deployment_mode", "self-hosted")
        self.deployment_mode = deployment_mode.lower()

        # Validate deployment mode
        valid_modes = {"self-hosted", "motherduck"}
        if self.deployment_mode not in valid_modes:
            raise ValueError(
                f"Invalid pg_duckdb deployment mode '{self.deployment_mode}'. "
                f"Valid modes: {', '.join(sorted(valid_modes))}"
            )

        # Configure for MotherDuck mode if specified
        if self.deployment_mode == "motherduck":
            self._configure_motherduck_mode(config)

        super().__init__(**config)

        # pg_duckdb-specific configuration
        self.force_execution = config.get("force_execution", True)
        self.postgres_scan_threads = config.get("postgres_scan_threads", 0)

        # MotherDuck token (set in _configure_motherduck_mode or from env)
        self.motherduck_token = config.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")

    def _configure_motherduck_mode(self, config: dict) -> None:
        """Configure adapter for MotherDuck hybrid mode.

        MotherDuck mode connects pg_duckdb to a MotherDuck cloud database,
        enabling hybrid queries that join local PostgreSQL tables with
        cloud-hosted MotherDuck data.

        Credentials via:
        - Config parameter: motherduck_token
        - Environment variable: MOTHERDUCK_TOKEN
        """
        token = config.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            raise ValueError(
                "MotherDuck deployment mode requires authentication token.\n"
                "Provide via --platform-option motherduck_token=<token> or "
                "set MOTHERDUCK_TOKEN environment variable.\n"
                "Get your token at https://app.motherduck.com/token"
            )
        config["motherduck_token"] = token

    def create_connection(self, **connection_config) -> Any:
        """Create PostgreSQL connection and configure pg_duckdb extension."""
        conn = super().create_connection(**connection_config)

        cursor = conn.cursor()
        try:
            # Verify pg_duckdb extension is installed
            cursor.execute("SELECT extversion FROM pg_extension WHERE extname = 'pg_duckdb'")
            result = cursor.fetchone()
            if result:
                self.logger.info(f"pg_duckdb extension version: {result[0]}")
            else:
                # Try to create the extension
                self.logger.info("pg_duckdb extension not found, attempting to create...")
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_duckdb")
                conn.commit()
                cursor.execute("SELECT extversion FROM pg_extension WHERE extname = 'pg_duckdb'")
                result = cursor.fetchone()
                if result:
                    self.logger.info(f"Created pg_duckdb extension version: {result[0]}")
                else:
                    raise RuntimeError(
                        "pg_duckdb extension is not available on this PostgreSQL server. "
                        "Install pg_duckdb (https://github.com/duckdb/pg_duckdb) or use "
                        "the 'postgresql' or 'duckdb' platform instead."
                    )

            # Set pg_duckdb GUC parameters
            if self.force_execution:
                cursor.execute("SET duckdb.force_execution = true")
                self.logger.info("Enabled duckdb.force_execution for DuckDB query routing")

            if self.postgres_scan_threads > 0:
                cursor.execute(f"SET duckdb.threads_for_postgres_scan = {int(self.postgres_scan_threads)}")
                self.logger.info(f"Set duckdb.threads_for_postgres_scan = {self.postgres_scan_threads}")

            # Configure MotherDuck if in motherduck mode
            if self.deployment_mode == "motherduck" and self.motherduck_token:
                # Use psycopg2 escaping to prevent SQL injection via token value
                escaped_token = cursor.mogrify("%s", (self.motherduck_token,)).decode()
                cursor.execute(f"SET duckdb.motherduck_token = {escaped_token}")
                self.logger.info("Configured MotherDuck token for hybrid queries")

            conn.commit()

        except RuntimeError:
            cursor.close()
            raise
        except Exception as e:
            self.logger.error(f"Failed to configure pg_duckdb extension: {e}")
            cursor.close()
            raise RuntimeError(f"pg_duckdb configuration failed: {e}") from e
        finally:
            if not cursor.closed:
                cursor.close()

        return conn

    def configure_for_benchmark(self, connection: Any, benchmark_type: str) -> None:
        """Apply pg_duckdb optimizations for benchmark type.

        pg_duckdb benefits from PostgreSQL OLAP settings but also adds
        DuckDB-specific execution configuration.
        """
        # Apply PostgreSQL OLAP optimizations first
        super().configure_for_benchmark(connection, benchmark_type)

        cursor = connection.cursor()
        try:
            if benchmark_type == "olap":
                # Ensure DuckDB execution is forced for analytical workloads
                cursor.execute("SET duckdb.force_execution = true")

            connection.commit()
        except Exception as e:
            self.logger.debug(f"Could not set pg_duckdb benchmark optimizations: {e}")
        finally:
            cursor.close()

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get pg_duckdb platform information."""
        platform_info = super().get_platform_info(connection)

        # Override platform type and name
        platform_info["platform_type"] = "pg_duckdb"
        platform_info["platform_name"] = "pg_duckdb"

        # Add pg_duckdb-specific configuration
        platform_info["configuration"]["force_execution"] = self.force_execution
        platform_info["configuration"]["postgres_scan_threads"] = self.postgres_scan_threads
        platform_info["configuration"]["deployment_mode"] = self.deployment_mode

        if connection:
            try:
                cursor = connection.cursor()

                # Get pg_duckdb version
                cursor.execute("SELECT extversion FROM pg_extension WHERE extname = 'pg_duckdb'")
                result = cursor.fetchone()
                if result:
                    platform_info["pg_duckdb_version"] = result[0]

                cursor.close()
            except Exception as e:
                self.logger.debug(f"Error getting pg_duckdb info: {e}")

        return platform_info

    def supports_tuning_type(self, tuning_type: Any) -> bool:
        """Check if pg_duckdb supports a specific tuning type.

        pg_duckdb operates on standard PostgreSQL heap tables, so tuning
        capabilities match PostgreSQL. The main optimization is the DuckDB
        execution engine itself, which benefits less from B-tree indexes
        for analytical queries.
        """
        try:
            from benchbox.core.tuning.interface import TuningType

            supported = {
                TuningType.PARTITIONING: True,  # PostgreSQL declarative partitioning
                TuningType.SORTING: False,  # No native sort keys
                TuningType.DISTRIBUTION: False,  # Not distributed
                TuningType.CLUSTERING: True,  # CLUSTER command available
                TuningType.PRIMARY_KEYS: True,  # Full constraint support
                TuningType.FOREIGN_KEYS: True,  # Full constraint support
            }
            return supported.get(tuning_type, False)
        except ImportError:
            return False


def _build_pg_duckdb_config(benchmark_config: dict, platform_options: dict) -> dict:
    """Build pg_duckdb configuration from benchmark and platform options.

    This function is registered with PlatformHookRegistry to provide
    pg_duckdb-specific configuration handling.
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
        # pg_duckdb-specific
        "force_execution": platform_options.get("force_execution", True),
        "postgres_scan_threads": platform_options.get("postgres_scan_threads", 0),
    }

    # Merge benchmark configuration
    config.update(benchmark_config)

    return config
