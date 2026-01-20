"""MotherDuck platform adapter for serverless DuckDB cloud.

MotherDuck is the managed cloud version of DuckDB, providing serverless
analytics with cloud storage integration. This adapter inherits DuckDB's
SQL dialect and benchmark compatibility while implementing MotherDuck-specific
authentication and connection handling.

Authentication:
- Uses MOTHERDUCK_TOKEN environment variable or config file
- Token can also be passed via --platform-option token=<token>

Connection:
- Uses DuckDB's native MotherDuck integration: md:database_name
- Supports hybrid queries accessing both local and cloud data

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

from benchbox.core.config_inheritance import (
    resolve_dialect_for_query_translation,
)

try:
    import duckdb
except ImportError:
    duckdb = None

from .base import PlatformAdapter

logger = logging.getLogger(__name__)


class MotherDuckAdapter(PlatformAdapter):
    """MotherDuck platform adapter - serverless DuckDB in the cloud.

    This adapter enables running benchmarks against MotherDuck, allowing
    direct comparison with local DuckDB performance.

    Authentication:
        Set MOTHERDUCK_TOKEN environment variable, or provide via config:
        --platform-option token=<your-motherduck-token>

    Example usage:
        benchbox run --platform motherduck --benchmark tpch --scale 0.01 \\
            --platform-option database=my_benchmark_db
    """

    def __init__(self, **config):
        """Initialize MotherDuck adapter.

        Args:
            **config: Configuration options:
                - token: MotherDuck authentication token (or use MOTHERDUCK_TOKEN env)
                - database: MotherDuck database name (default: benchbox)
                - memory_limit: Local memory limit for hybrid queries
        """
        super().__init__(**config)

        if duckdb is None:
            raise ImportError(
                "MotherDuck requires the duckdb package.\n"
                "Install with: uv add duckdb\n"
                "\nNote: MotherDuck support requires DuckDB >= 0.9.0"
            )

        # Use inherited dialect from DuckDB
        self._dialect = "duckdb"

        # Authentication
        self.token = config.get("token") or os.environ.get("MOTHERDUCK_TOKEN")
        if not self.token:
            raise ValueError(
                "MotherDuck requires authentication token.\n"
                "Set MOTHERDUCK_TOKEN environment variable, or provide via:\n"
                "  --platform-option token=<your-token>\n"
                "\nGet your token at: https://app.motherduck.com/token-request"
            )

        # Database configuration
        self.database = config.get("database", "benchbox")
        self.memory_limit = config.get("memory_limit", "4GB")

        # Connection state
        self.connection = None

        logger.info(f"MotherDuck adapter initialized for database: {self.database}")

    @property
    def platform_name(self) -> str:
        """Return platform display name."""
        return "MotherDuck"

    @staticmethod
    def add_cli_arguments(parser) -> None:
        """Add MotherDuck-specific CLI arguments."""
        md_group = parser.add_argument_group("MotherDuck Arguments")
        md_group.add_argument(
            "--motherduck-database",
            type=str,
            default="benchbox",
            help="MotherDuck database name (default: benchbox)",
        )
        md_group.add_argument(
            "--motherduck-token",
            type=str,
            help="MotherDuck authentication token (or use MOTHERDUCK_TOKEN env)",
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]):
        """Create MotherDuck adapter from unified configuration."""
        adapter_config = {
            "benchmark": config.get("benchmark"),
        }

        # Map CLI args to config
        if config.get("motherduck_database"):
            adapter_config["database"] = config["motherduck_database"]
        if config.get("motherduck_token"):
            adapter_config["token"] = config["motherduck_token"]
        if config.get("memory_limit"):
            adapter_config["memory_limit"] = config["memory_limit"]

        return cls(**adapter_config)

    def get_target_dialect(self) -> str:
        """Get the SQL dialect for query translation.

        MotherDuck uses DuckDB's SQL dialect, inherited via config_inheritance.
        """
        return resolve_dialect_for_query_translation("motherduck")

    def create_connection(self, connection_config: Optional[dict[str, Any]] = None):
        """Create connection to MotherDuck.

        Uses DuckDB's native MotherDuck integration with connection string:
        md:database_name?motherduck_token=TOKEN

        Returns:
            DuckDB connection connected to MotherDuck
        """
        if self.connection is not None:
            return self.connection

        # Build MotherDuck connection string
        connection_string = f"md:{self.database}?motherduck_token={self.token}"

        try:
            logger.info(f"Connecting to MotherDuck database: {self.database}")
            self.connection = duckdb.connect(connection_string)

            # Set memory limit for local operations
            self.connection.execute(f"SET memory_limit = '{self.memory_limit}'")

            # Test connection
            result = self.connection.execute("SELECT 1 AS test").fetchone()
            if result and result[0] == 1:
                logger.info("MotherDuck connection successful")
            else:
                raise ConnectionError("MotherDuck connection test failed")

            return self.connection

        except Exception as e:
            logger.error(f"Failed to connect to MotherDuck: {e}")
            raise ConnectionError(
                f"Failed to connect to MotherDuck: {e}\nCheck your MOTHERDUCK_TOKEN and network connection."
            )

    def close_connection(self, connection=None):
        """Close MotherDuck connection."""
        conn = connection or self.connection
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"Error closing MotherDuck connection: {e}")
        if connection is None:
            self.connection = None

    def execute_query(self, query: str, connection=None) -> tuple[Any, float]:
        """Execute a query against MotherDuck.

        Args:
            query: SQL query to execute
            connection: Optional connection to use

        Returns:
            Tuple of (result, execution_time_seconds)
        """
        conn = connection or self.connection
        if conn is None:
            conn = self.create_connection()

        start_time = time.perf_counter()
        try:
            result = conn.execute(query)
            rows = result.fetchall()
            execution_time = time.perf_counter() - start_time
            return rows, execution_time
        except Exception as e:
            execution_time = time.perf_counter() - start_time
            logger.error(f"Query execution failed after {execution_time:.2f}s: {e}")
            raise

    def get_platform_metadata(self) -> dict[str, Any]:
        """Get platform metadata including MotherDuck-specific info."""
        metadata = {
            "platform_type": "motherduck",
            "platform_name": self.platform_name,
            "dialect": self._dialect,
            "database": self.database,
            "inherits_from": "duckdb",
        }

        # Add DuckDB version info if connected
        if self.connection:
            try:
                result = self.connection.execute("SELECT version()").fetchone()
                if result:
                    metadata["duckdb_version"] = result[0]
            except Exception:
                pass

        return metadata

    def test_connection(self) -> bool:
        """Test MotherDuck connection."""
        try:
            conn = self.create_connection()
            result = conn.execute("SELECT 1").fetchone()
            return result is not None and result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def create_schema(self, benchmark, connection) -> float:
        """Create database schema for the benchmark.

        MotherDuck uses DuckDB's SQL dialect, so schema creation follows
        the same pattern as local DuckDB.

        Args:
            benchmark: Benchmark instance with schema definitions
            connection: DuckDB connection to MotherDuck

        Returns:
            Time taken to create schema in seconds
        """
        start_time = time.perf_counter()

        # Get CREATE TABLE statements from benchmark
        schema_sql = benchmark.get_create_tables_sql(dialect="duckdb")

        # Execute each statement
        for statement in schema_sql.split(";"):
            statement = statement.strip()
            if statement:
                connection.execute(statement)

        return time.perf_counter() - start_time

    def load_data(self, benchmark, connection, data_dir: Path) -> tuple[dict[str, int], float, dict[str, Any] | None]:
        """Load benchmark data into MotherDuck.

        Data can be loaded directly from local files into MotherDuck using
        DuckDB's file reading capabilities with MotherDuck's cloud backend.

        Args:
            benchmark: Benchmark instance
            connection: DuckDB connection to MotherDuck
            data_dir: Path to benchmark data directory

        Returns:
            Tuple of (row_counts, load_time, manifest)
        """
        start_time = time.perf_counter()
        row_counts: dict[str, int] = {}

        # Get table files from data directory
        for table_file in data_dir.glob("*.parquet"):
            table_name = table_file.stem

            # Load parquet file directly into MotherDuck
            logger.info(f"Loading {table_name} from {table_file}")
            connection.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM read_parquet('{table_file}')
            """)

            # Get row count
            result = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            row_counts[table_name] = result[0] if result else 0

        load_time = time.perf_counter() - start_time
        return row_counts, load_time, None

    def apply_platform_optimizations(self, platform_config, connection) -> None:
        """Apply platform-specific optimizations.

        MotherDuck handles optimization automatically - this is a no-op.
        """

    def apply_constraint_configuration(self, primary_key_config, foreign_key_config, connection) -> None:
        """Apply constraint configuration.

        MotherDuck/DuckDB doesn't enforce constraints by default,
        so this is typically a no-op unless explicitly configured.
        """

    def configure_for_benchmark(self, connection, benchmark_type: str) -> None:
        """Configure MotherDuck for benchmark execution.

        MotherDuck automatically handles most optimizations. We can set
        some DuckDB-level configurations for local processing if needed.

        Args:
            connection: DuckDB connection to MotherDuck
            benchmark_type: Type of benchmark (e.g., "olap")
        """
        # MotherDuck handles optimization automatically
        # Set reasonable defaults for analytical workloads
        try:
            connection.execute(f"SET memory_limit = '{self.memory_limit}'")
        except Exception as e:
            logger.warning(f"Could not set memory_limit: {e}")


__all__ = ["MotherDuckAdapter"]
