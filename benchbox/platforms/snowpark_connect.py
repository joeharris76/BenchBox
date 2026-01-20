"""Snowpark Connect platform adapter for PySpark-compatible execution on Snowflake.

Snowpark Connect provides a PySpark DataFrame API compatibility layer that executes
on Snowflake's native query engine. This is NOT Apache Spark - it translates
DataFrame operations to Snowflake SQL, providing a familiar API without requiring
a Spark cluster.

Key Features:
- PySpark DataFrame API compatibility
- Native Snowflake query execution
- No Spark cluster required
- Snowflake's query optimization

Limitations (compared to Apache Spark):
- RDD APIs not supported
- DataFrame.hint() is a no-op
- DataFrame.repartition() is a no-op
- Some advanced Spark features unavailable

Usage:
    from benchbox.platforms.snowpark_connect import SnowparkConnectAdapter

    adapter = SnowparkConnectAdapter(
        account="my-account",
        user="my-user",
        password="my-password",
        warehouse="COMPUTE_WH",
        database="BENCHBOX",
    )

    # Run TPC-H benchmark with PySpark API
    adapter.create_schema("tpch_sf1")
    adapter.load_data(["lineitem", "orders", ...], source_dir)
    result = adapter.execute_query("SELECT * FROM lineitem LIMIT 10")

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from benchbox.core.tuning.interface import (
        UnifiedTuningConfiguration,
    )

from benchbox.core.exceptions import ConfigurationError
from benchbox.platforms.base import PlatformAdapter
from benchbox.platforms.base.cloud_spark import SparkTuningMixin
from benchbox.utils.dependencies import (
    check_platform_dependencies,
    get_dependency_error_message,
)

try:
    from snowflake.snowpark import Session
    from snowflake.snowpark.exceptions import SnowparkSQLException

    SNOWPARK_AVAILABLE = True
except ImportError:
    Session = None
    SnowparkSQLException = Exception
    SNOWPARK_AVAILABLE = False

logger = logging.getLogger(__name__)


class SnowparkConnectAdapter(SparkTuningMixin, PlatformAdapter):
    """Snowpark Connect adapter for PySpark-compatible execution on Snowflake.

    Snowpark Connect provides a PySpark DataFrame API that executes natively
    on Snowflake. Unlike traditional Spark platforms, there is no Spark cluster -
    DataFrame operations are translated to Snowflake SQL.

    Execution Model:
    - Create Snowpark Session with Snowflake credentials
    - Execute DataFrame operations (translated to SQL)
    - Results retrieved directly from Snowflake
    - No Spark cluster startup/shutdown

    Key Features:
    - PySpark-compatible DataFrame API
    - Native Snowflake query optimization
    - Zero Spark infrastructure management
    - Instant "startup" (no cluster provisioning)

    Limitations:
    - RDD APIs not supported (DataFrame only)
    - DataFrame.hint() is a no-op
    - DataFrame.repartition() is a no-op
    - Some Spark UDFs may not be compatible

    Billing:
    - Standard Snowflake credit consumption
    - Based on warehouse size and query duration
    """

    def __init__(
        self,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        warehouse: str = "COMPUTE_WH",
        database: str = "BENCHBOX",
        schema: str = "PUBLIC",
        role: str | None = None,
        authenticator: str | None = None,
        private_key_path: str | None = None,
        private_key_passphrase: str | None = None,
        warehouse_size: str = "MEDIUM",
        session_parameters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the Snowpark Connect adapter.

        Args:
            account: Snowflake account identifier (e.g., "xy12345.us-east-1").
            user: Snowflake username.
            password: Snowflake password.
            warehouse: Virtual warehouse name (default: COMPUTE_WH).
            database: Database name (default: BENCHBOX).
            schema: Schema name (default: PUBLIC).
            role: Role to use for the session.
            authenticator: Authentication method (snowflake, externalbrowser, oauth).
            private_key_path: Path to private key for key pair authentication.
            private_key_passphrase: Passphrase for private key.
            warehouse_size: Warehouse size (default: MEDIUM).
            session_parameters: Additional Snowpark session parameters.
            **kwargs: Additional platform options.
        """
        if not SNOWPARK_AVAILABLE:
            deps_satisfied, missing = check_platform_dependencies("snowpark-connect")
            if not deps_satisfied:
                raise ConfigurationError(get_dependency_error_message("snowpark-connect", missing))

        if not account:
            raise ConfigurationError(
                "account is required for Snowpark Connect. "
                "Provide your Snowflake account identifier (e.g., 'xy12345.us-east-1')."
            )

        if not user:
            raise ConfigurationError("user is required for Snowpark Connect.")

        # Password or key-based auth required
        if not password and not private_key_path:
            raise ConfigurationError("Either password or private_key_path is required for authentication.")

        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.authenticator = authenticator
        self.private_key_path = private_key_path
        self.private_key_passphrase = private_key_passphrase
        self.warehouse_size = warehouse_size
        self.session_parameters = session_parameters or {}

        # Snowpark Session (lazy initialization)
        self._session: Any = None

        # Metrics tracking
        self._query_count = 0
        self._total_execution_time_seconds = 0.0

        # Benchmark configuration
        self._benchmark_type: str | None = None
        self._scale_factor: float = 1.0

        super().__init__(**kwargs)

    def _build_connection_parameters(self) -> dict[str, Any]:
        """Build Snowpark connection parameters.

        Returns:
            Dict of connection parameters for Snowpark Session.
        """
        params = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }

        if self.password:
            params["password"] = self.password

        if self.role:
            params["role"] = self.role

        if self.authenticator:
            params["authenticator"] = self.authenticator

        if self.private_key_path:
            # Load private key for key pair authentication
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            with open(self.private_key_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=(self.private_key_passphrase.encode() if self.private_key_passphrase else None),
                    backend=default_backend(),
                )
            params["private_key"] = private_key

        return params

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Return platform metadata.

        Args:
            connection: Not used (Snowpark Connect manages sessions internally).

        Returns:
            Dict with platform information including name, version, and capabilities.
        """
        return {
            "platform": "snowpark-connect",
            "display_name": "Snowpark Connect for Spark",
            "vendor": "Snowflake",
            "type": "pyspark_compatible",
            "account": self.account,
            "warehouse": self.warehouse,
            "database": self.database,
            "supports_sql": True,
            "supports_dataframe": True,
            "billing_model": "Snowflake credits",
            "limitations": [
                "RDD APIs not supported",
                "DataFrame.hint() is no-op",
                "DataFrame.repartition() is no-op",
            ],
        }

    def create_connection(self, **kwargs: Any) -> Any:
        """Create a Snowpark Session.

        Returns:
            Snowpark Session object.

        Raises:
            ConfigurationError: If connection fails.
        """
        try:
            if self._session is not None:
                # Check if session is still valid
                try:
                    self._session.sql("SELECT 1").collect()
                    logger.info("Using existing Snowpark session")
                    return self._session
                except Exception:
                    # Session invalid, create new one
                    self._session = None

            logger.info(f"Creating Snowpark session for {self.account}")
            connection_params = self._build_connection_parameters()

            # Add session parameters
            if self.session_parameters:
                connection_params["session_parameters"] = self.session_parameters

            self._session = Session.builder.configs(connection_params).create()

            # Verify connection
            result = self._session.sql("SELECT CURRENT_VERSION()").collect()
            version = result[0][0] if result else "unknown"

            logger.info(f"Connected to Snowflake {version} via Snowpark")
            return self._session

        except SnowparkSQLException as e:
            raise ConfigurationError(f"Failed to connect to Snowflake: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Snowpark session creation failed: {e}") from e

    def create_schema(self, schema_name: str | None = None) -> None:
        """Create database and schema if they don't exist.

        Args:
            schema_name: Schema name (uses self.database if not provided).
        """
        if self._session is None:
            raise ConfigurationError("No active session. Call create_connection() first.")

        database = schema_name or self.database

        # Create database if not exists
        self._session.sql(f"CREATE DATABASE IF NOT EXISTS {database}").collect()
        logger.info(f"Database '{database}' created or already exists")

        # Use the database
        self._session.sql(f"USE DATABASE {database}").collect()

        # Create schema if not exists
        self._session.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}").collect()
        self._session.sql(f"USE SCHEMA {self.schema}").collect()
        logger.info(f"Schema '{self.schema}' created or already exists")

    def load_data(
        self,
        tables: list[str],
        source_dir: Path | str,
        file_format: str = "parquet",
        **kwargs: Any,
    ) -> dict[str, int]:
        """Load benchmark data into Snowflake tables.

        Args:
            tables: List of table names to load.
            source_dir: Local directory containing table data files.
            file_format: Data file format (default: parquet).
            **kwargs: Additional options.

        Returns:
            Dict mapping table names to row counts.
        """
        if self._session is None:
            raise ConfigurationError("No active session. Call create_connection() first.")

        source_path = Path(source_dir)
        if not source_path.exists():
            raise ConfigurationError(f"Source directory not found: {source_dir}")

        table_stats = {}

        for table in tables:
            # Find data files for this table
            table_files = list(source_path.glob(f"{table}.*")) + list(source_path.glob(f"{table}/*.parquet"))

            if not table_files:
                logger.warning(f"No data files found for table {table}")
                continue

            # For Parquet files, use Snowpark DataFrame API
            if file_format.lower() == "parquet":
                for file_path in table_files:
                    if file_path.suffix == ".parquet":
                        # Create internal stage and upload
                        stage_name = f"@~/{table}"
                        self._session.sql(f"PUT file://{file_path} {stage_name} AUTO_COMPRESS=FALSE").collect()

                # Create table from staged files
                df = self._session.read.parquet(f"@~/{table}/")
                df.write.mode("overwrite").save_as_table(table)

                # Get row count
                count_result = self._session.sql(f"SELECT COUNT(*) FROM {table}").collect()
                row_count = count_result[0][0] if count_result else 0
                table_stats[table] = row_count
                logger.info(f"Loaded {row_count:,} rows into {table}")

            else:
                # For CSV/TBL files, use COPY INTO
                for file_path in table_files:
                    stage_name = f"@~/{table}"
                    self._session.sql(f"PUT file://{file_path} {stage_name}").collect()

                # Create file format and COPY INTO
                self._session.sql(
                    f"COPY INTO {table} FROM @~/{table}/ FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|')"
                ).collect()

                count_result = self._session.sql(f"SELECT COUNT(*) FROM {table}").collect()
                row_count = count_result[0][0] if count_result else 0
                table_stats[table] = row_count
                logger.info(f"Loaded {row_count:,} rows into {table}")

        return table_stats

    def execute_query(
        self,
        query: str,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Execute a SQL query using Snowpark.

        Args:
            query: SQL query to execute.
            **kwargs: Additional query options.

        Returns:
            Query results as list of dicts.
        """
        if self._session is None:
            raise ConfigurationError("No active session. Call create_connection() first.")

        start_time = time.time()

        try:
            result = self._session.sql(query).collect()
            elapsed = time.time() - start_time

            self._query_count += 1
            self._total_execution_time_seconds += elapsed

            # Convert to list of dicts
            if result:
                # Get column names from first row
                columns = result[0].asDict().keys() if hasattr(result[0], "asDict") else []
                return [row.asDict() if hasattr(row, "asDict") else dict(zip(columns, row)) for row in result]

            return []

        except SnowparkSQLException as e:
            raise RuntimeError(f"Query execution failed: {e}") from e

    def execute_dataframe(
        self,
        df_operation: Any,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Execute a Snowpark DataFrame operation.

        This method supports PySpark-style DataFrame operations that are
        translated to Snowflake SQL.

        Args:
            df_operation: Snowpark DataFrame with operations applied.
            **kwargs: Additional options.

        Returns:
            Query results as list of dicts.

        Note:
            Some Spark operations are not supported or are no-ops:
            - RDD operations: Not available
            - hint(): No-op (Snowflake uses its own optimizer)
            - repartition(): No-op (Snowflake handles partitioning)
        """
        if self._session is None:
            raise ConfigurationError("No active session. Call create_connection() first.")

        start_time = time.time()

        try:
            result = df_operation.collect()
            elapsed = time.time() - start_time

            self._query_count += 1
            self._total_execution_time_seconds += elapsed

            # Convert to list of dicts
            if result:
                return [row.asDict() if hasattr(row, "asDict") else dict(row) for row in result]

            return []

        except SnowparkSQLException as e:
            raise RuntimeError(f"DataFrame execution failed: {e}") from e

    def get_dataframe(self, table_name: str) -> Any:
        """Get a Snowpark DataFrame for a table.

        Args:
            table_name: Name of the table.

        Returns:
            Snowpark DataFrame.
        """
        if self._session is None:
            raise ConfigurationError("No active session. Call create_connection() first.")

        return self._session.table(table_name)

    def close(self) -> None:
        """Close the Snowpark session."""
        if self._session is not None:
            try:
                self._session.close()
                logger.info("Snowpark session closed")
            except Exception as e:
                logger.warning(f"Error closing session: {e}")
            finally:
                self._session = None

        logger.info(f"Executed {self._query_count} queries.")
        if self._total_execution_time_seconds > 0:
            logger.info(f"Total execution time: {self._total_execution_time_seconds:.1f}s")

    @staticmethod
    def add_cli_arguments(parser: Any) -> None:
        """Add Snowpark Connect-specific CLI arguments.

        Args:
            parser: Argument parser to add arguments to.
        """
        group = parser.add_argument_group("Snowpark Connect Options")
        group.add_argument(
            "--account",
            help="Snowflake account identifier (e.g., xy12345.us-east-1)",
        )
        group.add_argument(
            "--user",
            help="Snowflake username",
        )
        group.add_argument(
            "--password",
            help="Snowflake password",
        )
        group.add_argument(
            "--warehouse",
            default="COMPUTE_WH",
            help="Virtual warehouse name (default: COMPUTE_WH)",
        )
        group.add_argument(
            "--database",
            default="BENCHBOX",
            help="Database name (default: BENCHBOX)",
        )
        group.add_argument(
            "--schema",
            default="PUBLIC",
            help="Schema name (default: PUBLIC)",
        )
        group.add_argument(
            "--role",
            help="Role to use for the session",
        )
        group.add_argument(
            "--warehouse-size",
            default="MEDIUM",
            help="Warehouse size (default: MEDIUM)",
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> SnowparkConnectAdapter:
        """Create adapter from configuration dict.

        Args:
            config: Configuration dictionary.

        Returns:
            Configured SnowparkConnectAdapter instance.
        """
        params = {
            "account": config.get("account"),
            "user": config.get("user"),
            "password": config.get("password"),
            "warehouse": config.get("warehouse", "COMPUTE_WH"),
            "database": config.get("database", "BENCHBOX"),
            "schema": config.get("schema", "PUBLIC"),
            "role": config.get("role"),
            "authenticator": config.get("authenticator"),
            "private_key_path": config.get("private_key_path"),
            "private_key_passphrase": config.get("private_key_passphrase"),
            "warehouse_size": config.get("warehouse_size", "MEDIUM"),
        }

        return cls(**params)

    def configure_for_benchmark(self, connection: Any, benchmark_type: str) -> None:
        """Configure adapter for specific benchmark.

        Args:
            connection: Session object.
            benchmark_type: Benchmark type (tpch, tpcds, ssb).
        """
        self._benchmark_type = benchmark_type.lower()
        logger.info(f"Configuring Snowpark Connect for {benchmark_type} benchmark")

        # Disable result cache for accurate benchmarking
        if self._session:
            self._session.sql("ALTER SESSION SET USE_CACHED_RESULT = FALSE").collect()
            logger.debug("Disabled result cache for benchmarking")

    def apply_tuning_configuration(
        self,
        config: UnifiedTuningConfiguration,
    ) -> dict[str, Any]:
        """Apply unified tuning configuration.

        Args:
            config: Unified tuning configuration.

        Returns:
            Dict with results of applied configurations.
        """
        results: dict[str, Any] = {}

        if config.scale_factor:
            self._scale_factor = config.scale_factor

        if config.primary_keys:
            results["primary_keys"] = self.apply_primary_keys(config.primary_keys)

        if config.foreign_keys:
            results["foreign_keys"] = self.apply_foreign_keys(config.foreign_keys)

        if config.platform:
            results["platform_optimizations"] = self.apply_platform_optimizations(config.platform)

        return results

    # apply_primary_keys, apply_foreign_keys, apply_platform_optimizations,
    # and apply_constraint_configuration are inherited from SparkTuningMixin

    def get_target_dialect(self) -> str:
        """Return the target SQL dialect for Snowpark Connect.

        Snowpark Connect uses Snowflake SQL.

        Returns:
            The dialect string "snowflake".
        """
        return "snowflake"
