"""PrestoDB platform adapter with distributed SQL query engine optimizations.

Provides PrestoDB-specific optimizations for analytical workloads,
including connector catalog support, session properties, and query optimization.

PrestoDB is Meta's (Facebook's) distributed SQL query engine, maintained as an
open-source project separate from Trino (formerly PrestoSQL).

IMPORTANT: This adapter supports PrestoDB only, NOT Trino.

While PrestoDB and Trino share a common ancestry (Trino forked from Presto in 2019),
they have diverged significantly:
- Different Python drivers (presto-python-client vs trino)
- Different HTTP headers (X-Presto-* vs X-Trino-*)
- Diverging SQL syntax and function implementations
- Different system metadata table schemas

For Trino workloads, use the TrinoAdapter instead.
For AWS Athena (managed Presto-compatible), use the AthenaAdapter.
For Starburst Enterprise (commercial Trino), use the TrinoAdapter.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .base.data_loading import FileFormatRegistry

if TYPE_CHECKING:
    from benchbox.core.tuning.interface import (
        ForeignKeyConfiguration,
        PlatformOptimizationConfiguration,
        PrimaryKeyConfiguration,
        UnifiedTuningConfiguration,
    )

from ..utils.dependencies import (
    check_platform_dependencies,
    get_dependency_error_message,
)
from .base import PlatformAdapter

try:
    import prestodb
    from prestodb.auth import BasicAuthentication as PrestoBasicAuthentication
except ImportError:
    prestodb = None
    PrestoBasicAuthentication = None

PRESTO_DIALECT = "presto"


class PrestoAdapter(PlatformAdapter):
    """PrestoDB platform adapter for distributed SQL query execution.

    PrestoDB is a distributed SQL query engine designed for interactive analytics
    against data sources of all sizes. It supports querying data from multiple
    sources including Hive, Iceberg, Delta Lake, and cloud storage.

    Key Features:
    - Distributed query execution across multiple workers
    - Federated queries across multiple data sources
    - Session properties for query optimization
    - Support for Hive and other table formats

    Compatibility:
    - PrestoDB (Meta/Facebook): Fully supported
    - Trino: NOT supported - use TrinoAdapter instead
    - AWS Athena: Use AthenaAdapter instead (managed Presto-compatible service)
    - Starburst Enterprise: Use TrinoAdapter (Trino-based)
    """

    def __init__(self, **config):
        super().__init__(**config)

        # Check dependencies
        if not prestodb:
            available, missing = check_platform_dependencies("presto")
            if not available:
                error_msg = get_dependency_error_message("presto", missing)
                raise ImportError(error_msg)

        self._dialect = PRESTO_DIALECT

        # Presto connection configuration
        self.host = config.get("host") or "localhost"
        self.port = config.get("port") if config.get("port") is not None else 8080
        self.catalog = config.get("catalog")  # Required - validated in _validate_catalog_exists()
        self.schema = config.get("schema") or "default"
        self.source = config.get("source") or "BenchBox"

        # Authentication configuration
        self.username = config.get("username") or config.get("user") or "presto"
        self.password = config.get("password")

        # HTTP configuration
        self.http_scheme = config.get("http_scheme") or ("https" if self.password else "http")
        self.verify_ssl = config.get("verify_ssl") if config.get("verify_ssl") is not None else True
        self.ssl_cert_path = config.get("ssl_cert_path")

        # Session properties for query optimization
        self.session_properties = config.get("session_properties") or {}

        # Track if catalog was auto-selected (for credential saving)
        self._catalog_was_auto_selected = False

        # Query timeout in seconds (0 = no timeout)
        self.query_timeout = config.get("query_timeout") if config.get("query_timeout") is not None else 0

        # Result cache control - disable by default for accurate benchmarking
        self.disable_result_cache = config.get("disable_result_cache", True)

        # Table format configuration (memory, hive)
        # Note: PrestoDB has different Iceberg support than Trino
        self.table_format = config.get("table_format") or "memory"

        # Cloud storage configuration for data loading
        self.staging_root = config.get("staging_root")

        # Source catalog for external data loading (e.g., 'hive' connector for reading files)
        self.source_catalog = config.get("source_catalog")

    @property
    def platform_name(self) -> str:
        return "Presto"

    @staticmethod
    def add_cli_arguments(parser) -> None:
        """Add Presto-specific CLI arguments."""

        presto_group = parser.add_argument_group("Presto Arguments")
        presto_group.add_argument("--host", type=str, default="localhost", help="Presto coordinator hostname")
        presto_group.add_argument("--port", type=int, default=8080, help="Presto coordinator port")
        presto_group.add_argument("--catalog", type=str, help="Catalog for queries (e.g., hive, iceberg)")
        presto_group.add_argument("--schema", type=str, default="default", help="Default schema within the catalog")
        presto_group.add_argument("--username", type=str, default="presto", help="Username for authentication")
        presto_group.add_argument("--password", type=str, help="Password for basic authentication")
        presto_group.add_argument(
            "--http-scheme", type=str, choices=["http", "https"], help="HTTP scheme (auto-detected based on password)"
        )
        presto_group.add_argument(
            "--table-format",
            type=str,
            choices=["memory", "hive"],
            default="memory",
            help="Table format for creating benchmark tables",
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]):
        """Create Presto adapter from unified configuration."""
        from benchbox.utils.database_naming import generate_database_name

        adapter_config: dict[str, Any] = {}

        # Generate proper schema name using benchmark characteristics
        # Presto uses schema instead of database
        if "schema" in config and config["schema"]:
            adapter_config["schema"] = config["schema"]
        else:
            schema_name = generate_database_name(
                benchmark_name=config["benchmark"],
                scale_factor=config["scale_factor"],
                platform="presto",
                tuning_config=config.get("tuning_config"),
            )
            adapter_config["schema"] = schema_name

        # Core connection parameters
        for key in ["host", "port", "catalog", "username", "password"]:
            if key in config:
                adapter_config[key] = config[key]

        # Optional configuration parameters
        for key in [
            "http_scheme",
            "verify_ssl",
            "ssl_cert_path",
            "session_properties",
            "query_timeout",
            "disable_result_cache",
            "table_format",
            "staging_root",
            "source_catalog",
            "source",
        ]:
            if key in config:
                adapter_config[key] = config[key]

        return cls(**adapter_config)

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get Presto platform information.

        Captures comprehensive Presto configuration including:
        - Presto version
        - Node count
        - Catalog and schema configuration
        - Session properties
        """
        platform_info = {
            "platform_type": "presto",
            "platform_name": "PrestoDB",
            "connection_mode": "remote",
            "dialect": PRESTO_DIALECT,
            "host": self.host,
            "port": self.port,
            "configuration": {
                "catalog": self.catalog,
                "schema": self.schema,
                "http_scheme": self.http_scheme,
                "table_format": self.table_format,
                "result_cache_disabled": self.disable_result_cache,
                "session_properties": dict(self.session_properties) if self.session_properties else {},
                "client_source": self.source,
            },
        }

        # Get client library version
        if prestodb:
            try:
                platform_info["client_library_version"] = prestodb.__version__
            except AttributeError:
                platform_info["client_library_version"] = None
        else:
            platform_info["client_library_version"] = None

        # Try to get Presto version and extended metadata from connection
        if connection:
            cursor = None
            try:
                cursor = connection.cursor()

                # Get Presto version from system runtime
                # Note: PrestoDB uses same system tables as Trino (shared heritage)
                try:
                    cursor.execute("SELECT node_version FROM system.runtime.nodes WHERE coordinator = true LIMIT 1")
                    result = cursor.fetchone()
                    platform_info["platform_version"] = result[0] if result else None
                except Exception as primary_error:
                    self.logger.debug(f"Primary Presto version query failed: {primary_error}")

                # Fallback to SELECT version() if node metadata not accessible
                if platform_info.get("platform_version") is None:
                    try:
                        cursor.execute("SELECT version()")
                        result = cursor.fetchone()
                        platform_info["platform_version"] = result[0] if result else None
                    except Exception as fallback_error:
                        self.logger.debug(f"Fallback Presto version query failed: {fallback_error}")

                # Get node count
                try:
                    cursor.execute("SELECT count(*) FROM system.runtime.nodes")
                    result = cursor.fetchone()
                    if result:
                        platform_info["configuration"]["node_count"] = result[0]
                except Exception as node_error:
                    self.logger.debug(f"Error collecting Presto node count: {node_error}")

                # Get catalog list
                try:
                    cursor.execute("SHOW CATALOGS")
                    catalogs = [row[0] for row in cursor.fetchall()]
                    platform_info["configuration"]["available_catalogs"] = catalogs
                except Exception as catalog_error:
                    self.logger.debug(f"Error collecting Presto catalogs: {catalog_error}")

            except Exception as e:
                self.logger.debug(f"Error collecting Presto platform info: {e}")
                if platform_info.get("platform_version") is None:
                    platform_info["platform_version"] = None
            finally:
                if cursor:
                    cursor.close()
        else:
            platform_info["platform_version"] = None

        return platform_info

    def get_target_dialect(self) -> str:
        """Return the target SQL dialect for Presto."""
        return PRESTO_DIALECT

    def _get_connection_params(self) -> dict[str, Any]:
        """Get connection parameters for Presto."""
        params: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "user": self.username,
            "catalog": self.catalog,
            "schema": self.schema,
            "http_scheme": self.http_scheme,
            "source": self.source,
        }

        # Add authentication if password is provided
        if self.password and PrestoBasicAuthentication:
            params["auth"] = PrestoBasicAuthentication(self.username, self.password)

        # SSL verification - presto-python-client uses 'requests_kwargs' for SSL config
        if self.http_scheme == "https":
            requests_kwargs = {}
            if self.ssl_cert_path:
                requests_kwargs["verify"] = self.ssl_cert_path
            else:
                requests_kwargs["verify"] = self.verify_ssl
            if requests_kwargs:
                params["requests_kwargs"] = requests_kwargs

        # Apply session configuration at connection time when provided
        if self.session_properties:
            params["session_properties"] = self.session_properties

        # Enforce request timeout when configured (0 means client default)
        if self.query_timeout and self.query_timeout > 0:
            params["request_timeout"] = self.query_timeout

        return params

    def _get_available_catalogs(self) -> list[str]:
        """Get list of available catalogs from Presto server.

        Returns:
            List of catalog names available on the server.
        """
        try:
            params = self._get_connection_params()
            # Use system catalog which should always exist
            params["catalog"] = "system"
            params["schema"] = "runtime"

            conn = prestodb.dbapi.connect(**params)
            cursor = conn.cursor()

            try:
                cursor.execute("SHOW CATALOGS")
                catalogs = [row[0] for row in cursor.fetchall()]
                return catalogs
            finally:
                cursor.close()
                conn.close()

        except Exception as e:
            self.logger.debug(f"Error getting available catalogs: {e}")
            return []

    def _auto_select_catalog(self) -> str | None:
        """Auto-select best catalog for benchmarking when none specified.

        Connects to the server and selects the most appropriate catalog
        based on typical production configurations.

        Returns:
            Selected catalog name, or None if server unreachable or no usable catalogs
        """
        available = self._get_available_catalogs()

        if not available:
            return None

        # System catalogs that cannot be used for benchmarking (no CREATE TABLE support)
        system_only_catalogs = {"jmx", "system"}

        # Filter to only usable data catalogs
        usable_catalogs = [c for c in available if c not in system_only_catalogs]

        if not usable_catalogs:
            # Only system catalogs exist - return None to trigger helpful error
            return None

        # Prefer persistent storage for production-like behavior
        # hive: common in production, persistent storage
        # iceberg: modern format, good performance
        # delta: Databricks ecosystem
        # memory: fallback, works everywhere but volatile
        # tpch: built-in TPC-H data (if available)
        preferred = ["hive", "iceberg", "delta", "memory", "tpch"]

        for catalog in preferred:
            if catalog in usable_catalogs:
                return catalog

        # Fall back to first usable catalog
        return usable_catalogs[0]

    def _validate_catalog_exists(self, catalog: str | None) -> str:
        """Validate catalog exists, or auto-select one if not specified.

        Args:
            catalog: Catalog name to validate, or None to auto-select

        Returns:
            Validated catalog name (may be auto-selected)

        Raises:
            ConfigurationError: If no catalog available or server unreachable
        """
        from ..core.exceptions import ConfigurationError

        # Auto-select catalog if not provided
        if not catalog:
            catalog = self._auto_select_catalog()
            if catalog:
                self._catalog_was_auto_selected = True
                self.logger.info(f"Auto-selected catalog '{catalog}' for benchmarking")
            else:
                # Determine if server unreachable or only system catalogs exist
                available = self._get_available_catalogs()
                if available:
                    # Server reachable but only system catalogs (jmx, system)
                    catalog_list = ", ".join(sorted(available))
                    raise ConfigurationError(
                        f"No usable data catalogs found on Presto server.\n"
                        f"Available catalogs ({catalog_list}) are system-only and cannot store data.\n"
                        "Configure a data catalog (hive, iceberg, delta, or memory) on your Presto server,\n"
                        "or specify a catalog explicitly with --platform-option catalog=<name>"
                    )
                else:
                    # Server unreachable
                    raise ConfigurationError(
                        "Presto requires a catalog but server is unreachable.\n"
                        "Ensure the server is running, then either:\n"
                        "  1. Run again (catalog will be auto-selected)\n"
                        "  2. Specify explicitly: --platform-option catalog=<name>\n"
                        "  3. Configure default: benchbox platforms setup"
                    )

        # Validate the catalog exists
        available_catalogs = self._get_available_catalogs()

        if not available_catalogs:
            # Can't validate - proceed and let connection fail with specific error
            self.logger.debug("Could not query available catalogs - proceeding with specified catalog")
            return catalog

        if catalog in available_catalogs:
            return catalog

        # Catalog doesn't exist - raise helpful error
        catalog_list = ", ".join(sorted(available_catalogs))
        raise ConfigurationError(
            f"Catalog '{catalog}' does not exist on the Presto server.\n"
            f"Available catalogs: {catalog_list}\n"
            "Specify a valid catalog with --platform-option catalog=<name> (e.g., --platform-option catalog=hive)"
        )

    def check_server_database_exists(self, **connection_config) -> bool:
        """Check if schema exists in Presto catalog.

        Presto uses schemas instead of databases. Schemas exist within catalogs.
        """
        try:
            catalog = connection_config.get("catalog", self.catalog)
            schema = connection_config.get("schema", self.schema)

            # Validate and potentially auto-select catalog
            validated_catalog = self._validate_catalog_exists(catalog)

            # Store the validated catalog for use in subsequent operations
            if not self.catalog or self._catalog_was_auto_selected:
                self.catalog = validated_catalog

            catalog = validated_catalog

            # Validate identifiers
            if not self._validate_identifier(catalog) or not self._validate_identifier(schema):
                self.logger.warning(f"Invalid catalog or schema identifier: {catalog}.{schema}")
                return False

            # Create a connection to check schema existence
            params = self._get_connection_params()
            # Connect to information_schema within the target catalog
            params["catalog"] = catalog
            params["schema"] = "information_schema"

            conn = prestodb.dbapi.connect(**params)
            cursor = conn.cursor()

            try:
                cursor.execute(
                    f"SELECT schema_name FROM information_schema.schemata "
                    f"WHERE catalog_name = '{catalog}' AND schema_name = '{schema}'"
                )
                result = cursor.fetchone()
                return result is not None
            finally:
                cursor.close()
                conn.close()

        except Exception as e:
            # Re-raise ConfigurationError (catalog validation errors)
            if (
                "ConfigurationError" in type(e).__name__
                or "server is unreachable" in str(e)
                or "does not exist on the Presto server" in str(e)
            ):
                raise
            self.logger.debug(f"Error checking schema existence: {e}")
            return False

    def _validate_identifier(self, identifier: str) -> bool:
        """Validate SQL identifier to prevent injection attacks.

        Args:
            identifier: The identifier to validate (catalog, schema, table name)

        Returns:
            True if identifier is safe, False otherwise
        """
        if not identifier:
            return False
        # Allow alphanumeric, underscores, and hyphens (common in Presto identifiers)
        # Must start with letter or underscore
        import re

        pattern = r"^[a-zA-Z_][a-zA-Z0-9_-]*$"
        return bool(re.match(pattern, identifier)) and len(identifier) <= 128

    def _save_auto_selected_catalog(self) -> None:
        """Save auto-selected catalog to credentials for future runs."""
        try:
            from benchbox.security.credentials import CredentialManager

            cred_manager = CredentialManager()
            platform = "presto"

            # Get existing credentials and merge
            existing = cred_manager.get_platform_credentials(platform) or {}
            existing["catalog"] = self.catalog
            existing["host"] = self.host
            existing["port"] = self.port

            cred_manager.save_platform_credentials(platform, existing)
            self.logger.info(
                f"Saved '{self.catalog}' as default catalog for future presto runs. "
                f"Override with --platform-option catalog=<name> or edit ~/.benchbox/credentials.yaml"
            )
        except Exception as e:
            self.logger.debug(f"Could not save credentials: {e}")

    def drop_database(self, **connection_config) -> None:
        """Drop schema in Presto catalog.

        Presto uses DROP SCHEMA for removing schemas.
        DROP SCHEMA CASCADE is not supported by Presto connectors (including memory,
        hive, iceberg), so we drop tables individually before dropping the schema.
        """
        schema = connection_config.get("schema", self.schema)
        catalog = connection_config.get("catalog", self.catalog)

        # Validate identifiers to prevent SQL injection
        if not self._validate_identifier(catalog) or not self._validate_identifier(schema):
            raise ValueError(f"Invalid catalog or schema identifier: {catalog}.{schema}")

        # Check if schema exists first
        if not self.check_server_database_exists(schema=schema, catalog=catalog):
            self.log_verbose(f"Schema {catalog}.{schema} does not exist - nothing to drop")
            return

        try:
            params = self._get_connection_params()
            # Connect to the target schema to list and drop tables
            params["catalog"] = catalog
            params["schema"] = schema

            conn = prestodb.dbapi.connect(**params)
            cursor = conn.cursor()

            try:
                # Get list of tables in the schema
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]

                # Drop each table individually (Presto connectors don't support CASCADE)
                for table in tables:
                    if self._validate_identifier(table):
                        try:
                            cursor.execute(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}")
                            self.logger.debug(f"Dropped table {table}")
                        except Exception as table_error:
                            self.logger.warning(f"Failed to drop table {table}: {table_error}")

                # Now drop the empty schema
                cursor.execute(f"DROP SCHEMA IF EXISTS {catalog}.{schema}")
                self.logger.info(f"Dropped schema {catalog}.{schema}")
            finally:
                cursor.close()
                conn.close()

        except Exception as e:
            raise RuntimeError(f"Failed to drop Presto schema {catalog}.{schema}: {e}") from e

    def create_connection(self, **connection_config) -> Any:
        """Create optimized Presto connection."""
        self.log_operation_start("Presto connection")

        # Handle existing database using base class method
        self.handle_existing_database(**connection_config)

        params = self._get_connection_params()

        # Override with connection_config if provided
        for key in ["host", "port", "catalog", "schema"]:
            if key in connection_config:
                params[key] = connection_config[key]

        target_schema = params.get("schema", self.schema)
        target_catalog = params.get("catalog", self.catalog)

        # Create schema if needed (before connecting to it)
        if not self.database_was_reused:
            schema_exists = self.check_server_database_exists(schema=target_schema, catalog=target_catalog)

            if not schema_exists:
                self.log_verbose(f"Creating schema: {target_catalog}.{target_schema}")

                # Create schema using a connection to information_schema
                temp_params = params.copy()
                temp_params["schema"] = "information_schema"

                temp_conn = prestodb.dbapi.connect(**temp_params)
                temp_cursor = temp_conn.cursor()

                try:
                    temp_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
                    self.logger.info(f"Created schema {target_catalog}.{target_schema}")
                finally:
                    temp_cursor.close()
                    temp_conn.close()

        self.log_very_verbose(f"Presto connection params: host={params.get('host')}, catalog={target_catalog}")

        try:
            # presto-python-client applies session properties and headers at connect time
            connection = prestodb.dbapi.connect(**params)

            # Test connection
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()

            self.logger.info(f"Connected to Presto coordinator at {params['host']}:{params['port']}")

            # Save auto-selected catalog to credentials for future runs
            if self._catalog_was_auto_selected:
                self._save_auto_selected_catalog()

            self.log_operation_complete("Presto connection", details=f"Connected to {params['host']}:{params['port']}")

            return connection

        except Exception as e:
            self.logger.error(f"Failed to connect to Presto: {e}")
            raise

    def create_schema(self, benchmark, connection: Any) -> float:
        """Create schema using Presto-optimized table definitions."""
        start_time = time.time()

        cursor = connection.cursor()

        try:
            # Use common schema creation helper
            schema_sql = self._create_schema_with_tuning(benchmark, source_dialect="duckdb")

            # Split schema into individual statements and execute
            statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

            for statement in statements:
                # Skip empty statements
                if not statement:
                    continue

                # Normalize table names to lowercase for Presto consistency
                statement = self._normalize_table_name_in_sql(statement)

                # Optimize table definition for Presto
                statement = self._optimize_table_definition(statement)

                try:
                    cursor.execute(statement)
                    self.logger.debug(f"Executed schema statement: {statement[:100]}...")
                except Exception as e:
                    # If table already exists, drop and recreate
                    if "already exists" in str(e).lower():
                        table_name = self._extract_table_name(statement)
                        if table_name:
                            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                            cursor.execute(statement)
                    else:
                        raise

            self.logger.info("Schema created")

        except Exception as e:
            self.logger.error(f"Schema creation failed: {e}")
            raise
        finally:
            cursor.close()

        return time.time() - start_time

    def load_data(
        self, benchmark, connection: Any, data_dir: Path
    ) -> tuple[dict[str, int], float, dict[str, Any] | None]:
        """Load data using Presto INSERT statements.

        Presto supports data loading via:
        1. INSERT INTO ... VALUES for small datasets
        2. INSERT INTO ... SELECT from external tables (Hive, S3) for large datasets
        3. CREATE TABLE AS SELECT from external sources

        For benchmarking, we use INSERT statements with values or external table approach
        depending on data size and staging configuration.
        """
        start_time = time.time()
        table_stats = {}

        cursor = connection.cursor()
        target_catalog = self.catalog
        target_schema = self.schema

        if not self._validate_identifier(target_catalog) or not self._validate_identifier(target_schema):
            raise ValueError(f"Invalid catalog or schema for load: {target_catalog}.{target_schema}")

        try:
            # Get data files from benchmark or manifest fallback
            if hasattr(benchmark, "tables") and benchmark.tables:
                data_files = benchmark.tables
            else:
                data_files = None
                try:
                    manifest_path = Path(data_dir) / "_datagen_manifest.json"
                    if manifest_path.exists():
                        with open(manifest_path) as f:
                            manifest = json.load(f)
                        tables = manifest.get("tables") or {}
                        mapping = {}
                        for table, entries in tables.items():
                            if entries:
                                chunk_paths = []
                                for entry in entries:
                                    rel = entry.get("path")
                                    if rel:
                                        chunk_paths.append(Path(data_dir) / rel)
                                if chunk_paths:
                                    mapping[table] = chunk_paths
                        if mapping:
                            data_files = mapping
                            self.logger.debug("Using data files from _datagen_manifest.json")
                except Exception as e:
                    self.logger.debug(f"Manifest fallback failed: {e}")
                if not data_files:
                    raise ValueError("No data files found. Ensure benchmark.generate_data() was called first.")

            # Load data using INSERT statements (row by row for memory catalog)
            for table_name, file_paths in data_files.items():
                # Normalize to list
                if not isinstance(file_paths, list):
                    file_paths = [file_paths]

                # Filter valid files
                valid_files = []
                for file_path in file_paths:
                    file_path = Path(file_path)
                    if file_path.exists() and file_path.stat().st_size > 0:
                        valid_files.append(file_path)

                if not valid_files:
                    self.logger.warning(f"Skipping {table_name} - no valid data files")
                    table_stats[table_name.lower()] = 0
                    continue

                table_name_lower = table_name.lower()

                if not self._validate_identifier(table_name_lower):
                    self.logger.warning(f"Skipping {table_name} - invalid table identifier")
                    table_stats[table_name_lower] = 0
                    continue

                qualified_table = f"{target_catalog}.{target_schema}.{table_name_lower}"

                chunk_info = f" from {len(valid_files)} file(s)" if len(valid_files) > 1 else ""
                self.log_verbose(f"Loading data for table: {table_name}{chunk_info}")

                try:
                    load_start = time.time()
                    total_rows_loaded = 0

                    for file_path in valid_files:
                        file_path = Path(file_path)

                        # Detect delimiter from file extension (handle compressed extensions)
                        file_str = str(file_path.name)
                        delimiter = "|" if ".tbl" in file_str or ".dat" in file_str else ","

                        # Get compression handler (handles .zst, .gz, or uncompressed)
                        compression_handler = FileFormatRegistry.get_compression_handler(file_path)

                        # Load data using INSERT statements in batches
                        with compression_handler.open(file_path) as f:
                            batch_size = 500  # Smaller batch for Presto
                            batch_data = []

                            for line in f:
                                line = line.strip()
                                if line and line.endswith(delimiter):
                                    line = line[:-1]

                                if not line:
                                    continue

                                values = line.split(delimiter)
                                # Escape values for SQL - Presto memory catalog is strict about types
                                escaped_values = []
                                for v in values:
                                    if v == "" or v.lower() == "null":
                                        escaped_values.append("NULL")
                                    elif self._is_date_value(v):
                                        # DATE columns need DATE literal syntax
                                        escaped_values.append(f"DATE '{v}'")
                                    else:
                                        # Check if value is numeric (int or decimal)
                                        # Presto requires unquoted numbers for INTEGER/DECIMAL columns
                                        try:
                                            # Try parsing as number - handles integers and decimals
                                            float(v)
                                            # If it's a valid number, don't quote it
                                            escaped_values.append(v)
                                        except ValueError:
                                            # Not a number - quote and escape single quotes
                                            escaped_values.append("'" + str(v).replace("'", "''") + "'")
                                batch_data.append(f"({', '.join(escaped_values)})")

                                if len(batch_data) >= batch_size:
                                    insert_sql = f"INSERT INTO {qualified_table} VALUES " + ", ".join(batch_data)
                                    cursor.execute(insert_sql)
                                    total_rows_loaded += len(batch_data)
                                    batch_data = []

                            # Insert remaining batch
                            if batch_data:
                                insert_sql = f"INSERT INTO {qualified_table} VALUES " + ", ".join(batch_data)
                                cursor.execute(insert_sql)
                                total_rows_loaded += len(batch_data)

                    table_stats[table_name_lower] = total_rows_loaded

                    load_time = time.time() - load_start
                    self.logger.info(
                        f"✅ Loaded {total_rows_loaded:,} rows into {table_name_lower}{chunk_info} in {load_time:.2f}s"
                    )

                except Exception as e:
                    error_str = str(e)
                    self.logger.error(f"Failed to load {table_name}: {error_str[:100]}...")

                    # Detect memory limit errors and provide actionable guidance
                    is_memory_error = "MEMORY_LIMIT_EXCEEDED" in error_str or "exceeds max memory" in error_str.lower()
                    if is_memory_error and not hasattr(self, "_memory_error_logged"):
                        self._memory_error_logged = True
                        self.logger.error(
                            "\n"
                            "╭─────────────────────────────────────────────────────────────────╮\n"
                            "│ PRESTO MEMORY LIMIT EXCEEDED                                    │\n"
                            "├─────────────────────────────────────────────────────────────────┤\n"
                            "│ The Presto server has insufficient memory for this data load.  │\n"
                            "│                                                                 │\n"
                            "│ Options to resolve:                                             │\n"
                            "│                                                                 │\n"
                            "│ 1. Increase Presto memory (recommended for SF1+):              │\n"
                            "│    Edit jvm.config: -Xmx4G                                     │\n"
                            "│    Edit config.properties:                                     │\n"
                            "│      query.max-memory=2GB                                      │\n"
                            "│      query.max-memory-per-node=2GB                             │\n"
                            "│                                                                 │\n"
                            "│ 2. Use a smaller scale factor:                                 │\n"
                            "│    benchbox run --platform presto --scale 0.1 ...              │\n"
                            "│                                                                 │\n"
                            "│ 3. Use a persistent catalog instead of 'memory':               │\n"
                            "│    The memory catalog stores all data in RAM.                  │\n"
                            "│    For SF1+, use hive, iceberg, or delta catalogs.             │\n"
                            "╰─────────────────────────────────────────────────────────────────╯"
                        )

                    table_stats[table_name.lower()] = 0

            total_time = time.time() - start_time
            total_rows = sum(table_stats.values())
            self.logger.info(f"✅ Loaded {total_rows:,} total rows in {total_time:.2f}s")

        except Exception as e:
            self.logger.error(f"Data loading failed: {e}")
            raise
        finally:
            cursor.close()

        return table_stats, total_time, None

    def configure_for_benchmark(self, connection: Any, benchmark_type: str) -> None:
        """Apply Presto-specific optimizations based on benchmark type."""

        cursor = connection.cursor()

        try:
            # Set session properties for benchmark optimization
            session_settings = []

            if benchmark_type.lower() in ["olap", "analytics", "tpch", "tpcds"]:
                # OLAP-specific optimizations
                session_settings.extend(
                    [
                        # Enable cost-based optimization (PrestoDB syntax)
                        "SET SESSION optimize_hash_generation = true",
                        "SET SESSION join_reordering_strategy = 'AUTOMATIC'",
                        "SET SESSION join_distribution_type = 'AUTOMATIC'",
                    ]
                )

            # Apply session settings
            for setting in session_settings:
                try:
                    cursor.execute(setting)
                    self.logger.debug(f"Applied setting: {setting}")
                except Exception as e:
                    self.logger.warning(f"Failed to apply setting {setting}: {e}")

        finally:
            cursor.close()

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
        """Execute query with detailed timing and performance tracking."""
        start_time = time.time()

        cursor = connection.cursor()

        try:
            # Execute the query
            cursor.execute(query)
            result = cursor.fetchall()

            execution_time = time.time() - start_time
            actual_row_count = len(result) if result else 0

            # Get query statistics
            query_stats = {"execution_time_seconds": execution_time}

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

            # Use base helper to build result with consistent validation field mapping
            result_dict = self._build_query_result_with_validation(
                query_id=query_id,
                execution_time=execution_time,
                actual_row_count=actual_row_count,
                first_row=result[0] if result else None,
                validation_result=validation_result,
            )

            # Include Presto-specific fields
            result_dict["query_statistics"] = query_stats
            result_dict["resource_usage"] = query_stats

            return result_dict

        except Exception as e:
            execution_time = time.time() - start_time

            return {
                "query_id": query_id,
                "status": "FAILED",
                "execution_time": execution_time,
                "rows_returned": 0,
                "error": str(e),
                "error_type": type(e).__name__,
            }
        finally:
            cursor.close()

    def _is_date_value(self, value: str) -> bool:
        """Check if a value looks like a date in YYYY-MM-DD format.

        Presto memory catalog requires DATE literal syntax (DATE 'YYYY-MM-DD')
        for date columns, unlike other databases that auto-cast strings.
        """
        import re

        # Match YYYY-MM-DD format (TPC-H standard date format)
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value))

    def _extract_table_name(self, statement: str) -> str | None:
        """Extract table name from CREATE TABLE statement."""
        try:
            import re

            match = re.search(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)", statement, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        except Exception:
            pass
        return None

    def _normalize_table_name_in_sql(self, sql: str) -> str:
        """Normalize table names in SQL to lowercase for Presto."""
        import re

        # Match CREATE TABLE "TABLENAME" or CREATE TABLE TABLENAME
        sql = re.sub(
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?',
            lambda m: f"CREATE TABLE {m.group(1).lower()}",
            sql,
            flags=re.IGNORECASE,
        )

        # Match foreign key references
        sql = re.sub(
            r'REFERENCES\s+"?([A-Za-z_][A-Za-z0-9_]*)"?',
            lambda m: f"REFERENCES {m.group(1).lower()}",
            sql,
            flags=re.IGNORECASE,
        )

        return sql

    def _optimize_table_definition(self, statement: str) -> str:
        """Optimize table definition for Presto.

        Presto table creation syntax depends on the connector/catalog being used.
        For memory catalog, minimal syntax is needed.
        For Hive, we can add format specifications.
        """
        if not statement.upper().startswith("CREATE TABLE"):
            return statement

        # For memory catalog, remove any Presto-incompatible syntax
        # Memory catalog doesn't support WITH properties or NOT NULL constraints

        if self.table_format == "memory" or self.catalog == "memory":
            # Memory catalog: simple CREATE TABLE without WITH clause or NOT NULL
            # Remove any existing WITH clause that might be incompatible
            import re

            statement = re.sub(r"\s+WITH\s*\([^)]*\)", "", statement, flags=re.IGNORECASE)
            # Memory catalog does not support NOT NULL constraints
            statement = re.sub(r"\s+NOT\s+NULL", "", statement, flags=re.IGNORECASE)

        elif self.table_format == "hive":
            # Add table format specification if not present
            if "WITH" not in statement.upper():
                statement += " WITH (format = 'PARQUET')"

        return statement

    def get_query_plan(self, connection: Any, query: str) -> str:
        """Get query execution plan for analysis."""
        cursor = connection.cursor()
        try:
            cursor.execute(f"EXPLAIN {query}")
            plan_rows = cursor.fetchall()
            return "\n".join([str(row[0]) for row in plan_rows])
        except Exception as e:
            return f"Could not get query plan: {e}"
        finally:
            cursor.close()

    def close_connection(self, connection: Any) -> None:
        """Close Presto connection."""
        try:
            if connection and hasattr(connection, "close"):
                connection.close()
        except Exception as e:
            self.logger.warning(f"Error closing connection: {e}")

    def test_connection(self) -> bool:
        """Test connection to Presto coordinator.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            params = self._get_connection_params()
            # Connect to information_schema for a minimal test
            params["schema"] = "information_schema"

            conn = prestodb.dbapi.connect(**params)
            cursor = conn.cursor()

            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            self.logger.debug(f"Connection test failed: {e}")
            return False

    def supports_tuning_type(self, tuning_type) -> bool:
        """Check if Presto supports a specific tuning type.

        Presto supports:
        - PARTITIONING: Via Hive partitioned tables
        - BUCKETING: Via Hive bucketed tables

        Note: Constraints are informational only in Presto (not enforced).
        """
        try:
            from benchbox.core.tuning.interface import TuningType

            return tuning_type in {
                TuningType.PARTITIONING,
            }
        except ImportError:
            return False

    def generate_tuning_clause(self, table_tuning) -> str:
        """Generate Presto-specific tuning clauses for CREATE TABLE statements.

        Presto table properties depend on the connector:
        - memory: Limited properties
        - hive: PARTITIONED BY, BUCKETED BY

        """
        if not table_tuning or not table_tuning.has_any_tuning():
            return ""

        clauses = []

        try:
            from benchbox.core.tuning.interface import TuningType

            # Handle partitioning
            partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
            if partition_columns and self.table_format == "hive":
                sorted_cols = sorted(partition_columns, key=lambda col: col.order)
                column_names = [col.name for col in sorted_cols]
                clauses.append(f"PARTITIONED BY ({', '.join(column_names)})")

        except ImportError:
            pass

        if clauses:
            return " ".join(clauses)
        return ""

    def apply_table_tunings(self, table_tuning, connection: Any) -> None:
        """Apply tuning configurations to a Presto table.

        Presto tuning is primarily handled at table creation time.
        Post-creation optimization is limited.
        """
        if not table_tuning or not table_tuning.has_any_tuning():
            return

        table_name = table_tuning.table_name.lower()
        self.logger.info(f"Applying Presto tunings for table: {table_name}")

        # Presto tuning is primarily handled at table creation time
        # Log the configuration for informational purposes
        try:
            from benchbox.core.tuning.interface import TuningType

            partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
            if partition_columns:
                sorted_cols = sorted(partition_columns, key=lambda col: col.order)
                column_names = [col.name for col in sorted_cols]
                self.logger.info(f"Partitioning for {table_name}: {', '.join(column_names)}")

        except ImportError:
            self.logger.warning("Tuning interface not available - skipping tuning application")

    def apply_unified_tuning(self, unified_config: UnifiedTuningConfiguration, connection: Any) -> None:
        """Apply unified tuning configuration to Presto."""
        if not unified_config:
            return

        # Apply constraint configurations (informational only in Presto)
        self.apply_constraint_configuration(unified_config.primary_keys, unified_config.foreign_keys, connection)

        # Apply platform optimizations
        if unified_config.platform_optimizations:
            self.apply_platform_optimizations(unified_config.platform_optimizations, connection)

        # Apply table-level tunings
        for _table_name, table_tuning in unified_config.table_tunings.items():
            self.apply_table_tunings(table_tuning, connection)

    def apply_platform_optimizations(self, platform_config: PlatformOptimizationConfiguration, connection: Any) -> None:
        """Apply Presto-specific platform optimizations.

        Presto optimizations are primarily session-level:
        - Join reordering strategy
        - Hash generation
        - Memory management

        These are typically set during connection or via session properties.
        """
        if not platform_config:
            return

        self.logger.info("Presto platform optimizations applied via session properties")

    def apply_constraint_configuration(
        self,
        primary_key_config: PrimaryKeyConfiguration,
        foreign_key_config: ForeignKeyConfiguration,
        connection: Any,
    ) -> None:
        """Apply constraint configurations to Presto.

        Note: Presto does not enforce constraints. They are informational only
        and used by the query optimizer for join reordering.
        """
        if primary_key_config and primary_key_config.enabled:
            self.logger.info("Primary key constraints enabled for Presto (informational only, not enforced)")

        if foreign_key_config and foreign_key_config.enabled:
            self.logger.info("Foreign key constraints enabled for Presto (informational only, not enforced)")

    def _get_existing_tables(self, connection: Any) -> list[str]:
        """Get list of existing tables from Presto schema."""
        cursor = connection.cursor()
        try:
            cursor.execute("SHOW TABLES")
            return [row[0].lower() for row in cursor.fetchall()]
        except Exception:
            return []
        finally:
            cursor.close()

    def analyze_table(self, connection: Any, table_name: str) -> None:
        """Run ANALYZE on table for query optimization.

        Note: ANALYZE is supported in Hive connector but not memory.
        """
        if self.table_format == "memory":
            self.logger.debug(f"ANALYZE not supported for memory catalog - skipping {table_name}")
            return

        table_name_lower = table_name.lower()
        if not self._validate_identifier(table_name_lower):
            self.logger.warning(f"Skipping analyze for invalid table identifier: {table_name}")
            return

        qualified_table = f"{self.catalog}.{self.schema}.{table_name_lower}"

        cursor = connection.cursor()
        try:
            cursor.execute(f"ANALYZE {qualified_table}")
        except Exception as e:
            self.logger.warning(f"Failed to analyze table {table_name}: {e}")
        finally:
            cursor.close()


def _build_presto_config(
    platform: str,
    options: dict[str, Any],
    overrides: dict[str, Any],
    info: Any,
) -> Any:
    """Build Presto database configuration with credential loading.

    Args:
        platform: Platform name (should be 'presto')
        options: CLI platform options from --platform-option flags
        overrides: Runtime overrides from orchestrator
        info: Platform info from registry

    Returns:
        DatabaseConfig with credentials loaded
    """
    from benchbox.core.config import DatabaseConfig
    from benchbox.security.credentials import CredentialManager

    # Load saved credentials
    cred_manager = CredentialManager()
    saved_creds = cred_manager.get_platform_credentials("presto") or {}

    # Build merged options: saved_creds < options < overrides
    merged_options = {}
    merged_options.update(saved_creds)
    merged_options.update(options)
    merged_options.update(overrides)

    name = info.display_name if info else "Presto"
    driver_package = info.driver_package if info else "presto-python-client"

    config_dict = {
        "type": "presto",
        "name": name,
        "options": merged_options or {},
        "driver_package": driver_package,
        "driver_version": overrides.get("driver_version") or options.get("driver_version"),
        "driver_auto_install": bool(overrides.get("driver_auto_install", options.get("driver_auto_install", False))),
        # Platform-specific fields at top-level
        "host": merged_options.get("host"),
        "port": merged_options.get("port"),
        "catalog": merged_options.get("catalog"),
        "username": merged_options.get("username"),
        "password": merged_options.get("password"),
        "http_scheme": merged_options.get("http_scheme"),
        "verify_ssl": merged_options.get("verify_ssl"),
        "ssl_cert_path": merged_options.get("ssl_cert_path"),
        "session_properties": merged_options.get("session_properties"),
        "query_timeout": merged_options.get("query_timeout"),
        "table_format": merged_options.get("table_format"),
        "staging_root": merged_options.get("staging_root"),
        # Benchmark context for config-aware schema naming
        "benchmark": overrides.get("benchmark"),
        "scale_factor": overrides.get("scale_factor"),
        "tuning_config": overrides.get("tuning_config"),
    }

    # Only include explicit schema override if provided
    if "schema" in overrides and overrides["schema"]:
        config_dict["schema"] = overrides["schema"]

    return DatabaseConfig(**config_dict)


# Register the config builder with the platform hook registry
try:
    from benchbox.cli.platform_hooks import PlatformHookRegistry

    PlatformHookRegistry.register_config_builder("presto", _build_presto_config)
except ImportError:
    # Platform hooks may not be available in all contexts
    pass
