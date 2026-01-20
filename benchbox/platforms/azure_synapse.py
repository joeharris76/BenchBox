"""Azure Synapse Analytics platform adapter with ADLS/Blob integration.

Provides Azure Synapse Dedicated SQL Pool-specific optimizations for cloud-native
analytics, including PolyBase/COPY INTO for data loading and distribution key
optimization.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from benchbox.core.tuning.interface import (
        ForeignKeyConfiguration,
        PlatformOptimizationConfiguration,
        PrimaryKeyConfiguration,
        UnifiedTuningConfiguration,
    )

from ..utils.dependencies import check_platform_dependencies, get_dependency_error_message
from .base import PlatformAdapter

# Azure Synapse uses T-SQL dialect (compatible with SQL Server)
SYNAPSE_DIALECT = "tsql"

try:
    import pyodbc
except ImportError:
    pyodbc = None


class AzureSynapseAdapter(PlatformAdapter):
    """Azure Synapse Analytics platform adapter with cloud data warehouse optimizations."""

    def __init__(self, **config):
        super().__init__(**config)

        # Check dependencies with improved error message
        available, missing = check_platform_dependencies("synapse")
        if not available:
            error_msg = get_dependency_error_message("synapse", missing)
            raise ImportError(error_msg)

        self._dialect = SYNAPSE_DIALECT

        # Azure Synapse connection configuration
        self.server = config.get("server")
        self.database = config.get("database") or "benchbox"
        self.username = config.get("username")
        self.password = config.get("password")
        self.port = config.get("port") if config.get("port") is not None else 1433

        # Authentication options
        # "sql" for SQL auth, "aad_password" for Azure AD, "aad_msi" for Managed Identity
        self.auth_method = config.get("auth_method") or "sql"
        self.tenant_id = config.get("tenant_id")
        self.client_id = config.get("client_id")
        self.client_secret = config.get("client_secret")

        # ODBC driver configuration
        self.driver = config.get("driver") or "ODBC Driver 18 for SQL Server"

        # Schema configuration
        self.schema = config.get("schema") or "dbo"

        # Connection settings
        self.connect_timeout = config.get("connect_timeout") if config.get("connect_timeout") is not None else 30
        self.query_timeout = config.get("query_timeout") if config.get("query_timeout") is not None else 0
        self.encrypt = config.get("encrypt") if config.get("encrypt") is not None else True

        # Azure storage configuration for data loading
        staging_root = config.get("staging_root")
        if staging_root:
            from benchbox.utils.cloud_storage import get_cloud_path_info

            path_info = get_cloud_path_info(staging_root)
            if path_info["provider"] in ("az", "abfs", "abfss"):
                self.storage_account = path_info.get("account") or self._extract_storage_account(staging_root)
                self.container = path_info["bucket"]
                self.storage_path = path_info["path"].strip("/") if path_info["path"] else "benchbox-data"
                self.logger.info(
                    f"Using staging location from config: {path_info['provider']}://{self.container}/{self.storage_path}"
                )
            else:
                raise ValueError(
                    f"Azure Synapse requires Azure storage (az://, abfs://, abfss://), got: {path_info['provider']}://"
                )
        else:
            # Fall back to explicit storage configuration
            self.storage_account = config.get("storage_account")
            self.container = config.get("container")
            self.storage_path = config.get("storage_path") or "benchbox-data"

        # Storage credentials
        self.storage_sas_token = config.get("storage_sas_token")
        self.storage_account_key = config.get("storage_account_key")
        self.storage_credential = config.get("storage_credential")  # Database scoped credential name

        # Synapse-specific settings
        self.resource_class = config.get("resource_class") or "staticrc20"
        self.distribution_default = config.get("distribution_default") or "ROUND_ROBIN"

        # Result cache control - disable by default for accurate benchmarking
        self.disable_result_cache = config.get("disable_result_cache", True)

        # Validation strictness
        self.strict_validation = config.get("strict_validation", True)

        if self.auth_method == "sql" and not all([self.server, self.username, self.password]):
            missing = []
            if not self.server:
                missing.append("server (or SYNAPSE_SERVER)")
            if not self.username:
                missing.append("username (or SYNAPSE_USER)")
            if not self.password:
                missing.append("password (or SYNAPSE_PASSWORD)")

            from benchbox.core.exceptions import ConfigurationError

            raise ConfigurationError(
                f"Azure Synapse SQL authentication is incomplete. Missing: {', '.join(missing)}\n"
                "Configure with:\n"
                "  1. Environment variables: SYNAPSE_SERVER, SYNAPSE_USER, SYNAPSE_PASSWORD\n"
                "  2. CLI options: --platform-option server=<server>.sql.azuresynapse.net\n"
                "\n"
                "For Azure AD authentication, use --platform-option auth_method=aad_msi"
            )
        elif self.auth_method in ("aad_password", "aad_msi") and not self.server:
            from benchbox.core.exceptions import ConfigurationError

            raise ConfigurationError(
                "Azure Synapse AAD authentication requires a server endpoint.\n"
                "Configure with:\n"
                "  1. Environment variable: SYNAPSE_SERVER\n"
                "  2. CLI option: --platform-option server=<workspace>.sql.azuresynapse.net"
            )

    @property
    def platform_name(self) -> str:
        return "Azure Synapse"

    def get_target_dialect(self) -> str:
        """Return the target SQL dialect for Azure Synapse (T-SQL)."""
        return SYNAPSE_DIALECT

    @staticmethod
    def add_cli_arguments(parser) -> None:
        """Add Azure Synapse-specific CLI arguments."""
        synapse_group = parser.add_argument_group("Azure Synapse Arguments")
        synapse_group.add_argument("--server", type=str, help="Azure Synapse server endpoint")
        synapse_group.add_argument("--database", type=str, default="benchbox", help="Database name")
        synapse_group.add_argument("--username", type=str, help="Database username")
        synapse_group.add_argument("--password", type=str, help="Database password")
        synapse_group.add_argument(
            "--auth-method",
            type=str,
            choices=["sql", "aad_password", "aad_msi"],
            default="sql",
            help="Authentication method",
        )
        synapse_group.add_argument("--storage-account", type=str, help="Azure storage account for data staging")
        synapse_group.add_argument("--container", type=str, help="Azure blob container name")
        synapse_group.add_argument("--storage-sas-token", type=str, help="SAS token for storage access")
        synapse_group.add_argument("--resource-class", type=str, default="staticrc20", help="Workload resource class")

    @classmethod
    def from_config(cls, config: dict[str, Any]):
        """Create Azure Synapse adapter from unified configuration."""
        from benchbox.utils.database_naming import generate_database_name

        adapter_config: dict[str, Any] = {}

        # Generate proper database name using benchmark characteristics
        if "database" in config and config["database"]:
            adapter_config["database"] = config["database"]
        else:
            database_name = generate_database_name(
                benchmark_name=config["benchmark"],
                scale_factor=config["scale_factor"],
                platform="synapse",
                tuning_config=config.get("tuning_config"),
            )
            adapter_config["database"] = database_name

        # Copy config keys
        for key in [
            "server",
            "port",
            "username",
            "password",
            "schema",
            "auth_method",
            "tenant_id",
            "client_id",
            "client_secret",
            "driver",
            "connect_timeout",
            "query_timeout",
            "encrypt",
            "storage_account",
            "container",
            "storage_path",
            "storage_sas_token",
            "storage_account_key",
            "storage_credential",
            "resource_class",
            "distribution_default",
            "disable_result_cache",
            "strict_validation",
        ]:
            if key in config:
                adapter_config[key] = config[key]

        return cls(**adapter_config)

    def _extract_storage_account(self, url: str) -> str | None:
        """Extract storage account name from Azure blob URL."""
        # Format: abfss://container@account.dfs.core.windows.net/path
        match = re.search(r"@([^.]+)\.", url)
        return match.group(1) if match else None

    def _get_connection_string(self, database: str | None = None) -> str:
        """Build ODBC connection string for Azure Synapse."""
        db = database or self.database

        if self.auth_method == "sql":
            # SQL Server authentication
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={db};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"Encrypt={'yes' if self.encrypt else 'no'};"
                f"TrustServerCertificate=no;"
                f"Connection Timeout={self.connect_timeout};"
            )
        elif self.auth_method == "aad_password":
            # Azure AD password authentication
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={db};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"Authentication=ActiveDirectoryPassword;"
                f"Encrypt={'yes' if self.encrypt else 'no'};"
                f"TrustServerCertificate=no;"
                f"Connection Timeout={self.connect_timeout};"
            )
        elif self.auth_method == "aad_msi":
            # Azure AD Managed Identity authentication
            conn_str = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={db};"
                f"Authentication=ActiveDirectoryMsi;"
                f"Encrypt={'yes' if self.encrypt else 'no'};"
                f"TrustServerCertificate=no;"
                f"Connection Timeout={self.connect_timeout};"
            )
        else:
            raise ValueError(f"Unsupported auth method: {self.auth_method}")

        return conn_str

    def _get_connection_params(self, **connection_config) -> dict[str, Any]:
        """Get standardized connection parameters."""
        return {
            "server": connection_config.get("server", self.server),
            "port": connection_config.get("port", self.port),
            "database": connection_config.get("database", self.database),
            "username": connection_config.get("username", self.username),
            "password": connection_config.get("password", self.password),
        }

    def _create_admin_connection(self, **connection_config) -> Any:
        """Create Azure Synapse connection for admin operations (using master database)."""
        conn_str = self._get_connection_string(database="master")
        connection = pyodbc.connect(conn_str, autocommit=True)
        return connection

    def check_server_database_exists(self, **connection_config) -> bool:
        """Check if database exists in Azure Synapse."""
        try:
            connection = self._create_admin_connection()
            cursor = connection.cursor()

            database = connection_config.get("database", self.database)

            cursor.execute("SELECT name FROM sys.databases WHERE name = ?", (database,))
            result = cursor.fetchone()

            cursor.close()
            connection.close()

            return result is not None

        except Exception:
            return False

    def drop_database(self, **connection_config) -> None:
        """Drop database in Azure Synapse."""
        database = connection_config.get("database", self.database)

        if not self.check_server_database_exists(database=database):
            self.log_verbose(f"Database {database} does not exist - nothing to drop")
            return

        try:
            connection = self._create_admin_connection()
            cursor = connection.cursor()

            # Kill existing connections
            cursor.execute(
                f"""
                DECLARE @kill varchar(8000) = '';
                SELECT @kill = @kill + 'KILL ' + CONVERT(varchar(5), session_id) + ';'
                FROM sys.dm_exec_sessions
                WHERE database_id = DB_ID('{database}')
                AND session_id <> @@SPID;
                EXEC(@kill);
            """
            )

            # Drop database
            cursor.execute(f"DROP DATABASE [{database}]")

            cursor.close()
            connection.close()

        except Exception as e:
            raise RuntimeError(f"Failed to drop Azure Synapse database {database}: {e}")

    def create_connection(self, **connection_config) -> Any:
        """Create optimized Azure Synapse connection."""
        self.log_operation_start("Azure Synapse connection")

        # Handle existing database using base class method
        self.handle_existing_database(**connection_config)

        params = self._get_connection_params(**connection_config)
        target_database = params.get("database")

        # Create database if needed
        if not self.database_was_reused:
            database_exists = self.check_server_database_exists(database=target_database)

            if not database_exists:
                self.log_verbose(f"Creating database: {target_database}")

                try:
                    admin_conn = self._create_admin_connection()
                    cursor = admin_conn.cursor()

                    # Create database with default service objective
                    cursor.execute(f"CREATE DATABASE [{target_database}]")
                    self.logger.info(f"Created database {target_database}")

                    cursor.close()
                    admin_conn.close()
                except Exception as e:
                    self.logger.error(f"Failed to create database {target_database}: {e}")
                    raise

        try:
            conn_str = self._get_connection_string(database=target_database)
            connection = pyodbc.connect(conn_str, autocommit=True)

            # Test connection
            cursor = connection.cursor()
            cursor.execute("SELECT @@VERSION")
            cursor.fetchone()
            cursor.close()

            self.logger.info(f"Connected to Azure Synapse at {self.server}")

            self.log_operation_complete("Azure Synapse connection", details=f"Connected to {self.server}")

            return connection

        except Exception as e:
            self.logger.error(f"Failed to connect to Azure Synapse: {e}")
            raise

    def create_schema(self, benchmark, connection: Any) -> float:
        """Create schema using Azure Synapse-optimized table definitions."""
        self.log_operation_start("Azure Synapse schema creation")
        start_time = time.time()

        cursor = connection.cursor()

        try:
            # Create schema if needed
            if self.schema and self.schema.lower() != "dbo":
                self.log_verbose(f"Creating schema: {self.schema}")
                cursor.execute(
                    f"""
                    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{self.schema}')
                    BEGIN
                        EXEC('CREATE SCHEMA [{self.schema}]')
                    END
                """
                )

            # Use common schema creation helper
            schema_sql = self._create_schema_with_tuning(benchmark, source_dialect="duckdb")

            # Split schema into individual statements and execute
            statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

            for statement in statements:
                if not statement.upper().startswith("CREATE TABLE"):
                    continue

                # Extract table name
                table_name = self._extract_table_name(statement)
                if not table_name:
                    continue

                # Drop table if exists
                cursor.execute(
                    f"""
                    IF OBJECT_ID('{self.schema}.{table_name}', 'U') IS NOT NULL
                        DROP TABLE [{self.schema}].[{table_name}]
                """
                )

                # Optimize for Azure Synapse
                optimized_statement = self._optimize_table_definition(statement)
                cursor.execute(optimized_statement)
                self.logger.debug(f"Created table: {table_name}")

            self.logger.info("Schema created")

        except Exception as e:
            self.logger.error(f"Schema creation failed: {e}")
            raise
        finally:
            cursor.close()

        elapsed_time = time.time() - start_time
        self.log_operation_complete("Azure Synapse schema creation", details=f"Completed in {elapsed_time:.2f}s")
        return elapsed_time

    def load_data(
        self, benchmark, connection: Any, data_dir: Path
    ) -> tuple[dict[str, int], float, dict[str, Any] | None]:
        """Load data using Azure Synapse COPY INTO command with blob storage."""
        self.log_operation_start("Azure Synapse data loading")
        start_time = time.time()
        table_stats = {}

        cursor = connection.cursor()

        try:
            # Get data files from benchmark or manifest
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
                except Exception as e:
                    self.logger.debug(f"Manifest fallback failed: {e}")

                if not data_files:
                    raise ValueError("No data files found. Ensure benchmark.generate_data() was called first.")

            # Check if storage is configured
            if self.storage_account and self.container:
                table_stats = self._load_data_via_blob(cursor, data_files, data_dir)
            else:
                # Fall back to bulk insert (less efficient)
                self.logger.warning("No Azure storage configured, using BULK INSERT")
                table_stats = self._load_data_direct(cursor, data_files, data_dir)

            total_time = time.time() - start_time
            total_rows = sum(table_stats.values())
            self.logger.info(f"Loaded {total_rows:,} total rows in {total_time:.2f}s")
            self.log_operation_complete(
                "Azure Synapse data loading", details=f"Loaded {total_rows:,} rows in {total_time:.2f}s"
            )

        except Exception as e:
            self.logger.error(f"Data loading failed: {e}")
            raise
        finally:
            cursor.close()

        return table_stats, time.time() - start_time, None

    def _load_data_via_blob(self, cursor: Any, data_files: dict[str, Any], data_dir: Path) -> dict[str, int]:
        """Load data via Azure Blob Storage using COPY INTO."""
        table_stats = {}

        # Set up external data source if not exists
        self._setup_external_data_source(cursor)

        # Upload files and load
        for table_name, file_paths in data_files.items():
            if not isinstance(file_paths, list):
                file_paths = [file_paths]

            valid_files = [Path(f) for f in file_paths if Path(f).exists() and Path(f).stat().st_size > 0]

            if not valid_files:
                self.logger.warning(f"Skipping {table_name} - no valid data files")
                table_stats[table_name] = 0
                continue

            try:
                load_start = time.time()

                # Upload files to blob storage
                blob_paths = self._upload_to_blob(table_name, valid_files)

                # Determine file format
                first_file = valid_files[0]
                file_str = str(first_file.name)
                if ".tbl" in file_str or ".dat" in file_str:
                    field_terminator = "|"
                else:
                    field_terminator = ","

                # Use COPY INTO for loading
                qualified_table = f"[{self.schema}].[{table_name}]"
                for blob_path in blob_paths:
                    # Build COPY command based on credential type
                    if self.storage_sas_token:
                        credential_clause = f", CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = '{self.storage_sas_token}')"
                    elif self.storage_credential:
                        credential_clause = f", CREDENTIAL = '{self.storage_credential}'"
                    else:
                        credential_clause = ""

                    copy_sql = f"""
                        COPY INTO {qualified_table}
                        FROM '{blob_path}'
                        WITH (
                            FILE_TYPE = 'CSV',
                            FIELDTERMINATOR = '{field_terminator}',
                            ROWTERMINATOR = '0x0A',
                            FIRSTROW = 1,
                            ENCODING = 'UTF8'
                            {credential_clause}
                        )
                    """

                    cursor.execute(copy_sql)

                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}")
                row_count = cursor.fetchone()[0]
                table_stats[table_name] = row_count

                load_time = time.time() - load_start
                self.logger.info(f"Loaded {row_count:,} rows into {table_name} in {load_time:.2f}s")

            except Exception as e:
                self.logger.error(f"Failed to load {table_name}: {str(e)[:100]}...")
                table_stats[table_name] = 0

        return table_stats

    def _load_data_direct(self, cursor: Any, data_files: dict[str, Any], data_dir: Path) -> dict[str, int]:
        """Load data directly via INSERT statements (fallback method)."""
        table_stats = {}

        for table_name, file_paths in data_files.items():
            if not isinstance(file_paths, list):
                file_paths = [file_paths]

            valid_files = [Path(f) for f in file_paths if Path(f).exists() and Path(f).stat().st_size > 0]

            if not valid_files:
                self.logger.warning(f"Skipping {table_name} - no valid data files")
                table_stats[table_name] = 0
                continue

            try:
                load_start = time.time()
                total_rows = 0

                qualified_table = f"[{self.schema}].[{table_name}]"

                for file_path in valid_files:
                    file_str = str(file_path.name)
                    delimiter = "|" if ".tbl" in file_str or ".dat" in file_str else ","

                    with open(file_path) as f:
                        batch_size = 1000
                        batch_data = []

                        for line in f:
                            line = line.strip()
                            if line and line.endswith(delimiter):
                                line = line[:-1]

                            values = line.split(delimiter)
                            escaped_values = ["'" + str(v).replace("'", "''") + "'" for v in values]
                            batch_data.append(f"({', '.join(escaped_values)})")

                            if len(batch_data) >= batch_size:
                                insert_sql = f"INSERT INTO {qualified_table} VALUES " + ", ".join(batch_data)
                                cursor.execute(insert_sql)
                                total_rows += len(batch_data)
                                batch_data = []

                        if batch_data:
                            insert_sql = f"INSERT INTO {qualified_table} VALUES " + ", ".join(batch_data)
                            cursor.execute(insert_sql)
                            total_rows += len(batch_data)

                table_stats[table_name] = total_rows

                load_time = time.time() - load_start
                self.logger.info(f"Loaded {total_rows:,} rows into {table_name} in {load_time:.2f}s")

            except Exception as e:
                self.logger.error(f"Failed to load {table_name}: {str(e)[:100]}...")
                table_stats[table_name] = 0

        return table_stats

    def _setup_external_data_source(self, cursor: Any) -> None:
        """Set up external data source for COPY operations."""
        # Create master key if not exists (required for credentials)
        try:
            cursor.execute(
                """
                IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
                BEGIN
                    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'BenchBox#Temp123!'
                END
            """
            )
        except Exception as e:
            self.logger.debug(f"Master key setup: {e}")

    def _upload_to_blob(self, table_name: str, files: list[Path]) -> list[str]:
        """Upload files to Azure Blob Storage and return blob URLs."""
        blob_paths = []

        try:
            from azure.storage.blob import BlobServiceClient

            if self.storage_sas_token:
                account_url = f"https://{self.storage_account}.blob.core.windows.net"
                blob_service = BlobServiceClient(account_url=account_url, credential=self.storage_sas_token)
            elif self.storage_account_key:
                connection_string = (
                    f"DefaultEndpointsProtocol=https;"
                    f"AccountName={self.storage_account};"
                    f"AccountKey={self.storage_account_key};"
                    f"EndpointSuffix=core.windows.net"
                )
                blob_service = BlobServiceClient.from_connection_string(connection_string)
            else:
                # Use default Azure credentials (managed identity)
                from azure.identity import DefaultAzureCredential

                account_url = f"https://{self.storage_account}.blob.core.windows.net"
                blob_service = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())

            container_client = blob_service.get_container_client(self.container)

            for idx, file_path in enumerate(files):
                blob_name = f"{self.storage_path}/{table_name}_{idx}{file_path.suffix}"

                with open(file_path, "rb") as data:
                    container_client.upload_blob(name=blob_name, data=data, overwrite=True)

                blob_url = f"https://{self.storage_account}.blob.core.windows.net/{self.container}/{blob_name}"
                blob_paths.append(blob_url)

                self.log_very_verbose(f"Uploaded {file_path.name} to {blob_url}")

        except ImportError:
            raise ImportError(
                "Azure Storage SDK required for blob uploads. Install with: pip install azure-storage-blob azure-identity"
            )

        return blob_paths

    def configure_for_benchmark(self, connection: Any, benchmark_type: str) -> None:
        """Apply Azure Synapse-specific optimizations based on benchmark type."""
        cursor = connection.cursor()

        try:
            # Disable result cache for accurate benchmarking
            if self.disable_result_cache:
                cursor.execute("SET RESULT_SET_CACHING OFF")
                self.logger.debug("Disabled result set caching")

            # Set resource class for workload management
            if self.resource_class:
                try:
                    cursor.execute(f"EXEC sp_addrolemember '{self.resource_class}', '{self.username}'")
                except Exception:
                    # Role assignment may fail if already assigned or not available
                    pass

            if benchmark_type.lower() in ["olap", "analytics", "tpch", "tpcds"]:
                # OLAP-specific settings
                cursor.execute("SET ANSI_NULLS ON")
                cursor.execute("SET QUOTED_IDENTIFIER ON")

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
        self.log_operation_start("Azure Synapse query execution", query_id)
        start_time = time.time()

        cursor = connection.cursor()

        try:
            cursor.execute(query)
            result = cursor.fetchall()

            execution_time = time.time() - start_time
            actual_row_count = len(result) if result else 0

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

            # Build result using base helper
            result_dict = self._build_query_result_with_validation(
                query_id=query_id,
                execution_time=execution_time,
                actual_row_count=actual_row_count,
                first_row=result[0] if result else None,
                validation_result=validation_result,
            )

            self.log_verbose(f"Query {query_id} completed: {actual_row_count} rows in {execution_time:.3f}s")
            self.log_operation_complete("Azure Synapse query execution", query_id, f"returned {actual_row_count} rows")

            return result_dict

        except Exception as e:
            execution_time = time.time() - start_time
            self.log_verbose(f"Query {query_id} failed after {execution_time:.3f}s: {e}")

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

    def _extract_table_name(self, statement: str) -> str | None:
        """Extract table name from CREATE TABLE statement."""
        match = re.search(r"CREATE\s+TABLE\s+\[?([^\s\[\](]+)\]?", statement, re.IGNORECASE)
        return match.group(1).strip() if match else None

    def _optimize_table_definition(self, statement: str) -> str:
        """Optimize table definition for Azure Synapse."""
        if not statement.upper().startswith("CREATE TABLE"):
            return statement

        # Add schema prefix if not present
        table_name = self._extract_table_name(statement)
        if table_name and f"[{self.schema}]" not in statement:
            statement = re.sub(
                rf"CREATE\s+TABLE\s+\[?{re.escape(table_name)}\]?",
                f"CREATE TABLE [{self.schema}].[{table_name}]",
                statement,
                flags=re.IGNORECASE,
            )

        # Add distribution if not present
        if "WITH" not in statement.upper() or "DISTRIBUTION" not in statement.upper():
            # Default to ROUND_ROBIN distribution
            if statement.rstrip().endswith(")"):
                statement = statement.rstrip()[:-1] + f") WITH (DISTRIBUTION = {self.distribution_default})"
            else:
                statement += f" WITH (DISTRIBUTION = {self.distribution_default})"

        return statement

    def _get_existing_tables(self, connection: Any) -> list[str]:
        """Get list of existing tables from Azure Synapse."""
        cursor = connection.cursor()
        try:
            cursor.execute(
                f"""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{self.schema}'
                AND TABLE_TYPE = 'BASE TABLE'
            """
            )
            return [row[0].lower() for row in cursor.fetchall()]
        finally:
            cursor.close()

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get Azure Synapse platform information."""
        platform_info = {
            "platform_type": "azure_synapse",
            "platform_name": "Azure Synapse",
            "connection_mode": "remote",
            "cloud_provider": "Azure",
            "server": self.server,
            "configuration": {
                "database": self.database,
                "schema": self.schema,
                "resource_class": self.resource_class,
                "distribution_default": self.distribution_default,
                "result_cache_enabled": not self.disable_result_cache,
            },
            "dialect": SYNAPSE_DIALECT,
        }

        if connection:
            cursor = None
            try:
                cursor = connection.cursor()

                # Get version
                cursor.execute("SELECT @@VERSION")
                result = cursor.fetchone()
                platform_info["platform_version"] = result[0] if result else None

                # Get database size
                try:
                    cursor.execute(
                        f"""
                        SELECT
                            SUM(reserved_page_count) * 8.0 / 1024 AS size_mb
                        FROM sys.dm_pdw_nodes_db_partition_stats
                        WHERE DB_NAME(database_id) = '{self.database}'
                    """
                    )
                    result = cursor.fetchone()
                    if result:
                        platform_info["database_size_mb"] = result[0]
                except Exception:
                    pass

            except Exception as e:
                self.logger.debug(f"Error collecting platform info: {e}")
            finally:
                if cursor:
                    cursor.close()

        return platform_info

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

    def analyze_table(self, connection: Any, table_name: str) -> None:
        """Update statistics for query optimization."""
        cursor = connection.cursor()
        try:
            cursor.execute(f"UPDATE STATISTICS [{self.schema}].[{table_name}]")
            self.logger.info(f"Updated statistics for {table_name}")
        except Exception as e:
            self.logger.warning(f"Failed to update statistics for {table_name}: {e}")
        finally:
            cursor.close()

    def close_connection(self, connection: Any) -> None:
        """Close Azure Synapse connection."""
        try:
            if connection and hasattr(connection, "close"):
                connection.close()
        except Exception as e:
            self.logger.warning(f"Error closing connection: {e}")

    def test_connection(self) -> bool:
        """Test if connection can be established."""
        try:
            conn_str = self._get_connection_string()
            connection = pyodbc.connect(conn_str, autocommit=True)
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            connection.close()
            return True
        except Exception:
            return False

    def supports_tuning_type(self, tuning_type) -> bool:
        """Check if Azure Synapse supports a specific tuning type."""
        try:
            from benchbox.core.tuning.interface import TuningType

            return tuning_type in {
                TuningType.DISTRIBUTION,
                TuningType.PARTITIONING,
                TuningType.INDEXING,
            }
        except ImportError:
            return False

    def generate_tuning_clause(self, table_tuning) -> str:
        """Generate Azure Synapse-specific tuning clauses for CREATE TABLE statements."""
        if not table_tuning or not table_tuning.has_any_tuning():
            return ""

        clauses = []

        try:
            from benchbox.core.tuning.interface import TuningType

            # Handle distribution
            distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
            if distribution_columns:
                sorted_cols = sorted(distribution_columns, key=lambda col: col.order)
                dist_col = sorted_cols[0]
                clauses.append(f"DISTRIBUTION = HASH([{dist_col.name}])")
            else:
                clauses.append(f"DISTRIBUTION = {self.distribution_default}")

            # Handle partitioning
            partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
            if partition_columns:
                sorted_cols = sorted(partition_columns, key=lambda col: col.order)
                part_col = sorted_cols[0]
                clauses.append(f"PARTITION ([{part_col.name}] RANGE RIGHT FOR VALUES ())")

            # Clustered columnstore index is default for Synapse
            clauses.append("CLUSTERED COLUMNSTORE INDEX")

        except ImportError:
            pass

        return f"WITH ({', '.join(clauses)})" if clauses else ""

    def apply_table_tunings(self, table_tuning, connection: Any) -> None:
        """Apply tuning configurations to an Azure Synapse table."""
        if not table_tuning or not table_tuning.has_any_tuning():
            return

        table_name = table_tuning.table_name
        self.logger.info(f"Azure Synapse tunings for {table_name} applied during table creation")

        # Update statistics after tuning
        self.analyze_table(connection, table_name)

    def apply_unified_tuning(self, unified_config: UnifiedTuningConfiguration, connection: Any) -> None:
        """Apply unified tuning configuration to Azure Synapse."""
        if not unified_config:
            return

        self.apply_constraint_configuration(unified_config.primary_keys, unified_config.foreign_keys, connection)

        if unified_config.platform_optimizations:
            self.apply_platform_optimizations(unified_config.platform_optimizations, connection)

        for _table_name, table_tuning in unified_config.table_tunings.items():
            self.apply_table_tunings(table_tuning, connection)

    def apply_platform_optimizations(self, platform_config: PlatformOptimizationConfiguration, connection: Any) -> None:
        """Apply Azure Synapse-specific platform optimizations."""
        if not platform_config:
            return

        self.logger.info("Azure Synapse platform optimizations applied")

    def apply_constraint_configuration(
        self,
        primary_key_config: PrimaryKeyConfiguration,
        foreign_key_config: ForeignKeyConfiguration,
        connection: Any,
    ) -> None:
        """Apply constraint configurations to Azure Synapse.

        Note: Azure Synapse supports PRIMARY KEY and FOREIGN KEY for query optimization,
        but constraints are not enforced.
        """
        if primary_key_config and primary_key_config.enabled:
            self.logger.info("Primary key constraints enabled for Azure Synapse (informational only)")

        if foreign_key_config and foreign_key_config.enabled:
            self.logger.info("Foreign key constraints enabled for Azure Synapse (informational only)")


def _build_synapse_config(
    platform: str,
    options: dict[str, Any],
    overrides: dict[str, Any],
    info: Any,
) -> Any:
    """Build Azure Synapse database configuration with credential loading."""
    from benchbox.core.config import DatabaseConfig
    from benchbox.security.credentials import CredentialManager

    cred_manager = CredentialManager()
    saved_creds = cred_manager.get_platform_credentials("synapse") or {}

    merged_options = {}
    merged_options.update(saved_creds)
    merged_options.update(options)
    merged_options.update(overrides)

    name = info.display_name if info else "Azure Synapse"
    driver_package = info.driver_package if info else "pyodbc"

    config_dict = {
        "type": "synapse",
        "name": name,
        "options": merged_options or {},
        "driver_package": driver_package,
        "driver_version": overrides.get("driver_version") or options.get("driver_version"),
        "driver_auto_install": bool(overrides.get("driver_auto_install", options.get("driver_auto_install", False))),
        "server": merged_options.get("server"),
        "port": merged_options.get("port"),
        "username": merged_options.get("username"),
        "password": merged_options.get("password"),
        "schema": merged_options.get("schema"),
        "auth_method": merged_options.get("auth_method"),
        "storage_account": merged_options.get("storage_account"),
        "container": merged_options.get("container"),
        "storage_sas_token": merged_options.get("storage_sas_token"),
        "benchmark": overrides.get("benchmark"),
        "scale_factor": overrides.get("scale_factor"),
        "tuning_config": overrides.get("tuning_config"),
    }

    if "database" in overrides and overrides["database"]:
        config_dict["database"] = overrides["database"]

    return DatabaseConfig(**config_dict)


# Register the config builder with the platform hook registry
try:
    from benchbox.cli.platform_hooks import PlatformHookRegistry

    PlatformHookRegistry.register_config_builder("synapse", _build_synapse_config)
except ImportError:
    pass
