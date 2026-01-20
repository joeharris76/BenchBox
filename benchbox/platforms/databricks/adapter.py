"""Databricks platform adapter with Unity Catalog and Delta Lake optimization.

Provides Databricks-specific optimizations for large-scale analytics,
including Delta Lake table creation and cluster management.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import contextlib
import json
import logging
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

from benchbox.core.upload_validation import UploadValidationEngine
from benchbox.platforms.base import PlatformAdapter
from benchbox.utils.datagen_manifest import MANIFEST_FILENAME
from benchbox.utils.dependencies import check_platform_dependencies, get_dependency_error_message

try:
    from databricks import sql as databricks_sql
    from databricks.sql.client import Connection as DatabricksConnection
except ImportError:
    databricks_sql = None
    DatabricksConnection = None


class DatabricksAdapter(PlatformAdapter):
    """Databricks platform adapter with Delta Lake and Unity Catalog support."""

    def __init__(self, **config):
        super().__init__(**config)

        # Check dependencies with improved error message
        available, missing = check_platform_dependencies("databricks")
        if not available:
            error_msg = get_dependency_error_message("databricks", missing)
            raise ImportError(error_msg)

        self._dialect = "databricks"

        # Databricks configuration
        self.server_hostname = config.get("server_hostname") or config.get("host")
        self.http_path = config.get("http_path")
        self.access_token = config.get("access_token") or config.get("token")
        self.catalog = config.get("catalog") or "main"
        self.schema = config.get("schema") or "benchbox"
        # Unity Catalog Volume and staging support
        self.uc_catalog = config.get("uc_catalog")
        self.uc_schema = config.get("uc_schema")
        self.uc_volume = config.get("uc_volume")
        # Explicit staging root (e.g., dbfs:/Volumes/<cat>/<schema>/<volume>/... or s3://...)
        self.staging_root = config.get("staging_root")

        # Delta Lake settings
        self.enable_delta_optimization = (
            config.get("enable_delta_optimization") if config.get("enable_delta_optimization") is not None else True
        )
        self.delta_auto_optimize = (
            config.get("delta_auto_optimize") if config.get("delta_auto_optimize") is not None else True
        )
        self.delta_auto_compact = (
            config.get("delta_auto_compact") if config.get("delta_auto_compact") is not None else True
        )

        # Cluster settings
        self.cluster_size = config.get("cluster_size") or "Medium"
        self.auto_terminate_minutes = (
            config.get("auto_terminate_minutes") if config.get("auto_terminate_minutes") is not None else 30
        )

        # Schema creation settings
        self.create_catalog = config.get("create_catalog") if config.get("create_catalog") is not None else False

        # Upload/validation controls
        force_upload_val = config.get("force_upload")
        self.force_upload = bool(force_upload_val if force_upload_val is not None else False)

        # Result cache control - disable by default for accurate benchmarking
        self.disable_result_cache = config.get("disable_result_cache", True)

        if not self.server_hostname or not self.http_path or not self.access_token:
            missing = []
            if not self.server_hostname:
                missing.append("server_hostname (or DATABRICKS_HOST)")
            if not self.http_path:
                missing.append("http_path (or DATABRICKS_HTTP_PATH)")
            if not self.access_token:
                missing.append("access_token (or DATABRICKS_TOKEN)")

            from benchbox.core.exceptions import ConfigurationError

            raise ConfigurationError(
                f"Databricks configuration is incomplete. Missing: {', '.join(missing)}\n"
                "Configure with one of:\n"
                "  1. CLI: benchbox platforms setup --platform databricks\n"
                "  2. Environment variables: DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN\n"
                "  3. CLI options: --platform-option server_hostname=<host> --platform-option http_path=<path>"
            )

    @property
    def platform_name(self) -> str:
        return "Databricks"

    @staticmethod
    def add_cli_arguments(parser) -> None:
        """Add Databricks-specific CLI arguments."""
        db_group = parser.add_argument_group("Databricks Arguments")
        db_group.add_argument("--server-hostname", type=str, help="Databricks server hostname")
        db_group.add_argument("--http-path", type=str, help="Databricks SQL Warehouse HTTP path")
        db_group.add_argument("--access-token", type=str, help="Databricks access token")
        db_group.add_argument("--catalog", type=str, default="workspace", help="Databricks catalog name")
        db_group.add_argument(
            "--schema", type=str, default=None, help="Databricks schema name (auto-generated if not specified)"
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]):
        """Create Databricks adapter from unified configuration."""
        from benchbox.utils.database_naming import generate_database_name

        # Try auto-detection if credentials not provided
        adapter_config = {}
        very_verbose = config.get("very_verbose", False)

        # Check if we have valid (non-placeholder) credentials
        def is_placeholder(value):
            if not value:
                return True
            str_val = str(value)
            # Common placeholder patterns
            return (
                "your-workspace" in str_val
                or "your-warehouse-id" in str_val
                or "${" in str_val  # Environment variable placeholder
                or "example" in str_val.lower()
            )

        if not all(
            [
                config.get("server_hostname") and not is_placeholder(config.get("server_hostname")),
                config.get("http_path") and not is_placeholder(config.get("http_path")),
                config.get("access_token") and not is_placeholder(config.get("access_token")),
            ]
        ):
            auto_config = cls._auto_detect_databricks_config(very_verbose=very_verbose)
            if auto_config:
                adapter_config.update(auto_config)

        # Override with explicit config values (but skip placeholders)
        for key in ["server_hostname", "http_path", "access_token"]:
            if config.get(key) and not is_placeholder(config.get(key)):
                adapter_config[key] = config[key]

        # Handle catalog
        adapter_config["catalog"] = config.get("catalog", "workspace")

        # Handle schema - prioritize auto-generation when benchmark context is available
        # This ensures schema names reflect benchmark/scale/tuning configuration,
        # rather than using static values from credentials files
        provided_schema = config.get("schema")
        has_benchmark_context = "benchmark" in config and "scale_factor" in config

        if has_benchmark_context:
            # When running a benchmark, always auto-generate schema name unless
            # user provided an explicit non-default override
            is_default_schema = provided_schema in (None, "", "benchbox")

            if is_default_schema:
                # Generate proper schema name using benchmark configuration
                schema_name = generate_database_name(
                    benchmark_name=config["benchmark"],
                    scale_factor=config["scale_factor"],
                    platform="databricks",
                    tuning_config=config.get("tuning_config"),
                )
                adapter_config["schema"] = schema_name
            else:
                # User provided explicit non-default schema - honor it
                adapter_config["schema"] = provided_schema
        else:
            # No benchmark context - fall back to provided schema or default
            adapter_config["schema"] = provided_schema or "benchbox"

        # Pass through other relevant config
        for key in [
            "tuning_config",
            "verbose_enabled",
            "very_verbose",
            "uc_catalog",
            "uc_schema",
            "uc_volume",
            "staging_root",
        ]:
            if key in config:
                adapter_config[key] = config[key]

        return cls(**adapter_config)

    @staticmethod
    def _auto_detect_databricks_config(very_verbose: bool = False):
        """Auto-detect Databricks configuration from SDK."""
        logger = logging.getLogger("DatabricksAdapter")
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.sql import WarehousesAPI

            if very_verbose:
                logger.info("Attempting to auto-detect Databricks configuration from SDK...")

            workspace = WorkspaceClient()
            server_hostname = workspace.config.host.replace("https://", "")
            access_token = workspace.config.token

            if very_verbose:
                logger.info(f"Found Databricks host: {server_hostname}")

            warehouses = list(WarehousesAPI(workspace.api_client).list())
            if very_verbose:
                logger.info(f"Found {len(warehouses)} Databricks SQL Warehouses.")
                for wh in warehouses:
                    logger.info(f"  - Warehouse: {wh.name}, State: {wh.state}, ID: {wh.id}")

            http_path = None
            selected_warehouse = None

            if warehouses:
                # 1. Prefer a running warehouse
                running_wh = next((wh for wh in warehouses if str(wh.state) == "RUNNING"), None)
                if running_wh:
                    selected_warehouse = running_wh
                    if very_verbose:
                        logger.info(f"Selected running warehouse: {selected_warehouse.name}")
                else:
                    if very_verbose:
                        logger.info("No running warehouses found. Looking for an available one to auto-start.")
                    # 2. Otherwise, take the first available one that is not in a terminal state
                    available_wh = next(
                        (wh for wh in warehouses if str(wh.state) not in ["DELETING", "DELETED"]),
                        None,
                    )
                    if available_wh:
                        selected_warehouse = available_wh
                        if very_verbose:
                            logger.info(
                                f"Selected available warehouse to auto-start: {selected_warehouse.name} (State: {selected_warehouse.state})"
                            )

            if selected_warehouse:
                http_path = f"/sql/1.0/warehouses/{selected_warehouse.id}"
                if very_verbose:
                    logger.info(f"Using HTTP path: {http_path}")
            elif very_verbose:
                logger.warning("No suitable warehouse found for auto-detection.")

            return {
                "server_hostname": server_hostname,
                "http_path": http_path,
                "access_token": access_token,
            }
        except Exception as e:
            if very_verbose:
                logger.error(f"Databricks auto-detection failed: {e}")
            return None

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get Databricks platform information.

        Captures comprehensive Databricks configuration including:
        - Runtime/Spark version
        - Warehouse/cluster size and configuration
        - Compute tier and pricing information (best effort)
        - Photon acceleration status
        - Auto-scaling configuration

        Gracefully degrades if SDK is unavailable or permissions are insufficient.
        """
        platform_info = {
            "platform_type": "databricks",
            "platform_name": "Databricks",
            "connection_mode": "remote",
            "host": self.server_hostname,
            "configuration": {
                "catalog": self.catalog,
                "schema": self.schema,
                "http_path": self.http_path,
                "enable_delta_optimization": self.enable_delta_optimization,
                "delta_auto_optimize": self.delta_auto_optimize,
                "delta_auto_compact": self.delta_auto_compact,
                "cluster_mode": getattr(self, "cluster_mode", None),
                "spark_version": getattr(self, "spark_version", None),
                "result_cache_enabled": not self.disable_result_cache,
            },
        }

        # Get client library version
        try:
            import databricks.sql

            platform_info["client_library_version"] = getattr(databricks.sql, "__version__", None)
        except (ImportError, AttributeError):
            platform_info["client_library_version"] = None

        # Try to get Databricks runtime version from connection
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT version()")
                result = cursor.fetchone()
                if result:
                    platform_info["platform_version"] = result[0]
                else:
                    # Try alternative query for Spark version
                    cursor.execute("SELECT spark_version() as version")
                    result = cursor.fetchone()
                    platform_info["platform_version"] = result[0] if result else None
                cursor.close()
            except Exception as e:
                self.logger.debug(f"Could not query Databricks runtime version: {e}")
                platform_info["platform_version"] = None
        else:
            platform_info["platform_version"] = None

        # Try to get warehouse metadata using Databricks SDK (best effort)
        try:
            from databricks.sdk import WorkspaceClient

            # Extract warehouse ID from http_path (format: /sql/1.0/warehouses/{warehouse_id})
            warehouse_id = None
            if self.http_path and "/warehouses/" in self.http_path:
                warehouse_id = self.http_path.split("/warehouses/")[-1].strip("/")

            if warehouse_id:
                # Create workspace client
                workspace = WorkspaceClient(host=f"https://{self.server_hostname}", token=self.access_token)

                # Get warehouse configuration
                warehouse = workspace.warehouses.get(warehouse_id)

                # Detect if this is a serverless warehouse
                # Serverless warehouses have warehouse_type=PRO + enable_serverless_compute=True
                is_serverless = (
                    hasattr(warehouse, "warehouse_type")
                    and hasattr(warehouse, "enable_serverless_compute")
                    and warehouse.warehouse_type
                    and warehouse.warehouse_type.value == "PRO"
                    and warehouse.enable_serverless_compute is True
                )

                # Get raw warehouse type and override to SERVERLESS if detected
                raw_warehouse_type = (
                    warehouse.warehouse_type.value
                    if hasattr(warehouse, "warehouse_type") and warehouse.warehouse_type
                    else None
                )
                warehouse_type_display = "SERVERLESS" if is_serverless else raw_warehouse_type

                # Extract channel name and version from channel object
                channel_name = None
                warehouse_version = None
                if hasattr(warehouse, "channel") and warehouse.channel:
                    if hasattr(warehouse.channel, "name") and warehouse.channel.name:
                        channel_name = warehouse.channel.name.value
                    if hasattr(warehouse.channel, "dbsql_version"):
                        warehouse_version = warehouse.channel.dbsql_version

                    # Log if extraction fails to help debugging
                    if channel_name is None:
                        self.logger.debug(f"Channel name extraction failed for warehouse {warehouse_id}")
                    if warehouse_version is None:
                        self.logger.debug(f"Warehouse version extraction failed for warehouse {warehouse_id}")

                platform_info["compute_configuration"] = {
                    "warehouse_id": warehouse.id,
                    "warehouse_name": warehouse.name if hasattr(warehouse, "name") else None,
                    "warehouse_size": warehouse.cluster_size if hasattr(warehouse, "cluster_size") else None,
                    "warehouse_type": warehouse_type_display,
                    "auto_stop_mins": warehouse.auto_stop_mins if hasattr(warehouse, "auto_stop_mins") else None,
                    "min_num_clusters": warehouse.min_num_clusters if hasattr(warehouse, "min_num_clusters") else None,
                    "max_num_clusters": warehouse.max_num_clusters if hasattr(warehouse, "max_num_clusters") else None,
                    "enable_photon": warehouse.enable_photon if hasattr(warehouse, "enable_photon") else None,
                    "enable_serverless_compute": warehouse.enable_serverless_compute
                    if hasattr(warehouse, "enable_serverless_compute")
                    else None,
                    "spot_instance_policy": warehouse.spot_instance_policy.value
                    if hasattr(warehouse, "spot_instance_policy") and warehouse.spot_instance_policy
                    else None,
                    "channel": channel_name,
                    "warehouse_version": warehouse_version,
                    "state": warehouse.state.value if hasattr(warehouse, "state") else None,
                }

                self.logger.debug(f"Successfully captured Databricks warehouse metadata for {warehouse_id}")

        except ImportError:
            self.logger.debug("databricks-sdk not installed, skipping warehouse metadata collection")
        except Exception as e:
            self.logger.debug(
                f"Could not fetch Databricks warehouse metadata (insufficient permissions or API error): {e}"
            )

        return platform_info

    def get_target_dialect(self) -> str:
        """Return the target SQL dialect for Databricks."""
        return "databricks"

    def _get_connection_params(self, **connection_config) -> dict[str, Any]:
        """Get standardized connection parameters."""
        return {
            "server_hostname": connection_config.get("server_hostname", self.server_hostname),
            "http_path": connection_config.get("http_path", self.http_path),
            "access_token": connection_config.get("access_token", self.access_token),
        }

    def _create_admin_connection(self, **connection_config) -> Any:
        """Create Databricks connection for admin operations."""
        params = self._get_connection_params(**connection_config)

        # Basic connection without session configuration to work with all warehouse types
        return databricks_sql.connect(**params, user_agent_entry="BenchBox/1.0")

    def check_server_database_exists(self, **connection_config) -> bool:
        """Check if schema exists in Databricks catalog."""
        try:
            connection = self._create_admin_connection(**connection_config)
            cursor = connection.cursor()

            catalog = connection_config.get("catalog", self.catalog)
            schema = connection_config.get("schema", self.schema)

            # Check if catalog exists
            cursor.execute("SHOW CATALOGS")
            catalogs = [row[0] for row in cursor.fetchall()]

            if catalog not in catalogs:
                return False

            # Check if schema exists in catalog
            cursor.execute(f"SHOW SCHEMAS IN {catalog}")
            schemas = [row[0] for row in cursor.fetchall()]

            return schema in schemas

        except Exception:
            # If we can't connect or check, assume schema doesn't exist
            return False
        finally:
            if "connection" in locals():
                connection.close()

    def drop_database(self, **connection_config) -> None:
        """Drop schema in Databricks catalog."""
        try:
            connection = self._create_admin_connection(**connection_config)
            cursor = connection.cursor()

            catalog = connection_config.get("catalog", self.catalog)
            schema = connection_config.get("schema", self.schema)

            # Drop schema and all its tables
            cursor.execute(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")

        except Exception as e:
            raise RuntimeError(f"Failed to drop Databricks schema {catalog}.{schema}: {e}")
        finally:
            if "connection" in locals():
                connection.close()

    def create_connection(self, **connection_config) -> Any:
        """Create optimized Databricks SQL connection."""
        self.log_operation_start("Databricks connection")

        # Handle existing database using base class method
        self.handle_existing_database(**connection_config)

        try:
            params = self._get_connection_params(**connection_config)
            self.log_very_verbose(
                f"Databricks connection params: host={params.get('server_hostname')}, catalog={self.catalog}"
            )

            connection = self._create_admin_connection(**connection_config)

            # Test connection and set catalog
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchall()
            self.log_very_verbose("Databricks connection test successful")

            # Set catalog and schema context
            # If database is being reused, schema already exists - set it now
            # If database is new, schema will be created in create_schema() which will also set it
            cursor.execute(f"USE CATALOG {self.catalog}")
            if self.database_was_reused:
                cursor.execute(f"USE SCHEMA {self.schema}")
                self.log_very_verbose(f"Set schema context to {self.catalog}.{self.schema} (database reused)")
            else:
                self.log_very_verbose(f"Set catalog to {self.catalog}, schema will be set during schema creation")

            self.log_operation_complete(
                "Databricks connection",
                details=f"Connected to {params['server_hostname']}, catalog: {self.catalog}",
            )

            return connection

        except Exception as e:
            self.logger.error(f"Failed to connect to Databricks: {e}")
            raise

    def create_schema(self, benchmark, connection: Any) -> float:
        """Create schema using Databricks Delta Lake tables."""
        start_time = time.time()
        self.log_operation_start("Schema creation", f"benchmark: {benchmark.__class__.__name__}")

        # Get constraint settings from tuning configuration
        enable_primary_keys, enable_foreign_keys = self._get_constraint_configuration()
        self._log_constraint_configuration(enable_primary_keys, enable_foreign_keys)
        self.log_verbose(
            f"Schema constraints - Primary keys: {enable_primary_keys}, Foreign keys: {enable_foreign_keys}"
        )

        try:
            cursor = connection.cursor()

            # Step 1: Ensure catalog exists (if create_catalog is enabled)
            # Step 2: Create schema BEFORE attempting to USE it (correct order)
            if self.create_catalog:
                cursor.execute(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
                self.log_verbose(f"Created catalog and schema: {self.catalog}.{self.schema}")
            else:
                # Just create schema if catalog already exists
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
                self.log_verbose(f"Created schema: {self.catalog}.{self.schema}")

            # Step 3: Set catalog and schema context (now that schema exists)
            cursor.execute(f"USE CATALOG {self.catalog}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            self.log_very_verbose(f"Set schema context to: {self.catalog}.{self.schema}")

            # Use common schema creation helper
            schema_sql = self._create_schema_with_tuning(benchmark, source_dialect="duckdb")

            # Debug: Log schema SQL generation results
            self.log_verbose(f"Received schema SQL from _create_schema_with_tuning: {len(schema_sql)} characters")
            self.log_very_verbose(f"Schema SQL (first 300 chars): {schema_sql[:300]}")

            if not schema_sql or not schema_sql.strip():
                self.logger.error(f"Schema SQL is empty! Benchmark class: {benchmark.__class__.__name__}")
                self.logger.error(f"Benchmark has get_schema_sql: {hasattr(benchmark, 'get_schema_sql')}")
                raise RuntimeError(f"No schema SQL generated for {benchmark.__class__.__name__}")

            # Transform SQL syntax for Databricks compatibility
            original_len = len(schema_sql)
            schema_sql = self._fix_databricks_sql_syntax(schema_sql)
            self.log_very_verbose(
                f"After _fix_databricks_sql_syntax: {len(schema_sql)} characters (was {original_len})"
            )
            if len(schema_sql) != original_len:
                self.log_verbose(f"SQL length changed after Databricks syntax fix: {original_len} -> {len(schema_sql)}")

            # Split schema into individual statements and execute
            statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

            # Debug: Log statement count
            self.log_verbose(f"Parsed {len(statements)} CREATE TABLE statements from schema SQL")
            if not statements:
                self.logger.error("No CREATE TABLE statements found after parsing schema SQL")
                self.logger.error(f"Raw schema SQL (first 500 chars): {schema_sql[:500]}")
                raise RuntimeError("Schema SQL produced no executable statements")

            # Execute statements with error handling from base adapter
            tables_created, failed_tables = self._execute_schema_statements(
                statements, cursor, platform_transform_fn=self._convert_to_delta_table
            )

            duration = time.time() - start_time
            self.log_operation_complete("Schema creation", duration, f"{tables_created} Delta Lake tables created")

            return duration

        except Exception as e:
            self.logger.error(f"Schema creation failed: {e}")
            raise
        finally:
            if "cursor" in locals():
                cursor.close()

        return time.time() - start_time

    def _ensure_uc_volume_exists(self, uc_volume_path: str, connection: Any) -> None:
        """Ensure UC Volume exists, creating it if necessary.

        This method also creates the schema if it doesn't exist, providing
        a complete zero-setup experience for UC Volume workflows.

        Args:
            uc_volume_path: UC Volume path (e.g., dbfs:/Volumes/catalog/schema/volume)
            connection: Databricks SQL connection

        Raises:
            ValueError: If volume path is invalid or creation fails
        """
        # Parse volume path: dbfs:/Volumes/catalog/schema/volume
        volume_path = uc_volume_path.replace("dbfs:", "").rstrip("/")

        # Extract catalog, schema, volume from /Volumes/catalog/schema/volume
        if not volume_path.startswith("/Volumes/"):
            raise ValueError(f"Invalid UC Volume path: {uc_volume_path}. Must start with dbfs:/Volumes/")

        path_parts = volume_path.split("/")
        # path_parts = ['', 'Volumes', 'catalog', 'schema', 'volume', ...]

        if len(path_parts) < 5:
            # Be lenient in unit-test or minimal paths: skip ensure when volume parts are incomplete
            self.logger.warning(
                f"UC Volume path '{uc_volume_path}' missing components (expected dbfs:/Volumes/catalog/schema/volume). Skipping ensure."
            )
            return

        catalog = path_parts[2]
        schema = path_parts[3]
        volume = path_parts[4]

        self.log_verbose(f"Ensuring UC Volume exists: {catalog}.{schema}.{volume}")

        try:
            cursor = connection.cursor()

            # First, ensure the schema exists (required for volume creation)
            try:
                create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
                cursor.execute(create_schema_sql)
                self.log_very_verbose(f"Schema ready: {catalog}.{schema}")
            except Exception as schema_error:
                # If schema creation fails due to permissions, provide clear guidance
                error_msg = str(schema_error).lower()
                if "permission" in error_msg or "access denied" in error_msg or "unauthorized" in error_msg:
                    raise ValueError(
                        f"Permission denied creating schema: {catalog}.{schema}. "
                        f"Ensure you have CREATE SCHEMA permission on catalog {catalog}. "
                        f"Or create it manually: CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
                    )
                raise

            # Now create the volume (IF NOT EXISTS is safe)
            create_volume_sql = f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}"
            cursor.execute(create_volume_sql)

            self.log_verbose(f"✅ UC Volume ready: {catalog}.{schema}.{volume}")
            cursor.close()

        except ValueError:
            # Re-raise ValueError exceptions (our custom error messages)
            raise
        except Exception as e:
            error_msg = str(e).lower()

            # Check for permission errors
            if "permission" in error_msg or "access denied" in error_msg or "unauthorized" in error_msg:
                raise ValueError(
                    f"Permission denied creating UC Volume: {catalog}.{schema}.{volume}. "
                    f"Ensure you have CREATE VOLUME permission on schema {catalog}.{schema}. "
                    f"Or create it manually: CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}"
                )

            # Generic error
            raise ValueError(
                f"Failed to create UC Volume {catalog}.{schema}.{volume}: {e}. "
                f"Try creating manually: CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}"
            )

    def _upload_to_uc_volume(
        self,
        data_files: dict[str, Any],
        uc_volume_path: str,
        data_dir: Path,
        force_upload: bool = False,
    ) -> dict[str, str]:
        """Upload local data files to Unity Catalog Volume using Databricks Files API.

        For sharded files (e.g., customer.tbl.1.zst, customer.tbl.2.zst, ...),
        this method will find and upload ALL chunk files, returning a wildcard pattern
        for COPY INTO to use.

        Args:
            data_files: Dictionary of table_name -> local file path (may be first chunk only)
            uc_volume_path: UC Volume path (e.g., dbfs:/Volumes/catalog/schema/volume)
            data_dir: Base data directory (for resolving relative paths)

        Returns:
            Dictionary mapping table names to UC Volume file URIs (with wildcards for sharded tables)

        Raises:
            ImportError: If databricks-sdk not available
            Exception: If upload fails
        """
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            raise ImportError("databricks-sdk required for UC Volume uploads. Install with: uv add databricks-sdk")

        # Initialize Databricks SDK client (uses same credentials as SQL connector)
        workspace = WorkspaceClient(
            host=f"https://{self.server_hostname}",
            token=self.access_token,
        )

        # Convert dbfs:/Volumes/catalog/schema/volume to /Volumes/catalog/schema/volume
        volume_path = uc_volume_path.replace("dbfs:", "")

        # Extract local directory from DatabricksPath if applicable
        from benchbox.utils.cloud_storage import DatabricksPath

        if isinstance(data_dir, DatabricksPath):
            local_base_dir = data_dir._path  # Get actual local directory component
            self.log_very_verbose(f"Using DatabricksPath local component: {local_base_dir}")
        else:
            local_base_dir = Path(data_dir)

        # Pre-upload validation: if a local manifest exists and remote data matches it, reuse
        try:
            from benchbox.utils.cloud_storage import DatabricksPath
        except Exception:
            DatabricksPath = None  # type: ignore

        # Determine local manifest path
        if DatabricksPath is not None and isinstance(data_dir, DatabricksPath):
            manifest_path = data_dir._path / MANIFEST_FILENAME
        else:
            manifest_path = Path(data_dir) / MANIFEST_FILENAME

        if not force_upload and manifest_path.exists():
            # Use validation engine with Databricks adapter underneath (auto-detected)
            validation_engine = UploadValidationEngine()

            # Pass verbose flag from adapter settings for detailed validation reporting
            verbose = getattr(self, "very_verbose", False)

            should_upload, validation_result = validation_engine.should_upload_data(
                remote_path=uc_volume_path,
                local_manifest_path=manifest_path,
                force_upload=force_upload,
                verbose=verbose,
            )

            if not should_upload:
                # Data exists and is valid - rebuild mapping from remote manifest
                # Core module already logged validation messages
                remote_manifest = validation_result.remote_manifest
                if remote_manifest:
                    # Keep adapter-specific verbose logging for debugging
                    self.log_verbose("Reusing existing data from UC Volume (validation passed)")
                    return self._get_remote_file_uris_from_manifest(uc_volume_path, remote_manifest)
                else:
                    self.log_verbose(
                        "Pre-upload validation passed but remote manifest unavailable, proceeding with upload"
                    )

        # Upload manifest FIRST for atomic consistency
        if manifest_path.exists():
            try:
                self._upload_manifest_to_uc_volume(manifest_path, uc_volume_path, workspace)
            except Exception as e:
                self.logger.warning(f"Failed to upload manifest to UC Volume: {e}")

        uploaded_files = {}
        for table_name, file_path in data_files.items():
            local_path = Path(file_path) if not isinstance(file_path, Path) else file_path

            # If path is not absolute, it's relative to CWD - make it absolute for verification
            if not local_path.is_absolute():
                local_path = local_path.resolve()

            # Verify file exists and log details
            if not local_path.exists():
                self.logger.error(f"File not found for table {table_name}: {local_path}")
                self.logger.error(f"  Checked path: {local_path.absolute()}")
                self.logger.error(f"  CWD: {Path.cwd()}")
                continue

            file_size = local_path.stat().st_size
            self.log_very_verbose(f"Found {local_path.name} ({file_size:,} bytes) at {local_path}")

            # Check if this is a sharded file (e.g., customer.tbl.1.zst)
            # Pattern: base_name.ext.N.compression OR base_name.ext.N
            filename = local_path.name
            parts = filename.split(".")

            is_sharded = False
            chunk_files = []

            # Detect sharding patterns:
            # 1. customer.tbl.1.zst -> parts = ['customer', 'tbl', '1', 'zst']
            # 2. customer.tbl.1 -> parts = ['customer', 'tbl', '1']
            compression_exts = {".zst", ".gz", ".bz2", ".xz", ".lz4"}

            if len(parts) >= 3:
                # Check if second-to-last part is a digit (for compressed files)
                # OR if last part is a digit (for uncompressed files)
                if (
                    len(parts) >= 4
                    and parts[-1] in [ext.lstrip(".") for ext in compression_exts]
                    and parts[-2].isdigit()
                ):
                    # Pattern: customer.tbl.1.zst
                    is_sharded = True
                    base_parts = parts[:-2]  # ['customer', 'tbl']
                    compression = parts[-1]  # 'zst'
                    pattern = f"{'.'.join(base_parts)}.*.{compression}"
                elif parts[-1].isdigit():
                    # Pattern: customer.tbl.1
                    is_sharded = True
                    base_parts = parts[:-1]  # ['customer', 'tbl']
                    pattern = f"{'.'.join(base_parts)}.*"

                if is_sharded:
                    # Find all chunk files matching the pattern in the same directory
                    parent_dir = local_path.parent
                    chunk_files = sorted([f for f in parent_dir.glob(pattern) if f.is_file()])

                    if chunk_files:
                        self.log_verbose(f"Found {len(chunk_files)} chunk files for {table_name}: {pattern}")

            # Upload files
            if is_sharded and chunk_files:
                # Upload ALL chunk files
                for chunk_file in chunk_files:
                    # Validate file before upload
                    if not chunk_file.exists():
                        self.logger.error(f"Chunk file disappeared: {chunk_file}")
                        continue

                    chunk_size = chunk_file.stat().st_size
                    if chunk_size == 0:
                        self.logger.warning(f"Skipping empty chunk file: {chunk_file.name}")
                        continue

                    target_path = f"{volume_path}/{chunk_file.name}"
                    self.log_very_verbose(f"Uploading {chunk_file.name} ({chunk_size:,} bytes) to {target_path}")

                    try:
                        # Read file contents and verify before upload
                        with open(chunk_file, "rb") as f:
                            content = f.read()

                        if len(content) == 0:
                            self.logger.error(f"Read 0 bytes from {chunk_file} (expected {chunk_size})")
                            raise RuntimeError(f"Failed to read content from {chunk_file}")

                        if len(content) != chunk_size:
                            self.logger.warning(
                                f"Size mismatch for {chunk_file.name}: stat={chunk_size}, read={len(content)}"
                            )

                        # Upload using BytesIO to ensure we send what we read
                        from io import BytesIO

                        workspace.files.upload(target_path, BytesIO(content), overwrite=True)

                        self.log_very_verbose(f"Successfully uploaded {chunk_file.name} ({len(content):,} bytes)")
                    except Exception as e:
                        self.logger.error(f"Failed to upload {chunk_file.name} to UC Volume: {e}")
                        raise RuntimeError(f"Failed to upload {chunk_file.name} to {uc_volume_path}: {e}")

                # Return wildcard pattern for COPY INTO
                wildcard_uri = f"dbfs:{volume_path}/{pattern}"
                uploaded_files[table_name] = wildcard_uri
                self.log_verbose(f"Uploaded {len(chunk_files)} chunks for {table_name}, using wildcard: {wildcard_uri}")

            else:
                # Single file (not sharded) - upload with validation
                single_file_size = local_path.stat().st_size
                if single_file_size == 0:
                    self.logger.warning(f"Skipping empty file: {local_path.name}")
                    continue

                target_path = f"{volume_path}/{local_path.name}"
                self.log_verbose(f"Uploading {local_path.name} ({single_file_size:,} bytes) to {target_path}")

                try:
                    # Read file contents and verify before upload
                    with open(local_path, "rb") as f:
                        content = f.read()

                    if len(content) == 0:
                        self.logger.error(f"Read 0 bytes from {local_path} (expected {single_file_size})")
                        raise RuntimeError(f"Failed to read content from {local_path}")

                    if len(content) != single_file_size:
                        self.logger.warning(
                            f"Size mismatch for {local_path.name}: stat={single_file_size}, read={len(content)}"
                        )

                    # Upload using BytesIO to ensure we send what we read
                    from io import BytesIO

                    workspace.files.upload(target_path, BytesIO(content), overwrite=True)

                    # Store the dbfs:// URI for COPY INTO
                    uploaded_files[table_name] = f"dbfs:{target_path}"
                    self.log_verbose(f"Successfully uploaded {local_path.name} ({len(content):,} bytes)")

                except Exception as e:
                    self.logger.error(f"Failed to upload {local_path.name} to UC Volume: {e}")
                    raise RuntimeError(f"Failed to upload {local_path.name} to {uc_volume_path}: {e}")

        # Upload manifest last if present (manifest-first upload is handled by pre-upload validation)
        if manifest_path.exists():
            try:
                self._upload_manifest_to_uc_volume(manifest_path, uc_volume_path, workspace)
            except Exception as e:
                self.logger.warning(f"Failed to upload manifest to UC Volume: {e}")

        return uploaded_files

    def _upload_manifest_to_uc_volume(self, manifest_path: Path, uc_volume_path: str, workspace: Any) -> None:
        """Upload the manifest JSON to the UC Volume root."""
        try:
            target_path = uc_volume_path.replace("dbfs:", "")
            if not target_path.endswith("/" + MANIFEST_FILENAME):
                target_path = target_path.rstrip("/") + "/" + MANIFEST_FILENAME

            with open(manifest_path, "rb") as fh:
                content = fh.read()
            from io import BytesIO

            workspace.files.upload(target_path, BytesIO(content), overwrite=True)
            # Small log for visibility
            try:
                manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
                tables = manifest.get("tables") or {}
                self.logger.info(f"Uploaded manifest to {uc_volume_path} ({len(content)} bytes, {len(tables)} tables)")
            except Exception:
                self.logger.info(f"Uploaded manifest to {uc_volume_path}")
        except Exception as e:
            raise RuntimeError(f"Manifest upload failed: {e}")

    def _get_remote_file_uris_from_manifest(self, uc_volume_path: str, remote_manifest: dict) -> dict[str, str]:
        """Build UC Volume file URI map per table from manifest entries.

        For sharded tables, return a wildcard pattern like customer.tbl.*.zst
        """
        mapping: dict[str, str] = {}
        tables = remote_manifest.get("tables") or {}
        for table, entries in tables.items():
            if not entries:
                continue
            if len(entries) == 1:
                rel = entries[0].get("path")
                if rel:
                    mapping[table] = f"{uc_volume_path.rstrip('/')}/{rel}"
                continue
            # Try to detect a common sharded pattern: base.N[.ext]
            names = [str(e.get("path")) for e in entries if e.get("path")]
            if not names:
                continue

            # Derive wildcard: if all names share same prefix/suffix around a numeric segment
            # e.g., customer.tbl.1.zst -> base='customer.tbl', ext='.zst' => customer.tbl.*.zst
            def pattern_for(name: str) -> tuple[str, str, str]:
                parts = name.split(".")
                if len(parts) >= 3 and parts[-2].isdigit():
                    base = ".".join(parts[:-2])
                    ext = "." + parts[-1]
                    return base, ".*", ext
                if len(parts) >= 2 and parts[-1].isdigit():
                    base = ".".join(parts[:-1])
                    return base, ".*", ""
                # Fallback: wildcard whole name
                stem = Path(name).stem
                return stem, ".*", Path(name).suffix

            base0, star, ext0 = pattern_for(names[0])
            # Verify others align
            ok = True
            for n in names[1:]:
                b, s, e = pattern_for(n)
                if b != base0 or e != ext0:
                    ok = False
                    break
            if ok:
                wildcard = f"{base0}{star}{ext0}"
                mapping[table] = f"{uc_volume_path.rstrip('/')}/{wildcard}"
            else:
                # Fallback to first file
                mapping[table] = f"{uc_volume_path.rstrip('/')}/{names[0]}"
        return mapping

    def load_data(
        self, benchmark, connection: Any, data_dir: Path
    ) -> tuple[dict[str, int], float, dict[str, Any] | None]:
        """Load data using Databricks COPY INTO from UC Volumes or cloud storage.

        This implementation avoids temporary views and uses COPY INTO for robust ingestion.
        """
        start_time = time.time()
        self.log_operation_start("Data loading", f"benchmark: {benchmark.__class__.__name__}")
        self.log_very_verbose(f"Data directory: {data_dir}")

        table_stats = {}
        per_table_timings = {}  # Track detailed timings per table
        cursor = connection.cursor()

        try:
            # Get data files from benchmark
            data_files = None
            if hasattr(benchmark, "tables") and benchmark.tables:
                # Use generated data files from benchmark directly
                data_files = benchmark.tables
            elif hasattr(benchmark, "_impl") and hasattr(benchmark._impl, "tables") and benchmark._impl.tables:
                # Use generated data files from benchmark implementation
                data_files = benchmark._impl.tables

            if not data_files:
                # Manifest fallback
                try:
                    import json

                    manifest_path = Path(data_dir) / "_datagen_manifest.json"
                    if manifest_path.exists():
                        with open(manifest_path) as f:
                            manifest = json.load(f)
                        tables = manifest.get("tables") or {}
                        mapping = {}
                        for table, entries in tables.items():
                            if entries:
                                rel = entries[0].get("path")
                                if rel:
                                    mapping[table] = Path(data_dir) / rel
                        if mapping:
                            data_files = mapping
                            self.logger.debug("Using data files from _datagen_manifest.json")
                except Exception as e:
                    self.logger.debug(f"Manifest fallback failed: {e}")
            if not data_files:
                # No data files available - benchmark should have generated data first
                raise ValueError("No data files found. Ensure benchmark.generate_data() was called first.")

            # Determine staging root for COPY INTO
            def _is_cloud_uri(s: str) -> bool:
                return s.startswith(("s3://", "gs://", "abfss://", "dbfs:/"))

            stage_root = None

            # Check if data_dir is a DatabricksPath with dbfs_target
            from benchbox.utils.cloud_storage import DatabricksPath

            if isinstance(data_dir, DatabricksPath) and hasattr(data_dir, "dbfs_target") and data_dir.dbfs_target:
                # Use the dbfs target from DatabricksPath
                stage_root = data_dir.dbfs_target.rstrip("/")
                self.log_verbose(f"Using DatabricksPath dbfs_target: {stage_root}")

            # Prefer explicit staging_root
            elif isinstance(self.staging_root, str) and _is_cloud_uri(self.staging_root):
                stage_root = self.staging_root.rstrip("/")
            else:
                # Try to use UC Volume config
                if self.uc_catalog and self.uc_schema and self.uc_volume:
                    stage_root = f"dbfs:/Volumes/{self.uc_catalog}/{self.uc_schema}/{self.uc_volume}".rstrip("/")
                else:
                    # If data_dir looks like a cloud/DBFS URI, use it directly
                    data_dir_str = str(data_dir)
                    if _is_cloud_uri(data_dir_str):
                        stage_root = data_dir_str.rstrip("/")

            if not stage_root:
                raise ValueError(
                    "Databricks data loading requires a cloud/UC Volume staging location. "
                    "Add --output flag with cloud path `dbfs:/`; `s3://`, `gs://`, `abfss://`."
                )

            # If data is local and stage_root is a UC Volume, upload files first
            # For DatabricksPath, data is always local (temp dir) with remote target
            data_is_local = isinstance(data_dir, DatabricksPath) or not _is_cloud_uri(str(data_dir))

            def _is_complete_uc_volume_path(p: str) -> bool:
                v = p.replace("dbfs:", "").rstrip("/")
                if not v.startswith("/Volumes/"):
                    return False
                parts = v.split("/")
                return len(parts) >= 5  # ['', 'Volumes', 'catalog', 'schema', 'volume', ...]

            if data_is_local and stage_root.startswith("dbfs:/Volumes/") and _is_complete_uc_volume_path(stage_root):
                self.log_verbose(f"Uploading local data to UC Volume: {stage_root}")
                # Ensure UC Volume exists (create if necessary)
                self._ensure_uc_volume_exists(stage_root, connection)
                force_upload = getattr(self, "force_upload", False)
                original_files = dict(data_files)
                uploaded_files = self._upload_to_uc_volume(
                    data_files,
                    stage_root,
                    data_dir,
                    force_upload=force_upload,
                )
                # If upload returned a mapping, use it; otherwise, fall back to original mapping
                if uploaded_files:
                    data_files = uploaded_files
                else:
                    data_files = original_files
                self.log_verbose("Upload to UC Volume completed")

            # Ensure we're in the correct schema context for table operations
            cursor.execute(f"USE CATALOG {self.catalog}")
            cursor.execute(f"USE SCHEMA {self.schema}")
            self.log_verbose(f"Set schema context for data loading: {self.catalog}.{self.schema}")

            # Verify tables exist before attempting to load data
            cursor.execute(f"SHOW TABLES IN {self.catalog}.{self.schema}")
            existing_tables = {row[1].lower() for row in cursor.fetchall()}
            self.log_very_verbose(f"Found {len(existing_tables)} existing tables in {self.catalog}.{self.schema}")

            # Load data for each table using COPY INTO
            for table_name, file_path in data_files.items():
                try:
                    load_start = time.time()
                    table_name_upper = table_name.upper()

                    # Verify table exists before COPY INTO
                    if table_name.lower() not in existing_tables:
                        self.logger.error(f"Table {table_name_upper} not found in schema {self.catalog}.{self.schema}")
                        self.logger.error(f"Available tables: {sorted(existing_tables)}")
                        raise RuntimeError(
                            f"Table {table_name_upper} does not exist in {self.catalog}.{self.schema}. "
                            f"Ensure schema creation completed successfully before loading data."
                        )

                    # Determine path and delimiter
                    # After UC Volume upload, file_path is already a full URI (dbfs:/Volumes/...)
                    # Otherwise, construct from stage_root + filename
                    if isinstance(file_path, str) and file_path.startswith("dbfs:/Volumes/"):
                        # Already uploaded to UC Volume - use as-is (may contain wildcards for sharded tables)
                        file_uri = file_path
                        # Extract filename pattern (handle wildcards like customer.tbl.*.zst)
                        uri_path = file_path.replace("dbfs:", "")
                        filename = uri_path.split("/")[-1]  # Get last part of path (may have wildcards)
                    else:
                        # Construct URI from stage_root
                        rel = None
                        if hasattr(file_path, "name"):
                            rel = getattr(file_path, "name", None)
                        else:
                            # If this is already a string/path-like, just get filename
                            rel = Path(str(file_path)).name
                        filename = rel
                        file_uri = f"{stage_root}/{rel}"

                    # Detect file format - handle compressed files (.zst, .gz, .bz2)
                    # For wildcard patterns (e.g., customer.tbl.*.zst), remove wildcard for format detection
                    compression_exts = {".zst", ".gz", ".bz2", ".xz", ".lz4"}

                    # Strip wildcard component for format detection (customer.tbl.*.zst -> customer.tbl.zst)
                    filename_for_format = filename.replace(".*", "")
                    file_path_obj = Path(filename_for_format)

                    # Strip compression extension if present
                    base_name = filename_for_format
                    if file_path_obj.suffix in compression_exts:
                        base_name = file_path_obj.stem  # nation.tbl.zst -> nation.tbl

                    # Now get the actual data format suffix
                    format_suffix = Path(base_name).suffix or ".tbl"

                    # TPC benchmarks use pipe delimiter, CSV uses comma
                    # TPC-H uses .tbl files, TPC-DS uses .dat files - both are pipe-delimited
                    delimiter = "|" if format_suffix in [".tbl", ".dat"] else ","

                    # Get column names from benchmark schema for explicit column mapping
                    # This fixes the "Incoming schema has additional field(s): _c0, _c1, _c2" error
                    # Delta Lake requires explicit column mapping when header='false'
                    column_list = ""
                    if hasattr(benchmark, "get_schema"):
                        try:
                            schema = benchmark.get_schema()
                            # All benchmarks now return dict[str, dict] format
                            # Try case-insensitive lookup
                            table_schema = schema.get(table_name.lower())
                            if not table_schema:
                                # Fallback to uppercase lookup
                                table_schema = schema.get(table_name_upper.lower())
                            if not table_schema:
                                # Try original case
                                table_schema = schema.get(table_name)

                            if table_schema and "columns" in table_schema:
                                columns = [col["name"] for col in table_schema["columns"]]
                                if columns:
                                    column_list = f" ({', '.join(columns)})"
                                    self.log_very_verbose(
                                        f"Using explicit column mapping for {table_name_upper}: {len(columns)} columns"
                                    )
                        except Exception as e:
                            self.log_very_verbose(f"Could not get column list for {table_name}: {e}")

                    copy_sql = (
                        f"COPY INTO {table_name_upper}{column_list} FROM '{file_uri}' "
                        f"FILEFORMAT = CSV FORMAT_OPTIONS('delimiter'='{delimiter}', 'header'='false')"
                    )

                    # Log wildcard pattern for visibility
                    if "*" in file_uri:
                        self.log_verbose(f"Loading {table_name_upper} from wildcard pattern: {file_uri}")

                    # Time COPY INTO
                    copy_start = time.time()
                    cursor.execute(copy_sql)
                    copy_time = time.time() - copy_start

                    # Row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name_upper}")
                    row_count = cursor.fetchone()[0]
                    table_stats[table_name_upper] = row_count

                    # Optional optimize - track timing separately
                    optimize_time = 0.0
                    if self.enable_delta_optimization:
                        optimize_start = time.time()
                        with contextlib.suppress(Exception):
                            cursor.execute(f"OPTIMIZE {table_name_upper}")
                        optimize_time = time.time() - optimize_start

                    load_time = time.time() - load_start

                    # Store detailed timings
                    per_table_timings[table_name_upper] = {
                        "copy_into_ms": copy_time * 1000,
                        "optimize_ms": optimize_time * 1000,
                        "total_ms": load_time * 1000,
                        "rows": row_count,
                    }

                    self.logger.info(f"✅ Loaded {row_count:,} rows into {table_name_upper} in {load_time:.2f}s")

                except Exception as e:
                    self.logger.error(f"Failed to load {table_name}: {str(e)[:200]}")
                    table_stats[table_name.upper()] = 0
                    # Record failed table with zero timings
                    per_table_timings[table_name.upper()] = {
                        "copy_into_ms": 0,
                        "optimize_ms": 0,
                        "total_ms": 0,
                        "rows": 0,
                    }

            total_time = time.time() - start_time
            total_rows = sum(table_stats.values())
            self.log_operation_complete(
                "Data loading", total_time, f"{total_rows:,} total rows, {len(table_stats)} tables"
            )

        finally:
            cursor.close()

        return table_stats, total_time, per_table_timings

    def configure_for_benchmark(self, connection: Any, benchmark_type: str) -> None:
        """Apply Databricks-specific configurations including cache control.

        Applies result cache control first, then any user-provided custom Spark configurations.
        """
        cursor = connection.cursor()

        try:
            # Apply result cache control - disable by default for accurate benchmarking
            if self.disable_result_cache:
                try:
                    cursor.execute("SET use_cached_result = false")
                    self.logger.debug("Disabled result cache (use_cached_result = false)")
                except Exception as e:
                    self.logger.warning(f"Failed to disable result cache: {e}")

            # Apply user-provided configurations if specified
            if hasattr(self, "spark_configs") and self.spark_configs:
                for config_key, config_value in self.spark_configs.items():
                    try:
                        cursor.execute(f"SET {config_key} = {config_value}")
                        self.logger.debug(f"Set {config_key} = {config_value}")
                    except Exception as e:
                        self.logger.warning(f"Failed to set {config_key}: {e}")
            else:
                self.logger.debug("No custom Spark configurations to apply")

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
        """Execute query with detailed timing and profiling."""
        start_time = time.time()
        self.log_verbose(f"Executing query {query_id}")
        self.log_very_verbose(f"Query SQL (first 200 chars): {query[:200]}{'...' if len(query) > 200 else ''}")

        cursor = connection.cursor()

        try:
            # Schema context is already set in create_connection() and persists for the session
            # No need to set USE <catalog>.<schema> before every query - it adds unnecessary overhead
            # (Each USE statement = 1 extra round-trip to Databricks)

            # Execute the query
            # Note: Query dialect translation is now handled automatically by the base adapter
            cursor.execute(query)
            result = cursor.fetchall()

            execution_time = time.time() - start_time
            actual_row_count = len(result) if result else 0

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

            # Include Databricks-specific fields
            result_dict["translated_query"] = None  # Translation handled by base adapter

            # Add resource usage for cost calculation (execution time for DBU estimation)
            result_dict["resource_usage"] = {
                "execution_time_seconds": execution_time,
            }

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

    def _fix_databricks_sql_syntax(self, sql: str) -> str:
        """Transform SQL syntax for Databricks compatibility.

        This method removes SQL syntax that is not supported by Databricks/Spark SQL,
        particularly NULLS FIRST/LAST clauses in PRIMARY KEY constraints.

        Args:
            sql: SQL statement(s) to fix

        Returns:
            Fixed SQL with Databricks-compatible syntax
        """
        import re

        original_sql = sql

        # Pattern 1: Remove NULLS LAST/FIRST from PRIMARY KEY constraints
        # Databricks doesn't support NULLS ordering in PRIMARY KEY definitions
        # Match: PRIMARY KEY (col1, col2 NULLS LAST)
        # Also match: PRIMARY KEY (col1 NULLS FIRST, col2)
        nulls_in_pk_pattern = r"\b(PRIMARY\s+KEY\s*\([^)]*?)\s+NULLS\s+(LAST|FIRST)\s*([^)]*?\))"

        def remove_nulls_from_pk(match):
            # Reconstruct without the NULLS clause
            before = match.group(1)  # PRIMARY KEY (col1, col2
            after = match.group(3)  # remaining part + closing paren
            return f"{before} {after}".strip()

        fixed_sql = re.sub(nulls_in_pk_pattern, remove_nulls_from_pk, sql, flags=re.IGNORECASE)

        # Pattern 2: Remove standalone NULLS clauses in column definitions within PRIMARY KEY
        # This catches cases like: PRIMARY KEY (col1 NULLS LAST, col2 NULLS FIRST)
        # Apply multiple times to catch all occurrences
        max_iterations = 10  # Safety limit
        for _ in range(max_iterations):
            prev = fixed_sql
            fixed_sql = re.sub(
                r"\b(PRIMARY\s+KEY\s*\([^)]*?)\s+NULLS\s+(LAST|FIRST)\b",
                r"\1",
                fixed_sql,
                flags=re.IGNORECASE,
            )
            if fixed_sql == prev:
                break  # No more replacements

        # Log if any changes were made
        if fixed_sql != original_sql:
            changes_made = original_sql != fixed_sql
            if changes_made:
                self.log_very_verbose("Fixed Databricks SQL syntax (removed NULLS FIRST/LAST from PRIMARY KEY)")
                self.log_very_verbose(f"Before: {original_sql[:200]}...")
                self.log_very_verbose(f"After:  {fixed_sql[:200]}...")

        return fixed_sql

    def _convert_to_delta_table(self, statement: str) -> str:
        """Convert CREATE TABLE statement to Delta Lake format."""
        if not statement.upper().startswith("CREATE TABLE"):
            return statement

        # Ensure idempotency with OR REPLACE
        if "CREATE TABLE" in statement.upper() and "OR REPLACE" not in statement.upper():
            statement = statement.replace("CREATE TABLE", "CREATE OR REPLACE TABLE", 1)

        # Default to DELTA format when unspecified
        if "USING" not in statement.upper():
            # Find the closing parenthesis of column definitions
            paren_count = 0
            using_pos = len(statement)

            for i, char in enumerate(statement):
                if char == "(":
                    paren_count += 1
                elif char == ")":
                    paren_count -= 1
                    if paren_count == 0:
                        using_pos = i + 1
                        break

            # Insert USING DELTA clause
            statement = statement[:using_pos] + " USING DELTA" + statement[using_pos:]

        # Include Delta Lake optimization properties
        if "TBLPROPERTIES" not in statement.upper():
            statement += " TBLPROPERTIES ("
            properties = []

            if self.delta_auto_optimize:
                properties.append("'delta.autoOptimize.optimizeWrite' = 'true'")
                properties.append("'delta.autoOptimize.autoCompact' = 'true'")

            statement += ", ".join(properties) + ")"

        return statement

    def _get_platform_metadata(self, connection: Any) -> dict[str, Any]:
        """Get Databricks-specific metadata and system information."""
        metadata = {
            "platform": self.platform_name,
            "server_hostname": self.server_hostname,
            "catalog": self.catalog,
            "schema": self.schema,
            "result_cache_enabled": not self.disable_result_cache,
        }

        cursor = connection.cursor()

        try:
            # Get Spark version
            cursor.execute("SELECT version()")
            result = cursor.fetchone()
            metadata["spark_version"] = result[0] if result else "unknown"

            # Get current catalog and schema
            cursor.execute("SELECT current_catalog(), current_schema()")
            result = cursor.fetchone()
            if result:
                metadata["current_catalog"] = result[0]
                metadata["current_schema"] = result[1]

            # Get cluster information
            cursor.execute("SHOW FUNCTIONS LIKE 'current_*'")
            functions = cursor.fetchall()
            metadata["available_functions"] = [f[0] for f in functions]

            # Get Spark configurations
            cursor.execute("SET")
            configs = cursor.fetchall()
            spark_configs = {k: v for k, v in configs if k.startswith("spark.")}
            metadata["spark_configurations"] = spark_configs

        except Exception as e:
            metadata["metadata_error"] = str(e)
        finally:
            cursor.close()

        return metadata

    def analyze_table(self, connection: Any, table_name: str) -> None:
        """Run ANALYZE TABLE for better query optimization."""
        cursor = connection.cursor()
        try:
            cursor.execute(f"ANALYZE TABLE {table_name.upper()} COMPUTE STATISTICS")
            self.logger.info(f"Analyzed table {table_name.upper()}")
        except Exception as e:
            self.logger.warning(f"Failed to analyze table {table_name}: {e}")
        finally:
            cursor.close()

    def optimize_table(self, connection: Any, table_name: str) -> None:
        """Optimize Delta Lake table."""
        if not self.enable_delta_optimization:
            return

        cursor = connection.cursor()
        try:
            cursor.execute(f"OPTIMIZE {table_name.upper()}")
            self.logger.info(f"Optimized Delta table {table_name.upper()}")
        except Exception as e:
            self.logger.warning(f"Failed to optimize table {table_name}: {e}")
        finally:
            cursor.close()

    def vacuum_table(self, connection: Any, table_name: str, hours: int = 168) -> None:
        """Vacuum Delta Lake table to remove old files."""
        if not self.enable_delta_optimization:
            return

        cursor = connection.cursor()
        try:
            cursor.execute(f"VACUUM {table_name.upper()} RETAIN {hours} HOURS")
            self.logger.info(f"Vacuumed Delta table {table_name.upper()}")
        except Exception as e:
            self.logger.warning(f"Failed to vacuum table {table_name}: {e}")
        finally:
            cursor.close()

    def _get_existing_tables(self, connection: Any) -> list[str]:
        """Get list of existing tables in the Databricks schema."""
        try:
            cursor = connection.cursor()
            # Use Databricks-specific query to get tables in current schema
            cursor.execute(f"SHOW TABLES IN {self.catalog}.{self.schema}")
            result = cursor.fetchall()
            cursor.close()

            # Result format is (database, tableName, isTemporary)
            return [row[1] for row in result if not row[2]]  # Exclude temporary tables
        except Exception as e:
            self.logger.debug(f"Failed to get existing tables: {e}")
            return []

    def close_connection(self, connection: Any) -> None:
        """Close Databricks connection."""
        try:
            if connection and hasattr(connection, "close"):
                connection.close()
        except Exception as e:
            self.logger.warning(f"Error closing connection: {e}")

    def supports_tuning_type(self, tuning_type) -> bool:
        """Check if Databricks supports a specific tuning type.

        Databricks supports:
        - PARTITIONING: Via PARTITIONED BY clause in Delta Lake
        - CLUSTERING: Via CLUSTER BY clause (Delta Lake 2.0+)
        - DISTRIBUTION: Via Spark optimization hints and Z-ORDER clustering

        Args:
            tuning_type: The type of tuning to check support for

        Returns:
            True if the tuning type is supported by Databricks
        """
        # Import here to avoid circular imports
        try:
            from benchbox.core.tuning.interface import TuningType

            return tuning_type in {
                TuningType.PARTITIONING,
                TuningType.CLUSTERING,
                TuningType.DISTRIBUTION,
            }
        except ImportError:
            return False

    def generate_tuning_clause(self, table_tuning) -> str:
        """Generate Databricks-specific tuning clauses for CREATE TABLE statements.

        Databricks supports:
        - USING DELTA (Delta Lake format)
        - PARTITIONED BY (column1, column2, ...)
        - CLUSTER BY (column1, column2, ...) for Delta Lake 2.0+
        - Z-ORDER optimization

        Args:
            table_tuning: The tuning configuration for the table

        Returns:
            SQL clause string to be appended to CREATE TABLE statement
        """
        if not table_tuning or not table_tuning.has_any_tuning():
            return ""

        clauses = []

        try:
            # Import here to avoid circular imports
            from benchbox.core.tuning.interface import TuningType

            # Always use Delta Lake format for better performance
            clauses.append("USING DELTA")

            # Handle partitioning
            partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
            if partition_columns:
                # Sort by order and create partition clause
                sorted_cols = sorted(partition_columns, key=lambda col: col.order)
                column_names = [col.name for col in sorted_cols]

                partition_clause = f"PARTITIONED BY ({', '.join(column_names)})"
                clauses.append(partition_clause)

            # Handle clustering (Delta Lake 2.0+)
            cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)
            if cluster_columns:
                # Sort by order and create cluster clause
                sorted_cols = sorted(cluster_columns, key=lambda col: col.order)
                column_names = [col.name for col in sorted_cols]

                cluster_clause = f"CLUSTER BY ({', '.join(column_names)})"
                clauses.append(cluster_clause)

            # Distribution handled through Z-ORDER optimization (applied post-creation)

        except ImportError:
            # If tuning interface not available, at least use Delta format
            clauses.append("USING DELTA")

        return " ".join(clauses)

    def apply_table_tunings(self, table_tuning, connection: Any) -> None:
        """Apply tuning configurations to a Databricks Delta Lake table.

        Databricks tuning approach:
        - PARTITIONING: Handled via PARTITIONED BY in CREATE TABLE
        - CLUSTERING: Handled via CLUSTER BY in CREATE TABLE or ALTER TABLE
        - DISTRIBUTION: Achieved through Z-ORDER clustering and OPTIMIZE
        - Delta Lake optimization and maintenance

        Args:
            table_tuning: The tuning configuration to apply
            connection: Databricks connection

        Raises:
            ValueError: If the tuning configuration is invalid for Databricks
        """
        if not table_tuning or not table_tuning.has_any_tuning():
            return

        table_name = table_tuning.table_name.upper()
        self.logger.info(f"Applying Databricks tunings for table: {table_name}")

        cursor = connection.cursor()
        try:
            # Import here to avoid circular imports
            from benchbox.core.tuning.interface import TuningType

            # Check if table exists and is Delta format
            cursor.execute(f"DESCRIBE EXTENDED {table_name}")
            table_info = cursor.fetchall()

            is_delta_table = any("DELTA" in str(row).upper() for row in table_info)
            if not is_delta_table:
                self.logger.warning(
                    f"Table {table_name} is not a Delta table - some optimizations may not be available"
                )

            # Handle clustering via Z-ORDER optimization
            cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)
            distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)

            # Combine clustering and distribution columns for Z-ORDER
            zorder_columns = []
            if cluster_columns:
                sorted_cols = sorted(cluster_columns, key=lambda col: col.order)
                zorder_columns.extend([col.name for col in sorted_cols])

            if distribution_columns:
                sorted_cols = sorted(distribution_columns, key=lambda col: col.order)
                # Include distribution columns if not already in clustering
                for col in sorted_cols:
                    if col.name not in zorder_columns:
                        zorder_columns.append(col.name)

            if zorder_columns and is_delta_table:
                # Apply Z-ORDER optimization
                zorder_clause = f"OPTIMIZE {table_name} ZORDER BY ({', '.join(zorder_columns)})"
                try:
                    cursor.execute(zorder_clause)
                    self.logger.info(f"Applied Z-ORDER optimization to {table_name}: {', '.join(zorder_columns)}")
                except Exception as e:
                    self.logger.warning(f"Failed to apply Z-ORDER optimization to {table_name}: {e}")

            # Handle partitioning information (logging only, as it's defined at CREATE TABLE time)
            partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
            if partition_columns:
                sorted_cols = sorted(partition_columns, key=lambda col: col.order)
                column_names = [col.name for col in sorted_cols]
                self.logger.info(
                    f"Partitioning strategy for {table_name}: {', '.join(column_names)} (defined at CREATE TABLE time)"
                )

            # Handle sorting through clustering/Z-ORDER
            sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)
            if sort_columns:
                sorted_cols = sorted(sort_columns, key=lambda col: col.order)
                column_names = [col.name for col in sorted_cols]
                self.logger.info(
                    f"Sorting in Databricks achieved via Z-ORDER clustering for table {table_name}: {', '.join(column_names)}"
                )

            # Perform general Delta Lake optimizations
            if is_delta_table and self.enable_delta_optimization:
                try:
                    # Run OPTIMIZE to compact small files
                    cursor.execute(f"OPTIMIZE {table_name}")
                    self.logger.info(f"Optimized Delta table {table_name}")

                    # Refresh table statistics
                    cursor.execute(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
                    self.logger.info(f"Updated statistics for {table_name}")

                except Exception as e:
                    self.logger.warning(f"Failed to optimize Delta table {table_name}: {e}")

        except ImportError:
            self.logger.warning("Tuning interface not available - skipping tuning application")
        except Exception as e:
            raise ValueError(f"Failed to apply tunings to Databricks table {table_name}: {e}")
        finally:
            cursor.close()

    def apply_unified_tuning(self, unified_config: UnifiedTuningConfiguration, connection: Any) -> None:
        """Apply unified tuning configuration to Databricks.

        Args:
            unified_config: Unified tuning configuration to apply
            connection: Databricks connection
        """
        if not unified_config:
            return

        # Apply constraint configurations
        self.apply_constraint_configuration(unified_config.primary_keys, unified_config.foreign_keys, connection)

        # Apply platform optimizations
        if unified_config.platform_optimizations:
            self.apply_platform_optimizations(unified_config.platform_optimizations, connection)

        # Apply table-level tunings
        for _table_name, table_tuning in unified_config.table_tunings.items():
            self.apply_table_tunings(table_tuning, connection)

    def apply_platform_optimizations(self, platform_config: PlatformOptimizationConfiguration, connection: Any) -> None:
        """Apply Databricks-specific platform optimizations.

        Databricks optimizations include:
        - Spark configuration tuning (adaptive query execution, join strategies)
        - Delta Lake optimization settings (auto-optimize, auto-compact)
        - Cluster autoscaling and resource allocation
        - Unity Catalog performance settings

        Args:
            platform_config: Platform optimization configuration
            connection: Databricks connection
        """
        if not platform_config:
            return

        # Databricks optimizations are typically applied at Spark session level
        # Store optimizations for use during query execution and Delta Lake operations
        self.logger.info("Databricks platform optimizations stored for Spark session and Delta Lake management")

    def apply_constraint_configuration(
        self,
        primary_key_config: PrimaryKeyConfiguration,
        foreign_key_config: ForeignKeyConfiguration,
        connection: Any,
    ) -> None:
        """Apply constraint configurations to Databricks.

        Note: Databricks (Spark SQL) supports PRIMARY KEY and FOREIGN KEY constraints
        but they are informational only (not enforced). They are used for query optimization
        in Catalyst optimizer and must be applied during table creation time.

        Args:
            primary_key_config: Primary key constraint configuration
            foreign_key_config: Foreign key constraint configuration
            connection: Databricks connection
        """
        # Databricks constraints are applied at table creation time for Catalyst optimization
        # This method is called after tables are created, so log the configurations

        if primary_key_config and primary_key_config.enabled:
            self.logger.info(
                "Primary key constraints enabled for Databricks (informational only, applied during table creation)"
            )

        if foreign_key_config and foreign_key_config.enabled:
            self.logger.info(
                "Foreign key constraints enabled for Databricks (informational only, applied during table creation)"
            )

        # Databricks constraints are informational and used by Catalyst optimizer
        # No additional work to do here as they're applied during CREATE TABLE
