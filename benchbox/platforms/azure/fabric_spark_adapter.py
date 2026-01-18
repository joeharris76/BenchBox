"""Microsoft Fabric Spark platform adapter.

Microsoft Fabric is Microsoft's unified analytics platform providing SaaS Spark,
Data Factory, Power BI, and more. This adapter integrates with Fabric's Spark
pools via the Livy API for benchmark execution.

Key Features:
- SaaS: Fully managed, no infrastructure to configure
- OneLake: Unified storage with automatic lakehouse semantics
- Entra ID: Azure Active Directory authentication
- Livy: Apache Livy REST API for Spark session management

Usage:
    from benchbox.platforms.azure import FabricSparkAdapter

    adapter = FabricSparkAdapter(
        workspace_id="your-workspace-id",
        lakehouse_id="your-lakehouse-id",
        tenant_id="your-tenant-id",
    )

    # Run TPC-H benchmark
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
        ForeignKeyConfiguration,
        PlatformOptimizationConfiguration,
        PrimaryKeyConfiguration,
        UnifiedTuningConfiguration,
    )

from benchbox.core.exceptions import ConfigurationError
from benchbox.platforms.base import PlatformAdapter
from benchbox.platforms.base.cloud_spark import (
    CloudSparkStaging,
    SparkConfigOptimizer,
    SparkTuningMixin,
)
from benchbox.platforms.base.cloud_spark.config import CloudPlatform
from benchbox.utils.dependencies import (
    check_platform_dependencies,
    get_dependency_error_message,
)

try:
    from azure.identity import DefaultAzureCredential

    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    DefaultAzureCredential = None
    AZURE_IDENTITY_AVAILABLE = False

try:
    from azure.storage.filedatalake import DataLakeServiceClient

    AZURE_DATALAKE_AVAILABLE = True
except ImportError:
    DataLakeServiceClient = None
    AZURE_DATALAKE_AVAILABLE = False

try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    REQUESTS_AVAILABLE = False

logger = logging.getLogger(__name__)


class LivySessionState:
    """Livy session state constants."""

    NOT_STARTED = "not_started"
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"
    KILLED = "killed"
    SUCCESS = "success"


class LivyStatementState:
    """Livy statement state constants."""

    WAITING = "waiting"
    RUNNING = "running"
    AVAILABLE = "available"
    ERROR = "error"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class FabricSparkAdapter(SparkTuningMixin, PlatformAdapter):
    """Microsoft Fabric Spark platform adapter.

    Fabric Spark provides SaaS Spark execution within the Microsoft Fabric
    ecosystem. This adapter uses the Livy REST API for session and statement
    management, with OneLake for data staging.

    Execution Model:
    - Create Livy session in Fabric Spark pool
    - Execute Spark SQL statements via Livy
    - Results returned via Livy statement output
    - OneLake (ADLS Gen2) for data staging

    Key Features:
    - SaaS: Fully managed, no infrastructure
    - OneLake: Unified lakehouse storage
    - Delta Lake: Native Delta format support
    - Entra ID: Azure AD authentication

    Billing:
    - Capacity Units (CU) per workspace
    - Spark compute charged per CU-second
    - OneLake storage separate
    """

    # Fabric API endpoints
    FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
    ONELAKE_DFS_BASE = "https://onelake.dfs.fabric.microsoft.com"

    def __init__(
        self,
        workspace_id: str | None = None,
        lakehouse_id: str | None = None,
        tenant_id: str | None = None,
        livy_endpoint: str | None = None,
        onelake_path: str | None = None,
        spark_pool_name: str | None = None,
        timeout_minutes: int = 60,
        spark_config: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the Fabric Spark adapter.

        Args:
            workspace_id: Fabric workspace GUID (required).
            lakehouse_id: Fabric Lakehouse GUID (required).
            tenant_id: Azure tenant ID for authentication.
            livy_endpoint: Custom Livy endpoint URL (auto-derived if not provided).
            onelake_path: OneLake path for data staging (auto-derived if not provided).
            spark_pool_name: Spark pool name (default: uses workspace default).
            timeout_minutes: Statement timeout in minutes (default: 60).
            spark_config: Additional Spark configuration.
            **kwargs: Additional platform options.
        """
        if not AZURE_IDENTITY_AVAILABLE:
            deps_satisfied, missing = check_platform_dependencies("fabric-spark")
            if not deps_satisfied:
                raise ConfigurationError(get_dependency_error_message("fabric-spark", missing))

        if not workspace_id:
            raise ConfigurationError("workspace_id is required (Fabric workspace GUID)")

        if not lakehouse_id:
            raise ConfigurationError("lakehouse_id is required (Fabric Lakehouse GUID)")

        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.tenant_id = tenant_id
        self.spark_pool_name = spark_pool_name
        self.timeout_minutes = timeout_minutes
        self.user_spark_config = spark_config or {}

        # Derive Livy endpoint if not provided
        self.livy_endpoint = livy_endpoint or self._derive_livy_endpoint()

        # Derive OneLake path if not provided
        self.onelake_path = onelake_path or self._derive_onelake_path()

        # Initialize staging using cloud-spark shared infrastructure
        self._staging: CloudSparkStaging | None = None
        try:
            # OneLake supports abfss:// protocol
            staging_uri = (
                f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_id}/Files/benchbox"
            )
            self._staging = CloudSparkStaging.from_uri(staging_uri)
        except Exception as e:
            logger.warning(f"Failed to initialize OneLake staging: {e}")

        # Credential (lazy initialization)
        self._credential: Any = None
        self._access_token: str | None = None
        self._token_expires_at: float = 0

        # Session management
        self._session_id: int | None = None
        self._session_created_by_us = False

        # Metrics tracking
        self._query_count = 0
        self._total_statement_time_seconds = 0.0

        # Benchmark configuration (set via configure_for_benchmark)
        self._benchmark_type: str | None = None
        self._scale_factor: float = 1.0
        self._spark_config: dict[str, str] = {}

        super().__init__(**kwargs)

    def _derive_livy_endpoint(self) -> str:
        """Derive the Livy endpoint from workspace ID."""
        # Fabric Livy endpoint format
        return f"https://api.fabric.microsoft.com/v1/workspaces/{self.workspace_id}/lakehouses/{self.lakehouse_id}/livyApi/versions/2023-12-01/sessions"

    def _derive_onelake_path(self) -> str:
        """Derive the OneLake path for data staging."""
        return f"abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_id}"

    def _get_credential(self) -> Any:
        """Get or create Azure credential."""
        if self._credential is None:
            kwargs: dict[str, Any] = {}
            if self.tenant_id:
                # For service principal auth, tenant_id helps scope the credential
                kwargs["additionally_allowed_tenants"] = ["*"]
            self._credential = DefaultAzureCredential(**kwargs)
        return self._credential

    def _get_access_token(self) -> str:
        """Get a valid access token, refreshing if needed."""
        current_time = time.time()

        # Refresh token if expired or about to expire (5 minute buffer)
        if self._access_token is None or current_time >= self._token_expires_at - 300:
            credential = self._get_credential()
            # Fabric API scope
            token = credential.get_token("https://api.fabric.microsoft.com/.default")
            self._access_token = token.token
            self._token_expires_at = token.expires_on

        return self._access_token

    def _get_headers(self) -> dict[str, str]:
        """Get HTTP headers with authentication."""
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Return platform metadata.

        Args:
            connection: Not used (Fabric Spark manages sessions internally).

        Returns:
            Dict with platform information including name, version, and capabilities.
        """
        return {
            "platform": "fabric-spark",
            "display_name": "Microsoft Fabric Spark",
            "vendor": "Microsoft",
            "type": "managed_spark",
            "workspace_id": self.workspace_id,
            "lakehouse_id": self.lakehouse_id,
            "spark_pool": self.spark_pool_name,
            "supports_sql": True,
            "supports_dataframe": True,
            "billing_model": "Capacity Units (CU)",
            "storage": "OneLake",
        }

    def _create_session(self) -> int:
        """Create a new Livy session.

        Returns:
            The session ID.
        """
        if not REQUESTS_AVAILABLE:
            raise ConfigurationError("requests package is required for Fabric Spark")

        # Build session configuration
        session_config: dict[str, Any] = {
            "kind": "spark",
            "conf": {
                # Default Spark configuration for benchmarks
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            },
        }

        # Add user-provided Spark config
        session_config["conf"].update(self.user_spark_config)

        # Add benchmark-specific configuration
        if self._spark_config:
            session_config["conf"].update(self._spark_config)

        if self.spark_pool_name:
            session_config["name"] = f"benchbox-{self.spark_pool_name}"

        logger.info(f"Creating Livy session in workspace {self.workspace_id}")

        response = requests.post(
            self.livy_endpoint,
            headers=self._get_headers(),
            json=session_config,
            timeout=60,
        )

        if response.status_code not in (200, 201):
            raise ConfigurationError(f"Failed to create Livy session: {response.status_code} - {response.text}")

        session = response.json()
        session_id = session["id"]

        # Wait for session to be ready
        self._wait_for_session_state(session_id, [LivySessionState.IDLE])
        self._session_created_by_us = True

        logger.info(f"Livy session created: {session_id}")
        return session_id

    def _wait_for_session_state(
        self,
        session_id: int,
        target_states: list[str],
        timeout_seconds: int = 600,
    ) -> str:
        """Wait for session to reach a target state.

        Args:
            session_id: Session ID to wait for.
            target_states: List of acceptable target states.
            timeout_seconds: Maximum wait time.

        Returns:
            The final session state.
        """
        start_time = time.time()
        session_url = f"{self.livy_endpoint}/{session_id}"

        while time.time() - start_time < timeout_seconds:
            response = requests.get(
                session_url,
                headers=self._get_headers(),
                timeout=30,
            )

            if response.status_code != 200:
                raise ConfigurationError(f"Failed to get session status: {response.status_code}")

            session = response.json()
            state = session["state"]

            if state in target_states:
                return state
            if state in [LivySessionState.ERROR, LivySessionState.DEAD, LivySessionState.KILLED]:
                raise ConfigurationError(f"Session is in {state} state")

            time.sleep(5)

        raise ConfigurationError(f"Timeout waiting for session {session_id}")

    def _ensure_session(self) -> int:
        """Ensure a Livy session exists and is ready.

        Returns:
            The session ID.
        """
        if self._session_id is not None:
            # Verify session is still valid
            session_url = f"{self.livy_endpoint}/{self._session_id}"
            try:
                response = requests.get(
                    session_url,
                    headers=self._get_headers(),
                    timeout=30,
                )
                if response.status_code == 200:
                    session = response.json()
                    if session["state"] == LivySessionState.IDLE:
                        return self._session_id
                    if session["state"] == LivySessionState.BUSY:
                        # Wait for it to become idle
                        self._wait_for_session_state(self._session_id, [LivySessionState.IDLE])
                        return self._session_id
            except Exception as e:
                logger.warning(f"Session {self._session_id} is invalid: {e}")
                self._session_id = None

        # Create new session
        self._session_id = self._create_session()
        return self._session_id

    def _execute_statement(
        self,
        code: str,
        kind: str = "sql",
    ) -> dict[str, Any]:
        """Execute a statement in the Livy session.

        Args:
            code: The code to execute.
            kind: Statement kind ('sql', 'spark', 'pyspark').

        Returns:
            The statement result.
        """
        session_id = self._ensure_session()
        statements_url = f"{self.livy_endpoint}/{session_id}/statements"

        statement_data = {
            "code": code,
            "kind": kind,
        }

        start_time = time.time()

        response = requests.post(
            statements_url,
            headers=self._get_headers(),
            json=statement_data,
            timeout=60,
        )

        if response.status_code not in (200, 201):
            raise ConfigurationError(f"Failed to submit statement: {response.status_code} - {response.text}")

        statement = response.json()
        statement_id = statement["id"]

        # Wait for statement to complete
        result = self._wait_for_statement(session_id, statement_id)

        execution_time = time.time() - start_time
        self._total_statement_time_seconds += execution_time
        self._query_count += 1

        return result

    def _wait_for_statement(
        self,
        session_id: int,
        statement_id: int,
    ) -> dict[str, Any]:
        """Wait for statement to complete and return result.

        Args:
            session_id: Session ID.
            statement_id: Statement ID.

        Returns:
            The statement result.
        """
        timeout_seconds = self.timeout_minutes * 60
        start_time = time.time()
        statement_url = f"{self.livy_endpoint}/{session_id}/statements/{statement_id}"

        while time.time() - start_time < timeout_seconds:
            response = requests.get(
                statement_url,
                headers=self._get_headers(),
                timeout=30,
            )

            if response.status_code != 200:
                raise ConfigurationError(f"Failed to get statement status: {response.status_code}")

            statement = response.json()
            state = statement["state"]

            if state == LivyStatementState.AVAILABLE:
                output = statement.get("output", {})
                if output.get("status") == "error":
                    error_value = output.get("evalue", "Unknown error")
                    raise ConfigurationError(f"Statement failed: {error_value}")
                return output
            if state in [LivyStatementState.ERROR, LivyStatementState.CANCELLED]:
                raise ConfigurationError(f"Statement is in {state} state")

            time.sleep(2)

        raise ConfigurationError(f"Timeout waiting for statement {statement_id} after {timeout_seconds}s")

    def create_connection(self, **kwargs: Any) -> Any:
        """Verify Azure connectivity and workspace access.

        Returns:
            Dict with connection status and workspace info.

        Raises:
            ConfigurationError: If Azure connection fails.
        """
        try:
            # Test credential by getting a token
            self._get_access_token()

            # Test workspace access
            workspace_url = f"{self.FABRIC_API_BASE}/workspaces/{self.workspace_id}"
            response = requests.get(
                workspace_url,
                headers=self._get_headers(),
                timeout=30,
            )

            if response.status_code == 200:
                workspace = response.json()
                logger.info(f"Connected to Fabric workspace: {workspace.get('displayName', self.workspace_id)}")
                return {
                    "status": "connected",
                    "workspace_id": self.workspace_id,
                    "workspace_name": workspace.get("displayName"),
                    "lakehouse_id": self.lakehouse_id,
                }
            elif response.status_code == 401:
                raise ConfigurationError(
                    "Authentication failed. Ensure Azure credentials are configured "
                    "(az login, service principal, or managed identity)"
                )
            elif response.status_code == 403:
                raise ConfigurationError(
                    f"Access denied to workspace {self.workspace_id}. Check workspace permissions."
                )
            elif response.status_code == 404:
                raise ConfigurationError(
                    f"Workspace {self.workspace_id} not found. Verify the workspace ID is correct."
                )
            else:
                raise ConfigurationError(f"Failed to access workspace: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            raise ConfigurationError(f"Failed to connect to Fabric: {e}") from e

    def create_schema(self, schema_name: str | None = None) -> None:
        """Create schema in Lakehouse.

        Fabric Lakehouse manages schemas automatically through Delta tables.
        This method ensures the database context is set.

        Args:
            schema_name: Database/schema name.
        """
        database = schema_name or "default"

        # In Fabric Lakehouse, we just need to ensure we can create tables
        # The lakehouse automatically manages the schema
        logger.info(f"Using Lakehouse schema: {database}")

        # Optionally create the database if it doesn't exist
        if database != "default":
            self._execute_statement(
                f"CREATE DATABASE IF NOT EXISTS {database}",
                kind="sql",
            )

    def load_data(
        self,
        tables: list[str],
        source_dir: Path | str,
        file_format: str = "parquet",
        **kwargs: Any,
    ) -> dict[str, str]:
        """Upload benchmark data to OneLake and create Delta tables.

        Args:
            tables: List of table names to load.
            source_dir: Local directory containing table data files.
            file_format: Data file format (default: parquet).
            **kwargs: Additional options.

        Returns:
            Dict mapping table names to OneLake URIs.
        """
        source_path = Path(source_dir)
        if not source_path.exists():
            raise ConfigurationError(f"Source directory not found: {source_dir}")

        # Upload data to OneLake using staging infrastructure
        if self._staging:
            # Check if tables already exist
            if self._staging.tables_exist(tables):
                logger.info("Tables already exist in OneLake, skipping upload")
            else:
                logger.info(f"Uploading {len(tables)} tables to OneLake")
                self._staging.upload_tables(
                    tables=tables,
                    source_dir=source_path,
                    file_format=file_format,
                )

        # Create Delta tables from uploaded data
        table_uris = {}
        for table in tables:
            table_uri = f"{self.onelake_path}/Files/benchbox/tables/{table}"
            table_uris[table] = table_uri

            # Create Delta table from Parquet files
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table}
                USING DELTA
                LOCATION '{table_uri}'
            """
            try:
                self._execute_statement(create_sql, kind="sql")
                logger.debug(f"Created Delta table: {table}")
            except Exception as e:
                logger.warning(f"Failed to create table {table}: {e}")

        return table_uris

    def execute_query(
        self,
        query: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute a SQL query via Livy.

        Args:
            query: SQL query to execute.
            **kwargs: Additional options.

        Returns:
            Dict with query results.
        """
        start_time = time.time()

        result = self._execute_statement(query, kind="sql")

        execution_time = time.time() - start_time

        # Parse result data
        data = result.get("data", {})
        schema = data.get("schema", {})

        return {
            "success": True,
            "execution_time": execution_time,
            "row_count": len(data.get("values", [])),
            "columns": [f.get("name") for f in schema.get("fields", [])],
            "data": data.get("values", []),
        }

    def close(self) -> None:
        """Clean up resources and close Livy session."""
        if self._session_id is not None and self._session_created_by_us:
            try:
                session_url = f"{self.livy_endpoint}/{self._session_id}"
                requests.delete(
                    session_url,
                    headers=self._get_headers(),
                    timeout=30,
                )
                logger.info(f"Closed Livy session: {self._session_id}")
            except Exception as e:
                logger.warning(f"Failed to close session: {e}")
            finally:
                self._session_id = None

    def get_target_dialect(self) -> str:
        """Return the target SQL dialect for Fabric Spark.

        Fabric Spark uses Spark SQL dialect.

        Returns:
            The dialect string "spark".
        """
        return "spark"

    # --- Configuration Methods ---

    def configure_for_benchmark(
        self,
        benchmark: str,
        scale_factor: float | None = None,
        **options: Any,
    ) -> None:
        """Configure adapter for specific benchmark.

        Args:
            benchmark: Benchmark name (tpch, tpcds, ssb).
            scale_factor: Data scale factor.
            **options: Additional benchmark options.
        """
        self._benchmark_type = benchmark.lower()
        self._scale_factor = scale_factor or 1.0

        # Get optimized Spark configuration
        if self._benchmark_type == "tpch":
            config = SparkConfigOptimizer.for_tpch(
                scale_factor=self._scale_factor,
                platform=CloudPlatform.FABRIC,
            )
        elif self._benchmark_type == "tpcds":
            config = SparkConfigOptimizer.for_tpcds(
                scale_factor=self._scale_factor,
                platform=CloudPlatform.FABRIC,
            )
        elif self._benchmark_type == "ssb":
            config = SparkConfigOptimizer.for_ssb(
                scale_factor=self._scale_factor,
                platform=CloudPlatform.FABRIC,
            )
        else:
            # Default to TPC-H config for unknown benchmarks
            config = SparkConfigOptimizer.for_tpch(
                scale_factor=self._scale_factor,
                platform=CloudPlatform.FABRIC,
            )

        # Convert SparkConfig to dict
        self._spark_config = config.to_dict()
        logger.info(f"Configured for {benchmark} at SF={self._scale_factor}")

    def apply_platform_tuning(
        self,
        config: PlatformOptimizationConfiguration,
    ) -> None:
        """Apply platform-specific tuning configuration.

        Args:
            config: Platform optimization configuration.
        """
        # Extract Spark-relevant settings from config
        if hasattr(config, "spark_config") and config.spark_config:
            self._spark_config.update(config.spark_config)

    def apply_constraint_configuration(
        self,
        primary_keys: list[PrimaryKeyConfiguration] | None = None,
        foreign_keys: list[ForeignKeyConfiguration] | None = None,
    ) -> None:
        """Apply constraint configuration (no-op for Spark).

        Spark does not enforce primary/foreign key constraints.

        Args:
            primary_keys: Primary key configurations (ignored).
            foreign_keys: Foreign key configurations (ignored).
        """
        # Spark doesn't enforce constraints, but we log for visibility
        if primary_keys:
            logger.debug(f"Ignoring {len(primary_keys)} primary key constraints (Spark no-op)")
        if foreign_keys:
            logger.debug(f"Ignoring {len(foreign_keys)} foreign key constraints (Spark no-op)")

    def apply_unified_tuning(
        self,
        config: UnifiedTuningConfiguration,
    ) -> None:
        """Apply unified tuning configuration.

        Args:
            config: Unified tuning configuration.
        """
        if hasattr(config, "platform_optimization"):
            self.apply_platform_tuning(config.platform_optimization)

    # apply_primary_keys, apply_foreign_keys, apply_platform_optimizations,
    # and apply_constraint_configuration are inherited from SparkTuningMixin

    # --- CLI Methods ---

    @classmethod
    def add_cli_arguments(cls, parser: Any) -> None:
        """Add Fabric Spark CLI arguments.

        Args:
            parser: Argument parser to add arguments to.
        """
        parser.add_argument(
            "--workspace-id",
            help="Fabric workspace GUID",
            dest="workspace_id",
        )
        parser.add_argument(
            "--lakehouse-id",
            help="Fabric Lakehouse GUID",
            dest="lakehouse_id",
        )
        parser.add_argument(
            "--tenant-id",
            help="Azure tenant ID",
            dest="tenant_id",
        )
        parser.add_argument(
            "--spark-pool",
            help="Spark pool name",
            dest="spark_pool_name",
        )
        parser.add_argument(
            "--timeout",
            type=int,
            default=60,
            help="Statement timeout in minutes (default: 60)",
            dest="timeout_minutes",
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> FabricSparkAdapter:
        """Create adapter from configuration dictionary.

        Args:
            config: Configuration dictionary.

        Returns:
            FabricSparkAdapter instance.
        """
        return cls(
            workspace_id=config.get("workspace_id"),
            lakehouse_id=config.get("lakehouse_id"),
            tenant_id=config.get("tenant_id"),
            livy_endpoint=config.get("livy_endpoint"),
            onelake_path=config.get("onelake_path"),
            spark_pool_name=config.get("spark_pool_name"),
            timeout_minutes=config.get("timeout_minutes", 60),
            spark_config=config.get("spark_config"),
        )
