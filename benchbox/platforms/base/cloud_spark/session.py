"""Cloud Spark session management for managed Spark platforms.

Provides unified session lifecycle management across cloud Spark platforms:
- AWS EMR, EMR Serverless, Glue
- GCP Dataproc, Dataproc Serverless
- Azure Synapse Spark, Fabric Spark
- Databricks (uses Databricks Connect)

Session Protocols:
- Livy REST API (EMR, Dataproc, Synapse)
- Spark Connect (newer platforms, Databricks)
- Native SDK (Glue, Serverless platforms)

Usage:
    from benchbox.platforms.base.cloud_spark import CloudSparkSessionManager

    # Create session manager for EMR
    manager = CloudSparkSessionManager.for_emr(
        cluster_id="j-XXXXXXXXXXXXX",
        region="us-east-1",
    )

    # Create session and run query
    with manager.session() as spark:
        result = spark.sql("SELECT * FROM lineitem LIMIT 10")
        print(result.collect())

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SessionProtocol(Enum):
    """Supported session protocols for remote Spark."""

    LIVY = "livy"  # Livy REST API (EMR, Dataproc, Synapse)
    SPARK_CONNECT = "spark_connect"  # Spark Connect protocol
    DATABRICKS_CONNECT = "databricks_connect"  # Databricks Connect
    NATIVE_SDK = "native_sdk"  # Platform-native SDK (Glue, Serverless)


class SessionState(Enum):
    """Session lifecycle states."""

    NOT_STARTED = "not_started"
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"


@dataclass
class SessionConfig:
    """Configuration for cloud Spark sessions."""

    # Connection settings
    protocol: SessionProtocol
    endpoint: str  # API endpoint or cluster address
    port: int = 443

    # Authentication
    credentials: dict[str, Any] = field(default_factory=dict)

    # Session settings
    session_name: str = "benchbox-session"
    spark_version: str | None = None
    driver_memory: str = "4g"
    executor_memory: str = "4g"
    executor_cores: int = 2
    num_executors: int | None = None  # None = auto-scale

    # Spark configuration
    spark_conf: dict[str, str] = field(default_factory=dict)

    # Timeouts
    session_start_timeout: int = 300  # seconds
    statement_timeout: int = 3600  # 1 hour default
    idle_timeout: int = 600  # 10 minutes

    # Cost tracking
    track_cost: bool = True
    cost_unit: str = "DBU"  # DBU, CU, slot-hours, etc.


@dataclass
class SessionMetrics:
    """Metrics collected during session lifecycle."""

    session_id: str | None = None
    start_time: float | None = None
    end_time: float | None = None
    statements_executed: int = 0
    bytes_scanned: int = 0
    bytes_shuffled: int = 0
    cost_units: float = 0.0

    @property
    def duration_seconds(self) -> float:
        """Calculate session duration in seconds."""
        if self.start_time is None:
            return 0.0
        end = self.end_time or time.time()
        return end - self.start_time


class CloudSparkSessionManager(ABC):
    """Abstract base class for cloud Spark session management.

    Provides a unified interface for creating and managing Spark sessions
    across different cloud platforms and protocols.
    """

    def __init__(self, config: SessionConfig) -> None:
        """Initialize session manager.

        Args:
            config: Session configuration
        """
        self.config = config
        self._session: Any = None
        self._state = SessionState.NOT_STARTED
        self._metrics = SessionMetrics()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @classmethod
    def for_emr(
        cls,
        cluster_id: str,
        region: str = "us-east-1",
        **kwargs: Any,
    ) -> CloudSparkSessionManager:
        """Create session manager for AWS EMR.

        Args:
            cluster_id: EMR cluster ID (j-XXXXXXXXXXXXX)
            region: AWS region
            **kwargs: Additional configuration

        Returns:
            EMR session manager
        """
        config = SessionConfig(
            protocol=SessionProtocol.LIVY,
            endpoint=f"https://{cluster_id}.emr.{region}.amazonaws.com",
            credentials={"region": region},
            **kwargs,
        )
        return LivySessionManager(config)

    @classmethod
    def for_dataproc(
        cls,
        project_id: str,
        region: str,
        cluster_name: str,
        **kwargs: Any,
    ) -> CloudSparkSessionManager:
        """Create session manager for GCP Dataproc.

        Args:
            project_id: GCP project ID
            region: GCP region
            cluster_name: Dataproc cluster name
            **kwargs: Additional configuration

        Returns:
            Dataproc session manager
        """
        config = SessionConfig(
            protocol=SessionProtocol.LIVY,
            endpoint=f"https://{cluster_name}-m.{region}.c.{project_id}.internal:8998",
            credentials={"project_id": project_id, "region": region},
            **kwargs,
        )
        return LivySessionManager(config)

    @classmethod
    def for_synapse(
        cls,
        workspace_name: str,
        spark_pool_name: str,
        **kwargs: Any,
    ) -> CloudSparkSessionManager:
        """Create session manager for Azure Synapse Spark.

        Args:
            workspace_name: Synapse workspace name
            spark_pool_name: Spark pool name
            **kwargs: Additional configuration

        Returns:
            Synapse session manager
        """
        config = SessionConfig(
            protocol=SessionProtocol.LIVY,
            endpoint=f"https://{workspace_name}.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/{spark_pool_name}",
            **kwargs,
        )
        return LivySessionManager(config)

    @classmethod
    def for_databricks(
        cls,
        host: str,
        cluster_id: str,
        token: str,
        **kwargs: Any,
    ) -> CloudSparkSessionManager:
        """Create session manager for Databricks Connect.

        Args:
            host: Databricks workspace URL
            cluster_id: Cluster ID
            token: Access token
            **kwargs: Additional configuration

        Returns:
            Databricks Connect session manager
        """
        config = SessionConfig(
            protocol=SessionProtocol.DATABRICKS_CONNECT,
            endpoint=host,
            credentials={"token": token, "cluster_id": cluster_id},
            **kwargs,
        )
        return DatabricksConnectSessionManager(config)

    @property
    def state(self) -> SessionState:
        """Get current session state."""
        return self._state

    @property
    def metrics(self) -> SessionMetrics:
        """Get session metrics."""
        return self._metrics

    @property
    def is_active(self) -> bool:
        """Check if session is active and usable."""
        return self._state in (SessionState.IDLE, SessionState.BUSY)

    @abstractmethod
    def create_session(self) -> Any:
        """Create a new Spark session.

        Returns:
            SparkSession or equivalent object
        """

    @abstractmethod
    def get_session(self) -> Any:
        """Get or create the Spark session.

        Returns:
            SparkSession or equivalent object
        """

    @abstractmethod
    def close_session(self) -> None:
        """Close the Spark session and release resources."""

    @abstractmethod
    def execute_statement(self, code: str) -> dict[str, Any]:
        """Execute a code statement in the session.

        Args:
            code: Python or SQL code to execute

        Returns:
            Execution result with output and status
        """

    @abstractmethod
    def get_session_info(self) -> dict[str, Any]:
        """Get session information and status.

        Returns:
            Session info including state, resources, etc.
        """

    @contextmanager
    def session(self) -> Iterator[Any]:
        """Context manager for session lifecycle.

        Yields:
            SparkSession or equivalent object

        Example:
            with manager.session() as spark:
                result = spark.sql("SELECT * FROM table")
        """
        try:
            spark = self.get_session()
            self._metrics.start_time = time.time()
            yield spark
        finally:
            self._metrics.end_time = time.time()
            self.close_session()


class LivySessionManager(CloudSparkSessionManager):
    """Livy REST API session manager for EMR, Dataproc, Synapse."""

    def __init__(self, config: SessionConfig) -> None:
        super().__init__(config)
        self._session_id: int | None = None
        self._http_client: Any = None

    def _get_http_client(self) -> Any:
        """Get or create HTTP client for Livy API."""
        if self._http_client is None:
            try:
                import requests
            except ImportError as e:
                raise ImportError("requests required for Livy API. Install with: uv add requests") from e
            self._http_client = requests.Session()
        return self._http_client

    def _livy_request(
        self,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make a request to Livy API.

        Args:
            method: HTTP method
            path: API path
            json: Request body

        Returns:
            Response JSON
        """
        client = self._get_http_client()
        url = f"{self.config.endpoint}{path}"

        headers = {"Content-Type": "application/json"}

        response = client.request(method, url, json=json, headers=headers)
        response.raise_for_status()

        return response.json()

    def create_session(self) -> Any:
        """Create a new Livy session."""
        self._state = SessionState.STARTING
        self._logger.info("Creating Livy session...")

        # Build session config
        session_conf = {
            "kind": "pyspark",
            "name": self.config.session_name,
            "driverMemory": self.config.driver_memory,
            "executorMemory": self.config.executor_memory,
            "executorCores": self.config.executor_cores,
        }

        if self.config.num_executors:
            session_conf["numExecutors"] = self.config.num_executors

        if self.config.spark_conf:
            session_conf["conf"] = self.config.spark_conf

        # Create session
        response = self._livy_request("POST", "/sessions", json=session_conf)
        self._session_id = response["id"]
        self._metrics.session_id = str(self._session_id)

        # Wait for session to be ready
        self._wait_for_session_ready()

        self._state = SessionState.IDLE
        self._logger.info(f"Livy session {self._session_id} ready")

        return self._session_id

    def _wait_for_session_ready(self) -> None:
        """Wait for Livy session to reach idle state."""
        start = time.time()
        timeout = self.config.session_start_timeout

        while time.time() - start < timeout:
            info = self._livy_request("GET", f"/sessions/{self._session_id}")
            state = info.get("state", "unknown")

            if state == "idle":
                return
            elif state in ("dead", "error", "killed"):
                raise RuntimeError(f"Livy session failed: {state}")

            time.sleep(5)

        raise TimeoutError(f"Session start timeout after {timeout}s")

    def get_session(self) -> Any:
        """Get or create Livy session."""
        if self._session_id is None:
            self.create_session()
        return self._session_id

    def close_session(self) -> None:
        """Close Livy session."""
        if self._session_id is not None:
            self._state = SessionState.SHUTTING_DOWN
            try:
                self._livy_request("DELETE", f"/sessions/{self._session_id}")
                self._logger.info(f"Livy session {self._session_id} closed")
            except Exception as e:
                self._logger.warning(f"Error closing session: {e}")
            finally:
                self._session_id = None
                self._state = SessionState.DEAD

    def execute_statement(self, code: str) -> dict[str, Any]:
        """Execute code in Livy session."""
        if self._session_id is None:
            raise RuntimeError("No active session")

        self._state = SessionState.BUSY

        # Submit statement
        response = self._livy_request(
            "POST",
            f"/sessions/{self._session_id}/statements",
            json={"code": code},
        )
        statement_id = response["id"]

        # Wait for completion
        result = self._wait_for_statement(statement_id)

        self._metrics.statements_executed += 1
        self._state = SessionState.IDLE

        return result

    def _wait_for_statement(self, statement_id: int) -> dict[str, Any]:
        """Wait for statement execution to complete."""
        start = time.time()
        timeout = self.config.statement_timeout

        while time.time() - start < timeout:
            response = self._livy_request(
                "GET",
                f"/sessions/{self._session_id}/statements/{statement_id}",
            )
            state = response.get("state", "waiting")

            if state == "available":
                return response.get("output", {})
            elif state in ("error", "cancelled"):
                raise RuntimeError(f"Statement failed: {response}")

            time.sleep(1)

        raise TimeoutError(f"Statement timeout after {timeout}s")

    def get_session_info(self) -> dict[str, Any]:
        """Get Livy session information."""
        if self._session_id is None:
            return {"state": "not_started"}
        return self._livy_request("GET", f"/sessions/{self._session_id}")


class DatabricksConnectSessionManager(CloudSparkSessionManager):
    """Databricks Connect session manager."""

    def __init__(self, config: SessionConfig) -> None:
        super().__init__(config)
        self._spark: Any = None

    def create_session(self) -> SparkSession:
        """Create Databricks Connect session."""
        try:
            from databricks.connect import DatabricksSession
        except ImportError as e:
            raise ImportError("databricks-connect required. Install with: uv add databricks-connect") from e

        self._state = SessionState.STARTING
        self._logger.info("Creating Databricks Connect session...")

        builder = DatabricksSession.builder
        builder = builder.host(self.config.endpoint)

        if "token" in self.config.credentials:
            builder = builder.token(self.config.credentials["token"])
        if "cluster_id" in self.config.credentials:
            builder = builder.clusterId(self.config.credentials["cluster_id"])

        self._spark = builder.getOrCreate()
        self._state = SessionState.IDLE
        self._metrics.session_id = "databricks-connect"
        self._metrics.start_time = time.time()

        self._logger.info("Databricks Connect session ready")
        return self._spark

    def get_session(self) -> SparkSession:
        """Get or create Databricks Connect session."""
        if self._spark is None:
            self.create_session()
        return self._spark

    def close_session(self) -> None:
        """Close Databricks Connect session."""
        if self._spark is not None:
            self._state = SessionState.SHUTTING_DOWN
            try:
                self._spark.stop()
                self._logger.info("Databricks Connect session stopped")
            except Exception as e:
                self._logger.warning(f"Error stopping session: {e}")
            finally:
                self._spark = None
                self._state = SessionState.DEAD
                self._metrics.end_time = time.time()

    def execute_statement(self, code: str) -> dict[str, Any]:
        """Execute SQL code in Databricks Connect session.

        Args:
            code: SQL statement to execute (SELECT, CREATE, INSERT, DROP, ALTER, USE, SHOW, DESCRIBE).

        Returns:
            Dict with execution results.

        Raises:
            RuntimeError: If no active session or if code is not valid SQL.

        Note:
            Only SQL execution is supported for security reasons.
            Arbitrary Python code execution is not permitted.
        """
        if self._spark is None:
            raise RuntimeError("No active session")

        self._state = SessionState.BUSY

        # Only allow SQL execution for security - no arbitrary code execution
        sql_prefixes = (
            "SELECT",
            "CREATE",
            "INSERT",
            "DROP",
            "ALTER",
            "USE",
            "SHOW",
            "DESCRIBE",
            "EXPLAIN",
            "SET",
            "WITH",
            "MERGE",
            "UPDATE",
            "DELETE",
            "TRUNCATE",
        )
        code_upper = code.strip().upper()
        if not any(code_upper.startswith(prefix) for prefix in sql_prefixes):
            self._state = SessionState.IDLE
            raise RuntimeError(
                f"Only SQL statements are supported. Code must start with one of: {', '.join(sql_prefixes)}"
            )

        result = self._spark.sql(code).collect()
        output = {"data": [list(row) for row in result]}

        self._metrics.statements_executed += 1
        self._state = SessionState.IDLE

        return output

    def get_session_info(self) -> dict[str, Any]:
        """Get Databricks Connect session information."""
        if self._spark is None:
            return {"state": "not_started"}

        return {
            "state": self._state.value,
            "session_id": self._metrics.session_id,
            "protocol": "databricks_connect",
            "spark_version": self._spark.version if self._spark else None,
        }
