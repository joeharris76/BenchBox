"""Onehouse API client for Quanton managed Spark.

Provides HTTP client for interacting with Onehouse Quanton API for:
- Job submission and management
- Cluster lifecycle
- Status polling and result retrieval

Onehouse Quanton is a serverless managed Spark compute runtime with:
- 2-3x better price-performance vs AWS EMR and Databricks
- Multi-table-format support: Apache Hudi, Apache Iceberg, Delta Lake
- Apache XTable integration for cross-format metadata translation
- Serverless architecture with intelligent cluster management

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from benchbox.core.exceptions import ConfigurationError
from benchbox.utils.dependencies import (
    check_platform_dependencies,
    get_dependency_error_message,
)

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    HTTPAdapter = None
    Retry = None
    REQUESTS_AVAILABLE = False

logger = logging.getLogger(__name__)


class JobState(str, Enum):
    """Quanton job state constants."""

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    CANCELLING = "CANCELLING"

    @property
    def is_terminal(self) -> bool:
        """Check if job state is terminal (no further transitions)."""
        return self in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED}

    @property
    def is_success(self) -> bool:
        """Check if job completed successfully."""
        return self == JobState.SUCCEEDED


class TableFormat(str, Enum):
    """Supported table formats for Quanton."""

    HUDI = "hudi"
    ICEBERG = "iceberg"
    DELTA = "delta"


@dataclass
class JobResult:
    """Result of a Quanton job execution."""

    job_id: str
    state: JobState
    duration_seconds: float | None = None
    error_message: str | None = None
    output_location: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class ClusterConfig:
    """Quanton cluster configuration."""

    cluster_size: str = "small"  # small, medium, large, xlarge
    spark_version: str = "3.5"
    auto_scaling: bool = True
    min_workers: int = 1
    max_workers: int = 10
    idle_timeout_minutes: int = 15


class OnehouseClient:
    """HTTP client for Onehouse Quanton API.

    Provides methods for:
    - Authentication and session management
    - Job submission and status polling
    - Cluster management
    - Result retrieval

    Usage:
        client = OnehouseClient(
            api_key="your-api-key",
            region="us-east-1",
        )

        # Submit a Spark SQL job
        job_id = client.submit_sql_job(
            sql="SELECT * FROM my_table LIMIT 10",
            database="my_database",
        )

        # Wait for completion
        result = client.wait_for_job(job_id)
    """

    DEFAULT_API_ENDPOINT = "api.onehouse.ai"
    DEFAULT_TIMEOUT_SECONDS = 30
    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 0.5

    def __init__(
        self,
        api_key: str | None = None,
        region: str = "us-east-1",
        api_endpoint: str | None = None,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        cluster_config: ClusterConfig | None = None,
    ) -> None:
        """Initialize the Onehouse client.

        Args:
            api_key: Onehouse API key for authentication.
            region: AWS region for cluster deployment.
            api_endpoint: Custom API endpoint (for testing or private deployments).
            timeout_seconds: Request timeout in seconds.
            cluster_config: Cluster configuration for job execution.
        """
        if not REQUESTS_AVAILABLE:
            deps_satisfied, missing = check_platform_dependencies("quanton")
            if not deps_satisfied:
                raise ConfigurationError(get_dependency_error_message("quanton", missing))

        self.api_key = api_key
        self.region = region
        self.api_endpoint = api_endpoint or self.DEFAULT_API_ENDPOINT
        self.timeout_seconds = timeout_seconds
        self.cluster_config = cluster_config or ClusterConfig()

        self._session: Any = None
        self._cluster_id: str | None = None

    def _get_session(self) -> Any:
        """Get or create HTTP session with retry configuration."""
        if self._session is None:
            self._session = requests.Session()

            # Configure retry strategy
            retry_strategy = Retry(
                total=self.MAX_RETRIES,
                backoff_factor=self.RETRY_BACKOFF_FACTOR,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET", "POST", "PUT", "DELETE"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._session.mount("https://", adapter)
            self._session.mount("http://", adapter)

            # Set default headers
            self._session.headers.update(
                {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": "BenchBox/1.0 (Onehouse Quanton Adapter)",
                }
            )

            if self.api_key:
                self._session.headers["Authorization"] = f"Bearer {self.api_key}"

        return self._session

    def _build_url(self, path: str) -> str:
        """Build full API URL from path."""
        base = f"https://{self.api_endpoint}"
        return f"{base}/v1/{path.lstrip('/')}"

    def _request(
        self,
        method: str,
        path: str,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make authenticated API request.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API path
            data: Request body data
            params: Query parameters

        Returns:
            Response JSON as dict

        Raises:
            ConfigurationError: On authentication or configuration errors
            RuntimeError: On API errors
        """
        session = self._get_session()
        url = self._build_url(path)

        try:
            response = session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                timeout=self.timeout_seconds,
            )

            if response.status_code == 401:
                raise ConfigurationError("Invalid Onehouse API key")
            if response.status_code == 403:
                raise ConfigurationError("Insufficient permissions for this operation")

            response.raise_for_status()
            return response.json() if response.content else {}

        except requests.exceptions.ConnectionError as e:
            raise ConfigurationError(f"Failed to connect to Onehouse API: {e}") from e
        except requests.exceptions.Timeout as e:
            raise RuntimeError(f"Request timeout: {e}") from e
        except requests.exceptions.HTTPError as e:
            error_detail = ""
            try:
                error_detail = e.response.json().get("message", str(e))
            except Exception:
                error_detail = str(e)
            raise RuntimeError(f"API error: {error_detail}") from e

    def test_connection(self) -> bool:
        """Test API connectivity and authentication.

        Returns:
            True if connection and authentication successful
        """
        try:
            self._request("GET", "/health")
            return True
        except Exception as e:
            logger.warning(f"Connection test failed: {e}")
            return False

    def get_cluster_status(self) -> dict[str, Any]:
        """Get current cluster status.

        Returns:
            Dict with cluster state information
        """
        if not self._cluster_id:
            return {"status": "no_cluster", "message": "No cluster provisioned"}

        return self._request("GET", f"/clusters/{self._cluster_id}")

    def provision_cluster(self) -> str:
        """Provision a new Quanton cluster.

        Returns:
            Cluster ID
        """
        data = {
            "region": self.region,
            "size": self.cluster_config.cluster_size,
            "spark_version": self.cluster_config.spark_version,
            "auto_scaling": {
                "enabled": self.cluster_config.auto_scaling,
                "min_workers": self.cluster_config.min_workers,
                "max_workers": self.cluster_config.max_workers,
            },
            "idle_timeout_minutes": self.cluster_config.idle_timeout_minutes,
        }

        response = self._request("POST", "/clusters", data=data)
        cluster_id = response.get("cluster_id")
        if not cluster_id:
            raise RuntimeError("API response missing cluster_id")
        self._cluster_id = cluster_id
        logger.info(f"Provisioned Quanton cluster: {self._cluster_id}")
        return cluster_id

    def terminate_cluster(self) -> None:
        """Terminate the current cluster."""
        if self._cluster_id:
            try:
                self._request("DELETE", f"/clusters/{self._cluster_id}")
                logger.info(f"Terminated cluster: {self._cluster_id}")
            except Exception as e:
                logger.warning(f"Failed to terminate cluster: {e}")
            finally:
                self._cluster_id = None

    def submit_sql_job(
        self,
        sql: str,
        database: str,
        table_format: TableFormat = TableFormat.ICEBERG,
        output_location: str | None = None,
        spark_config: dict[str, str] | None = None,
        job_name: str | None = None,
    ) -> str:
        """Submit a Spark SQL job for execution.

        Args:
            sql: SQL query to execute
            database: Database name
            table_format: Table format (hudi, iceberg, delta)
            output_location: S3 location for results (optional)
            spark_config: Additional Spark configuration
            job_name: Optional job name for tracking

        Returns:
            Job ID for tracking
        """
        job_id = job_name or f"benchbox-{uuid.uuid4().hex[:12]}"

        data = {
            "job_id": job_id,
            "job_type": "sql",
            "database": database,
            "sql": sql,
            "table_format": table_format.value,
            "spark_config": spark_config or {},
        }

        if output_location:
            data["output_location"] = output_location

        if self._cluster_id:
            data["cluster_id"] = self._cluster_id

        response = self._request("POST", "/jobs", data=data)
        submitted_job_id = response.get("job_id", job_id)
        logger.debug(f"Submitted SQL job: {submitted_job_id}")
        return submitted_job_id

    def submit_pyspark_job(
        self,
        script: str,
        script_args: list[str] | None = None,
        database: str | None = None,
        table_format: TableFormat = TableFormat.ICEBERG,
        output_location: str | None = None,
        spark_config: dict[str, str] | None = None,
        job_name: str | None = None,
    ) -> str:
        """Submit a PySpark script job for execution.

        Args:
            script: PySpark script content
            script_args: Command-line arguments for script
            database: Database name
            table_format: Table format (hudi, iceberg, delta)
            output_location: S3 location for results
            spark_config: Additional Spark configuration
            job_name: Optional job name

        Returns:
            Job ID for tracking
        """
        job_id = job_name or f"benchbox-{uuid.uuid4().hex[:12]}"

        data = {
            "job_id": job_id,
            "job_type": "pyspark",
            "script": script,
            "script_args": script_args or [],
            "table_format": table_format.value,
            "spark_config": spark_config or {},
        }

        if database:
            data["database"] = database
        if output_location:
            data["output_location"] = output_location
        if self._cluster_id:
            data["cluster_id"] = self._cluster_id

        response = self._request("POST", "/jobs", data=data)
        submitted_job_id = response.get("job_id", job_id)
        logger.debug(f"Submitted PySpark job: {submitted_job_id}")
        return submitted_job_id

    def get_job_status(self, job_id: str) -> JobResult:
        """Get status of a job.

        Args:
            job_id: Job ID to check

        Returns:
            JobResult with current state
        """
        response = self._request("GET", f"/jobs/{job_id}")

        state_str = response.get("state", "PENDING")
        try:
            state = JobState(state_str.upper())
        except ValueError:
            state = JobState.PENDING

        return JobResult(
            job_id=job_id,
            state=state,
            duration_seconds=response.get("duration_seconds"),
            error_message=response.get("error_message"),
            output_location=response.get("output_location"),
            metrics=response.get("metrics", {}),
        )

    def wait_for_job(
        self,
        job_id: str,
        timeout_minutes: int = 60,
        poll_interval_seconds: int = 5,
    ) -> JobResult:
        """Wait for a job to complete.

        Args:
            job_id: Job ID to wait for
            timeout_minutes: Maximum wait time
            poll_interval_seconds: Interval between status checks

        Returns:
            Final JobResult

        Raises:
            RuntimeError: If job fails or times out
        """
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            result = self.get_job_status(job_id)

            if result.state.is_terminal:
                if result.state.is_success:
                    logger.info(f"Job {job_id} succeeded in {result.duration_seconds:.1f}s")
                    return result
                else:
                    error_msg = result.error_message or f"Job failed with state: {result.state}"
                    raise RuntimeError(f"Job {job_id} failed: {error_msg}")

            logger.debug(f"Job {job_id} state: {result.state}")
            time.sleep(poll_interval_seconds)

        raise RuntimeError(f"Job {job_id} timed out after {timeout_minutes} minutes")

    def cancel_job(self, job_id: str) -> None:
        """Cancel a running job.

        Args:
            job_id: Job ID to cancel
        """
        self._request("POST", f"/jobs/{job_id}/cancel")
        logger.info(f"Cancelled job: {job_id}")

    def get_job_results(self, job_id: str) -> list[dict[str, Any]]:
        """Retrieve job results.

        Args:
            job_id: Job ID

        Returns:
            List of result rows as dicts
        """
        response = self._request("GET", f"/jobs/{job_id}/results")
        return response.get("rows", [])

    def create_database(self, database: str, location: str | None = None) -> None:
        """Create a database in the Quanton metastore.

        Args:
            database: Database name
            location: S3 location for database storage
        """
        data = {"database": database}
        if location:
            data["location"] = location

        self._request("POST", "/databases", data=data)
        logger.info(f"Created database: {database}")

    def close(self) -> None:
        """Close the client and clean up resources."""
        self.terminate_cluster()
        if self._session:
            self._session.close()
            self._session = None
