"""Shared fixtures for platform integration tests.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import os
import time
from pathlib import Path

import pytest


def get_env_or_skip(var_name: str, platform_name: str) -> str:
    """Get environment variable or skip test if not set.

    Args:
        var_name: Name of environment variable
        platform_name: Name of platform for error message

    Returns:
        Value of environment variable

    Raises:
        pytest.skip: If environment variable is not set
    """
    value = os.getenv(var_name)
    if not value:
        pytest.skip(f"Skipping {platform_name} live test: {var_name} not set")
    assert value is not None  # Type narrowing after skip
    return value


@pytest.fixture(scope="module")
def unique_test_schema() -> str:
    """Generate unique schema name for test isolation.

    Returns:
        Schema name with timestamp suffix
    """
    timestamp = int(time.time())
    return f"benchbox_test_{timestamp}"


@pytest.fixture(scope="module")
def test_scale_factor() -> float:
    """Get scale factor for live tests (defaults to 0.01 for minimal cost).

    Returns:
        Scale factor for benchmark data generation
    """
    return float(os.getenv("BENCHBOX_TEST_SCALE_FACTOR", "0.01"))


@pytest.fixture(scope="module")
def test_output_dir(tmp_path_factory) -> Path:
    """Create temporary output directory for live test data.

    Args:
        tmp_path_factory: pytest fixture for creating temp directories

    Returns:
        Path to temporary output directory
    """
    return tmp_path_factory.mktemp("live_test_output")


# ==============================================================================
# Databricks Fixtures
# ==============================================================================


@pytest.fixture(scope="module")
def databricks_credentials() -> dict[str, str]:
    """Get Databricks credentials from environment.

    Returns:
        Dictionary with Databricks connection parameters

    Raises:
        pytest.skip: If required credentials are not set
    """
    return {
        "server_hostname": get_env_or_skip("DATABRICKS_HOST", "Databricks"),
        "http_path": get_env_or_skip("DATABRICKS_HTTP_PATH", "Databricks"),
        "access_token": get_env_or_skip("DATABRICKS_TOKEN", "Databricks"),
        "catalog": os.getenv("DATABRICKS_CATALOG", "main"),
        "schema": os.getenv("DATABRICKS_SCHEMA", "default"),
    }


@pytest.fixture(scope="module")
def live_databricks_adapter(databricks_credentials):
    """Create Databricks adapter with live credentials.

    Args:
        databricks_credentials: Fixture providing credentials

    Yields:
        DatabricksAdapter instance connected to live workspace
    """
    from benchbox.platforms.databricks import DatabricksAdapter

    adapter = DatabricksAdapter(**databricks_credentials)
    yield adapter
    # No cleanup needed - adapter manages its own connections


# ==============================================================================
# Snowflake Fixtures
# ==============================================================================


@pytest.fixture(scope="module")
def snowflake_credentials() -> dict[str, str | None]:
    """Get Snowflake credentials from environment.

    Returns:
        Dictionary with Snowflake connection parameters

    Raises:
        pytest.skip: If required credentials are not set
    """
    return {
        "account": get_env_or_skip("SNOWFLAKE_ACCOUNT", "Snowflake"),
        "username": get_env_or_skip("SNOWFLAKE_USERNAME", "Snowflake"),
        "password": get_env_or_skip("SNOWFLAKE_PASSWORD", "Snowflake"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "BENCHBOX"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }


@pytest.fixture(scope="module")
def live_snowflake_adapter(snowflake_credentials):
    """Create Snowflake adapter with live credentials.

    Args:
        snowflake_credentials: Fixture providing credentials

    Yields:
        SnowflakeAdapter instance connected to live account
    """
    from benchbox.platforms.snowflake import SnowflakeAdapter

    adapter = SnowflakeAdapter(**snowflake_credentials)
    yield adapter
    # No cleanup needed - adapter manages its own connections


# ==============================================================================
# BigQuery Fixtures
# ==============================================================================


@pytest.fixture(scope="module")
def bigquery_credentials() -> dict[str, str | None]:
    """Get BigQuery credentials from environment.

    Returns:
        Dictionary with BigQuery connection parameters

    Raises:
        pytest.skip: If required credentials are not set
    """
    # Check for service account credentials
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        pytest.skip("Skipping BigQuery live test: GOOGLE_APPLICATION_CREDENTIALS not set")

    return {
        "project_id": get_env_or_skip("BIGQUERY_PROJECT", "BigQuery"),
        "dataset_id": os.getenv("BIGQUERY_DATASET", "benchbox_test"),
        "location": os.getenv("BIGQUERY_LOCATION", "US"),
        "storage_bucket": os.getenv("BIGQUERY_STORAGE_BUCKET"),
    }


@pytest.fixture(scope="module")
def live_bigquery_adapter(bigquery_credentials):
    """Create BigQuery adapter with live credentials.

    Args:
        bigquery_credentials: Fixture providing credentials

    Yields:
        BigQueryAdapter instance connected to live project
    """
    from benchbox.platforms.bigquery import BigQueryAdapter

    adapter = BigQueryAdapter(**bigquery_credentials)
    yield adapter
    # No cleanup needed - adapter manages its own connections


# ==============================================================================
# Common Cleanup Fixtures
# ==============================================================================


@pytest.fixture
def cleanup_test_schema(request):
    """Cleanup fixture to remove test schemas after tests complete.

    Usage:
        def test_something(live_adapter, cleanup_test_schema):
            schema_name = "test_schema"
            cleanup_test_schema(live_adapter, schema_name)
            # ... test code that creates schema ...

    Args:
        request: pytest request object

    Returns:
        Cleanup function that accepts (adapter, schema_name)
    """
    schemas_to_cleanup = []

    def register_cleanup(adapter, schema_name: str):
        """Register a schema for cleanup after test."""
        schemas_to_cleanup.append((adapter, schema_name))

    yield register_cleanup

    # Cleanup all registered schemas
    for adapter, schema_name in schemas_to_cleanup:
        try:
            connection = adapter.create_connection()
            try:
                # Try to drop the schema - implementation varies by platform
                adapter.execute_sql(connection, f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
            except Exception as e:
                # Log but don't fail - cleanup is best-effort
                print(f"Warning: Failed to cleanup schema {schema_name}: {e}")
            finally:
                adapter.close_connection(connection)
        except Exception as e:
            print(f"Warning: Failed to connect for cleanup of schema {schema_name}: {e}")
