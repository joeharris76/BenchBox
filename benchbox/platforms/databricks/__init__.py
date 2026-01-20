"""Databricks platform support for BenchBox.

Provides credential management and setup utilities for Databricks
SQL Warehouse connections.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from benchbox.core.config import DatabaseConfig
    from benchbox.platforms.base.models import PlatformInfo

# Import and re-export the Databricks adapters
# Import and re-export dependency checking utilities
from benchbox.utils.dependencies import check_platform_dependencies, get_dependency_error_message

from .adapter import DatabricksAdapter
from .dataframe_adapter import DATABRICKS_CONNECT_AVAILABLE, DatabricksDataFrameAdapter

__all__ = [
    "DatabricksAdapter",
    "DatabricksDataFrameAdapter",
    "DATABRICKS_CONNECT_AVAILABLE",
    "check_platform_dependencies",
    "get_dependency_error_message",
]


def _build_databricks_config(
    platform: str,
    options: dict[str, Any],
    overrides: dict[str, Any],
    info: Optional["PlatformInfo"],
) -> "DatabaseConfig":
    """Build Databricks database configuration with credential loading.

    This function loads saved credentials from the CredentialManager and
    merges them with CLI options and runtime overrides.

    Args:
        platform: Platform name (should be 'databricks')
        options: CLI platform options from --platform-option flags
        overrides: Runtime overrides from orchestrator
        info: Platform info from registry

    Returns:
        DatabaseConfig with credentials loaded and platform-specific fields at top-level
    """
    from benchbox.core.config import DatabaseConfig
    from benchbox.security.credentials import CredentialManager

    # Load saved credentials
    cred_manager = CredentialManager()
    saved_creds = cred_manager.get_platform_credentials("databricks") or {}

    # Build merged options: saved_creds < options < overrides
    merged_options = {}
    merged_options.update(saved_creds)
    merged_options.update(options)
    merged_options.update(overrides)

    # Extract credential fields for DatabaseConfig
    name = info.display_name if info else "Databricks"
    driver_package = info.driver_package if info else "databricks-sql-connector"

    # Build config dict with platform-specific fields at top-level
    # This allows DatabricksAdapter.__init__() to access them via config.get()
    config_dict = {
        "type": "databricks",
        "name": name,
        "options": merged_options or {},  # Ensure options is never None (Pydantic v2 uses None if explicitly passed)
        "driver_package": driver_package,
        "driver_version": overrides.get("driver_version") or options.get("driver_version"),
        "driver_auto_install": bool(overrides.get("driver_auto_install", options.get("driver_auto_install", False))),
        # Platform-specific fields at top-level (adapters expect these here)
        "server_hostname": merged_options.get("server_hostname"),
        "http_path": merged_options.get("http_path"),
        "access_token": merged_options.get("access_token"),
        "catalog": merged_options.get("catalog"),
        "schema": merged_options.get("schema"),
        "uc_catalog": merged_options.get("uc_catalog"),
        "uc_schema": merged_options.get("uc_schema"),
        "uc_volume": merged_options.get("uc_volume"),
        "staging_root": merged_options.get("staging_root"),
    }

    return DatabaseConfig(**config_dict)


# NOTE: Registration of the config builder is done in benchbox/platforms/__init__.py
# after all imports are complete, to avoid circular import issues with the
# databricks package structure.
