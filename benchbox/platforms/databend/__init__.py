"""Databend cloud-native OLAP platform adapter.

Provides Databend-specific optimizations for cloud-native analytical workloads,
using Snowflake-compatible SQL dialect via sqlglot for query translation.

Deployment Modes:
- Cloud: Databend Cloud managed service (requires credentials)
- Self-hosted: User-managed Databend cluster with object storage backend

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from typing import Any, Optional

from benchbox.platforms.databend.adapter import DatabendAdapter


def _build_databend_config(
    platform: str,
    options: dict[str, Any],
    overrides: dict[str, Any],
    info: Optional[Any],
) -> Any:
    """Build Databend database configuration with credential loading.

    Args:
        platform: Platform name (should be 'databend')
        options: CLI platform options from --platform-option flags
        overrides: Runtime overrides from orchestrator
        info: Platform info from registry

    Returns:
        DatabaseConfig with credentials loaded
    """
    from benchbox.core.schemas import DatabaseConfig
    from benchbox.security.credentials import CredentialManager

    # Load saved credentials
    cred_manager = CredentialManager()
    saved_creds = cred_manager.get_platform_credentials("databend") or {}

    # Build merged options: saved_creds < options < overrides
    merged_options = {}
    merged_options.update(saved_creds)
    merged_options.update(options)
    merged_options.update(overrides)

    name = info.display_name if info else "Databend"
    driver_package = info.driver_package if info else "databend-driver"

    config_dict = {
        "type": "databend",
        "name": name,
        "options": merged_options or {},
        "driver_package": driver_package,
        "driver_version": overrides.get("driver_version") or options.get("driver_version"),
        "driver_auto_install": bool(overrides.get("driver_auto_install", options.get("driver_auto_install", False))),
        # Platform-specific fields
        "host": merged_options.get("host"),
        "port": merged_options.get("port"),
        "username": merged_options.get("username"),
        "password": merged_options.get("password"),
        "database": merged_options.get("database"),
        "dsn": merged_options.get("dsn"),
        "warehouse": merged_options.get("warehouse"),
        "ssl": merged_options.get("ssl"),
        "disable_result_cache": merged_options.get("disable_result_cache"),
        # Benchmark context
        "benchmark": overrides.get("benchmark"),
        "scale_factor": overrides.get("scale_factor"),
        "tuning_config": overrides.get("tuning_config"),
    }

    return DatabaseConfig(**config_dict)


# NOTE: Registration of the config builder is done in benchbox/platforms/__init__.py
# via _make_lazy_config_builder(), not here, to avoid circular import issues.

__all__ = ["DatabendAdapter", "_build_databend_config"]
