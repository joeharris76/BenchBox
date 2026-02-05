"""ClickHouse Cloud platform adapter for managed ClickHouse service.

ClickHouse Cloud is the managed cloud version of ClickHouse, providing
serverless and dedicated compute options with automatic scaling.
This adapter inherits from ClickHouseAdapter to reuse all shared logic
(mixins, client code) while implementing cloud-specific defaults.

Authentication:
- Uses environment variables or config file:
  - CLICKHOUSE_CLOUD_HOST: Hostname (e.g., abc123.us-east-2.aws.clickhouse.cloud)
  - CLICKHOUSE_CLOUD_PASSWORD: Password for authentication
  - CLICKHOUSE_CLOUD_USER: Username (default: "default")

Connection:
- Uses clickhouse-connect for HTTPS-based communication (port 8443)
- Supports compression and secure connections by default

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from benchbox.platforms.clickhouse import ClickHouseAdapter

logger = logging.getLogger(__name__)


class ClickHouseCloudAdapter(ClickHouseAdapter):
    """ClickHouse Cloud platform adapter - managed ClickHouse service.

    This adapter enables running benchmarks against ClickHouse Cloud,
    allowing direct comparison with self-hosted ClickHouse performance.

    Authentication:
        Set environment variables, or provide via platform options:
        - CLICKHOUSE_CLOUD_HOST: Cloud hostname
        - CLICKHOUSE_CLOUD_PASSWORD: Authentication password
        - CLICKHOUSE_CLOUD_USER: Username (default: "default")

    Example usage:
        benchbox run --platform clickhouse-cloud --benchmark tpch --scale 0.01

        # With explicit options
        benchbox run --platform clickhouse-cloud --benchmark tpch \\
            --platform-option host=abc123.us-east-2.aws.clickhouse.cloud \\
            --platform-option password=my-password
    """

    def __init__(self, **config):
        """Initialize ClickHouse Cloud adapter.

        Forces deployment_mode to "cloud" and validates credentials upfront.

        Args:
            **config: Configuration options:
                - host: ClickHouse Cloud hostname (or use CLICKHOUSE_CLOUD_HOST env)
                - password: Authentication password (or use CLICKHOUSE_CLOUD_PASSWORD env)
                - username: Username (default: "default", or use CLICKHOUSE_CLOUD_USER env)
                - database: Database name (default: "default")
        """
        # Force cloud deployment mode - this is the key distinction
        config["deployment_mode"] = "cloud"
        # Internal flag to bypass the base adapter's cloud mode rejection
        # (cloud is only valid when called from this subclass)
        config["_is_cloud_subclass"] = True

        # Apply cloud-specific defaults from environment before parent init
        self._apply_cloud_defaults(config)

        # Call parent initialization (handles mixin composition)
        super().__init__(**config)

        # Remove internal flag from config to prevent leakage to other components
        config.pop("_is_cloud_subclass", None)

        logger.info(f"ClickHouse Cloud adapter initialized for host: {self.host}")

    def _apply_cloud_defaults(self, config: dict[str, Any]) -> None:
        """Apply cloud-specific defaults from environment variables.

        Environment variables follow the existing ClickHouse Cloud pattern:
        - CLICKHOUSE_CLOUD_HOST
        - CLICKHOUSE_CLOUD_PASSWORD
        - CLICKHOUSE_CLOUD_USER

        Args:
            config: Configuration dictionary to update with defaults
        """
        # Host from env if not provided
        if "host" not in config or not config["host"]:
            config["host"] = os.environ.get("CLICKHOUSE_CLOUD_HOST")

        # Password from env if not provided
        if "password" not in config or not config["password"]:
            config["password"] = os.environ.get("CLICKHOUSE_CLOUD_PASSWORD")

        # Username with default fallback
        if "username" not in config or not config["username"]:
            config["username"] = os.environ.get("CLICKHOUSE_CLOUD_USER", "default")

    @property
    def platform_name(self) -> str:
        """Return platform display name."""
        return "ClickHouse Cloud"

    @staticmethod
    def add_cli_arguments(parser) -> None:
        """Add ClickHouse Cloud-specific CLI arguments."""
        cloud_group = parser.add_argument_group("ClickHouse Cloud Arguments")
        cloud_group.add_argument(
            "--host",
            type=str,
            help="ClickHouse Cloud hostname (e.g., abc123.us-east-2.aws.clickhouse.cloud)",
        )
        cloud_group.add_argument(
            "--password",
            type=str,
            help="ClickHouse Cloud password (or use CLICKHOUSE_CLOUD_PASSWORD env)",
        )
        cloud_group.add_argument(
            "--username",
            type=str,
            default="default",
            help="Username (default: 'default')",
        )
        cloud_group.add_argument(
            "--database",
            type=str,
            default="default",
            help="Database name (default: 'default')",
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]):
        """Create ClickHouse Cloud adapter from unified configuration.

        Maps CLI arguments and platform options to adapter configuration.

        Args:
            config: Unified configuration dictionary from CLI/orchestrator

        Returns:
            Configured ClickHouseCloudAdapter instance
        """
        adapter_config: dict[str, Any] = {}

        # Map cloud-specific parameters
        for key in ["host", "password", "username", "database"]:
            if key in config and config[key] is not None:
                adapter_config[key] = config[key]

        # Map optional performance settings
        for key in [
            "max_memory_usage",
            "max_execution_time",
            "max_threads",
            "disable_result_cache",
            "compression",
        ]:
            if key in config and config[key] is not None:
                adapter_config[key] = config[key]

        # Pass through benchmark context for potential use
        if "benchmark" in config:
            adapter_config["benchmark"] = config["benchmark"]

        return cls(**adapter_config)

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get ClickHouse Cloud platform information.

        Extends base ClickHouse platform info with cloud-specific details.

        Args:
            connection: Optional active connection

        Returns:
            Dictionary with platform metadata
        """
        info = super().get_platform_info(connection)
        info["platform_type"] = "clickhouse-cloud"
        info["platform_name"] = "ClickHouse Cloud"
        info["connection_mode"] = "cloud"
        info["configuration"]["deployment"] = "managed"
        return info


def _build_clickhouse_cloud_config(
    platform: str,
    options: dict[str, Any],
    overrides: dict[str, Any],
    info: Any,
) -> Any:
    """Build ClickHouse Cloud database configuration with credential loading.

    Args:
        platform: Platform name (should be 'clickhouse-cloud')
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
    saved_creds = cred_manager.get_platform_credentials("clickhouse-cloud") or {}

    # Build merged options: saved_creds < options < overrides
    merged_options = {}
    merged_options.update(saved_creds)
    merged_options.update(options)
    merged_options.update(overrides)

    name = info.display_name if info else "ClickHouse Cloud"
    driver_package = info.driver_package if info else "clickhouse-connect"

    config_dict = {
        "type": "clickhouse-cloud",
        "name": name,
        "options": merged_options or {},
        "driver_package": driver_package,
        "driver_version": overrides.get("driver_version") or options.get("driver_version"),
        "driver_auto_install": bool(overrides.get("driver_auto_install", options.get("driver_auto_install", False))),
        # Platform-specific fields at top-level
        "host": merged_options.get("host"),
        "password": merged_options.get("password"),
        "username": merged_options.get("username"),
        "database": merged_options.get("database"),
        # Optional settings
        "max_memory_usage": merged_options.get("max_memory_usage"),
        "max_execution_time": merged_options.get("max_execution_time"),
        "disable_result_cache": merged_options.get("disable_result_cache"),
        "compression": merged_options.get("compression"),
        # Benchmark context
        "benchmark": overrides.get("benchmark"),
        "scale_factor": overrides.get("scale_factor"),
        "tuning_config": overrides.get("tuning_config"),
    }

    return DatabaseConfig(**config_dict)


# Register the config builder with the platform hook registry
try:
    from benchbox.cli.platform_hooks import PlatformHookRegistry

    PlatformHookRegistry.register_config_builder("clickhouse-cloud", _build_clickhouse_cloud_config)
except ImportError:
    # Platform hooks may not be available in all contexts (e.g., minimal installs, testing)
    logger.debug("Platform hooks not available, skipping ClickHouse Cloud config builder registration")


__all__ = ["ClickHouseCloudAdapter"]
