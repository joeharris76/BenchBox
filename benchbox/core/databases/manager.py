"""
Core database management utilities.

Provides reusable connection testing and platform configuration shaping for
programmatic and CLI consumers, keeping interactive UX in the CLI layer.
"""

from __future__ import annotations

from benchbox.core.config import DatabaseConfig, SystemProfile
from benchbox.core.platform_config import get_platform_config
from benchbox.platforms import get_platform_adapter


def check_connection(database_config: DatabaseConfig, system_profile: SystemProfile | None = None) -> bool:
    """Check database connectivity using the platform adapter.

    Args:
        database_config: Core database configuration
        system_profile: Optional system profile for sizing parameters

    Returns:
        True if connection check succeeds, False otherwise
    """
    platform_cfg = get_platform_config(database_config, system_profile)
    adapter = get_platform_adapter(database_config.type, **platform_cfg)
    return adapter.test_connection()
