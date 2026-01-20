"""Platform configuration inheritance system.

This module provides configuration inheritance for platforms that share SQL dialect,
benchmark compatibility, and data type mappings with parent platforms.

For example:
- MotherDuck inherits from DuckDB (same SQL dialect, same benchmark queries)
- Starburst inherits from Trino (same SQL dialect)
- ClickHouse Cloud inherits from ClickHouse (same SQL dialect)

What inherits:
- SQL dialect identifier (for query translation)
- Benchmark compatibility (which benchmarks/queries are supported)
- Data type mappings (DECIMAL precision, timestamp handling)

What does NOT inherit:
- Connection configuration (auth, endpoints)
- Credential requirements
- Cloud storage settings

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Any, Optional

from benchbox.core.platform_registry import PlatformRegistry


def get_inherited_dialect(platform_name: str) -> str:
    """Get the SQL dialect for a platform, following inheritance chain.

    If a platform has an inherits_from parent, returns the parent's dialect.
    This ensures child platforms (like MotherDuck) use the same SQL dialect
    as their parent (DuckDB).

    Args:
        platform_name: Name of the platform (aliases resolved automatically)

    Returns:
        SQL dialect identifier (e.g., 'duckdb', 'clickhouse', 'trino')
    """
    # Walk up the inheritance chain to find the root platform's dialect
    current = PlatformRegistry.resolve_platform_name(platform_name)
    visited = set()

    while current:
        if current in visited:
            # Circular inheritance detected - break the loop
            break
        visited.add(current)

        # Get the parent platform if any
        parent = PlatformRegistry.get_inherited_platform(current)
        if parent:
            current = parent
        else:
            break

    # Return the root platform name as the dialect
    # (platforms use their name as dialect by convention)
    return current


def get_platform_family_dialect(platform_name: str) -> str:
    """Get the SQL dialect based on platform family.

    Uses the platform_family field to determine dialect. This is useful
    for platforms that are part of a family but don't have direct inheritance
    (e.g., all ClickHouse deployment modes share the clickhouse family).

    Args:
        platform_name: Name of the platform (aliases resolved automatically)

    Returns:
        SQL dialect identifier based on platform family
    """
    canonical = PlatformRegistry.resolve_platform_name(platform_name)
    family = PlatformRegistry.get_platform_family(canonical)

    if family:
        return family

    # No family defined, use inherited dialect
    return get_inherited_dialect(canonical)


def build_inherited_config(
    platform_name: str,
    user_config: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Build configuration with inherited values from parent platform.

    Configuration is merged with this priority (highest to lowest):
    1. User-provided configuration (explicit overrides)
    2. Child platform defaults
    3. Parent platform inherited values

    Args:
        platform_name: Name of the platform
        user_config: User-provided configuration overrides

    Returns:
        Merged configuration dictionary with inherited values
    """
    canonical = PlatformRegistry.resolve_platform_name(platform_name)
    config: dict[str, Any] = {}

    # Get parent platform configuration if any
    parent = PlatformRegistry.get_inherited_platform(canonical)
    if parent:
        parent_caps = PlatformRegistry.get_platform_capabilities(parent)
        if parent_caps:
            # Inherit dialect-related configuration
            config["_inherited_from"] = parent
            config["_inherited_dialect"] = get_inherited_dialect(parent)
            config["_platform_family"] = PlatformRegistry.get_platform_family(parent)

    # Get this platform's capabilities
    caps = PlatformRegistry.get_platform_capabilities(canonical)
    if caps:
        # Apply platform family if defined
        if caps.platform_family:
            config["_platform_family"] = caps.platform_family

    # Apply user configuration (highest priority)
    if user_config:
        config.update(user_config)

    return config


def get_benchmark_compatibility(platform_name: str) -> dict[str, bool]:
    """Get benchmark compatibility for a platform, including inherited support.

    Child platforms inherit benchmark compatibility from their parent.
    For example, MotherDuck supports all benchmarks that DuckDB supports.

    Args:
        platform_name: Name of the platform

    Returns:
        Dictionary mapping benchmark names to support status
    """
    canonical = PlatformRegistry.resolve_platform_name(platform_name)
    compatibility: dict[str, bool] = {}

    # Start with parent's compatibility
    parent = PlatformRegistry.get_inherited_platform(canonical)
    if parent:
        parent_compat = get_benchmark_compatibility(parent)
        compatibility.update(parent_compat)

    # Get this platform's metadata for any overrides
    metadata = PlatformRegistry.get_platform_info(canonical)
    if metadata and hasattr(metadata, "benchmark_compatibility"):
        # Override with platform-specific compatibility
        compatibility.update(getattr(metadata, "benchmark_compatibility", {}))

    return compatibility


def resolve_dialect_for_query_translation(platform_name: str) -> str:
    """Resolve the dialect to use for SQL query translation.

    This is the primary interface for benchmark query translation. It returns
    the dialect identifier that the query translator should use.

    Priority:
    1. Platform family (if defined)
    2. Inherited dialect (walk up inheritance chain)
    3. Platform name (default fallback)

    Args:
        platform_name: Name of the platform

    Returns:
        Dialect identifier for query translation
    """
    canonical = PlatformRegistry.resolve_platform_name(platform_name)

    # Check platform family first
    caps = PlatformRegistry.get_platform_capabilities(canonical)
    if caps and caps.platform_family:
        return caps.platform_family

    # Check for inherited dialect
    parent = PlatformRegistry.get_inherited_platform(canonical)
    if parent:
        return resolve_dialect_for_query_translation(parent)

    # Default to platform name
    return canonical
