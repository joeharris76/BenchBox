"""Format capability detection and platform compatibility mappings.

This module defines which table formats are supported by each platform and provides
utilities for format selection and capability queries.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SupportLevel(Enum):
    """Level of support for a table format on a platform."""

    NATIVE = "native"  # Full native support
    EXTENSION = "extension"  # Requires extension/plugin
    EXPERIMENTAL = "experimental"  # Experimental support, may be unstable
    NOT_SUPPORTED = "not_supported"  # Format not supported


@dataclass(frozen=True)
class FormatCapability:
    """Capability information for a table format.

    Attributes:
        format_name: Name of the format (e.g., 'parquet', 'delta', 'iceberg')
        display_name: Human-readable name (e.g., 'Apache Parquet', 'Delta Lake')
        file_extension: File extension or directory marker
        features: Set of supported features
        supported_platforms: Dict mapping platform name to support level
    """

    format_name: str
    display_name: str
    file_extension: str
    features: set[str]
    supported_platforms: dict[str, SupportLevel]


# Define format capabilities
PARQUET_CAPABILITY = FormatCapability(
    format_name="parquet",
    display_name="Apache Parquet",
    file_extension=".parquet",
    features={
        "predicate_pushdown",
        "column_pruning",
        "partition_pruning",
        "compression",
        "statistics",
    },
    supported_platforms={
        "duckdb": SupportLevel.NATIVE,
        "datafusion": SupportLevel.NATIVE,
        "clickhouse": SupportLevel.NATIVE,
        "databricks": SupportLevel.NATIVE,
        "snowflake": SupportLevel.NATIVE,
        "bigquery": SupportLevel.NATIVE,
        "redshift": SupportLevel.NATIVE,
        "postgresql": SupportLevel.EXTENSION,
        "sqlite": SupportLevel.EXTENSION,
    },
)

DELTA_CAPABILITY = FormatCapability(
    format_name="delta",
    display_name="Delta Lake",
    file_extension="",  # Delta is directory-based
    features={
        "time_travel",
        "acid_transactions",
        "schema_evolution",
        "optimize",
        "vacuum",
        "z_order",
    },
    supported_platforms={
        "databricks": SupportLevel.NATIVE,
        "duckdb": SupportLevel.EXTENSION,  # delta extension
        # Spark, Trino will be added when those platforms are implemented
    },
)

ICEBERG_CAPABILITY = FormatCapability(
    format_name="iceberg",
    display_name="Apache Iceberg",
    file_extension="",  # Iceberg is directory-based
    features={
        "time_travel",
        "partition_evolution",
        "schema_evolution",
        "hidden_partitioning",
        "snapshot_management",
    },
    supported_platforms={
        "duckdb": SupportLevel.EXPERIMENTAL,  # iceberg extension
        # Trino, Spark, Athena will be added when those platforms are implemented
    },
)

# Registry of all format capabilities
CAPABILITIES_REGISTRY: dict[str, FormatCapability] = {
    "parquet": PARQUET_CAPABILITY,
    "delta": DELTA_CAPABILITY,
    "iceberg": ICEBERG_CAPABILITY,
}

# Platform format preferences (which format to prefer when multiple available)
PLATFORM_FORMAT_PREFERENCES: dict[str, list[str]] = {
    "duckdb": ["parquet", "delta", "tbl", "csv"],
    "datafusion": ["parquet", "tbl", "csv"],
    "clickhouse": ["parquet", "tbl", "csv"],
    "databricks": ["delta", "parquet", "tbl", "csv"],
    "snowflake": ["parquet", "tbl", "csv"],
    "bigquery": ["parquet", "tbl", "csv"],
    "redshift": ["parquet", "tbl", "csv"],
    "postgresql": ["parquet", "tbl", "csv"],
    "sqlite": ["parquet", "tbl", "csv"],
}


def get_supported_formats(platform_name: str) -> list[str]:
    """Get list of formats supported by a platform.

    Args:
        platform_name: Name of the platform (e.g., 'duckdb', 'databricks')

    Returns:
        List of supported format names, ordered by preference
    """
    supported = []

    # Get platform's format preference order
    preference_order = PLATFORM_FORMAT_PREFERENCES.get(platform_name, [])

    # Check each format in preference order
    for format_name in preference_order:
        capability = CAPABILITIES_REGISTRY.get(format_name)
        if not capability:
            # Unknown format (tbl, csv handled separately)
            supported.append(format_name)
            continue

        # Check if platform supports this format
        support_level = capability.supported_platforms.get(platform_name)
        if support_level and support_level != SupportLevel.NOT_SUPPORTED:
            supported.append(format_name)

    return supported


def get_preferred_format(platform_name: str, available_formats: list[str] | None = None) -> str:
    """Get the preferred format for a platform.

    Args:
        platform_name: Name of the platform
        available_formats: Optional list of available formats to choose from.
                         If None, returns the most preferred supported format.

    Returns:
        Preferred format name, or 'tbl' if no formats available
    """
    supported = get_supported_formats(platform_name)

    if available_formats:
        # Find first supported format that's also available
        for fmt in supported:
            if fmt in available_formats:
                return fmt
        # Fallback to first available format
        return available_formats[0] if available_formats else "tbl"

    # Return most preferred supported format
    return supported[0] if supported else "tbl"


def is_format_supported(platform_name: str, format_name: str) -> bool:
    """Check if a format is supported on a platform.

    Args:
        platform_name: Name of the platform
        format_name: Name of the format

    Returns:
        True if format is supported (at any level except NOT_SUPPORTED)
    """
    # Legacy formats (tbl, csv) are always supported
    if format_name in {"tbl", "csv", "dat"}:
        return True

    capability = CAPABILITIES_REGISTRY.get(format_name)
    if not capability:
        return False

    support_level = capability.supported_platforms.get(platform_name)
    return support_level is not None and support_level != SupportLevel.NOT_SUPPORTED


def get_format_capability(format_name: str) -> FormatCapability | None:
    """Get capability information for a format.

    Args:
        format_name: Name of the format

    Returns:
        FormatCapability if format is known, None otherwise
    """
    return CAPABILITIES_REGISTRY.get(format_name)


def has_feature(format_name: str, feature: str) -> bool:
    """Check if a format supports a specific feature.

    Args:
        format_name: Name of the format
        feature: Feature name (e.g., 'time_travel', 'predicate_pushdown')

    Returns:
        True if format supports the feature
    """
    capability = get_format_capability(format_name)
    return capability is not None and feature in capability.features
