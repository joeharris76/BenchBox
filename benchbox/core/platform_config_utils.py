"""Utilities for normalizing and standardizing platform configuration metadata.

This module provides helper functions for working with platform configuration
information captured during benchmark execution, including warehouse size
normalization, cloud provider standardization, and configuration comparison.

Copyright 2026 Joe Harris / BenchBox Project
Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from typing import Any


def normalize_warehouse_size(platform_type: str, size: str | None) -> dict[str, Any] | None:
    """Normalize warehouse/cluster size across different platforms.

    Args:
        platform_type: The platform type (databricks, snowflake, bigquery, redshift, etc.)
        size: The platform-specific size designation

    Returns:
        Normalized size information with standardized tier and raw value,
        or None if size cannot be determined

    Examples:
        >>> normalize_warehouse_size("snowflake", "X-SMALL")
        {'tier': 'xs', 'raw_value': 'X-SMALL', 'platform': 'snowflake'}

        >>> normalize_warehouse_size("databricks", "2X-Small")
        {'tier': 'xxs', 'raw_value': '2X-Small', 'platform': 'databricks'}

        >>> normalize_warehouse_size("redshift", "dc2.large")
        {'tier': 'small', 'raw_value': 'dc2.large', 'platform': 'redshift', 'node_type': 'dc2.large'}
    """
    if not size:
        return None

    size_lower = size.lower().strip()

    # Databricks warehouse sizes
    if platform_type == "databricks":
        databricks_size_map = {
            "2x-small": "xxs",
            "x-small": "xs",
            "small": "s",
            "medium": "m",
            "large": "l",
            "x-large": "xl",
            "2x-large": "xxl",
            "3x-large": "xxxl",
            "4x-large": "xxxxl",
        }
        tier = databricks_size_map.get(size_lower)
        return {
            "tier": tier if tier else "custom",
            "raw_value": size,
            "platform": platform_type,
        }

    # Snowflake warehouse sizes
    elif platform_type == "snowflake":
        snowflake_size_map = {
            "x-small": "xs",
            "small": "s",
            "medium": "m",
            "large": "l",
            "x-large": "xl",
            "2x-large": "xxl",
            "3x-large": "xxxl",
            "4x-large": "xxxxl",
            "5x-large": "xxxxxl",
            "6x-large": "xxxxxxl",
        }
        tier = snowflake_size_map.get(size_lower)
        return {
            "tier": tier if tier else "custom",
            "raw_value": size,
            "platform": platform_type,
        }

    # Redshift node types
    elif platform_type == "redshift":
        # Map node types to relative sizes
        node_type_map = {
            "dc2.large": "small",
            "dc2.8xlarge": "large",
            "ra3.xlplus": "medium",
            "ra3.4xlarge": "large",
            "ra3.16xlarge": "xlarge",
        }
        tier = node_type_map.get(size_lower, "custom")
        return {
            "tier": tier,
            "raw_value": size,
            "platform": platform_type,
            "node_type": size,
        }

    # BigQuery uses slots, not named sizes
    elif platform_type == "bigquery":
        # If it's a number, treat as slot count
        try:
            slot_count = int(size)
            # Rough mapping of slots to tiers
            if slot_count < 100:
                tier = "small"
            elif slot_count < 500:
                tier = "medium"
            elif slot_count < 2000:
                tier = "large"
            else:
                tier = "xlarge"

            return {
                "tier": tier,
                "raw_value": size,
                "platform": platform_type,
                "slot_count": slot_count,
            }
        except (ValueError, TypeError):
            return {
                "tier": "on-demand",
                "raw_value": size,
                "platform": platform_type,
            }

    # ClickHouse, DuckDB, SQLite don't have predefined sizes
    return {
        "tier": "n/a",
        "raw_value": size,
        "platform": platform_type,
    }


def standardize_cloud_provider(provider: str | None) -> str | None:
    """Standardize cloud provider names across platforms.

    Args:
        provider: Raw cloud provider string from platform

    Returns:
        Standardized provider name (AWS, GCP, Azure, or original value)

    Examples:
        >>> standardize_cloud_provider("amazon")
        'AWS'

        >>> standardize_cloud_provider("gcp")
        'GCP'

        >>> standardize_cloud_provider("azure")
        'Azure'
    """
    if not provider:
        return None

    provider_lower = provider.lower().strip()

    # AWS aliases
    if provider_lower in ("aws", "amazon", "amazon web services"):
        return "AWS"

    # GCP aliases
    elif provider_lower in ("gcp", "google", "google cloud", "google cloud platform"):
        return "GCP"

    # Azure aliases
    elif provider_lower in ("azure", "microsoft azure", "ms azure"):
        return "Azure"

    # Return original if not recognized
    return provider


def extract_region_info(platform_info: dict[str, Any]) -> dict[str, str | None]:
    """Extract and standardize region information from platform_info.

    Args:
        platform_info: Platform information dictionary from get_platform_info()

    Returns:
        Dictionary with cloud_provider and cloud_region keys

    Examples:
        >>> platform_info = {"cloud_provider": "AWS", "cloud_region": "us-east-1"}
        >>> extract_region_info(platform_info)
        {'cloud_provider': 'AWS', 'cloud_region': 'us-east-1'}
    """
    result: dict[str, str | None] = {
        "cloud_provider": None,
        "cloud_region": None,
    }

    # Try direct fields first
    if "cloud_provider" in platform_info:
        result["cloud_provider"] = standardize_cloud_provider(platform_info["cloud_provider"])

    if "cloud_region" in platform_info:
        result["cloud_region"] = platform_info.get("cloud_region")

    # Try configuration nested object
    config = platform_info.get("configuration", {})
    if config:
        if not result["cloud_provider"] and "cloud_provider" in config:
            result["cloud_provider"] = standardize_cloud_provider(config["cloud_provider"])

        if not result["cloud_region"] and "region" in config:
            result["cloud_region"] = config.get("region")

    return result


def extract_compute_info(platform_info: dict[str, Any]) -> dict[str, Any]:
    """Extract compute configuration summary from platform_info.

    Args:
        platform_info: Platform information dictionary from get_platform_info()

    Returns:
        Dictionary summarizing compute configuration with standardized fields

    Examples:
        >>> platform_info = {
        ...     "platform_type": "snowflake",
        ...     "compute_configuration": {"warehouse_size": "LARGE", "warehouse_type": "STANDARD"}
        ... }
        >>> extract_compute_info(platform_info)
        {'warehouse_size': 'LARGE', 'warehouse_type': 'STANDARD', 'normalized_size': {...}}
    """
    compute_config = platform_info.get("compute_configuration", {})
    platform_type = platform_info.get("platform_type", "unknown")

    result: dict[str, Any] = {}

    # Extract warehouse/cluster size
    size_field = None
    if "warehouse_size" in compute_config:
        size_field = compute_config["warehouse_size"]
    elif "cluster_size" in compute_config:
        size_field = compute_config["cluster_size"]
    elif "node_type" in compute_config:
        size_field = compute_config["node_type"]

    if size_field:
        result["raw_size"] = size_field
        result["normalized_size"] = normalize_warehouse_size(platform_type, size_field)

    # Extract node/cluster count
    if "num_compute_nodes" in compute_config:
        result["node_count"] = compute_config["num_compute_nodes"]
    elif "min_num_clusters" in compute_config:
        result["min_clusters"] = compute_config["min_num_clusters"]
        result["max_clusters"] = compute_config["max_num_clusters"]

    # Extract special features
    if "enable_photon" in compute_config:
        result["photon_enabled"] = compute_config["enable_photon"]

    if "enable_serverless_compute" in compute_config:
        result["serverless_enabled"] = compute_config["enable_serverless_compute"]

    if "enable_query_acceleration" in compute_config:
        result["query_acceleration"] = compute_config["enable_query_acceleration"]

    # Extract pricing model for BigQuery
    if "pricing_model" in compute_config:
        result["pricing_model"] = compute_config["pricing_model"]

    return result


def get_platform_summary(platform_info: dict[str, Any]) -> str:
    """Generate human-readable summary of platform configuration.

    Args:
        platform_info: Platform information dictionary from get_platform_info()

    Returns:
        Human-readable summary string

    Examples:
        >>> platform_info = {
        ...     "platform_name": "Snowflake",
        ...     "platform_version": "8.2.1",
        ...     "cloud_provider": "AWS",
        ...     "cloud_region": "us-east-1",
        ...     "compute_configuration": {"warehouse_size": "LARGE"}
        ... }
        >>> get_platform_summary(platform_info)
        'Snowflake 8.2.1 on AWS (us-east-1), warehouse: LARGE'
    """
    parts = []

    # Platform name and version
    name = platform_info.get("platform_name", "Unknown Platform")
    version = platform_info.get("platform_version")
    if version:
        parts.append(f"{name} {version}")
    else:
        parts.append(name)

    # Cloud provider and region
    region_info = extract_region_info(platform_info)
    if region_info["cloud_provider"]:
        cloud_str = region_info["cloud_provider"]
        if region_info["cloud_region"]:
            cloud_str += f" ({region_info['cloud_region']})"
        parts.append(f"on {cloud_str}")

    # Compute configuration
    compute_info = extract_compute_info(platform_info)
    if "raw_size" in compute_info:
        parts.append(f"warehouse: {compute_info['raw_size']}")

    return ", ".join(parts)


__all__ = [
    "normalize_warehouse_size",
    "standardize_cloud_provider",
    "extract_region_info",
    "extract_compute_info",
    "get_platform_summary",
]
