"""Storage cost estimation for benchmark data.

This module provides rough storage cost estimates based on data size and duration.
Storage pricing varies by platform, region, and storage tier.
"""

from typing import Any

from benchbox.core.cost.pricing import _map_region_to_tier

# Storage pricing per TB per month (USD)
# Source: Public cloud provider pricing (2025)
STORAGE_PRICES_PER_TB_MONTH = {
    "snowflake": {
        "us": 23.00,  # On-demand storage
        "eu": 25.00,
        "ap": 26.00,
        "ca": 24.00,
        "other": 27.00,
    },
    "bigquery": {
        "us": 20.00,  # Active storage
        "eu": 22.00,
        "ap": 23.00,
        "ca": 21.00,
        "other": 24.00,
    },
    "redshift": {
        "us": 24.00,  # RA3 managed storage
        "eu": 26.00,
        "ap": 27.00,
        "ca": 25.00,
        "other": 28.00,
    },
    "databricks": {
        "us": 23.00,  # Delta Lake storage
        "eu": 25.00,
        "ap": 26.00,
        "ca": 24.00,
        "other": 27.00,
    },
    "duckdb": {
        "us": 0.00,  # Local storage
        "eu": 0.00,
        "ap": 0.00,
        "ca": 0.00,
        "other": 0.00,
    },
    "clickhouse": {
        "us": 0.00,  # Local storage
        "eu": 0.00,
        "ap": 0.00,
        "ca": 0.00,
        "other": 0.00,
    },
}

# Storage pricing notes by platform
STORAGE_NOTES = {
    "snowflake": "On-demand storage pricing. Time Travel and Fail-safe may incur additional costs.",
    "bigquery": "Active storage pricing. Long-term storage (90+ days) costs $10/TB/month.",
    "redshift": "RA3 managed storage pricing. Dense node storage included in compute cost.",
    "databricks": "Delta Lake storage on underlying cloud provider (S3/ADLS/GCS).",
    "duckdb": "Local storage, no cloud costs.",
    "clickhouse": "Local storage, no cloud costs.",
}


def estimate_storage_cost(
    platform: str,
    total_bytes: int,
    storage_duration_hours: float,
    region: str = "us-east-1",
) -> dict[str, Any]:
    """Estimate storage cost based on data size and duration.

    This provides a rough estimate for planning purposes. Actual storage costs
    depend on many factors including:
    - Compression ratios
    - Storage tier (active vs long-term)
    - Time Travel / Fail-safe settings
    - Data replication
    - Snapshots and backups

    Args:
        platform: Platform name (snowflake, bigquery, redshift, databricks, etc.)
        total_bytes: Total data size in bytes
        storage_duration_hours: How long data is stored (hours)
        region: Cloud region

    Returns:
        Dictionary with storage cost estimate and metadata:
        - storage_cost: Estimated cost in USD
        - storage_tb: Data size in TB
        - price_per_tb_month: Monthly storage price per TB
        - duration_hours: Storage duration in hours
        - note: Platform-specific storage notes
    """
    platform_lower = platform.lower()

    # Convert bytes to TB
    bytes_per_tb = 1024**4
    tb = total_bytes / bytes_per_tb

    # Get region tier
    region_tier = _map_region_to_tier(region)

    # Get price per TB per month
    if platform_lower in STORAGE_PRICES_PER_TB_MONTH:
        price_per_tb_month = STORAGE_PRICES_PER_TB_MONTH[platform_lower].get(
            region_tier, STORAGE_PRICES_PER_TB_MONTH[platform_lower].get("us", 23.00)
        )
    else:
        # Default fallback for unknown platforms
        price_per_tb_month = 23.00

    # Prorate for actual duration
    hours_per_month = 730  # Average hours per month (365 days / 12 months * 24 hours)
    storage_cost = tb * price_per_tb_month * (storage_duration_hours / hours_per_month)

    return {
        "storage_cost": storage_cost,
        "storage_tb": tb,
        "price_per_tb_month": price_per_tb_month,
        "duration_hours": storage_duration_hours,
        "note": STORAGE_NOTES.get(platform_lower, "Storage cost estimate based on standard cloud pricing."),
    }
