"""Pricing tables for cloud database platforms.

This module contains published list prices for compute resources on each platform.
Prices are based on public pricing documentation and do not include:
- Enterprise discounts
- Reserved capacity pricing
- Commitment-based discounts
- Storage costs
- Network/data transfer costs

Prices are organized by platform, cloud provider, region, and resource type.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Pricing metadata
PRICING_VERSION = "2025.11"  # Semantic versioning (YYYY.MM)
PRICING_LAST_UPDATED = "2025-11-09"  # ISO 8601 date
PRICING_SOURCE = "Cloud provider public pricing pages"
PRICING_VALIDATION_DATE = datetime.fromisoformat(PRICING_LAST_UPDATED)

# Currency for all prices
CURRENCY = "USD"

# ============================================================================
# SNOWFLAKE PRICING
# ============================================================================

# Snowflake credit prices by edition, cloud provider, and region tier
# Source: Snowflake public pricing (2025)
SNOWFLAKE_CREDIT_PRICES: dict[str, dict[str, dict[str, float]]] = {
    "standard": {
        "aws": {
            "us": 2.00,  # US regions (us-east-1, us-west-2, etc.)
            "eu": 2.50,  # EU regions (20-25% premium)
            "ap": 2.60,  # Asia-Pacific regions (20-30% premium)
            "ca": 2.20,  # Canada regions
            "other": 2.70,  # Other regions (Middle East, South America, etc.)
        },
        "azure": {
            "us": 2.00,
            "eu": 2.50,
            "ap": 2.60,
            "ca": 2.20,
            "other": 2.70,
        },
        "gcp": {
            "us": 2.00,
            "eu": 2.50,
            "ap": 2.60,
            "ca": 2.20,
            "other": 2.70,
        },
    },
    "enterprise": {
        "aws": {
            "us": 3.00,
            "eu": 3.75,
            "ap": 3.90,
            "ca": 3.30,
            "other": 4.05,
        },
        "azure": {
            "us": 3.00,
            "eu": 3.75,
            "ap": 3.90,
            "ca": 3.30,
            "other": 4.05,
        },
        "gcp": {
            "us": 3.00,
            "eu": 3.75,
            "ap": 3.90,
            "ca": 3.30,
            "other": 4.05,
        },
    },
    "business_critical": {
        "aws": {
            "us": 4.00,
            "eu": 5.00,
            "ap": 5.20,
            "ca": 4.40,
            "other": 5.40,
        },
        "azure": {
            "us": 4.00,
            "eu": 5.00,
            "ap": 5.20,
            "ca": 4.40,
            "other": 5.40,
        },
        "gcp": {
            "us": 4.00,
            "eu": 5.00,
            "ap": 5.20,
            "ca": 4.40,
            "other": 5.40,
        },
    },
}

# ============================================================================
# ATHENA PRICING
# ============================================================================

# Athena pricing is simple: $5.00 per TB of data scanned
# Source: AWS Athena pricing (2025)
# Note: All regions use the same $5/TB rate. No regional variation.
ATHENA_PRICE_PER_TB = 5.00


# ============================================================================
# BIGQUERY PRICING
# ============================================================================

# BigQuery on-demand analysis pricing (per TB of data processed)
# Source: Google Cloud BigQuery pricing (2025)
BIGQUERY_ON_DEMAND_PRICES: dict[str, float] = {
    "us": 5.00,  # US multi-region and single regions
    "eu": 5.00,  # EU multi-region
    "asia": 5.00,  # Asia multi-region
    "us-single": 5.00,  # US single regions (Iowa, Oregon, Virginia, etc.)
    "eu-single": 5.50,  # EU single regions (some are higher)
    "asia-single": 5.50,  # Asia single regions
    "australia": 6.00,  # Australia regions
    "southamerica": 6.25,  # South America regions
    "middleeast": 6.00,  # Middle East regions
    "other": 5.50,  # Other regions
}

# ============================================================================
# REDSHIFT PRICING
# ============================================================================

# Redshift on-demand pricing by node type and region tier (per node-hour)
# Source: AWS Redshift pricing (2025)
REDSHIFT_NODE_PRICES: dict[str, dict[str, float]] = {
    # DC2 (Dense Compute) nodes
    "dc2.large": {
        "us-east-1": 0.25,
        "us-east-2": 0.25,
        "us-west-1": 0.27,
        "us-west-2": 0.25,
        "eu-west-1": 0.28,
        "eu-west-2": 0.32,
        "eu-central-1": 0.30,
        "ap-southeast-1": 0.29,
        "ap-southeast-2": 0.31,
        "ap-northeast-1": 0.29,
        "other": 0.30,
    },
    "dc2.8xlarge": {
        "us-east-1": 6.40,
        "us-east-2": 6.40,
        "us-west-1": 6.90,
        "us-west-2": 6.40,
        "eu-west-1": 7.20,
        "eu-west-2": 8.20,
        "eu-central-1": 7.60,
        "ap-southeast-1": 7.40,
        "ap-southeast-2": 7.90,
        "ap-northeast-1": 7.40,
        "other": 7.50,
    },
    # RA3 nodes (with managed storage)
    "ra3.xlplus": {
        "us-east-1": 1.086,
        "us-east-2": 1.086,
        "us-west-1": 1.173,
        "us-west-2": 1.086,
        "eu-west-1": 1.217,
        "eu-west-2": 1.304,
        "eu-central-1": 1.282,
        "ap-southeast-1": 1.260,
        "ap-southeast-2": 1.347,
        "ap-northeast-1": 1.260,
        "other": 1.250,
    },
    "ra3.4xlarge": {
        "us-east-1": 3.61,
        "us-east-2": 3.61,
        "us-west-1": 3.90,
        "us-west-2": 3.61,
        "eu-west-1": 4.05,
        "eu-west-2": 4.34,
        "eu-central-1": 4.26,
        "ap-southeast-1": 4.19,
        "ap-southeast-2": 4.48,
        "ap-northeast-1": 4.19,
        "other": 4.15,
    },
    "ra3.16xlarge": {
        "us-east-1": 14.44,
        "us-east-2": 14.44,
        "us-west-1": 15.60,
        "us-west-2": 14.44,
        "eu-west-1": 16.20,
        "eu-west-2": 17.36,
        "eu-central-1": 17.04,
        "ap-southeast-1": 16.76,
        "ap-southeast-2": 17.92,
        "ap-northeast-1": 16.76,
        "other": 16.60,
    },
    # DS2 (Dense Storage) - legacy
    "ds2.xlarge": {
        "us-east-1": 0.85,
        "us-east-2": 0.85,
        "us-west-1": 0.92,
        "us-west-2": 0.85,
        "eu-west-1": 0.95,
        "eu-west-2": 1.02,
        "eu-central-1": 1.00,
        "other": 1.00,
    },
    "ds2.8xlarge": {
        "us-east-1": 6.80,
        "us-east-2": 6.80,
        "us-west-1": 7.35,
        "us-west-2": 6.80,
        "eu-west-1": 7.60,
        "eu-west-2": 8.15,
        "eu-central-1": 8.00,
        "other": 8.00,
    },
}

# ============================================================================
# DATABRICKS PRICING
# ============================================================================

# Databricks DBU prices by cloud provider, tier, and workload type
# Source: Databricks public pricing (2025)
# Note: These are DBU prices. Customer also pays underlying compute from cloud provider.

DATABRICKS_DBU_PRICES: dict[str, dict[str, dict[str, float]]] = {
    "aws": {
        "standard": {
            "all_purpose": 0.40,
            "jobs": 0.15,
            "sql_warehouse": 0.22,
            "ml": 0.40,
        },
        "premium": {
            "all_purpose": 0.55,
            "jobs": 0.20,
            "sql_warehouse": 0.22,
            "ml": 0.55,
        },
        "enterprise": {
            "all_purpose": 0.65,
            "jobs": 0.20,
            "sql_warehouse": 0.30,
            "ml": 0.65,
        },
    },
    "azure": {
        "standard": {
            "all_purpose": 0.44,  # Azure runs 10% higher
            "jobs": 0.17,
            "sql_warehouse": 0.24,
            "ml": 0.44,
        },
        "premium": {
            "all_purpose": 0.60,
            "jobs": 0.22,
            "sql_warehouse": 0.24,
            "ml": 0.60,
        },
        "enterprise": {
            "all_purpose": 0.71,
            "jobs": 0.22,
            "sql_warehouse": 0.33,
            "ml": 0.71,
        },
    },
    "gcp": {
        "standard": {
            "all_purpose": 0.40,
            "jobs": 0.15,
            "sql_warehouse": 0.22,
            "ml": 0.40,
        },
        "premium": {
            "all_purpose": 0.55,
            "jobs": 0.20,
            "sql_warehouse": 0.22,
            "ml": 0.55,
        },
        "enterprise": {
            "all_purpose": 0.65,
            "jobs": 0.20,
            "sql_warehouse": 0.30,
            "ml": 0.65,
        },
    },
}

# ============================================================================
# AZURE SYNAPSE PRICING
# ============================================================================

# Synapse Serverless SQL Pool: $5.00 per TB of data processed
# Source: Azure Synapse Analytics pricing (2025)
# Note: Same pricing model as Athena and BigQuery
SYNAPSE_SERVERLESS_PRICE_PER_TB = 5.00

# Synapse Dedicated SQL Pool: DWU-based pricing (per hour)
# Source: Azure Synapse Analytics pricing (2025)
# Prices shown are for East US region (typical US pricing)
# DWU levels: DW100c to DW30000c (c = compute optimized)
SYNAPSE_DEDICATED_DWU_PRICES: dict[str, dict[str, float]] = {
    # Format: DWU level -> region tier -> price per hour
    "dw100c": {"us": 1.20, "eu": 1.44, "ap": 1.56, "other": 1.68},
    "dw200c": {"us": 2.40, "eu": 2.88, "ap": 3.12, "other": 3.36},
    "dw300c": {"us": 3.60, "eu": 4.32, "ap": 4.68, "other": 5.04},
    "dw400c": {"us": 4.80, "eu": 5.76, "ap": 6.24, "other": 6.72},
    "dw500c": {"us": 6.00, "eu": 7.20, "ap": 7.80, "other": 8.40},
    "dw1000c": {"us": 12.00, "eu": 14.40, "ap": 15.60, "other": 16.80},
    "dw1500c": {"us": 18.00, "eu": 21.60, "ap": 23.40, "other": 25.20},
    "dw2000c": {"us": 24.00, "eu": 28.80, "ap": 31.20, "other": 33.60},
    "dw2500c": {"us": 30.00, "eu": 36.00, "ap": 39.00, "other": 42.00},
    "dw3000c": {"us": 36.00, "eu": 43.20, "ap": 46.80, "other": 50.40},
    "dw5000c": {"us": 60.00, "eu": 72.00, "ap": 78.00, "other": 84.00},
    "dw6000c": {"us": 72.00, "eu": 86.40, "ap": 93.60, "other": 100.80},
    "dw7500c": {"us": 90.00, "eu": 108.00, "ap": 117.00, "other": 126.00},
    "dw10000c": {"us": 120.00, "eu": 144.00, "ap": 156.00, "other": 168.00},
    "dw15000c": {"us": 180.00, "eu": 216.00, "ap": 234.00, "other": 252.00},
    "dw30000c": {"us": 360.00, "eu": 432.00, "ap": 468.00, "other": 504.00},
}


# ============================================================================
# AZURE FABRIC PRICING
# ============================================================================

# Fabric Capacity Units (CU) pricing per hour
# Source: Microsoft Fabric pricing (2025)
# Price varies by region; shown are approximate rates
FABRIC_CU_PRICES: dict[str, float] = {
    "us": 0.18,  # US regions (typical)
    "eu": 0.20,  # European regions (~10% higher)
    "ap": 0.22,  # Asia-Pacific regions (~20% higher)
    "other": 0.20,  # Other regions
}

# Fabric capacity SKU sizes (SKU -> CU count)
FABRIC_SKU_CU_MAP: dict[str, int] = {
    "f2": 2,
    "f4": 4,
    "f8": 8,
    "f16": 16,
    "f32": 32,
    "f64": 64,
    "f128": 128,
    "f256": 256,
    "f512": 512,
    "f1024": 1024,
    "f2048": 2048,
}


# ============================================================================
# FIREBOLT PRICING
# ============================================================================

# Firebolt uses Firebolt Units (FBUs) for billing
# Source: Firebolt pricing documentation (2025)
# FBU rate depends on engine node type; consumption is per-second
#
# Note: This implementation uses pay-as-you-go pricing. Firebolt also offers:
# - Annual commitment discounts (varies by volume)
# - Reserved capacity pricing (available through sales)
# This default pricing (~$0.0833/FBU) reflects typical on-demand rates.

# FBU consumption rates per hour by node type
# These represent the FBU/hour consumption for each node type
FIREBOLT_NODE_FBU_RATES: dict[str, float] = {
    "s": 8.0,  # Small nodes
    "m": 16.0,  # Medium nodes
    "l": 32.0,  # Large nodes
    "xl": 64.0,  # Extra-large nodes
}

# FBU price per unit (pay-as-you-go rate)
# This is an approximation based on typical pricing. Actual rates may vary:
# - Commitment discounts available for annual or multi-year contracts
# - Volume discounts apply at higher consumption levels
# - Reserved capacity pricing available through enterprise agreements
FIREBOLT_FBU_PRICE = 0.0833  # ~$0.0833 per FBU (pay-as-you-go)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def get_athena_price_per_tb() -> float:
    """Get the Athena price per TB of data scanned.

    Athena has a flat rate of $5.00 per TB across all regions.

    Returns:
        Price per TB in USD
    """
    return ATHENA_PRICE_PER_TB


def get_snowflake_credit_price(edition: str, cloud: str, region: str) -> float:
    """Get the price per Snowflake credit.

    Args:
        edition: Snowflake edition (standard, enterprise, business_critical)
        cloud: Cloud provider (aws, azure, gcp)
        region: AWS region code (e.g., us-east-1, eu-west-1)

    Returns:
        Price per credit in USD
    """
    edition = edition.lower().replace("-", "_").replace(" ", "_")
    cloud = cloud.lower()

    # Determine region tier
    region_tier = _map_region_to_tier(region)

    # Get price from table
    try:
        return SNOWFLAKE_CREDIT_PRICES[edition][cloud][region_tier]
    except KeyError:
        # Fallback to standard/aws/us if not found
        return SNOWFLAKE_CREDIT_PRICES.get("standard", {}).get("aws", {}).get("us", 2.00)


def get_bigquery_price_per_tb(location: str) -> float:
    """Get the BigQuery on-demand price per TB processed.

    Args:
        location: BigQuery location/region (e.g., us-east1, EU, us)

    Returns:
        Price per TB in USD
    """
    location = location.lower()

    # Multi-region pricing (best rates)
    if location in ["us", "us-multi"]:
        return BIGQUERY_ON_DEMAND_PRICES["us"]
    elif location in ["eu", "eu-multi"]:
        return BIGQUERY_ON_DEMAND_PRICES["eu"]
    elif location in ["asia", "asia-multi"]:
        return BIGQUERY_ON_DEMAND_PRICES["asia"]

    # US single regions (same as multi-region)
    us_single_regions = {
        "us-central1",
        "us-east1",
        "us-east4",
        "us-west1",
        "us-west2",
        "us-west3",
        "us-west4",
        "northamerica-northeast1",
        "northamerica-northeast2",  # Canada
    }
    if location in us_single_regions or location.startswith("us-"):
        return BIGQUERY_ON_DEMAND_PRICES["us-single"]

    # EU single regions
    eu_single_regions = {
        "europe-central2",
        "europe-north1",
        "europe-southwest1",
        "europe-west1",
        "europe-west2",
        "europe-west3",
        "europe-west4",
        "europe-west6",
        "europe-west8",
        "europe-west9",
    }
    if location in eu_single_regions or location.startswith("europe-"):
        return BIGQUERY_ON_DEMAND_PRICES["eu-single"]

    # Asia single regions
    asia_single_regions = {
        "asia-east1",
        "asia-east2",  # Taiwan, Hong Kong
        "asia-northeast1",
        "asia-northeast2",
        "asia-northeast3",  # Tokyo, Osaka, Seoul
        "asia-south1",
        "asia-south2",  # Mumbai, Delhi
        "asia-southeast1",
        "asia-southeast2",  # Singapore, Jakarta
    }
    if location in asia_single_regions or location.startswith("asia-"):
        return BIGQUERY_ON_DEMAND_PRICES["asia-single"]

    # Australia regions (higher pricing)
    australia_regions = {"australia-southeast1", "australia-southeast2"}
    if location in australia_regions or location.startswith("australia-"):
        return BIGQUERY_ON_DEMAND_PRICES["australia"]

    # South America regions (higher pricing)
    southamerica_regions = {"southamerica-east1", "southamerica-west1"}
    if location in southamerica_regions or location.startswith("southamerica-"):
        return BIGQUERY_ON_DEMAND_PRICES["southamerica"]

    # Middle East regions (higher pricing)
    middleeast_regions = {"me-west1", "me-central1", "me-central2"}
    if location in middleeast_regions or location.startswith("me-"):
        return BIGQUERY_ON_DEMAND_PRICES["middleeast"]

    # Default to 'other' pricing for unknown regions
    return BIGQUERY_ON_DEMAND_PRICES["other"]


def get_redshift_node_price(node_type: str, region: str) -> float:
    """Get the Redshift on-demand price per node-hour.

    Args:
        node_type: Redshift node type (e.g., dc2.large, ra3.4xlarge)
        region: AWS region code (e.g., us-east-1)

    Returns:
        Price per node-hour in USD
    """
    node_type = node_type.lower()
    region = region.lower()

    # Get price from table
    try:
        return REDSHIFT_NODE_PRICES[node_type][region]
    except KeyError:
        # Try with 'other' fallback
        if node_type in REDSHIFT_NODE_PRICES:
            return REDSHIFT_NODE_PRICES[node_type].get("other", 1.00)
        # Default fallback
        return 1.00


def get_databricks_dbu_price(cloud: str, tier: str, workload_type: str) -> float:
    """Get the Databricks DBU price.

    Args:
        cloud: Cloud provider (aws, azure, gcp)
        tier: Databricks tier (standard, premium, enterprise)
        workload_type: Workload type (all_purpose, jobs, sql_warehouse, ml)

    Returns:
        Price per DBU in USD
    """
    cloud = cloud.lower()
    tier = tier.lower()
    workload_type = workload_type.lower().replace("-", "_").replace(" ", "_")

    # Get price from table
    try:
        return DATABRICKS_DBU_PRICES[cloud][tier][workload_type]
    except KeyError:
        # Fallback to aws/premium/all_purpose
        return DATABRICKS_DBU_PRICES.get("aws", {}).get("premium", {}).get("all_purpose", 0.55)


def get_synapse_serverless_price_per_tb() -> float:
    """Get the Azure Synapse Serverless SQL Pool price per TB.

    Synapse Serverless has a flat rate of $5.00 per TB across all regions.

    Returns:
        Price per TB in USD
    """
    return SYNAPSE_SERVERLESS_PRICE_PER_TB


def get_synapse_dedicated_price(dwu_level: str, region: str) -> float:
    """Get the Azure Synapse Dedicated SQL Pool price per DWU-hour.

    Args:
        dwu_level: DWU level (e.g., dw100c, dw1000c, dw30000c)
        region: Azure region code

    Returns:
        Price per hour in USD for the specified DWU level
    """
    dwu_level = dwu_level.lower()
    region_tier = _map_region_to_tier(region)

    try:
        return SYNAPSE_DEDICATED_DWU_PRICES[dwu_level][region_tier]
    except KeyError:
        # Fallback: try with "us" tier or default to DW100c US pricing
        if dwu_level in SYNAPSE_DEDICATED_DWU_PRICES:
            price = SYNAPSE_DEDICATED_DWU_PRICES[dwu_level].get("us", 1.20)
            if region_tier != "us":
                logger.warning(
                    f"Regional pricing for Synapse {dwu_level} in tier '{region_tier}' not available; "
                    f"using US pricing as fallback"
                )
            return price
        logger.warning(f"DWU level '{dwu_level}' not found in pricing table; defaulting to DW100c US pricing")
        return SYNAPSE_DEDICATED_DWU_PRICES.get("dw100c", {}).get("us", 1.20)


def get_fabric_cu_price(region: str) -> float:
    """Get the Microsoft Fabric Capacity Unit price per hour.

    Args:
        region: Azure region code

    Returns:
        Price per CU per hour in USD
    """
    region_tier = _map_region_to_tier(region)
    return FABRIC_CU_PRICES.get(region_tier, FABRIC_CU_PRICES["other"])


def get_fabric_sku_cu_count(sku: str) -> int:
    """Get the number of Capacity Units for a Fabric SKU.

    Args:
        sku: Fabric SKU (e.g., f2, f64, f2048)

    Returns:
        Number of Capacity Units for the SKU
    """
    sku = sku.lower()
    cu_count = FABRIC_SKU_CU_MAP.get(sku)
    if cu_count is not None:
        return cu_count
    logger.warning(f"Unknown Fabric SKU '{sku}'; defaulting to F2 (2 CUs)")
    return 2


def get_firebolt_fbu_rate(node_type: str) -> float:
    """Get the FBU consumption rate per hour for a Firebolt node type.

    Args:
        node_type: Node type (s, m, l, xl)

    Returns:
        FBUs consumed per hour for the node type
    """
    node_type = node_type.lower()
    fbu_rate = FIREBOLT_NODE_FBU_RATES.get(node_type)
    if fbu_rate is not None:
        return fbu_rate
    logger.warning(f"Unknown Firebolt node type '{node_type}'; defaulting to M (16 FBU/hour)")
    return FIREBOLT_NODE_FBU_RATES["m"]


def get_firebolt_fbu_price() -> float:
    """Get the Firebolt price per FBU.

    Returns:
        Price per FBU in USD
    """
    return FIREBOLT_FBU_PRICE


def _map_region_to_tier(region: str) -> str:
    """Map an AWS/Azure/GCP region code to a pricing tier.

    Provides granular regional pricing mappings for improved cost accuracy.
    Target: ±5% accuracy (vs ±10-20% with coarse mappings).

    Args:
        region: Region code (e.g., us-east-1, eu-west-2, asia-southeast1)

    Returns:
        Pricing tier: us, eu, ap, ca, or other
    """
    region = region.lower()

    # US regions (AWS, Azure, GCP)
    us_regions = {
        # AWS US
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        # Azure US
        "eastus",
        "eastus2",
        "centralus",
        "northcentralus",
        "southcentralus",
        "westus",
        "westus2",
        "westus3",
        "westcentralus",
        # GCP US
        "us-central1",
        "us-east1",
        "us-east4",
        "us-west1",
        "us-west2",
        "us-west3",
        "us-west4",
    }
    if region in us_regions or region.startswith("us-"):
        return "us"

    # Canada regions
    canada_regions = {
        "ca-central-1",  # AWS
        "canadacentral",
        "canadaeast",  # Azure
        "northamerica-northeast1",
        "northamerica-northeast2",  # GCP (Montreal, Toronto)
    }
    if region in canada_regions or region.startswith("ca-"):
        return "ca"

    # EU regions (Western + Northern Europe)
    eu_regions = {
        # AWS EU
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",  # Ireland, London, Paris
        "eu-central-1",
        "eu-central-2",  # Frankfurt, Zurich
        "eu-north-1",  # Stockholm
        "eu-south-1",
        "eu-south-2",  # Milan, Spain
        # Azure EU
        "northeurope",
        "westeurope",
        "francecentral",
        "francesouth",
        "germanynorth",
        "germanywestcentral",
        "norwayeast",
        "norwaywest",
        "switzerlandnorth",
        "switzerlandwest",
        "uksouth",
        "ukwest",
        "swedencentral",
        "swedensouth",
        # GCP EU
        "europe-west1",
        "europe-west2",
        "europe-west3",
        "europe-west4",
        "europe-west6",
        "europe-west8",
        "europe-west9",
        "europe-central2",
        "europe-north1",
        "europe-southwest1",
    }
    if region in eu_regions or region.startswith(("eu-", "europe-")):
        return "eu"

    # Asia-Pacific regions
    ap_regions = {
        # AWS AP
        "ap-south-1",
        "ap-south-2",  # Mumbai, Hyderabad
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-northeast-3",  # Tokyo, Seoul, Osaka
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-southeast-3",
        "ap-southeast-4",  # Singapore, Sydney, Jakarta, Melbourne
        "ap-east-1",  # Hong Kong
        # Azure AP
        "eastasia",
        "southeastasia",  # Hong Kong, Singapore
        "australiaeast",
        "australiacentral",
        "australiasoutheast",
        "japaneast",
        "japanwest",
        "koreacentral",
        "koreasouth",
        "centralindia",
        "southindia",
        "westindia",
        "jioindiawest",
        "jioindiacentral",
        # GCP AP
        "asia-east1",
        "asia-east2",  # Taiwan, Hong Kong
        "asia-northeast1",
        "asia-northeast2",
        "asia-northeast3",  # Tokyo, Osaka, Seoul
        "asia-south1",
        "asia-south2",  # Mumbai, Delhi
        "asia-southeast1",
        "asia-southeast2",  # Singapore, Jakarta
        "australia-southeast1",
        "australia-southeast2",  # Sydney, Melbourne
    }
    if region in ap_regions or region.startswith(("ap-", "asia-", "australia")):
        return "ap"

    # Middle East regions - typically higher pricing
    middle_east_regions = {
        "me-south-1",
        "me-central-1",  # AWS Bahrain, UAE
        "uaenorth",
        "uaecentral",  # Azure UAE
        "qatarcentral",  # Azure Qatar
        "me-west1",  # GCP Tel Aviv
    }
    if region in middle_east_regions:
        return "other"  # Higher pricing tier

    # South America regions - typically higher pricing
    south_america_regions = {
        "sa-east-1",  # AWS Sao Paulo
        "brazilsouth",
        "brazilsoutheast",  # Azure Brazil
        "southamerica-east1",
        "southamerica-west1",  # GCP Sao Paulo, Santiago
    }
    if region in south_america_regions:
        return "other"  # Higher pricing tier

    # Africa regions - typically higher pricing
    africa_regions = {
        "af-south-1",  # AWS Cape Town
        "southafricanorth",
        "southafricawest",  # Azure South Africa
    }
    if region in africa_regions:
        return "other"  # Higher pricing tier

    # Default to 'other' for unknown regions
    return "other"


def get_pricing_age_days() -> int:
    """Return number of days since pricing was last updated.

    Returns:
        Number of days between now and PRICING_LAST_UPDATED
    """
    return (datetime.now() - PRICING_VALIDATION_DATE).days


def is_pricing_stale(threshold_days: int = 90) -> bool:
    """Check if pricing is older than threshold.

    Args:
        threshold_days: Number of days after which pricing is considered stale (default: 90)

    Returns:
        True if pricing age exceeds threshold, False otherwise
    """
    return get_pricing_age_days() > threshold_days
