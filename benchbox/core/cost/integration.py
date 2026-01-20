"""Integration helpers for adding cost estimation to benchmark results.

This module provides utilities to calculate and attach cost information
to benchmark results after execution.
"""

import logging
from typing import Any, Optional

from benchbox.core.cost.calculator import CostCalculator
from benchbox.core.cost.models import PhaseCost
from benchbox.core.results.models import BenchmarkResults

logger = logging.getLogger(__name__)


# Required platform configuration fields for cost calculation
PLATFORM_CONFIG_REQUIREMENTS = {
    "snowflake": {
        "required": ["edition", "cloud", "region"],
        "optional": ["warehouse_size"],
    },
    "bigquery": {
        "required": ["location"],
        "optional": [],
    },
    "redshift": {
        "required": ["node_type", "node_count", "region"],
        "optional": [],
    },
    "databricks": {
        "required": ["cloud", "tier", "workload_type", "cluster_size_dbu_per_hour"],
        "optional": ["warehouse_size"],
    },
}


def validate_platform_config(platform: str, config: dict[str, Any]) -> tuple[bool, list[str]]:
    """Validate platform configuration has required fields for cost calculation.

    Args:
        platform: Platform name (case-insensitive)
        config: Platform configuration dictionary

    Returns:
        Tuple of (is_valid, list of warning messages)
    """
    warnings = []
    platform_lower = platform.lower()

    # Check if we have config requirements for this platform
    if platform_lower not in PLATFORM_CONFIG_REQUIREMENTS:
        # Unknown platform or doesn't require config validation
        return True, warnings

    requirements = PLATFORM_CONFIG_REQUIREMENTS[platform_lower]

    # Check required fields
    for field in requirements.get("required", []):
        if field not in config or config[field] is None:
            warnings.append(
                f"Missing required config field '{field}' for {platform} cost calculation. "
                f"Cost estimation may be inaccurate or fail."
            )

    is_valid = len(warnings) == 0
    return is_valid, warnings


def add_cost_estimation_to_results(
    results: BenchmarkResults,
    platform_config: Optional[dict[str, Any]] = None,
) -> BenchmarkResults:
    """Add cost estimation to benchmark results.

    This function:
    1. Calculates costs for individual queries based on resource_usage
    2. Aggregates costs by phase (power_test, throughput_test, maintenance_test)
    3. Calculates total benchmark cost
    4. Adds cost_summary to the results object

    Args:
        results: BenchmarkResults object from benchmark execution
        platform_config: Optional platform configuration override
                        (extracted from results.platform_info if not provided)

    Returns:
        Updated BenchmarkResults with cost information
    """
    # Extract platform early for error logging
    platform = results.platform if results.platform else "unknown"

    try:
        if not results.platform:
            logger.debug("No platform specified in results, skipping cost estimation")
            return results

        # Get platform config from results if not provided
        if platform_config is None:
            platform_config = _extract_platform_config_from_results(results)

        # Validate platform configuration
        config_valid, config_warnings = validate_platform_config(platform, platform_config)
        if config_warnings:
            for warning in config_warnings:
                logger.warning(f"Platform config validation: {warning}")
            if not config_valid:
                logger.error(
                    f"Platform configuration incomplete for {platform}. Cost estimation may fail or be inaccurate."
                )

        calculator = CostCalculator()

        # Calculate query-level costs and update query_results
        for query_result in results.query_results or []:
            if isinstance(query_result, dict):
                resource_usage = query_result.get("resource_usage")
                if resource_usage:
                    query_cost = calculator.calculate_query_cost(
                        platform=platform,
                        resource_usage=resource_usage,
                        platform_config=platform_config,
                    )
                    if query_cost:
                        query_result["cost"] = query_cost.compute_cost

        # Calculate phase-level costs
        phase_costs = _calculate_phase_costs(results, platform, platform_config, calculator)

        # Calculate total benchmark cost
        from benchbox.core.cost.pricing import (
            PRICING_LAST_UPDATED,
            PRICING_VERSION,
            get_pricing_age_days,
            is_pricing_stale,
        )

        platform_details = {
            "platform": platform,
            "platform_type": platform_config.get("platform_type"),
            "region": platform_config.get("region"),
            "warehouse_size": platform_config.get("warehouse_size"),
            "node_type": platform_config.get("node_type"),
            "edition": platform_config.get("edition"),
            "tier": platform_config.get("tier"),
            "pricing_version": PRICING_VERSION,
            "pricing_date": PRICING_LAST_UPDATED,
        }

        # Remove None values
        platform_details = {k: v for k, v in platform_details.items() if v is not None}

        benchmark_cost = calculator.calculate_benchmark_cost(
            phase_costs=phase_costs,
            platform_details=platform_details,
        )

        # Set cost model and add warnings based on platform type
        platform_lower = platform.lower()
        if platform_lower == "redshift":
            benchmark_cost.cost_model = "marginal"
            node_count = platform_config.get("node_count", "N")
            node_type = platform_config.get("node_type", "unknown")

            # Get price for warning message
            from benchbox.core.cost.pricing import get_redshift_node_price

            region = platform_config.get("region", "us-east-1")
            price_per_node_hour = get_redshift_node_price(node_type, region)

            benchmark_cost.warnings.append(
                f"Redshift costs show marginal per-query costs, not total cluster TCO. "
                f"Cluster idle time is excluded. For full cluster cost, calculate: "
                f"cluster_runtime_hours × {node_count} nodes × ${price_per_node_hour:.2f}/hour."
            )
        elif platform_lower == "databricks":
            workload_type = platform_config.get("workload_type", "")
            if workload_type == "all_purpose":
                benchmark_cost.cost_model = "marginal"
                benchmark_cost.warnings.append(
                    "Databricks all-purpose cluster costs show marginal per-query costs. "
                    "Cluster idle time is excluded. For SQL warehouses, costs represent actual usage."
                )
            else:
                benchmark_cost.cost_model = "actual"
        elif platform_lower in ["snowflake", "bigquery"] or platform_lower in ["duckdb", "clickhouse"]:
            benchmark_cost.cost_model = "actual"
        else:
            benchmark_cost.cost_model = "estimated"

        # Add pricing staleness warning if applicable
        if is_pricing_stale(threshold_days=90):
            pricing_age = get_pricing_age_days()
            benchmark_cost.warnings.append(
                f"Pricing data is {pricing_age} days old (last updated: {PRICING_LAST_UPDATED}). "
                f"Costs may be inaccurate. Please check for pricing updates."
            )

        # Add storage cost estimate if data was loaded
        if results.data_size_mb and results.data_size_mb > 0:
            from benchbox.core.cost.storage import estimate_storage_cost

            # Convert MB to bytes
            total_bytes = int(results.data_size_mb * 1024 * 1024)

            # Estimate storage duration: minimum 1 hour for benchmarks
            storage_duration_hours = max(1.0, results.duration_seconds / 3600.0)

            region = platform_config.get("region", "us-east-1")
            storage_est = estimate_storage_cost(
                platform=platform,
                total_bytes=total_bytes,
                storage_duration_hours=storage_duration_hours,
                region=region,
            )

            benchmark_cost.storage_cost = storage_est["storage_cost"]
            platform_details["storage_estimate"] = storage_est

            # Add warning about storage cost estimate
            if storage_est["storage_cost"] > 0:
                benchmark_cost.warnings.append(
                    f"Storage cost estimate: ${storage_est['storage_cost']:.4f} for "
                    f"{storage_est['storage_tb']:.2f} TB over {storage_duration_hours:.1f} hours. "
                    f"{storage_est['note']}"
                )

        # Add cost_summary to results
        results.cost_summary = benchmark_cost.to_dict()

        logger.info(f"Cost estimation complete: ${benchmark_cost.total_cost:.4f} across {len(phase_costs)} phases")

    except Exception as e:
        logger.warning(
            f"Failed to add cost estimation to results for platform '{platform}': {e}",
            exc_info=True,
            extra={
                "platform": platform,
                "benchmark_name": results.benchmark_name,
                "scale_factor": results.scale_factor,
            },
        )

    return results


def _extract_platform_config_from_results(results: BenchmarkResults) -> dict[str, Any]:
    """Extract platform configuration from BenchmarkResults.platform_info."""
    config: dict[str, Any] = {}

    if not results.platform_info:
        return config

    platform_info = results.platform_info
    platform_type = platform_info.get("platform_type", "")

    # Common fields
    config["platform_type"] = platform_type

    # Platform-specific extraction
    if platform_type == "snowflake":
        config_section = platform_info.get("configuration", {})
        config["edition"] = platform_info.get("edition", "standard")
        config["cloud"] = platform_info.get("cloud_provider", "aws")
        config["region"] = platform_info.get("region", "us-east-1")
        config["warehouse_size"] = config_section.get("warehouse_size")

    elif platform_type == "bigquery":
        config_section = platform_info.get("configuration", {})
        config["location"] = config_section.get("location", "us")

    elif platform_type == "redshift":
        config_section = platform_info.get("configuration", {})
        cluster_info = platform_info.get("cluster_info", {})
        config["node_type"] = cluster_info.get("node_type", "dc2.large")
        config["node_count"] = cluster_info.get("number_of_nodes", 1)
        config["region"] = platform_info.get("region", "us-east-1")

    elif platform_type == "databricks":
        config_section = platform_info.get("configuration", {})
        compute_config = platform_info.get("compute_configuration", {})

        # Infer cloud from hostname - check both configuration and host fields
        server_hostname = config_section.get("server_hostname", "") or platform_info.get("host", "")
        if "azuredatabricks" in server_hostname:
            cloud = "azure"
        elif "gcp.databricks.com" in server_hostname:
            cloud = "gcp"
        else:
            cloud = "aws"

        config["cloud"] = cloud
        config["tier"] = platform_info.get("tier", "premium")

        # Determine workload type from warehouse metadata
        warehouse_type = compute_config.get("warehouse_type")

        if warehouse_type:
            # Map Databricks warehouse types to workload types for cost calculation
            # SERVERLESS warehouses (detected by adapter as warehouse_type=PRO + enable_serverless_compute=True)
            # are displayed as warehouse_type="SERVERLESS" and map to serverless_sql
            # PRO and CLASSIC warehouses map to sql_compute
            if warehouse_type.upper() == "SERVERLESS":
                config["workload_type"] = "serverless_sql"
            else:
                config["workload_type"] = "sql_compute"
        else:
            config["workload_type"] = "all_purpose"  # Default assumption

        # Extract actual cluster size from warehouse configuration
        warehouse_size = compute_config.get("warehouse_size")
        if warehouse_size:
            # Map warehouse size to DBU per hour
            # See: https://docs.databricks.com/sql/admin/warehouse-types.html
            # Classic/Pro SQL Warehouses:
            # 2X-Small: 1 DBU/hour, X-Small: 2 DBU/hour, Small: 4 DBU/hour
            # Medium: 8 DBU/hour, Large: 16 DBU/hour, X-Large: 32 DBU/hour
            # 2X-Large: 64 DBU/hour, 3X-Large: 128 DBU/hour, 4X-Large: 256 DBU/hour
            size_to_dbu = {
                "2X-Small": 1.0,
                "X-Small": 2.0,
                "Small": 4.0,
                "Medium": 8.0,
                "Large": 16.0,
                "X-Large": 32.0,
                "2X-Large": 64.0,
                "3X-Large": 128.0,
                "4X-Large": 256.0,
            }
            config["cluster_size_dbu_per_hour"] = size_to_dbu.get(warehouse_size, 2.0)
            config["warehouse_size"] = warehouse_size
        else:
            # Fallback: Use conservative estimate
            config["cluster_size_dbu_per_hour"] = 2.0
            logger.warning(
                "Databricks warehouse size not available in platform_info, using conservative estimate of 2.0 DBU/hour. "
                "For accurate cost estimation, ensure databricks-sdk is installed."
            )

    return config


def _calculate_phase_costs(
    results: BenchmarkResults,
    platform: str,
    platform_config: dict[str, Any],
    calculator: CostCalculator,
) -> list[PhaseCost]:
    """Calculate costs for each benchmark phase."""
    phase_costs = []

    # Process execution phases if available
    if results.execution_phases:
        # Power Test Phase
        if results.execution_phases.power_test:
            power_phase = results.execution_phases.power_test
            query_costs = []
            for query_exec in power_phase.query_executions:
                if hasattr(query_exec, "resource_usage") and query_exec.resource_usage:
                    qc = calculator.calculate_query_cost(
                        platform=platform,
                        resource_usage=query_exec.resource_usage,
                        platform_config=platform_config,
                    )
                    if qc:
                        query_costs.append(qc)

            if query_costs:
                phase_cost = calculator.calculate_phase_cost("power_test", query_costs)
                # Add timing context
                if hasattr(power_phase, "duration_ms"):
                    phase_cost.wall_clock_duration_seconds = power_phase.duration_ms / 1000.0
                phase_cost.concurrent_streams = 1  # Power test is sequential
                phase_costs.append(phase_cost)

        # Throughput Test Phase
        if results.execution_phases.throughput_test:
            throughput_phase = results.execution_phases.throughput_test
            query_costs = []
            for stream in throughput_phase.streams:
                for query_exec in stream.query_executions:
                    if hasattr(query_exec, "resource_usage") and query_exec.resource_usage:
                        qc = calculator.calculate_query_cost(
                            platform=platform,
                            resource_usage=query_exec.resource_usage,
                            platform_config=platform_config,
                        )
                        if qc:
                            query_costs.append(qc)

            if query_costs:
                phase_cost = calculator.calculate_phase_cost("throughput_test", query_costs)
                # Add timing context
                if hasattr(throughput_phase, "duration_ms"):
                    phase_cost.wall_clock_duration_seconds = throughput_phase.duration_ms / 1000.0
                if hasattr(throughput_phase, "streams"):
                    phase_cost.concurrent_streams = len(throughput_phase.streams)
                phase_costs.append(phase_cost)

        # Maintenance Test Phase
        if results.execution_phases.maintenance_test:
            maintenance_phase = results.execution_phases.maintenance_test
            query_costs = []
            for query_exec in maintenance_phase.query_executions:
                if hasattr(query_exec, "resource_usage") and query_exec.resource_usage:
                    qc = calculator.calculate_query_cost(
                        platform=platform,
                        resource_usage=query_exec.resource_usage,
                        platform_config=platform_config,
                    )
                    if qc:
                        query_costs.append(qc)

            if query_costs:
                phase_cost = calculator.calculate_phase_cost("data_maintenance", query_costs)
                # Add timing context
                if hasattr(maintenance_phase, "duration_ms"):
                    phase_cost.wall_clock_duration_seconds = maintenance_phase.duration_ms / 1000.0
                phase_cost.concurrent_streams = 1  # Maintenance test is typically sequential
                phase_costs.append(phase_cost)

    # Fallback: If no execution_phases, calculate from flat query_results
    if not phase_costs and results.query_results:
        query_costs = []
        for query_result in results.query_results:
            if isinstance(query_result, dict):
                resource_usage = query_result.get("resource_usage")
                if resource_usage:
                    qc = calculator.calculate_query_cost(
                        platform=platform,
                        resource_usage=resource_usage,
                        platform_config=platform_config,
                    )
                    if qc:
                        query_costs.append(qc)

        if query_costs:
            phase_cost = calculator.calculate_phase_cost("all_queries", query_costs)
            phase_costs.append(phase_cost)

    return phase_costs
