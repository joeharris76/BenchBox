"""Scale factor formatting utilities for consistent naming across BenchBox.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""


def format_scale_factor(scale_factor: float) -> str:
    """Format scale factor for filenames and identifiers.

    Rules:
    - Values >= 1: No leading zero (sf1, sf10, sf100)
    - Values < 1: Leading zero + decimal digits (sf01, sf001, sf0001)
    - Leading zero implies the value is < 1

    Examples:
    - 1.0 -> sf1 (no leading zero)
    - 0.1 -> sf01 (leading zero implies < 1)
    - 0.01 -> sf001 (leading zero implies < 1)
    - 0.001 -> sf0001 (leading zero implies < 1)
    - 10 -> sf10 (no leading zero)
    - 1.5 -> sf15 (remove decimal point, no leading zero)

    Args:
        scale_factor: The scale factor to format

    Returns:
        Formatted scale factor string (e.g., "sf1", "sf01", "sf001")
    """
    if scale_factor >= 1:
        if scale_factor == int(scale_factor):
            # Integer values >= 1: no leading zero
            return f"sf{int(scale_factor)}"
        else:
            # Non-integer values >= 1: remove decimal point
            # 1.5 -> sf15, 2.25 -> sf225
            str_val = f"{scale_factor}".replace(".", "")
            return f"sf{str_val}"
    else:
        # Values < 1: leading zero + decimal digits only
        # Convert to string, remove "0.", add leading zero
        decimal_str = f"{scale_factor:.10f}".rstrip("0")  # Remove trailing zeros
        if "." in decimal_str:
            after_decimal = decimal_str.split(".")[1]
            return f"sf0{after_decimal}"
        else:
            # Edge case: exactly 0
            return "sf0"


def format_benchmark_name(benchmark_name: str, scale_factor: float) -> str:
    """Format benchmark name with scale factor.

    Args:
        benchmark_name: Name of the benchmark (e.g., "tpch", "tpcds")
        scale_factor: Scale factor value

    Returns:
        Formatted benchmark name (e.g., "tpch_sf1", "tpcds_sf01")
    """
    sf_str = format_scale_factor(scale_factor)
    return f"{benchmark_name}_{sf_str}"


def format_data_directory(benchmark_name: str, scale_factor: float) -> str:
    """Format data directory name with scale factor.

    Args:
        benchmark_name: Name of the benchmark
        scale_factor: Scale factor value

    Returns:
        Formatted directory name (e.g., "tpch_sf1_data", "tpcds_sf01_data")
    """
    sf_str = format_scale_factor(scale_factor)
    return f"{benchmark_name}_{sf_str}_data"


def format_schema_name(benchmark_name: str, scale_factor: float) -> str:
    """Format database schema name with scale factor.

    Args:
        benchmark_name: Name of the benchmark
        scale_factor: Scale factor value

    Returns:
        Formatted schema name (e.g., "tpch_sf1", "tpcds_sf01")
    """
    sf_str = format_scale_factor(scale_factor)
    return f"{benchmark_name}_{sf_str}"
