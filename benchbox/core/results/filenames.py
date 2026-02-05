"""Result filename utilities.

Centralizes result filename construction so all exporters and CLI paths
use the same format.
"""

from __future__ import annotations

from datetime import datetime

from benchbox.utils.scale_factor import format_scale_factor


def normalize_platform_for_filename(platform: str) -> str:
    """Normalize platform name for use in filenames.

    - Strips mode suffixes (-df, -sql) to avoid duplication
    - Replaces hyphens with underscores for consistency
    - Lowercases the platform name
    """
    base = platform.lower()
    for suffix in ("-df", "-sql"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base.replace("-", "_")


def abbreviate_mode_for_filename(mode: str) -> str:
    """Abbreviate execution mode for filenames.

    - 'dataframe' -> 'df'
    - 'sql' -> 'sql'
    - 'data_only' -> 'data'
    """
    if mode == "dataframe":
        return "df"
    if mode == "data_only":
        return "data"
    return mode


def build_result_filename_base(
    benchmark_id: str,
    scale_factor: float,
    platform: str,
    timestamp: str | datetime,
    execution_id: str | None = None,
    mode: str | None = None,
) -> str:
    """Build a consistent base filename for result exports (no extension).

    Args:
        benchmark_id: Benchmark identifier (e.g., 'tpch', 'tpcds')
        scale_factor: Scale factor value
        platform: Platform name (e.g., 'duckdb', 'datafusion-df')
        timestamp: Timestamp string or datetime object
        execution_id: Optional execution identifier
        mode: Optional execution mode ('sql', 'dataframe', 'data_only').
              When provided, includes normalized platform + abbreviated mode.
    """
    if isinstance(timestamp, datetime):
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    else:
        timestamp_str = str(timestamp)

    sf_str = format_scale_factor(scale_factor)

    # Normalize platform and include mode if provided
    if mode:
        normalized_platform = normalize_platform_for_filename(platform)
        abbrev_mode = abbreviate_mode_for_filename(mode)
        platform_part = f"{normalized_platform}_{abbrev_mode}"
    else:
        # Legacy behavior: just replace hyphens with underscores
        platform_part = platform.lower().replace("-", "_")

    base = f"{benchmark_id}_{sf_str}_{platform_part}_{timestamp_str}"
    if execution_id:
        base = f"{base}_{execution_id}"
    return base


def build_result_filename(
    benchmark_id: str,
    scale_factor: float,
    platform: str,
    timestamp: str | datetime,
    execution_id: str | None = None,
    extension: str = "json",
    mode: str | None = None,
) -> str:
    """Build a consistent filename for result exports (with extension)."""
    base = build_result_filename_base(
        benchmark_id=benchmark_id,
        scale_factor=scale_factor,
        platform=platform,
        timestamp=timestamp,
        execution_id=execution_id,
        mode=mode,
    )
    return f"{base}.{extension}"
