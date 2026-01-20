"""Unified adapter factory for SQL and DataFrame execution modes.

This module provides a single entry point for getting platform adapters,
abstracting the difference between SQL and DataFrame execution modes,
and routing to appropriate deployment modes (local, self-hosted, managed).

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Any, Literal, Optional

from benchbox.core.platform_registry import PlatformRegistry


def _normalize_platform_name(platform: str) -> tuple[str, bool, Optional[str]]:
    """Normalize platform name, detect DataFrame mode, and extract deployment mode.

    Handles two suffix conventions:
    1. `-df` suffix for DataFrame platforms
    2. `:deployment` suffix for deployment modes

    These can be combined: `databricks-df:serverless`

    Examples:
        - `polars-df` -> (`polars`, True, None)       - DataFrame mode implied
        - `clickhouse:cloud` -> (`clickhouse`, False, 'cloud')  - Deployment implied
        - `clickhouse:local` -> (`clickhouse`, False, 'local')  - Deployment implied
        - `firebolt:core` -> (`firebolt`, False, 'core')        - Deployment implied
        - `databricks-df:serverless` -> (`databricks`, True, 'serverless')  - Both
        - `polars` -> (`polars`, False, None)         - Use platform defaults
        - `duckdb` -> (`duckdb`, False, None)         - Use platform defaults

    Args:
        platform: Raw platform name from CLI or config

    Returns:
        Tuple of (base_platform_name, is_df_mode_implied, deployment_mode)
    """
    platform_lower = platform.lower()
    deployment_mode: Optional[str] = None
    df_mode_implied = False

    # First, check for :deployment suffix (must check before -df to handle combined case)
    if ":" in platform_lower:
        base_part, deployment_mode = platform_lower.rsplit(":", 1)
        platform_lower = base_part

    # Then check for -df suffix
    if platform_lower.endswith("-df"):
        # Strip -df suffix and indicate DataFrame mode is implied
        platform_lower = platform_lower[:-3]
        df_mode_implied = True

    return platform_lower, df_mode_implied, deployment_mode


def get_adapter(
    platform: str,
    mode: Optional[Literal["sql", "dataframe"]] = None,
    deployment: Optional[str] = None,
    **config: Any,
) -> Any:
    """Get adapter for platform, execution mode, and deployment mode.

    This is the unified entry point for obtaining platform adapters. It validates
    that the platform supports the requested mode and deployment, and routes to
    the appropriate adapter factory.

    Args:
        platform: Platform name with optional suffixes:
            - `-df` suffix for DataFrame mode (e.g., 'polars-df', 'pandas-df')
            - `:deployment` suffix for deployment mode (e.g., 'clickhouse:cloud')
            - Combined (e.g., 'databricks-df:serverless')
        mode: Execution mode ('sql' or 'dataframe'). If None, uses platform default
              (or 'dataframe' if platform has -df suffix).
        deployment: Deployment mode ('local', 'server', 'cloud', etc.). If None,
            uses deployment from platform name suffix or platform default.
        **config: Platform-specific configuration options

    Returns:
        Platform adapter instance (either SQL PlatformAdapter or DataFrame adapter)

    Raises:
        ValueError: If platform does not support the requested mode or deployment
        ImportError: If required dependencies are not installed

    Examples:
        >>> adapter = get_adapter("duckdb")  # Local DuckDB (default)
        >>> adapter = get_adapter("clickhouse:cloud")  # ClickHouse Cloud
        >>> adapter = get_adapter("clickhouse", deployment="local")  # chDB
        >>> adapter = get_adapter("polars-df")  # Polars DataFrame mode
    """
    # Normalize platform name (strip -df suffix, extract :deployment suffix)
    base_platform, df_mode_implied, deployment_from_name = _normalize_platform_name(platform)

    # Get capabilities and resolve mode
    caps = PlatformRegistry.get_platform_capabilities(base_platform)
    if caps is None:
        raise ValueError(f"Unknown platform: {platform}")

    # Resolve execution mode: explicit mode > df suffix implied > platform default
    if mode is not None:
        resolved_mode = mode
    elif df_mode_implied:
        resolved_mode = "dataframe"
    else:
        resolved_mode = caps.default_mode

    # Validate execution mode support
    if not PlatformRegistry.supports_mode(base_platform, resolved_mode):
        mode_support = []
        if caps.supports_sql:
            mode_support.append("sql")
        if caps.supports_dataframe:
            mode_support.append("dataframe")
        supported = ", ".join(mode_support) if mode_support else "none"
        raise ValueError(f"Platform '{platform}' does not support {resolved_mode} mode. Supported modes: {supported}")

    # Resolve deployment mode: explicit > from name suffix > platform default
    resolved_deployment = _resolve_deployment_mode(base_platform, deployment, deployment_from_name, caps)

    # Validate deployment mode support
    if resolved_deployment is not None:
        if not PlatformRegistry.supports_deployment_mode(base_platform, resolved_deployment):
            available = PlatformRegistry.get_available_deployment_modes(base_platform)
            if available:
                available_str = ", ".join(available)
                raise ValueError(
                    f"Platform '{base_platform}' does not support deployment mode '{resolved_deployment}'. "
                    f"Available: {available_str}"
                )
            else:
                raise ValueError(
                    f"Platform '{base_platform}' does not support deployment modes. "
                    f"Remove the ':{resolved_deployment}' suffix."
                )

    # Pass deployment_mode to adapters via config if resolved
    if resolved_deployment is not None:
        config["deployment_mode"] = resolved_deployment

    # Route to appropriate adapter factory
    if resolved_mode == "sql":
        return _get_sql_adapter(base_platform, **config)
    else:
        return _get_dataframe_adapter(base_platform, **config)


def _resolve_deployment_mode(
    platform: str,
    explicit_deployment: Optional[str],
    deployment_from_name: Optional[str],
    caps: Any,
) -> Optional[str]:
    """Resolve deployment mode with priority: explicit > name suffix > platform default.

    Args:
        platform: Base platform name
        explicit_deployment: Explicitly requested deployment mode (highest priority)
        deployment_from_name: Deployment mode extracted from platform name suffix
        caps: Platform capabilities

    Returns:
        Resolved deployment mode, or None if platform has no deployment modes
    """
    # If platform has no deployment modes, return None
    if not caps.deployment_modes:
        # If user explicitly requested a deployment, that's an error
        # (will be caught by validation in caller)
        if explicit_deployment or deployment_from_name:
            return explicit_deployment or deployment_from_name
        return None

    # Priority 1: Explicit deployment parameter
    if explicit_deployment is not None:
        return explicit_deployment

    # Priority 2: Deployment from platform name suffix
    if deployment_from_name is not None:
        return deployment_from_name

    # Priority 3: Platform default deployment
    return caps.default_deployment


def _get_sql_adapter(platform: str, **config: Any) -> Any:
    """Get SQL adapter for platform.

    Args:
        platform: Platform name
        **config: Platform configuration

    Returns:
        SQL PlatformAdapter instance
    """
    # Import here to avoid circular imports
    from benchbox.platforms import get_platform_adapter

    return get_platform_adapter(platform, **config)


def _get_dataframe_adapter(platform: str, **config: Any) -> Any:
    """Get DataFrame adapter for platform.

    Args:
        platform: Platform name (without -df suffix)
        **config: Platform configuration

    Returns:
        DataFrame adapter instance
    """
    # Import here to avoid circular imports
    from benchbox.platforms.dataframe import (
        CUDF_AVAILABLE,
        DASK_AVAILABLE,
        DATAFUSION_DF_AVAILABLE,
        MODIN_AVAILABLE,
        PANDAS_AVAILABLE,
        POLARS_AVAILABLE,
        PYSPARK_AVAILABLE,
        CuDFDataFrameAdapter,
        DaskDataFrameAdapter,
        DataFusionDataFrameAdapter,
        ModinDataFrameAdapter,
        PandasDataFrameAdapter,
        PolarsDataFrameAdapter,
        PySparkDataFrameAdapter,
    )

    adapter_mapping = {
        "polars": (PolarsDataFrameAdapter, POLARS_AVAILABLE, "uv add polars"),
        "pandas": (PandasDataFrameAdapter, PANDAS_AVAILABLE, "uv add pandas"),
        "modin": (ModinDataFrameAdapter, MODIN_AVAILABLE, "uv add modin[ray]"),
        "cudf": (CuDFDataFrameAdapter, CUDF_AVAILABLE, "pip install cudf-cu12"),
        "dask": (DaskDataFrameAdapter, DASK_AVAILABLE, "uv add dask[distributed]"),
        "pyspark": (PySparkDataFrameAdapter, PYSPARK_AVAILABLE, "uv add benchbox --extra dataframe-pyspark"),
        "datafusion": (DataFusionDataFrameAdapter, DATAFUSION_DF_AVAILABLE, "uv add datafusion"),
    }

    if platform not in adapter_mapping:
        available = ", ".join(sorted(adapter_mapping.keys()))
        raise ValueError(f"Unknown DataFrame platform: {platform}. Available: {available}")

    adapter_class, is_available, install_cmd = adapter_mapping[platform]

    if not is_available or adapter_class is None:
        raise ImportError(
            f"DataFrame platform '{platform}' is not available. Install required dependencies: {install_cmd}"
        )

    return adapter_class(**config)


def is_dataframe_mode(platform: str, mode: Optional[str] = None) -> bool:
    """Check if the effective execution mode is dataframe.

    Args:
        platform: Platform name (may include -df suffix or :deployment suffix)
        mode: Explicit mode or None for default

    Returns:
        True if effective mode is dataframe
    """
    if mode is not None:
        return mode == "dataframe"

    # Check if -df suffix implies DataFrame mode
    base_platform, df_mode_implied, _ = _normalize_platform_name(platform)
    if df_mode_implied:
        return True

    return PlatformRegistry.get_default_mode(base_platform) == "dataframe"


def get_available_modes(platform: str) -> list[str]:
    """Get available execution modes for a platform.

    Args:
        platform: Platform name (may include -df suffix or :deployment suffix)

    Returns:
        List of supported modes ('sql', 'dataframe', or both)
    """
    base_platform, _, _ = _normalize_platform_name(platform)
    caps = PlatformRegistry.get_platform_capabilities(base_platform)
    if caps is None:
        return []

    modes = []
    if caps.supports_sql:
        modes.append("sql")
    if caps.supports_dataframe:
        modes.append("dataframe")
    return modes


def get_available_deployments(platform: str) -> list[str]:
    """Get available deployment modes for a platform.

    Args:
        platform: Platform name (may include -df suffix or :deployment suffix)

    Returns:
        List of supported deployment modes (e.g., ['local', 'server', 'cloud'])
    """
    base_platform, _, _ = _normalize_platform_name(platform)
    return PlatformRegistry.get_available_deployment_modes(base_platform)


def get_default_deployment(platform: str) -> Optional[str]:
    """Get the default deployment mode for a platform.

    Args:
        platform: Platform name (may include -df suffix or :deployment suffix)

    Returns:
        Default deployment mode or None if platform has no deployment modes
    """
    base_platform, _, _ = _normalize_platform_name(platform)
    return PlatformRegistry.get_default_deployment(base_platform)
