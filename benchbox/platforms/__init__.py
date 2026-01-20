"""Database platform adapters for optimized benchmark execution.

Provides database-specific optimizations for benchmark execution,
separating benchmark logic from platform-specific implementation details.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Optional, Type

from benchbox.core.platform_registry import PlatformRegistry
from benchbox.utils.runtime_env import ensure_driver_version

from .base import BenchmarkResults, ConnectionConfig, PlatformAdapter

# Import local platform adapters
try:
    from .duckdb import DuckDBAdapter
except ImportError:
    DuckDBAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .motherduck import MotherDuckAdapter
except ImportError:
    MotherDuckAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .sqlite import SQLiteAdapter
except ImportError:
    SQLiteAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .datafusion import DataFusionAdapter
except ImportError:
    DataFusionAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .polars_platform import PolarsAdapter
except ImportError:
    PolarsAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

# Import cloud platform adapters with graceful fallback
# ClickHouse is optional and may not be included in pip-installed packages
try:
    from . import clickhouse as _clickhouse_module
    from .clickhouse import ClickHouseAdapter

    # Provide module access for legacy patches/tests (only if clickhouse is available)
    clickhouse = _clickhouse_module
except ImportError:
    _clickhouse_module = None  # type: ignore[assignment]
    clickhouse = None  # type: ignore[assignment]
    ClickHouseAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .databricks import DatabricksAdapter
except ImportError:
    DatabricksAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .bigquery import BigQueryAdapter
except ImportError:
    BigQueryAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .redshift import RedshiftAdapter
except ImportError:
    RedshiftAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .snowflake import SnowflakeAdapter
except ImportError:
    SnowflakeAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .trino import TrinoAdapter
except ImportError:
    TrinoAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .athena import AthenaAdapter
except ImportError:
    AthenaAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .spark import SparkAdapter
except ImportError:
    SparkAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .pyspark import PySparkSQLAdapter
except ImportError:
    PySparkSQLAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .firebolt import FireboltAdapter
except ImportError:
    FireboltAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .influxdb import InfluxDBAdapter
except ImportError:
    InfluxDBAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .presto import PrestoAdapter
except ImportError:
    PrestoAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .postgresql import PostgreSQLAdapter
except ImportError:
    PostgreSQLAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .timescaledb import TimescaleDBAdapter
except ImportError:
    TimescaleDBAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .azure_synapse import AzureSynapseAdapter
except ImportError:
    AzureSynapseAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

try:
    from .fabric_warehouse import FabricWarehouseAdapter, MicrosoftFabricAdapter
except ImportError:
    FabricWarehouseAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]
    MicrosoftFabricAdapter: Optional[Type[PlatformAdapter]] = None  # type: ignore[assignment,misc]

# DataFrame platform adapters - these use a different interface (DataFrame API instead of SQL)
# Import availability flags and adapters with graceful fallback
try:
    from .dataframe import POLARS_AVAILABLE, PolarsDataFrameAdapter
except ImportError:
    POLARS_AVAILABLE = False
    PolarsDataFrameAdapter = None  # type: ignore[assignment,misc]

try:
    from .dataframe import PANDAS_AVAILABLE, PandasDataFrameAdapter
except ImportError:
    PANDAS_AVAILABLE = False
    PandasDataFrameAdapter = None  # type: ignore[assignment,misc]

try:
    from .dataframe import MODIN_AVAILABLE, ModinDataFrameAdapter
except ImportError:
    MODIN_AVAILABLE = False
    ModinDataFrameAdapter = None  # type: ignore[assignment,misc]

try:
    from .dataframe import CUDF_AVAILABLE, CuDFDataFrameAdapter
except ImportError:
    CUDF_AVAILABLE = False
    CuDFDataFrameAdapter = None  # type: ignore[assignment,misc]

try:
    from .dataframe import DASK_AVAILABLE, DaskDataFrameAdapter
except ImportError:
    DASK_AVAILABLE = False
    DaskDataFrameAdapter = None  # type: ignore[assignment,misc]

try:
    from .dataframe import DATAFUSION_DF_AVAILABLE, DataFusionDataFrameAdapter
except ImportError:
    DATAFUSION_DF_AVAILABLE = False
    DataFusionDataFrameAdapter = None  # type: ignore[assignment,misc]

# Import platform checker for DataFrame platform status
try:
    from .dataframe import DataFramePlatformChecker
except ImportError:
    DataFramePlatformChecker = None  # type: ignore[assignment,misc]

try:
    from .dataframe import PYSPARK_AVAILABLE, PySparkDataFrameAdapter
except ImportError:
    PYSPARK_AVAILABLE = False
    PySparkDataFrameAdapter = None  # type: ignore[assignment,misc]

__all__ = [
    "PlatformAdapter",
    "ConnectionConfig",
    "BenchmarkResults",
    "DuckDBAdapter",
    "MotherDuckAdapter",
    "DataFusionAdapter",
    "PolarsAdapter",
    "ClickHouseAdapter",
    "DatabricksAdapter",
    "BigQueryAdapter",
    "RedshiftAdapter",
    "SnowflakeAdapter",
    "TrinoAdapter",
    "AthenaAdapter",
    "SparkAdapter",
    "PySparkSQLAdapter",
    "FireboltAdapter",
    "InfluxDBAdapter",
    "PrestoAdapter",
    "PostgreSQLAdapter",
    "AzureSynapseAdapter",
    "FabricWarehouseAdapter",
    "MicrosoftFabricAdapter",  # Backward compatibility alias
    # DataFrame adapters
    "PolarsDataFrameAdapter",
    "PandasDataFrameAdapter",
    "ModinDataFrameAdapter",
    "CuDFDataFrameAdapter",
    "DaskDataFrameAdapter",
    "DataFusionDataFrameAdapter",
    "PySparkDataFrameAdapter",
    "POLARS_AVAILABLE",
    "PANDAS_AVAILABLE",
    "MODIN_AVAILABLE",
    "CUDF_AVAILABLE",
    "DASK_AVAILABLE",
    "DATAFUSION_DF_AVAILABLE",
    "PYSPARK_AVAILABLE",
    "DataFramePlatformChecker",
    # Unified adapter factory (--mode and --deployment flag support)
    "get_adapter",
    "is_dataframe_mode",
    "get_available_modes",
    "get_available_deployments",
    "get_default_deployment",
    # Functions
    "get_platform_adapter",
    "get_dataframe_adapter",
    "list_available_platforms",
    "list_available_dataframe_platforms",
    "get_platform_requirements",
    "get_dataframe_requirements",
    "check_platform_connectivity",
    "is_dataframe_platform",
]


# Import unified adapter factory
from benchbox.platforms.adapter_factory import (
    get_adapter,
    get_available_deployments,
    get_available_modes,
    get_default_deployment,
    is_dataframe_mode,
)


def get_platform_adapter(platform_name: str, **config) -> PlatformAdapter:
    """Factory function to create platform adapters.

    This function delegates adapter lookup to PlatformRegistry (the single source
    of truth for platform definitions) while handling CLI-specific concerns like
    error messages and driver version resolution.

    Args:
        platform_name: Name of the platform (aliases like 'sqlite3' are resolved)
        **config: Platform-specific configuration

    Returns:
        Configured platform adapter instance

    Raises:
        ValueError: If platform is not supported
        ImportError: If platform dependencies are not installed
    """
    # Resolve aliases and normalize to canonical name via PlatformRegistry
    canonical_name = PlatformRegistry.resolve_platform_name(platform_name)

    # Get adapter class from registry (single source of truth)
    try:
        adapter_class = PlatformRegistry.get_adapter_class(canonical_name)
    except ValueError:
        # Platform not registered - provide helpful error with available platforms
        available = ", ".join(PlatformRegistry.get_available_platforms())
        raise ValueError(f"Unsupported platform: {platform_name}. Available: {available}")

    # Check if adapter class is actually available (deps installed)
    if adapter_class is None:
        platform_info = PlatformRegistry.get_platform_info(canonical_name)
        install_cmd = platform_info.installation_command if platform_info else "unknown"
        raise ImportError(f"Platform '{platform_name}' is not available. Install required dependencies: {install_cmd}")

    driver_package = config.pop("driver_package", None)
    driver_version = config.pop("driver_version", None)
    driver_version_resolved = config.pop("driver_version_resolved", None)
    driver_auto_install = bool(config.pop("driver_auto_install", False))

    # Get platform info for driver metadata (already resolved to canonical name)
    platform_info = PlatformRegistry.get_platform_info(canonical_name)
    install_hint = platform_info.installation_command if platform_info else "unknown"
    package_hint = driver_package or (platform_info.driver_package if platform_info else None)
    requested_version = driver_version or driver_version_resolved

    resolution = ensure_driver_version(
        package_name=package_hint,
        requested_version=requested_version,
        auto_install=driver_auto_install,
        install_hint=install_hint,
    )

    resolved_version = resolution.resolved or driver_version_resolved
    requested = resolution.requested or driver_version

    # Use from_config() if adapter supports config-aware initialization (e.g., Databricks, Snowflake)
    # This enables proper schema naming based on benchmark/scale/tuning configuration
    if hasattr(adapter_class, "from_config") and callable(adapter_class.from_config):
        adapter_instance = adapter_class.from_config(config)
    else:
        # Simple adapters use direct constructor (e.g., DuckDB, SQLite)
        adapter_instance = adapter_class(**config)

    # Attach driver metadata for downstream consumers (CLI summaries, exports).
    adapter_instance.driver_package = resolution.package or package_hint
    adapter_instance.driver_version_requested = requested
    adapter_instance.driver_version_resolved = resolved_version
    adapter_instance.driver_auto_install_used = resolution.auto_install_used or driver_auto_install

    return adapter_instance


def list_available_platforms() -> dict[str, bool]:
    """List all platforms and their availability status.

    Delegates to PlatformRegistry.get_platform_availability() which is
    the single source of truth for platform availability.

    Returns:
        Dictionary mapping platform names to availability boolean.
    """
    return PlatformRegistry.get_platform_availability()


def get_platform_requirements(platform_name: str) -> str:
    """Get installation requirements for a platform.

    Delegates to PlatformRegistry.get_platform_requirements() which is
    the single source of truth for platform metadata.

    Args:
        platform_name: Name of the platform (aliases are resolved automatically)

    Returns:
        Installation command string
    """
    return PlatformRegistry.get_platform_requirements(platform_name)


def check_platform_connectivity(platform_name: str, **config) -> bool:
    """Check connectivity to a platform using its adapter.

    Args:
        platform_name: Name of the platform to test
        **config: Platform configuration

    Returns:
        True if connection successful, False otherwise
    """
    try:
        adapter = get_platform_adapter(platform_name, **config)
        return adapter.test_connection()
    except Exception:
        return False


# ============================================================================
# DataFrame Platform Support
# ============================================================================


def get_dataframe_adapter(platform_name: str, **config):
    """Factory function to create DataFrame platform adapters.

    DataFrame adapters use native DataFrame APIs (e.g., Polars expressions,
    Pandas operations) instead of SQL for query execution.

    Args:
        platform_name: Name of the DataFrame platform ('polars-df', 'pandas-df', etc.)
        **config: Platform-specific configuration options

    Returns:
        DataFrame adapter instance

    Raises:
        ValueError: If platform is not a recognized DataFrame platform
        ImportError: If required dependencies are not installed
    """
    dataframe_mapping = {
        "polars-df": PolarsDataFrameAdapter,
        "pandas-df": PandasDataFrameAdapter,
        "modin-df": ModinDataFrameAdapter,
        "cudf-df": CuDFDataFrameAdapter,
        "dask-df": DaskDataFrameAdapter,
        "datafusion-df": DataFusionDataFrameAdapter,
        "pyspark-df": PySparkDataFrameAdapter,
    }

    platform_lower = platform_name.lower()

    if platform_lower not in dataframe_mapping:
        available = ", ".join(sorted(dataframe_mapping.keys()))
        raise ValueError(f"Unknown DataFrame platform: {platform_name}. Available: {available}")

    adapter_class = dataframe_mapping[platform_lower]

    if adapter_class is None:
        requirements = get_dataframe_requirements(platform_lower)
        raise ImportError(
            f"DataFrame platform '{platform_name}' is not available. Install required dependencies: {requirements}"
        )

    return adapter_class(**config)


def list_available_dataframe_platforms() -> dict[str, bool]:
    """List all DataFrame platforms and their availability status.

    Returns:
        Dictionary mapping platform name to availability boolean
    """
    platforms = {
        "polars-df": POLARS_AVAILABLE,
        "pandas-df": PANDAS_AVAILABLE,
        "modin-df": MODIN_AVAILABLE,
        "cudf-df": CUDF_AVAILABLE,
        "dask-df": DASK_AVAILABLE,
        "datafusion-df": DATAFUSION_DF_AVAILABLE,
        "pyspark-df": PYSPARK_AVAILABLE,
    }
    return platforms


def get_dataframe_requirements(platform_name: str) -> str:
    """Get installation requirements for a DataFrame platform.

    Args:
        platform_name: Name of the DataFrame platform

    Returns:
        Installation command string
    """
    requirements = {
        "polars-df": "uv add polars (core dependency - should be installed)",
        "pandas-df": "uv add benchbox --extra dataframe-pandas",
        "modin-df": "uv add modin[ray] (or modin[dask])",
        "cudf-df": "pip install cudf-cu12 (requires NVIDIA GPU with CUDA)",
        "dask-df": "uv add dask[distributed]",
        "datafusion-df": "uv add datafusion (or uv add benchbox --extra datafusion)",
        "pyspark-df": "uv add benchbox --extra dataframe-pyspark",
    }

    return requirements.get(platform_name.lower(), "Unknown DataFrame platform")


def is_dataframe_platform(platform_name: str) -> bool:
    """Check if a platform name refers to a DataFrame platform.

    Args:
        platform_name: Platform name to check

    Returns:
        True if the platform is a DataFrame platform
    """
    return platform_name.lower() in {
        "polars-df",
        "pandas-df",
        "modin-df",
        "cudf-df",
        "dask-df",
        "datafusion-df",
        "pyspark-df",
    }


# ============================================================================
# Platform Hook Registration
# ============================================================================
# IMPORTANT: This registration block MUST be at the bottom of the module to
# avoid circular import issues.
#
# The registration imports benchbox.cli.platform_hooks, which imports
# benchbox.core.platform_registry, which can trigger re-entrant imports of
# benchbox.platforms. By placing this registration at the very bottom, we
# ensure that all platform imports (clickhouse, databricks, etc.) have
# completed before we import PlatformHookRegistry.
#
# Previous circular import chain that this placement prevents:
#   1. from benchbox.platforms.databricks import DatabricksAdapter
#   2. benchbox/platforms/__init__.py starts loading
#   3. Line 14: imports platform_registry → triggers platforms.base import
#   4. Lines 32-40: tries to import clickhouse module
#   5. clickhouse/__init__.py:27 imports platform_hooks
#   6. platform_hooks imports platform_registry
#   7. platform_registry tries to re-import platforms
#   8. ERROR: platforms is partially initialized (stuck at clickhouse import)
#
# With this placement at the bottom, all imports complete before registration.
# ============================================================================

# Register config builders for platforms that can't do it themselves due to circular imports
# (specifically, package-structured platforms like databricks)
try:
    from benchbox.cli.platform_hooks import PlatformHookRegistry, PlatformOptionSpec, parse_bool

    # Register Databricks config builder (can't register in databricks/__init__.py due to circular import)
    if DatabricksAdapter is not None:
        from benchbox.platforms.databricks import _build_databricks_config

        PlatformHookRegistry.register_config_builder("databricks", _build_databricks_config)

        # Register Databricks platform options
        PlatformHookRegistry.register_option_specs(
            "databricks",
            PlatformOptionSpec(
                name="uc_catalog",
                help="Unity Catalog catalog name for staging data",
            ),
            PlatformOptionSpec(
                name="uc_schema",
                help="Unity Catalog schema name for staging data",
            ),
            PlatformOptionSpec(
                name="uc_volume",
                help="Unity Catalog volume name for staging data",
            ),
            PlatformOptionSpec(
                name="staging_root",
                help="Cloud storage path for staging data (e.g., dbfs:/Volumes/..., s3://..., abfss://...)",
            ),
        )

    # Register BigQuery config builder and platform options
    if BigQueryAdapter is not None:
        from benchbox.platforms.bigquery import _build_bigquery_config

        PlatformHookRegistry.register_config_builder("bigquery", _build_bigquery_config)

        # Register BigQuery platform options
        PlatformHookRegistry.register_option_specs(
            "bigquery",
            PlatformOptionSpec(
                name="staging_root",
                help="GCS path for staging data (e.g., gs://bucket/path)",
            ),
            PlatformOptionSpec(
                name="storage_bucket",
                help="GCS bucket name for data staging (alternative to staging_root)",
            ),
            PlatformOptionSpec(
                name="storage_prefix",
                help="GCS path prefix within bucket for data staging",
            ),
        )

    # Register Trino config builder and platform options
    if TrinoAdapter is not None:
        from benchbox.platforms.trino import _build_trino_config

        PlatformHookRegistry.register_config_builder("trino", _build_trino_config)

        # Register Trino platform options
        PlatformHookRegistry.register_option_specs(
            "trino",
            PlatformOptionSpec(
                name="catalog",
                help="Trino catalog to use (e.g., hive, iceberg, memory). Auto-discovered if not specified.",
            ),
            PlatformOptionSpec(
                name="staging_root",
                help="Cloud storage path for staging data (e.g., s3://..., gs://..., abfss://...)",
            ),
            PlatformOptionSpec(
                name="table_format",
                help="Table format for creating tables (memory, hive, iceberg, delta)",
                default="memory",
            ),
            PlatformOptionSpec(
                name="source_catalog",
                help="Source catalog for external data loading (e.g., hive connector)",
            ),
        )

    # Register Firebolt config builder and platform options
    if FireboltAdapter is not None:
        from benchbox.platforms.firebolt import _build_firebolt_config

        PlatformHookRegistry.register_config_builder("firebolt", _build_firebolt_config)

        # Register Firebolt platform options
        PlatformHookRegistry.register_option_specs(
            "firebolt",
            PlatformOptionSpec(
                name="firebolt_mode",
                help="Explicit Firebolt mode: 'core' for local Docker, 'cloud' for managed Firebolt",
            ),
            PlatformOptionSpec(
                name="url",
                help="Firebolt Core endpoint URL (default: http://localhost:3473)",
                default="http://localhost:3473",
            ),
            PlatformOptionSpec(
                name="client_id",
                help="Firebolt Cloud OAuth client ID",
            ),
            PlatformOptionSpec(
                name="client_secret",
                help="Firebolt Cloud OAuth client secret",
            ),
            PlatformOptionSpec(
                name="account_name",
                help="Firebolt Cloud account name",
            ),
            PlatformOptionSpec(
                name="engine_name",
                help="Firebolt Cloud engine name",
            ),
            PlatformOptionSpec(
                name="api_endpoint",
                help="Firebolt Cloud API endpoint",
                default="api.app.firebolt.io",
            ),
        )

    # Register Presto config builder and platform options
    if PrestoAdapter is not None:
        from benchbox.platforms.presto import _build_presto_config

        PlatformHookRegistry.register_config_builder("presto", _build_presto_config)

        # Register Presto platform options
        PlatformHookRegistry.register_option_specs(
            "presto",
            PlatformOptionSpec(
                name="catalog",
                help="Presto catalog to use (e.g., hive, memory). Auto-discovered if not specified.",
            ),
            PlatformOptionSpec(
                name="staging_root",
                help="Cloud storage path for staging data (e.g., s3://..., gs://...)",
            ),
            PlatformOptionSpec(
                name="table_format",
                help="Table format for creating tables (memory, hive)",
                default="memory",
            ),
            PlatformOptionSpec(
                name="source_catalog",
                help="Source catalog for external data loading (e.g., hive connector)",
            ),
        )

    # Register PostgreSQL config builder and platform options
    if PostgreSQLAdapter is not None:
        from benchbox.platforms.postgresql import _build_postgresql_config

        PlatformHookRegistry.register_config_builder("postgresql", _build_postgresql_config)

        # Register PostgreSQL platform options
        PlatformHookRegistry.register_option_specs(
            "postgresql",
            PlatformOptionSpec(
                name="host",
                help="PostgreSQL server hostname",
                default="localhost",
            ),
            PlatformOptionSpec(
                name="port",
                help="PostgreSQL server port",
                default="5432",
            ),
            PlatformOptionSpec(
                name="database",
                help="PostgreSQL database name (auto-generated if not specified)",
            ),
            PlatformOptionSpec(
                name="username",
                help="PostgreSQL username",
                default="postgres",
            ),
            PlatformOptionSpec(
                name="password",
                help="PostgreSQL password",
            ),
            PlatformOptionSpec(
                name="schema",
                help="PostgreSQL schema name",
                default="public",
            ),
            PlatformOptionSpec(
                name="work_mem",
                help="PostgreSQL work_mem setting for queries",
                default="256MB",
            ),
            PlatformOptionSpec(
                name="enable_timescale",
                help="Enable TimescaleDB extensions if available",
                default="false",
            ),
        )

    # Register TimescaleDB config builder and platform options
    if TimescaleDBAdapter is not None:
        from benchbox.platforms.timescaledb import _build_timescaledb_config

        PlatformHookRegistry.register_config_builder("timescaledb", _build_timescaledb_config)

        # Register TimescaleDB platform options
        PlatformHookRegistry.register_option_specs(
            "timescaledb",
            PlatformOptionSpec(
                name="host",
                help="TimescaleDB server hostname",
                default="localhost",
            ),
            PlatformOptionSpec(
                name="port",
                help="TimescaleDB server port",
                default="5432",
            ),
            PlatformOptionSpec(
                name="database",
                help="TimescaleDB database name (auto-generated if not specified)",
            ),
            PlatformOptionSpec(
                name="username",
                help="TimescaleDB username",
                default="postgres",
            ),
            PlatformOptionSpec(
                name="password",
                help="TimescaleDB password",
            ),
            PlatformOptionSpec(
                name="schema",
                help="TimescaleDB schema name",
                default="public",
            ),
            PlatformOptionSpec(
                name="chunk_interval",
                help="Chunk time interval for hypertables (e.g., '1 day', '1 week')",
                default="1 day",
            ),
            PlatformOptionSpec(
                name="compression_enabled",
                help="Enable compression on hypertables",
                default="false",
            ),
            PlatformOptionSpec(
                name="compression_after",
                help="Compress chunks older than this interval (e.g., '7 days')",
                default="7 days",
            ),
        )

    # Register Azure Synapse config builder and platform options
    if AzureSynapseAdapter is not None:
        from benchbox.platforms.azure_synapse import _build_synapse_config

        PlatformHookRegistry.register_config_builder("synapse", _build_synapse_config)

        # Register Azure Synapse platform options
        PlatformHookRegistry.register_option_specs(
            "synapse",
            PlatformOptionSpec(
                name="server",
                help="Azure Synapse server endpoint (e.g., myworkspace.sql.azuresynapse.net)",
            ),
            PlatformOptionSpec(
                name="database",
                help="Azure Synapse database name (auto-generated if not specified)",
            ),
            PlatformOptionSpec(
                name="username",
                help="Azure Synapse username",
            ),
            PlatformOptionSpec(
                name="password",
                help="Azure Synapse password",
            ),
            PlatformOptionSpec(
                name="auth_method",
                help="Authentication method: sql, aad_password, or aad_msi",
                default="sql",
            ),
            PlatformOptionSpec(
                name="storage_account",
                help="Azure storage account for data staging",
            ),
            PlatformOptionSpec(
                name="container",
                help="Azure blob container name",
            ),
            PlatformOptionSpec(
                name="storage_sas_token",
                help="SAS token for Azure storage access",
            ),
            PlatformOptionSpec(
                name="resource_class",
                help="Workload resource class (e.g., staticrc20, staticrc30)",
                default="staticrc20",
            ),
        )

    # Register Microsoft Fabric Warehouse config builder and platform options
    if FabricWarehouseAdapter is not None:
        # Fabric uses from_config pattern, no separate config builder needed
        # (the adapter's from_config method handles configuration)

        # Register Fabric Warehouse platform options
        PlatformHookRegistry.register_option_specs(
            "fabric_dw",
            PlatformOptionSpec(
                name="server",
                help="Fabric warehouse endpoint (e.g., workspace-guid.datawarehouse.fabric.microsoft.com)",
            ),
            PlatformOptionSpec(
                name="workspace",
                help="Fabric workspace name or GUID",
            ),
            PlatformOptionSpec(
                name="warehouse",
                help="Fabric warehouse name",
            ),
            PlatformOptionSpec(
                name="database",
                help="Database/warehouse name (alias for --warehouse)",
            ),
            PlatformOptionSpec(
                name="auth_method",
                help="Authentication method: service_principal, default_credential, or interactive",
                default="default_credential",
            ),
            PlatformOptionSpec(
                name="tenant_id",
                help="Azure tenant ID for service principal auth",
            ),
            PlatformOptionSpec(
                name="client_id",
                help="Service principal client ID",
            ),
            PlatformOptionSpec(
                name="client_secret",
                help="Service principal client secret",
            ),
            PlatformOptionSpec(
                name="staging_path",
                help="OneLake staging path for data loading",
                default="benchbox-staging",
            ),
        )

    # ========================================================================
    # DataFrame Platform Hooks
    # ========================================================================

    # Register Polars DataFrame platform options
    if PolarsDataFrameAdapter is not None:
        PlatformHookRegistry.register_option_specs(
            "polars-df",
            PlatformOptionSpec(
                name="streaming",
                help="Enable streaming mode for large datasets",
                parser=parse_bool,
                default="false",
            ),
            PlatformOptionSpec(
                name="rechunk",
                help="Rechunk data for better memory layout",
                parser=parse_bool,
                default="true",
            ),
            PlatformOptionSpec(
                name="n_rows",
                help="Limit number of rows to read (for testing)",
                parser=int,
            ),
        )

    # Register Pandas DataFrame platform options
    if PandasDataFrameAdapter is not None:
        PlatformHookRegistry.register_option_specs(
            "pandas-df",
            PlatformOptionSpec(
                name="dtype_backend",
                help="Backend for nullable dtypes",
                choices=("numpy", "numpy_nullable", "pyarrow"),
                default="numpy_nullable",
            ),
        )

    # Register Modin DataFrame platform options
    if ModinDataFrameAdapter is not None:
        PlatformHookRegistry.register_option_specs(
            "modin-df",
            PlatformOptionSpec(
                name="engine",
                help="Modin execution engine",
                choices=("ray", "dask"),
                default="ray",
            ),
        )

    # Register cuDF DataFrame platform options
    if CuDFDataFrameAdapter is not None:
        PlatformHookRegistry.register_option_specs(
            "cudf-df",
            PlatformOptionSpec(
                name="device_id",
                help="CUDA device ID to use",
                parser=int,
                default="0",
            ),
            PlatformOptionSpec(
                name="spill_to_host",
                help="Enable GPU memory spilling to host RAM",
                parser=parse_bool,
                default="true",
            ),
        )

    # Register Dask DataFrame platform options
    if DaskDataFrameAdapter is not None:
        PlatformHookRegistry.register_option_specs(
            "dask-df",
            PlatformOptionSpec(
                name="n_workers",
                help="Number of worker processes",
                parser=int,
            ),
            PlatformOptionSpec(
                name="threads_per_worker",
                help="Threads per worker process",
                parser=int,
                default="1",
            ),
            PlatformOptionSpec(
                name="use_distributed",
                help="Use distributed scheduler (enables dashboard)",
                parser=parse_bool,
                default="false",
            ),
            PlatformOptionSpec(
                name="scheduler_address",
                help="Connect to existing scheduler (e.g., 'tcp://...')",
            ),
        )

    # Register DataFusion DataFrame platform options
    if DataFusionDataFrameAdapter is not None:
        PlatformHookRegistry.register_option_specs(
            "datafusion-df",
            PlatformOptionSpec(
                name="target_partitions",
                help="Number of target partitions for parallelism (default: CPU count)",
                parser=int,
            ),
            PlatformOptionSpec(
                name="repartition_joins",
                help="Enable automatic repartitioning for joins",
                parser=parse_bool,
                default="true",
            ),
            PlatformOptionSpec(
                name="parquet_pushdown",
                help="Enable predicate/projection pushdown for Parquet files",
                parser=parse_bool,
                default="true",
            ),
            PlatformOptionSpec(
                name="batch_size",
                help="Batch size for query execution",
                parser=int,
                default="8192",
            ),
            PlatformOptionSpec(
                name="memory_limit",
                help="Memory limit for fair spill pool (e.g., '8G', '16GB')",
            ),
            PlatformOptionSpec(
                name="temp_dir",
                help="Temporary directory for disk spilling (default: system temp)",
            ),
        )
except ImportError:
    # Platform hooks may not be available in all contexts
    pass
