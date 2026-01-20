"""DataFrame platform adapters for BenchBox.

This package provides DataFrame-specific platform adapters that enable
benchmarking of DataFrame libraries using native DataFrame APIs instead of SQL.

Architecture:
- ExpressionFamilyAdapter: Base class for expression-based libraries
  (Polars, PySpark, DataFusion)
- PandasFamilyAdapter: Base class for Pandas-like libraries
  (Pandas, Modin, cuDF, Vaex, Dask)

Each adapter type provides:
- Data loading (CSV, Parquet)
- Table registration
- Query execution using native DataFrame operations
- Result collection and validation

Usage:
    # Expression family (Polars example)
    from benchbox.platforms.dataframe import PolarsDataFrameAdapter

    adapter = PolarsDataFrameAdapter(working_dir="./benchmark_data")
    ctx = adapter.create_context()
    adapter.load_tables(ctx, data_dir)
    result = adapter.execute_query(ctx, query)

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.platforms.dataframe.expression_family import (
    ExpressionFamilyAdapter,
    ExpressionFamilyContext,
)
from benchbox.platforms.dataframe.pandas_df import (
    PANDAS_AVAILABLE,
    PandasDataFrameAdapter,
)
from benchbox.platforms.dataframe.pandas_family import (
    PandasFamilyAdapter,
    PandasFamilyContext,
)
from benchbox.platforms.dataframe.platform_checker import (
    DATAFRAME_PLATFORMS,
    DataFrameFamily,
    DataFramePlatformChecker,
    PlatformInfo,
    PlatformStatus,
    format_platform_status_table,
    get_installation_suggestion,
    get_platform_error_message,
    require_platform,
)
from benchbox.platforms.dataframe.polars_df import (
    POLARS_AVAILABLE,
    PolarsDataFrameAdapter,
)

# Modin adapter (optional - requires modin[ray] or modin[dask])
try:
    from benchbox.platforms.dataframe.modin_df import (
        MODIN_AVAILABLE,
        ModinDataFrameAdapter,
    )
except ImportError:
    MODIN_AVAILABLE = False
    ModinDataFrameAdapter = None  # type: ignore[assignment,misc]

# cuDF adapter (optional - requires NVIDIA GPU and cudf)
try:
    from benchbox.platforms.dataframe.cudf_df import (
        CUDF_AVAILABLE,
        CuDFDataFrameAdapter,
    )
except ImportError:
    CUDF_AVAILABLE = False
    CuDFDataFrameAdapter = None  # type: ignore[assignment,misc]

# Dask adapter (optional - requires dask[distributed])
try:
    from benchbox.platforms.dataframe.dask_df import (
        DASK_AVAILABLE,
        DaskDataFrameAdapter,
    )
except ImportError:
    DASK_AVAILABLE = False
    DaskDataFrameAdapter = None  # type: ignore[assignment,misc]

# DataFusion adapter (optional - requires datafusion)
try:
    from benchbox.platforms.dataframe.datafusion_df import (
        DATAFUSION_DF_AVAILABLE,
        DataFusionDataFrameAdapter,
    )
except ImportError:
    DATAFUSION_DF_AVAILABLE = False
    DataFusionDataFrameAdapter = None  # type: ignore[assignment,misc]

# PySpark adapter (optional - requires pyspark)
try:
    from benchbox.platforms.dataframe.pyspark_df import (
        PYSPARK_AVAILABLE,
        PYSPARK_VERSION,
        PySparkDataFrameAdapter,
    )
except ImportError:
    PYSPARK_AVAILABLE = False
    PYSPARK_VERSION = None  # type: ignore[assignment]
    PySparkDataFrameAdapter = None  # type: ignore[assignment,misc]

__all__ = [
    # Expression Family
    "ExpressionFamilyAdapter",
    "ExpressionFamilyContext",
    # Pandas Family
    "PandasFamilyAdapter",
    "PandasFamilyContext",
    # Polars
    "PolarsDataFrameAdapter",
    "POLARS_AVAILABLE",
    # Pandas
    "PandasDataFrameAdapter",
    "PANDAS_AVAILABLE",
    # Modin
    "ModinDataFrameAdapter",
    "MODIN_AVAILABLE",
    # cuDF
    "CuDFDataFrameAdapter",
    "CUDF_AVAILABLE",
    # Dask
    "DaskDataFrameAdapter",
    "DASK_AVAILABLE",
    # DataFusion
    "DataFusionDataFrameAdapter",
    "DATAFUSION_DF_AVAILABLE",
    # PySpark
    "PySparkDataFrameAdapter",
    "PYSPARK_AVAILABLE",
    "PYSPARK_VERSION",
    # Platform Checker
    "DataFramePlatformChecker",
    "DataFrameFamily",
    "PlatformInfo",
    "PlatformStatus",
    "DATAFRAME_PLATFORMS",
    "format_platform_status_table",
    "get_installation_suggestion",
    "get_platform_error_message",
    "require_platform",
]
