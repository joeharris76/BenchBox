"""Unified platform comparison module.

Provides a unified interface for comparing benchmark performance across
both SQL and DataFrame platforms.

Key Features:
- Single CLI command (benchbox compare --run) for all platform types
- Automatic platform type detection (SQL vs DataFrame)
- Unified result format and visualization
- Support for embedded, cloud, and distributed platforms

Usage:
    from benchbox.core.comparison import (
        UnifiedBenchmarkSuite,
        UnifiedBenchmarkConfig,
        PlatformType,
        run_unified_comparison,
    )

    # Run comparison
    results = run_unified_comparison(
        platforms=["duckdb", "sqlite"],
        scale_factor=0.01,
    )

    # Or with explicit configuration
    suite = UnifiedBenchmarkSuite(
        config=UnifiedBenchmarkConfig(
            platform_type=PlatformType.SQL,
            scale_factor=0.1,
            benchmark="tpch",
        )
    )
    results = suite.run_comparison(platforms=["duckdb", "clickhouse"])

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.core.comparison.plotter import (
    UnifiedComparisonPlotter,
)
from benchbox.core.comparison.suite import (
    UnifiedBenchmarkSuite,
    run_unified_comparison,
)
from benchbox.core.comparison.types import (
    DATAFRAME_PLATFORM_SUFFIX,
    SQL_PLATFORMS,
    ComparisonMode,
    PlatformType,
    UnifiedBenchmarkConfig,
    UnifiedComparisonSummary,
    UnifiedPlatformResult,
    UnifiedQueryResult,
    detect_platform_type,
    detect_platform_types,
)

__all__ = [
    # Types
    "PlatformType",
    "ComparisonMode",
    "SQL_PLATFORMS",
    "DATAFRAME_PLATFORM_SUFFIX",
    "detect_platform_type",
    "detect_platform_types",
    "UnifiedBenchmarkConfig",
    "UnifiedQueryResult",
    "UnifiedPlatformResult",
    "UnifiedComparisonSummary",
    # Suite
    "UnifiedBenchmarkSuite",
    "run_unified_comparison",
    # Plotter
    "UnifiedComparisonPlotter",
]
