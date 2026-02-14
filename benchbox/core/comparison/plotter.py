"""Unified visualization for platform comparisons.

Provides unified plotting capabilities for both SQL and DataFrame
platform comparison results.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from pathlib import Path

from benchbox.core.comparison.types import (
    PlatformType,
    UnifiedPlatformResult,
)

logger = logging.getLogger(__name__)


class UnifiedComparisonPlotter:
    """Generate ASCII visualizations for unified platform comparisons.

    Works with both SQL and DataFrame comparison results, providing
    consistent visualization across platform types.

    Example:
        plotter = UnifiedComparisonPlotter(results, theme="light")
        charts = plotter.generate_charts(output_dir="charts/")
    """

    def __init__(
        self,
        results: list[UnifiedPlatformResult],
        theme: str = "light",
    ):
        """Initialize the plotter with comparison results.

        Args:
            results: List of platform results from UnifiedBenchmarkSuite
            theme: Chart theme ("light" or "dark")
        """
        if not results:
            raise ValueError("No results provided for visualization")
        self.results = results
        self.theme = theme
        self.platform_type = results[0].platform_type if results else PlatformType.SQL

    def generate_charts(self, output_dir: str | Path, **kwargs: object) -> dict[str, Path]:
        """Generate ASCII charts from comparison results.

        Renders performance_bar, distribution_box, and query_heatmap charts
        as ``.txt`` files in the specified output directory.

        Args:
            output_dir: Directory to write chart files into.

        Returns:
            Mapping of chart type name to exported file path.
        """
        from benchbox.core.visualization.chart_generator import (
            generate_comparison_charts,
            normalized_from_unified,
        )

        normalized = normalized_from_unified(self.results)
        return generate_comparison_charts(normalized, output_dir, theme=self.theme)


__all__ = ["UnifiedComparisonPlotter"]
