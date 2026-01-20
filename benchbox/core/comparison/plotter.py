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
    """Generate visualizations for unified platform comparisons.

    Works with both SQL and DataFrame comparison results, providing
    consistent visualization across platform types.

    Example:
        plotter = UnifiedComparisonPlotter(results, theme="light")
        plotter.generate_charts(output_dir="charts/", formats=["png", "html"])
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

    def generate_charts(
        self,
        output_dir: str | Path,
        formats: list[str] | None = None,
        chart_types: list[str] | None = None,
        dpi: int = 300,
    ) -> dict[str, dict[str, Path]]:
        """Generate all applicable charts from results.

        Args:
            output_dir: Directory to save charts
            formats: Export formats (default: ["png", "html"])
            chart_types: Chart types to generate (default: auto-select)
            dpi: Resolution for raster formats

        Returns:
            Dict mapping chart_type -> format -> file_path
        """
        try:
            from benchbox.core.visualization import (
                BarDatum,
                DistributionBoxPlot,
                DistributionSeries,
                PerformanceBarChart,
                QueryVarianceHeatmap,
                export_figure,
                get_theme,
            )
        except ImportError as e:
            logger.warning(f"Visualization dependencies not available: {e}")
            return {}

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        formats = formats or ["png", "html"]
        chart_types = chart_types or self._suggest_chart_types()
        theme_settings = get_theme(self.theme)

        exports: dict[str, dict[str, Path]] = {}

        # Store imported classes as instance attributes
        self._bar_datum_cls = BarDatum
        self._performance_bar_cls = PerformanceBarChart
        self._distribution_series_cls = DistributionSeries
        self._distribution_box_cls = DistributionBoxPlot
        self._query_heatmap_cls = QueryVarianceHeatmap

        for chart_type in chart_types:
            try:
                if chart_type == "performance_bar":
                    fig, name = self._render_performance_bar()
                elif chart_type == "distribution_box":
                    fig, name = self._render_distribution_box()
                elif chart_type == "query_heatmap":
                    fig, name = self._render_query_heatmap()
                else:
                    continue

                if fig is None:
                    continue

                export_paths = export_figure(
                    fig,
                    output_dir=output_path,
                    base_name=name,
                    formats=formats,
                    dpi=dpi,
                    theme=theme_settings,
                )
                exports[chart_type] = export_paths

            except Exception as e:
                logger.warning(f"Failed to generate {chart_type} chart: {e}")

        return exports

    def _suggest_chart_types(self) -> list[str]:
        """Suggest appropriate chart types based on data."""
        types = ["performance_bar"]

        # Add distribution if we have per-query data
        if any(r.query_results for r in self.results):
            types.append("distribution_box")

        # Add heatmap if multiple platforms and queries
        if len(self.results) > 1 and any(r.query_results for r in self.results):
            types.append("query_heatmap")

        return types

    def _render_performance_bar(self):
        """Render performance comparison bar chart."""
        bars = []
        for result in self.results:
            value = result.geometric_mean_ms or result.total_time_ms or 0
            bars.append(
                self._bar_datum_cls(
                    label=result.platform,
                    value=value,
                    platform=result.platform,
                )
            )

        if not bars:
            return None, None

        # Mark best/worst
        values = [b.value for b in bars if b.value > 0]
        if values:
            best = min(values)
            worst = max(values)
            for bar in bars:
                if bar.value > 0:
                    bar.is_best = bar.value == best
                    bar.is_worst = bar.value == worst

        type_label = self.platform_type.value.title()
        chart = self._performance_bar_cls(
            data=bars,
            title=f"{type_label} Platform Performance Comparison",
            metric_label="Geometric Mean (ms)",
            sort_by="value",
        )
        return chart.figure(), f"{self.platform_type.value}_performance"

    def _render_distribution_box(self):
        """Render query time distribution box plot."""
        series = []
        for result in self.results:
            times = []
            for qr in result.query_results:
                if qr.status == "SUCCESS" and qr.execution_times_ms:
                    times.extend(qr.execution_times_ms)
            if times:
                series.append(self._distribution_series_cls(name=result.platform, values=times))

        if not series:
            return None, None

        type_label = self.platform_type.value.title()
        chart = self._distribution_box_cls(
            series=series,
            title=f"{type_label} Query Time Distribution",
            y_title="Execution Time (ms)",
        )
        return chart.figure(), f"{self.platform_type.value}_distribution"

    def _render_query_heatmap(self):
        """Render query variance heatmap."""
        # Collect all query IDs
        query_ids = sorted({qr.query_id for r in self.results for qr in r.query_results})
        if not query_ids or len(self.results) < 2:
            return None, None

        platform_names = [r.platform for r in self.results]
        matrix: list[list[float | None]] = []

        for query_id in query_ids:
            row = []
            for result in self.results:
                qr = next((q for q in result.query_results if q.query_id == query_id), None)
                row.append(qr.mean_time_ms if qr and qr.status == "SUCCESS" else None)
            matrix.append(row)

        type_label = self.platform_type.value.title()
        chart = self._query_heatmap_cls(
            matrix=matrix,
            queries=query_ids,
            platforms=platform_names,
            title=f"{type_label} Query Performance Heatmap",
        )
        return chart.figure(), f"{self.platform_type.value}_query_heatmap"


__all__ = ["UnifiedComparisonPlotter"]
