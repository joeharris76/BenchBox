"""BenchBox visualization toolkit built on Plotly."""

from __future__ import annotations

from benchbox.core.visualization.charts import (
    BarDatum,
    CostPerformancePoint,
    CostPerformanceScatterPlot,
    DistributionBoxPlot,
    DistributionSeries,
    PerformanceBarChart,
    QueryVarianceHeatmap,
    TimeSeriesLineChart,
    TimeSeriesPoint,
)
from benchbox.core.visualization.exceptions import VisualizationDependencyError, VisualizationError
from benchbox.core.visualization.exporters import SUPPORTED_EXPORT_FORMATS, export_figure
from benchbox.core.visualization.result_plotter import NormalizedQuery, NormalizedResult, ResultPlotter
from benchbox.core.visualization.styles import ThemeSettings, apply_common_layout, apply_theme, get_theme
from benchbox.core.visualization.templates import ChartTemplate, get_template, list_templates
from benchbox.core.visualization.utils import slugify

__all__ = [
    "BarDatum",
    "CostPerformancePoint",
    "CostPerformanceScatterPlot",
    "DistributionBoxPlot",
    "DistributionSeries",
    "PerformanceBarChart",
    "QueryVarianceHeatmap",
    "TimeSeriesLineChart",
    "TimeSeriesPoint",
    "SUPPORTED_EXPORT_FORMATS",
    "export_figure",
    "VisualizationDependencyError",
    "VisualizationError",
    "ThemeSettings",
    "apply_common_layout",
    "apply_theme",
    "get_theme",
    "ChartTemplate",
    "get_template",
    "list_templates",
    "ResultPlotter",
    "NormalizedResult",
    "NormalizedQuery",
    "slugify",
]
