"""BenchBox visualization toolkit — ASCII-only charting."""

from __future__ import annotations

from benchbox.core.visualization.exceptions import VisualizationDependencyError, VisualizationError
from benchbox.core.visualization.exporters import export_ascii, render_ascii_chart
from benchbox.core.visualization.post_run_summary import PostRunSummary, generate_post_run_summary
from benchbox.core.visualization.result_plotter import NormalizedQuery, NormalizedResult, ResultPlotter
from benchbox.core.visualization.templates import ChartTemplate, get_template, list_templates
from benchbox.core.visualization.utils import slugify

__all__ = [
    "ChartTemplate",
    "NormalizedQuery",
    "NormalizedResult",
    "PostRunSummary",
    "ResultPlotter",
    "VisualizationDependencyError",
    "VisualizationError",
    "export_ascii",
    "generate_post_run_summary",
    "get_template",
    "list_templates",
    "render_ascii_chart",
    "slugify",
]
