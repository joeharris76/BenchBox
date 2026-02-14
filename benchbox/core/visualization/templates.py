"""Template definitions for common BenchBox chart sets."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from benchbox.core.visualization.exceptions import VisualizationError


@dataclass(frozen=True)
class ChartTemplate:
    """Named chart template describing chart types and export formats."""

    name: str
    description: str
    chart_types: Sequence[str]
    formats: Sequence[str] = field(default_factory=lambda: ("ascii",))
    options: dict[str, Any] = field(default_factory=dict)


_TEMPLATES: dict[str, ChartTemplate] = {
    "default": ChartTemplate(
        name="default",
        description="Baseline set for a single benchmark run (bar + box + histogram + heatmap when available).",
        chart_types=("performance_bar", "distribution_box", "query_histogram", "query_heatmap"),
        formats=("ascii",),
    ),
    "flagship": ChartTemplate(
        name="flagship",
        description="Eight-platform flagship comparison with heatmap, histogram, and cost frontier.",
        chart_types=("performance_bar", "query_heatmap", "query_histogram", "cost_scatter", "distribution_box"),
        formats=("ascii",),
    ),
    "head_to_head": ChartTemplate(
        name="head_to_head",
        description="Two-platform comparison with win/loss emphasis.",
        chart_types=("performance_bar", "distribution_box", "query_histogram", "query_heatmap"),
        formats=("ascii",),
    ),
    "trends": ChartTemplate(
        name="trends",
        description="Multi-period performance trend lines with regression overlay.",
        chart_types=("time_series", "performance_bar"),
        formats=("ascii",),
    ),
    "cost_optimization": ChartTemplate(
        name="cost_optimization",
        description="Cost breakdown and price/performance frontier.",
        chart_types=("cost_scatter", "performance_bar"),
        formats=("ascii",),
    ),
    "comparison": ChartTemplate(
        name="comparison",
        description="Two-run comparison report with paired bars, diverging distribution, and summary box.",
        chart_types=("comparison_bar", "diverging_bar", "summary_box"),
        formats=("ascii",),
    ),
}


def get_template(name: str) -> ChartTemplate:
    """Lookup a chart template by name."""
    normalized = name.lower().replace("-", "_")
    try:
        return _TEMPLATES[normalized]
    except KeyError as exc:
        raise VisualizationError(
            f"Unknown chart template '{name}'. Available: {', '.join(sorted(_TEMPLATES))}"
        ) from exc


def list_templates() -> list[ChartTemplate]:
    """Return all available templates."""
    return list(_TEMPLATES.values())
