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
    formats: Sequence[str] = field(default_factory=lambda: ("png", "html"))
    options: dict[str, Any] = field(default_factory=dict)


_TEMPLATES: dict[str, ChartTemplate] = {
    "default": ChartTemplate(
        name="default",
        description="Baseline set for a single benchmark run (bar + box + heatmap when available).",
        chart_types=("performance_bar", "distribution_box", "query_heatmap"),
        formats=("png", "html"),
    ),
    "flagship": ChartTemplate(
        name="flagship",
        description="Eight-platform flagship comparison with heatmap and cost frontier.",
        chart_types=("performance_bar", "query_heatmap", "cost_scatter", "distribution_box"),
        formats=("png", "svg", "html"),
    ),
    "head_to_head": ChartTemplate(
        name="head_to_head",
        description="Two-platform comparison with win/loss emphasis.",
        chart_types=("performance_bar", "distribution_box", "query_heatmap"),
        formats=("png", "html"),
    ),
    "trends": ChartTemplate(
        name="trends",
        description="Multi-period performance trend lines with regression overlay.",
        chart_types=("time_series", "performance_bar"),
        formats=("png", "svg", "html"),
    ),
    "cost_optimization": ChartTemplate(
        name="cost_optimization",
        description="Cost breakdown and price/performance frontier.",
        chart_types=("cost_scatter", "performance_bar"),
        formats=("png", "svg", "html"),
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
