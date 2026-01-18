"""Styling utilities and templates for BenchBox visualizations."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from itertools import cycle

from benchbox.core.visualization.dependencies import require_plotly
from benchbox.core.visualization.exceptions import VisualizationError

# Colorblind-friendly categorical palette (Okabe-Ito inspired)
DEFAULT_PALETTE: Sequence[str] = (
    "#1b9e77",
    "#d95f02",
    "#7570b3",
    "#e7298a",
    "#66a61e",
    "#e6ab02",
    "#a6761d",
    "#666666",
)


@dataclass
class ThemeSettings:
    """Theme configuration for Plotly charts."""

    mode: str = "light"
    palette: Sequence[str] = DEFAULT_PALETTE
    font_family: str = "Source Sans Pro, Open Sans, Arial, sans-serif"
    background_color: str = "#ffffff"
    paper_color: str = "#ffffff"
    grid_color: str = "rgba(0,0,0,0.08)"
    text_color: str = "#2f3437"
    secondary_text_color: str = "#6b7075"
    title_size: int = 18
    label_size: int = 12
    legend_size: int = 11
    best_color: str = "#1b9e77"  # Green for best/Pareto-optimal
    worst_color: str = "#d95f02"  # Orange for worst performers


def get_theme(mode: str = "light", palette: Sequence[str] | None = None) -> ThemeSettings:
    """Return theme settings for the given mode."""
    normalized = mode.lower()
    base_palette = tuple(palette) if palette else DEFAULT_PALETTE

    if normalized not in {"light", "dark"}:
        raise VisualizationError(f"Unsupported theme mode '{mode}'. Expected 'light' or 'dark'.")

    if normalized == "dark":
        return ThemeSettings(
            mode="dark",
            palette=base_palette,
            background_color="#0f1116",
            paper_color="#0f1116",
            grid_color="rgba(255,255,255,0.08)",
            text_color="#e8e8e8",
            secondary_text_color="#b0b4ba",
        )

    return ThemeSettings(mode="light", palette=base_palette)


def build_template(theme: ThemeSettings) -> dict:
    """Construct a Plotly template dictionary from theme settings."""
    return {
        "layout": {
            "colorway": list(theme.palette),
            "font": {
                "family": theme.font_family,
                "color": theme.text_color,
                "size": theme.label_size,
            },
            "paper_bgcolor": theme.paper_color,
            "plot_bgcolor": theme.background_color,
            "title": {"font": {"size": theme.title_size + 2, "color": theme.text_color}},
            "legend": {
                "title": {"font": {"size": theme.legend_size, "color": theme.text_color}},
                "font": {"size": theme.legend_size, "color": theme.text_color},
                "bgcolor": "rgba(0,0,0,0)",
            },
            "xaxis": {
                "gridcolor": theme.grid_color,
                "zeroline": False,
                "title": {"font": {"size": theme.label_size, "color": theme.text_color}},
            },
            "yaxis": {
                "gridcolor": theme.grid_color,
                "zeroline": False,
                "title": {"font": {"size": theme.label_size, "color": theme.text_color}},
            },
        }
    }


def apply_common_layout(
    fig,
    theme: ThemeSettings,
    title: str | None = None,
    x_title: str | None = None,
    y_title: str | None = None,
    legend_title: str | None = None,
) -> None:
    """Apply standard BenchBox layout settings to a Plotly figure."""
    template = build_template(theme)
    fig.update_layout(template=template)

    fig.update_layout(
        title={"text": title, "x": 0.02, "xanchor": "left"},
        xaxis={
            "title": {"text": x_title},
            "gridcolor": theme.grid_color,
            "zeroline": False,
        },
        yaxis={
            "title": {"text": y_title},
            "gridcolor": theme.grid_color,
            "zeroline": False,
        },
        legend={"title": {"text": legend_title}},
        margin={"l": 70, "r": 40, "t": 60, "b": 60},
    )


def apply_theme(fig, theme: ThemeSettings) -> None:
    """Register and apply the BenchBox template to a figure."""
    _, pio = require_plotly()
    template_name = f"benchbox_{theme.mode}"
    pio.templates[template_name] = build_template(theme)
    fig.update_layout(template=template_name)


def color_cycle(palette: Sequence[str] | None = None) -> Iterable[str]:
    """Return an infinite color cycle from the chosen palette."""
    return cycle(palette or DEFAULT_PALETTE)
