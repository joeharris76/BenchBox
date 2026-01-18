"""Generate charts from BenchBox benchmark results."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import click

from benchbox.cli.shared import console
from benchbox.core.visualization import (
    ResultPlotter,
    VisualizationDependencyError,
    VisualizationError,
    list_templates,
)
from benchbox.core.visualization.utils import slugify

SUPPORTED_CHART_TYPES = ("performance_bar", "distribution_box", "query_heatmap", "cost_scatter", "time_series")


@click.command("visualize")
@click.argument("sources", nargs=-1, type=click.Path(), required=False)
@click.option(
    "--output",
    "output_dir",
    type=click.Path(),
    default="charts",
    show_default=True,
    help="Output directory for generated charts.",
)
@click.option(
    "--format",
    "formats",
    multiple=True,
    default=("png", "html"),
    show_default=True,
    help="Export formats (png, svg, pdf, html).",
)
@click.option(
    "--chart-type",
    "chart_types",
    multiple=True,
    default=("auto",),
    show_default=True,
    help="Chart types to generate (auto|all|performance_bar|distribution_box|query_heatmap|cost_scatter|time_series).",
)
@click.option(
    "--template",
    "template_name",
    type=click.Choice([t.name for t in list_templates()], case_sensitive=False),
    help="Named template to use (overrides chart-type selection).",
)
@click.option(
    "--group-by",
    type=click.Choice(["platform", "benchmark"], case_sensitive=False),
    help="Group results and render a chart set per group.",
)
@click.option(
    "--dpi", type=click.IntRange(72, 600), default=300, show_default=True, help="DPI for raster exports (72-600)."
)
@click.option(
    "--theme",
    type=click.Choice(["light", "dark"], case_sensitive=False),
    default="light",
    show_default=True,
)
@click.option(
    "--smart/--no-smart",
    default=True,
    show_default=True,
    help="Enable/disable smart chart selection based on data characteristics.",
)
@click.pass_context
def visualize(
    ctx: click.Context,
    sources: Sequence[str],
    output_dir: str,
    formats: Sequence[str],
    chart_types: Sequence[str],
    template_name: str | None,
    group_by: str | None,
    dpi: int,
    theme: str,
    smart: bool,
):
    """Generate publication-ready charts from BenchBox results."""

    try:
        plotter = ResultPlotter.from_sources(sources or None, theme=theme)
    except (VisualizationError, VisualizationDependencyError) as exc:
        console.print(f"[red]Error:[/red] {exc}")
        ctx.exit(1)

    formats = tuple(formats) if formats else ("png", "html")
    chosen_chart_types: list[str] | None = None

    if template_name:
        chosen_chart_types = None
    elif chart_types:
        lowered = [c.lower() for c in chart_types]
        if "auto" in lowered:
            chosen_chart_types = None
        elif "all" in lowered:
            chosen_chart_types = list(SUPPORTED_CHART_TYPES)
        else:
            chosen_chart_types = [c for c in lowered if c in SUPPORTED_CHART_TYPES]
            unsupported = set(lowered) - set(SUPPORTED_CHART_TYPES)
            if unsupported:
                console.print(f"[yellow]Warning:[/yellow] Ignoring unsupported chart types: {', '.join(unsupported)}")

    try:
        if group_by:
            grouped = plotter.group_by(group_by.lower())
            for key, group_plotter in grouped.items():
                sub_output = Path(output_dir) / f"{group_by.lower()}-{slugify(key)}"
                group_plotter.generate_all_charts(
                    output_dir=sub_output,
                    formats=formats,
                    template_name=template_name,
                    chart_types=chosen_chart_types,
                    smart=smart,
                    dpi=dpi,
                )
                console.print(f"[green]✓[/green] Charts written to {sub_output}")
        else:
            exports = plotter.generate_all_charts(
                output_dir=output_dir,
                formats=formats,
                template_name=template_name,
                chart_types=chosen_chart_types,
                smart=smart,
                dpi=dpi,
            )
            if exports:
                console.print(f"[green]✓[/green] Generated {len(exports)} chart types in {output_dir}")
    except VisualizationDependencyError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        ctx.exit(1)
    except VisualizationError as exc:
        console.print(f"[red]Visualization failed:[/red] {exc}")
        ctx.exit(1)


__all__ = ["visualize"]
