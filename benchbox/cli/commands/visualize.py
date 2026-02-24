"""Generate charts from BenchBox benchmark results."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import click

from benchbox.cli.shared import console
from benchbox.core.visualization import (
    ResultPlotter,
    list_templates,
)
from benchbox.core.visualization.chart_types import ALL_CHART_TYPES, CHART_TYPE_SPECS

SUPPORTED_CHART_TYPES = ALL_CHART_TYPES


def _render_ascii_charts(
    ctx: click.Context,
    sources: Sequence[str],
    chart_types: Sequence[str],
    theme: str,
    use_color: bool = True,
    use_unicode: bool = True,
    template_name: str | None = None,
) -> None:
    """Render ASCII charts to console using ResultPlotter for normalization."""
    from rich.text import Text

    from benchbox.core.visualization.ascii.base import ASCIIChartOptions
    from benchbox.core.visualization.ascii_runtime import render_ascii_chart_from_results

    def print_ansi(content: str) -> None:
        """Print string with ANSI codes through Rich console."""
        console.print(Text.from_ansi(content))

    # Resolve chart types from template or explicit list
    types_to_render, strict_pairwise_validation = _resolve_chart_types(ctx, chart_types, template_name)

    if not types_to_render:
        types_to_render = ["performance_bar"]

    # Resolve source paths
    source_paths = _resolve_source_paths(ctx, sources)

    # Use ResultPlotter for consistent normalization
    try:
        plotter = ResultPlotter.from_sources(source_paths, theme=theme)
        results = plotter.results
    except Exception as e:
        console.print(f"[red]Error loading results: {e}[/red]")
        ctx.exit(1)

    # Pairwise comparison charts require exactly two results.
    pairwise_only = [
        ct for ct in types_to_render if CHART_TYPE_SPECS.get(ct, None) and CHART_TYPE_SPECS[ct].requires_two_results
    ]
    if pairwise_only and len(results) != 2 and strict_pairwise_validation:
        charts = ", ".join(pairwise_only)
        console.print(f"[red]Error: chart type(s) require exactly 2 results: {charts}[/red]")
        console.print(f"[yellow]Provided result count: {len(results)}[/yellow]")
        ctx.exit(1)

    opts = ASCIIChartOptions(use_color=use_color, use_unicode=use_unicode, theme=theme)

    from benchbox.core.visualization.utils import extract_chart_metadata

    metadata = extract_chart_metadata(results)

    for chart_type in types_to_render:
        try:
            content = render_ascii_chart_from_results(results, chart_type, options=opts, metadata=metadata)
            if content:
                print_ansi(content)
                console.print()
            elif template_name:
                console.print(f"[yellow]Skipped {chart_type}: not applicable for current inputs.[/yellow]")
        except Exception as e:
            console.print(f"[yellow]Warning: Could not render {chart_type}: {e}[/yellow]")


def _resolve_chart_types(
    ctx: click.Context,
    chart_types: Sequence[str],
    template_name: str | None,
) -> tuple[list[str], bool]:
    """Resolve chart types from template or explicit list. Returns (types, strict_pairwise)."""
    strict_pairwise_validation = True
    if template_name:
        from benchbox.core.visualization.templates import get_template

        try:
            tmpl = get_template(template_name)
            return list(tmpl.chart_types), strict_pairwise_validation
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            ctx.exit(1)

    lowered = [c.lower() for c in chart_types]
    if "auto" in lowered or "all" in lowered:
        return list(SUPPORTED_CHART_TYPES), False

    invalid = [c for c in lowered if c not in SUPPORTED_CHART_TYPES]
    if invalid:
        valid = ", ".join(SUPPORTED_CHART_TYPES)
        console.print(f"[red]Error: Unknown chart type(s): {', '.join(invalid)}[/red]")
        console.print(f"[yellow]Valid chart types: {valid}[/yellow]")
        ctx.exit(1)
    return list(lowered), strict_pairwise_validation


def _resolve_source_paths(ctx: click.Context, sources: Sequence[str]) -> list[str]:
    """Resolve source paths from arguments or find recent results."""
    source_paths = list(sources) if sources else []

    if not source_paths:
        results_dir = Path("benchmark_runs/results")
        if results_dir.exists():
            json_files = sorted(results_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            source_paths = [str(f) for f in json_files[:5]]

    if not source_paths:
        console.print("[yellow]No result files found. Specify sources or run a benchmark first.[/yellow]")
        ctx.exit(1)

    return source_paths


@click.command("visualize")
@click.argument("sources", nargs=-1, type=click.Path(), required=False)
@click.option(
    "--chart-type",
    "chart_types",
    multiple=True,
    default=("auto",),
    show_default=True,
    help=f"Chart types: auto|all|{'|'.join(SUPPORTED_CHART_TYPES)}.",
)
@click.option(
    "--template",
    "template_name",
    type=click.Choice([t.name for t in list_templates()], case_sensitive=False),
    help="Named template to use (overrides chart-type selection).",
)
@click.option(
    "--theme",
    type=click.Choice(["light", "dark"], case_sensitive=False),
    default="light",
    show_default=True,
)
@click.option(
    "--no-color",
    is_flag=True,
    default=False,
    help="Disable ANSI colors in ASCII output (for piping to files or plain terminals).",
)
@click.option(
    "--no-unicode",
    is_flag=True,
    default=False,
    help="Use ASCII-only characters (for terminals without Unicode support).",
)
@click.pass_context
def visualize(
    ctx: click.Context,
    sources: Sequence[str],
    chart_types: Sequence[str],
    template_name: str | None,
    theme: str,
    no_color: bool,
    no_unicode: bool,
):
    """Generate ASCII charts from BenchBox results."""
    _render_ascii_charts(
        ctx,
        sources,
        chart_types,
        theme,
        use_color=not no_color,
        use_unicode=not no_unicode,
        template_name=template_name,
    )


__all__ = ["visualize"]
