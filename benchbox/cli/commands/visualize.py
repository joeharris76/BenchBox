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

SUPPORTED_CHART_TYPES = (
    "performance_bar",
    "distribution_box",
    "query_heatmap",
    "query_histogram",
    "cost_scatter",
    "time_series",
)


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

    from benchbox.core.visualization.ascii import (
        ASCIIBarChart,
        ASCIIBoxPlot,
        ASCIIHeatmap,
        ASCIILineChart,
        ASCIIQueryHistogram,
        ASCIIScatterPlot,
    )
    from benchbox.core.visualization.ascii.bar_chart import BarData
    from benchbox.core.visualization.ascii.base import ASCIIChartOptions
    from benchbox.core.visualization.ascii.box_plot import BoxPlotSeries
    from benchbox.core.visualization.ascii.histogram import HistogramBar
    from benchbox.core.visualization.ascii.line_chart import LinePoint
    from benchbox.core.visualization.ascii.scatter_plot import ScatterPoint

    def print_ansi(content: str) -> None:
        """Print string with ANSI codes through Rich console."""
        console.print(Text.from_ansi(content))

    # Resolve chart types from template or explicit list
    if template_name:
        from benchbox.core.visualization.templates import get_template

        try:
            tmpl = get_template(template_name)
            types_to_render = list(tmpl.chart_types)
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            ctx.exit(1)
    else:
        lowered = [c.lower() for c in chart_types]
        if "auto" in lowered or "all" in lowered:
            types_to_render = list(SUPPORTED_CHART_TYPES)
        else:
            types_to_render = [c for c in lowered if c in SUPPORTED_CHART_TYPES]

    if not types_to_render:
        types_to_render = ["performance_bar"]  # Default

    # Resolve source paths
    source_paths = list(sources) if sources else []

    if not source_paths:
        # Try to find recent results
        results_dir = Path("benchmark_runs/results")
        if results_dir.exists():
            json_files = sorted(results_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            source_paths = [str(f) for f in json_files[:5]]  # Latest 5

    if not source_paths:
        console.print("[yellow]No result files found. Specify sources or run a benchmark first.[/yellow]")
        ctx.exit(1)

    # Use ResultPlotter for consistent normalization
    try:
        plotter = ResultPlotter.from_sources(source_paths, theme=theme)
        results = plotter.results
    except Exception as e:
        console.print(f"[red]Error loading results: {e}[/red]")
        ctx.exit(1)

    # Create chart options
    opts = ASCIIChartOptions(
        use_color=use_color,
        use_unicode=use_unicode,
        theme=theme,
    )

    # Extract metadata for subtitle display (scale factor, platform version, tuning)
    from benchbox.mcp.tools.visualization import _extract_chart_metadata

    metadata = _extract_chart_metadata(results)

    # Render each requested chart type
    for chart_type in types_to_render:
        try:
            if chart_type == "performance_bar":
                bar_data: list[BarData] = []
                for r in results:
                    bar_data.append(BarData(label=r.platform, value=r.total_time_ms or 0))

                if bar_data:
                    sorted_data = sorted(bar_data, key=lambda x: x.value)
                    sorted_data[0].is_best = True
                    if len(sorted_data) > 1:
                        sorted_data[-1].is_worst = True

                chart = ASCIIBarChart(
                    data=bar_data,
                    title="Performance Comparison",
                    metric_label="Execution Time (ms)",
                    options=opts,
                    metadata=metadata,
                )
                print_ansi(chart.render())
                console.print()

            elif chart_type == "distribution_box":
                series_data: list[BoxPlotSeries] = []
                for r in results:
                    timings = [q.execution_time_ms for q in r.queries if q.execution_time_ms is not None]
                    if timings:
                        series_data.append(BoxPlotSeries(name=r.platform, values=timings))

                if series_data:
                    chart = ASCIIBoxPlot(
                        series=series_data,
                        title="Query Time Distribution",
                        y_label="Execution Time (ms)",
                        options=opts,
                        metadata=metadata,
                    )
                    print_ansi(chart.render())
                    console.print()

            elif chart_type == "query_heatmap":
                platforms = [r.platform for r in results]
                query_ids: list[str] = []
                platform_timings: dict[str, dict[str, float]] = {}

                for r in results:
                    platform_timings[r.platform] = {}
                    for q in r.queries:
                        if q.execution_time_ms is not None:
                            platform_timings[r.platform][q.query_id] = q.execution_time_ms
                            if q.query_id not in query_ids:
                                query_ids.append(q.query_id)

                if query_ids and platforms:
                    matrix: list[list[float]] = []
                    for qid in query_ids:
                        row = [platform_timings.get(p, {}).get(qid, 0) for p in platforms]
                        matrix.append(row)

                    chart = ASCIIHeatmap(
                        matrix=matrix,
                        row_labels=query_ids,
                        col_labels=platforms,
                        title="Query Execution Heatmap",
                        value_label="ms",
                        options=opts,
                        metadata=metadata,
                    )
                    print_ansi(chart.render())
                    console.print()

            elif chart_type == "query_histogram":
                query_timings: dict[tuple[str, str], list[float]] = {}
                platforms_in_results = [r.platform for r in results]
                use_platform = len(set(platforms_in_results)) > 1

                for r in results:
                    for q in r.queries:
                        if q.execution_time_ms is not None:
                            key = (r.platform, q.query_id)
                            query_timings.setdefault(key, []).append(q.execution_time_ms)

                histogram_data: list[HistogramBar] = []
                for (platform, query_id), timings in query_timings.items():
                    mean_latency = sum(timings) / len(timings)
                    histogram_data.append(
                        HistogramBar(
                            query_id=query_id,
                            latency_ms=mean_latency,
                            platform=platform if use_platform else None,
                        )
                    )

                if histogram_data:
                    query_means: dict[str, list[float]] = {}
                    for bar in histogram_data:
                        query_means.setdefault(bar.query_id, []).append(bar.latency_ms)
                    avg_by_query = {qid: sum(v) / len(v) for qid, v in query_means.items()}
                    if avg_by_query:
                        best_qid = min(avg_by_query, key=avg_by_query.get)  # type: ignore[arg-type]
                        worst_qid = max(avg_by_query, key=avg_by_query.get)  # type: ignore[arg-type]
                        for bar in histogram_data:
                            bar.is_best = bar.query_id == best_qid
                            bar.is_worst = bar.query_id == worst_qid

                    chart = ASCIIQueryHistogram(
                        data=histogram_data,
                        title="Query Latency Histogram",
                        y_label="Execution Time (ms)",
                        options=opts,
                        metadata=metadata,
                    )
                    print_ansi(chart.render())
                    console.print()

            elif chart_type == "cost_scatter":
                scatter_data: list[ScatterPoint] = []
                for r in results:
                    total_time = r.total_time_ms or 0
                    cost = r.cost_total or 0
                    performance = 3600000 / total_time if total_time > 0 else 0
                    scatter_data.append(ScatterPoint(name=r.platform, x=cost, y=performance))

                if scatter_data:
                    chart = ASCIIScatterPlot(
                        points=scatter_data,
                        title="Cost vs Performance",
                        x_label="Cost (USD)",
                        y_label="Queries per Hour",
                        options=opts,
                        metadata=metadata,
                    )
                    print_ansi(chart.render())
                    console.print()

            elif chart_type == "time_series":
                line_data: list[LinePoint] = []
                for i, r in enumerate(results):
                    total_time = r.total_time_ms or 0
                    x_label = r.timestamp.strftime("%Y-%m-%d") if r.timestamp else f"Run {i + 1}"
                    line_data.append(LinePoint(series=r.platform, x=x_label, y=total_time))

                if line_data:
                    chart = ASCIILineChart(
                        points=line_data,
                        title="Performance Trend",
                        x_label="Run",
                        y_label="Execution Time (ms)",
                        options=opts,
                        metadata=metadata,
                    )
                    print_ansi(chart.render())
                    console.print()

            else:
                # Delegate template-only chart types (comparison_bar, diverging_bar, summary_box)
                # to the MCP rendering helper which handles all chart types.
                from benchbox.mcp.tools.visualization import _render_single_ascii_chart

                content = _render_single_ascii_chart(results, chart_type, opts)
                if content:
                    print_ansi(content)
                    console.print()

        except Exception as e:
            console.print(f"[yellow]Warning: Could not render {chart_type}: {e}[/yellow]")


@click.command("visualize")
@click.argument("sources", nargs=-1, type=click.Path(), required=False)
@click.option(
    "--chart-type",
    "chart_types",
    multiple=True,
    default=("auto",),
    show_default=True,
    help="Chart types: auto|all|performance_bar|distribution_box|query_heatmap|query_histogram|cost_scatter|time_series.",
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
