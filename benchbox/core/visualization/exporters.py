"""Export helpers for BenchBox visualizations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from benchbox.core.visualization.exceptions import VisualizationError


def export_ascii(
    ascii_content: str,
    output_dir: str | Path,
    base_name: str,
    format: str = "txt",
) -> Path:
    """Export ASCII chart content to a text file.

    Args:
        ascii_content: The rendered ASCII chart string.
        output_dir: Directory to write files into.
        base_name: Base filename without extension.
        format: Output format (txt or ascii).

    Returns:
        Path to the exported file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    extension = "txt" if format in ("ascii", "txt") else format
    target = output_path / f"{base_name}.{extension}"

    try:
        target.write_text(ascii_content, encoding="utf-8")
    except Exception as exc:
        raise VisualizationError(f"Failed to write ASCII chart: {exc}") from exc

    return target


def render_ascii_chart(
    chart_type: str,
    data: Any,
    title: str | None = None,
    options: Any | None = None,
    **kwargs: Any,
) -> str:
    """Render data as an ASCII chart.

    Args:
        chart_type: Type of chart (performance_bar, distribution_box, query_heatmap,
                    query_histogram, cost_scatter, time_series, comparison_bar,
                    diverging_bar, summary_box).
        data: Chart data in the appropriate format for the chart type.
        title: Optional chart title.
        options: ChartOptions instance.
        **kwargs: Additional arguments passed to the chart constructor.

    Returns:
        Rendered ASCII chart as a string.

    Raises:
        VisualizationError: If chart type is not supported.
    """
    from benchbox.core.visualization.ascii_api import (
        BarChart,
        BarData,
        BoxPlot,
        BoxPlotSeries,
        ChartOptions,
        Heatmap,
        Histogram,
        HistogramBar,
        LineChart,
        LinePoint,
        ScatterPlot,
        ScatterPoint,
    )

    opts = options or ChartOptions()

    if chart_type == "performance_bar":
        converted = []
        for item in data:
            if isinstance(item, BarData):
                converted.append(item)
            else:
                converted.append(
                    BarData(
                        label=getattr(item, "label", str(item)),
                        value=getattr(item, "value", 0),
                        group=getattr(item, "group", None),
                        error=getattr(item, "error", None),
                        is_best=getattr(item, "is_best", False),
                        is_worst=getattr(item, "is_worst", False),
                    )
                )
        chart = BarChart(data=converted, title=title, options=opts, **kwargs)

    elif chart_type == "distribution_box":
        converted = []
        for item in data:
            if isinstance(item, BoxPlotSeries):
                converted.append(item)
            else:
                converted.append(
                    BoxPlotSeries(
                        name=getattr(item, "name", str(item)),
                        values=list(getattr(item, "values", [])),
                    )
                )
        chart = BoxPlot(series=converted, title=title, options=opts, **kwargs)

    elif chart_type == "query_heatmap":
        if isinstance(data, dict):
            kwargs.setdefault("value_label", "ms")
            chart = Heatmap(
                matrix=data.get("matrix", []),
                row_labels=data.get("queries", data.get("row_labels", [])),
                col_labels=data.get("platforms", data.get("col_labels", [])),
                title=title,
                options=opts,
                **kwargs,
            )
        else:
            raise VisualizationError("Heatmap data must be a dict with matrix, queries, platforms")

    elif chart_type == "query_histogram":
        converted = []
        for item in data:
            if isinstance(item, HistogramBar):
                converted.append(item)
            else:
                converted.append(
                    HistogramBar(
                        label=getattr(item, "label", str(item)),
                        value=getattr(item, "value", 0),
                        platform=getattr(item, "platform", None),
                        error=getattr(item, "error", None),
                        is_best=getattr(item, "is_best", False),
                        is_worst=getattr(item, "is_worst", False),
                    )
                )
        kwargs.setdefault("y_label", "Execution Time (ms)")
        chart = Histogram(data=converted, title=title, options=opts, **kwargs)

    elif chart_type == "cost_scatter":
        converted = []
        for item in data:
            if isinstance(item, ScatterPoint):
                converted.append(item)
            else:
                converted.append(
                    ScatterPoint(
                        name=getattr(item, "name", str(item)),
                        x=getattr(item, "x", 0),
                        y=getattr(item, "y", 0),
                    )
                )
        kwargs.setdefault("x_label", "Cost (USD)")
        kwargs.setdefault("y_label", "Performance")
        chart = ScatterPlot(points=converted, title=title, options=opts, **kwargs)

    elif chart_type == "time_series":
        converted = []
        for item in data:
            if isinstance(item, LinePoint):
                converted.append(item)
            else:
                converted.append(
                    LinePoint(
                        series=getattr(item, "series", "Series"),
                        x=getattr(item, "x", 0),
                        y=getattr(item, "y", 0),
                        label=getattr(item, "label", None),
                    )
                )
        chart = LineChart(points=converted, title=title, options=opts, **kwargs)

    elif chart_type == "comparison_bar":
        from benchbox.core.visualization.ascii_api import ComparisonBar, ComparisonBarData

        converted = []
        for item in data:
            if isinstance(item, ComparisonBarData):
                converted.append(item)
            else:
                converted.append(
                    ComparisonBarData(
                        label=getattr(item, "label", str(item)),
                        baseline_value=getattr(item, "baseline_value", 0),
                        comparison_value=getattr(item, "comparison_value", 0),
                        baseline_name=getattr(item, "baseline_name", "Baseline"),
                        comparison_name=getattr(item, "comparison_name", "Comparison"),
                    )
                )
        kwargs.setdefault("metric_label", "Execution Time (ms)")
        chart = ComparisonBar(data=converted, title=title, options=opts, **kwargs)

    elif chart_type == "diverging_bar":
        from benchbox.core.visualization.ascii_api import DivergingBar, DivergingBarData

        converted = []
        for item in data:
            if isinstance(item, DivergingBarData):
                converted.append(item)
            else:
                converted.append(
                    DivergingBarData(
                        label=getattr(item, "label", str(item)),
                        pct_change=getattr(item, "pct_change", 0),
                    )
                )
        chart = DivergingBar(data=converted, title=title, options=opts, **kwargs)

    elif chart_type == "percentile_ladder":
        from benchbox.core.visualization.ascii_api import PercentileData, PercentileLadder

        converted = []
        for item in data:
            if isinstance(item, PercentileData):
                converted.append(item)
            else:
                converted.append(
                    PercentileData(
                        name=getattr(item, "name", str(item)),
                        p50=getattr(item, "p50", 0),
                        p90=getattr(item, "p90", 0),
                        p95=getattr(item, "p95", 0),
                        p99=getattr(item, "p99", 0),
                    )
                )
        kwargs.setdefault("metric_label", "ms")
        chart = PercentileLadder(data=converted, title=title, options=opts, **kwargs)

    elif chart_type == "normalized_speedup":
        from benchbox.core.visualization.ascii_api import NormalizedSpeedup, SpeedupData

        converted = []
        for item in data:
            if isinstance(item, SpeedupData):
                converted.append(item)
            else:
                converted.append(
                    SpeedupData(
                        name=getattr(item, "name", str(item)),
                        ratio=getattr(item, "ratio", 1.0),
                        is_baseline=getattr(item, "is_baseline", False),
                    )
                )
        chart = NormalizedSpeedup(data=converted, title=title, options=opts, **kwargs)

    elif chart_type == "stacked_phase":
        from benchbox.core.visualization.ascii_api import StackedBar, StackedBarData, StackedBarSegment

        converted = []
        for item in data:
            if isinstance(item, StackedBarData):
                converted.append(item)
            else:
                segments = [
                    StackedBarSegment(
                        phase_name=getattr(s, "phase_name", str(s)),
                        value=getattr(s, "value", 0),
                        color=getattr(s, "color", None),
                    )
                    for s in getattr(item, "segments", [])
                ]
                converted.append(
                    StackedBarData(
                        label=getattr(item, "label", str(item)),
                        segments=segments,
                        total=getattr(item, "total", None),
                    )
                )
        kwargs.setdefault("metric_label", "ms")
        chart = StackedBar(data=converted, title=title, options=opts, **kwargs)

    elif chart_type == "sparkline_table":
        from benchbox.core.visualization.ascii_api import SparklineTable, SparklineTableData

        if isinstance(data, SparklineTableData):
            chart = SparklineTable(data=data, title=title, options=opts, **kwargs)
        else:
            raise VisualizationError("sparkline_table data must be a SparklineTableData instance")

    elif chart_type == "cdf_chart":
        from benchbox.core.visualization.ascii_api import CDFChart, CDFSeriesData

        converted = []
        for item in data:
            if isinstance(item, CDFSeriesData):
                converted.append(item)
            else:
                converted.append(
                    CDFSeriesData(
                        name=getattr(item, "name", str(item)),
                        values=list(getattr(item, "values", [])),
                    )
                )
        chart = CDFChart(data=converted, title=title, options=opts, **kwargs)

    elif chart_type == "rank_table":
        from benchbox.core.visualization.ascii_api import RankTable, RankTableData

        if isinstance(data, RankTableData):
            chart = RankTable(data=data, title=title, options=opts, **kwargs)
        else:
            raise VisualizationError("rank_table data must be a RankTableData instance")

    elif chart_type == "summary_box":
        from benchbox.core.visualization.ascii_api import SummaryBox, SummaryStats

        if isinstance(data, SummaryStats):
            stats = data
        elif isinstance(data, dict):
            field_names = [f.name for f in SummaryStats.__dataclass_fields__.values()]
            stats_payload = {
                "title": title or "Benchmark Summary",
                "primary_label": "Geo Mean",
                "secondary_label": "Median",
                "total_label": "Total",
                "count_label": "Queries",
            }
            stats_payload.update({k: v for k, v in data.items() if k in field_names})
            stats = SummaryStats(**stats_payload)
        else:
            stats = SummaryStats(
                title=title or "Benchmark Summary",
                primary_label="Geo Mean",
                secondary_label="Median",
                total_label="Total",
                count_label="Queries",
            )
        chart = SummaryBox(stats=stats, options=opts)

    else:
        raise VisualizationError(f"Unsupported ASCII chart type: {chart_type}")

    return chart.render()
