"""Visualization tools for BenchBox MCP server.

Provides tools for generating ASCII charts and visualizations from benchmark results.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from benchbox.mcp.errors import ErrorCode, make_error

logger = logging.getLogger(__name__)

# Tool annotations for read-only visualization info tools
VIZ_READONLY_ANNOTATIONS = ToolAnnotations(
    title="Visualization information",
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)

# Tool annotations for chart generation (creates files)
VIZ_GENERATE_ANNOTATIONS = ToolAnnotations(
    title="Generate visualization",
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)

# Default output directories
DEFAULT_CHARTS_DIR = Path("benchmark_runs/charts")
DEFAULT_RESULTS_DIR = Path("benchmark_runs/results")

# Supported chart types
CHART_TYPE_DESCRIPTIONS: dict[str, str] = {
    "performance_bar": "Bar chart comparing total runtime across platforms",
    "distribution_box": "Box plot showing query execution time distribution",
    "query_heatmap": "Heatmap comparing per-query execution times across platforms",
    "query_histogram": "Vertical bar histogram showing latency per query (auto-splits for >33 queries)",
    "cost_scatter": "Scatter plot of cost vs performance (requires cost data)",
    "time_series": "Line chart showing performance trends over time",
    "comparison_bar": "Paired side-by-side bars comparing two runs per query with % change annotations",
    "diverging_bar": "Centered-zero chart showing regression/improvement distribution sorted by magnitude",
    "summary_box": "Bordered panel with aggregate stats (geo mean, total time, improved/regressed counts)",
}


def _resolve_result_path(result_file: str) -> Path | None:
    """Resolve a result file path within the results directory."""
    if ".." in result_file or result_file.startswith("/") or result_file.startswith("\\"):
        return None

    path = DEFAULT_RESULTS_DIR / result_file
    if path.exists():
        try:
            results_dir_resolved = DEFAULT_RESULTS_DIR.resolve()
            path_resolved = path.resolve()
            if not str(path_resolved).startswith(str(results_dir_resolved)):
                return None
        except Exception:
            return None
        return path

    if not result_file.endswith(".json"):
        path = DEFAULT_RESULTS_DIR / (result_file + ".json")
        if path.exists():
            try:
                results_dir_resolved = DEFAULT_RESULTS_DIR.resolve()
                path_resolved = path.resolve()
                if not str(path_resolved).startswith(str(results_dir_resolved)):
                    return None
            except Exception:
                return None
            return path

    return None


def _extract_chart_metadata(results: list) -> dict[str, Any]:
    """Extract chart metadata from results for subtitle display."""
    metadata: dict[str, Any] = {}
    if not results:
        return metadata

    r = results[0]
    metadata["benchmark"] = r.benchmark
    metadata["scale_factor"] = r.scale_factor

    # Extract platform version from raw result data
    raw = getattr(r, "raw", {}) or {}
    platform_block = raw.get("platform") or raw.get("platform_info") or {}
    version = platform_block.get("version")
    if version:
        metadata["platform_version"] = f"{r.platform} {version}"

    # Extract tuning config
    config_block = raw.get("config") or {}
    tuning = config_block.get("tuning") or config_block.get("tuning_config")
    if tuning:
        metadata["tuning"] = tuning

    return metadata


def _render_single_ascii_chart(
    results: list,
    chart_type: str,
    opts: Any,
) -> str | None:
    """Render a single ASCII chart for the given chart type.

    Returns the rendered chart string, or None if chart type is unsupported.
    """
    from benchbox.core.visualization.ascii import (
        ASCIIBarChart,
        ASCIIBoxPlot,
        ASCIIHeatmap,
        ASCIILineChart,
        ASCIIQueryHistogram,
        ASCIIScatterPlot,
    )
    from benchbox.core.visualization.ascii.bar_chart import BarData
    from benchbox.core.visualization.ascii.box_plot import BoxPlotSeries
    from benchbox.core.visualization.ascii.histogram import HistogramBar
    from benchbox.core.visualization.ascii.line_chart import LinePoint
    from benchbox.core.visualization.ascii.scatter_plot import ScatterPoint

    metadata = _extract_chart_metadata(results)

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
        return chart.render()

    elif chart_type == "distribution_box":
        series_data: list[BoxPlotSeries] = []
        for r in results:
            timings = [q.execution_time_ms for q in r.queries if q.execution_time_ms is not None]
            if timings:
                series_data.append(BoxPlotSeries(name=r.platform, values=timings))

        if not series_data:
            return None

        chart = ASCIIBoxPlot(
            series=series_data,
            title="Query Time Distribution",
            y_label="Execution Time (ms)",
            options=opts,
            metadata=metadata,
        )
        return chart.render()

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

        if not query_ids:
            return None

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
        return chart.render()

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

        if not histogram_data:
            return None

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
        return chart.render()

    elif chart_type == "cost_scatter":
        scatter_data: list[ScatterPoint] = []
        for r in results:
            total_time = r.total_time_ms or 0
            cost = r.cost_total or 0
            performance = 3600000 / total_time if total_time > 0 else 0
            scatter_data.append(ScatterPoint(name=r.platform, x=cost, y=performance))

        if not scatter_data:
            return None

        chart = ASCIIScatterPlot(
            points=scatter_data,
            title="Cost vs Performance",
            x_label="Cost (USD)",
            y_label="Queries per Hour",
            options=opts,
            metadata=metadata,
        )
        return chart.render()

    elif chart_type == "time_series":
        line_data: list[LinePoint] = []
        for i, r in enumerate(results):
            total_time = r.total_time_ms or 0
            x_label = r.timestamp.strftime("%Y-%m-%d") if r.timestamp else f"Run {i + 1}"
            line_data.append(LinePoint(series=r.platform, x=x_label, y=total_time))

        if not line_data:
            return None

        chart = ASCIILineChart(
            points=line_data,
            title="Performance Trend",
            x_label="Run",
            y_label="Execution Time (ms)",
            options=opts,
            metadata=metadata,
        )
        return chart.render()

    elif chart_type == "comparison_bar":
        if len(results) < 2:
            return None

        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        baseline = results[0]
        comparison = results[1]

        baseline_timings: dict[str, float] = {}
        for q in baseline.queries:
            if q.execution_time_ms is not None:
                baseline_timings[q.query_id] = q.execution_time_ms

        comparison_timings: dict[str, float] = {}
        for q in comparison.queries:
            if q.execution_time_ms is not None:
                comparison_timings[q.query_id] = q.execution_time_ms

        shared_queries = sorted(set(baseline_timings) & set(comparison_timings))
        if not shared_queries:
            return None

        comp_data = [
            ComparisonBarData(
                label=qid,
                baseline_value=baseline_timings[qid],
                comparison_value=comparison_timings[qid],
                baseline_name=baseline.platform,
                comparison_name=comparison.platform,
            )
            for qid in shared_queries
        ]

        chart = ASCIIComparisonBar(
            data=comp_data,
            title=f"{baseline.platform} vs {comparison.platform}",
            metric_label="Execution Time (ms)",
            options=opts,
            metadata=metadata,
        )
        return chart.render()

    elif chart_type == "diverging_bar":
        if len(results) < 2:
            return None

        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        baseline = results[0]
        comparison = results[1]

        baseline_timings: dict[str, float] = {}
        for q in baseline.queries:
            if q.execution_time_ms is not None:
                baseline_timings[q.query_id] = q.execution_time_ms

        comparison_timings: dict[str, float] = {}
        for q in comparison.queries:
            if q.execution_time_ms is not None:
                comparison_timings[q.query_id] = q.execution_time_ms

        shared_queries = sorted(set(baseline_timings) & set(comparison_timings))
        if not shared_queries:
            return None

        div_data: list[DivergingBarData] = []
        for qid in shared_queries:
            b_val = baseline_timings[qid]
            c_val = comparison_timings[qid]
            pct = ((c_val - b_val) / b_val * 100) if b_val > 0 else 0
            div_data.append(DivergingBarData(label=qid, pct_change=pct))

        chart = ASCIIDivergingBar(
            data=div_data,
            title=f"{baseline.platform} vs {comparison.platform}: Changes",
            options=opts,
            metadata=metadata,
        )
        return chart.render()

    elif chart_type == "summary_box":
        import math

        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        if len(results) >= 2:
            baseline = results[0]
            comparison = results[1]

            b_times = [q.execution_time_ms for q in baseline.queries if q.execution_time_ms and q.execution_time_ms > 0]
            c_times = [
                q.execution_time_ms for q in comparison.queries if q.execution_time_ms and q.execution_time_ms > 0
            ]
            b_geo = math.exp(sum(math.log(t) for t in b_times) / len(b_times)) if b_times else None
            c_geo = math.exp(sum(math.log(t) for t in c_times) / len(c_times)) if c_times else None

            b_map = {q.query_id: q.execution_time_ms for q in baseline.queries if q.execution_time_ms is not None}
            c_map = {q.query_id: q.execution_time_ms for q in comparison.queries if q.execution_time_ms is not None}
            shared = sorted(set(b_map) & set(c_map))
            changes: list[tuple[str, float]] = []
            for qid in shared:
                bv = b_map[qid]
                cv = c_map[qid]
                pct = ((cv - bv) / bv * 100) if bv > 0 else 0
                changes.append((qid, pct))

            n_improved = sum(1 for _, p in changes if p < -2)
            n_regressed = sum(1 for _, p in changes if p > 2)
            n_stable = len(changes) - n_improved - n_regressed

            sorted_changes = sorted(changes, key=lambda x: x[1])
            best = [(q, p) for q, p in sorted_changes if p < 0][:3]
            worst = [(q, p) for q, p in reversed(sorted_changes) if p > 0][:3]

            stats = SummaryStats(
                title=f"{baseline.platform} vs {comparison.platform} Summary",
                geo_mean_baseline_ms=b_geo,
                geo_mean_comparison_ms=c_geo,
                total_time_baseline_ms=baseline.total_time_ms,
                total_time_comparison_ms=comparison.total_time_ms,
                baseline_name=baseline.platform,
                comparison_name=comparison.platform,
                num_queries=len(shared),
                num_improved=n_improved,
                num_stable=n_stable,
                num_regressed=n_regressed,
                best_queries=best,
                worst_queries=worst,
            )
        else:
            r = results[0]
            times = [q.execution_time_ms for q in r.queries if q.execution_time_ms and q.execution_time_ms > 0]
            geo = math.exp(sum(math.log(t) for t in times) / len(times)) if times else None

            sorted_by_time = sorted(
                [(q.query_id, q.execution_time_ms) for q in r.queries if q.execution_time_ms is not None],
                key=lambda x: x[1],
            )

            stats = SummaryStats(
                title=f"{r.platform} Summary",
                geo_mean_ms=geo,
                total_time_ms=r.total_time_ms,
                num_queries=len(r.queries),
                best_queries=sorted_by_time[:3],
                worst_queries=sorted_by_time[-3:][::-1] if len(sorted_by_time) > 3 else [],
            )

        chart = ASCIISummaryBox(stats=stats, options=opts, metadata=metadata)
        return chart.render()

    return None


def _generate_ascii_chart(
    resolved_paths: list[Path],
    chart_type: str,
    file_list: list[str],
    template_name: str | None = None,
) -> dict[str, Any]:
    """Generate ASCII chart from result files.

    Returns chart content as inline string.
    """
    from benchbox.core.visualization.ascii.base import ASCIIChartOptions
    from benchbox.core.visualization.result_plotter import ResultPlotter

    try:
        plotter = ResultPlotter.from_sources(resolved_paths)
        results = plotter.results

        opts = ASCIIChartOptions(use_color=False)

        if template_name is not None:
            from benchbox.core.visualization.templates import get_template

            try:
                chart_template = get_template(template_name)
            except Exception:
                from benchbox.core.visualization.templates import list_templates

                available = [t.name for t in list_templates()]
                return make_error(
                    ErrorCode.VALIDATION_ERROR,
                    f"Unknown template: {template_name}",
                    details={"available_templates": available},
                )

            chart_contents: list[str] = []
            chart_types_rendered: list[str] = []
            separator = "\n" + "─" * 60 + "\n\n"

            for ct in chart_template.chart_types:
                content = _render_single_ascii_chart(results, ct, opts)
                if content:
                    chart_contents.append(content)
                    chart_types_rendered.append(ct)

            if not chart_contents:
                return make_error(
                    ErrorCode.INTERNAL_ERROR,
                    "No charts could be generated from the template",
                    details={"template": template_name, "chart_types": list(chart_template.chart_types)},
                )

            combined_content = separator.join(chart_contents)

            return {
                "status": "generated",
                "template": template_name,
                "template_description": chart_template.description,
                "chart_types": chart_types_rendered,
                "chart_count": len(chart_contents),
                "format": "ascii",
                "content": combined_content,
                "source_files": file_list,
                "note": f"Template '{template_name}' rendered {len(chart_contents)} ASCII charts inline.",
            }

        content = _render_single_ascii_chart(results, chart_type, opts)

        if content is None:
            return make_error(
                ErrorCode.VALIDATION_ERROR,
                f"Unsupported chart type for ASCII format: {chart_type}",
                details={"valid_types": list(CHART_TYPE_DESCRIPTIONS.keys())},
            )

        return {
            "status": "generated",
            "chart_type": chart_type,
            "format": "ascii",
            "content": content,
            "source_files": file_list,
            "note": "ASCII chart rendered inline. Copy the 'content' field to display.",
        }

    except Exception as e:
        logger.exception(f"ASCII chart generation failed: {e}")
        return make_error(
            ErrorCode.INTERNAL_ERROR,
            f"ASCII chart generation failed: {e}",
            details={"exception_type": type(e).__name__},
        )


def register_visualization_tools(mcp: FastMCP) -> None:
    """Register visualization tools with the MCP server."""

    @mcp.tool(annotations=VIZ_READONLY_ANNOTATIONS)
    def suggest_charts(result_files: str) -> dict[str, Any]:
        """Analyze results and suggest appropriate chart types.

        Args:
            result_files: Comma-separated list of result filenames

        Returns:
            Chart suggestions with data profile and primary recommendation.
        """
        file_list = [f.strip() for f in result_files.split(",") if f.strip()]
        if not file_list:
            return make_error(
                ErrorCode.VALIDATION_ERROR,
                "No result files provided",
                suggestion="Provide comma-separated list of result filenames",
            )

        resolved_paths: list[Path] = []
        missing_files: list[str] = []

        for result_file in file_list:
            path = _resolve_result_path(result_file)
            if path:
                resolved_paths.append(path)
            else:
                missing_files.append(result_file)

        if missing_files:
            return make_error(
                ErrorCode.RESOURCE_NOT_FOUND,
                f"Result file(s) not found: {', '.join(missing_files)}",
                details={"missing_files": missing_files},
            )

        if not resolved_paths:
            return make_error(ErrorCode.VALIDATION_ERROR, "No valid result files found")

        from benchbox.core.visualization.exceptions import VisualizationError
        from benchbox.core.visualization.result_plotter import ResultPlotter

        try:
            plotter = ResultPlotter.from_sources(resolved_paths)
            results = plotter.results

            num_results = len(results)
            platforms = sorted({r.platform for r in results})
            has_cost_data = any(r.cost_total is not None for r in results)
            has_query_data = any(r.queries for r in results)
            has_timestamps = any(r.timestamp is not None for r in results)
            total_queries = sum(len(r.queries) for r in results)

            suggestions: list[dict[str, str]] = []

            suggestions.append(
                {
                    "chart_type": "performance_bar",
                    "reason": "Compare total runtime across platforms/configurations",
                    "priority": "high",
                }
            )

            if has_query_data:
                suggestions.append(
                    {
                        "chart_type": "distribution_box",
                        "reason": f"Show query time distribution ({total_queries} queries)",
                        "priority": "high" if total_queries > 5 else "medium",
                    }
                )

            if num_results >= 2 and has_query_data:
                suggestions.append(
                    {
                        "chart_type": "query_heatmap",
                        "reason": f"Compare per-query performance across {len(platforms)} platform(s)",
                        "priority": "high" if num_results == 2 else "medium",
                    }
                )

            if has_query_data:
                suggestions.append(
                    {
                        "chart_type": "query_histogram",
                        "reason": f"Show per-query latency bars ({total_queries} queries)"
                        + (" - auto-splits for readability" if total_queries > 33 else ""),
                        "priority": "high" if total_queries > 20 else "medium",
                    }
                )

            if has_cost_data:
                suggestions.append(
                    {
                        "chart_type": "cost_scatter",
                        "reason": "Plot cost vs performance trade-offs",
                        "priority": "high",
                    }
                )

            if num_results >= 3 and has_timestamps:
                suggestions.append(
                    {
                        "chart_type": "time_series",
                        "reason": f"Show performance trends over {num_results} runs",
                        "priority": "medium",
                    }
                )

            if num_results >= 2 and has_query_data:
                primary = "query_heatmap"
                primary_reason = "Best for comparing per-query performance"
            elif has_query_data:
                primary = "distribution_box"
                primary_reason = "Best for understanding query time distribution"
            else:
                primary = "performance_bar"
                primary_reason = "Best general-purpose comparison chart"

            return {
                "suggestions": suggestions,
                "primary": {"chart_type": primary, "reason": primary_reason},
                "data_profile": {
                    "result_count": num_results,
                    "platforms": platforms,
                    "total_queries": total_queries,
                    "has_cost_data": has_cost_data,
                    "has_timestamps": has_timestamps,
                },
                "source_files": file_list,
            }

        except VisualizationError as e:
            return make_error(
                ErrorCode.INTERNAL_ERROR,
                f"Could not analyze results: {e}",
                details={"result_files": file_list},
            )
        except Exception as e:
            logger.exception(f"Chart suggestion failed: {e}")
            return make_error(
                ErrorCode.INTERNAL_ERROR,
                f"Chart suggestion failed: {e}",
                details={"exception_type": type(e).__name__},
            )

    @mcp.tool(annotations=VIZ_GENERATE_ANNOTATIONS)
    def generate_chart(
        result_files: str,
        chart_type: str = "performance_bar",
        template: str | None = None,
        output_dir: str | None = None,
        format: str = "ascii",
    ) -> dict[str, Any]:
        """Generate chart(s) from benchmark results.

        Args:
            result_files: Comma-separated list of result filenames
            chart_type: Chart type: performance_bar, distribution_box, query_heatmap, cost_scatter, time_series, comparison_bar, diverging_bar, summary_box
            template: Template for chart sets: default, flagship, head_to_head, trends, cost_optimization, comparison
            output_dir: Custom output directory (relative to charts dir)
            format: Output format: 'ascii' for terminal-friendly text output

        Returns:
            Chart generation status with ASCII chart content if format='ascii'.

        ASCII Format Examples:
            - Single chart: generate_chart(result_files="run1.json", chart_type="performance_bar", format="ascii")
            - Template: generate_chart(result_files="run1.json,run2.json", template="head_to_head", format="ascii")
            - Comparison: generate_chart(result_files="duckdb.json,polars.json", chart_type="query_heatmap", format="ascii")

        ASCII charts are rendered inline and returned in the 'content' field. They include:
            - ANSI colors for terminal display (copy-paste preserves formatting)
            - Unicode box-drawing characters for clean visualization
            - Best/worst highlighting, legends, and scale indicators
        """
        file_list = [f.strip() for f in result_files.split(",") if f.strip()]
        if not file_list:
            return make_error(
                ErrorCode.VALIDATION_ERROR,
                "No result files provided",
                suggestion="Provide comma-separated list of result filenames",
            )

        # Validate chart type if no template
        if template is None and chart_type not in CHART_TYPE_DESCRIPTIONS:
            return make_error(
                ErrorCode.VALIDATION_ERROR,
                f"Unknown chart type: {chart_type}",
                details={"valid_chart_types": list(CHART_TYPE_DESCRIPTIONS.keys())},
            )

        # Validate template if provided
        if template is not None:
            from benchbox.core.visualization.templates import get_template, list_templates

            try:
                get_template(template)
            except Exception:
                available_templates = [t.name for t in list_templates()]
                return make_error(
                    ErrorCode.VALIDATION_ERROR,
                    f"Unknown template: {template}",
                    details={"available_templates": available_templates},
                )

        resolved_paths: list[Path] = []
        missing_files: list[str] = []

        for result_file in file_list:
            path = _resolve_result_path(result_file)
            if path:
                resolved_paths.append(path)
            else:
                missing_files.append(result_file)

        if missing_files:
            return make_error(
                ErrorCode.RESOURCE_NOT_FOUND,
                f"Result file(s) not found: {', '.join(missing_files)}",
                details={"missing_files": missing_files},
            )

        if not resolved_paths:
            return make_error(ErrorCode.VALIDATION_ERROR, "No valid result files found")

        return _generate_ascii_chart(resolved_paths, chart_type, file_list, template_name=template)
