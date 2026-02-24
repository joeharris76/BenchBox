"""Post-run summary chart generation for automatic display after benchmark runs.

Provides a single shared function used by both CLI and MCP to render
summary charts from a BenchmarkResults object.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import math
import re
import statistics
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from benchbox.core.results.query_normalizer import format_query_id, normalize_query_id
from benchbox.core.visualization.ascii.bar_chart import ASCIIBarChart, BarData
from benchbox.core.visualization.ascii.base import ASCIIChartOptions
from benchbox.core.visualization.ascii.histogram import ASCIIQueryHistogram, HistogramBar
from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

if TYPE_CHECKING:
    from benchbox.core.results.models import BenchmarkResults

logger = logging.getLogger(__name__)


@dataclass
class PostRunSummary:
    """Rendered summary charts from a single benchmark run."""

    summary_box: str
    query_histogram: str
    charts: list[str] = field(default_factory=list)


def generate_post_run_summary(
    result: BenchmarkResults,
    *,
    theme: str = "dark",
    color: bool = True,
    unicode: bool = True,
    max_width: int | None = None,
) -> PostRunSummary:
    """Build summary charts from a single benchmark run result.

    Args:
        result: Completed benchmark results with query timing data.
        theme: Color theme ("dark" or "light").
        color: Whether to use ANSI colors.
        unicode: Whether to use Unicode box-drawing characters.
        max_width: Maximum chart width (None for auto-detect).

    Returns:
        PostRunSummary with rendered ASCII charts.
    """
    options = ASCIIChartOptions(
        theme=theme,
        use_color=color,
        use_unicode=unicode,
    )
    if max_width is not None:
        options.width = max_width

    # Extract successful query results with timing data
    successful = [
        q for q in (result.query_results or []) if q.get("status") == "SUCCESS" and q.get("execution_time_ms")
    ]

    if not successful:
        # Nothing to chart — return empty summary
        return PostRunSummary(summary_box="", query_histogram="", charts=[])

    # Aggregate per-query: group by canonical query ID, compute mean latency.
    # Multiple executions per query occur with multi-stream power runs.
    query_timings: dict[str, list[float]] = {}
    query_display_ids: dict[str, str] = {}
    query_order: list[str] = []
    for q in successful:
        raw_query_id = q["query_id"]
        canonical_query_id = normalize_query_id(raw_query_id)
        if canonical_query_id not in query_timings:
            query_order.append(canonical_query_id)
            if re.fullmatch(r"\d+[A-Za-z]*", canonical_query_id):
                query_display_ids[canonical_query_id] = format_query_id(canonical_query_id, with_prefix=True)
            else:
                query_display_ids[canonical_query_id] = canonical_query_id
        query_timings.setdefault(canonical_query_id, []).append(q["execution_time_ms"])

    query_means: dict[str, float] = {qid: sum(timings) / len(timings) for qid, timings in query_timings.items()}

    mean_values = [query_means[qid] for qid in query_order]

    # Geometric mean of per-query means
    log_sum = sum(math.log(max(t, 0.001)) for t in mean_values)
    geo_mean = math.exp(log_sum / len(mean_values))

    total_time = sum(mean_values)
    median_time = statistics.median(mean_values)

    # Best (fastest) and worst (slowest) by per-query mean
    sorted_queries = sorted(query_order, key=lambda qid: query_means[qid])
    best = [(query_display_ids[qid], query_means[qid]) for qid in sorted_queries[:3]]
    worst = [(query_display_ids[qid], query_means[qid]) for qid in sorted_queries[-3:]]
    worst.reverse()  # Slowest first

    title = f"{result.benchmark_name} on {result.platform} (SF {result.scale_factor})"

    # Extract system environment info from result if available
    environment = _extract_environment(result.system_profile, platform_info=result.platform_info)

    stats = SummaryStats(
        title=title,
        geo_mean_ms=geo_mean,
        median_ms=median_time,
        total_time_ms=total_time,
        num_queries=len(query_order),
        best_queries=best,
        worst_queries=worst,
        environment=environment,
    )

    summary_box_chart = ASCIISummaryBox(stats, options=options)
    summary_box_text = summary_box_chart.render()

    # Build query latency chart — choose orientation based on label length.
    # Vertical histogram works for short numeric IDs (Q1-Q22).
    # Horizontal bars are more readable for long descriptive names
    # (e.g., "aggregation_groupby_large").
    best_qid = sorted_queries[0] if sorted_queries else None
    worst_qid = sorted_queries[-1] if sorted_queries else None
    display_ids = [query_display_ids[qid] for qid in query_order]
    use_horizontal = _should_use_horizontal(display_ids)

    if use_horizontal:
        histogram_text = _render_horizontal_bars(
            query_order, query_display_ids, query_means, best_qid, worst_qid, options
        )
    else:
        histogram_bars = [
            HistogramBar(
                query_id=query_display_ids[qid],
                latency_ms=query_means[qid],
                is_best=(qid == best_qid),
                is_worst=(qid == worst_qid),
            )
            for qid in query_order
        ]
        histogram_chart = ASCIIQueryHistogram(
            data=histogram_bars,
            title="Query Latency",
            options=options,
        )
        histogram_text = histogram_chart.render()

    charts = [summary_box_text, histogram_text]

    return PostRunSummary(
        summary_box=summary_box_text,
        query_histogram=histogram_text,
        charts=charts,
    )


_HORIZONTAL_LABEL_THRESHOLD = 6


def _should_use_horizontal(display_ids: list[str]) -> bool:
    """Return True if horizontal bars are preferable for these query labels.

    Uses median label length: if the median exceeds the threshold, vertical
    bars would truncate most labels to unreadable 2-3 character stubs.
    """
    if not display_ids:
        return False
    median_len = statistics.median(len(qid) for qid in display_ids)
    return median_len > _HORIZONTAL_LABEL_THRESHOLD


def _render_horizontal_bars(
    query_order: list[str],
    query_display_ids: dict[str, str],
    query_means: dict[str, float],
    best_qid: str | None,
    worst_qid: str | None,
    options: ASCIIChartOptions,
) -> str:
    """Render a horizontal bar chart for queries with long names."""
    bars = [
        BarData(
            label=query_display_ids[qid],
            value=query_means[qid],
            is_best=(qid == best_qid),
            is_worst=(qid == worst_qid),
        )
        for qid in query_order
    ]
    chart = ASCIIBarChart(
        data=bars,
        title="Query Latency",
        metric_label="ms",
        sort_by="value",
        options=options,
    )
    return chart.render()


def _extract_environment(
    system_profile: dict | None,
    platform_info: dict | None = None,
) -> dict[str, str] | None:
    """Build an ordered environment dict from a system_profile dict.

    Accepts both SystemProfile keys (os_name, cpu_cores_logical, memory_total_gb)
    and SystemInfo.to_dict() keys (os_type, cpu_cores, total_memory_gb)
    and loader.py keys (os_type, cpu_count, memory_gb).

    Returns None if the profile is missing or has no usable fields.
    """
    if not system_profile and not platform_info:
        return None

    env: dict[str, str] = {}

    if system_profile:
        os_name = system_profile.get("os_name") or system_profile.get("os_type", "")
        os_version = system_profile.get("os_version") or system_profile.get("os_release", "")
        if os_name:
            env["OS"] = f"{os_name} {os_version}".strip()

        python_version = system_profile.get("python_version", "")
        if python_version:
            env["Python"] = python_version

        cpus = (
            system_profile.get("cpu_cores_logical")
            or system_profile.get("cpu_cores")
            or system_profile.get("cpu_count")
        )
        arch = system_profile.get("architecture", "")
        if cpus:
            env["CPUs"] = f"{cpus} ({arch})" if arch else str(cpus)

        mem_gb = (
            system_profile.get("memory_total_gb")
            or system_profile.get("total_memory_gb")
            or system_profile.get("memory_gb")
        )
        if mem_gb is not None:
            env["Memory"] = f"{mem_gb:.0f} GB"

    if platform_info and isinstance(platform_info, dict):
        version = (
            platform_info.get("platform_version")
            or platform_info.get("version")
            or platform_info.get("driver_version_actual")
        )
        if version:
            platform_name = platform_info.get("platform_name") or platform_info.get("name", "")
            env["Driver"] = f"{platform_name} {version}".strip() if platform_name else str(version)

    return env if env else None
