"""High-level chart orchestration for benchmark results."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import benchbox
from benchbox.core.results.loader import find_latest_result
from benchbox.core.results.models import BenchmarkResults
from benchbox.core.visualization.charts import (
    BarDatum,
    CostPerformancePoint,
    CostPerformanceScatterPlot,
    DistributionBoxPlot,
    DistributionSeries,
    PerformanceBarChart,
    QueryVarianceHeatmap,
    TimeSeriesLineChart,
    TimeSeriesPoint,
)
from benchbox.core.visualization.exceptions import VisualizationError
from benchbox.core.visualization.exporters import export_figure
from benchbox.core.visualization.styles import ThemeSettings, get_theme
from benchbox.core.visualization.templates import ChartTemplate, get_template
from benchbox.core.visualization.utils import slugify
from benchbox.utils.scale_factor import format_scale_factor

logger = logging.getLogger(__name__)


@dataclass
class NormalizedQuery:
    query_id: str
    execution_time_ms: float | None
    status: str = "UNKNOWN"


@dataclass
class NormalizedResult:
    benchmark: str
    platform: str
    scale_factor: str | float | int
    execution_id: str | None
    timestamp: datetime | None
    total_time_ms: float | None
    avg_time_ms: float | None
    success_rate: float | None
    cost_total: float | None
    queries: list[NormalizedQuery] = field(default_factory=list)
    source_path: Path | None = None
    raw: dict[str, Any] = field(default_factory=dict)


class ResultPlotter:
    """Generate publication-ready charts from BenchBox results."""

    def __init__(self, results: Sequence[NormalizedResult], theme: ThemeSettings | str = "light"):
        if not results:
            raise VisualizationError("No results provided for visualization.")
        self.results = list(results)
        self.theme = theme if isinstance(theme, ThemeSettings) else get_theme(theme)

    # ------------------------------------------------------------------ Loading
    @classmethod
    def from_sources(
        cls,
        sources: Sequence[str | Path] | None = None,
        theme: ThemeSettings | str = "light",
    ) -> ResultPlotter:
        """Create a plotter from JSON result files or directories."""
        normalized: list[NormalizedResult] = []
        resolved_sources = cls._expand_sources(sources)
        for path in resolved_sources:
            try:
                with open(path, encoding="utf-8") as handle:
                    data = json.load(handle)
            except FileNotFoundError:
                logger.warning("Result file not found: %s", path)
                continue
            except json.JSONDecodeError as exc:
                logger.warning("Skipping invalid JSON file %s: %s", path, exc)
                continue

            normalized.append(cls._normalize_dict(data, source_path=path))

        if not normalized:
            raise VisualizationError("No valid result files found for visualization.")
        return cls(normalized, theme=theme)

    @classmethod
    def from_benchmark_results(
        cls,
        results: Sequence[BenchmarkResults],
        theme: ThemeSettings | str = "light",
    ) -> ResultPlotter:
        normalized = [cls._normalize_benchmark_result(res) for res in results]
        return cls(normalized, theme=theme)

    @staticmethod
    def _expand_sources(sources: Sequence[str | Path] | None) -> list[Path]:
        if sources:
            candidates = []
            for source in sources:
                path = Path(source)
                if path.is_dir():
                    candidates.extend(path.rglob("*.json"))
                elif path.is_file():
                    candidates.append(path)
            return sorted({p.resolve() for p in candidates})

        latest = find_latest_result(Path("benchmark_runs/results"))
        if not latest:
            raise VisualizationError("No result sources provided and none found in benchmark_runs/results.")
        return [latest]

    @staticmethod
    def _normalize_dict(data: dict[str, Any], source_path: Path | None) -> NormalizedResult:
        benchmark_block = data.get("benchmark", {}) or {}
        execution_block = data.get("execution", {}) or {}
        results_block = data.get("results", {}) or {}
        queries_block = results_block.get("queries", {}) or {}
        timing_block = results_block.get("timing", {}) or {}
        cost_block = data.get("cost_summary") or results_block.get("cost_summary") or {}

        benchmark = benchmark_block.get("name") or benchmark_block.get("id") or "unknown"
        platform = execution_block.get("platform") or (data.get("platform") or {}).get("name", "unknown")
        scale = benchmark_block.get("scale_factor") or "unknown"
        execution_id = execution_block.get("id") or execution_block.get("execution_id")
        timestamp_raw = execution_block.get("timestamp")
        timestamp = None
        if timestamp_raw:
            try:
                timestamp = datetime.fromisoformat(timestamp_raw)
            except ValueError:
                timestamp = None

        total_ms = timing_block.get("total_ms")
        avg_ms = timing_block.get("avg_ms")
        success_rate = queries_block.get("success_rate")
        cost_total = cost_block.get("total_cost")

        queries: list[NormalizedQuery] = []
        for query in queries_block.get("details", []) or []:
            queries.append(
                NormalizedQuery(
                    query_id=query.get("id") or query.get("query_id") or "unknown",
                    execution_time_ms=query.get("execution_time_ms"),
                    status=query.get("status", "UNKNOWN"),
                )
            )

        return NormalizedResult(
            benchmark=str(benchmark),
            platform=str(platform),
            scale_factor=scale,
            execution_id=execution_id,
            timestamp=timestamp,
            total_time_ms=total_ms,
            avg_time_ms=avg_ms,
            success_rate=success_rate,
            cost_total=cost_total,
            queries=queries,
            source_path=source_path,
            raw=data,
        )

    @staticmethod
    def _normalize_benchmark_result(result: BenchmarkResults) -> NormalizedResult:
        timestamp = getattr(result, "timestamp", None)
        if timestamp and not isinstance(timestamp, datetime):
            try:
                timestamp = datetime.fromisoformat(str(timestamp))
            except Exception:
                timestamp = None

        queries: list[NormalizedQuery] = []
        for query in getattr(result, "query_results", []) or []:
            execution_time_ms = query.get("execution_time_ms")
            if execution_time_ms is None and "execution_time" in query:
                execution_time_ms = float(query["execution_time"]) * 1000.0
            queries.append(
                NormalizedQuery(
                    query_id=query.get("query_id") or query.get("id") or "unknown",
                    execution_time_ms=execution_time_ms,
                    status=query.get("status", "UNKNOWN"),
                )
            )

        total_ms = getattr(result, "total_execution_time", None)
        if total_ms is not None:
            total_ms = float(total_ms) * 1000.0

        avg_ms = getattr(result, "average_query_time", None)
        if avg_ms is not None:
            avg_ms = float(avg_ms) * 1000.0

        success_rate = None
        if getattr(result, "total_queries", None):
            success_rate = (result.successful_queries or 0) / float(result.total_queries)

        cost_summary = getattr(result, "cost_summary", None) or {}

        return NormalizedResult(
            benchmark=str(getattr(result, "benchmark_name", "unknown")),
            platform=str(getattr(result, "platform", "unknown")),
            scale_factor=getattr(result, "scale_factor", "unknown"),
            execution_id=getattr(result, "execution_id", None),
            timestamp=timestamp,
            total_time_ms=total_ms,
            avg_time_ms=avg_ms,
            success_rate=success_rate,
            cost_total=cost_summary.get("total_cost") if isinstance(cost_summary, dict) else None,
            queries=queries,
            source_path=None,
            raw={},
        )

    # ---------------------------------------------------------------- Generation
    def generate_all_charts(
        self,
        output_dir: str | Path,
        formats: Sequence[str] | None = None,
        template_name: str | None = None,
        chart_types: Sequence[str] | None = None,
        smart: bool = True,
        dpi: int = 300,
    ) -> dict[str, dict[str, Path]]:
        """Generate charts for the provided results."""
        output_path = Path(output_dir)
        template: ChartTemplate | None = None
        if template_name:
            template = get_template(template_name)

        chosen_formats = list(formats or (template.formats if template else ("png", "html")))
        chosen_chart_types = list(chart_types or (template.chart_types if template else ()))
        if not chosen_chart_types:
            chosen_chart_types = self._suggest_chart_types() if smart else ["performance_bar"]

        available_renderers = {
            "performance_bar": self._render_performance_bar,
            "distribution_box": self._render_distribution_box,
            "query_heatmap": self._render_query_heatmap,
            "cost_scatter": self._render_cost_scatter,
            "time_series": self._render_time_series,
        }

        exports: dict[str, dict[str, Path]] = {}
        for chart_type in chosen_chart_types:
            renderer = available_renderers.get(chart_type)
            if not renderer:
                logger.warning("Unknown chart type '%s' requested; skipping.", chart_type)
                continue

            figure_info = renderer()
            if figure_info is None:
                continue

            fig, base_name = figure_info
            metadata = self._export_metadata(chart_type=chart_type)
            export_paths = export_figure(
                fig,
                output_dir=output_path,
                base_name=base_name,
                formats=chosen_formats,
                dpi=dpi,
                metadata=metadata,
                theme=self.theme,
            )
            exports[chart_type] = export_paths
        return exports

    # ----------------------------------------------------------------- Renderers
    def _render_performance_bar(self):
        metric_label = "Total Runtime (s)"
        bars: list[BarDatum] = []
        for result in self.results:
            value_ms = result.total_time_ms or (result.avg_time_ms or 0) * len(result.queries or [0])
            value_seconds = (value_ms or 0) / 1000.0
            bars.append(BarDatum(label=result.platform, value=value_seconds, platform=result.platform))

        if not bars:
            logger.info("No performance data available for bar chart.")
            return None

        # Highlight best/worst (lower is better)
        values = [bar.value for bar in bars]
        if values:
            best = min(values)
            worst = max(values)
            for bar in bars:
                bar.is_best = bar.value == best
                bar.is_worst = bar.value == worst

        chart = PerformanceBarChart(
            data=bars,
            title=f"{self._benchmark_label()} Performance",
            metric_label=metric_label,
            sort_by="value",
        )
        return chart.figure(), f"{self._slug_prefix()}performance"

    def _render_distribution_box(self):
        series: list[DistributionSeries] = []
        for result in self.results:
            times = [q.execution_time_ms for q in result.queries if q.execution_time_ms is not None]
            if times:
                series.append(DistributionSeries(name=result.platform, values=times))

        if not series:
            logger.info("No per-query timings available for distribution chart.")
            return None

        chart = DistributionBoxPlot(
            series=series,
            title=f"{self._benchmark_label()} Latency Distribution",
            y_title="Execution Time (ms)",
        )
        return chart.figure(), f"{self._slug_prefix()}distribution"

    def _render_query_heatmap(self):
        # Build a matrix of query execution times (ms)
        query_ids = sorted({q.query_id for result in self.results for q in result.queries})
        if not query_ids or len(self.results) < 2:
            logger.info("Heatmap requires at least one query and multiple platforms.")
            return None

        platform_names = [result.platform for result in self.results]
        matrix: list[list[float | None]] = []
        for query_id in query_ids:
            row = []
            for result in self.results:
                match = next((q for q in result.queries if q.query_id == query_id), None)
                row.append(match.execution_time_ms if match else None)
            matrix.append(row)

        chart = QueryVarianceHeatmap(
            matrix=matrix,
            queries=query_ids,
            platforms=platform_names,
            title=f"{self._benchmark_label()} Query Variance",
        )
        return chart.figure(), f"{self._slug_prefix()}query-variance"

    def _render_cost_scatter(self):
        points: list[CostPerformancePoint] = []
        for result in self.results:
            if result.cost_total is None:
                continue

            performance = self._performance_score(result)
            points.append(
                CostPerformancePoint(
                    name=result.platform,
                    performance=performance,
                    cost=result.cost_total,
                    platform=result.platform,
                    metadata={"execution_id": result.execution_id} if result.execution_id else None,
                )
            )

        if not points:
            logger.info("No cost data available for cost-performance scatter plot.")
            return None

        chart = CostPerformanceScatterPlot(
            points=points,
            title=f"{self._benchmark_label()} Cost vs Performance",
            performance_label="Performance score (higher is better)",
            cost_label="Total cost (USD)",
        )
        return chart.figure(), f"{self._slug_prefix()}cost-performance"

    def _render_time_series(self):
        if len(self.results) < 2:
            logger.info("Time-series chart requires at least two results.")
            return None

        points: list[TimeSeriesPoint] = []
        for result in self.results:
            x_value = result.timestamp.isoformat() if result.timestamp else (result.execution_id or "run")
            points.append(
                TimeSeriesPoint(
                    series=result.platform,
                    x=x_value,
                    y=(result.total_time_ms or 0) / 1000.0,
                    label=self._format_scale(result.scale_factor) if result.scale_factor else None,
                )
            )

        chart = TimeSeriesLineChart(
            points=points,
            title=f"{self._benchmark_label()} Performance Trend",
            metric_label="Total Runtime (s)",
        )
        return chart.figure(), f"{self._slug_prefix()}trend"

    # ---------------------------------------------------------------- Utilities
    def _suggest_chart_types(self) -> list[str]:
        types = ["performance_bar"]
        if any(result.cost_total is not None for result in self.results):
            types.append("cost_scatter")
        if any(result.queries for result in self.results):
            types.append("distribution_box")
        if len(self.results) > 1 and any(result.queries for result in self.results):
            types.append("query_heatmap")
        if len(self.results) > 2:
            types.append("time_series")
        return types

    def _performance_score(self, result: NormalizedResult) -> float:
        if result.avg_time_ms and result.avg_time_ms > 0:
            return 1000.0 / result.avg_time_ms
        if result.total_time_ms and result.total_time_ms > 0 and result.queries:
            return (len(result.queries) * 1000.0) / result.total_time_ms
        return 0.0

    def _export_metadata(self, chart_type: str) -> dict[str, Any]:
        return {
            "chart_type": chart_type,
            "benchmark": self._benchmark_label(),
            "scale_factor": self._format_scale(self.results[0].scale_factor),
            "platforms": sorted({r.platform for r in self.results}),
            "benchbox_version": benchbox.__version__,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sources": [str(r.source_path) for r in self.results if r.source_path],
        }

    def _benchmark_label(self) -> str:
        benchmarks = {r.benchmark for r in self.results}
        if len(benchmarks) == 1:
            return next(iter(benchmarks))
        return ", ".join(sorted(benchmarks))

    def _slug_prefix(self) -> str:
        primary = self.results[0]
        benchmark = slugify(primary.benchmark)
        platforms = "-".join(sorted({slugify(r.platform) for r in self.results}))
        return f"{benchmark}_{platforms}_"

    def _format_scale(self, scale: str | float | int) -> str:
        try:
            return format_scale_factor(float(scale))
        except Exception:
            return str(scale)

    def group_by(self, field: str) -> dict[str, ResultPlotter]:
        """Split results by a field (platform or benchmark) for batch rendering."""
        if field not in {"platform", "benchmark"}:
            raise VisualizationError("group_by must be 'platform' or 'benchmark'.")

        groups: dict[str, list[NormalizedResult]] = {}
        for result in self.results:
            key = getattr(result, field)
            groups.setdefault(key, []).append(result)

        return {key: ResultPlotter(values, theme=self.theme) for key, values in groups.items()}
