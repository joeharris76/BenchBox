"""Plotly-based chart primitives for BenchBox visualization."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from statistics import mean

from benchbox.core.visualization.dependencies import require_plotly
from benchbox.core.visualization.exceptions import VisualizationError
from benchbox.core.visualization.styles import ThemeSettings, apply_common_layout, color_cycle, get_theme


@dataclass
class BarDatum:
    """Data point for performance bar charts."""

    label: str
    value: float
    platform: str | None = None
    error: float | None = None
    annotation: str | None = None
    group: str | None = None
    is_best: bool = False
    is_worst: bool = False


@dataclass
class TimeSeriesPoint:
    """Data point for time-series charts."""

    series: str
    x: str | float
    y: float
    label: str | None = None
    version: str | None = None


@dataclass
class CostPerformancePoint:
    """Data point for cost/performance scatter plots."""

    name: str
    performance: float
    cost: float
    platform: str | None = None
    metadata: dict | None = None


@dataclass
class DistributionSeries:
    """Data series for distribution/box plots."""

    name: str
    values: Sequence[float]


class BaseChart:
    """Base utilities shared by all chart types."""

    def __init__(self, theme: ThemeSettings | None = None):
        self.theme = theme or get_theme()

    def _apply_layout(
        self,
        fig,
        title: str | None,
        x_title: str | None,
        y_title: str | None,
        legend_title: str | None = None,
    ) -> None:
        apply_common_layout(fig, self.theme, title=title, x_title=x_title, y_title=y_title, legend_title=legend_title)


class PerformanceBarChart(BaseChart):
    """Multi-platform performance comparison bar chart."""

    def __init__(
        self,
        data: Sequence[BarDatum],
        title: str | None = None,
        metric_label: str = "Execution Time (ms)",
        sort_by: str = "value",
        barmode: str = "group",
        show_annotations: bool = True,
    ):
        super().__init__()
        if barmode not in {"group", "stack"}:
            raise VisualizationError("barmode must be 'group' or 'stack'")
        self.data = list(data)
        self.title = title or "Platform Performance Comparison"
        self.metric_label = metric_label
        self.sort_by = sort_by
        self.barmode = barmode
        self.show_annotations = show_annotations

    def figure(self):
        go, _ = require_plotly()
        fig = go.Figure()

        series_by_group: dict[str, list[BarDatum]] = {}
        for datum in self.data:
            group = datum.group or "Series"
            series_by_group.setdefault(group, []).append(datum)

        label_order = self._ordered_labels(self.data, self.sort_by)
        palette = color_cycle(self.theme.palette)

        for group_name, group_items in series_by_group.items():
            # Ensure all labels exist to keep alignment across groups
            values = []
            errors = []
            annotations = []
            colors = []
            labels = []

            label_to_item = {item.label: item for item in group_items}
            for label in label_order:
                item = label_to_item.get(label)
                if not item:
                    continue
                values.append(item.value)
                errors.append(item.error)
                annotations.append(item.annotation or "")

                if item.is_best:
                    colors.append(self.theme.best_color)
                elif item.is_worst:
                    colors.append(self.theme.worst_color)
                else:
                    colors.append(next(palette))
                labels.append(label)

            fig.add_bar(
                x=labels,
                y=values,
                name=group_name,
                error_y={"type": "data", "array": errors, "visible": any(e is not None for e in errors)},
                marker_color=colors,
                text=annotations if self.show_annotations else None,
                textposition="outside",
            )

        fig.update_layout(barmode=self.barmode)
        self._apply_layout(fig, title=self.title, x_title="Platform", y_title=self.metric_label, legend_title="Series")
        return fig

    @staticmethod
    def _ordered_labels(data: Sequence[BarDatum], sort_by: str) -> list[str]:
        if sort_by == "value":
            totals: dict[str, float] = {}
            for item in data:
                totals[item.label] = totals.get(item.label, 0.0) + float(item.value)
            return [label for label, _ in sorted(totals.items(), key=lambda pair: pair[1], reverse=True)]
        if sort_by == "label":
            return sorted({item.label for item in data})
        return list({item.label for item in data})


class TimeSeriesLineChart(BaseChart):
    """Performance trend line chart with optional regression overlay."""

    def __init__(
        self,
        points: Sequence[TimeSeriesPoint],
        title: str | None = None,
        metric_label: str = "Execution Time (ms)",
        show_trend: bool = True,
    ):
        super().__init__()
        self.points = list(points)
        self.title = title or "Performance Trend"
        self.metric_label = metric_label
        self.show_trend = show_trend

    def figure(self):
        go, _ = require_plotly()
        fig = go.Figure()
        palette = color_cycle(self.theme.palette)

        series_map: dict[str, list[TimeSeriesPoint]] = {}
        for point in self.points:
            series_map.setdefault(point.series, []).append(point)

        for series_name, series_points in series_map.items():
            series_points = sorted(series_points, key=lambda p: p.x)
            x_values = [p.x for p in series_points]
            y_values = [p.y for p in series_points]
            text = [p.label for p in series_points]
            fig.add_scatter(
                x=x_values,
                y=y_values,
                mode="lines+markers",
                name=series_name,
                marker={"size": 8, "color": next(palette)},
                text=text,
                textposition="top center",
            )

            if self.show_trend and len(series_points) >= 3:
                trend = self._trendline(y_values)
                fig.add_scatter(
                    x=x_values,
                    y=trend,
                    mode="lines",
                    name=f"{series_name} trend",
                    line={"dash": "dash", "width": 2, "color": "#6b7075"},
                    showlegend=False,
                )

        self._apply_layout(fig, title=self.title, x_title="Run", y_title=self.metric_label, legend_title="Series")
        return fig

    @staticmethod
    def _trendline(values: Sequence[float]) -> list[float]:
        """Simple least-squares fit using ordinal positions for x."""
        n = len(values)
        x_vals = list(range(n))
        mean_x = mean(x_vals)
        mean_y = mean(values)
        denom = sum((x - mean_x) ** 2 for x in x_vals)
        if denom == 0:
            return list(values)
        slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_vals, values)) / denom
        intercept = mean_y - slope * mean_x
        return [intercept + slope * x for x in x_vals]


class CostPerformanceScatterPlot(BaseChart):
    """Scatter plot with Pareto frontier highlighting for cost vs performance."""

    def __init__(
        self,
        points: Sequence[CostPerformancePoint],
        title: str | None = None,
        performance_label: str = "Queries per Hour (higher is better)",
        cost_label: str = "Cost (USD)",
    ):
        super().__init__()
        self.points = list(points)
        self.title = title or "Cost vs Performance"
        self.performance_label = performance_label
        self.cost_label = cost_label

    def figure(self):
        go, _ = require_plotly()
        fig = go.Figure()
        palette = color_cycle(self.theme.palette)

        frontier = self._pareto_frontier(self.points)
        frontier_names = {point.name for point in frontier}

        for point in self.points:
            is_frontier = point.name in frontier_names
            fig.add_scatter(
                x=[point.cost],
                y=[point.performance],
                mode="markers+text",
                name=point.name,
                marker={
                    "size": 12,
                    "color": self.theme.best_color if is_frontier else next(palette),
                    "line": {"width": 1, "color": self.theme.text_color},
                },
                text=[point.platform or point.name],
                textposition="top center",
                hovertemplate=self._hover_text(point),
            )

        # Draw Pareto frontier line for emphasis
        if frontier:
            frontier_sorted = sorted(frontier, key=lambda p: p.cost)
            fig.add_scatter(
                x=[p.cost for p in frontier_sorted],
                y=[p.performance for p in frontier_sorted],
                mode="lines",
                name="Pareto frontier",
                line={"dash": "dash", "color": self.theme.best_color},
            )

        self._apply_layout(fig, title=self.title, x_title=self.cost_label, y_title=self.performance_label)
        return fig

    @staticmethod
    def _pareto_frontier(points: Sequence[CostPerformancePoint]) -> list[CostPerformancePoint]:
        sorted_points = sorted(points, key=lambda p: (p.cost, -p.performance))
        frontier: list[CostPerformancePoint] = []
        best_perf = float("-inf")
        for point in sorted_points:
            if point.performance >= best_perf:
                frontier.append(point)
                best_perf = point.performance
        return frontier

    @staticmethod
    def _hover_text(point: CostPerformancePoint) -> str:
        platform = point.platform or point.name
        meta_lines = []
        if point.metadata:
            meta_lines = [f"{k}: {v}" for k, v in point.metadata.items()]
        meta = "<br>".join(meta_lines)
        return "<br>".join(
            [
                f"<b>{platform}</b>",
                f"Performance: {point.performance:.2f}",
                f"Cost: ${point.cost:,.2f}",
                meta,
                "<extra></extra>",
            ]
        )


class QueryVarianceHeatmap(BaseChart):
    """Query × platform variance heatmap."""

    def __init__(
        self,
        matrix: Sequence[Sequence[float]],
        queries: Sequence[str],
        platforms: Sequence[str],
        title: str | None = None,
        colorbar_title: str = "Coefficient of Variation",
    ):
        super().__init__()
        self.matrix = matrix
        self.queries = queries
        self.platforms = platforms
        self.title = title or "Query Variance Heatmap"
        self.colorbar_title = colorbar_title

    def figure(self):
        go, _ = require_plotly()
        fig = go.Figure(
            data=go.Heatmap(
                z=self.matrix,
                x=self.platforms,
                y=self.queries,
                colorscale="RdYlBu_r",
                colorbar={"title": self.colorbar_title},
                hoverongaps=False,
            )
        )
        self._apply_layout(fig, title=self.title, x_title="Platform", y_title="Query ID")
        fig.update_layout(yaxis={"autorange": "reversed"})
        return fig


class DistributionBoxPlot(BaseChart):
    """Latency distribution box plots."""

    def __init__(
        self,
        series: Sequence[DistributionSeries],
        title: str | None = None,
        y_title: str = "Execution Time (ms)",
        show_mean: bool = True,
    ):
        super().__init__()
        self.series = list(series)
        self.title = title or "Latency Distribution"
        self.y_title = y_title
        self.show_mean = show_mean

    def figure(self):
        go, _ = require_plotly()
        fig = go.Figure()
        colors = color_cycle(self.theme.palette)

        for serie in self.series:
            fig.add_box(
                y=list(serie.values),
                name=serie.name,
                marker_color=next(colors),
                boxmean="sd" if self.show_mean else False,
            )

        self._apply_layout(fig, title=self.title, x_title="Series", y_title=self.y_title)
        return fig
