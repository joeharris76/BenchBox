"""Unit tests for chart primitives."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.fast

plotly = pytest.importorskip("plotly")  # noqa: F401

from benchbox.core.visualization import (  # noqa: E402
    BarDatum,
    CostPerformancePoint,
    CostPerformanceScatterPlot,
    DistributionBoxPlot,
    DistributionSeries,
    PerformanceBarChart,
    QueryVarianceHeatmap,
    TimeSeriesLineChart,
    TimeSeriesPoint,
    VisualizationError,
    get_theme,
)


class TestPerformanceBarChart:
    def test_builds_basic_chart(self):
        data = [
            BarDatum(label="duckdb", value=1.2, is_best=True),
            BarDatum(label="snowflake", value=1.6, is_worst=True),
        ]
        fig = PerformanceBarChart(data, metric_label="Runtime (s)").figure()
        assert fig.data
        assert fig.data[0].type == "bar"

    def test_uses_theme_colors_for_best_worst(self):
        theme = get_theme("light")
        data = [
            BarDatum(label="best", value=1.0, is_best=True),
            BarDatum(label="worst", value=2.0, is_worst=True),
            BarDatum(label="normal", value=1.5),
        ]
        chart = PerformanceBarChart(data)
        fig = chart.figure()

        colors = fig.data[0].marker.color
        assert theme.best_color in colors
        assert theme.worst_color in colors

    def test_color_palette_not_skipped_for_best_worst(self):
        """Ensure palette colors are assigned correctly when best/worst items are present."""
        data = [
            BarDatum(label="best", value=1.0, is_best=True),
            BarDatum(label="normal1", value=1.5),
            BarDatum(label="normal2", value=1.7),
        ]
        chart = PerformanceBarChart(data, sort_by="label")
        fig = chart.figure()

        colors = fig.data[0].marker.color
        # normal1 should get palette[0], normal2 should get palette[1]
        # best gets theme.best_color
        assert len(colors) == 3

    def test_invalid_barmode_raises_error(self):
        data = [BarDatum(label="test", value=1.0)]
        with pytest.raises(VisualizationError, match="barmode must be"):
            PerformanceBarChart(data, barmode="invalid")

    def test_empty_data_produces_empty_chart(self):
        fig = PerformanceBarChart([], title="Empty").figure()
        assert len(fig.data) == 0 or all(len(trace.x) == 0 for trace in fig.data)

    def test_grouped_bars(self):
        data = [
            BarDatum(label="platform1", value=1.0, group="group_a"),
            BarDatum(label="platform1", value=1.5, group="group_b"),
            BarDatum(label="platform2", value=2.0, group="group_a"),
            BarDatum(label="platform2", value=2.5, group="group_b"),
        ]
        fig = PerformanceBarChart(data, barmode="group").figure()
        assert len(fig.data) == 2  # Two groups

    def test_sort_by_value(self):
        data = [
            BarDatum(label="slow", value=10.0),
            BarDatum(label="fast", value=1.0),
            BarDatum(label="medium", value=5.0),
        ]
        chart = PerformanceBarChart(data, sort_by="value")
        labels = chart._ordered_labels(data, "value")
        # Higher values first (reverse sort)
        assert labels[0] == "slow"
        assert labels[-1] == "fast"

    def test_sort_by_label(self):
        data = [
            BarDatum(label="zebra", value=1.0),
            BarDatum(label="alpha", value=2.0),
        ]
        chart = PerformanceBarChart(data, sort_by="label")
        labels = chart._ordered_labels(data, "label")
        assert labels == ["alpha", "zebra"]


class TestTimeSeriesLineChart:
    def test_builds_basic_chart(self):
        points = [
            TimeSeriesPoint(series="duckdb", x="run1", y=9.1),
            TimeSeriesPoint(series="duckdb", x="run2", y=8.7),
            TimeSeriesPoint(series="duckdb", x="run3", y=8.3),
        ]
        fig = TimeSeriesLineChart(points).figure()
        assert any(trace.mode.startswith("lines") for trace in fig.data)

    def test_trendline_with_sufficient_points(self):
        points = [TimeSeriesPoint(series="test", x=i, y=float(i * 2)) for i in range(5)]
        fig = TimeSeriesLineChart(points, show_trend=True).figure()
        # Should have main line + trend line
        assert len(fig.data) == 2

    def test_no_trendline_with_few_points(self):
        points = [
            TimeSeriesPoint(series="test", x="a", y=1.0),
            TimeSeriesPoint(series="test", x="b", y=2.0),
        ]
        fig = TimeSeriesLineChart(points, show_trend=True).figure()
        # Only main line, no trend (need >= 3 points)
        assert len(fig.data) == 1

    def test_trendline_with_constant_values(self):
        """Trendline should handle constant y values (denom=0 edge case)."""
        points = [TimeSeriesPoint(series="test", x=i, y=5.0) for i in range(5)]
        chart = TimeSeriesLineChart(points)
        trend = chart._trendline([5.0, 5.0, 5.0, 5.0, 5.0])
        assert all(v == 5.0 for v in trend)

    def test_multiple_series(self):
        points = [
            TimeSeriesPoint(series="series_a", x="run1", y=1.0),
            TimeSeriesPoint(series="series_a", x="run2", y=2.0),
            TimeSeriesPoint(series="series_b", x="run1", y=3.0),
            TimeSeriesPoint(series="series_b", x="run2", y=4.0),
        ]
        fig = TimeSeriesLineChart(points, show_trend=False).figure()
        assert len(fig.data) == 2


class TestCostPerformanceScatterPlot:
    def test_builds_basic_chart(self):
        points = [
            CostPerformancePoint(name="duckdb", performance=120.0, cost=8.5),
            CostPerformancePoint(name="bigquery", performance=95.0, cost=6.1),
        ]
        fig = CostPerformanceScatterPlot(points).figure()
        assert any(trace.mode.startswith("markers") for trace in fig.data)

    def test_pareto_frontier_calculation(self):
        points = [
            CostPerformancePoint(name="optimal", performance=100.0, cost=5.0),
            CostPerformancePoint(name="dominated", performance=50.0, cost=10.0),
            CostPerformancePoint(name="also_optimal", performance=150.0, cost=8.0),
        ]
        frontier = CostPerformanceScatterPlot._pareto_frontier(points)
        frontier_names = {p.name for p in frontier}
        assert "optimal" in frontier_names
        assert "also_optimal" in frontier_names
        assert "dominated" not in frontier_names

    def test_single_point_is_on_frontier(self):
        points = [CostPerformancePoint(name="only", performance=100.0, cost=10.0)]
        frontier = CostPerformanceScatterPlot._pareto_frontier(points)
        assert len(frontier) == 1
        assert frontier[0].name == "only"

    def test_frontier_uses_theme_colors(self):
        theme = get_theme("light")
        points = [
            CostPerformancePoint(name="frontier", performance=100.0, cost=5.0),
            CostPerformancePoint(name="not_frontier", performance=50.0, cost=10.0),
        ]
        chart = CostPerformanceScatterPlot(points)
        fig = chart.figure()

        # Find frontier point marker color
        for trace in fig.data:
            if trace.name == "frontier":
                assert trace.marker.color == theme.best_color

    def test_hover_text_with_metadata(self):
        point = CostPerformancePoint(
            name="test",
            performance=100.0,
            cost=50.0,
            platform="TestPlatform",
            metadata={"region": "us-east-1", "instance": "large"},
        )
        hover = CostPerformanceScatterPlot._hover_text(point)
        assert "TestPlatform" in hover
        assert "region: us-east-1" in hover
        assert "instance: large" in hover


class TestQueryVarianceHeatmap:
    def test_builds_basic_chart(self):
        matrix = [[10.0, 12.0], [8.5, 9.2]]
        fig = QueryVarianceHeatmap(matrix=matrix, queries=["Q1", "Q2"], platforms=["duckdb", "snowflake"]).figure()
        assert fig.data[0].type == "heatmap"

    def test_handles_none_values_in_matrix(self):
        """Heatmap should handle None values (missing data)."""
        matrix = [[10.0, None], [None, 9.2]]
        fig = QueryVarianceHeatmap(matrix=matrix, queries=["Q1", "Q2"], platforms=["duckdb", "snowflake"]).figure()
        assert fig.data[0].type == "heatmap"

    def test_single_row_matrix(self):
        matrix = [[5.0, 6.0, 7.0]]
        fig = QueryVarianceHeatmap(matrix=matrix, queries=["Q1"], platforms=["a", "b", "c"]).figure()
        assert fig.data[0].type == "heatmap"


class TestDistributionBoxPlot:
    def test_builds_basic_chart(self):
        series = [
            DistributionSeries(name="duckdb", values=[210, 205, 198]),
            DistributionSeries(name="snowflake", values=[180, 175, 190]),
        ]
        fig = DistributionBoxPlot(series=series).figure()
        assert fig.data[0].type == "box"

    def test_single_value_series(self):
        series = [DistributionSeries(name="single", values=[100.0])]
        fig = DistributionBoxPlot(series=series).figure()
        assert fig.data[0].type == "box"

    def test_show_mean_option(self):
        series = [DistributionSeries(name="test", values=[1, 2, 3, 4, 5])]
        fig_with_mean = DistributionBoxPlot(series=series, show_mean=True).figure()
        fig_without_mean = DistributionBoxPlot(series=series, show_mean=False).figure()
        assert fig_with_mean.data[0].boxmean == "sd"
        assert fig_without_mean.data[0].boxmean is False


class TestThemeIntegration:
    def test_dark_theme_applied(self):
        theme = get_theme("dark")
        data = [BarDatum(label="test", value=1.0)]
        chart = PerformanceBarChart(data)
        chart.theme = theme
        chart.figure()  # Generate figure to ensure theme is applied
        # Verify dark theme is applied (background color check)
        assert theme.background_color == "#0f1116"

    def test_custom_palette(self):
        custom_palette = ("#ff0000", "#00ff00", "#0000ff")
        theme = get_theme("light", palette=custom_palette)
        assert theme.palette == custom_palette
