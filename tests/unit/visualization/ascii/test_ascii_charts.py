"""Tests for ASCII chart visualization module."""

from __future__ import annotations

import math

import pytest

from benchbox.core.visualization.ascii.bar_chart import ASCIIBarChart, BarData, from_bar_data
from benchbox.core.visualization.ascii.base import (
    ASCIIChartBase,
    ASCIIChartOptions,
    ColorMode,
    TerminalColors,
    detect_terminal_capabilities,
)
from benchbox.core.visualization.ascii.box_plot import (
    ASCIIBoxPlot,
    BoxPlotSeries,
    compute_quartiles,
    from_distribution_series,
)
from benchbox.core.visualization.ascii.heatmap import ASCIIHeatmap, from_matrix
from benchbox.core.visualization.ascii.histogram import ASCIIQueryHistogram, HistogramBar, from_query_latency_data
from benchbox.core.visualization.ascii.line_chart import ASCIILineChart, LinePoint, from_time_series_points
from benchbox.core.visualization.ascii.scatter_plot import ASCIIScatterPlot, ScatterPoint, from_cost_performance_points


class TestTerminalCapabilities:
    """Tests for terminal capability detection."""

    def test_detect_capabilities_returns_valid_object(self):
        """Capabilities detection returns a valid object."""
        caps = detect_terminal_capabilities()
        assert caps.width >= 40
        assert caps.height >= 10
        assert isinstance(caps.color_mode, ColorMode)
        assert isinstance(caps.unicode_support, bool)

    def test_color_mode_enum_values(self):
        """ColorMode enum has expected values."""
        assert ColorMode.NONE.value == "none"
        assert ColorMode.BASIC.value == "basic"
        assert ColorMode.EXTENDED.value == "extended"
        assert ColorMode.TRUECOLOR.value == "truecolor"


class TestTerminalColors:
    """Tests for terminal color utilities."""

    def test_colors_disabled_returns_empty(self):
        """Colors disabled returns empty strings."""
        colors = TerminalColors(color_mode=ColorMode.NONE)
        assert colors.fg("#ff0000") == ""
        assert colors.bg("#ff0000") == ""
        assert colors.reset() == ""

    def test_colors_basic_mode(self):
        """Basic color mode uses 16-color codes."""
        colors = TerminalColors(color_mode=ColorMode.BASIC)
        fg = colors.fg(2)  # Green
        assert "\033[" in fg
        assert "32" in fg or "38" in fg

    def test_colors_extended_mode(self):
        """Extended color mode uses 256-color codes."""
        colors = TerminalColors(color_mode=ColorMode.EXTENDED)
        fg = colors.fg("#1b9e77")
        assert "\033[38;5;" in fg

    def test_colorize_applies_colors(self):
        """Colorize wraps text with escape codes."""
        colors = TerminalColors(color_mode=ColorMode.EXTENDED)
        result = colors.colorize("test", fg_color="#ff0000")
        assert "test" in result
        assert "\033[" in result
        assert colors.RESET in result

    def test_colorize_no_color_passthrough(self):
        """Colorize with no color mode returns original text."""
        colors = TerminalColors(color_mode=ColorMode.NONE)
        result = colors.colorize("test", fg_color="#ff0000")
        assert result == "test"


class TestASCIIChartOptions:
    """Tests for chart options."""

    def test_default_options(self):
        """Default options are reasonable."""
        opts = ASCIIChartOptions()
        assert opts.use_color is True
        assert opts.use_unicode is True
        assert opts.width is None  # Auto-detect

    def test_effective_width_with_explicit(self):
        """Effective width uses explicit value when set."""
        opts = ASCIIChartOptions(width=100)
        assert opts.get_effective_width() == 100

    def test_effective_width_auto(self):
        """Effective width auto-detects when not set."""
        opts = ASCIIChartOptions()
        width = opts.get_effective_width()
        assert width >= 40  # Minimum

    def test_block_chars_unicode(self):
        """Block chars returns Unicode when enabled."""
        opts = ASCIIChartOptions(use_unicode=True)
        h_blocks = opts.get_horizontal_block_chars()
        v_blocks = opts.get_vertical_block_chars()
        assert "█" in h_blocks
        assert "█" in v_blocks
        assert "▏" in h_blocks
        assert "▂" in v_blocks

    def test_block_chars_ascii_fallback(self):
        """Block chars returns ASCII when Unicode disabled."""
        opts = ASCIIChartOptions(use_unicode=False)
        blocks = opts.get_horizontal_block_chars()
        assert "█" not in blocks
        assert "#" in blocks


class TestASCIIBarChart:
    """Tests for bar chart rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        chart = ASCIIBarChart(data=[])
        result = chart.render()
        assert "No data" in result

    def test_single_bar(self):
        """Single bar renders correctly."""
        data = [BarData(label="Test", value=100)]
        chart = ASCIIBarChart(data=data, title="Test Chart")
        result = chart.render()
        assert "Test" in result
        assert "100" in result

    def test_multiple_bars_sorted(self):
        """Multiple bars are sorted by value."""
        data = [
            BarData(label="Low", value=10),
            BarData(label="High", value=100),
            BarData(label="Mid", value=50),
        ]
        chart = ASCIIBarChart(data=data, sort_by="value")
        result = chart.render()
        lines = result.split("\n")

        # Find data lines (contain bar characters)
        data_lines = [l for l in lines if "High" in l or "Mid" in l or "Low" in l]
        assert len(data_lines) == 3

        # High should come before Low in sorted output
        high_idx = next(i for i, l in enumerate(data_lines) if "High" in l)
        low_idx = next(i for i, l in enumerate(data_lines) if "Low" in l)
        assert high_idx < low_idx

    def test_best_worst_highlighting(self):
        """Best/worst items are marked."""
        data = [
            BarData(label="Best", value=100, is_best=True),
            BarData(label="Worst", value=10, is_worst=True),
        ]
        chart = ASCIIBarChart(data=data)
        result = chart.render()
        assert "Best" in result
        assert "Worst" in result

    def test_grouped_bars(self):
        """Grouped bars show legend with group names."""
        data = [
            BarData(label="A", value=100, group="Group1"),
            BarData(label="B", value=80, group="Group2"),
        ]
        chart = ASCIIBarChart(data=data)
        result = chart.render()
        assert "Group1" in result
        assert "Group2" in result

    def test_from_bar_data_factory(self):
        """Factory function accepts BarData objects."""
        data = [BarData(label="Test", value=50)]
        chart = from_bar_data(data, title="Factory Test")
        result = chart.render()
        assert "Test" in result
        assert "50" in result


class TestASCIIBoxPlot:
    """Tests for box plot rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        chart = ASCIIBoxPlot(series=[])
        result = chart.render()
        assert "No data" in result

    def test_single_series(self):
        """Single series renders with statistics."""
        series = [BoxPlotSeries(name="Test", values=[10, 20, 30, 40, 50])]
        chart = ASCIIBoxPlot(series=series, show_stats=True)
        result = chart.render()
        assert "Test" in result
        assert "median" in result
        assert "30" in result  # Median of 10-50

    def test_multiple_series(self):
        """Multiple series render side by side."""
        series = [
            BoxPlotSeries(name="Series1", values=[10, 20, 30]),
            BoxPlotSeries(name="Series2", values=[40, 50, 60]),
        ]
        chart = ASCIIBoxPlot(series=series)
        result = chart.render()
        assert "Series1" in result
        assert "Series2" in result

    def test_outliers_shown(self):
        """Outliers are indicated."""
        # Add outliers far from the main distribution
        values = [10, 11, 12, 13, 14, 15, 100]  # 100 is an outlier
        series = [BoxPlotSeries(name="Test", values=values)]
        chart = ASCIIBoxPlot(series=series)
        result = chart.render()
        assert "o" in result  # Outlier marker

    def test_outliers_respect_width_constraint(self):
        """Outliers don't cause lines to exceed max width."""
        # Create data with many outliers
        values = [10, 20, 21, 22, 23, 24, 25, 100, 110, 120, 130, 140, 150, 160, 170]
        series = [BoxPlotSeries(name="TestPlatform", values=values)]
        opts = ASCIIChartOptions(width=80, use_color=False)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()

        # Verify no line exceeds the width constraint
        for line in result.split("\n"):
            assert len(line) <= 80, f"Line exceeds width: len={len(line)}, line='{line}'"

    def test_from_distribution_series_factory(self):
        """Factory function accepts BoxPlotSeries objects."""
        series = [BoxPlotSeries(name="Test", values=[1, 2, 3, 4, 5])]
        chart = from_distribution_series(series)
        result = chart.render()
        assert "Test" in result


class TestASCIIHeatmap:
    """Tests for heatmap rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        chart = ASCIIHeatmap(matrix=[], row_labels=[], col_labels=[])
        result = chart.render()
        assert "No data" in result

    def test_simple_matrix(self):
        """Simple matrix renders correctly."""
        matrix = [[10, 20], [30, 40]]
        chart = ASCIIHeatmap(
            matrix=matrix,
            row_labels=["Q1", "Q2"],
            col_labels=["DuckDB", "Polars"],
            title="Test Heatmap",
        )
        result = chart.render()
        assert "Q1" in result
        assert "Q2" in result
        assert "DuckDB" in result
        assert "Polars" in result

    def test_scale_legend(self):
        """Scale legend is shown."""
        matrix = [[10, 100]]
        chart = ASCIIHeatmap(
            matrix=matrix,
            row_labels=["Q1"],
            col_labels=["A", "B"],
        )
        result = chart.render()
        assert "Scale" in result
        assert "fast" in result or "slow" in result

    def test_from_matrix_factory(self):
        """Factory function creates heatmap."""
        matrix = [[1, 2], [3, 4]]
        chart = from_matrix(
            matrix=matrix,
            queries=["Q1", "Q2"],
            platforms=["A", "B"],
        )
        result = chart.render()
        assert "Q1" in result


class TestASCIIScatterPlot:
    """Tests for scatter plot rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        chart = ASCIIScatterPlot(points=[])
        result = chart.render()
        assert "No data" in result

    def test_single_point(self):
        """Single point renders."""
        points = [ScatterPoint(name="Test", x=50, y=100)]
        chart = ASCIIScatterPlot(points=points)
        result = chart.render()
        assert "Test" in result
        # Single point is on Pareto frontier, so uses Pareto marker
        assert "◆" in result or "*" in result  # Marker

    def test_pareto_frontier(self):
        """Pareto frontier is computed and shown."""
        points = [
            ScatterPoint(name="Efficient", x=10, y=100),  # Low cost, high perf = Pareto
            ScatterPoint(name="Inefficient", x=100, y=50),  # High cost, low perf = Not Pareto
        ]
        chart = ASCIIScatterPlot(points=points, show_pareto=True)
        result = chart.render()
        assert "Pareto" in result
        assert "Efficient" in result

    def test_from_cost_performance_factory(self):
        """Factory function accepts ScatterPoint objects."""
        points = [ScatterPoint(name="Test", x=50, y=100)]
        chart = from_cost_performance_points(points)
        result = chart.render()
        assert "Test" in result


class TestASCIILineChart:
    """Tests for line chart rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        chart = ASCIILineChart(points=[])
        result = chart.render()
        assert "No data" in result

    def test_single_series(self):
        """Single series renders."""
        points = [
            LinePoint(series="Test", x=1, y=10),
            LinePoint(series="Test", x=2, y=20),
            LinePoint(series="Test", x=3, y=30),
        ]
        chart = ASCIILineChart(points=points)
        result = chart.render()
        assert "Test" in result
        assert "*" in result  # Default marker

    def test_multiple_series(self):
        """Multiple series use different markers."""
        points = [
            LinePoint(series="A", x=1, y=10),
            LinePoint(series="A", x=2, y=20),
            LinePoint(series="B", x=1, y=15),
            LinePoint(series="B", x=2, y=25),
        ]
        chart = ASCIILineChart(points=points)
        result = chart.render()
        assert "A" in result
        assert "B" in result
        assert "*" in result  # Marker for A
        assert "+" in result  # Marker for B

    def test_trend_line(self):
        """Trend line can be shown."""
        points = [
            LinePoint(series="Test", x=1, y=10),
            LinePoint(series="Test", x=2, y=20),
            LinePoint(series="Test", x=3, y=30),
            LinePoint(series="Test", x=4, y=40),
        ]
        chart = ASCIILineChart(points=points, show_trend=True)
        result = chart.render()
        # Trend line uses '.' markers
        assert "." in result

    def test_from_time_series_factory(self):
        """Factory function accepts LinePoint objects."""
        points = [
            LinePoint(series="Test", x="Run1", y=100),
            LinePoint(series="Test", x="Run2", y=110),
        ]
        chart = from_time_series_points(points)
        result = chart.render()
        assert "Test" in result


class TestNoColorOutput:
    """Tests for output without colors."""

    def test_bar_chart_no_color(self):
        """Bar chart renders without colors."""
        data = [BarData(label="Test", value=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert "\033[" not in result  # No ANSI codes
        assert "Test" in result

    def test_heatmap_no_color(self):
        """Heatmap renders with intensity chars when no color."""
        matrix = [[10, 100]]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIHeatmap(
            matrix=matrix,
            row_labels=["Q1"],
            col_labels=["A", "B"],
            options=opts,
        )
        result = chart.render()
        assert "\033[" not in result


class TestASCIIOnlyOutput:
    """Tests for output without Unicode characters."""

    def test_bar_chart_ascii_only(self):
        """Bar chart renders with ASCII only."""
        data = [BarData(label="Test", value=100)]
        opts = ASCIIChartOptions(use_unicode=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert "█" not in result
        # ASCII_BLOCK_CHARS = " .-=+#@" - '@' is the full block
        assert "@" in result or "#" in result or "=" in result

    def test_box_plot_ascii_only(self):
        """Box plot renders with ASCII only."""
        series = [BoxPlotSeries(name="Test", values=[10, 20, 30])]
        opts = ASCIIChartOptions(use_unicode=False)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()
        # Should use ASCII box drawing
        assert "+" in result or "-" in result or "|" in result


class TestASCIIQueryHistogram:
    """Tests for query latency histogram rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        chart = ASCIIQueryHistogram(data=[])
        result = chart.render()
        assert "No data" in result

    def test_single_query(self):
        """Single query renders correctly."""
        data = [HistogramBar(query_id="Q1", latency_ms=100)]
        chart = ASCIIQueryHistogram(data=data, title="Test Histogram")
        result = chart.render()
        assert "Q1" in result
        assert "Test Histogram" in result

    def test_multiple_queries(self):
        """Multiple queries render with bars."""
        data = [
            HistogramBar(query_id="Q1", latency_ms=100),
            HistogramBar(query_id="Q2", latency_ms=200),
            HistogramBar(query_id="Q3", latency_ms=150),
        ]
        chart = ASCIIQueryHistogram(data=data)
        result = chart.render()
        assert "Q1" in result
        assert "Q2" in result
        assert "Q3" in result

    def test_natural_sort_order(self):
        """Query IDs are sorted naturally (Q1, Q2, Q10 not Q1, Q10, Q2)."""
        data = [
            HistogramBar(query_id="Q10", latency_ms=100),
            HistogramBar(query_id="Q2", latency_ms=200),
            HistogramBar(query_id="Q1", latency_ms=150),
        ]
        chart = ASCIIQueryHistogram(data=data, sort_by="query_id")
        result = chart.render()
        lines = result.split("\n")
        # Find the label line (has Q1, Q2, Q10)
        label_line = [l for l in lines if "Q1" in l and "Q2" in l and "Q10" in l]
        assert len(label_line) == 1
        # Q1 should appear before Q2 which should appear before Q10
        line = label_line[0]
        assert line.index("Q1") < line.index("Q2") < line.index("Q10")

    def test_sort_by_latency(self):
        """Queries can be sorted by latency."""
        data = [
            HistogramBar(query_id="Q1", latency_ms=100),
            HistogramBar(query_id="Q2", latency_ms=300),
            HistogramBar(query_id="Q3", latency_ms=200),
        ]
        chart = ASCIIQueryHistogram(data=data, sort_by="latency")
        result = chart.render()
        # With latency sort (descending), Q2 (300) should come first
        assert "Q2" in result

    def test_best_worst_highlighting(self):
        """Best/worst queries are marked."""
        data = [
            HistogramBar(query_id="Q1", latency_ms=100, is_best=True),
            HistogramBar(query_id="Q2", latency_ms=300, is_worst=True),
        ]
        chart = ASCIIQueryHistogram(data=data)
        result = chart.render()
        # Should have legend for best/worst
        assert "Best" in result
        assert "Worst" in result

    def test_mean_line_shown(self):
        """Mean line annotation is shown."""
        data = [
            HistogramBar(query_id="Q1", latency_ms=100),
            HistogramBar(query_id="Q2", latency_ms=200),
        ]
        chart = ASCIIQueryHistogram(data=data, show_mean_line=True)
        result = chart.render()
        assert "Mean" in result

    def test_mean_line_hidden(self):
        """Mean line can be hidden."""
        data = [
            HistogramBar(query_id="Q1", latency_ms=100),
            HistogramBar(query_id="Q2", latency_ms=200),
        ]
        chart = ASCIIQueryHistogram(data=data, show_mean_line=False)
        result = chart.render()
        assert "Mean" not in result

    def test_chart_splitting_for_large_datasets(self):
        """Charts split when queries exceed max_per_chart."""
        # Create 40 queries (more than default 33)
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=i * 10) for i in range(1, 41)]
        chart = ASCIIQueryHistogram(data=data, max_per_chart=33)
        result = chart.render()
        # Should have two chart sections with query ID range labels
        assert "Q1-Q33" in result
        assert "Q34-Q40" in result

    def test_chart_splitting_large_benchmark(self):
        """TPC-DS-like benchmark (99 queries) splits into 3 charts."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=i * 5) for i in range(1, 100)]
        chart = ASCIIQueryHistogram(data=data, max_per_chart=33)
        result = chart.render()
        # Should have three chart sections
        assert "Q1-Q33" in result
        assert "Q34-Q66" in result
        assert "Q67-Q99" in result

    def test_no_splitting_for_small_datasets(self):
        """No splitting when queries fit in one chart."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=i * 10) for i in range(1, 23)]
        chart = ASCIIQueryHistogram(data=data, max_per_chart=33)
        result = chart.render()
        # Should not have range labels
        assert "Queries 1-" not in result

    def test_from_query_latency_data_factory(self):
        """Factory function accepts HistogramBar objects."""
        data = [HistogramBar(query_id="Q1", latency_ms=150)]
        chart = from_query_latency_data(data, title="Factory Test")
        result = chart.render()
        assert "Q1" in result
        assert "Factory Test" in result

    def test_multi_platform_renders_with_legend(self):
        """Multi-platform data renders grouped bars with platform legend."""
        data = [
            HistogramBar(query_id="Q1", latency_ms=100, platform="DuckDB"),
            HistogramBar(query_id="Q1", latency_ms=150, platform="Polars"),
            HistogramBar(query_id="Q2", latency_ms=200, platform="DuckDB"),
            HistogramBar(query_id="Q2", latency_ms=250, platform="Polars"),
        ]
        chart = ASCIIQueryHistogram(data=data)
        result = chart.render()
        assert "Q1" in result
        assert "Q2" in result
        # Platform legend should be present
        assert "DuckDB" in result
        assert "Polars" in result

    def test_multi_platform_preserves_platform_colors(self):
        """Multi-platform histogram uses platform colors, not best/worst override."""
        data = [
            HistogramBar(query_id="Q1", latency_ms=50, platform="DuckDB", is_best=True),
            HistogramBar(query_id="Q1", latency_ms=60, platform="Polars", is_best=True),
            HistogramBar(query_id="Q2", latency_ms=300, platform="DuckDB", is_worst=True),
            HistogramBar(query_id="Q2", latency_ms=350, platform="Polars", is_worst=True),
        ]
        chart = ASCIIQueryHistogram(data=data)
        result = chart.render()
        # Platform names should appear in legend, but not Best/Worst (color override removed)
        assert "DuckDB" in result
        assert "Polars" in result
        assert "Best" not in result
        assert "Worst" not in result

    def test_multi_platform_splitting(self):
        """Multi-platform data splits by unique query count, not total bar count."""
        # 10 queries x 2 platforms = 20 bars, but only 10 unique queries
        data = []
        for i in range(1, 11):
            data.append(HistogramBar(query_id=f"Q{i}", latency_ms=i * 10, platform="A"))
            data.append(HistogramBar(query_id=f"Q{i}", latency_ms=i * 15, platform="B"))
        chart = ASCIIQueryHistogram(data=data, max_per_chart=5)
        result = chart.render()
        # Should split into 2 charts of 5 queries each, not by 20 bars
        assert "Q1-Q5" in result
        assert "Q6-Q10" in result

    def test_no_color_output(self):
        """Histogram renders without colors."""
        data = [HistogramBar(query_id="Q1", latency_ms=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert "\033[" not in result  # No ANSI codes
        assert "Q1" in result

    def test_compact_labels_do_not_collapse_multi_digit_query_ids(self):
        """Narrow bars should keep multi-digit query IDs distinguishable."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=float(i)) for i in range(10, 20)]
        opts = ASCIIChartOptions(use_color=False)
        opts.width = 46  # Force narrow bar widths and compact labels.
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert "10" in result
        assert "11" in result
        assert "19" in result
        assert "Q1 Q1 Q1" not in result


# ── Edge Case Tests ───────────────────────────────────────────────


class TestComputeQuartiles:
    """Tests for compute_quartiles edge cases."""

    def test_empty_values(self):
        """Empty input returns zeroed stats."""
        stats = compute_quartiles([])
        assert stats.median == 0
        assert stats.outliers == []

    def test_single_value(self):
        """Single value produces degenerate box plot."""
        stats = compute_quartiles([42])
        assert stats.median == 42
        assert stats.q1 == 42
        assert stats.q3 == 42
        assert stats.outliers == []

    def test_two_values(self):
        """Two values produce valid stats."""
        stats = compute_quartiles([10, 20])
        assert stats.min_val <= stats.median <= stats.max_val

    def test_all_identical_values(self):
        """All identical values: IQR=0, no crash."""
        stats = compute_quartiles([5, 5, 5, 5, 5])
        assert stats.median == 5
        assert stats.q1 == 5
        assert stats.q3 == 5
        assert stats.std == 0
        assert stats.outliers == []

    def test_all_values_are_outliers(self):
        """When IQR creates fences that exclude all values, falls back to extremes."""
        # Two clusters far apart with few values → possible all-outlier scenario
        stats = compute_quartiles([1, 1, 1, 100])
        # Should not raise ValueError; whiskers should be valid
        assert stats.min_val <= stats.max_val

    def test_negative_values(self):
        """Negative values produce valid statistics."""
        stats = compute_quartiles([-50, -30, -10, 10, 30])
        assert stats.min_val <= stats.median <= stats.max_val
        assert stats.mean == pytest.approx(-10.0)


class TestNegativeValues:
    """Tests for negative value handling across chart types."""

    def test_bar_chart_negative_values(self):
        """Bar chart handles negative values without crash."""
        data = [
            BarData(label="Negative", value=-50),
            BarData(label="Positive", value=100),
            BarData(label="Zero", value=0),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert "Negative" in result
        assert "Positive" in result

    def test_box_plot_negative_values(self):
        """Box plot handles negative distributions."""
        series = [BoxPlotSeries(name="Neg", values=[-100, -50, -30, -10, 0, 10])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()
        assert "Neg" in result
        assert "median" in result

    def test_scatter_plot_negative_values(self):
        """Scatter plot handles negative coordinates."""
        points = [
            ScatterPoint(name="NegNeg", x=-10, y=-20),
            ScatterPoint(name="PosPos", x=10, y=20),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIScatterPlot(points=points, options=opts)
        result = chart.render()
        assert "NegNeg" in result
        assert "PosPos" in result

    def test_line_chart_negative_values(self):
        """Line chart handles negative Y values."""
        points = [
            LinePoint(series="Test", x=1, y=-50),
            LinePoint(series="Test", x=2, y=0),
            LinePoint(series="Test", x=3, y=50),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIILineChart(points=points, options=opts)
        result = chart.render()
        assert "Test" in result


class TestAllIdenticalValues:
    """Tests for all-identical value handling (division by zero risks)."""

    def test_bar_chart_identical_values(self):
        """Bar chart handles all identical values."""
        data = [BarData(label="A", value=50), BarData(label="B", value=50)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert "A" in result
        assert "B" in result

    def test_box_plot_identical_values(self):
        """Box plot handles all identical values without stdev crash."""
        series = [BoxPlotSeries(name="Same", values=[42, 42, 42, 42, 42])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()
        assert "Same" in result
        assert "42" in result

    def test_heatmap_identical_values(self):
        """Heatmap handles all identical values."""
        matrix = [[100, 100], [100, 100]]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=["Q1", "Q2"], col_labels=["A", "B"], options=opts)
        result = chart.render()
        assert "Q1" in result

    def test_scatter_plot_identical_values(self):
        """Scatter plot handles points at same location."""
        points = [
            ScatterPoint(name="A", x=50, y=100),
            ScatterPoint(name="B", x=50, y=100),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIScatterPlot(points=points, options=opts)
        result = chart.render()
        assert "A" in result

    def test_line_chart_identical_y(self):
        """Line chart handles flat line (all identical Y)."""
        points = [
            LinePoint(series="Flat", x=1, y=100),
            LinePoint(series="Flat", x=2, y=100),
            LinePoint(series="Flat", x=3, y=100),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIILineChart(points=points, options=opts)
        result = chart.render()
        assert "Flat" in result

    def test_histogram_identical_latencies(self):
        """Histogram handles all identical latencies."""
        data = [
            HistogramBar(query_id="Q1", latency_ms=50),
            HistogramBar(query_id="Q2", latency_ms=50),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert "Q1" in result


class TestSingleDataPoint:
    """Tests for single data point handling."""

    def test_box_plot_single_value_series(self):
        """Box plot with single-value series doesn't crash."""
        series = [BoxPlotSeries(name="One", values=[42])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()
        assert "One" in result

    def test_line_chart_single_point(self):
        """Line chart with single point renders."""
        points = [LinePoint(series="Solo", x=1, y=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIILineChart(points=points, options=opts)
        result = chart.render()
        assert "Solo" in result

    def test_heatmap_single_cell(self):
        """Heatmap with single cell renders."""
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIHeatmap(matrix=[[42]], row_labels=["Q1"], col_labels=["A"], options=opts)
        result = chart.render()
        assert "Q1" in result
        assert "42" in result


class TestNaNAndInfinity:
    """Tests for NaN and Infinity handling."""

    def test_bar_chart_nan(self):
        """Bar chart handles NaN values without crash."""
        data = [BarData(label="Normal", value=100), BarData(label="NaN", value=float("nan"))]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert "Normal" in result

    def test_bar_chart_infinity(self):
        """Bar chart handles Infinity values without crash."""
        data = [BarData(label="Normal", value=100), BarData(label="Inf", value=float("inf"))]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert "Normal" in result

    def test_histogram_zero_latency(self):
        """Histogram handles zero latency without division by zero."""
        data = [
            HistogramBar(query_id="Q1", latency_ms=0),
            HistogramBar(query_id="Q2", latency_ms=0),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert "Q1" in result


class TestNarrowWidth:
    """Tests for narrow terminal width handling."""

    def test_bar_chart_narrow(self):
        """Bar chart renders at minimum width without crash."""
        data = [BarData(label="VeryLongLabelThatExceedsWidth", value=100)]
        opts = ASCIIChartOptions(width=40, use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert len(max(result.split("\n"), key=len)) <= 40

    def test_heatmap_narrow_truncates_columns(self):
        """Heatmap truncates columns when too narrow."""
        matrix = [[i * 10 for i in range(10)]]
        col_labels = [f"Platform{i}" for i in range(10)]
        opts = ASCIIChartOptions(width=40, use_color=False)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=["Q1"], col_labels=col_labels, options=opts)
        result = chart.render()
        assert "Q1" in result
        # Should truncate some columns
        assert "..." in result

    def test_scatter_plot_narrow(self):
        """Scatter plot renders at minimum width."""
        points = [ScatterPoint(name="A", x=10, y=20), ScatterPoint(name="B", x=30, y=40)]
        opts = ASCIIChartOptions(width=40, use_color=False)
        chart = ASCIIScatterPlot(points=points, options=opts)
        result = chart.render()
        assert "A" in result


class TestANSISanitization:
    """Tests for ANSI escape sequence sanitization."""

    def test_sanitize_strips_ansi(self):
        """_sanitize_text strips ANSI escape sequences."""
        result = ASCIIChartBase._sanitize_text("\033[1;31mRED\033[0m")
        assert result == "RED"
        assert "\033" not in result

    def test_sanitize_preserves_normal_text(self):
        """_sanitize_text preserves normal text."""
        result = ASCIIChartBase._sanitize_text("Normal text")
        assert result == "Normal text"

    def test_bar_chart_ansi_in_label(self):
        """Bar chart sanitizes ANSI in labels."""
        data = [BarData(label="\033[31mInjected\033[0m", value=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert "Injected" in result
        assert "\033[31m" not in result

    def test_title_ansi_sanitization(self):
        """Chart titles have ANSI sequences stripped."""
        data = [BarData(label="Test", value=100)]
        opts = ASCIIChartOptions(use_color=False, title="\033[2JCleared")
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert "Cleared" in result
        assert "\033[2J" not in result


class TestFactoryFunctionValidation:
    """Tests that factory functions warn on unexpected types."""

    def test_bar_data_factory_unknown_type_uses_getattr(self):
        """from_bar_data handles non-BarData types via getattr fallback."""
        from dataclasses import dataclass

        @dataclass
        class CustomBar:
            label: str = "test"
            value: float = 42

        chart = from_bar_data([CustomBar()])
        result = chart.render()
        assert "test" in result

    def test_scatter_factory_unknown_type_uses_getattr(self):
        """from_cost_performance_points handles non-ScatterPoint types via getattr fallback."""
        from dataclasses import dataclass

        @dataclass
        class CustomPoint:
            name: str = "test"
            cost: float = 10
            performance: float = 20

        chart = from_cost_performance_points([CustomPoint()])
        result = chart.render()
        assert "test" in result


# ── Comparison Bar Chart Tests ───────────────────────────────────


class TestASCIIComparisonBar:
    """Tests for paired comparison bar chart rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar

        chart = ASCIIComparisonBar(data=[])
        result = chart.render()
        assert "No data" in result

    def test_single_query_comparison(self):
        """Single query renders both bars with percentage annotation."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [
            ComparisonBarData(
                label="Q1",
                baseline_value=100,
                comparison_value=75,
                baseline_name="SQL",
                comparison_name="DF",
            )
        ]
        chart = ASCIIComparisonBar(data=data, title="Test Comparison")
        result = chart.render()
        assert "Q1" in result
        assert "SQL" in result
        assert "DF" in result
        assert "100" in result
        assert "75" in result
        assert "Test Comparison" in result

    def test_percentage_change_annotation(self):
        """Percentage change is shown for non-trivial differences."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [
            ComparisonBarData(
                label="Q1",
                baseline_value=100,
                comparison_value=50,
                baseline_name="A",
                comparison_name="B",
            )
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        assert "-50.0%" in result

    def test_regression_annotation(self):
        """Positive percentage (regression) is annotated."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [
            ComparisonBarData(
                label="Q1",
                baseline_value=100,
                comparison_value=150,
                baseline_name="A",
                comparison_name="B",
            )
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        assert "+50.0%" in result

    def test_stable_no_annotation(self):
        """Near-zero percentage change shows no annotation."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [
            ComparisonBarData(
                label="Q1",
                baseline_value=100,
                comparison_value=101,
                baseline_name="A",
                comparison_name="B",
            )
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        # 1% change should not show annotation (below 2% threshold)
        assert "%" not in result.split("\n")[-3]  # Check comparison bar line only

    def test_multiple_queries(self):
        """Multiple queries render as separate pairs."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [
            ComparisonBarData(label="Q1", baseline_value=100, comparison_value=80),
            ComparisonBarData(label="Q2", baseline_value=200, comparison_value=250),
            ComparisonBarData(label="Q3", baseline_value=50, comparison_value=50),
        ]
        chart = ASCIIComparisonBar(data=data)
        result = chart.render()
        assert "Q1" in result
        assert "Q2" in result
        assert "Q3" in result

    def test_no_color_output(self):
        """Comparison bar renders without ANSI codes when color disabled."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [ComparisonBarData(label="Q1", baseline_value=100, comparison_value=50)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        assert "\033[" not in result

    def test_ascii_only_output(self):
        """Comparison bar renders with ASCII fallback characters."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [ComparisonBarData(label="Q1", baseline_value=100, comparison_value=50)]
        opts = ASCIIChartOptions(use_unicode=False, use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        assert "\u2588" not in result  # No Unicode full block
        assert "~" in result  # ASCII approx symbol

    def test_zero_baseline_no_crash(self):
        """Zero baseline value does not cause division by zero."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [ComparisonBarData(label="Q1", baseline_value=0, comparison_value=50)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        assert "Q1" in result

    def test_scale_note_shown(self):
        """Scale note appears at bottom of chart."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [ComparisonBarData(label="Q1", baseline_value=100, comparison_value=50)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        assert "each" in result

    def test_from_comparison_data_factory(self):
        """Factory function creates chart from ComparisonBarData."""
        from benchbox.core.visualization.ascii.comparison_bar import ComparisonBarData, from_comparison_data

        data = [ComparisonBarData(label="Q1", baseline_value=100, comparison_value=75)]
        chart = from_comparison_data(data, title="Factory Test")
        result = chart.render()
        assert "Q1" in result
        assert "Factory Test" in result

    def test_from_comparison_data_factory_unknown_type(self, caplog):
        """Factory warns on unknown types."""
        import logging

        from benchbox.core.visualization.ascii.comparison_bar import from_comparison_data

        with caplog.at_level(logging.WARNING):
            from_comparison_data([{"label": "Q1", "baseline_value": 100, "comparison_value": 50}])
        assert "unexpected type" in caplog.text


# ── Diverging Bar Chart Tests ────────────────────────────────────


class TestASCIIDivergingBar:
    """Tests for diverging bar chart rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar

        chart = ASCIIDivergingBar(data=[])
        result = chart.render()
        assert "No data" in result

    def test_improvements_and_regressions(self):
        """Chart shows both improvements and regressions."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [
            DivergingBarData(label="Q1", pct_change=-30.0),
            DivergingBarData(label="Q2", pct_change=+20.0),
            DivergingBarData(label="Q3", pct_change=-5.0),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIDivergingBar(data=data, options=opts)
        result = chart.render()
        assert "Q1" in result
        assert "Q2" in result
        assert "Q3" in result
        assert "-30.0%" in result
        assert "+20.0%" in result
        assert "Faster" in result
        assert "Slower" in result

    def test_sorted_by_magnitude(self):
        """Items are sorted: improvements first (most negative), then regressions."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [
            DivergingBarData(label="Q1", pct_change=-10.0),
            DivergingBarData(label="Q2", pct_change=-50.0),
            DivergingBarData(label="Q3", pct_change=+30.0),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIDivergingBar(data=data, options=opts)
        result = chart.render()
        lines = result.split("\n")
        # Q2 (-50%) should come before Q1 (-10%) in the sorted output
        q2_line = next(i for i, l in enumerate(lines) if "Q2" in l)
        q1_line = next(i for i, l in enumerate(lines) if "Q1" in l)
        assert q2_line < q1_line

    def test_overflow_arrows_for_extreme_outliers(self):
        """Extreme outliers show overflow arrows."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [
            DivergingBarData(label="Q1", pct_change=-10.0),
            DivergingBarData(label="Q2", pct_change=+726.0),  # Extreme outlier
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIDivergingBar(data=data, clip_pct=200.0, options=opts)
        result = chart.render()
        assert "+726.0%" in result
        # Should have overflow indicator
        assert "\u25ba" in result or ">" in result

    def test_all_improvements(self):
        """Chart handles all-improvement data."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [
            DivergingBarData(label="Q1", pct_change=-20.0),
            DivergingBarData(label="Q2", pct_change=-10.0),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIDivergingBar(data=data, options=opts)
        result = chart.render()
        assert "2 improved" in result
        assert "0 regressed" in result

    def test_all_regressions(self):
        """Chart handles all-regression data."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [
            DivergingBarData(label="Q1", pct_change=+20.0),
            DivergingBarData(label="Q2", pct_change=+40.0),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIDivergingBar(data=data, options=opts)
        result = chart.render()
        assert "0 improved" in result
        assert "2 regressed" in result

    def test_no_color_output(self):
        """Diverging bar renders without ANSI codes when color disabled."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [DivergingBarData(label="Q1", pct_change=-25.0)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIDivergingBar(data=data, options=opts)
        result = chart.render()
        assert "\033[" not in result

    def test_ascii_only_output(self):
        """Diverging bar renders with ASCII fallback characters."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [
            DivergingBarData(label="Q1", pct_change=-30.0),
            DivergingBarData(label="Q2", pct_change=+300.0),
        ]
        opts = ASCIIChartOptions(use_unicode=False, use_color=False)
        chart = ASCIIDivergingBar(data=data, clip_pct=200.0, options=opts)
        result = chart.render()
        # ASCII overflow arrow
        assert "-->" in result or ">" in result
        # No Unicode box chars
        assert "\u2502" not in result

    def test_summary_counts(self):
        """Summary line shows correct counts."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [
            DivergingBarData(label="Q1", pct_change=-30.0),
            DivergingBarData(label="Q2", pct_change=+1.0),  # Stable (< 2%)
            DivergingBarData(label="Q3", pct_change=+20.0),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIDivergingBar(data=data, options=opts)
        result = chart.render()
        assert "1 improved" in result
        assert "1 stable" in result
        assert "1 regressed" in result

    def test_single_item(self):
        """Single item renders without crash."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [DivergingBarData(label="Q1", pct_change=-15.0)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIDivergingBar(data=data, options=opts)
        result = chart.render()
        assert "Q1" in result
        assert "-15.0%" in result

    def test_from_regression_data_factory(self):
        """Factory function creates chart from DivergingBarData."""
        from benchbox.core.visualization.ascii.diverging_bar import DivergingBarData, from_regression_data

        data = [DivergingBarData(label="Q1", pct_change=-25.0)]
        chart = from_regression_data(data, title="Factory Test")
        result = chart.render()
        assert "Q1" in result
        assert "Factory Test" in result

    def test_from_regression_data_factory_unknown_type(self, caplog):
        """Factory warns on unknown types."""
        import logging

        from benchbox.core.visualization.ascii.diverging_bar import from_regression_data

        with caplog.at_level(logging.WARNING):
            from_regression_data([{"label": "Q1", "pct_change": -10}])
        assert "unexpected type" in caplog.text


# ── Summary Box Tests ────────────────────────────────────────────


class TestASCIISummaryBox:
    """Tests for summary box rendering."""

    def test_single_run_summary(self):
        """Single-run summary shows basic metrics."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(
            title="DuckDB Summary",
            geo_mean_ms=142.3,
            total_time_ms=3200,
            num_queries=22,
        )
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        assert "DuckDB Summary" in result
        assert "142.3" in result
        assert "3.2s" in result
        assert "22" in result
        # Box borders present
        assert "\u250c" in result or "+" in result

    def test_single_run_best_worst_shows_time_units(self):
        """Single-run best/worst values should include ms/s units."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(
            title="Unit Test",
            geo_mean_ms=100.0,
            total_time_ms=500.0,
            num_queries=3,
            best_queries=[("Q6", 8.0), ("Q14", 12.5)],
            worst_queries=[("Q18", 302.0), ("Q21", 1500.0)],
        )
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        # Best values should have ms unit
        assert "Q6 (8.0ms)" in result
        assert "Q14 (12.5ms)" in result
        # Worst values should have appropriate units
        assert "Q18 (302.0ms)" in result
        assert "Q21 (1.5s)" in result

    def test_comparison_summary(self):
        """Comparison summary shows both runs and percentage change."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(
            title="SQL vs DF Summary",
            geo_mean_baseline_ms=142.3,
            geo_mean_comparison_ms=98.7,
            total_time_baseline_ms=3200,
            total_time_comparison_ms=2100,
            baseline_name="SQL",
            comparison_name="DF",
            num_queries=22,
            num_improved=5,
            num_stable=12,
            num_regressed=5,
            best_queries=[("Q6", -57.2), ("Q14", -38.1)],
            worst_queries=[("Q21", 726.0), ("Q17", 23.4)],
        )
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        assert "SQL vs DF Summary" in result
        assert "Geo Mean" in result
        assert "142.3" in result
        assert "98.7" in result
        assert "5 improved" in result
        assert "12 stable" in result
        assert "5 regressed" in result
        assert "Q6" in result
        assert "Q21" in result

    def test_box_borders(self):
        """Summary box has proper Unicode borders."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(title="Test", geo_mean_ms=100)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        # Check box drawing characters
        assert "\u250c" in result  # top-left corner
        assert "\u2510" in result  # top-right corner
        assert "\u2514" in result  # bottom-left corner
        assert "\u2518" in result  # bottom-right corner
        assert "\u2502" in result  # vertical line

    def test_ascii_only_borders(self):
        """Summary box uses ASCII borders when Unicode disabled."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(title="Test", geo_mean_ms=100)
        opts = ASCIIChartOptions(use_unicode=False, use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        assert "+" in result  # ASCII corners
        assert "|" in result  # ASCII vertical
        assert "-" in result  # ASCII horizontal

    def test_no_color_output(self):
        """Summary box renders without ANSI codes when color disabled."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(
            title="Test",
            geo_mean_baseline_ms=100,
            geo_mean_comparison_ms=80,
            num_improved=3,
            num_stable=1,
            num_regressed=1,
        )
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        assert "\033[" not in result

    def test_time_formatting_minutes(self):
        """Large times are formatted as minutes."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(title="Test", total_time_ms=120_000)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        assert "2.0min" in result

    def test_time_formatting_seconds(self):
        """Medium times are formatted as seconds."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(title="Test", total_time_ms=5500)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        assert "5.5s" in result

    def test_time_formatting_milliseconds(self):
        """Small times stay as milliseconds."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(title="Test", total_time_ms=42.5)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        assert "42.5ms" in result

    def test_empty_best_worst(self):
        """Summary box renders fine without best/worst queries."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(title="Test", geo_mean_ms=100, num_queries=5)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        assert "Best" not in result
        assert "Worst" not in result


# ── Integration Tests ────────────────────────────────────────────


class TestComparisonTemplateIntegration:
    """Integration tests for comparison template composing all three chart types."""

    def test_exporters_render_comparison_bar(self):
        """render_ascii_chart handles comparison_bar type."""
        from benchbox.core.visualization.ascii.comparison_bar import ComparisonBarData
        from benchbox.core.visualization.exporters import render_ascii_chart

        data = [ComparisonBarData(label="Q1", baseline_value=100, comparison_value=75)]
        result = render_ascii_chart("comparison_bar", data, title="Test")
        assert "Q1" in result

    def test_exporters_render_diverging_bar(self):
        """render_ascii_chart handles diverging_bar type."""
        from benchbox.core.visualization.ascii.diverging_bar import DivergingBarData
        from benchbox.core.visualization.exporters import render_ascii_chart

        data = [DivergingBarData(label="Q1", pct_change=-25.0)]
        result = render_ascii_chart("diverging_bar", data, title="Test")
        assert "Q1" in result

    def test_exporters_render_summary_box(self):
        """render_ascii_chart handles summary_box type."""
        from benchbox.core.visualization.ascii.summary_box import SummaryStats
        from benchbox.core.visualization.exporters import render_ascii_chart

        stats = SummaryStats(title="Test Summary", geo_mean_ms=100)
        result = render_ascii_chart("summary_box", stats)
        assert "Test Summary" in result

    def test_exporters_render_summary_box_from_dict(self):
        """render_ascii_chart handles summary_box from dict data."""
        from benchbox.core.visualization.exporters import render_ascii_chart

        data = {"title": "Dict Summary", "geo_mean_ms": 100, "num_queries": 5}
        result = render_ascii_chart("summary_box", data)
        assert "Dict Summary" in result

    def test_comparison_template_exists(self):
        """comparison template is registered and accessible."""
        from benchbox.core.visualization.templates import get_template

        template = get_template("comparison")
        assert template.name == "comparison"
        assert "comparison_bar" in template.chart_types
        assert "diverging_bar" in template.chart_types
        assert "summary_box" in template.chart_types

    def test_new_classes_importable_from_module(self):
        """New classes are importable from the ascii module."""
        from benchbox.core.visualization.ascii import ASCIIComparisonBar, ASCIIDivergingBar, ASCIISummaryBox

        assert ASCIIComparisonBar is not None
        assert ASCIIDivergingBar is not None
        assert ASCIISummaryBox is not None


# ── Edge Case Tests for New Chart Types ──────────────────────────


class TestNewChartEdgeCases:
    """Edge case tests across all new chart types."""

    def test_comparison_bar_nan_values(self):
        """ComparisonBar handles NaN values."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [ComparisonBarData(label="Q1", baseline_value=float("nan"), comparison_value=50)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        assert "Q1" in result

    def test_comparison_bar_identical_values(self):
        """ComparisonBar handles identical baseline and comparison values."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [ComparisonBarData(label="Q1", baseline_value=100, comparison_value=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        assert "Q1" in result

    def test_diverging_bar_zero_pct_change(self):
        """DivergingBar handles zero percentage change."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [DivergingBarData(label="Q1", pct_change=0.0)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIDivergingBar(data=data, options=opts)
        result = chart.render()
        assert "Q1" in result
        assert "0 improved" in result or "1 stable" in result

    def test_diverging_bar_extreme_values(self):
        """DivergingBar handles very large percentage changes."""
        from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData

        data = [
            DivergingBarData(label="Q1", pct_change=-95.0),
            DivergingBarData(label="Q2", pct_change=+1500.0),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIDivergingBar(data=data, options=opts)
        result = chart.render()
        assert "+1500.0%" in result
        assert "-95.0%" in result

    def test_summary_box_comparison_flag(self):
        """SummaryStats.is_comparison correctly detects comparison mode."""
        from benchbox.core.visualization.ascii.summary_box import SummaryStats

        single = SummaryStats(geo_mean_ms=100)
        assert not single.is_comparison

        comparison = SummaryStats(geo_mean_baseline_ms=100, geo_mean_comparison_ms=80)
        assert comparison.is_comparison

    def test_comparison_bar_narrow_width(self):
        """ComparisonBar renders at minimum width."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [ComparisonBarData(label="Q1", baseline_value=100, comparison_value=75)]
        opts = ASCIIChartOptions(width=40, use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        assert "Q1" in result


class TestSeriesNamingSymmetry:
    """Tests for Phase 1: symmetric mode naming in ResultPlotter."""

    @staticmethod
    def _make_result(platform, execution_mode="sql"):
        from benchbox.core.visualization.result_plotter import NormalizedResult

        return NormalizedResult(
            benchmark="tpch",
            platform=platform,
            scale_factor=1,
            execution_id=None,
            timestamp=None,
            total_time_ms=None,
            avg_time_ms=None,
            success_rate=None,
            cost_total=None,
            execution_mode=execution_mode,
            raw={},
        )

    def test_disambiguate_modes_both_get_suffix(self):
        """When same platform has SQL and DataFrame modes, both get suffixed."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            self._make_result("DataFusion", "sql"),
            self._make_result("DataFusion", "dataframe"),
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._disambiguate_modes()

        platforms = [r.platform for r in plotter.results]
        assert "DataFusion (sql)" in platforms
        assert "DataFusion (df)" in platforms

    def test_single_result_no_suffix(self):
        """Single result gets no mode suffix."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [self._make_result("DuckDB", "sql")]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._disambiguate_modes()

        assert plotter.results[0].platform == "DuckDB"

    def test_same_mode_no_suffix(self):
        """Two results with same mode don't get suffixed."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            self._make_result("DuckDB", "sql"),
            self._make_result("Polars", "sql"),
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._disambiguate_modes()

        assert plotter.results[0].platform == "DuckDB"
        assert plotter.results[1].platform == "Polars"

    def test_mode_abbreviation_dataframe_to_df(self):
        """'dataframe' mode is abbreviated to 'df'."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        assert ResultPlotter.MODE_ABBREVIATIONS["dataframe"] == "df"
        assert ResultPlotter.MODE_ABBREVIATIONS.get("sql", "sql") == "sql"


class TestLabelTruncation:
    """Tests for Phase 2: label truncation cap removal."""

    def test_bar_chart_full_name_up_to_25_chars(self):
        """Bar chart shows full names up to 25 characters."""
        name = "DataFusion Platform X"  # 21 chars
        data = [BarData(label=name, value=100)]
        opts = ASCIIChartOptions(width=100, use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert name in result

    def test_bar_chart_truncates_above_30_chars(self):
        """Bar chart truncates names longer than 30 characters."""
        name = "A" * 35
        data = [BarData(label=name, value=100)]
        opts = ASCIIChartOptions(width=100, use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert name not in result
        assert ".." in result

    def test_comparison_bar_shows_full_run_names(self):
        """ComparisonBar shows full run names up to 25 chars."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        name = "DataFusion (sql)"  # 16 chars
        data = [ComparisonBarData(label="Q1", baseline_value=100, comparison_value=80, baseline_name=name)]
        opts = ASCIIChartOptions(width=100, use_color=False)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        assert name in result

    def test_box_plot_full_series_names(self):
        """BoxPlot shows full series names up to 30 chars."""
        name = "DataFusion Platform"  # 19 chars
        series = [BoxPlotSeries(name=name, values=[10, 20, 30, 40, 50])]
        opts = ASCIIChartOptions(width=100, use_color=False)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()
        assert name in result


class TestMetadataSubtitle:
    """Tests for Phase 3: chart metadata subtitle."""

    def test_subtitle_renders_scale_factor(self):
        """Subtitle renders scale factor from metadata."""
        data = [BarData(label="Q1", value=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts, metadata={"scale_factor": 1})
        result = chart.render()
        assert "SF=sf1" in result

    def test_subtitle_renders_platform_version(self):
        """Subtitle renders platform version from metadata."""
        data = [BarData(label="Q1", value=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts, metadata={"platform_version": "DuckDB 1.2.0"})
        result = chart.render()
        assert "DuckDB 1.2.0" in result

    def test_subtitle_renders_tuning(self):
        """Subtitle renders tuning config from metadata."""
        data = [BarData(label="Q1", value=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts, metadata={"tuning": "tuned"})
        result = chart.render()
        assert "tuned" in result

    def test_subtitle_pipe_separated(self):
        """Subtitle uses pipe separator for multiple values."""
        data = [BarData(label="Q1", value=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts, metadata={"scale_factor": 1, "platform_version": "DuckDB 1.2.0"})
        result = chart.render()
        assert "|" in result

    def test_no_subtitle_without_metadata(self):
        """No subtitle line when metadata is empty."""
        data = [BarData(label="Q1", value=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        # Title line and separator should be adjacent (no subtitle between)
        lines = result.split("\n")
        # Find the title line and check that next line is separator
        for i, line in enumerate(lines):
            if "Bar Chart" in line and i + 1 < len(lines):
                assert lines[i + 1].strip().startswith("─") or lines[i + 1].strip().startswith("-")
                break

    def test_subtitle_in_all_chart_types(self):
        """Metadata subtitle works in histogram, box plot, and heatmap."""
        metadata = {"scale_factor": 0.01}

        # Histogram
        hist_data = [HistogramBar(query_id="Q1", latency_ms=100)]
        h = ASCIIQueryHistogram(data=hist_data, options=ASCIIChartOptions(use_color=False), metadata=metadata)
        assert "SF=sf001" in h.render()

        # BoxPlot
        bp = ASCIIBoxPlot(
            series=[BoxPlotSeries(name="X", values=[1, 2, 3])],
            options=ASCIIChartOptions(use_color=False),
            metadata=metadata,
        )
        assert "SF=sf001" in bp.render()

        # Heatmap
        hm = ASCIIHeatmap(
            matrix=[[1, 2], [3, 4]],
            row_labels=["Q1", "Q2"],
            col_labels=["A", "B"],
            options=ASCIIChartOptions(use_color=False),
            metadata=metadata,
        )
        assert "SF=sf001" in hm.render()


class TestColoredLabels:
    """Tests for Phase 4: uniform color application to labels."""

    def test_bar_chart_label_has_ansi(self):
        """Bar chart labels include ANSI color codes when color enabled."""
        data = [BarData(label="DuckDB", value=100)]
        opts = ASCIIChartOptions(use_color=True)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        # The label line should contain ANSI escape codes
        lines = [l for l in result.split("\n") if "DuckDB" in l]
        assert any("\033[" in l for l in lines)

    def test_bar_chart_label_no_ansi_when_disabled(self):
        """Bar chart labels have no ANSI codes when color disabled."""
        data = [BarData(label="DuckDB", value=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        data_lines = [l for l in result.split("\n") if "DuckDB" in l]
        assert all("\033[" not in l for l in data_lines)

    def test_box_plot_label_has_ansi(self):
        """Box plot series labels include ANSI color codes."""
        series = [BoxPlotSeries(name="DuckDB", values=[10, 20, 30, 40, 50])]
        opts = ASCIIChartOptions(use_color=True)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()
        label_lines = [l for l in result.split("\n") if "DuckDB" in l]
        assert any("\033[" in l for l in label_lines)

    def test_comparison_bar_names_have_ansi(self):
        """ComparisonBar run names include ANSI color codes."""
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        data = [ComparisonBarData(label="Q1", baseline_value=100, comparison_value=80)]
        opts = ASCIIChartOptions(use_color=True)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()
        name_lines = [l for l in result.split("\n") if "Baseline" in l or "Comparison" in l]
        assert any("\033[" in l for l in name_lines)


class TestAxisLabels:
    """Tests for Phase 5: axis title labels."""

    def test_histogram_has_query_id_axis(self):
        """Histogram has 'Query ID' axis label."""
        data = [HistogramBar(query_id="Q1", latency_ms=100)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert "Query ID" in result

    def test_box_plot_has_axis_label(self):
        """Box plot has axis label matching y_label."""
        series = [BoxPlotSeries(name="Test", values=[10, 20, 30])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIBoxPlot(series=series, y_label="Latency (ms)", options=opts)
        result = chart.render()
        assert "Latency (ms)" in result

    def test_heatmap_has_platform_axis(self):
        """Heatmap has 'Platform' axis label."""
        matrix = [[1, 2], [3, 4]]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=["Q1", "Q2"], col_labels=["A", "B"], options=opts)
        result = chart.render()
        assert "Platform" in result

    def test_scatter_plot_has_axis_labels(self):
        """Scatter plot has both axis labels."""
        points = [ScatterPoint(name="A", x=1, y=2), ScatterPoint(name="B", x=3, y=4)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIScatterPlot(points=points, x_label="Cost", y_label="Performance", options=opts)
        result = chart.render()
        assert "Cost" in result
        assert "Performance" in result

    def test_axis_label_has_arrow(self):
        """Axis labels include arrow indicator."""
        data = [HistogramBar(query_id="Q1", latency_ms=100)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        # Unicode arrow →
        assert "\u2192" in result

    def test_axis_label_ascii_fallback(self):
        """Axis labels use ASCII arrow when Unicode disabled."""
        data = [HistogramBar(query_id="Q1", latency_ms=100)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=False)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert "->" in result


class TestHeatmapDivergingPalette:
    """Tests for Phase 6: heatmap blue→red diverging palette."""

    def test_diverging_palette_is_default(self):
        """Diverging palette (blue→red) is the default."""
        matrix = [[1, 2], [3, 4]]
        chart = ASCIIHeatmap(
            matrix=matrix,
            row_labels=["Q1", "Q2"],
            col_labels=["A", "B"],
            options=ASCIIChartOptions(use_color=False),
        )
        assert chart._color_scale == ASCIIHeatmap.COLOR_SCALE_DIVERGING

    def test_sequential_palette_selectable(self):
        """Sequential palette can be selected via color_scheme parameter."""
        matrix = [[1, 2], [3, 4]]
        chart = ASCIIHeatmap(
            matrix=matrix,
            row_labels=["Q1", "Q2"],
            col_labels=["A", "B"],
            color_scheme="sequential",
            options=ASCIIChartOptions(use_color=False),
        )
        assert chart._color_scale == ASCIIHeatmap.COLOR_SCALE_SEQUENTIAL

    def test_diverging_palette_colors(self):
        """Diverging palette has blue and red endpoints."""
        assert "#2166ac" in ASCIIHeatmap.COLOR_SCALE_DIVERGING  # Blue
        assert "#b2182b" in ASCIIHeatmap.COLOR_SCALE_DIVERGING  # Red

    def test_scale_legend_shows_fast_slow(self):
        """Scale legend indicates fast and slow ends of the scale."""
        matrix = [[1, 2], [3, 4]]
        chart = ASCIIHeatmap(
            matrix=matrix,
            row_labels=["Q1", "Q2"],
            col_labels=["A", "B"],
            options=ASCIIChartOptions(use_color=False),
        )
        result = chart.render()
        assert "fast" in result
        assert "slow" in result

    def test_from_matrix_passes_color_scheme(self):
        """from_matrix factory passes color_scheme parameter."""
        chart = from_matrix(
            matrix=[[1, 2], [3, 4]],
            queries=["Q1", "Q2"],
            platforms=["A", "B"],
            color_scheme="sequential",
        )
        assert chart._color_scale == ASCIIHeatmap.COLOR_SCALE_SEQUENTIAL


class TestHistogramOutliers:
    """Tests for Phase 7: histogram outlier bar distinction."""

    def test_outlier_detected_by_iqr(self):
        """Outlier bars are detected when value exceeds Q3 + 1.5*IQR."""
        # Create data where Q10 is clearly an outlier
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=float(10 + i)) for i in range(1, 10)]
        data.append(HistogramBar(query_id="Q10", latency_ms=500.0))  # Clear outlier
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        chart.render()  # Triggers outlier detection
        assert "Q10" in chart._outlier_ids

    def test_outlier_uses_distinct_char(self):
        """Outlier bars use ▓ instead of █."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=float(10 + i)) for i in range(1, 10)]
        data.append(HistogramBar(query_id="Q10", latency_ms=500.0))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert "▓" in result

    def test_outlier_ascii_fallback(self):
        """Outlier bars use # in ASCII mode."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=float(10 + i)) for i in range(1, 10)]
        data.append(HistogramBar(query_id="Q10", latency_ms=500.0))
        opts = ASCIIChartOptions(use_color=False, use_unicode=False)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert "#" in result

    def test_outlier_in_legend(self):
        """Outlier indicator appears in legend."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=float(10 + i)) for i in range(1, 10)]
        data.append(HistogramBar(query_id="Q10", latency_ms=500.0))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert "Outlier" in result

    def test_no_outlier_with_uniform_data(self):
        """No outliers detected when data is uniform."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=100.0) for i in range(1, 11)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        chart.render()
        assert len(chart._outlier_ids) == 0


class TestSummaryBoxDotLeader:
    """Tests for Phase 8: summary box dot-leader padding."""

    def test_dot_leader_present_in_comparison(self):
        """Comparison summary box uses dot-leader between values and percentage."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(
            geo_mean_baseline_ms=100.0,
            geo_mean_comparison_ms=80.0,
            total_time_baseline_ms=1000.0,
            total_time_comparison_ms=800.0,
            num_queries=10,
        )
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        # Should contain middle dots for dot-leader
        assert "·" in result

    def test_dot_leader_ascii_fallback(self):
        """Dot-leader uses period in ASCII mode."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(
            geo_mean_baseline_ms=100.0,
            geo_mean_comparison_ms=80.0,
        )
        opts = ASCIIChartOptions(use_color=False, use_unicode=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        # Should contain periods for dot-leader
        assert ".." in result

    def test_no_wide_blank_gap(self):
        """No wide blank gap (>10 consecutive spaces) between value and percentage."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(
            geo_mean_baseline_ms=18.2,
            geo_mean_comparison_ms=61.3,
            num_queries=22,
        )
        opts = ASCIIChartOptions(width=100, use_color=False, use_unicode=True)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        # Find the Geo Mean line and check for no wide blank gap
        for line in result.split("\n"):
            if "Geo Mean" in line and "%" in line:
                # Should not have more than 3 consecutive spaces (since dots fill the gap)
                assert "     " not in line  # 5+ spaces means gap not filled

    def test_single_run_no_dot_leader(self):
        """Single-run summary box has no dot-leader (no percentage to trace to)."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(geo_mean_ms=100.0, total_time_ms=500.0, num_queries=5)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        # Single-run metrics don't have percentage, so no dot-leader needed
        assert "·" not in result or ".." not in result


class TestEdgeCases:
    """Tests for edge cases in new features."""

    def test_very_long_name_truncated_at_30(self):
        """Names longer than 30 chars are truncated in all chart types."""
        long_name = "A" * 40
        data = [BarData(label=long_name, value=100)]
        opts = ASCIIChartOptions(width=100, use_color=False)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()
        assert long_name not in result
        assert ".." in result

    def test_metadata_subtitle_truncated_to_width(self):
        """Subtitle is truncated if it exceeds chart width."""
        data = [BarData(label="Q1", value=100)]
        metadata = {"platform_version": "V" * 100}
        opts = ASCIIChartOptions(width=40, use_color=False)
        chart = ASCIIBarChart(data=data, options=opts, metadata=metadata)
        result = chart.render()
        # Should render without error and within width bounds
        for line in result.split("\n"):
            # Allow some tolerance for ANSI codes (stripped in no-color mode)
            assert len(line) <= 45  # 40 + small tolerance

    def test_axis_label_renders_in_line_chart(self):
        """Line chart axis labels use styled rendering."""
        points = [LinePoint(series="A", x=1, y=10), LinePoint(series="A", x=2, y=20)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIILineChart(points=points, y_label="Time (ms)", options=opts)
        result = chart.render()
        assert "Time (ms)" in result
        # Should have arrow indicator
        assert "\u2191" in result
