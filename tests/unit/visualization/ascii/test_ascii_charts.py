"""Tests for ASCII chart visualization module."""

from __future__ import annotations

import math

import pytest

from benchbox.core.visualization.ascii.bar_chart import ASCIIBarChart, BarData, from_bar_data
from benchbox.core.visualization.ascii.base import (
    TRUNCATION_MARKER,
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
from benchbox.core.visualization.ascii.cdf_chart import (
    ASCIICDFChart,
    CDFSeriesData,
    from_query_results as cdf_from_query_results,
)
from benchbox.core.visualization.ascii.heatmap import ASCIIHeatmap, from_matrix
from benchbox.core.visualization.ascii.histogram import ASCIIQueryHistogram, HistogramBar, from_query_latency_data
from benchbox.core.visualization.ascii.line_chart import ASCIILineChart, LinePoint, from_time_series_points
from benchbox.core.visualization.ascii.normalized_speedup import (
    ASCIINormalizedSpeedup,
    SpeedupData,
    from_normalized_results,
)
from benchbox.core.visualization.ascii.percentile_ladder import (
    ASCIIPercentileLadder,
    PercentileData,
    compute_percentile,
    from_query_results,
)
from benchbox.core.visualization.ascii.rank_table import ASCIIRankTable, RankTableData, from_heatmap_data
from benchbox.core.visualization.ascii.scatter_plot import ASCIIScatterPlot, ScatterPoint, from_cost_performance_points
from benchbox.core.visualization.ascii.sparkline_table import (
    ASCIISparklineTable,
    SparklineColumn,
    SparklineTableData,
)
from benchbox.core.visualization.ascii.stacked_bar import ASCIIStackedBar, StackedBarData, StackedBarSegment
from tests.fixtures.result_dict_fixtures import make_normalized_result


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

    def test_no_color_heatmap_uses_column_separators(self):
        """No-color heatmap includes explicit separators between data columns."""
        matrix = [[34, 40], [83, 94]]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=["Q1", "Q2"], col_labels=["A", "B"], options=opts)
        result = chart.render()
        q1_line = next(line for line in result.splitlines() if "Q1" in line)
        assert q1_line.count("│") >= 2

    def test_no_color_heatmap_right_aligns_values_with_density_fill(self):
        """No-color heatmap right-aligns values and fills cell area with intensity chars."""
        matrix = [[50, 140], [20, 80]]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=["Q1", "Q2"], col_labels=["A", "B"], options=opts)
        result = chart.render()
        q1_line = next(line for line in result.splitlines() if "Q1" in line)
        assert "░" in q1_line or "▒" in q1_line or "▓" in q1_line or "█" in q1_line
        assert "50" in q1_line
        assert "140" in q1_line

    def test_no_color_heatmap_cells_are_fixed_width_with_value_spacing(self):
        """No-color heatmap keeps equal cell widths and a separator space before numbers."""
        matrix = [[34, 140], [83, 94]]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=["Q1", "Q2"], col_labels=["A", "B"], options=opts)
        result = chart.render()
        q1_line = next(line for line in result.splitlines() if "Q1" in line)

        data_region = q1_line.split("│", 1)[1]
        cells = [c for c in data_region.split("│") if c.strip()]
        assert len(cells) == 2
        assert len(cells[0]) == len(cells[1])
        assert " 34" in cells[0]
        assert " 140" in cells[1]

    def test_height_constrained_heatmap_shows_row_truncation_indicator(self):
        """Row truncation indicator is shown when options.height limits visible rows."""
        matrix = [[float(i), float(i + 1)] for i in range(12)]
        rows = [f"Q{i}" for i in range(12)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True, height=12)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=rows, col_labels=["A", "B"], options=opts)
        result = chart.render()
        assert "more rows" in result


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

        with caplog.at_level(logging.WARNING, logger="benchbox.core.visualization.ascii.comparison_bar"):
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

        with caplog.at_level(logging.WARNING, logger="benchbox.core.visualization.ascii.diverging_bar"):
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

    def test_long_title_does_not_overflow_box_width(self):
        """Very long titles are truncated to fit the configured width."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        width = 80
        stats = SummaryStats(title="X" * 200, geo_mean_ms=100)
        opts = ASCIIChartOptions(width=width, use_color=False)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        for line in result.splitlines():
            assert len(line) == width

    def test_long_best_worst_text_does_not_overflow_box_width(self):
        """Best/worst rows are truncated to preserve box borders."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        width = 80
        stats = SummaryStats(
            title="Summary",
            geo_mean_baseline_ms=100,
            geo_mean_comparison_ms=130,
            total_time_baseline_ms=4000,
            total_time_comparison_ms=3000,
            num_queries=3,
            num_improved=1,
            num_stable=1,
            num_regressed=1,
            best_queries=[
                ("aggregation_groupby_large", -12.2),
                ("exchange_merge_join_extremely_verbose_name", -10.0),
                ("read_parquet_single", -9.2),
            ],
            worst_queries=[("another_extremely_verbose_query_identifier_name", 55.0)],
        )
        opts = ASCIIChartOptions(width=width, use_color=False, use_unicode=True)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        for line in result.splitlines():
            assert len(line) == width

    def test_two_column_mode_includes_percentage_deltas(self):
        """Two-column summary mode keeps percentage deltas visible."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(
            title="Summary",
            geo_mean_baseline_ms=100,
            geo_mean_comparison_ms=130,
            total_time_baseline_ms=4000,
            total_time_comparison_ms=3000,
            num_queries=22,
            environment={"OS": "macOS", "Python": "3.12.2", "CPUs": "10", "Memory": "16GB"},
        )
        opts = ASCIIChartOptions(width=120, use_color=False, use_unicode=True)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        assert "+30.0%" in result
        assert "-25.0%" in result

    def test_two_column_mode_colorizes_percentage_deltas_when_color_enabled(self):
        """Two-column mode keeps colored percentage deltas when color output is enabled."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        stats = SummaryStats(
            title="Summary",
            geo_mean_baseline_ms=100,
            geo_mean_comparison_ms=130,
            total_time_baseline_ms=4000,
            total_time_comparison_ms=3000,
            num_queries=22,
            environment={"OS": "macOS", "Python": "3.12.2"},
        )
        opts = ASCIIChartOptions(width=120, use_color=True, use_unicode=True)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()
        assert "+30.0%" in result
        assert "-25.0%" in result
        assert "\033[" in result


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

    def test_disambiguate_modes_both_get_suffix(self):
        """When same platform has SQL and DataFrame modes, both get suffixed."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            make_normalized_result(platform="DataFusion", execution_mode="sql"),
            make_normalized_result(platform="DataFusion", execution_mode="dataframe"),
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

        results = [make_normalized_result(platform="DuckDB", execution_mode="sql")]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._disambiguate_modes()

        assert plotter.results[0].platform == "DuckDB"

    def test_same_mode_no_suffix(self):
        """Two results with same mode don't get suffixed."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            make_normalized_result(platform="DuckDB", execution_mode="sql"),
            make_normalized_result(platform="Polars", execution_mode="sql"),
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


class TestDisambiguateVersions:
    """Tests for _disambiguate_versions in ResultPlotter."""

    def test_different_versions_appended_to_label(self):
        """When same platform has different driver versions, both labels get version appended."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            make_normalized_result(platform="DuckDB", platform_version="1.0.0"),
            make_normalized_result(platform="DuckDB", platform_version="1.4.3"),
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._disambiguate_versions()

        platforms = {r.platform for r in plotter.results}
        assert "DuckDB 1.0.0" in platforms
        assert "DuckDB 1.4.3" in platforms

    def test_same_version_labels_unchanged(self):
        """When both runs share the same version, labels are not modified."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            make_normalized_result(platform="DuckDB", platform_version="1.4.3"),
            make_normalized_result(platform="DuckDB", platform_version="1.4.3"),
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._disambiguate_versions()

        for r in plotter.results:
            assert r.platform == "DuckDB"

    def test_different_platforms_unaffected(self):
        """Different platforms with no version collision are not modified."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            make_normalized_result(platform="DuckDB", platform_version="1.0.0"),
            make_normalized_result(platform="Polars", platform_version="0.19.0"),
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._disambiguate_versions()

        platforms = [r.platform for r in plotter.results]
        assert "DuckDB" in platforms
        assert "Polars" in platforms

    def test_no_version_in_raw_skipped(self):
        """Results without version info in raw data are not modified."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            make_normalized_result(platform="DuckDB"),
            make_normalized_result(platform="DuckDB"),
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._disambiguate_versions()

        for r in plotter.results:
            assert r.platform == "DuckDB"


class TestSortResultsByVersion:
    """Tests for _sort_results_by_version in ResultPlotter."""

    def test_versions_sorted_ascending(self):
        """Results with version labels are sorted from oldest to newest."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            make_normalized_result(platform="DuckDB 1.3.2"),
            make_normalized_result(platform="DuckDB 1.0.0"),
            make_normalized_result(platform="DuckDB 1.2.2"),
            make_normalized_result(platform="DuckDB 1.1.3"),
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._sort_results_by_version()

        ordered = [r.platform for r in plotter.results]
        assert ordered == ["DuckDB 1.0.0", "DuckDB 1.1.3", "DuckDB 1.2.2", "DuckDB 1.3.2"]

    def test_prerelease_sorts_after_stable(self):
        """Pre-release (dev) versions sort after the corresponding stable release."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            make_normalized_result(platform="DuckDB 1.5.0-dev7404"),
            make_normalized_result(platform="DuckDB 1.4.4"),
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._sort_results_by_version()

        ordered = [r.platform for r in plotter.results]
        assert ordered == ["DuckDB 1.4.4", "DuckDB 1.5.0-dev7404"]

    def test_six_versions_full_order(self):
        """Full six-version scenario from the user's test case sorts correctly."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        insert_order = [
            "DuckDB 1.1.3",
            "DuckDB 1.0.0",
            "DuckDB 1.2.2",
            "DuckDB 1.3.2",
            "DuckDB 1.4.4",
            "DuckDB 1.5.0-dev7404",
        ]
        expected = [
            "DuckDB 1.0.0",
            "DuckDB 1.1.3",
            "DuckDB 1.2.2",
            "DuckDB 1.3.2",
            "DuckDB 1.4.4",
            "DuckDB 1.5.0-dev7404",
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = [make_normalized_result(platform=p) for p in insert_order]
        plotter._sort_results_by_version()

        assert [r.platform for r in plotter.results] == expected

    def test_non_versioned_labels_sort_last(self):
        """Labels without a version number sort after versioned ones."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [
            make_normalized_result(platform="Polars"),
            make_normalized_result(platform="DuckDB 1.0.0"),
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._sort_results_by_version()

        assert plotter.results[0].platform == "DuckDB 1.0.0"
        assert plotter.results[1].platform == "Polars"

    def test_single_result_unchanged(self):
        """Single result is unaffected."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        results = [make_normalized_result(platform="DuckDB 1.0.0")]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        plotter._sort_results_by_version()

        assert plotter.results[0].platform == "DuckDB 1.0.0"


@pytest.mark.fast
class TestHeatmapColumnFitting:
    """Tests for heatmap column-shrinking behavior (fit all cols before truncating)."""

    def _make_heatmap(self, num_cols: int, width: int = 120) -> ASCIIHeatmap:
        """Build a heatmap with ``num_cols`` platforms and a fixed chart width."""
        matrix = [[float(10 * (i + 1) + j) for j in range(num_cols)] for i in range(3)]
        row_labels = [f"Q{i + 1}" for i in range(3)]
        col_labels = [f"Platform{j + 1}" for j in range(num_cols)]
        opts = ASCIIChartOptions(width=width, use_color=False)
        return ASCIIHeatmap(matrix=matrix, row_labels=row_labels, col_labels=col_labels, options=opts)

    def test_all_columns_shown_when_they_fit(self):
        """When columns comfortably fit at default cell width, all are rendered."""
        chart = self._make_heatmap(num_cols=3, width=120)
        result = chart.render()
        for j in range(1, 4):
            assert f"Platform{j}" in result

    def test_columns_compressed_instead_of_truncated(self):
        """When columns overflow at default width, cell_width shrinks to fit all cols."""
        # 6 columns with long labels at width=120 would normally truncate; they must all appear.
        chart = self._make_heatmap(num_cols=6, width=120)
        result = chart.render()
        for j in range(1, 7):
            assert f"Platform{j}" in result
        # The truncation indicator should NOT appear when compression succeeds.
        assert "..." not in result

    def test_many_columns_compressed_to_minimum(self):
        """Very many columns compress to minimum cell width and all remain visible."""
        chart = self._make_heatmap(num_cols=10, width=120)
        result = chart.render()
        # At least the first and last column labels must appear.
        assert "Platform1" in result
        assert "Platform10" in result

    def test_truncation_only_when_compression_insufficient(self):
        """Truncation indicator appears only when compressed cell_width < minimum viable width."""
        # 40 columns in 80 chars: each cell would need 2 chars, below minimum (5).
        # In this case truncation is acceptable.
        chart = self._make_heatmap(num_cols=40, width=80)
        result = chart.render()
        # The chart must render without crashing; first column always visible.
        assert "Platform1" in result

    def test_row_labels_all_present(self):
        """Row labels are always rendered regardless of column compression."""
        chart = self._make_heatmap(num_cols=6, width=120)
        result = chart.render()
        for i in range(1, 4):
            assert f"Q{i}" in result


@pytest.mark.fast
class TestHistogramWidthAware:
    """Tests for histogram width-aware chunking and footer wrapping."""

    def _make_bars(self, num_queries: int, platforms: list[str]) -> list[HistogramBar]:
        """Build HistogramBar list for ``num_queries`` queries across ``platforms``."""
        bars: list[HistogramBar] = []
        for p in platforms:
            for q in range(1, num_queries + 1):
                bars.append(HistogramBar(query_id=str(q), latency_ms=10.0 + q, platform=p))
        return bars

    # ------------------------------------------------------------------ chunking

    def test_chunk_data_respects_max_queries_override(self):
        """_chunk_data splits by the max_queries override, not self.max_per_chart."""
        bars = self._make_bars(10, ["A", "B"])
        opts = ASCIIChartOptions(width=120, use_color=False)
        hist = ASCIIQueryHistogram(data=bars, options=opts)
        # Bootstrap internal state that render() normally sets.
        hist._use_grouped = True
        hist._platforms = ["A", "B"]
        chunks = hist._chunk_data(hist._sort_data(), max_queries=5)
        assert len(chunks) == 2
        # Each chunk should contain bars for exactly 5 unique queries.
        for chunk in chunks:
            unique_qids = {b.query_id for b in chunk}
            assert len(unique_qids) == 5

    def test_chunk_data_no_split_when_fits(self):
        """_chunk_data returns one chunk when unique queries ≤ max_queries."""
        bars = self._make_bars(8, ["A", "B"])
        opts = ASCIIChartOptions(width=120, use_color=False)
        hist = ASCIIQueryHistogram(data=bars, options=opts)
        hist._use_grouped = True
        hist._platforms = ["A", "B"]
        chunks = hist._chunk_data(hist._sort_data(), max_queries=10)
        assert len(chunks) == 1

    def test_grouped_render_splits_when_queries_exceed_width(self):
        """render() splits into multiple charts when 6 platforms × 22 queries exceed width=120."""
        platforms = [f"DuckDB 1.{v}.0" for v in range(6)]
        bars = self._make_bars(22, platforms)
        opts = ASCIIChartOptions(width=120, use_color=False)
        hist = ASCIIQueryHistogram(data=bars, options=opts)
        result = hist.render()
        # Two chart blocks separated by a blank line.
        assert "\n\n" in result

    def test_grouped_render_single_chart_when_few_queries(self):
        """render() produces one chart when queries fit within the width."""
        platforms = ["A", "B"]
        bars = self._make_bars(5, platforms)
        opts = ASCIIChartOptions(width=120, use_color=False)
        hist = ASCIIQueryHistogram(data=bars, options=opts)
        result = hist.render()
        # Only one chart header line rendered (no split into multiple charts).
        assert result.count("Query Latency Histogram") == 1

    # ------------------------------------------------------------------ footer wrapping

    def test_footer_wraps_when_legend_exceeds_width(self):
        """_build_grouped_footer wraps long legends to multiple lines."""
        # Build 6 long-named platforms to force a wide footer.
        platforms = [f"DuckDB {1 + i}.{i}.{i}" for i in range(6)]
        bars = self._make_bars(3, platforms)
        opts = ASCIIChartOptions(width=80, use_color=False)
        hist = ASCIIQueryHistogram(data=bars, options=opts)
        # Drive internal state.
        hist._use_grouped = True
        hist._platforms = platforms
        hist._outlier_bar_keys = set()
        platform_colors = dict.fromkeys(platforms, "#ffffff")
        platform_fills = dict.fromkeys(platforms, "█")
        from benchbox.core.visualization.ascii.base import ColorMode, TerminalColors

        colors = TerminalColors(color_mode=ColorMode.NONE)
        footer = hist._build_grouped_footer(
            bars,
            global_mean=15.0,
            platform_colors=platform_colors,
            platform_fills=platform_fills,
            no_color=True,
            colors=colors,
            y_axis_width=8,
            width=80,
        )
        # Should contain at least one newline (wrapped to multiple lines).
        assert "\n" in footer

    def test_footer_single_line_when_legend_fits(self):
        """_build_grouped_footer stays on one line when legend is short."""
        platforms = ["A", "B"]
        bars = self._make_bars(2, platforms)
        opts = ASCIIChartOptions(width=120, use_color=False)
        hist = ASCIIQueryHistogram(data=bars, options=opts)
        hist._use_grouped = True
        hist._platforms = platforms
        hist._outlier_bar_keys = set()
        platform_colors = dict.fromkeys(platforms, "#ffffff")
        platform_fills = dict.fromkeys(platforms, "█")
        from benchbox.core.visualization.ascii.base import ColorMode, TerminalColors

        colors = TerminalColors(color_mode=ColorMode.NONE)
        footer = hist._build_grouped_footer(
            bars,
            global_mean=12.0,
            platform_colors=platform_colors,
            platform_fills=platform_fills,
            no_color=True,
            colors=colors,
            y_axis_width=8,
            width=120,
        )
        assert "\n" not in footer
        assert "A" in footer
        assert "B" in footer

    def test_footer_no_line_exceeds_width(self):
        """Each wrapped line of the footer stays within the chart width."""
        import re as _re

        platforms = [f"DuckDB {1 + i}.{i}.{i}" for i in range(6)]
        bars = self._make_bars(3, platforms)
        chart_width = 80
        opts = ASCIIChartOptions(width=chart_width, use_color=False)
        hist = ASCIIQueryHistogram(data=bars, options=opts)
        hist._use_grouped = True
        hist._platforms = platforms
        hist._outlier_bar_keys = set()
        platform_colors = dict.fromkeys(platforms, "#ffffff")
        platform_fills = dict.fromkeys(platforms, "█")
        from benchbox.core.visualization.ascii.base import ColorMode, TerminalColors

        colors = TerminalColors(color_mode=ColorMode.NONE)
        footer = hist._build_grouped_footer(
            bars,
            global_mean=15.0,
            platform_colors=platform_colors,
            platform_fills=platform_fills,
            no_color=True,
            colors=colors,
            y_axis_width=8,
            width=chart_width,
        )
        ansi_re = _re.compile(r"\x1b\[[0-9;]*m")
        for line in footer.split("\n"):
            visible_len = len(ansi_re.sub("", line))
            assert visible_len <= chart_width, f"Footer line too wide ({visible_len} > {chart_width}): {line!r}"


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
        """Outlier bars use the configured distinct no-color unicode glyph."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=float(10 + i)) for i in range(1, 10)]
        data.append(HistogramBar(query_id="Q10", latency_ms=500.0))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert chart._get_outlier_char(no_color=True) in result

    def test_outlier_ascii_fallback(self):
        """Outlier bars use the configured no-color ASCII fallback glyph."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=float(10 + i)) for i in range(1, 10)]
        data.append(HistogramBar(query_id="Q10", latency_ms=500.0))
        opts = ASCIIChartOptions(use_color=False, use_unicode=False)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()
        assert chart._get_outlier_char(no_color=True) in result

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

    def test_grouped_outlier_marks_only_the_outlier_bar(self):
        """Grouped mode tracks outliers per platform+query bar, not per query ID."""
        data: list[HistogramBar] = []
        for i in range(1, 11):
            baseline = 80 + i
            comparison = 82 + i
            if i == 10:
                comparison = 500  # clear outlier on one platform only
            data.append(HistogramBar(query_id=f"Q{i}", latency_ms=baseline, platform="DF(df)"))
            data.append(HistogramBar(query_id=f"Q{i}", latency_ms=comparison, platform="DF(sql)"))

        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        chart.render()

        assert ("DF(sql)", "Q10") in chart._outlier_bar_keys
        assert ("DF(df)", "Q10") not in chart._outlier_bar_keys


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

    def test_no_color_percentage_arrows_do_not_overflow_box_width(self):
        """No-color percentage arrows are included in width calculations."""
        from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

        width = 120
        stats = SummaryStats(
            title="DataFusion (df) vs DataFusion (sql) Summary",
            geo_mean_baseline_ms=62.6,
            geo_mean_comparison_ms=66.5,  # +6.2% -> includes up arrow in no-color mode
            total_time_baseline_ms=4900.0,
            total_time_comparison_ms=4831.0,  # -1.4% (no arrow threshold)
            num_queries=22,
        )
        opts = ASCIIChartOptions(width=width, use_color=False, use_unicode=True)
        chart = ASCIISummaryBox(stats=stats, options=opts)
        result = chart.render()

        for line in result.splitlines():
            assert len(line) == width


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


# ── Percentile Ladder Chart Tests ────────────────────────────────


class TestASCIIPercentileLadder:
    """Tests for percentile ladder chart rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        chart = ASCIIPercentileLadder(data=[])
        result = chart.render()
        assert "No data" in result

    def test_single_platform(self):
        """Single platform renders correctly."""
        data = [PercentileData("DuckDB", 12, 45, 78, 120)]
        chart = ASCIIPercentileLadder(data=data)
        result = chart.render()
        assert "DuckDB" in result
        assert "12" in result
        assert "120" in result
        assert "P50" in result
        assert "P99" in result

    def test_multiple_platforms(self):
        """Multiple platforms render as separate rows."""
        data = [
            PercentileData("DuckDB", 12, 45, 78, 120),
            PercentileData("Polars", 15, 52, 95, 310),
            PercentileData("Pandas", 25, 85, 140, 280),
        ]
        chart = ASCIIPercentileLadder(data=data)
        result = chart.render()
        assert "DuckDB" in result
        assert "Polars" in result
        assert "Pandas" in result

    def test_band_fill_chars_unicode(self):
        """Unicode mode uses ░▒▓█ fill characters."""
        data = [PercentileData("Test", 10, 20, 30, 40)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()
        assert "░" in result
        assert "█" in result

    def test_band_fill_chars_ascii(self):
        """ASCII mode uses .=#@ fill characters."""
        data = [PercentileData("Test", 10, 20, 30, 40)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=False)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()
        assert "░" not in result
        assert "." in result or "=" in result or "#" in result or "@" in result

    def test_no_color_output(self):
        """Chart renders without ANSI codes when color disabled."""
        data = [PercentileData("Test", 10, 20, 30, 40)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()
        assert "\033[" not in result

    def test_legend_shows_all_bands(self):
        """Legend shows all four percentile bands."""
        data = [PercentileData("Test", 10, 20, 30, 40)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()
        assert "P50" in result
        assert "P90" in result
        assert "P95" in result
        assert "P99" in result

    def test_annotation_shows_pipe_separated_values(self):
        """Annotation shows values separated by pipes."""
        data = [PercentileData("Test", 10, 20, 30, 40)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()
        # Should contain pipe-separated values
        assert "10.0 | 20.0 | 30.0 | 40.0" in result

    def test_annotation_values_use_shared_width_and_precision(self):
        """Annotation values are right-justified with consistent decimal precision."""
        data = [
            PercentileData("DataFusion (df)", 66.5, 123.9, 165.9, 217.1),
            PercentileData("DataFusion (sql)", 62, 120.7, 146.2, 237.1),
        ]
        opts = ASCIIChartOptions(use_color=False, width=120)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()

        assert " 66.5 | 123.9 | 165.9 | 217.1" in result
        assert " 62.0 | 120.7 | 146.2 | 237.1" in result

    def test_identical_percentiles(self):
        """Identical percentile values render without crash."""
        data = [PercentileData("Flat", 50, 50, 50, 50)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()
        assert "Flat" in result

    def test_zero_percentiles(self):
        """Zero percentile values render without crash."""
        data = [PercentileData("Zero", 0, 0, 0, 0)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()
        assert "Zero" in result

    def test_narrow_width(self):
        """Chart renders at minimum width."""
        data = [PercentileData("VeryLongPlatformName", 10, 50, 80, 200)]
        opts = ASCIIChartOptions(width=40, use_color=False)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()
        assert len(result) > 0

    def test_metadata_subtitle(self):
        """Metadata subtitle renders when provided."""
        data = [PercentileData("Test", 10, 20, 30, 40)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIPercentileLadder(data=data, options=opts, metadata={"scale_factor": 1})
        result = chart.render()
        assert "SF=sf1" in result


class TestComputePercentile:
    """Tests for compute_percentile function."""

    def test_empty_input(self):
        """Empty input returns 0."""
        assert compute_percentile([], 50) == 0.0

    def test_single_value(self):
        """Single value returns that value for any percentile."""
        assert compute_percentile([42], 50) == 42
        assert compute_percentile([42], 99) == 42

    def test_two_values(self):
        """Two values interpolate correctly."""
        result = compute_percentile([10, 20], 50)
        assert result == 15.0

    def test_known_percentiles(self):
        """Known percentile values from sorted array."""
        vals = list(range(1, 101))  # 1 to 100
        assert compute_percentile(vals, 50) == 50.5
        assert compute_percentile(vals, 0) == 1.0
        assert compute_percentile(vals, 100) == 100.0

    def test_p99_near_max(self):
        """P99 is close to maximum for large arrays."""
        vals = list(range(1, 1001))
        p99 = compute_percentile(vals, 99)
        assert 990 <= p99 <= 1000

    def test_unsorted_input(self):
        """Function handles unsorted input correctly."""
        result = compute_percentile([50, 10, 30, 20, 40], 50)
        assert result == 30.0


class TestFromQueryResults:
    """Tests for from_query_results factory function."""

    def test_basic_factory(self):
        """Factory creates chart from raw query times."""
        chart = from_query_results(
            [("DuckDB", [10, 20, 30, 50, 120])],
            options=ASCIIChartOptions(use_color=False),
        )
        result = chart.render()
        assert "DuckDB" in result

    def test_empty_values(self):
        """Factory handles platform with no query times."""
        chart = from_query_results(
            [("Empty", [])],
            options=ASCIIChartOptions(use_color=False),
        )
        result = chart.render()
        assert "Empty" in result

    def test_multi_platform_factory(self):
        """Factory handles multiple platforms."""
        chart = from_query_results(
            [
                ("DuckDB", [10, 20, 30, 50, 120]),
                ("Polars", [15, 25, 45, 80, 310]),
            ],
            options=ASCIIChartOptions(use_color=False),
        )
        result = chart.render()
        assert "DuckDB" in result
        assert "Polars" in result

    def test_exporters_render_percentile_ladder(self):
        """render_ascii_chart handles percentile_ladder type."""
        from benchbox.core.visualization.exporters import render_ascii_chart

        data = [PercentileData("DuckDB", 12, 45, 78, 120)]
        result = render_ascii_chart("percentile_ladder", data, title="Test")
        assert "DuckDB" in result

    def test_percentile_ladder_importable_from_module(self):
        """ASCIIPercentileLadder is importable from the ascii module."""
        from benchbox.core.visualization.ascii import ASCIIPercentileLadder

        assert ASCIIPercentileLadder is not None


# ── Normalized Speedup Chart Tests ────────────────────────────────


class TestASCIINormalizedSpeedup:
    """Tests for normalized speedup chart rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        chart = ASCIINormalizedSpeedup(data=[])
        result = chart.render()
        assert "No data" in result

    def test_single_platform_baseline(self):
        """Single platform at baseline renders."""
        data = [SpeedupData("SQLite", 1.0, True)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIINormalizedSpeedup(data=data, options=opts)
        result = chart.render()
        assert "SQLite" in result
        assert "1.00x" in result

    def test_faster_and_slower(self):
        """Faster and slower platforms render on both sides."""
        data = [
            SpeedupData("SQLite", 1.0, True),
            SpeedupData("DuckDB", 8.2, False),
            SpeedupData("Pandas", 0.4, False),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIINormalizedSpeedup(data=data, options=opts)
        result = chart.render()
        assert "SQLite" in result
        assert "DuckDB" in result
        assert "Pandas" in result
        assert "8.20x" in result
        assert "0.40x" in result

    def test_log2_symmetry(self):
        """2x faster and 0.5x slower produce equal bar lengths."""
        data = [
            SpeedupData("Base", 1.0, True),
            SpeedupData("Fast", 2.0, False),
            SpeedupData("Slow", 0.5, False),
        ]
        opts = ASCIIChartOptions(use_color=False, width=80)
        chart = ASCIINormalizedSpeedup(data=data, options=opts)
        result = chart.render()
        # Both should appear in the output
        assert "2.00x" in result
        assert "0.50x" in result

    def test_no_color_output(self):
        """Chart renders without ANSI codes when color disabled."""
        data = [SpeedupData("Test", 1.5, False)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIINormalizedSpeedup(data=data, options=opts)
        result = chart.render()
        assert "\033[" not in result

    def test_direction_labels(self):
        """Chart shows Slower/Faster direction labels."""
        data = [SpeedupData("Test", 2.0, False)]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIINormalizedSpeedup(data=data, options=opts)
        result = chart.render()
        assert "Slower" in result
        assert "Faster" in result


class TestFromNormalizedResults:
    """Tests for from_normalized_results factory."""

    def test_basic_factory(self):
        """Factory creates chart from timing data."""
        chart = from_normalized_results(
            [("SQLite", 5000), ("DuckDB", 610)],
            baseline="SQLite",
            options=ASCIIChartOptions(use_color=False),
        )
        result = chart.render()
        assert "SQLite" in result
        assert "DuckDB" in result

    def test_slowest_baseline(self):
        """'slowest' auto-selects the slowest platform."""
        chart = from_normalized_results(
            [("A", 100), ("B", 500), ("C", 200)],
            baseline="slowest",
            options=ASCIIChartOptions(use_color=False),
        )
        result = chart.render()
        assert "1.00x" in result  # B should be 1.0x

    def test_fastest_baseline(self):
        """'fastest' auto-selects the fastest platform."""
        chart = from_normalized_results(
            [("A", 100), ("B", 500), ("C", 200)],
            baseline="fastest",
            options=ASCIIChartOptions(use_color=False),
        )
        result = chart.render()
        assert "1.00x" in result

    def test_empty_input(self):
        """Empty input renders no data."""
        chart = from_normalized_results([], options=ASCIIChartOptions(use_color=False))
        result = chart.render()
        assert "No data" in result

    def test_exporters_render_normalized_speedup(self):
        """render_ascii_chart handles normalized_speedup type."""
        from benchbox.core.visualization.exporters import render_ascii_chart

        data = [SpeedupData("Test", 2.0, False)]
        result = render_ascii_chart("normalized_speedup", data, title="Test")
        assert "Test" in result


# ── Stacked Bar Chart Tests ────────────────────────────────


class TestASCIIStackedBar:
    """Tests for stacked bar chart rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        chart = ASCIIStackedBar(data=[])
        result = chart.render()
        assert "No data" in result

    def test_single_platform_single_phase(self):
        """Single platform with one phase renders."""
        data = [StackedBarData("DuckDB", [StackedBarSegment("Power", 8000)])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()
        assert "DuckDB" in result
        assert "Power" in result

    def test_multi_platform_multi_phase(self):
        """Multiple platforms with multiple phases render."""
        data = [
            StackedBarData(
                "DuckDB",
                [
                    StackedBarSegment("Load", 1500),
                    StackedBarSegment("Power", 8000),
                ],
            ),
            StackedBarData(
                "SQLite",
                [
                    StackedBarSegment("Load", 5000),
                    StackedBarSegment("Power", 15000),
                ],
            ),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()
        assert "DuckDB" in result
        assert "SQLite" in result
        assert "Load" in result
        assert "Power" in result

    def test_total_auto_computed(self):
        """Total is auto-computed from segments if not provided."""
        d = StackedBarData("Test", [StackedBarSegment("A", 100), StackedBarSegment("B", 200)])
        assert d.total == 300

    def test_zero_phases_skipped(self):
        """Phases with zero value render without crash."""
        data = [
            StackedBarData(
                "Test",
                [
                    StackedBarSegment("A", 100),
                    StackedBarSegment("B", 0),
                    StackedBarSegment("C", 200),
                ],
            )
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()
        assert "Test" in result

    def test_time_formatting(self):
        """Time values format correctly in annotations."""
        data = [
            StackedBarData("Fast", [StackedBarSegment("Run", 500)], total=500),
            StackedBarData("Med", [StackedBarSegment("Run", 5000)], total=5000),
            StackedBarData("Slow", [StackedBarSegment("Run", 120000)], total=120000),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()
        assert "500ms" in result
        assert "5.0s" in result
        assert "2.0min" in result

    def test_legend_shows_phases(self):
        """Legend shows phase names."""
        data = [
            StackedBarData(
                "Test",
                [
                    StackedBarSegment("DataGen", 100),
                    StackedBarSegment("Load", 200),
                ],
            )
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()
        assert "DataGen" in result
        assert "Load" in result

    def test_no_color_output(self):
        """Chart renders without ANSI codes when color disabled."""
        data = [StackedBarData("Test", [StackedBarSegment("A", 100)])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()
        assert "\033[" not in result

    def test_exporters_render_stacked_phase(self):
        """render_ascii_chart handles stacked_phase type."""
        from benchbox.core.visualization.exporters import render_ascii_chart

        data = [StackedBarData("Test", [StackedBarSegment("Power", 5000)])]
        result = render_ascii_chart("stacked_phase", data, title="Test")
        assert "Test" in result


# ── Sparkline Table Tests ────────────────────────────────


class TestASCIISparklineTable:
    """Tests for sparkline table chart rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        data = SparklineTableData([], [])
        chart = ASCIISparklineTable(data=data)
        result = chart.render()
        assert "No data" in result

    def test_single_metric(self):
        """Single metric column renders."""
        cols = [SparklineColumn("Total", {"DuckDB": 1240, "Polars": 1580}, False)]
        data = SparklineTableData(["DuckDB", "Polars"], cols)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISparklineTable(data=data, options=opts)
        result = chart.render()
        assert "DuckDB" in result
        assert "Polars" in result
        assert "Total" in result

    def test_higher_is_better_inversion(self):
        """Higher-is-better columns show highest value with tallest bar."""
        cols = [SparklineColumn("Success", {"A": 100, "B": 50}, True)]
        data = SparklineTableData(["A", "B"], cols)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISparklineTable(data=data, options=opts)
        result = chart.render()
        assert "A" in result
        assert "B" in result

    def test_multiple_metrics(self):
        """Multiple metrics render as columns."""
        cols = [
            SparklineColumn("Latency", {"DuckDB": 56, "Polars": 72}, False),
            SparklineColumn("Success", {"DuckDB": 100, "Polars": 100}, True),
        ]
        data = SparklineTableData(["DuckDB", "Polars"], cols)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISparklineTable(data=data, options=opts)
        result = chart.render()
        assert "Latency" in result
        assert "Success" in result

    def test_legend_present(self):
        """Legend shows best/worst indicator."""
        cols = [SparklineColumn("Test", {"A": 1, "B": 2}, False)]
        data = SparklineTableData(["A", "B"], cols)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISparklineTable(data=data, options=opts)
        result = chart.render()
        assert "best" in result
        assert "worst" in result

    def test_no_color_output(self):
        """Chart renders without ANSI codes when color disabled."""
        cols = [SparklineColumn("Test", {"A": 1}, False)]
        data = SparklineTableData(["A"], cols)
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIISparklineTable(data=data, options=opts)
        result = chart.render()
        assert "\033[" not in result

    def test_long_platform_names_not_truncated_when_width_allows(self):
        """Long platform names remain fully visible when chart width allows it."""
        cols = [SparklineColumn("Total(ms)", {"DataFusion (df)": 100, "DataFusion (sql)": 110}, False)]
        data = SparklineTableData(["DataFusion (df)", "DataFusion (sql)"], cols)
        opts = ASCIIChartOptions(use_color=False, width=120)
        chart = ASCIISparklineTable(data=data, options=opts)
        result = chart.render()
        assert "DataFusion (df)" in result
        assert "DataFusion (sql)" in result
        assert "DataFusion (.." not in result

    def test_truncated_platform_names_are_disambiguated(self):
        """When truncation is required, colliding labels are made unique."""
        p1 = "VeryLongPlatformNameSharedPrefix-DataFusion-ModeA"
        p2 = "VeryLongPlatformNameSharedPrefix-DataFusion-ModeB"
        cols = [SparklineColumn("Total(ms)", {p1: 100, p2: 110}, False)]
        data = SparklineTableData([p1, p2], cols)
        opts = ASCIIChartOptions(use_color=False, width=40)
        chart = ASCIISparklineTable(data=data, options=opts)
        result = chart.render()
        assert "~1" in result
        assert "~2" in result

    def test_exporters_render_sparkline_table(self):
        """render_ascii_chart handles sparkline_table type."""
        from benchbox.core.visualization.exporters import render_ascii_chart

        cols = [SparklineColumn("Test", {"A": 1}, False)]
        data = SparklineTableData(["A"], cols)
        result = render_ascii_chart("sparkline_table", data, title="Test")
        assert "A" in result


# ── CDF Chart Tests ────────────────────────────────


class TestASCIICDFChart:
    """Tests for CDF chart rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        chart = ASCIICDFChart(data=[])
        result = chart.render()
        assert "No data" in result

    def test_empty_values(self):
        """Series with empty values returns message."""
        data = [CDFSeriesData("Empty", [])]
        chart = ASCIICDFChart(data=data)
        result = chart.render()
        assert "No data" in result

    def test_single_series(self):
        """Single series renders with markers."""
        data = [CDFSeriesData("DuckDB", [10, 20, 30, 50, 120])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIICDFChart(data=data, options=opts)
        result = chart.render()
        assert "DuckDB" in result
        assert "100%" in result
        assert "0%" in result

    def test_multi_series(self):
        """Multiple series render with different markers."""
        data = [
            CDFSeriesData("DuckDB", [10, 20, 30, 50, 120]),
            CDFSeriesData("Polars", [15, 25, 45, 80, 310]),
        ]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIICDFChart(data=data, options=opts)
        result = chart.render()
        assert "DuckDB" in result
        assert "Polars" in result
        assert "*" in result  # First series marker
        assert "+" in result  # Second series marker

    def test_identical_values(self):
        """Identical values render without crash."""
        data = [CDFSeriesData("Flat", [50, 50, 50, 50, 50])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIICDFChart(data=data, options=opts)
        result = chart.render()
        assert "Flat" in result

    def test_single_value(self):
        """Single value renders without crash."""
        data = [CDFSeriesData("Solo", [42])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIICDFChart(data=data, options=opts)
        result = chart.render()
        assert "Solo" in result

    def test_y_axis_fixed_0_100(self):
        """Y-axis is fixed at 0-100% range."""
        data = [CDFSeriesData("Test", [10, 20, 30])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIICDFChart(data=data, options=opts)
        result = chart.render()
        assert "100%" in result
        assert "0%" in result

    def test_no_color_output(self):
        """Chart renders without ANSI codes when color disabled."""
        data = [CDFSeriesData("Test", [10, 20, 30])]
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIICDFChart(data=data, options=opts)
        result = chart.render()
        assert "\033[" not in result

    def test_factory_function(self):
        """from_query_results factory creates CDF chart."""
        chart = cdf_from_query_results(
            [("DuckDB", [10, 20, 30])],
            options=ASCIIChartOptions(use_color=False),
        )
        result = chart.render()
        assert "DuckDB" in result

    def test_exporters_render_cdf_chart(self):
        """render_ascii_chart handles cdf_chart type."""
        from benchbox.core.visualization.exporters import render_ascii_chart

        data = [CDFSeriesData("Test", [10, 20, 30])]
        result = render_ascii_chart("cdf_chart", data, title="Test")
        assert "Test" in result


# ── Rank Table Tests ────────────────────────────────


class TestASCIIRankTable:
    """Tests for rank table chart rendering."""

    def test_empty_data(self):
        """Empty data returns message."""
        data = RankTableData([], [], {})
        chart = ASCIIRankTable(data=data)
        result = chart.render()
        assert "No data" in result

    def test_two_platforms_two_queries(self):
        """Basic 2x2 ranking renders."""
        data = RankTableData(
            ["Q1", "Q2"],
            ["DuckDB", "Polars"],
            {
                ("DuckDB", "Q1"): 10,
                ("Polars", "Q1"): 20,
                ("DuckDB", "Q2"): 30,
                ("Polars", "Q2"): 15,
            },
        )
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIRankTable(data=data, options=opts)
        result = chart.render()
        assert "DuckDB" in result
        assert "Polars" in result
        assert "1st" in result
        assert "2nd" in result
        assert "Wins" in result

    def test_tie_handling(self):
        """Tied platforms get the same rank."""
        data = RankTableData(
            ["Q1"],
            ["A", "B", "C"],
            {("A", "Q1"): 10, ("B", "Q1"): 10, ("C", "Q1"): 20},
        )
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIRankTable(data=data, options=opts)
        result = chart.render()
        # A and B should both be 1st, C should be 3rd (not 2nd)
        assert "1st" in result
        assert "3rd" in result

    def test_georank_computed(self):
        """Geometric mean rank is computed."""
        data = RankTableData(
            ["Q1", "Q2"],
            ["A", "B"],
            {("A", "Q1"): 10, ("B", "Q1"): 20, ("A", "Q2"): 30, ("B", "Q2"): 15},
        )
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIRankTable(data=data, options=opts)
        result = chart.render()
        assert "GeoRank" in result

    def test_natural_sort_order(self):
        """Queries are naturally sorted (Q1, Q2, Q10 not Q1, Q10, Q2)."""
        data = RankTableData(
            ["Q10", "Q2", "Q1"],
            ["A", "B"],
            {
                ("A", "Q1"): 10,
                ("B", "Q1"): 20,
                ("A", "Q2"): 10,
                ("B", "Q2"): 20,
                ("A", "Q10"): 10,
                ("B", "Q10"): 20,
            },
        )
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIRankTable(data=data, options=opts)
        result = chart.render()
        lines = result.split("\n")
        # Find data rows that start with Q and a digit (not header "Query", title, or separator)
        data_lines = [l for l in lines if l.strip() and l.strip()[0:2] in ("Q1", "Q2")]
        # Filter out title and header lines
        data_lines = [l for l in data_lines if "Ranking" not in l and "Query " not in l]
        assert len(data_lines) == 3
        # Check order is Q1, Q2, Q10 (natural sort)
        assert data_lines[0].strip().startswith("Q1 ")
        assert data_lines[1].strip().startswith("Q2 ")
        assert data_lines[2].strip().startswith("Q10")

    def test_win_counts(self):
        """Win counts are correctly tallied."""
        data = RankTableData(
            ["Q1", "Q2", "Q3"],
            ["A", "B"],
            {
                ("A", "Q1"): 10,
                ("B", "Q1"): 20,
                ("A", "Q2"): 20,
                ("B", "Q2"): 10,
                ("A", "Q3"): 10,
                ("B", "Q3"): 20,
            },
        )
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIRankTable(data=data, options=opts)
        result = chart.render()
        # A wins 2 queries, B wins 1
        lines = [l for l in result.split("\n") if "Wins" in l]
        assert len(lines) == 1
        assert "2" in lines[0]
        assert "1" in lines[0]

    def test_no_color_output(self):
        """Chart renders without ANSI codes when color disabled."""
        data = RankTableData(
            ["Q1"],
            ["A", "B"],
            {("A", "Q1"): 10, ("B", "Q1"): 20},
        )
        opts = ASCIIChartOptions(use_color=False)
        chart = ASCIIRankTable(data=data, options=opts)
        result = chart.render()
        assert "\033[" not in result

    def test_from_heatmap_data_factory(self):
        """from_heatmap_data creates rank table from matrix format."""
        chart = from_heatmap_data(
            [[10, 20], [30, 15]],
            ["Q1", "Q2"],
            ["DuckDB", "Polars"],
            options=ASCIIChartOptions(use_color=False),
        )
        result = chart.render()
        assert "DuckDB" in result
        assert "Polars" in result

    def test_exporters_render_rank_table(self):
        """render_ascii_chart handles rank_table type."""
        from benchbox.core.visualization.exporters import render_ascii_chart

        data = RankTableData(
            ["Q1"],
            ["A", "B"],
            {("A", "Q1"): 10, ("B", "Q1"): 20},
        )
        result = render_ascii_chart("rank_table", data, title="Test")
        assert "A" in result


# ── All New Charts Module Import Tests ────────────────────────────────


class TestNewChartsModuleImports:
    """Tests that all new chart types are importable from the ascii module."""

    def test_import_all_new_charts(self):
        """All new chart classes are importable from the ascii module."""
        from benchbox.core.visualization.ascii import (
            ASCIICDFChart,
            ASCIINormalizedSpeedup,
            ASCIIRankTable,
            ASCIISparklineTable,
            ASCIIStackedBar,
        )

        assert ASCIICDFChart is not None
        assert ASCIINormalizedSpeedup is not None
        assert ASCIIRankTable is not None
        assert ASCIISparklineTable is not None
        assert ASCIIStackedBar is not None


# ---------------------------------------------------------------------------
# Session regression tests: heatmap ordering, bar colors, outlier markers,
# box-plot scale capping, stats table, and centralized severity markers
# ---------------------------------------------------------------------------


class TestOutlierSeverityMarkers:
    """Unit tests for the centralised outlier_severity_markers() helper."""

    def test_value_at_or_below_scale_returns_empty(self):
        from benchbox.core.visualization.ascii.base import outlier_severity_markers

        assert outlier_severity_markers(100, 100) == ""
        assert outlier_severity_markers(50, 100) == ""

    def test_scale_zero_returns_empty(self):
        from benchbox.core.visualization.ascii.base import outlier_severity_markers

        assert outlier_severity_markers(10, 0) == ""

    @pytest.mark.parametrize(
        "value, scale_max, expected_count",
        [
            (150, 100, 1),  # 1.5× → 1 marker
            (200, 100, 1),  # 2× boundary → 1 marker
            (201, 100, 2),  # just over 2× → 2 markers
            (500, 100, 2),  # 5× boundary → 2 markers
            (501, 100, 3),  # just over 5× → 3 markers
            (1000, 100, 3),  # 10× boundary → 3 markers
            (1001, 100, 4),  # just over 10× → 4 markers
            (50000, 100, 4),  # extreme → 4 markers
        ],
    )
    def test_severity_thresholds(self, value, scale_max, expected_count):
        from benchbox.core.visualization.ascii.base import TRUNCATION_MARKER, outlier_severity_markers

        result = outlier_severity_markers(value, scale_max)
        assert result == TRUNCATION_MARKER * expected_count


class TestHeatmapQueryOrdering:
    """Verify _build_query_matrix sorts query IDs naturally."""

    def test_query_ids_sorted_numerically(self):
        """Query IDs like '14','2','9' should be sorted as 2, 9, 14."""
        from types import SimpleNamespace

        from benchbox.core.visualization.ascii_runtime import _build_query_matrix

        queries = [
            SimpleNamespace(query_id="14", execution_time_ms=100),
            SimpleNamespace(query_id="2", execution_time_ms=200),
            SimpleNamespace(query_id="9", execution_time_ms=150),
        ]
        result = SimpleNamespace(platform="DuckDB", queries=queries)
        _, query_ids, _ = _build_query_matrix([result])
        assert query_ids == ["2", "9", "14"]

    def test_query_ids_with_prefix_sorted(self):
        """Query IDs like 'Q2','Q10','Q1' should sort naturally."""
        from types import SimpleNamespace

        from benchbox.core.visualization.ascii_runtime import _build_query_matrix

        queries = [
            SimpleNamespace(query_id="Q10", execution_time_ms=10),
            SimpleNamespace(query_id="Q2", execution_time_ms=20),
            SimpleNamespace(query_id="Q1", execution_time_ms=30),
        ]
        result = SimpleNamespace(platform="DuckDB", queries=queries)
        _, query_ids, _ = _build_query_matrix([result])
        assert query_ids == ["Q1", "Q2", "Q10"]


class TestBarChartColorCycling:
    """Verify bars cycle through palette colours instead of 2-color scheme."""

    def test_ungrouped_bars_get_distinct_colors(self):
        """Each non-grouped bar should get a different palette colour."""
        import re

        data = [BarData(label=f"P{i}", value=(5 - i) * 100) for i in range(5)]
        opts = ASCIIChartOptions(use_color=True, use_unicode=True)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()

        # Match both 256-color (\x1b[38;5;Nm) and truecolor (\x1b[38;2;R;G;Bm) codes
        ansi_color_re = re.compile(r"\x1b\[38;[25];([\d;]+)m")
        colors_seen: set[str] = set()
        for line in result.split("\n"):
            for label in [f"P{i}" for i in range(5)]:
                if label in line:
                    matches = ansi_color_re.findall(line)
                    colors_seen.update(matches)

        # Should have more than 2 unique colours (the old behaviour)
        assert len(colors_seen) >= 3, f"Expected ≥3 colours, got {len(colors_seen)}: {colors_seen}"


class TestBarChartOutlierSeverityMarkers:
    """Verify bar chart truncation uses severity-scaled ▸ markers."""

    def test_extreme_outlier_shows_multiple_markers(self):
        """A bar 20× the scale should show 4 ▸ markers."""
        from benchbox.core.visualization.ascii.base import TRUNCATION_MARKER

        # 10 small bars + 1 extreme outlier to trigger truncation (needs >5 bars,
        # max > median*10 and max > p95*3)
        data = [BarData(label=f"Q{i}", value=10) for i in range(10)]
        data.append(BarData(label="Outlier", value=10000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()

        outlier_line = [l for l in result.split("\n") if "Outlier" in l]
        assert outlier_line, "Outlier bar not found"
        marker_count = outlier_line[0].count(TRUNCATION_MARKER)
        assert marker_count >= 2, f"Expected ≥2 severity markers, got {marker_count}"

    def test_duplicate_labels_do_not_inherit_outlier_truncation(self):
        """Non-outlier rows with the same label as an outlier keep their true bar length."""
        data = [BarData(label="dup", value=10) for _ in range(10)]
        data.append(BarData(label="dup", value=10000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True, width=70)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()

        dup_lines = [line for line in result.split("\n") if line.startswith("dup ")]
        assert len(dup_lines) == 11
        short_dup_lines = [line for line in dup_lines if " 10" in line and TRUNCATION_MARKER not in line]
        assert short_dup_lines, "Expected non-outlier duplicate-label rows without truncation markers"


class TestBoxPlotScaleCapping:
    """Verify box plot scale is capped at max_whisker × 1.5."""

    def test_extreme_outlier_does_not_dominate_scale(self):
        """With one extreme value, scale should be capped, not span full range."""
        series = [
            BoxPlotSeries(name="Normal", values=[10, 20, 30, 40, 50]),
            BoxPlotSeries(name="WithOutlier", values=[10, 20, 30, 40, 50, 10000]),
        ]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()

        # The axis max label should NOT be "10.0K" — it should be capped
        assert "10.0K" not in result, "Scale should be capped, not span to 10K"

    def test_no_capping_when_no_extreme_outliers(self):
        """Without extreme outliers, scale should reflect actual data range."""
        series = [BoxPlotSeries(name="A", values=[10, 20, 30, 40, 50])]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()
        assert "50" in result


class TestBoxPlotOutlierTruncationMarkers:
    """Verify box plot outliers beyond scale_max show severity ▸ markers."""

    def test_truncated_outlier_shows_marker(self):
        from benchbox.core.visualization.ascii.base import TRUNCATION_MARKER

        # Values where 10000 is an extreme outlier well beyond whisker×1.5
        series = [BoxPlotSeries(name="Test", values=[10, 20, 30, 40, 50, 10000])]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Truncated outlier should show ▸ marker"

    def test_severity_markers_form_contiguous_block(self):
        """Severity ▸ markers must not be interleaved with outlier o dots."""
        # Many outliers ensure some occupy rightmost positions
        values = list(range(10, 60)) + [5000, 6000, 7000, 8000, 9000, 10000]
        series = [BoxPlotSeries(name="Test", values=values)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True, width=80)
        chart = ASCIIBoxPlot(series=series, options=opts)
        result = chart.render()

        # Find the middle line (contains the label)
        mid_line = [l for l in result.split("\n") if "Test" in l]
        assert mid_line, "Box plot middle line not found"
        text = mid_line[0]
        # Extract the trailing marker region: everything after the last whisker end
        # The ▸ markers should be contiguous (no 'o' between them)
        marker_region = text[text.rfind(TRUNCATION_MARKER[0]) - 3 :] if TRUNCATION_MARKER in text else ""
        if marker_region:
            # Between the first ▸ and the end, there should be no 'o'
            first_marker = marker_region.index(TRUNCATION_MARKER)
            after_first = marker_region[first_marker:]
            assert "o" not in after_first, f"Outlier 'o' found within severity markers: {after_first!r}"

    def test_no_dead_line_before_stats_table(self):
        """There should be no blank line between axis label and stats table."""
        series = [BoxPlotSeries(name="A", values=[10, 20, 30, 40, 50])]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIBoxPlot(series=series, show_stats=True, options=opts)
        result = chart.render()

        lines = result.split("\n")
        # Find the axis label line (contains "→")
        axis_idx = next((i for i, l in enumerate(lines) if "→" in l), None)
        assert axis_idx is not None, "Axis label not found"
        # The next line should be the stats header, not blank
        assert lines[axis_idx + 1].strip() != "", "Blank line between axis label and stats table"


class TestBoxPlotSeriesSpacing:
    """Verify no dead vertical space between series."""

    def test_no_blank_line_between_series(self):
        """Adjacent series should not have blank lines between them."""
        series = [
            BoxPlotSeries(name="A", values=[10, 20, 30, 40, 50]),
            BoxPlotSeries(name="B", values=[15, 25, 35, 45, 55]),
        ]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIBoxPlot(series=series, show_stats=False, options=opts)
        result = chart.render()

        lines = result.split("\n")
        # Find lines with series labels
        label_indices = [i for i, l in enumerate(lines) if "A" in l.split()[0:1] or "B" in l.split()[0:1]]
        if len(label_indices) >= 2:
            # Between the bottom of series A (label_idx[0]+1) and top of series B
            # (label_idx[1]-1), there should be no blank line
            gap = label_indices[1] - label_indices[0]
            # Each series is 3 lines (top, mid, bottom), so gap should be exactly 3
            assert gap == 3, f"Expected 3-line gap between series labels, got {gap}"


class TestBoxPlotStatsTable:
    """Verify statistics are rendered as an aligned table."""

    def test_stats_table_has_header_and_separator(self):
        series = [BoxPlotSeries(name="Test", values=[10, 20, 30, 40, 50])]
        chart = ASCIIBoxPlot(series=series, show_stats=True)
        result = chart.render()

        assert "median" in result
        assert "mean" in result
        assert "std" in result
        # Separator line with ─
        assert "─" in result.split("median")[-1]

    def test_stats_table_uniform_decimals(self):
        """All values in a column should use the same decimal format."""
        series = [
            BoxPlotSeries(name="A", values=[10, 20, 30, 40, 50]),  # median=30.0
            BoxPlotSeries(name="B", values=[15, 25, 35, 45, 55]),  # median=35.0
        ]
        chart = ASCIIBoxPlot(series=series, show_stats=True)
        result = chart.render()

        # Both medians should have .0 suffix for consistency
        lines = result.split("\n")
        stat_lines = [l for l in lines if l.strip().startswith(("A", "B"))]
        for line in stat_lines:
            # Find numeric values — they should all have exactly one decimal place
            import re

            numbers = re.findall(r"\d+\.\d+", line)
            for num in numbers:
                decimal_places = len(num.split(".")[1])
                assert decimal_places == 1, f"Expected 1 decimal place, got {decimal_places} in '{num}'"

    def test_stats_table_k_suffix_for_large_values(self):
        """Large values should use K suffix uniformly in their column."""
        series = [
            BoxPlotSeries(name="A", values=[1000, 2000, 3000, 4000, 5000]),
            BoxPlotSeries(name="B", values=[1500, 2500, 3500, 4500, 5500]),
        ]
        chart = ASCIIBoxPlot(series=series, show_stats=True)
        result = chart.render()

        # All stat values should use K suffix since they're all ≥1000
        lines = result.split("\n")
        stat_lines = [l for l in lines if l.strip().startswith(("A", "B"))]
        for line in stat_lines:
            assert "K" in line, f"Expected K suffix in stats line: {line}"


class TestComparisonBarOutlierSeverityMarkers:
    """Verify comparison bar truncation uses severity-scaled ▸ markers."""

    def test_extreme_outlier_shows_severity_markers(self):
        from benchbox.core.visualization.ascii.base import TRUNCATION_MARKER
        from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData

        # One query with extreme baseline value to trigger truncation
        data = [
            ComparisonBarData(
                label="Q1", baseline_value=10, comparison_value=15, baseline_name="Old", comparison_name="New"
            ),
            ComparisonBarData(
                label="Q2", baseline_value=20, comparison_value=25, baseline_name="Old", comparison_name="New"
            ),
            ComparisonBarData(
                label="Q3", baseline_value=5000, comparison_value=12, baseline_name="Old", comparison_name="New"
            ),
        ]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIComparisonBar(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Truncated outlier bar should show ▸ marker"


# ---------------------------------------------------------------------------
# Histogram outlier truncation (scale capping)
# ---------------------------------------------------------------------------


class TestHistogramOutlierTruncation:
    """Verify histogram caps scale at IQR fence so outliers don't compress data."""

    def test_extreme_outlier_caps_scale(self):
        """With one extreme value, Y-axis max should not show the outlier's value."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=10 + i) for i in range(10)]
        data.append(HistogramBar(query_id="Q99", latency_ms=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()

        # The axis should NOT show 50K — it should be capped
        assert "50.0K" not in result, "Scale should be capped, not span to 50K"

    def test_extreme_outlier_shows_severity_markers(self):
        """Truncated histogram bar should show ▸ severity markers."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=10 + i) for i in range(10)]
        data.append(HistogramBar(query_id="Q99", latency_ms=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Truncated bar should show ▸ marker"

    def test_no_capping_without_extreme_outliers(self):
        """Uniform data should not trigger scale capping."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=10 + i * 2) for i in range(10)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result, "No truncation expected for uniform data"

    def test_grouped_histogram_outlier_truncation(self):
        """Multi-platform histogram should also cap scale and show markers."""
        data = []
        for plat in ["DuckDB", "Polars"]:
            for i in range(6):
                data.append(HistogramBar(query_id=f"Q{i}", latency_ms=10 + i, platform=plat))
        data.append(HistogramBar(query_id="Q99", latency_ms=50000, platform="DuckDB"))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Grouped histogram should show truncation markers"

    def test_footer_shows_truncated_legend(self):
        """Footer should include a 'Truncated' legend entry when scale is capped."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=10 + i) for i in range(10)]
        data.append(HistogramBar(query_id="Q99", latency_ms=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()

        assert "Truncated" in result, "Footer should mention 'Truncated'"


# ---------------------------------------------------------------------------
# Heatmap outlier truncation (P95 capping)
# ---------------------------------------------------------------------------


class TestHeatmapOutlierTruncation:
    """Verify heatmap caps color scale at P95×2 for extreme outliers."""

    def test_extreme_outlier_shows_truncation_marker(self):
        """Cells exceeding P95×2 should have ▸ appended to their value."""
        # 9 normal values + 1 extreme outlier
        row_labels = [f"Q{i}" for i in range(10)]
        col_labels = ["Platform"]
        matrix = [[10 + i] for i in range(9)]
        matrix.append([50000])  # extreme outlier
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=row_labels, col_labels=col_labels, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Outlier cell should show ▸ marker"

    def test_range_footer_shows_capping_info(self):
        """Range footer should mention scale capping when truncation is active."""
        row_labels = [f"Q{i}" for i in range(10)]
        col_labels = ["Platform"]
        matrix = [[10 + i] for i in range(9)]
        matrix.append([50000])
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=row_labels, col_labels=col_labels, options=opts)
        result = chart.render()

        assert "capped" in result.lower(), "Footer should mention scale capping"

    def test_no_capping_without_outliers(self):
        """Uniform data should not trigger capping or truncation markers."""
        row_labels = [f"Q{i}" for i in range(10)]
        col_labels = ["Platform"]
        matrix = [[10 + i * 2] for i in range(10)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=row_labels, col_labels=col_labels, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result
        assert "capped" not in result.lower()

    def test_heatmap_with_color_truncation(self):
        """Heatmap with color should also show truncation markers."""
        row_labels = [f"Q{i}" for i in range(10)]
        col_labels = ["Platform"]
        matrix = [[10 + i] for i in range(9)]
        matrix.append([50000])
        opts = ASCIIChartOptions(use_color=True, use_unicode=True)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=row_labels, col_labels=col_labels, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result

    def test_zero_heavy_matrix_still_caps_outlier_scale(self):
        """Sparse positive baseline should still allow capping an extreme outlier."""
        row_labels = [f"Q{i}" for i in range(20)]
        col_labels = ["Platform"]
        matrix = [[0.0] for _ in range(18)] + [[10.0], [50000.0]]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=row_labels, col_labels=col_labels, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result
        assert "capped" in result.lower()

    def test_single_positive_zero_heavy_matrix_has_no_false_truncation(self):
        """One positive value among zeros should not be marked as truncated."""
        row_labels = [f"Q{i}" for i in range(20)]
        col_labels = ["Platform"]
        matrix = [[0.0] for _ in range(19)] + [[10.0]]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIHeatmap(matrix=matrix, row_labels=row_labels, col_labels=col_labels, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result
        assert "capped" not in result.lower()


# ---------------------------------------------------------------------------
# Stacked bar outlier truncation (P95 capping)
# ---------------------------------------------------------------------------


class TestStackedBarOutlierTruncation:
    """Verify stacked bar caps scale when extreme totals compress other bars."""

    def test_extreme_outlier_shows_severity_markers(self):
        """A bar with extreme total should show ▸ severity markers."""
        data = [
            StackedBarData(
                label=f"P{i}",
                segments=[StackedBarSegment(phase_name="Load", value=10 + i)],
            )
            for i in range(10)
        ]
        data.append(
            StackedBarData(
                label="Outlier",
                segments=[StackedBarSegment(phase_name="Load", value=50000)],
            )
        )
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Truncated bar should show ▸ marker"

    def test_no_capping_without_extreme_outliers(self):
        """Uniform totals should not trigger truncation."""
        data = [
            StackedBarData(
                label=f"P{i}",
                segments=[StackedBarSegment(phase_name="Load", value=10 + i * 2)],
            )
            for i in range(10)
        ]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result

    def test_outlier_bar_still_shows_correct_total(self):
        """The total annotation should show the actual (uncapped) value."""
        data = [
            StackedBarData(
                label=f"P{i}",
                segments=[StackedBarSegment(phase_name="Load", value=10)],
            )
            for i in range(10)
        ]
        data.append(
            StackedBarData(
                label="Outlier",
                segments=[StackedBarSegment(phase_name="Load", value=60000)],
            )
        )
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()

        # The total annotation should still show the real value (1.0min or 60.0s)
        assert "1.0min" in result or "60.0s" in result, "Total should show actual value"

    def test_duplicate_labels_do_not_inherit_outlier_truncation(self):
        """Truncation must be determined per row total, not by platform label text."""
        data = [
            StackedBarData(
                label="dup",
                segments=[StackedBarSegment(phase_name="Load", value=10)],
            )
            for _ in range(10)
        ]
        data.append(
            StackedBarData(
                label="dup",
                segments=[StackedBarSegment(phase_name="Load", value=10000)],
            )
        )
        opts = ASCIIChartOptions(use_color=False, use_unicode=True, width=70)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()

        dup_lines = [line for line in result.split("\n") if line.startswith("dup ")]
        assert len(dup_lines) == 11
        short_dup_lines = [line for line in dup_lines if "10ms" in line and TRUNCATION_MARKER not in line]
        assert short_dup_lines, "Expected non-outlier duplicate-label rows without truncation markers"


# ---------------------------------------------------------------------------
# Scatter plot outlier truncation (P95 axis capping)
# ---------------------------------------------------------------------------


class TestScatterPlotOutlierTruncation:
    """Verify scatter plot caps axes when one extreme point wastes plot area."""

    def test_extreme_outlier_caps_axis(self):
        """With one extreme x value, axis labels should not span to that value."""
        points = [ScatterPoint(name=f"P{i}", x=10 + i, y=100 + i) for i in range(10)]
        points.append(ScatterPoint(name="Extreme", x=50000, y=150))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIScatterPlot(points=points, options=opts)
        result = chart.render()

        # The axis labels (before "Points:" section) should not show 50K
        axis_section = result.split("Points:")[0] if "Points:" in result else result
        assert "50.0K" not in axis_section, "X-axis should be capped, not span to 50K"

    def test_truncated_point_shows_marker_in_legend(self):
        """Legend should show ▸ for truncated points."""
        points = [ScatterPoint(name=f"P{i}", x=10 + i, y=100 + i) for i in range(10)]
        points.append(ScatterPoint(name="Extreme", x=50000, y=150))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIScatterPlot(points=points, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Truncated point should show ▸ in legend"

    def test_no_capping_without_outliers(self):
        """Uniform data should not trigger axis capping."""
        points = [ScatterPoint(name=f"P{i}", x=10 + i * 5, y=100 + i * 10) for i in range(10)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIScatterPlot(points=points, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result

    def test_y_axis_outlier_capping(self):
        """Extreme y value should also be capped."""
        points = [ScatterPoint(name=f"P{i}", x=10 + i, y=100 + i) for i in range(10)]
        points.append(ScatterPoint(name="Extreme", x=15, y=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIScatterPlot(points=points, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result

    def test_zero_heavy_points_still_trigger_capping(self):
        """Sparse positive baseline should still allow capping an extreme outlier."""
        points = [ScatterPoint(name=f"P{i}", x=0.0, y=0.0) for i in range(18)]
        points.append(ScatterPoint(name="P18", x=10.0, y=10.0))
        points.append(ScatterPoint(name="Outlier", x=50000.0, y=50000.0))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIScatterPlot(points=points, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result

    def test_single_positive_zero_heavy_points_have_no_false_truncation(self):
        """One positive point among zeros should not be marked truncated."""
        points = [ScatterPoint(name=f"P{i}", x=0.0, y=0.0) for i in range(19)]
        points.append(ScatterPoint(name="P19", x=10.0, y=10.0))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIScatterPlot(points=points, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result


# ---------------------------------------------------------------------------
# Line chart outlier truncation (Y-axis capping)
# ---------------------------------------------------------------------------


class TestLineChartOutlierTruncation:
    """Verify line chart caps y-axis when one spike compresses all series."""

    def test_extreme_spike_caps_y_axis(self):
        """With one extreme y value, y-axis should not show that value."""
        points = [LinePoint(series="A", x=i, y=10 + i) for i in range(10)]
        points.append(LinePoint(series="A", x=10, y=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIILineChart(points=points, options=opts)
        result = chart.render()

        assert "50.0K" not in result, "Y-axis should be capped"

    def test_capped_shows_truncation_note(self):
        """When y-axis is capped, a note should appear."""
        points = [LinePoint(series="A", x=i, y=10 + i) for i in range(10)]
        points.append(LinePoint(series="A", x=10, y=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIILineChart(points=points, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Should show truncation note"
        assert "capped" in result.lower(), "Should mention capping"

    def test_no_capping_without_spikes(self):
        """Uniform data should not trigger y-axis capping."""
        points = [LinePoint(series="A", x=i, y=10 + i * 2) for i in range(10)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIILineChart(points=points, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result

    def test_zero_heavy_series_still_caps_y_axis(self):
        """Sparse positive baseline should still allow capping an extreme outlier."""
        points = [LinePoint(series="A", x=i, y=0.0) for i in range(18)]
        points.append(LinePoint(series="A", x=18, y=10.0))
        points.append(LinePoint(series="A", x=19, y=50000.0))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIILineChart(points=points, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result
        assert "capped" in result.lower()


# ---------------------------------------------------------------------------
# CDF chart outlier truncation (X-axis capping)
# ---------------------------------------------------------------------------


class TestCDFChartOutlierTruncation:
    """Verify CDF chart caps x-axis when extreme tail bunches all data left."""

    def test_extreme_tail_caps_x_axis(self):
        """With one extreme value, x-axis should be capped."""
        values = list(range(10, 30))  # 20 normal values
        values.append(50000)  # extreme outlier
        data = [CDFSeriesData(name="Platform", values=values)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIICDFChart(data=data, options=opts)
        result = chart.render()

        assert "50.0K" not in result, "X-axis should be capped"

    def test_capped_shows_truncation_marker(self):
        """Legend should include truncation marker when x-axis is capped."""
        values = list(range(10, 30))
        values.append(50000)
        data = [CDFSeriesData(name="Platform", values=values)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIICDFChart(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result

    def test_no_capping_without_extreme_tail(self):
        """Uniform data should not trigger x-axis capping."""
        values = list(range(10, 30))
        data = [CDFSeriesData(name="Platform", values=values)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIICDFChart(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result

    def test_zero_heavy_distribution_still_caps_x_axis(self):
        """Sparse positive baseline should still allow capping an extreme tail value."""
        values = [0.0] * 18 + [10.0, 50000.0]
        data = [CDFSeriesData(name="Platform", values=values)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIICDFChart(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result


# ---------------------------------------------------------------------------
# Percentile ladder outlier truncation (P99 capping)
# ---------------------------------------------------------------------------


class TestPercentileLadderOutlierTruncation:
    """Verify percentile ladder caps scale when one extreme P99 compresses others."""

    def test_extreme_p99_shows_severity_markers(self):
        """A platform with extreme P99 should show ▸ severity markers."""
        data = [PercentileData(name=f"P{i}", p50=10 + i, p90=20 + i, p95=30 + i, p99=40 + i) for i in range(10)]
        data.append(PercentileData(name="Outlier", p50=15, p90=25, p95=35, p99=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Truncated bar should show ▸ marker"

    def test_no_capping_without_extreme_p99(self):
        """Uniform P99 values should not trigger truncation."""
        data = [PercentileData(name=f"P{i}", p50=10 + i, p90=20 + i, p95=30 + i, p99=40 + i * 2) for i in range(10)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result

    def test_annotation_shows_actual_values(self):
        """The annotation should still show the real P99 value, not capped."""
        data = [PercentileData(name=f"P{i}", p50=10, p90=20, p95=30, p99=40) for i in range(10)]
        data.append(PercentileData(name="Outlier", p50=15, p90=25, p95=35, p99=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()

        assert "50000" in result, "Annotation should show actual P99 value"

    def test_zero_heavy_p99_still_truncates_outlier(self):
        """Sparse positive baseline should still allow capping an extreme P99."""
        data = [PercentileData(name=f"P{i}", p50=0, p90=0, p95=0, p99=0) for i in range(8)]
        data.append(PercentileData(name="P8", p50=0, p90=0, p95=0, p99=10))
        data.append(PercentileData(name="Outlier", p50=0, p90=0, p95=0, p99=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result

    def test_duplicate_labels_do_not_inherit_outlier_truncation(self):
        """Duplicate names should not cause non-outlier rows to render as truncated."""
        data = [PercentileData(name="dup", p50=1, p90=2, p95=3, p99=10) for _ in range(10)]
        data.append(PercentileData(name="dup", p50=5, p90=9, p95=10, p99=10000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True, width=80)
        chart = ASCIIPercentileLadder(data=data, options=opts)
        result = chart.render()

        dup_lines = [line for line in result.split("\n") if line.startswith("dup ")]
        assert len(dup_lines) == 11
        non_outlier_lines = [line for line in dup_lines if " |    10.0" in line and TRUNCATION_MARKER not in line]
        assert non_outlier_lines, "Expected duplicate non-outlier rows without truncation markers"


# ---------------------------------------------------------------------------
# Bar chart zero-heavy outlier truncation
# ---------------------------------------------------------------------------


class TestBarChartZeroHeavyTruncation:
    """Verify bar chart handles zero-heavy distributions with outlier capping."""

    def test_zero_heavy_bars_still_trigger_capping(self):
        """Sparse positive baseline plus one extreme outlier should truncate."""
        data = [BarData(label=f"Q{i}", value=0) for i in range(18)]
        data.append(BarData(label="Q18", value=10))
        data.append(BarData(label="Outlier", value=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Zero-heavy bar chart should still cap and truncate outlier"

    def test_normal_data_no_false_positive(self):
        """Uniform non-zero data should not trigger truncation."""
        data = [BarData(label=f"Q{i}", value=10 + i * 2) for i in range(10)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIBarChart(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result


# ---------------------------------------------------------------------------
# Histogram zero-heavy outlier truncation
# ---------------------------------------------------------------------------


class TestHistogramZeroHeavyTruncation:
    """Verify histogram handles zero-heavy distributions with IQR fallback."""

    def test_zero_heavy_latencies_still_trigger_capping(self):
        """Sparse positive baseline plus one extreme outlier should truncate."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=0) for i in range(18)]
        data.append(HistogramBar(query_id="Q18", latency_ms=10))
        data.append(HistogramBar(query_id="Q99", latency_ms=50000))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Zero-heavy histogram should still cap and truncate outlier"

    def test_normal_data_no_false_positive(self):
        """Uniform non-zero latencies should not trigger false truncation."""
        data = [HistogramBar(query_id=f"Q{i}", latency_ms=10 + i * 2) for i in range(10)]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIQueryHistogram(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result


# ---------------------------------------------------------------------------
# Stacked bar zero-heavy outlier truncation
# ---------------------------------------------------------------------------


class TestStackedBarZeroHeavyTruncation:
    """Verify stacked bar handles zero-heavy distributions with outlier capping."""

    def test_zero_heavy_totals_still_trigger_capping(self):
        """Sparse positive baseline plus one extreme outlier should truncate."""
        data = [
            StackedBarData(label=f"P{i}", segments=[StackedBarSegment(phase_name="Load", value=0)]) for i in range(19)
        ]
        data[-1] = StackedBarData(label="P18", segments=[StackedBarSegment(phase_name="Load", value=10)])
        data.append(StackedBarData(label="Outlier", segments=[StackedBarSegment(phase_name="Load", value=50000)]))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER in result, "Zero-heavy stacked bar should still cap and truncate outlier"

    def test_normal_data_no_false_positive(self):
        """Uniform non-zero totals should not trigger truncation."""
        data = [
            StackedBarData(label=f"P{i}", segments=[StackedBarSegment(phase_name="Load", value=10 + i * 2)])
            for i in range(10)
        ]
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIStackedBar(data=data, options=opts)
        result = chart.render()

        assert TRUNCATION_MARKER not in result


# ---------------------------------------------------------------------------
# Scatter plot duplicate-label truncation
# ---------------------------------------------------------------------------


class TestScatterPlotDuplicateLabelTruncation:
    """Verify scatter plot uses value-based truncation, not name-based."""

    def test_duplicate_names_do_not_inherit_outlier_truncation(self):
        """Non-outlier points with same name as an outlier should not show truncation marker."""
        points = [ScatterPoint(name="dup", x=10 + i, y=100 + i) for i in range(10)]
        points.append(ScatterPoint(name="dup", x=50000, y=150))
        opts = ASCIIChartOptions(use_color=False, use_unicode=True)
        chart = ASCIIScatterPlot(points=points, options=opts)
        result = chart.render()

        dup_lines = [line for line in result.split("\n") if "dup:" in line]
        truncated = [line for line in dup_lines if TRUNCATION_MARKER in line]
        non_truncated = [line for line in dup_lines if TRUNCATION_MARKER not in line]
        assert len(truncated) == 1, f"Expected exactly 1 truncated dup line, got {len(truncated)}"
        assert len(non_truncated) == 10, f"Expected 10 non-truncated dup lines, got {len(non_truncated)}"


class TestPowerBarRenderer:
    """Tests for _render_power_bar in ascii_runtime."""

    def test_renders_with_valid_power_data(self):
        """power_bar renders a bar per platform when power_at_size is present."""
        from benchbox.core.visualization.ascii.base import ASCIIChartOptions
        from benchbox.core.visualization.ascii_runtime import render_ascii_chart_from_results

        results = [
            make_normalized_result(platform="DuckDB 1.0.0", benchmark="tpcds", scale_factor=10, power_at_size=385807.0),
            make_normalized_result(platform="DuckDB 1.5.0", benchmark="tpcds", scale_factor=10, power_at_size=669328.0),
        ]
        opts = ASCIIChartOptions(use_color=False)
        output = render_ascii_chart_from_results(results, "power_bar", opts, {})
        assert output is not None
        assert "DuckDB 1.0.0" in output
        assert "DuckDB 1.5.0" in output
        assert "Power@Size" in output

    def test_returns_none_when_no_power_data(self):
        """power_bar returns None when no results carry power_at_size."""
        from benchbox.core.visualization.ascii.base import ASCIIChartOptions
        from benchbox.core.visualization.ascii_runtime import render_ascii_chart_from_results

        results = [
            make_normalized_result(platform="DuckDB", benchmark="tpcds", scale_factor=10),
            make_normalized_result(platform="Polars", benchmark="tpcds", scale_factor=10),
        ]
        opts = ASCIIChartOptions(use_color=False)
        output = render_ascii_chart_from_results(results, "power_bar", opts, {})
        assert output is None

    def test_best_is_highest_value(self):
        """The result with the highest Power@Size is marked is_best."""
        from benchbox.core.visualization.ascii.bar_chart import BarData
        from benchbox.core.visualization.ascii.base import ASCIIChartOptions
        from benchbox.core.visualization.ascii_runtime import _render_power_bar

        captured: list[BarData] = []

        class _CapturingBarChart:
            def __init__(self, data, **kwargs):
                captured.extend(data)

            def render(self):
                return ""

        import benchbox.core.visualization.ascii_runtime as runtime

        original = runtime.ASCIIBarChart
        runtime.ASCIIBarChart = _CapturingBarChart  # type: ignore[assignment]
        try:
            results = [
                make_normalized_result(platform="Slow", benchmark="tpcds", scale_factor=10, power_at_size=100.0),
                make_normalized_result(platform="Fast", benchmark="tpcds", scale_factor=10, power_at_size=900.0),
                make_normalized_result(platform="Mid", benchmark="tpcds", scale_factor=10, power_at_size=500.0),
            ]
            _render_power_bar(results, ASCIIChartOptions(use_color=False), {})
        finally:
            runtime.ASCIIBarChart = original  # type: ignore[assignment]

        best = [d for d in captured if d.is_best]
        worst = [d for d in captured if d.is_worst]
        assert len(best) == 1 and best[0].label == "Fast"
        assert len(worst) == 1 and worst[0].label == "Slow"

    def test_single_result_no_worst(self):
        """With a single result, is_worst is not set (nothing to compare against)."""
        from benchbox.core.visualization.ascii.bar_chart import BarData
        from benchbox.core.visualization.ascii.base import ASCIIChartOptions
        from benchbox.core.visualization.ascii_runtime import _render_power_bar

        captured: list[BarData] = []

        class _CapturingBarChart:
            def __init__(self, data, **kwargs):
                captured.extend(data)

            def render(self):
                return ""

        import benchbox.core.visualization.ascii_runtime as runtime

        original = runtime.ASCIIBarChart
        runtime.ASCIIBarChart = _CapturingBarChart  # type: ignore[assignment]
        try:
            _render_power_bar(
                [make_normalized_result(platform="Only", benchmark="tpcds", scale_factor=10, power_at_size=500.0)],
                ASCIIChartOptions(use_color=False),
                {},
            )
        finally:
            runtime.ASCIIBarChart = original  # type: ignore[assignment]

        assert captured[0].is_best is True
        assert not any(d.is_worst for d in captured)

    def test_mixed_results_only_power_data_rendered(self):
        """Results without power_at_size are silently excluded from the chart."""
        from benchbox.core.visualization.ascii.base import ASCIIChartOptions
        from benchbox.core.visualization.ascii_runtime import render_ascii_chart_from_results

        results = [
            make_normalized_result(platform="HasPower", benchmark="tpcds", scale_factor=10, power_at_size=500.0),
            make_normalized_result(platform="NoPower", benchmark="tpcds", scale_factor=10),
        ]
        opts = ASCIIChartOptions(use_color=False)
        output = render_ascii_chart_from_results(results, "power_bar", opts, {})
        assert output is not None
        assert "HasPower" in output
        assert "NoPower" not in output


class TestNormalizeDictPowerAtSize:
    """Tests for power_at_size extraction in ResultPlotter._normalize_dict."""

    @staticmethod
    def _make_payload(power_at_size=None):
        payload = {
            "benchmark": {"name": "tpcds", "scale_factor": 10},
            "execution": {"platform": "duckdb"},
            "results": {"timing": {"total_ms": 5000}},
        }
        if power_at_size is not None:
            payload["summary"] = {"tpc_metrics": {"power_at_size": power_at_size}}
        return payload

    def test_extracts_power_at_size_from_tpc_metrics(self):
        """_normalize_dict populates power_at_size from summary.tpc_metrics."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        result = ResultPlotter._normalize_dict(self._make_payload(power_at_size=385807.0), source_path=None)
        assert result.power_at_size == pytest.approx(385807.0)

    def test_power_at_size_none_when_absent(self):
        """_normalize_dict sets power_at_size to None when the field is missing."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        result = ResultPlotter._normalize_dict(self._make_payload(), source_path=None)
        assert result.power_at_size is None

    def test_power_at_size_coerced_to_float(self):
        """Integer power_at_size values in JSON are coerced to float."""
        from benchbox.core.visualization.result_plotter import ResultPlotter

        result = ResultPlotter._normalize_dict(self._make_payload(power_at_size=385807), source_path=None)
        assert isinstance(result.power_at_size, float)


class TestSuggestChartTypesPowerBar:
    """Tests for power_bar inclusion in _suggest_chart_types."""

    @staticmethod
    def _make_plotter(power_values: list[float | None]):
        from benchbox.core.visualization.result_plotter import NormalizedResult, ResultPlotter

        results = [
            NormalizedResult(
                benchmark="tpcds",
                platform=f"platform_{i}",
                scale_factor=10,
                execution_id=None,
                timestamp=None,
                total_time_ms=1000.0,
                avg_time_ms=None,
                success_rate=None,
                cost_total=None,
                power_at_size=v,
            )
            for i, v in enumerate(power_values)
        ]
        plotter = ResultPlotter.__new__(ResultPlotter)
        plotter.results = results
        return plotter

    def test_power_bar_suggested_when_power_data_present(self):
        plotter = self._make_plotter([385807.0, 669328.0])
        assert "power_bar" in plotter._suggest_chart_types()

    def test_power_bar_not_suggested_without_power_data(self):
        plotter = self._make_plotter([None, None])
        assert "power_bar" not in plotter._suggest_chart_types()

    def test_power_bar_suggested_when_at_least_one_has_power(self):
        """Partial power data is enough to suggest the chart."""
        plotter = self._make_plotter([None, 500.0])
        assert "power_bar" in plotter._suggest_chart_types()


class TestRobustP95Fallback:
    """Regression tests for robust_p95 zero-heavy behavior."""

    def test_single_positive_does_not_artificially_shrink_p95(self):
        """One positive value among zeros should keep p95 at that value."""
        from benchbox.core.visualization.ascii.base import robust_p95

        vals = [0.0] * 19 + [10.0]
        assert robust_p95(vals) == 10.0

    def test_sparse_positive_tail_uses_positive_rank(self):
        """With sparse positives, p95 should come from positive-tail nearest rank."""
        from benchbox.core.visualization.ascii.base import robust_p95

        vals = [0.0] * 18 + [10.0, 50000.0]
        assert robust_p95(vals) == 10.0
