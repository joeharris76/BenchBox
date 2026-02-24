"""Tests for no-color (greyscale) rendering across all ASCII chart types.

Verifies that when use_color=False, charts produce readable output with
structural differentiation (distinct fill patterns, series markers, arrows)
instead of relying on ANSI color codes.
"""

from __future__ import annotations

import re

import pytest

from benchbox.core.visualization.ascii.bar_chart import ASCIIBarChart, BarData
from benchbox.core.visualization.ascii.base import (
    ASCII_FILL_PATTERNS,
    ASCII_SERIES_MARKERS,
    FILL_PATTERNS,
    SERIES_MARKERS,
    ASCIIChartOptions,
)
from benchbox.core.visualization.ascii.box_plot import ASCIIBoxPlot, BoxPlotSeries
from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar, ComparisonBarData
from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar, DivergingBarData
from benchbox.core.visualization.ascii.heatmap import ASCIIHeatmap, from_matrix
from benchbox.core.visualization.ascii.histogram import ASCIIQueryHistogram, HistogramBar
from benchbox.core.visualization.ascii.sparkline_table import ASCIISparklineTable, SparklineColumn, SparklineTableData
from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox, SummaryStats

pytestmark = pytest.mark.fast

# Regex to detect ANSI escape codes
ANSI_RE = re.compile(r"\033\[[0-9;]*[A-Za-z]")

NO_COLOR_OPTS = ASCIIChartOptions(use_color=False, width=80)
NO_COLOR_ASCII_OPTS = ASCIIChartOptions(use_color=False, use_unicode=False, width=80)


class TestPalettes:
    """Verify fill pattern and series marker palettes have sufficient unique entries."""

    def test_fill_patterns_has_8_unique(self):
        assert len(set(FILL_PATTERNS)) >= 8

    def test_series_markers_has_8_unique(self):
        assert len(set(SERIES_MARKERS)) >= 8

    def test_ascii_fill_patterns_has_8_unique(self):
        assert len(set(ASCII_FILL_PATTERNS)) >= 8

    def test_ascii_series_markers_has_8_unique(self):
        assert len(set(ASCII_SERIES_MARKERS)) >= 8

    def test_fill_patterns_differ_from_each_other(self):
        """Each fill pattern entry is distinct from adjacent entries."""
        for i in range(len(FILL_PATTERNS) - 1):
            assert FILL_PATTERNS[i] != FILL_PATTERNS[i + 1]


class TestSeriesHelpers:
    """Verify get_series_fill and get_series_marker return correct values."""

    def test_series_fill_returns_uniform_when_color_on(self):
        opts = ASCIIChartOptions(use_color=True)
        assert opts.get_series_fill(0) == opts.get_series_fill(1)

    def test_series_fill_returns_distinct_when_color_off(self):
        opts = ASCIIChartOptions(use_color=False)
        assert opts.get_series_fill(0) != opts.get_series_fill(1)

    def test_series_marker_returns_uniform_when_color_on(self):
        opts = ASCIIChartOptions(use_color=True)
        assert opts.get_series_marker(0) == opts.get_series_marker(1)

    def test_series_marker_returns_distinct_when_color_off(self):
        opts = ASCIIChartOptions(use_color=False)
        assert opts.get_series_marker(0) != opts.get_series_marker(1)

    def test_series_fill_wraps_around(self):
        opts = ASCIIChartOptions(use_color=False)
        n = len(FILL_PATTERNS)
        assert opts.get_series_fill(0) == opts.get_series_fill(n)


def _assert_no_ansi(output: str) -> None:
    """Assert that output contains no ANSI escape codes."""
    matches = ANSI_RE.findall(output)
    assert not matches, f"Found ANSI codes in output: {matches[:5]}"


class TestBarChartNoColor:
    """ASCIIBarChart uses distinct fill patterns when color is off."""

    def test_no_ansi_codes(self):
        data = [BarData(label="A", value=100), BarData(label="B", value=200)]
        chart = ASCIIBarChart(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        _assert_no_ansi(output)

    def test_grouped_bars_use_distinct_fills(self):
        data = [
            BarData(label="Q1", value=100, group="DuckDB"),
            BarData(label="Q2", value=200, group="DuckDB"),
            BarData(label="Q3", value=150, group="Polars"),
            BarData(label="Q4", value=180, group="Polars"),
        ]
        chart = ASCIIBarChart(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        _assert_no_ansi(output)
        # The two groups should use different fill characters
        fill_0 = NO_COLOR_OPTS.get_series_fill(0)
        fill_1 = NO_COLOR_OPTS.get_series_fill(1)
        assert fill_0 in output
        assert fill_1 in output
        assert fill_0 != fill_1

    def test_renders_non_empty(self):
        data = [BarData(label="X", value=50)]
        chart = ASCIIBarChart(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        assert len(output) > 10


class TestComparisonBarNoColor:
    """ASCIIComparisonBar uses distinct fills and structural arrows."""

    def _make_data(self):
        return [
            ComparisonBarData(
                label="Q1",
                baseline_value=100,
                comparison_value=150,
                baseline_name="SQL",
                comparison_name="DF",
            ),
            ComparisonBarData(
                label="Q2",
                baseline_value=200,
                comparison_value=120,
                baseline_name="SQL",
                comparison_name="DF",
            ),
        ]

    def test_no_ansi_codes(self):
        chart = ASCIIComparisonBar(data=self._make_data(), options=NO_COLOR_OPTS)
        output = chart.render()
        _assert_no_ansi(output)

    def test_distinct_fill_patterns(self):
        chart = ASCIIComparisonBar(data=self._make_data(), options=NO_COLOR_OPTS)
        output = chart.render()
        fill_0 = NO_COLOR_OPTS.get_series_fill(0)
        fill_1 = NO_COLOR_OPTS.get_series_fill(1)
        assert fill_0 in output
        assert fill_1 in output

    def test_distinct_legend_markers(self):
        chart = ASCIIComparisonBar(data=self._make_data(), options=NO_COLOR_OPTS)
        output = chart.render()
        marker_0 = NO_COLOR_OPTS.get_series_marker(0)
        marker_1 = NO_COLOR_OPTS.get_series_marker(1)
        assert marker_0 in output
        assert marker_1 in output

    def test_regression_arrow_present(self):
        chart = ASCIIComparisonBar(data=self._make_data(), options=NO_COLOR_OPTS)
        output = chart.render()
        # Q1 has regression (+50%), should have up arrow
        assert "\u2191" in output or "^" in output

    def test_improvement_arrow_present(self):
        chart = ASCIIComparisonBar(data=self._make_data(), options=NO_COLOR_OPTS)
        output = chart.render()
        # Q2 has improvement (-40%), should have down arrow
        assert "\u2193" in output or "v" in output


class TestHistogramNoColor:
    """ASCIIQueryHistogram uses distinct markers and fills in no-color mode."""

    def test_no_ansi_codes_simple(self):
        data = [HistogramBar(query_id="Q1", latency_ms=100), HistogramBar(query_id="Q2", latency_ms=200)]
        chart = ASCIIQueryHistogram(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        _assert_no_ansi(output)

    def test_no_ansi_codes_grouped(self):
        data = [
            HistogramBar(query_id="Q1", latency_ms=100, platform="DuckDB"),
            HistogramBar(query_id="Q1", latency_ms=150, platform="Polars"),
            HistogramBar(query_id="Q2", latency_ms=200, platform="DuckDB"),
            HistogramBar(query_id="Q2", latency_ms=180, platform="Polars"),
        ]
        chart = ASCIIQueryHistogram(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        _assert_no_ansi(output)

    def test_grouped_distinct_fills(self):
        data = [
            HistogramBar(query_id="Q1", latency_ms=100, platform="DuckDB"),
            HistogramBar(query_id="Q1", latency_ms=150, platform="Polars"),
        ]
        chart = ASCIIQueryHistogram(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        fill_0 = NO_COLOR_OPTS.get_series_fill(0)
        fill_1 = NO_COLOR_OPTS.get_series_fill(1)
        assert fill_0 in output
        assert fill_1 in output

    def test_grouped_legend_uses_series_fills(self):
        data = [
            HistogramBar(query_id="Q1", latency_ms=100, platform="DuckDB"),
            HistogramBar(query_id="Q1", latency_ms=150, platform="Polars"),
        ]
        chart = ASCIIQueryHistogram(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        fill_0 = NO_COLOR_OPTS.get_series_fill(0)
        fill_1 = NO_COLOR_OPTS.get_series_fill(1)
        assert f"{fill_0} DuckDB" in output
        assert f"{fill_1} Polars" in output

    def test_grouped_outlier_glyph_distinct_from_series_fills(self):
        data = [
            HistogramBar(query_id="Q1", latency_ms=100, platform="A"),
            HistogramBar(query_id="Q1", latency_ms=150, platform="B"),
            HistogramBar(query_id="Q2", latency_ms=90, platform="A"),
            HistogramBar(query_id="Q2", latency_ms=900, platform="B"),
            HistogramBar(query_id="Q3", latency_ms=95, platform="A"),
            HistogramBar(query_id="Q3", latency_ms=145, platform="B"),
            HistogramBar(query_id="Q4", latency_ms=92, platform="A"),
            HistogramBar(query_id="Q4", latency_ms=142, platform="B"),
        ]
        chart = ASCIIQueryHistogram(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        legend_line = output.splitlines()[-1]
        fill_0 = NO_COLOR_OPTS.get_series_fill(0)
        fill_1 = NO_COLOR_OPTS.get_series_fill(1)
        assert f"{fill_0} A" in legend_line
        assert f"{fill_1} B" in legend_line
        assert "Outlier" in legend_line
        assert f"{fill_0} Outlier" not in legend_line
        assert f"{fill_1} Outlier" not in legend_line


class TestSummaryBoxNoColor:
    """ASCIISummaryBox uses structural arrows for improved/regressed."""

    def test_no_ansi_codes(self):
        stats = SummaryStats(
            title="Test",
            geo_mean_baseline_ms=100,
            geo_mean_comparison_ms=80,
            num_queries=10,
            num_improved=5,
            num_stable=3,
            num_regressed=2,
        )
        chart = ASCIISummaryBox(stats=stats, options=NO_COLOR_OPTS)
        output = chart.render()
        _assert_no_ansi(output)

    def test_arrows_in_counts(self):
        stats = SummaryStats(
            title="Test",
            geo_mean_baseline_ms=100,
            geo_mean_comparison_ms=80,
            num_queries=10,
            num_improved=5,
            num_stable=3,
            num_regressed=2,
        )
        chart = ASCIISummaryBox(stats=stats, options=NO_COLOR_OPTS)
        output = chart.render()
        # Should contain structural arrows for improved/regressed
        assert "\u2193" in output or "v" in output  # improvement arrow
        assert "\u2191" in output or "^" in output  # regression arrow


class TestDivergingBarNoColor:
    """ASCIIDivergingBar uses structural arrows for summary counts."""

    def test_no_ansi_codes(self):
        data = [
            DivergingBarData(label="Q1", pct_change=-20),
            DivergingBarData(label="Q2", pct_change=15),
            DivergingBarData(label="Q3", pct_change=0.5),
        ]
        chart = ASCIIDivergingBar(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        _assert_no_ansi(output)

    def test_summary_has_arrows(self):
        data = [
            DivergingBarData(label="Q1", pct_change=-20),
            DivergingBarData(label="Q2", pct_change=15),
        ]
        chart = ASCIIDivergingBar(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        assert "\u2193" in output or "v" in output
        assert "\u2191" in output or "^" in output


class TestBoxPlotNoColor:
    """ASCIIBoxPlot keeps plain series labels in no-color mode."""

    def test_no_ansi_codes(self):
        series = [
            BoxPlotSeries(name="DuckDB", values=[10, 20, 30, 40, 50]),
            BoxPlotSeries(name="Polars", values=[15, 25, 35, 45, 55]),
        ]
        chart = ASCIIBoxPlot(series=series, options=NO_COLOR_OPTS)
        output = chart.render()
        _assert_no_ansi(output)

    def test_multi_series_has_plain_labels(self):
        series = [
            BoxPlotSeries(name="DuckDB", values=[10, 20, 30, 40, 50]),
            BoxPlotSeries(name="Polars", values=[15, 25, 35, 45, 55]),
        ]
        chart = ASCIIBoxPlot(series=series, options=NO_COLOR_OPTS)
        output = chart.render()
        marker_0 = NO_COLOR_OPTS.get_series_marker(0)
        marker_1 = NO_COLOR_OPTS.get_series_marker(1)
        assert "DuckDB" in output
        assert "Polars" in output
        assert f"{marker_0} DuckDB" not in output
        assert f"{marker_1} Polars" not in output

    def test_single_series_no_marker_prefix(self):
        series = [BoxPlotSeries(name="DuckDB", values=[10, 20, 30, 40, 50])]
        chart = ASCIIBoxPlot(series=series, options=NO_COLOR_OPTS)
        output = chart.render()
        marker_0 = NO_COLOR_OPTS.get_series_marker(0)
        assert f"{marker_0} DuckDB" not in output

    def test_plain_labels_do_not_force_extra_name_truncation(self):
        series = [
            BoxPlotSeries(name="DataFusion (df)", values=[10, 20, 30, 40, 50]),
            BoxPlotSeries(name="DataFusion (sql)", values=[12, 22, 32, 42, 52]),
        ]
        chart = ASCIIBoxPlot(series=series, options=NO_COLOR_OPTS)
        output = chart.render()
        assert "DataFusion (df)" in output
        assert "DataFusion (sql)" in output


class TestSparklineTableNoColor:
    """ASCIISparklineTable adds +/- markers for best/worst."""

    def test_no_ansi_codes(self):
        columns = [
            SparklineColumn(name="GeoMean", values={"DuckDB": 100, "Polars": 150}),
            SparklineColumn(name="Total", values={"DuckDB": 500, "Polars": 600}),
        ]
        data = SparklineTableData(platforms=["DuckDB", "Polars"], columns=columns)
        chart = ASCIISparklineTable(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        _assert_no_ansi(output)

    def test_best_worst_markers(self):
        columns = [
            SparklineColumn(name="GeoMean", values={"DuckDB": 100, "Polars": 150}),
        ]
        data = SparklineTableData(platforms=["DuckDB", "Polars"], columns=columns)
        chart = ASCIISparklineTable(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        # Best (lowest) should have + marker, worst should have -
        assert "+" in output
        assert "-" in output


class TestHeatmapNoColor:
    """Verify heatmap's existing no-color fallback works."""

    def test_no_ansi_codes(self):
        matrix = [[100, 150], [200, 180]]
        chart = from_matrix(
            matrix=matrix,
            queries=["Q1", "Q2"],
            platforms=["DuckDB", "Polars"],
            options=NO_COLOR_OPTS,
        )
        output = chart.render()
        _assert_no_ansi(output)

    def test_uses_intensity_chars(self):
        matrix = [[100, 150], [200, 180]]
        chart = from_matrix(
            matrix=matrix,
            queries=["Q1", "Q2"],
            platforms=["DuckDB", "Polars"],
            options=NO_COLOR_OPTS,
        )
        output = chart.render()
        # Should contain intensity characters (░▒▓█) instead of colored backgrounds
        intensity_chars = set("░▒▓█")
        found = any(c in output for c in intensity_chars)
        assert found, "Expected intensity characters in no-color heatmap output"


class TestLegendNoColor:
    """Verify _render_legend uses distinct markers when color is off."""

    def test_legend_markers_are_distinct(self):
        data = [
            BarData(label="Q1", value=100, group="A"),
            BarData(label="Q2", value=200, group="B"),
            BarData(label="Q3", value=150, group="C"),
        ]
        chart = ASCIIBarChart(data=data, options=NO_COLOR_OPTS)
        output = chart.render()
        _assert_no_ansi(output)
        # Legend should have 3 distinct markers
        marker_0 = NO_COLOR_OPTS.get_series_marker(0)
        marker_1 = NO_COLOR_OPTS.get_series_marker(1)
        marker_2 = NO_COLOR_OPTS.get_series_marker(2)
        assert marker_0 in output
        assert marker_1 in output
        assert marker_2 in output
        assert len({marker_0, marker_1, marker_2}) == 3


class TestASCIIOnlyNoColor:
    """Verify no-color + no-unicode (ASCII-only) mode uses ASCII fallback palettes end-to-end."""

    def test_bar_chart_uses_ascii_fills(self):
        data = [
            BarData(label="Q1", value=100, group="DuckDB"),
            BarData(label="Q2", value=200, group="Polars"),
        ]
        chart = ASCIIBarChart(data=data, options=NO_COLOR_ASCII_OPTS)
        output = chart.render()
        _assert_no_ansi(output)
        fill_0 = NO_COLOR_ASCII_OPTS.get_series_fill(0)
        fill_1 = NO_COLOR_ASCII_OPTS.get_series_fill(1)
        assert fill_0 == ASCII_FILL_PATTERNS[0]
        assert fill_1 == ASCII_FILL_PATTERNS[1]
        assert fill_0 in output
        assert fill_1 in output

    def test_comparison_bar_uses_ascii_markers(self):
        data = [
            ComparisonBarData(label="Q1", baseline_value=100, comparison_value=150),
            ComparisonBarData(label="Q2", baseline_value=200, comparison_value=120),
        ]
        chart = ASCIIComparisonBar(data=data, options=NO_COLOR_ASCII_OPTS)
        output = chart.render()
        _assert_no_ansi(output)
        marker_0 = NO_COLOR_ASCII_OPTS.get_series_marker(0)
        marker_1 = NO_COLOR_ASCII_OPTS.get_series_marker(1)
        assert marker_0 == ASCII_SERIES_MARKERS[0]
        assert marker_1 == ASCII_SERIES_MARKERS[1]
        assert marker_0 in output
        assert marker_1 in output
        # ASCII arrows: ^ for regression, v for improvement
        assert "^" in output
        assert "v" in output

    def test_heatmap_uses_ascii_separators(self):
        chart = from_matrix(
            matrix=[[100, 150], [200, 180]],
            queries=["Q1", "Q2"],
            platforms=["DuckDB", "Polars"],
            options=NO_COLOR_ASCII_OPTS,
        )
        output = chart.render()
        assert "┬" not in output
        assert "┼" not in output
        assert "│" not in output
