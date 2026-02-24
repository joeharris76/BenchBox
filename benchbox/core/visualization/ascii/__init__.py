"""ASCII chart visualizations for terminal display.

This module provides text-based chart rendering for CLI output,
MCP server responses, and CI/CD logs.
"""

from benchbox.core.visualization.ascii.bar_chart import ASCIIBarChart
from benchbox.core.visualization.ascii.base import (
    ASCIIChartBase,
    TerminalColors,
    detect_terminal_capabilities,
)
from benchbox.core.visualization.ascii.box_plot import ASCIIBoxPlot
from benchbox.core.visualization.ascii.cdf_chart import ASCIICDFChart
from benchbox.core.visualization.ascii.comparison_bar import ASCIIComparisonBar
from benchbox.core.visualization.ascii.diverging_bar import ASCIIDivergingBar
from benchbox.core.visualization.ascii.heatmap import ASCIIHeatmap
from benchbox.core.visualization.ascii.histogram import ASCIIQueryHistogram
from benchbox.core.visualization.ascii.line_chart import ASCIILineChart
from benchbox.core.visualization.ascii.normalized_speedup import ASCIINormalizedSpeedup
from benchbox.core.visualization.ascii.percentile_ladder import ASCIIPercentileLadder
from benchbox.core.visualization.ascii.rank_table import ASCIIRankTable
from benchbox.core.visualization.ascii.scatter_plot import ASCIIScatterPlot
from benchbox.core.visualization.ascii.sparkline_table import ASCIISparklineTable
from benchbox.core.visualization.ascii.stacked_bar import ASCIIStackedBar
from benchbox.core.visualization.ascii.summary_box import ASCIISummaryBox

__all__ = [
    "ASCIIBarChart",
    "ASCIIBoxPlot",
    "ASCIICDFChart",
    "ASCIIChartBase",
    "ASCIIComparisonBar",
    "ASCIIDivergingBar",
    "ASCIIHeatmap",
    "ASCIILineChart",
    "ASCIINormalizedSpeedup",
    "ASCIIPercentileLadder",
    "ASCIIQueryHistogram",
    "ASCIIRankTable",
    "ASCIIScatterPlot",
    "ASCIISparklineTable",
    "ASCIIStackedBar",
    "ASCIISummaryBox",
    "TerminalColors",
    "detect_terminal_capabilities",
]
