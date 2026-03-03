"""ASCII CDF (Cumulative Distribution Function) chart for latency distribution."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from benchbox.core.visualization.ascii.base import (
    DEFAULT_PALETTE,
    TRUNCATION_MARKER,
    ASCIIChartBase,
    ASCIIChartOptions,
    robust_p95,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

# Markers for each series (matching ASCIILineChart convention)
_MARKERS = ("*", "+", "o", "x", "^", "v", "#", "@")


@dataclass
class CDFSeriesData:
    """CDF series data for a single platform."""

    name: str
    values: list[float]


class ASCIICDFChart(ASCIIChartBase):
    """CDF chart showing cumulative distribution of query latency.

    The X-axis is execution time, the Y-axis is the cumulative proportion
    (0% to 100%), and each platform is a separate line/curve.

    Example output:
    ```
    Cumulative Distribution of Query Latency
    ────────────────────────────────────────────────────
    100%│                         ****+++++++++++
        │                    ****++++
     75%│                ****+++
        │            ***++++
     50%│         **+++
        │       *+++
     25%│     *++
        │    *+
      0%└────────────────────────────────────────
        0      50    100    150    200    300 ms
      * DuckDB    + Polars
    ```
    """

    PLOT_HEIGHT = 15  # Default grid height

    def __init__(
        self,
        data: Sequence[CDFSeriesData],
        title: str | None = None,
        height: int = PLOT_HEIGHT,
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.data = list(data)
        self.title = title or "Cumulative Distribution of Query Latency"
        self.height = max(5, height)
        self._x_capped = False

    def render(self) -> str:
        """Render the CDF chart as a string."""
        self._detect_capabilities()

        if not self.data or all(len(d.values) == 0 for d in self.data):
            return "No data to display"

        colors = self.options.get_colors()
        width = self.options.get_effective_width()
        box = self.options.get_box_chars()

        # Y-axis is fixed 0-100%
        y_label_width = 5  # "100%│"
        plot_width = width - y_label_width - 2
        if plot_width < 10:
            plot_width = 10

        # Find global X range across all series (filter NaN/Inf)
        all_values: list[float] = []
        for d in self.data:
            all_values.extend(v for v in d.values if math.isfinite(v))
        if not all_values:
            return "No data to display"

        x_min = min(all_values)
        x_max = max(all_values)
        if x_min == x_max:
            x_max = x_min + 1  # Avoid division by zero

        # Cap x_max at P95×2 so one extreme tail doesn't bunch all data left
        if len(all_values) >= 5:
            p95 = robust_p95(all_values)
            if p95 > 0 and x_max > p95 * 3:
                x_max = p95 * 2
                self._x_capped = True

        # Build the grid
        grid = [[" " for _ in range(plot_width)] for _ in range(self.height)]

        # Plot each series
        for series_idx, series in enumerate(self.data):
            finite_vals = [v for v in series.values if math.isfinite(v)]
            if not finite_vals:
                continue
            marker = _MARKERS[series_idx % len(_MARKERS)]
            sorted_vals = sorted(finite_vals)
            n = len(sorted_vals)

            # For each point, compute (x_value, cumulative_proportion)
            prev_col = -1
            for i, val in enumerate(sorted_vals):
                proportion = (i + 1) / n  # 0.0 to 1.0
                col = int((val - x_min) / (x_max - x_min) * (plot_width - 1))
                col = max(0, min(plot_width - 1, col))
                row = int((1.0 - proportion) * (self.height - 1))
                row = max(0, min(self.height - 1, row))

                # Place marker (newer series overwrites older at same position)
                grid[row][col] = marker

                # Fill horizontal connection from previous column
                if prev_col >= 0 and col > prev_col + 1:
                    prev_row = int((1.0 - ((i) / n)) * (self.height - 1))
                    prev_row = max(0, min(self.height - 1, prev_row))
                    for fill_col in range(prev_col + 1, col):
                        if grid[prev_row][fill_col] == " ":
                            grid[prev_row][fill_col] = marker

                prev_col = col

        lines: list[str] = []

        # Title and subtitle
        lines.append(self._render_title(self.title, width))
        subtitle = self._render_subtitle(width)
        if subtitle:
            lines.append(subtitle)
        lines.append(self._render_horizontal_line(width))

        # Render grid with Y-axis labels
        for row in range(self.height):
            proportion = 1.0 - (row / (self.height - 1))
            pct = int(proportion * 100)

            # Show Y-axis label at 0%, 25%, 50%, 75%, 100%
            if pct in (0, 25, 50, 75, 100):
                y_label = f"{pct:3d}%"
            else:
                y_label = "    "

            # Color each cell by series
            row_str = ""
            for col in range(plot_width):
                ch = grid[row][col]
                if ch in _MARKERS:
                    series_idx = _MARKERS.index(ch)
                    color = DEFAULT_PALETTE[series_idx % len(DEFAULT_PALETTE)]
                    row_str += colors.colorize(ch, fg_color=color)
                else:
                    if row == self.height - 1:
                        # Bottom axis row: use horizontal line char for empty cells
                        row_str += box["h"]
                    else:
                        row_str += ch

            if row == self.height - 1:
                lines.append(f"{y_label}{box['bl']}{row_str}")
            else:
                lines.append(f"{y_label}{box['v']}{row_str}")

        # X-axis labels
        x_label_line = " " * y_label_width
        x_values = [x_min, (x_min + x_max) / 2, x_max]
        x_positions = [0, plot_width // 2, plot_width - 1]
        x_labels = [self._format_value(v) for v in x_values]

        x_label_parts = [" " * (plot_width + 1)]
        for pos, label in zip(x_positions, x_labels):
            start = max(0, min(plot_width - len(label), pos - len(label) // 2))
            x_label_parts[0] = x_label_parts[0][: start + 1] + label + x_label_parts[0][start + 1 + len(label) :]

        lines.append(f"{x_label_line}{x_label_parts[0].rstrip()}")

        # Legend
        legend_parts: list[str] = []
        for i, series in enumerate(self.data):
            marker = _MARKERS[i % len(_MARKERS)]
            color = DEFAULT_PALETTE[i % len(DEFAULT_PALETTE)]
            colored_marker = colors.colorize(marker, fg_color=color)
            legend_parts.append(f"{colored_marker} {series.name}")
        legend_line = "  " + "    ".join(legend_parts)
        if self._x_capped:
            legend_line += f"  {TRUNCATION_MARKER} X-axis capped"
        lines.append(legend_line)

        return "\n".join(lines)


def from_query_results(
    platform_queries: Sequence[tuple[str, Sequence[float]]],
    title: str | None = None,
    options: ASCIIChartOptions | None = None,
) -> ASCIICDFChart:
    """Create ASCIICDFChart from raw query timing data.

    Args:
        platform_queries: Sequence of (platform_name, query_times) tuples.
        title: Optional chart title.
        options: Chart rendering options.

    Returns:
        Configured ASCIICDFChart instance.
    """
    data = [CDFSeriesData(name=name, values=list(values)) for name, values in platform_queries]
    return ASCIICDFChart(data=data, title=title, options=options)
