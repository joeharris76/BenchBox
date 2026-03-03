"""ASCII box plot for distribution visualization."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from statistics import mean, stdev
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

from benchbox.core.visualization.ascii.base import (
    DEFAULT_PALETTE,
    ASCIIChartBase,
    ASCIIChartOptions,
    outlier_severity_markers,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class BoxPlotStats:
    """Computed statistics for a box plot."""

    min_val: float
    q1: float
    median: float
    q3: float
    max_val: float
    mean: float
    std: float
    outliers: list[float]


def compute_quartiles(values: Sequence[float]) -> BoxPlotStats:
    """Compute box plot statistics from a sequence of values."""
    if not values:
        return BoxPlotStats(0, 0, 0, 0, 0, 0, 0, [])

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def percentile(p: float) -> float:
        k = (n - 1) * p
        f = int(k)
        c = f + 1 if f + 1 < n else f
        return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])

    q1 = percentile(0.25)
    med = percentile(0.50)
    q3 = percentile(0.75)

    # IQR-based whiskers
    iqr = q3 - q1
    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

    # Whiskers extend to min/max within fences
    within_fences = [v for v in sorted_vals if lower_fence <= v <= upper_fence]
    if within_fences:
        whisker_low = within_fences[0]
        whisker_high = within_fences[-1]
    else:
        # All values are outliers — fall back to data extremes
        whisker_low = sorted_vals[0]
        whisker_high = sorted_vals[-1]

    # Outliers are beyond the fences
    outliers = [v for v in sorted_vals if v < lower_fence or v > upper_fence]

    return BoxPlotStats(
        min_val=whisker_low,
        q1=q1,
        median=med,
        q3=q3,
        max_val=whisker_high,
        mean=mean(sorted_vals),
        std=stdev(sorted_vals) if n > 1 else 0,
        outliers=outliers,
    )


@dataclass
class BoxPlotSeries:
    """Data series for box plot."""

    name: str
    values: Sequence[float]


class ASCIIBoxPlot(ASCIIChartBase):
    """ASCII box plot for showing distributions.

    Example output:
    ```
    Latency Distribution (ms)
    ────────────────────────────────────────
              ╷     ┌──────┬──────┐     ╷
    DuckDB    ├─────┤      │      ├─────┤  o o
              ╵     └──────┴──────┘     ╵
              ╷   ┌────┬────────┐         ╷
    Polars    ├───┤    │        ├─────────┤
              ╵   └────┴────────┘         ╵
    ──────────┼────────┼────────┼────────┼───
              0      100      200      300  ms

    Statistics:
      DuckDB: median=120.5, mean=125.3, std=45.2
      Polars: median=85.2, mean=90.1, std=32.8
    ```
    """

    # Box drawing characters for horizontal box plots
    BOX_TOP = "┬"
    BOX_BOTTOM = "┴"
    BOX_LEFT = "├"
    BOX_RIGHT = "┤"
    WHISKER_H = "─"
    WHISKER_V_TOP = "╷"
    WHISKER_V_BOTTOM = "╵"
    MEDIAN_LINE = "│"
    OUTLIER = "o"

    def __init__(
        self,
        series: Sequence[BoxPlotSeries],
        title: str | None = None,
        y_label: str = "Value",
        show_stats: bool = True,
        show_mean: bool = True,
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.series = list(series)
        self.title = title or "Box Plot"
        self.y_label = y_label
        self.show_stats = show_stats
        self.show_mean = show_mean

    def render(self) -> str:
        """Render the box plot as a string."""
        self._detect_capabilities()

        if not self.series:
            return "No data to display"

        colors = self.options.get_colors()
        width = self.options.get_effective_width()

        # Compute statistics for all series
        stats_list: list[tuple[str, BoxPlotStats]] = []
        for s in self.series:
            stats = compute_quartiles(s.values)
            stats_list.append((s.name, stats))

        # Find global min/max for scale
        all_values: list[float] = []
        for s in self.series:
            all_values.extend(s.values)

        if not all_values:
            return "No data to display"

        global_min = min(all_values)
        global_max = max(all_values)

        # Cap the scale at max whisker × 1.5 to prevent extreme outliers from
        # crushing the box plots.  If the actual data max is lower, use that instead.
        max_whisker = max(stats.max_val for _, stats in stats_list)
        scale_max = min(max_whisker * 1.5, global_max)

        value_range = scale_max - global_min if scale_max > global_min else 1

        # Layout calculations — dynamic label width, capped at 30
        name_width = min(30, max(len(s.name) for s in self.series))
        label_width = name_width

        plot_width = width - label_width - 4
        if plot_width < 20:
            plot_width = 20

        lines: list[str] = []

        # Title and subtitle
        if self.options.title or self.title:
            title_text = f"{self.options.title or self.title} ({self.y_label})"
            lines.append(self._render_title(title_text, width))
            subtitle = self._render_subtitle(width)
            if subtitle:
                lines.append(subtitle)
            lines.append(self._render_horizontal_line(width))
            lines.append("")

        palette = list(DEFAULT_PALETTE)

        # Render each series as a horizontal box plot
        for i, (name, stats) in enumerate(stats_list):
            color = palette[i % len(palette)]
            label = self._truncate_label(name, name_width).ljust(label_width)

            # Convert values to positions
            def to_pos(val: float) -> int:
                if value_range == 0:
                    return plot_width // 2
                return int((val - global_min) / value_range * (plot_width - 1))

            pos_min = to_pos(stats.min_val)
            pos_q1 = to_pos(stats.q1)
            pos_med = to_pos(stats.median)
            pos_q3 = to_pos(stats.q3)
            pos_max = to_pos(stats.max_val)

            # Build the three lines for this box plot
            # Top line: whisker caps
            top_line = [" "] * plot_width
            top_line[pos_min] = self.WHISKER_V_TOP
            top_line[pos_max] = self.WHISKER_V_TOP
            for p in range(pos_q1, pos_q3 + 1):
                if p == pos_q1:
                    top_line[p] = "┌"
                elif p == pos_q3:
                    top_line[p] = "┐"
                elif p == pos_med:
                    top_line[p] = self.BOX_TOP
                else:
                    top_line[p] = "─"

            # Middle line: box with whiskers
            mid_line = [" "] * plot_width
            # Left whisker
            for p in range(pos_min, pos_q1):
                mid_line[p] = self.WHISKER_H
            mid_line[pos_min] = self.BOX_LEFT
            # Box
            for p in range(pos_q1, pos_q3 + 1):
                if p in (pos_q1, pos_q3):
                    mid_line[p] = "│"
                elif p == pos_med:
                    mid_line[p] = self.MEDIAN_LINE
                else:
                    mid_line[p] = " "
            # Right whisker
            for p in range(pos_q3 + 1, pos_max + 1):
                mid_line[p] = self.WHISKER_H
            mid_line[pos_max] = self.BOX_RIGHT

            # Bottom line: whisker caps
            bottom_line = [" "] * plot_width
            bottom_line[pos_min] = self.WHISKER_V_BOTTOM
            bottom_line[pos_max] = self.WHISKER_V_BOTTOM
            for p in range(pos_q1, pos_q3 + 1):
                if p == pos_q1:
                    bottom_line[p] = "└"
                elif p == pos_q3:
                    bottom_line[p] = "┘"
                elif p == pos_med:
                    bottom_line[p] = self.BOX_BOTTOM
                else:
                    bottom_line[p] = "─"

            # Add outliers to middle line at their actual scaled positions
            if stats.outliers:
                # Collect truncated outliers to render severity markers after the loop
                max_truncated: float = 0
                for outlier_val in stats.outliers:
                    if outlier_val > scale_max:
                        max_truncated = max(max_truncated, outlier_val)
                    else:
                        outlier_pos = to_pos(outlier_val)
                        outlier_pos = max(0, min(outlier_pos, plot_width - 1))
                        if mid_line[outlier_pos] == " ":
                            mid_line[outlier_pos] = self.OUTLIER

                # Render truncation markers scaled to severity (1-4 ▸ chars)
                # Markers overwrite any existing outlier dots to form a
                # contiguous block at the right edge of the plot.
                if max_truncated > 0:
                    marker_str = outlier_severity_markers(max_truncated, scale_max)
                    start = plot_width - len(marker_str)
                    for offset in range(len(marker_str)):
                        pos = start + offset
                        if 0 <= pos < plot_width:
                            mid_line[pos] = marker_str[offset]

            # Colorize and add
            top_colored = colors.colorize("".join(top_line), fg_color=color)
            mid_colored = colors.colorize("".join(mid_line), fg_color=color)
            bottom_colored = colors.colorize("".join(bottom_line), fg_color=color)

            colored_label = colors.colorize(label, fg_color=color)
            lines.append(f"{' ' * label_width}  {top_colored}")
            lines.append(f"{colored_label}  {mid_colored}")
            lines.append(f"{' ' * label_width}  {bottom_colored}")

        # X-axis scale
        axis_line = ["─"] * plot_width
        lines.append(f"{' ' * label_width}  {''.join(axis_line)}")

        # Scale labels
        scale_line = [" "] * plot_width
        min_label = self._format_value(global_min)
        max_label = self._format_value(scale_max)
        mid_val = (global_min + scale_max) / 2
        mid_label = self._format_value(mid_val)

        # Place labels
        scale_line[0 : len(min_label)] = list(min_label)
        mid_pos = plot_width // 2 - len(mid_label) // 2
        scale_line[mid_pos : mid_pos + len(mid_label)] = list(mid_label)
        scale_line[plot_width - len(max_label) :] = list(max_label)

        lines.append(f"{' ' * label_width}  {''.join(scale_line)}")

        # Axis label
        lines.append(self._render_axis_label(self.y_label, width, axis="x"))

        # Statistics table
        if self.show_stats:
            name_col_w = max(len(name) for name, _ in stats_list)
            headers = ["median"]
            if self.show_mean:
                headers.extend(["mean", "std"])

            # Collect raw values per column so we can format each column uniformly
            raw_rows: list[tuple[str, list[float]]] = []
            for name, stats in stats_list:
                vals = [stats.median]
                if self.show_mean:
                    vals.extend([stats.mean, stats.std])
                raw_rows.append((name, vals))

            # Format each column with consistent precision and suffix
            num_cols = len(headers)
            formatted_cols: list[list[str]] = [[] for _ in range(num_cols)]
            for col_idx in range(num_cols):
                col_vals = [row[1][col_idx] for row in raw_rows]
                col_max = max(abs(v) for v in col_vals) if col_vals else 0
                # Choose suffix based on the largest value in the column
                if col_max >= 1_000_000:
                    suffix, divisor = "M", 1_000_000
                elif col_max >= 1_000:
                    suffix, divisor = "K", 1_000
                else:
                    suffix, divisor = "", 1
                for v in col_vals:
                    formatted_cols[col_idx].append(f"{v / divisor:.1f}{suffix}")

            # Build rows and measure column widths
            rows: list[tuple[str, list[str]]] = []
            col_widths = [len(h) for h in headers]
            for row_idx, (name, _) in enumerate(raw_rows):
                vals = [formatted_cols[c][row_idx] for c in range(num_cols)]
                for j, v in enumerate(vals):
                    col_widths[j] = max(col_widths[j], len(v))
                rows.append((name, vals))

            # Header row
            header = " " * (name_col_w + 2)
            header += "  ".join(h.rjust(col_widths[j]) for j, h in enumerate(headers))
            lines.append(header)
            sep = " " * (name_col_w + 2) + "  ".join("─" * col_widths[j] for j in range(num_cols))
            lines.append(sep)

            # Data rows
            for name, vals in rows:
                row = name.ljust(name_col_w) + "  "
                row += "  ".join(v.rjust(col_widths[j]) for j, v in enumerate(vals))
                lines.append(row)

        return "\n".join(lines)


def from_distribution_series(
    series: Sequence,
    title: str | None = None,
    y_label: str = "Execution Time (ms)",
    show_stats: bool = True,
    show_mean: bool = True,
    options: ASCIIChartOptions | None = None,
) -> ASCIIBoxPlot:
    """Create ASCIIBoxPlot from objects with name/values attributes."""
    converted: list[BoxPlotSeries] = []
    for item in series:
        if isinstance(item, BoxPlotSeries):
            converted.append(item)
        else:
            converted.append(
                BoxPlotSeries(
                    name=getattr(item, "name", str(item)),
                    values=list(getattr(item, "values", [])),
                )
            )

    return ASCIIBoxPlot(
        series=converted,
        title=title,
        y_label=y_label,
        show_stats=show_stats,
        show_mean=show_mean,
        options=options,
    )
