"""ASCII line chart for time series visualization."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from statistics import mean
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

from benchbox.core.visualization.ascii.base import (
    DEFAULT_PALETTE,
    TRUNCATION_MARKER,
    ASCIIChartBase,
    ASCIIChartOptions,
    ColorMode,
    robust_p95,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class LinePoint:
    """Data point for line chart."""

    series: str
    x: float | str  # Can be numeric or label (e.g., date)
    y: float
    label: str | None = None


class ASCIILineChart(ASCIIChartBase):
    """ASCII line chart for time series and trend visualization.

    Example output:
    ```
    Performance Trend
    ────────────────────────────────────────
         │
     350 │                              *
         │                         *
     300 │                    *
         │               *
     250 │          *
         │     *
     200 │*
         └────────────────────────────────────
          Run1  Run2  Run3  Run4  Run5  Run6

    Legend:
      ─*─ DuckDB
      ─+─ Polars
    ```
    """

    # Line/point markers for different series
    MARKERS = ["*", "+", "o", "x", "^", "v", "#", "@"]

    def __init__(
        self,
        points: Sequence[LinePoint],
        title: str | None = None,
        x_label: str = "X",
        y_label: str = "Y",
        show_trend: bool = False,
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.points = list(points)
        self.title = title or "Line Chart"
        self.x_label = x_label
        self.y_label = y_label
        self.show_trend = show_trend
        self._y_capped = False

    def render(self) -> str:
        """Render the line chart as a string."""
        self._detect_capabilities()

        if not self.points:
            return "No data to display"

        colors = self.options.get_colors()
        box_chars = self.options.get_box_chars()
        width = self.options.get_effective_width()

        # Group points by series
        series_map: dict[str, list[LinePoint]] = {}
        for point in self.points:
            series_map.setdefault(point.series, []).append(point)

        # Determine x-axis type (numeric or categorical)
        sample_x = self.points[0].x
        is_numeric_x = isinstance(sample_x, (int, float))

        # Sort points within each series
        for series_name in series_map:
            if is_numeric_x:
                series_map[series_name] = sorted(series_map[series_name], key=lambda p: float(p.x))  # type: ignore
            else:
                # For string x values, preserve order of appearance
                pass

        # Find data bounds
        all_y = [p.y for p in self.points]
        y_min, y_max = min(all_y), max(all_y)

        # Cap y_max at P95×2 so one spike doesn't compress all series
        if len(all_y) >= 5:
            p95_y = robust_p95(all_y)
            if p95_y > 0 and y_max > p95_y * 3:
                y_max = p95_y * 2
                self._y_capped = True

        if is_numeric_x:
            all_x = [float(p.x) for p in self.points]  # type: ignore
            x_min, x_max = min(all_x), max(all_x)
        else:
            # Categorical x: collect unique labels in order
            x_labels: list[str] = []
            seen: set[str] = set()
            for p in self.points:
                x_str = str(p.x)
                if x_str not in seen:
                    x_labels.append(x_str)
                    seen.add(x_str)
            x_min, x_max = 0.0, float(len(x_labels) - 1) if x_labels else 1.0

        # Add padding to y range
        y_range = y_max - y_min if y_max > y_min else 1
        y_min -= y_range * 0.05
        y_max += y_range * 0.1  # Extra top padding for markers

        lines: list[str] = []

        # Title and subtitle
        if self.options.title or self.title:
            lines.append(self._render_title(self.options.title or self.title, width))
            subtitle = self._render_subtitle(width)
            if subtitle:
                lines.append(subtitle)
            lines.append(self._render_horizontal_line(width))

        # Plot dimensions
        y_axis_width = 8
        plot_width = width - y_axis_width - 2
        plot_height = 12

        if plot_width < 20:
            plot_width = 20

        # Create plot grid
        grid: list[list[str]] = [[" " for _ in range(plot_width)] for _ in range(plot_height)]

        # Assign markers and colors to series
        palette = list(DEFAULT_PALETTE)
        series_styles: dict[str, tuple[str, str]] = {}
        for i, series_name in enumerate(series_map.keys()):
            marker = self.MARKERS[i % len(self.MARKERS)]
            color = palette[i % len(palette)]
            series_styles[series_name] = (marker, color)

        # Build marker→color lookup for cell-level colorization
        marker_color_map: dict[str, str] = dict(series_styles.values())

        # Helper to convert data coords to grid coords
        def to_grid_x(x_val: float | str) -> int:
            if is_numeric_x:
                x_num = float(x_val)  # type: ignore
                x_range = x_max - x_min if x_max > x_min else 1
                return int((x_num - x_min) / x_range * (plot_width - 1))
            else:
                # Categorical
                idx = x_labels.index(str(x_val)) if str(x_val) in x_labels else 0
                return int(idx / (len(x_labels) - 1) * (plot_width - 1)) if len(x_labels) > 1 else plot_width // 2

        def to_grid_y(y_val: float) -> int:
            y_range = y_max - y_min if y_max > y_min else 1
            gy = int((y_val - y_min) / y_range * (plot_height - 1))
            return plot_height - 1 - gy  # Invert since row 0 is top

        # Plot each series
        for series_name, series_points in series_map.items():
            marker, _ = series_styles[series_name]

            # Draw lines between consecutive points
            for i in range(len(series_points) - 1):
                p1, p2 = series_points[i], series_points[i + 1]
                gx1, gy1 = to_grid_x(p1.x), to_grid_y(p1.y)
                gx2, gy2 = to_grid_x(p2.x), to_grid_y(p2.y)

                # Draw line using Bresenham-like approach
                steps = max(abs(gx2 - gx1), abs(gy2 - gy1), 1)
                for step in range(1, steps):
                    t = step / steps
                    lx = int(gx1 + t * (gx2 - gx1))
                    ly = int(gy1 + t * (gy2 - gy1))
                    if 0 <= lx < plot_width and 0 <= ly < plot_height:
                        if grid[ly][lx] == " ":
                            # Use different line chars based on slope
                            if abs(gy2 - gy1) > abs(gx2 - gx1):
                                grid[ly][lx] = "|"
                            else:
                                grid[ly][lx] = "-"

            # Draw points (on top of lines)
            for point in series_points:
                gx, gy = to_grid_x(point.x), to_grid_y(point.y)
                if 0 <= gx < plot_width and 0 <= gy < plot_height:
                    grid[gy][gx] = marker

        # Draw trend lines if requested
        if self.show_trend:
            for series_name, series_points in series_map.items():
                if len(series_points) >= 3:
                    trend_y = self._compute_trend([p.y for p in series_points])
                    for i, trend_val in enumerate(trend_y):
                        gx = int(i / (len(trend_y) - 1) * (plot_width - 1)) if len(trend_y) > 1 else plot_width // 2
                        gy = to_grid_y(trend_val)
                        if 0 <= gx < plot_width and 0 <= gy < plot_height:
                            if grid[gy][gx] == " ":
                                grid[gy][gx] = "."

        # Build output with y-axis
        y_step = (y_max - y_min) / max(1, plot_height - 1)

        for row_idx, row in enumerate(grid):
            y_val = y_max - row_idx * y_step
            y_label_str = self._format_value(y_val).rjust(y_axis_width - 1)

            if row_idx == 0:
                axis_char = "┐"
            elif row_idx == plot_height - 1:
                axis_char = "┘"
            else:
                axis_char = "│"

            # Colorize grid cells individually to avoid str.replace() collisions
            if colors.color_mode != ColorMode.NONE:
                colored_cells = []
                for cell in row:
                    if cell in marker_color_map:
                        colored_cells.append(colors.colorize(cell, fg_color=marker_color_map[cell]))
                    else:
                        colored_cells.append(cell)
                row_content = "".join(colored_cells)
            else:
                row_content = "".join(row)

            lines.append(f"{y_label_str}{axis_char}{row_content}")

        # X-axis
        x_axis = " " * y_axis_width + "└" + box_chars["h"] * plot_width
        lines.append(x_axis)

        # X-axis labels
        if is_numeric_x:
            x_labels_line = " " * y_axis_width + " "
            x_step = (x_max - x_min) / 4 if x_max > x_min else 1
            for i in range(5):
                x_val = x_min + i * x_step
                label = self._format_value(x_val)
                pos = int(i * (plot_width - 1) / 4)
                start = pos - len(label) // 2
                if start >= 0 and start + len(label) <= plot_width:
                    padded = " " * (y_axis_width + 1 + start) + label
                    if len(x_labels_line) < len(padded):
                        x_labels_line = x_labels_line.ljust(len(padded))
                    x_labels_line = padded + x_labels_line[len(padded) :]
            lines.append(x_labels_line[:width])
        else:
            # Show categorical labels
            x_labels_line = " " * (y_axis_width + 1)
            label_width = plot_width // len(x_labels) if x_labels else plot_width
            for label in x_labels:
                truncated = self._truncate_label(label, label_width - 1)
                x_labels_line += truncated.center(label_width)
            lines.append(x_labels_line[:width])

        # Axis labels
        lines.append("")
        lines.append(self._render_axis_label(self.y_label, width, axis="y"))

        # Legend
        if len(series_map) > 1 or self.options.show_legend:
            lines.append("")
            lines.append("Legend:")
            for series_name, (marker, color) in series_styles.items():
                if colors.color_mode != ColorMode.NONE:
                    marker_colored = colors.colorize(f"─{marker}─", fg_color=color)
                else:
                    marker_colored = f"─{marker}─"
                lines.append(f"  {marker_colored} {series_name}")

        if self._y_capped:
            lines.append(f"  {TRUNCATION_MARKER} Y-axis capped (outlier values clipped to top)")

        return "\n".join(lines)

    def _compute_trend(self, values: Sequence[float]) -> list[float]:
        """Compute linear trend line using least squares."""
        n = len(values)
        if n < 2:
            return list(values)

        x_vals = list(range(n))
        mean_x = mean(x_vals)
        mean_y = mean(values)

        denom = sum((x - mean_x) ** 2 for x in x_vals)
        if denom == 0:
            return list(values)

        slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_vals, values)) / denom
        intercept = mean_y - slope * mean_x

        return [intercept + slope * x for x in x_vals]


def from_time_series_points(
    points: Sequence,
    title: str | None = None,
    x_label: str = "Run",
    y_label: str = "Execution Time (ms)",
    show_trend: bool = False,
    options: ASCIIChartOptions | None = None,
) -> ASCIILineChart:
    """Create ASCIILineChart from objects with series/x/y attributes."""
    converted: list[LinePoint] = []
    for item in points:
        if isinstance(item, LinePoint):
            converted.append(item)
        else:
            converted.append(
                LinePoint(
                    series=getattr(item, "series", "Series"),
                    x=getattr(item, "x", 0),
                    y=getattr(item, "y", 0),
                    label=getattr(item, "label", None),
                )
            )

    return ASCIILineChart(
        points=converted,
        title=title,
        x_label=x_label,
        y_label=y_label,
        show_trend=show_trend,
        options=options,
    )
