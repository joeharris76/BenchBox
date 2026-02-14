"""ASCII heatmap for query x platform comparison."""

from __future__ import annotations

from typing import TYPE_CHECKING

from benchbox.core.visualization.ascii.base import ASCIIChartBase, ASCIIChartOptions, ColorMode

if TYPE_CHECKING:
    from collections.abc import Sequence


class ASCIIHeatmap(ASCIIChartBase):
    """ASCII heatmap showing query execution times across platforms.

    Example output (with colors):
    ```
    Query Execution Heatmap (ms)
    ────────────────────────────────────────
              DuckDB   Polars   Pandas
    Q1        ░░12.3   ▒▒15.2   ▓▓45.8
    Q2        ░░8.5    ░░9.1    ▒▒22.4
    Q3        ▒▒25.1   ░░18.3   ██85.2
    ...

    Scale: ░ fast  ▒ medium  ▓ slow  █ slowest
    ```
    """

    # Sequential color scale (original): green → yellow → red
    COLOR_SCALE_SEQUENTIAL = [
        "#1b9e77",  # Fast - teal/green
        "#66a61e",  # Good - green
        "#e6ab02",  # Medium - yellow
        "#d95f02",  # Slow - orange
        "#e7298a",  # Very slow - magenta/red
    ]

    # Diverging color scale: blue (fast) → neutral → red (slow)
    COLOR_SCALE_DIVERGING = [
        "#2166ac",  # Fast - blue
        "#67a9cf",  # Good - light blue
        "#f7f7f7",  # Neutral - near-white
        "#ef8a62",  # Slow - salmon
        "#b2182b",  # Very slow - red
    ]

    # Default (kept for backward compatibility)
    COLOR_SCALE = COLOR_SCALE_DIVERGING

    def __init__(
        self,
        matrix: Sequence[Sequence[float]],
        row_labels: Sequence[str],
        col_labels: Sequence[str],
        title: str | None = None,
        value_label: str = "ms",
        show_values: bool = True,
        color_scheme: str = "diverging",
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.matrix = [list(row) for row in matrix]
        self.row_labels = list(row_labels)
        self.col_labels = list(col_labels)
        self.title = title or "Heatmap"
        self.value_label = value_label
        self._show_values = show_values
        self._color_scale = self.COLOR_SCALE_SEQUENTIAL if color_scheme == "sequential" else self.COLOR_SCALE_DIVERGING

    # Background colors for heatmap cells (dark text on colored background)
    # Diverging: blue (fast) → neutral → red (slow)
    BG_SCALE_DIVERGING = [
        "#4393c3",  # Fast - medium blue (readable with dark text)
        "#92c5de",  # Good - light blue
        "#d1e5f0",  # Neutral - very light blue
        "#fdb863",  # Slow - light orange
        "#d6604d",  # Very slow - salmon/red
    ]

    # Text colors for contrast against each background
    FG_ON_BG = [
        "#000000",  # dark text on blue
        "#000000",  # dark text on light blue
        "#000000",  # dark text on very light blue
        "#000000",  # dark text on light orange
        "#ffffff",  # white text on red
    ]

    def render(self) -> str:
        """Render the heatmap as a string."""
        self._detect_capabilities()

        if not self.matrix or not self.row_labels or not self.col_labels:
            return "No data to display"

        colors = self.options.get_colors()
        intensity_chars = self.options.get_intensity_chars()
        width = self.options.get_effective_width()

        # Find min/max for normalization
        all_values = [v for row in self.matrix for v in row if v is not None]
        if not all_values:
            return "No data to display"

        min_val = min(all_values)
        max_val = max(all_values)
        value_range = max_val - min_val if max_val > min_val else 1

        lines: list[str] = []

        # Title and subtitle
        if self.options.title or self.title:
            title_text = f"{self.options.title or self.title} ({self.value_label})"
            lines.append(self._render_title(title_text, width))
            subtitle = self._render_subtitle(width)
            if subtitle:
                lines.append(subtitle)
            lines.append(self._render_horizontal_line(width))
            lines.append("")

        # Calculate column widths — dynamic row label width, capped at 20
        max_row_label = min(20, max(len(label) for label in self.row_labels))

        # Cell width: accommodate column labels and formatted values with padding
        max_col_label = max(len(label) for label in self.col_labels) + 2
        if self._show_values:
            max_value_len = max(len(self._format_value(v)) for v in all_values) + 4  # generous padding
            cell_width = max(max_col_label, max_value_len)
        else:
            cell_width = max_col_label

        # 1-char gap between columns
        col_gap = 1

        # Check if we need to truncate columns
        available_width = width - max_row_label - 2
        max_cols = available_width // (cell_width + col_gap)
        max_cols = max(1, max_cols)

        display_cols = self.col_labels[:max_cols]
        display_matrix = [row[:max_cols] for row in self.matrix]
        truncated = len(self.col_labels) > max_cols

        # Header row with column labels
        header = " " * (max_row_label + 2)
        for j, label in enumerate(display_cols):
            header += self._truncate_label(label, cell_width).center(cell_width)
            if j < len(display_cols) - 1:
                header += " " * col_gap
        lines.append(header)

        # Separator
        total_data_width = cell_width * len(display_cols) + col_gap * (len(display_cols) - 1)
        sep = " " * (max_row_label + 1) + "┬" + "─" * total_data_width
        lines.append(sep)

        # Data rows
        use_bg = colors.color_mode != ColorMode.NONE
        bg_scale = self.BG_SCALE_DIVERGING

        for row_label, row_data in zip(self.row_labels, display_matrix):
            label = self._truncate_label(row_label, max_row_label).rjust(max_row_label)
            row_str = f"{label} │"

            for j, value in enumerate(row_data):
                if value is None:
                    cell = " - ".center(cell_width)
                else:
                    normalized = (value - min_val) / value_range if value_range > 0 else 0.5
                    color_idx = min(len(bg_scale) - 1, int(normalized * (len(bg_scale) - 1)))

                    value_str = self._format_value(value)

                    if use_bg:
                        # Full-width colored background cell with centered value
                        padded = value_str.center(cell_width)
                        cell = colors.colorize(
                            padded,
                            fg_color=self.FG_ON_BG[color_idx],
                            bg_color=bg_scale[color_idx],
                        )
                    else:
                        # No-color fallback: intensity prefix + value
                        intensity_idx = min(
                            len(intensity_chars) - 1,
                            int(normalized * (len(intensity_chars) - 1)),
                        )
                        cell = f"{intensity_chars[intensity_idx]}{value_str}".center(cell_width)

                row_str += cell
                if j < len(row_data) - 1:
                    row_str += " " * col_gap

            if truncated:
                row_str += " ..."

            lines.append(row_str)

        # Row count indicator if truncated
        if len(self.row_labels) > len(display_matrix):
            lines.append(f"... ({len(self.row_labels) - len(display_matrix)} more rows)")

        # Axis labels
        lines.append(self._render_axis_label("Platform", width, axis="x"))

        # Scale legend
        lines.append("")
        if use_bg:
            scale_line = "Scale: "
            scale_labels = ["fast", "", "", "", "slow"]
            for i, lbl in enumerate(scale_labels):
                block = f" {lbl} " if lbl else "   "
                scale_line += colors.colorize(block, fg_color=self.FG_ON_BG[i], bg_color=bg_scale[i])
            scale_line += f"  ({self.value_label})"
            lines.append(scale_line)
        else:
            scale_line = "Scale: "
            for char in intensity_chars[1:]:
                scale_line += char + " "
            scale_line += f"(fast → slow, {self.value_label})"
            lines.append(scale_line)

        # Min/max values
        lines.append(f"Range: {self._format_value(min_val)} - {self._format_value(max_val)} {self.value_label}")

        return "\n".join(lines)


def from_matrix(
    matrix: Sequence[Sequence[float]],
    queries: Sequence[str],
    platforms: Sequence[str],
    title: str | None = None,
    value_label: str = "ms",
    show_values: bool = True,
    color_scheme: str = "diverging",
    options: ASCIIChartOptions | None = None,
) -> ASCIIHeatmap:
    """Create ASCIIHeatmap from matrix data (compatible with QueryHeatmap)."""
    return ASCIIHeatmap(
        matrix=matrix,
        row_labels=queries,
        col_labels=platforms,
        title=title,
        value_label=value_label,
        show_values=show_values,
        color_scheme=color_scheme,
        options=options,
    )
