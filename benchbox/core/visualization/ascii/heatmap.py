"""ASCII heatmap for query x platform comparison."""

from __future__ import annotations

from typing import TYPE_CHECKING

from benchbox.core.visualization.ascii.base import (
    TRUNCATION_MARKER,
    ASCIIChartBase,
    ASCIIChartOptions,
    ColorMode,
    robust_p95,
)

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
        self._scale_max = float("inf")

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
        box = self.options.get_box_chars()
        width = self.options.get_effective_width()

        # Find min/max for normalization
        all_values = [v for row in self.matrix for v in row if v is not None]
        if not all_values:
            return "No data to display"

        min_val = min(all_values)
        max_val = max(all_values)

        # Cap scale at P95×2 to prevent extreme outliers from washing out the heatmap
        scale_max = max_val
        if len(all_values) >= 5:
            p95 = robust_p95(all_values)
            if p95 > 0 and max_val > p95 * 2:
                scale_max = p95 * 2
        self._scale_max = scale_max

        value_range = scale_max - min_val if scale_max > min_val else 1

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

        # Layout calculation
        max_row_label, cell_width, col_sep = self._compute_layout(box, all_values)

        # Column fitting: shrink cell_width to fit all columns before truncating
        available_width = width - max_row_label - 2
        num_cols = len(self.col_labels)
        max_cols = max(1, available_width // (cell_width + len(col_sep)))
        if num_cols > max_cols:
            # Try to reduce cell_width so all columns fit
            min_cell_width = max(5, max(len(self._format_value(v)) for v in all_values) + 2)
            compressed = (available_width // num_cols) - len(col_sep)
            if compressed >= min_cell_width:
                cell_width = compressed
                max_cols = num_cols
        max_rows = len(self.row_labels)
        if self.options.height is not None:
            max_rows = max(1, self.options.height - 10)

        display_cols = self.col_labels[:max_cols]
        display_row_labels = self.row_labels[:max_rows]
        display_matrix = [row[:max_cols] for row in self.matrix[:max_rows]]
        col_truncated = len(self.col_labels) > max_cols
        row_truncated = len(self.row_labels) > len(display_row_labels)

        # Header and separator
        lines.extend(self._render_header(display_cols, max_row_label, cell_width, col_sep, box))

        # Data rows
        use_bg = colors.color_mode != ColorMode.NONE
        bg_scale = self.BG_SCALE_DIVERGING

        for row_label, row_data in zip(display_row_labels, display_matrix):
            row_str = self._render_data_row(
                row_label,
                row_data,
                max_row_label,
                cell_width,
                col_sep,
                box,
                min_val,
                value_range,
                use_bg,
                bg_scale,
                colors,
                intensity_chars,
                col_truncated,
            )
            lines.append(row_str)

        if row_truncated:
            lines.append(f"... ({len(self.row_labels) - len(display_row_labels)} more rows)")

        # Footer
        lines.append(self._render_axis_label("Platform", width, axis="x"))
        lines.append("")
        lines.append(self._render_scale_legend(use_bg, bg_scale, colors, intensity_chars))
        range_str = f"Range: {self._format_value(min_val)} - {self._format_value(max_val)} {self.value_label}"
        if scale_max < max_val:
            range_str += f"  (scale capped at {self._format_value(scale_max)}, {TRUNCATION_MARKER}=truncated)"
        lines.append(range_str)

        return "\n".join(lines)

    def _compute_layout(self, box: dict[str, str], all_values: list[float]) -> tuple[int, int, str]:
        """Compute row label width, cell width, and column separator."""
        max_row_label = min(20, max(len(label) for label in self.row_labels))
        max_col_label = max(len(label) for label in self.col_labels) + 2
        if self._show_values:
            max_value_len = max(len(self._format_value(v)) for v in all_values) + 4
            cell_width = max(max_col_label, max_value_len)
        else:
            cell_width = max_col_label
        return max_row_label, cell_width, box["v"]

    def _render_header(
        self,
        display_cols: list[str],
        max_row_label: int,
        cell_width: int,
        col_sep: str,
        box: dict[str, str],
    ) -> list[str]:
        """Render the column header row and separator line."""
        header = " " * (max_row_label + 2)
        for j, label in enumerate(display_cols):
            header += self._truncate_label(label, cell_width).center(cell_width)
            if j < len(display_cols) - 1:
                header += col_sep

        sep_core = (box["h"] * cell_width + box["cross"]) * (len(display_cols) - 1) + box["h"] * cell_width
        sep = " " * (max_row_label + 1) + box["tm"] + sep_core
        return [header, sep]

    def _render_data_row(
        self,
        row_label: str,
        row_data: list[float | None],
        max_row_label: int,
        cell_width: int,
        col_sep: str,
        box: dict[str, str],
        min_val: float,
        value_range: float,
        use_bg: bool,
        bg_scale: list[str],
        colors: object,
        intensity_chars: list[str],
        col_truncated: bool,
    ) -> str:
        """Render a single data row with colored/intensity cells."""
        label = self._truncate_label(row_label, max_row_label).rjust(max_row_label)
        row_str = f"{label} {box['v']}"

        for j, value in enumerate(row_data):
            cell = self._render_cell(
                value,
                cell_width,
                min_val,
                value_range,
                use_bg,
                bg_scale,
                colors,
                intensity_chars,
            )
            row_str += cell
            if j < len(row_data) - 1:
                row_str += col_sep

        if col_truncated:
            row_str += " ..."
        return row_str

    def _render_cell(
        self,
        value: float | None,
        cell_width: int,
        min_val: float,
        value_range: float,
        use_bg: bool,
        bg_scale: list[str],
        colors: object,
        intensity_chars: list[str],
    ) -> str:
        """Render a single heatmap cell (colored background or intensity chars)."""
        if value is None:
            return (" " * max(0, cell_width - 1)) + "-"

        # Clamp for color normalization; values beyond scale_max map to max color
        clamped = min(value, self._scale_max)
        normalized = (clamped - min_val) / value_range if value_range > 0 else 0.5
        color_idx = min(len(bg_scale) - 1, int(normalized * (len(bg_scale) - 1)))
        value_str = self._format_value(value)

        # Mark cells that exceed the capped scale
        is_truncated = value > self._scale_max
        if is_truncated:
            value_str += TRUNCATION_MARKER

        if use_bg:
            padded = value_str.center(cell_width)
            return colors.colorize(
                padded,
                fg_color=self.FG_ON_BG[color_idx],
                bg_color=bg_scale[color_idx],
            )

        intensity_idx = min(len(intensity_chars) - 1, int(normalized * (len(intensity_chars) - 1)))
        fill_width = max(0, cell_width - len(value_str) - 1)
        if fill_width > 0:
            return (intensity_chars[intensity_idx] * fill_width) + " " + value_str
        return value_str.rjust(cell_width)

    def _render_scale_legend(
        self,
        use_bg: bool,
        bg_scale: list[str],
        colors: object,
        intensity_chars: list[str],
    ) -> str:
        """Render the color/intensity scale legend line."""
        if use_bg:
            scale_line = "Scale: "
            scale_labels = ["fast", "", "", "", "slow"]
            for i, lbl in enumerate(scale_labels):
                block = f" {lbl} " if lbl else "   "
                scale_line += colors.colorize(block, fg_color=self.FG_ON_BG[i], bg_color=bg_scale[i])
            scale_line += f"  ({self.value_label})"
            return scale_line

        scale_line = "Scale: "
        for char in intensity_chars[1:]:
            scale_line += char + " "
        scale_line += f"(fast → slow, {self.value_label})"
        return scale_line


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
