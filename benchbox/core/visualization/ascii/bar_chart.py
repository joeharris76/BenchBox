"""ASCII horizontal bar chart for performance comparisons."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

from benchbox.core.visualization.ascii.base import DEFAULT_PALETTE, ASCIIChartBase, ASCIIChartOptions

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class BarData:
    """Data for a single bar."""

    label: str
    value: float
    group: str | None = None
    error: float | None = None
    is_best: bool = False
    is_worst: bool = False


class ASCIIBarChart(ASCIIChartBase):
    """Horizontal bar chart rendered in ASCII/Unicode.

    Example output:
    ```
    Performance Comparison (ms)
    ────────────────────────────────────────
    DuckDB     ████████████████████████  240.5
    Polars     ████████████████████      200.1
    Pandas     ████████████████████████████████  320.8
    ```
    """

    def __init__(
        self,
        data: Sequence[BarData],
        title: str | None = None,
        metric_label: str = "Value",
        sort_by: str = "value",
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.data = list(data)
        self.title = title or "Bar Chart"
        self.metric_label = metric_label
        self.sort_by = sort_by

    def render(self) -> str:
        """Render the bar chart as a string."""
        self._detect_capabilities()

        if not self.data:
            return "No data to display"

        colors = self.options.get_colors()
        blocks = self.options.get_horizontal_block_chars()
        width = self.options.get_effective_width()

        # Sort data
        sorted_data = self._sort_data()

        # Calculate dimensions — dynamic label width, capped at 30
        max_label_len = min(30, max(len(d.label) for d in sorted_data))
        max_value = max(d.value for d in sorted_data) if sorted_data else 1
        min_value = min(d.value for d in sorted_data) if sorted_data else 0

        # Outlier truncation: cap the scale so extreme values don't squash all others
        scale_max = max_value
        truncated_labels: set[str] = set()
        if len(sorted_data) > 5:
            values = sorted(d.value for d in sorted_data)
            p95_idx = max(0, int(len(values) * 0.95) - 1)
            p95 = values[p95_idx]
            median_val = values[len(values) // 2]
            if median_val > 0 and max_value > median_val * 10 and max_value > p95 * 3:
                scale_max = p95 * 2
                truncated_labels = {d.label for d in sorted_data if d.value > scale_max}

        # Reserve space for: label + " " + bar + " " + value
        value_width = max(8, len(self._format_value(max_value)) + 2)
        bar_width = width - max_label_len - value_width - 3

        if bar_width < 10:
            bar_width = 10
            max_label_len = max(5, width - bar_width - value_width - 3)

        lines: list[str] = []

        # Title and subtitle
        if self.options.title or self.title:
            title_text = f"{self.options.title or self.title} ({self.metric_label})"
            lines.append(self._render_title(title_text, width))
            subtitle = self._render_subtitle(width)
            if subtitle:
                lines.append(subtitle)
            lines.append(self._render_horizontal_line(width))

        # Determine if we have groups
        groups = sorted({d.group for d in sorted_data if d.group})
        use_groups = len(groups) > 1

        # Color palette
        palette = list(DEFAULT_PALETTE)
        group_colors: dict[str, str] = {}
        if use_groups:
            for i, group in enumerate(groups):
                group_colors[group] = palette[i % len(palette)]

        # Render bars
        for datum in sorted_data:
            label = self._truncate_label(datum.label, max_label_len)
            label_padded = label.ljust(max_label_len)

            # Calculate bar length (handle zero/negative values)
            effective_min = min(0, min_value)
            if scale_max > effective_min:
                ratio = max(0, (datum.value - effective_min) / (scale_max - effective_min))
            else:
                ratio = 1.0 if datum.value > 0 else 0.0

            is_truncated = datum.label in truncated_labels
            if is_truncated:
                ratio = 1.0  # Fill the full bar width

            bar_chars = int(ratio * bar_width * 8)  # 8 sub-character increments
            full_blocks = bar_chars // 8
            partial = bar_chars % 8

            bar = blocks[-1] * full_blocks  # Full blocks
            if partial > 0 and full_blocks < bar_width:
                bar += blocks[partial]  # Partial block
            bar = bar.ljust(bar_width)

            # Mark truncated bars with a break indicator
            if is_truncated:
                bar = bar[: bar_width - 1] + "\u25b8"  # ▸ right-pointing triangle

            # Determine bar color
            if datum.is_best:
                bar_color = "#1b9e77"  # Green
            elif datum.is_worst:
                bar_color = "#d95f02"  # Orange
            elif use_groups and datum.group:
                bar_color = group_colors.get(datum.group, palette[0])
            else:
                bar_color = palette[0]

            # Format value with optional error
            value_str = self._format_value(datum.value)
            if datum.error is not None:
                value_str += f" ±{self._format_value(datum.error)}"
            value_str = value_str.rjust(value_width)

            # Colorize bar and label
            colored_bar = colors.colorize(bar, fg_color=bar_color)
            colored_label = colors.colorize(label_padded, fg_color=bar_color)

            lines.append(f"{colored_label} {colored_bar} {value_str}")

        # Add legend if grouped
        if use_groups:
            legend_items = [(group, group_colors[group]) for group in groups]
            lines.extend(self._render_legend(legend_items, colors))

        return "\n".join(lines)

    def _sort_data(self) -> list[BarData]:
        """Sort data according to sort_by setting."""
        if self.sort_by == "value":
            return sorted(self.data, key=lambda d: d.value, reverse=True)
        if self.sort_by == "label":
            return sorted(self.data, key=lambda d: d.label)
        return list(self.data)


def from_bar_data(
    data: Sequence,
    title: str | None = None,
    metric_label: str = "Execution Time (ms)",
    sort_by: str = "value",
    options: ASCIIChartOptions | None = None,
) -> ASCIIBarChart:
    """Create ASCIIBarChart from objects with label/value attributes."""
    converted: list[BarData] = []
    for item in data:
        if isinstance(item, BarData):
            converted.append(item)
        else:
            converted.append(
                BarData(
                    label=getattr(item, "label", str(item)),
                    value=getattr(item, "value", 0),
                    group=getattr(item, "group", None),
                    error=getattr(item, "error", None),
                    is_best=getattr(item, "is_best", False),
                    is_worst=getattr(item, "is_worst", False),
                )
            )

    return ASCIIBarChart(
        data=converted,
        title=title,
        metric_label=metric_label,
        sort_by=sort_by,
        options=options,
    )
