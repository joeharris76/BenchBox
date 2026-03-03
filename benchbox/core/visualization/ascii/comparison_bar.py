"""ASCII paired comparison bar chart for side-by-side run comparison."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from benchbox.core.visualization.ascii.base import (
    DEFAULT_PALETTE,
    ASCIIChartBase,
    ASCIIChartOptions,
    TerminalColors,
    outlier_severity_markers,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


@dataclass
class ComparisonBarData:
    """Data for a single query comparison between two runs."""

    label: str
    baseline_value: float
    comparison_value: float
    baseline_name: str = "Baseline"
    comparison_name: str = "Comparison"


class ASCIIComparisonBar(ASCIIChartBase):
    """Paired comparison bar chart showing side-by-side bars per query.

    Each query shows two horizontal bars (e.g., SQL and DataFrame) with
    percentage change annotation. Color-coded: green for improvement,
    red for regression, dim for stable.

    Example output:
    ```
    SQL vs DataFrame Comparison (ms)
    ────────────────────────────────────────────────────
    Q1   SQL  ████████████████████████  240.5
         DF   ██████████████████        180.2    -25.1%
    Q2   SQL  ████████████████          160.0
         DF   ████████████████████████  240.8    +50.5%
    ────────────────────────────────────────────────────
    each █ ~ 10.0ms
    ```
    """

    # Threshold for "stable" (no meaningful change)
    STABLE_THRESHOLD_PCT = 2.0

    def __init__(
        self,
        data: Sequence[ComparisonBarData],
        title: str | None = None,
        metric_label: str = "Execution Time (ms)",
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.data = list(data)
        self.title = title or "Comparison"
        self.metric_label = metric_label

    def render(self) -> str:
        """Render the paired comparison bar chart as a string."""
        self._detect_capabilities()

        if not self.data:
            return "No data to display"

        colors = self.options.get_colors()
        blocks = self.options.get_horizontal_block_chars()
        width = self.options.get_effective_width()

        # Determine names from first item
        baseline_name = self.data[0].baseline_name
        comparison_name = self.data[0].comparison_name

        # Truncate run names for display — dynamic width, capped at 25
        max_name_len = max(len(baseline_name), len(comparison_name))
        name_display_len = min(max_name_len, 25)
        baseline_short = self._truncate_label(baseline_name, name_display_len)
        comparison_short = self._truncate_label(comparison_name, name_display_len)

        # Calculate layout dimensions — dynamic label width, capped at 30
        max_label_len = min(30, max((len(d.label) for d in self.data), default=2))
        # annotation: " +1234.5%" = 10 chars max
        annotation_width = 10
        value_width = 8
        # label + " " + name + " " + bar + " " + value + annotation
        overhead = max_label_len + 1 + name_display_len + 1 + 1 + value_width + annotation_width
        bar_width = max(10, width - overhead)

        # Find max value for scaling (consider both baseline and comparison)
        all_values = []
        for d in self.data:
            if math.isfinite(d.baseline_value):
                all_values.append(d.baseline_value)
            if math.isfinite(d.comparison_value):
                all_values.append(d.comparison_value)

        if not all_values:
            return "No finite data to display"

        max_value = max(all_values)
        if max_value <= 0:
            max_value = 1.0

        # Check for outliers that need truncation
        sorted_vals = sorted(all_values, reverse=True)
        # Use the second-highest value as scale reference if top value is an outlier
        truncate_threshold = None
        if len(sorted_vals) > 2:
            median_val = sorted_vals[len(sorted_vals) // 2]
            if median_val > 0 and sorted_vals[0] / median_val > 5:
                # Top value is >5x the median; truncate at 2x the second value
                truncate_threshold = sorted_vals[1] * 2
                max_value = truncate_threshold

        lines: list[str] = []

        # Title and subtitle
        title_text = f"{self.title} ({self.metric_label})"
        lines.append(self._render_title(title_text, width))
        subtitle = self._render_subtitle(width)
        if subtitle:
            lines.append(subtitle)
        lines.append(self._render_horizontal_line(width))

        # Assign palette colors and fill patterns to baseline and comparison
        palette = list(DEFAULT_PALETTE)
        baseline_color = palette[0]
        comparison_color = palette[1]
        no_color = not self.options.use_color
        baseline_fill = self.options.get_series_fill(0)
        comparison_fill = self.options.get_series_fill(1)

        # Render each query pair
        for i, d in enumerate(self.data):
            label = self._truncate_label(d.label, max_label_len).ljust(max_label_len)

            # Calculate percentage change
            if d.baseline_value > 0:
                pct_change = ((d.comparison_value - d.baseline_value) / d.baseline_value) * 100
            else:
                pct_change = 0.0

            # Baseline bar (first line)
            b_fill = baseline_fill if no_color else None
            b_bar = self._render_bar(d.baseline_value, max_value, bar_width, blocks, truncate_threshold, b_fill)
            b_bar_colored = colors.colorize(b_bar, fg_color=baseline_color)
            b_value = self._format_value(d.baseline_value)
            b_name = colors.colorize(baseline_short.ljust(name_display_len), fg_color=baseline_color)
            lines.append(f"{label} {b_name} {b_bar_colored} {b_value.rjust(value_width)}")

            # Comparison bar (second line) with percentage annotation
            c_fill = comparison_fill if no_color else None
            c_bar = self._render_bar(d.comparison_value, max_value, bar_width, blocks, truncate_threshold, c_fill)
            c_bar_colored = colors.colorize(c_bar, fg_color=comparison_color)
            c_value = self._format_value(d.comparison_value)
            c_name = colors.colorize(comparison_short.ljust(name_display_len), fg_color=comparison_color)

            # Format annotation
            annotation = self._format_annotation(pct_change, colors)
            empty_label = " " * max_label_len
            lines.append(f"{empty_label} {c_name} {c_bar_colored} {c_value.rjust(value_width)}{annotation}")

            # Blank line between query pairs (except last)
            if i < len(self.data) - 1:
                lines.append("")

        # Scale note
        lines.append(self._render_horizontal_line(width))
        scale_char = baseline_fill if no_color else blocks[-1]
        scale_value = max_value / bar_width
        unit = self._extract_unit(self.metric_label)
        lines.append(f"each {scale_char} {self._format_approx()} {self._format_value(scale_value)}{unit}")

        # Legend with distinct markers
        b_marker_char = self.options.get_series_marker(0)
        c_marker_char = self.options.get_series_marker(1)
        b_marker = colors.colorize(b_marker_char, fg_color=baseline_color)
        c_marker = colors.colorize(c_marker_char, fg_color=comparison_color)
        if no_color:
            down_arrow = "\u2193" if self.options.use_unicode else "v"
            up_arrow = "\u2191" if self.options.use_unicode else "^"
            lines.append(f"  {b_marker} {baseline_short}  {c_marker} {comparison_short}")
            lines.append(f"  {down_arrow} improvement (negative %)  {up_arrow} regression (positive %)")
        else:
            green = colors.colorize("improvement", fg_color="#66a61e")
            red = colors.colorize("regression", fg_color="#d95f02")
            lines.append(f"  {b_marker} {baseline_short}  {c_marker} {comparison_short}")
            lines.append(f"  {green} (negative %)  {red} (positive %)")

        return "\n".join(lines)

    def _render_bar(
        self,
        value: float,
        max_value: float,
        bar_width: int,
        blocks: str,
        truncate_threshold: float | None,
        fill_char: str | None = None,
    ) -> str:
        """Render a single bar with 1/8 block precision and optional truncation.

        Args:
            fill_char: Override fill character for no-color mode. When set,
                       uses this character instead of blocks[-1] for full blocks
                       and skips sub-character partial rendering.
        """
        if not math.isfinite(value) or value <= 0:
            return " " * bar_width

        truncated = False
        display_value = value
        if truncate_threshold is not None and value > truncate_threshold:
            display_value = truncate_threshold
            truncated = True

        ratio = min(1.0, display_value / max_value)
        bar_chars = int(ratio * bar_width * 8)
        full_blocks = bar_chars // 8
        partial = bar_chars % 8

        fc = fill_char or blocks[-1]
        bar = fc * full_blocks
        if partial > 0 and full_blocks < bar_width:
            bar += fc if fill_char else blocks[partial]

        if truncated and truncate_threshold and truncate_threshold > 0:
            markers = outlier_severity_markers(value, truncate_threshold)
            bar = bar[: bar_width - len(markers)] + markers
        else:
            bar = bar.ljust(bar_width)

        return bar

    def _format_annotation(self, pct_change: float, colors: TerminalColors) -> str:
        """Format inline percentage change annotation with color or structural symbols."""
        if abs(pct_change) < self.STABLE_THRESHOLD_PCT:
            return ""

        no_color = not self.options.use_color
        if pct_change > 0:
            # Regression (slower)
            arrow = "\u2191" if self.options.use_unicode else "^"
            text = f"  +{pct_change:.1f}%"
            if no_color:
                return f"{text} {arrow}"
            return colors.colorize(text, fg_color="#d95f02")
        else:
            # Improvement (faster)
            arrow = "\u2193" if self.options.use_unicode else "v"
            text = f"  {pct_change:.1f}%"
            if no_color:
                return f"{text} {arrow}"
            return colors.colorize(text, fg_color="#66a61e")

    @staticmethod
    def _extract_unit(metric_label: str) -> str:
        """Extract the unit from a metric label like 'Execution Time (ms)'."""
        start = metric_label.rfind("(")
        end = metric_label.rfind(")")
        if start != -1 and end != -1 and end > start:
            return metric_label[start + 1 : end]
        return ""

    def _format_approx(self) -> str:
        """Return the approximately-equal symbol."""
        if self.options.use_unicode:
            return "\u2248"
        return "~"


def from_comparison_data(
    data: Sequence,
    title: str | None = None,
    metric_label: str = "Execution Time (ms)",
    options: ASCIIChartOptions | None = None,
) -> ASCIIComparisonBar:
    """Create ASCIIComparisonBar from comparison data.

    Accepts ComparisonBarData objects or dicts with compatible keys.
    """
    converted: list[ComparisonBarData] = []
    for item in data:
        if isinstance(item, ComparisonBarData):
            converted.append(item)
        else:
            logger.warning(
                "from_comparison_data: unexpected type %s, using getattr fallback",
                type(item).__name__,
            )
            converted.append(
                ComparisonBarData(
                    label=getattr(item, "label", str(item)),
                    baseline_value=getattr(item, "baseline_value", 0),
                    comparison_value=getattr(item, "comparison_value", 0),
                    baseline_name=getattr(item, "baseline_name", "Baseline"),
                    comparison_name=getattr(item, "comparison_name", "Comparison"),
                )
            )

    return ASCIIComparisonBar(
        data=converted,
        title=title,
        metric_label=metric_label,
        options=options,
    )
