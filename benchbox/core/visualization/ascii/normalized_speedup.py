"""ASCII normalized speedup chart showing relative performance vs a baseline."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from benchbox.core.visualization.ascii.base import ASCIIChartBase, ASCIIChartOptions

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


@dataclass
class SpeedupData:
    """Normalized speedup data for a single platform."""

    name: str
    ratio: float  # e.g. 2.0 = 2x faster than baseline, 0.5 = 2x slower
    is_baseline: bool = False


class ASCIINormalizedSpeedup(ASCIIChartBase):
    """Normalized speedup chart showing platform performance relative to a baseline.

    Bars extend right for faster-than-baseline ratios and left for slower,
    with a vertical reference line at the baseline (1.0x). Uses log2 scaling
    so that 2x and 0.5x are symmetric around the baseline.

    Example output:
    ```
    Normalized Performance (baseline: SQLite = 1.0x)
    ─────────────────────────────────────────────────
              Slower     │     Faster
    SQLite               │█  1.0x
    Pandas         ██████│   0.4x
    DuckDB               │████████████████████  8.2x
    ```
    """

    def __init__(
        self,
        data: Sequence[SpeedupData],
        title: str | None = None,
        baseline_name: str | None = None,
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.data = list(data)
        self.baseline_name = baseline_name
        self.title = title

    def render(self) -> str:
        """Render the normalized speedup chart as a string."""
        self._detect_capabilities()

        if not self.data:
            return "No data to display"

        colors = self.options.get_colors()
        width = self.options.get_effective_width()
        box = self.options.get_box_chars()

        # Determine baseline
        baseline = self._find_baseline()

        # Build title (explicit options.title takes precedence)
        title = self.options.title or self.title
        if not title:
            if baseline:
                title = f"Normalized Performance (baseline: {baseline} = 1.0x)"
            else:
                title = "Normalized Performance"

        # Layout calculation
        max_label_len = min(25, max(len(d.name) for d in self.data))
        annotation_width = 8  # " 12.3x"
        bar_area = width - max_label_len - 1 - 1 - 1 - annotation_width
        half_bar = max(5, bar_area // 2)

        # Compute log2 scale: find max |log2(ratio)| for scaling
        # Clamp extreme ratios to avoid overflow; treat non-positive as very slow
        _MIN_RATIO = 1e-6
        _MAX_RATIO = 1e6
        log_ratios = []
        for d in self.data:
            clamped = max(_MIN_RATIO, min(_MAX_RATIO, d.ratio)) if d.ratio > 0 else _MIN_RATIO
            log_ratios.append(math.log2(clamped))

        max_log = max((abs(lr) for lr in log_ratios), default=1.0)
        if max_log <= 0:
            max_log = 1.0

        lines: list[str] = []

        # Title and subtitle
        lines.append(self._render_title(title, width))
        subtitle = self._render_subtitle(width)
        if subtitle:
            lines.append(subtitle)
        lines.append(self._render_horizontal_line(width))

        # Header
        slower_label = "Slower"
        faster_label = "Faster"
        header_left = slower_label.rjust(max_label_len + 1 + half_bar)
        header_right = f"  {faster_label}"
        lines.append(f"{header_left}{box['v']}{header_right}")

        # Determine fill character
        fill = self.options.get_horizontal_block_chars()[-1]  # Full block

        # Sort: baseline first, then by ratio descending
        sorted_data = sorted(
            zip(self.data, log_ratios),
            key=lambda pair: (not pair[0].is_baseline, -pair[0].ratio),
        )

        for datum, log_ratio in sorted_data:
            label = self._truncate_label(datum.name, max_label_len).ljust(max_label_len)
            bar_len = max(0, int((abs(log_ratio) / max_log) * half_bar))

            # Ensure baseline always gets at least 1 char
            if datum.is_baseline and bar_len == 0:
                bar_len = 1

            if log_ratio >= 0:
                # Faster or baseline: bar extends right
                left_side = " " * half_bar
                right_side = (fill * bar_len).ljust(half_bar)
                colored_left = left_side
                colored_right = colors.colorize(right_side, fg_color="#1b9e77")
            else:
                # Slower: bar extends left
                left_side = (fill * bar_len).rjust(half_bar)
                right_side = " " * half_bar
                colored_left = colors.colorize(left_side, fg_color="#d95f02")
                colored_right = right_side

            # Format annotation
            annotation = self._format_ratio(datum.ratio).rjust(annotation_width)

            # Colorize label
            if datum.is_baseline:
                colored_label = colors.colorize(label, fg_color="#666666")
            else:
                fg = "#1b9e77" if log_ratio >= 0 else "#d95f02"
                colored_label = colors.colorize(label, fg_color=fg)

            lines.append(f"{colored_label} {colored_left}{box['v']}{colored_right} {annotation}")

        lines.append(self._render_horizontal_line(width))

        return "\n".join(lines)

    def _find_baseline(self) -> str | None:
        """Find the baseline platform name."""
        if self.baseline_name:
            return self.baseline_name
        for d in self.data:
            if d.is_baseline:
                return d.name
        return None

    @staticmethod
    def _format_ratio(ratio: float) -> str:
        """Format a speedup ratio for display."""
        if not math.isfinite(ratio) or ratio <= 0:
            return "N/A"
        if ratio >= 100:
            return f"{ratio:.0f}x"
        if ratio >= 10:
            return f"{ratio:.1f}x"
        return f"{ratio:.2f}x"


def from_normalized_results(
    platform_times: Sequence[tuple[str, float]],
    baseline: str | None = None,
    title: str | None = None,
    options: ASCIIChartOptions | None = None,
) -> ASCIINormalizedSpeedup:
    """Create ASCIINormalizedSpeedup from platform timing data.

    Args:
        platform_times: Sequence of (platform_name, total_time_ms) tuples.
        baseline: Name of baseline platform, or "slowest"/"fastest" for auto-selection.
        title: Optional chart title.
        options: Chart rendering options.

    Returns:
        Configured ASCIINormalizedSpeedup instance.
    """
    if not platform_times:
        return ASCIINormalizedSpeedup(data=[], title=title, options=options)

    # Select baseline
    if baseline == "slowest":
        baseline_name = max(platform_times, key=lambda x: x[1])[0]
    elif baseline == "fastest":
        baseline_name = min(platform_times, key=lambda x: x[1])[0]
    elif baseline:
        baseline_name = baseline
    else:
        # Default: use the slowest platform
        baseline_name = max(platform_times, key=lambda x: x[1])[0]

    # Find baseline time
    baseline_time = next((t for n, t in platform_times if n == baseline_name), None)
    if baseline_time is None or baseline_time <= 0:
        baseline_time = 1.0

    converted: list[SpeedupData] = []
    for name, time_ms in platform_times:
        ratio = baseline_time / time_ms if time_ms > 0 else 0.0
        converted.append(SpeedupData(name=name, ratio=ratio, is_baseline=(name == baseline_name)))

    return ASCIINormalizedSpeedup(
        data=converted,
        title=title,
        baseline_name=baseline_name,
        options=options,
    )
