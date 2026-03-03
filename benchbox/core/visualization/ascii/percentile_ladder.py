"""ASCII percentile ladder chart for tail-latency visualization."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

from benchbox.core.visualization.ascii.base import (
    ASCIIChartBase,
    ASCIIChartOptions,
    TerminalColors,
    outlier_severity_markers,
    robust_p95,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class PercentileData:
    """Percentile latency data for a single platform."""

    name: str
    p50: float
    p90: float
    p95: float
    p99: float


# Percentile band definitions: (label,)
_BANDS = [
    ("P50",),
    ("P90",),
    ("P95",),
    ("P99",),
]

# Band fill characters for Unicode mode (progressive density)
_BAND_FILLS_UNICODE = ("░", "▒", "▓", "█")
# Band fill characters for ASCII mode
_BAND_FILLS_ASCII = (".", "=", "#", "@")
_ANNOTATION_DECIMALS = 1


def compute_percentile(values: Sequence[float], p: float) -> float:
    """Compute the p-th percentile using linear interpolation.

    Args:
        values: Sequence of numeric values (will be sorted internally).
        p: Percentile in range [0, 100].

    Returns:
        Interpolated percentile value.
    """
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    # Use the "exclusive" percentile method (linear interpolation)
    k = (p / 100) * (n - 1)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


class ASCIIPercentileLadder(ASCIIChartBase):
    """ASCII percentile ladder chart showing P50/P90/P95/P99 latency bands.

    Each platform gets a single row with layered horizontal bar segments
    where each percentile extends further right, creating a "ladder" appearance.

    Example output:
    ```
    Percentile Latency by Platform (ms)
    ────────────────────────────────────────────────────────
    DuckDB   ░░░░░░░░░▒▒▒▒▒▓▓██          12 | 45 | 78 | 120
    Polars   ░░░░░░░░░░▒▒▒▒▒▒▓▓▓▓██████  15 | 52 | 95 | 310
    ────────────────────────────────────────────────────────
    ░ P50   ▒ P90   ▓ P95   █ P99
    ```
    """

    # Colors for each percentile band (from DEFAULT_PALETTE)
    BAND_COLORS = (
        "#1b9e77",  # P50 - teal (good)
        "#e6ab02",  # P90 - yellow (caution)
        "#d95f02",  # P95 - orange (warning)
        "#e7298a",  # P99 - magenta (critical)
    )

    def __init__(
        self,
        data: Sequence[PercentileData],
        title: str | None = None,
        metric_label: str = "ms",
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.data = list(data)
        self.title = title or "Percentile Latency by Platform"
        self.metric_label = metric_label

    def render(self) -> str:
        """Render the percentile ladder chart as a string."""
        self._detect_capabilities()

        if not self.data:
            return "No data to display"

        colors = self.options.get_colors()
        width = self.options.get_effective_width()
        use_unicode = self.options.use_unicode

        fills = _BAND_FILLS_UNICODE if use_unicode else _BAND_FILLS_ASCII

        # Calculate dimensions
        max_label_len = min(25, max(len(d.name) for d in self.data))

        # Format the values annotation: "P50 | P90 | P95 | P99"
        # Find the max p99 to determine bar scaling and shared annotation width
        max_p99 = max(d.p99 for d in self.data) if self.data else 1

        # Cap scale so one extreme P99 doesn't compress all other bars
        scale_max = max_p99
        truncation_active = False
        all_p99 = sorted(d.p99 for d in self.data)
        if len(all_p99) > 5:
            p95_val = robust_p95(all_p99)
            median_val = all_p99[len(all_p99) // 2]
            median_gate = median_val > 0 and max_p99 > median_val * 10
            zero_heavy_gate = median_val <= 0
            if p95_val > 0 and max_p99 > p95_val * 3 and (median_gate or zero_heavy_gate):
                scale_max = p95_val * 2
                truncation_active = True

        annotation_value_width = self._annotation_value_width()

        # Create sample annotation to measure width
        sample_annotation = self._format_annotation(
            PercentileData("", max_p99, max_p99, max_p99, max_p99), annotation_value_width
        )
        annotation_width = len(sample_annotation) + 2  # + padding

        bar_width = width - max_label_len - annotation_width - 3
        if bar_width < 10:
            bar_width = 10
            max_label_len = max(5, width - bar_width - annotation_width - 3)

        lines: list[str] = []

        # Title and subtitle
        if self.options.title or self.title:
            title_text = f"{self.options.title or self.title} ({self.metric_label})"
            lines.append(self._render_title(title_text, width))
            subtitle = self._render_subtitle(width)
            if subtitle:
                lines.append(subtitle)
            lines.append(self._render_horizontal_line(width))

        # Render each platform row
        for datum in self.data:
            label = self._truncate_label(datum.name, max_label_len)
            label_padded = label.ljust(max_label_len)

            # Build the layered bar: each band extends from 0 to its percentile value
            is_truncated = truncation_active and datum.p99 > scale_max
            bar = self._render_bar(datum, bar_width, scale_max, fills, colors, is_truncated)

            # Format annotation
            annotation = self._format_annotation(datum, annotation_value_width)

            # Colorize label with the first band color
            colored_label = colors.colorize(label_padded, fg_color=self.BAND_COLORS[0])

            lines.append(f"{colored_label} {bar}  {annotation}")

        # Separator
        lines.append(self._render_horizontal_line(width))

        # Legend
        legend_parts: list[str] = []
        for i, (band_label,) in enumerate(_BANDS):
            fill = fills[i]
            color = self.BAND_COLORS[i]
            colored_fill = colors.colorize(fill, fg_color=color)
            legend_parts.append(f"{colored_fill} {band_label}")
        lines.append("  ".join(legend_parts))

        return "\n".join(lines)

    def _render_bar(
        self,
        datum: PercentileData,
        bar_width: int,
        max_val: float,
        fills: tuple[str, ...],
        colors: TerminalColors,
        is_truncated: bool = False,
    ) -> str:
        """Render a single layered percentile bar.

        The bar is built from left to right. Each percentile band occupies
        the region from the previous percentile to its own value.
        """
        if max_val <= 0:
            return " " * bar_width

        # Reserve space for severity markers on truncated bars
        markers = ""
        effective_width = bar_width
        if is_truncated:
            markers = outlier_severity_markers(datum.p99, max_val)
            effective_width = max(1, bar_width - len(markers))

        percentiles = [datum.p50, datum.p90, datum.p95, datum.p99]
        # Clamp percentiles to max_val for position calculation
        clamped = [min(p, max_val) for p in percentiles]

        # Calculate the character position for each percentile
        positions = [min(effective_width, max(0, int((p / max_val) * effective_width))) for p in clamped]

        # Ensure minimum visibility: if a band has non-zero width but maps to 0 chars,
        # give it at least 1 char. Interleave clamping to avoid exceeding effective_width.
        for i in range(len(positions)):
            if percentiles[i] > 0 and positions[i] == 0:
                positions[i] = 1
            # Ensure monotonically increasing
            if i > 0 and positions[i] <= positions[i - 1] and percentiles[i] > percentiles[i - 1]:
                positions[i] = positions[i - 1] + 1
            # Clamp immediately after each adjustment
            positions[i] = min(effective_width, positions[i])

        if is_truncated:
            # Fill to full effective width for truncated bars
            positions[-1] = effective_width

        # Build the bar character by character
        bar_chars: list[str] = []
        prev_pos = 0
        for i, pos in enumerate(positions):
            segment_len = pos - prev_pos
            if segment_len > 0:
                fill = fills[i]
                color = self.BAND_COLORS[i]
                segment = fill * segment_len
                bar_chars.append(colors.colorize(segment, fg_color=color))
            prev_pos = pos

        # Append severity markers for truncated bars
        if markers:
            bar_chars.append(markers)
            prev_pos += len(markers)

        # Pad remaining space
        remaining = bar_width - prev_pos
        if remaining > 0:
            bar_chars.append(" " * remaining)

        return "".join(bar_chars)

    def _format_annotation(self, datum: PercentileData, value_width: int) -> str:
        """Format the percentile values annotation."""
        parts = [
            self._format_annotation_value(datum.p50).rjust(value_width),
            self._format_annotation_value(datum.p90).rjust(value_width),
            self._format_annotation_value(datum.p95).rjust(value_width),
            self._format_annotation_value(datum.p99).rjust(value_width),
        ]
        return " | ".join(parts)

    def _annotation_value_width(self) -> int:
        """Compute shared width for right-justified annotation values."""
        all_values = [v for d in self.data for v in (d.p50, d.p90, d.p95, d.p99)]
        if not all_values:
            return len(self._format_annotation_value(0.0))
        return max(len(self._format_annotation_value(v)) for v in all_values)

    @staticmethod
    def _format_annotation_value(value: float) -> str:
        """Format annotation value with consistent decimal precision."""
        if not math.isfinite(value):
            return "N/A"
        return f"{value:.{_ANNOTATION_DECIMALS}f}"


def from_query_results(
    platform_queries: Sequence[tuple[str, Sequence[float]]],
    title: str | None = None,
    metric_label: str = "ms",
    options: ASCIIChartOptions | None = None,
) -> ASCIIPercentileLadder:
    """Create ASCIIPercentileLadder from raw query timing data.

    Args:
        platform_queries: Sequence of (platform_name, query_times) tuples.
        title: Optional chart title.
        metric_label: Unit label for values.
        options: Chart rendering options.

    Returns:
        Configured ASCIIPercentileLadder instance.
    """
    converted: list[PercentileData] = []
    for name, values in platform_queries:
        vals = list(values)
        if not vals:
            converted.append(PercentileData(name=name, p50=0, p90=0, p95=0, p99=0))
            continue
        converted.append(
            PercentileData(
                name=name,
                p50=compute_percentile(vals, 50),
                p90=compute_percentile(vals, 90),
                p95=compute_percentile(vals, 95),
                p99=compute_percentile(vals, 99),
            )
        )

    return ASCIIPercentileLadder(
        data=converted,
        title=title,
        metric_label=metric_label,
        options=options,
    )
