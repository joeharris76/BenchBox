"""ASCII stacked bar chart for benchmark phase time decomposition."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from benchbox.core.visualization.ascii.base import DEFAULT_PALETTE, ASCIIChartBase, ASCIIChartOptions, TerminalColors

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


@dataclass
class StackedBarSegment:
    """A single segment within a stacked bar."""

    phase_name: str
    value: float
    color: str | None = None  # Optional override; defaults to palette rotation


@dataclass
class StackedBarData:
    """Stacked bar data for a single platform."""

    label: str
    segments: list[StackedBarSegment] = field(default_factory=list)
    total: float | None = None  # Auto-computed from segments if None

    def __post_init__(self):
        if self.total is None:
            self.total = sum(s.value for s in self.segments)


# Fill characters for each phase segment (Unicode mode)
_SEGMENT_FILLS_UNICODE = ("░", "▒", "▓", "█", "▚", "▞", "▖", "▗")
_SEGMENT_FILLS_ASCII = (".", "-", "=", "#", "+", "*", "~", "^")


class ASCIIStackedBar(ASCIIChartBase):
    """Stacked horizontal bar chart showing phase-level time decomposition.

    Each bar represents a platform and the segments show time spent in each
    benchmark phase. Each phase uses a distinct fill character and color.

    Example output:
    ```
    Phase Breakdown by Platform
    ──────────────────────────────────────────────────────
    DuckDB   ░░▒▒▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██████              12.4s
    Polars   ░░░▒▒▒▒▒▒▓▓▓▓▓▓▓▓▓▓▓▓▓▓████████████          15.2s
    SQLite   ░░░░▒▒▒▒▒▒▒▒▒▒▒▓▓▓▓▓▓▓▓▓████████████████     28.7s
    ──────────────────────────────────────────────────────
    ░ DataGen  ▒ Load  ▓ Power  █ Throughput
    ```
    """

    def __init__(
        self,
        data: Sequence[StackedBarData],
        title: str | None = None,
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.data = list(data)
        self.title = title or "Phase Breakdown by Platform"

    def render(self) -> str:
        """Render the stacked bar chart as a string."""
        self._detect_capabilities()

        if not self.data:
            return "No data to display"

        colors = self.options.get_colors()
        width = self.options.get_effective_width()
        use_unicode = self.options.use_unicode

        fills = _SEGMENT_FILLS_UNICODE if use_unicode else _SEGMENT_FILLS_ASCII

        # Collect unique phase names in order of first appearance
        phase_names: list[str] = []
        for d in self.data:
            for s in d.segments:
                if s.phase_name not in phase_names:
                    phase_names.append(s.phase_name)

        # Calculate dimensions
        max_label_len = min(25, max(len(d.label) for d in self.data))
        total_annotation_width = 10  # " 999.9s"
        bar_width = width - max_label_len - total_annotation_width - 3
        if bar_width < 10:
            bar_width = 10

        # Find max total for scaling
        max_total = max((d.total or 0 for d in self.data), default=1)
        if max_total <= 0:
            max_total = 1

        lines: list[str] = []

        # Title and subtitle
        lines.append(self._render_title(self.title, width))
        subtitle = self._render_subtitle(width)
        if subtitle:
            lines.append(subtitle)
        lines.append(self._render_horizontal_line(width))

        # Render each platform row
        for datum in self.data:
            label = self._truncate_label(datum.label, max_label_len)
            label_padded = label.ljust(max_label_len)

            # Build stacked bar
            bar = self._render_stacked_bar(datum, bar_width, max_total, fills, phase_names, colors)

            # Format total time annotation
            total = datum.total or 0
            total_str = self._format_time(total).rjust(total_annotation_width)

            # Colorize label
            colored_label = colors.colorize(label_padded, fg_color=DEFAULT_PALETTE[0])

            lines.append(f"{colored_label} {bar}  {total_str}")

        # Separator
        lines.append(self._render_horizontal_line(width))

        # Legend
        legend_parts: list[str] = []
        for i, phase_name in enumerate(phase_names):
            fill = fills[i % len(fills)]
            color = DEFAULT_PALETTE[i % len(DEFAULT_PALETTE)]
            colored_fill = colors.colorize(fill, fg_color=color)
            legend_parts.append(f"{colored_fill} {phase_name}")
        lines.append("  ".join(legend_parts))

        return "\n".join(lines)

    def _render_stacked_bar(
        self,
        datum: StackedBarData,
        bar_width: int,
        max_total: float,
        fills: tuple[str, ...],
        phase_names: list[str],
        colors: TerminalColors,
    ) -> str:
        """Render a single stacked bar with colored segments."""
        total = datum.total or 0
        if total <= 0 or max_total <= 0:
            return " " * bar_width

        # Total bar length proportional to this platform's total vs max
        total_bar_len = max(1, int((total / max_total) * bar_width))

        # Calculate each segment's proportional length (sum duplicates)
        seg_values: dict[str, float] = {}
        for s in datum.segments:
            seg_values[s.phase_name] = seg_values.get(s.phase_name, 0) + s.value
        bar_chars: list[str] = []
        used = 0

        for i, phase_name in enumerate(phase_names):
            val = seg_values.get(phase_name, 0)
            if val <= 0:
                continue
            seg_len = max(1, int((val / total) * total_bar_len))
            # Clamp so we don't exceed total_bar_len
            seg_len = min(seg_len, total_bar_len - used)
            if seg_len <= 0:
                continue

            fill = fills[i % len(fills)]
            # Use segment color override if available, otherwise palette
            seg = next((s for s in datum.segments if s.phase_name == phase_name), None)
            color = seg.color if seg and seg.color else DEFAULT_PALETTE[i % len(DEFAULT_PALETTE)]
            segment = fill * seg_len
            bar_chars.append(colors.colorize(segment, fg_color=color))
            used += seg_len

        # Pad remaining
        remaining = bar_width - used
        if remaining > 0:
            bar_chars.append(" " * remaining)

        return "".join(bar_chars)

    @staticmethod
    def _format_time(ms: float) -> str:
        """Format milliseconds as a human-readable time string."""
        if ms >= 60_000:
            return f"{ms / 60_000:.1f}min"
        if ms >= 1_000:
            return f"{ms / 1_000:.1f}s"
        return f"{ms:.0f}ms"


def from_phase_data(
    data: Sequence[StackedBarData],
    title: str | None = None,
    options: ASCIIChartOptions | None = None,
) -> ASCIIStackedBar:
    """Create ASCIIStackedBar from phase breakdown data.

    Args:
        data: Sequence of StackedBarData instances.
        title: Optional chart title.
        options: Chart rendering options.

    Returns:
        Configured ASCIIStackedBar instance.
    """
    return ASCIIStackedBar(data=data, title=title, options=options)
