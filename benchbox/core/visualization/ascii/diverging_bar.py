"""ASCII diverging bar chart for regression/improvement distribution."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from benchbox.core.visualization.ascii.base import ASCIIChartBase, ASCIIChartOptions

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


@dataclass
class DivergingBarData:
    """Data for a single item in a diverging bar chart."""

    label: str
    pct_change: float  # Negative = improvement, positive = regression


class ASCIIDivergingBar(ASCIIChartBase):
    """Diverging bar chart showing percentage changes centered on zero.

    Left side = improvements (faster), right side = regressions (slower).
    Grouped by direction: improvements first (strongest to weakest),
    then regressions (weakest to strongest). Handles extreme outliers
    with overflow arrows.

    Example output:
    ```
    Regression / Improvement Distribution
    ────────────────────────────────────────────────────
                  Faster  |  Slower
    Q6   ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ |                       -57.2%
    Q14  ▓▓▓▓▓▓▓▓▓▓       |                       -38.1%
    Q3            ░░░░░░░ |                        -5.2%
    Q1                    | ░░░░░░░                +4.8%
    Q17                   | ▒▒▒▒▒▒▒▒▒▒▒▒         +23.4%
    Q21                   | ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒──►  +726.0%
    ────────────────────────────────────────────────────
    ```
    """

    # Outlier threshold: clip bars beyond this percentage
    DEFAULT_CLIP_PCT = 200.0

    def __init__(
        self,
        data: Sequence[DivergingBarData],
        title: str | None = None,
        clip_pct: float = DEFAULT_CLIP_PCT,
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.data = list(data)
        self.title = title or "Regression / Improvement Distribution"
        self.clip_pct = clip_pct

    def render(self) -> str:
        """Render the diverging bar chart as a string."""
        self._detect_capabilities()

        if not self.data:
            return "No data to display"

        colors = self.options.get_colors()
        width = self.options.get_effective_width()
        box = self.options.get_box_chars()

        # Sort by magnitude (largest absolute change first)
        # Group improvements (negative) first, then regressions (positive)
        improvements = sorted(
            [d for d in self.data if d.pct_change < 0],
            key=lambda d: d.pct_change,  # most negative first
        )
        regressions = sorted(
            [d for d in self.data if d.pct_change >= 0],
            key=lambda d: d.pct_change,  # least positive first (near zero first)
        )
        sorted_data = improvements + regressions

        if not sorted_data:
            return "No data to display"

        # Layout: label + " " + left_bar + "|" + right_bar + " " + annotation
        max_label_len = min(30, max((len(d.label) for d in sorted_data), default=2))
        annotation_width = 10  # " +1234.5%"
        # Split remaining space evenly for left and right bars
        bar_area = width - max_label_len - 1 - 1 - 1 - annotation_width  # label + spaces + center + annotation
        half_bar = max(5, bar_area // 2)

        # Find max absolute percentage for scaling (clipped)
        max_abs_pct = max(
            (min(abs(d.pct_change), self.clip_pct) for d in sorted_data),
            default=1.0,
        )
        if max_abs_pct <= 0:
            max_abs_pct = 1.0

        lines: list[str] = []

        # Title and subtitle
        lines.append(self._render_title(self.title, width))
        subtitle = self._render_subtitle(width)
        if subtitle:
            lines.append(subtitle)
        lines.append(self._render_horizontal_line(width))

        # Header showing direction
        faster_label = "Faster"
        slower_label = "Slower"
        header_left = faster_label.rjust(max_label_len + 1 + half_bar)
        header_right = f"  {slower_label}"
        lines.append(f"{header_left}{box['v']}{header_right}")

        # Determine bar fill characters
        intensity = self.options.get_intensity_chars()
        # Use different intensities based on magnitude
        strong_char = intensity[-1] if len(intensity) > 1 else "#"  # █ (strong change)
        medium_char = intensity[-2] if len(intensity) > 2 else "="  # ▓ (medium change)
        weak_char = intensity[1] if len(intensity) > 1 else "."  # ░ (small change)

        # Render bars
        for d in sorted_data:
            label = self._truncate_label(d.label, max_label_len).ljust(max_label_len)
            abs_pct = abs(d.pct_change)

            # Choose fill character based on magnitude
            if abs_pct > 50:
                fill = strong_char
            elif abs_pct > 15:
                fill = medium_char
            else:
                fill = weak_char

            # Calculate bar length
            clipped = abs_pct > self.clip_pct
            display_pct = min(abs_pct, self.clip_pct)
            bar_len = max(0, int((display_pct / max_abs_pct) * half_bar))

            # Build bar with potential overflow arrow
            if d.pct_change < 0:
                # Improvement: bar extends left from center
                bar_str = fill * bar_len
                if clipped:
                    arrow = self._overflow_arrow_left()
                    if len(arrow) < bar_len:
                        bar_str = arrow + fill * (bar_len - len(arrow))
                    else:
                        bar_str = arrow[:bar_len] if bar_len > 0 else ""
                left_side = bar_str.rjust(half_bar)
                right_side = " " * half_bar
            else:
                # Regression: bar extends right from center
                bar_str = fill * bar_len
                if clipped:
                    arrow = self._overflow_arrow_right()
                    if len(arrow) < bar_len:
                        bar_str = fill * (bar_len - len(arrow)) + arrow
                    else:
                        bar_str = arrow[:bar_len] if bar_len > 0 else ""
                left_side = " " * half_bar
                right_side = bar_str.ljust(half_bar)

            # Color the bar
            if d.pct_change < 0:
                colored_left = colors.colorize(left_side, fg_color="#66a61e")
                colored_right = right_side
            elif d.pct_change > 0:
                colored_left = left_side
                colored_right = colors.colorize(right_side, fg_color="#d95f02")
            else:
                colored_left = left_side
                colored_right = right_side

            # Format annotation
            if d.pct_change > 0:
                annotation = f"+{d.pct_change:.1f}%"
            else:
                annotation = f"{d.pct_change:.1f}%"
            annotation = annotation.rjust(annotation_width)

            lines.append(f"{label} {colored_left}{box['v']}{colored_right} {annotation}")

        lines.append(self._render_horizontal_line(width))

        # Summary counts
        n_improved = sum(1 for d in self.data if d.pct_change < -self._stable_threshold())
        n_regressed = sum(1 for d in self.data if d.pct_change > self._stable_threshold())
        n_stable = len(self.data) - n_improved - n_regressed
        improved_text = colors.colorize(f"{n_improved} improved", fg_color="#66a61e")
        regressed_text = colors.colorize(f"{n_regressed} regressed", fg_color="#d95f02")
        lines.append(f"  {improved_text}  {n_stable} stable  {regressed_text}")

        return "\n".join(lines)

    def _overflow_arrow_left(self) -> str:
        """Left-pointing overflow arrow for clipped improvements."""
        if self.options.use_unicode:
            return "\u25c4\u2500\u2500"  # ◄──
        return "<--"

    def _overflow_arrow_right(self) -> str:
        """Right-pointing overflow arrow for clipped regressions."""
        if self.options.use_unicode:
            return "\u2500\u2500\u25ba"  # ──►
        return "-->"

    def _stable_threshold(self) -> float:
        """Percentage threshold below which a change is considered stable."""
        return 2.0


def from_regression_data(
    data: Sequence,
    title: str | None = None,
    options: ASCIIChartOptions | None = None,
) -> ASCIIDivergingBar:
    """Create ASCIIDivergingBar from regression data.

    Accepts DivergingBarData objects or dicts with compatible keys.
    """
    converted: list[DivergingBarData] = []
    for item in data:
        if isinstance(item, DivergingBarData):
            converted.append(item)
        else:
            logger.warning(
                "from_regression_data: unexpected type %s, using getattr fallback",
                type(item).__name__,
            )
            converted.append(
                DivergingBarData(
                    label=getattr(item, "label", str(item)),
                    pct_change=getattr(item, "pct_change", 0),
                )
            )

    return ASCIIDivergingBar(
        data=converted,
        title=title,
        options=options,
    )
