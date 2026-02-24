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

        sorted_data = self._sort_by_direction()
        if not sorted_data:
            return "No data to display"

        # Layout calculation
        max_label_len = min(30, max((len(d.label) for d in sorted_data), default=2))
        annotation_width = 10
        bar_area = width - max_label_len - 1 - 1 - 1 - annotation_width
        half_bar = max(5, bar_area // 2)

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
        header_left = "Faster".rjust(max_label_len + 1 + half_bar)
        lines.append(f"{header_left}{box['v']}  Slower")

        # Determine bar fill characters
        intensity = self.options.get_intensity_chars()
        fill_chars = self._select_fill_chars(intensity)

        # Render bars
        for d in sorted_data:
            line = self._render_bar_row(
                d, max_label_len, half_bar, max_abs_pct, annotation_width, fill_chars, colors, box
            )
            lines.append(line)

        lines.append(self._render_horizontal_line(width))
        lines.append(self._render_summary_counts(colors))

        return "\n".join(lines)

    def _sort_by_direction(self) -> list[DivergingBarData]:
        """Sort data: improvements (negative) first, then regressions (positive)."""
        improvements = sorted(
            [d for d in self.data if d.pct_change < 0],
            key=lambda d: d.pct_change,
        )
        regressions = sorted(
            [d for d in self.data if d.pct_change >= 0],
            key=lambda d: d.pct_change,
        )
        return improvements + regressions

    def _select_fill_chars(self, intensity: list[str]) -> tuple[str, str, str]:
        """Select strong, medium, and weak fill characters from intensity scale."""
        strong = intensity[-1] if len(intensity) > 1 else "#"
        medium = intensity[-2] if len(intensity) > 2 else "="
        weak = intensity[1] if len(intensity) > 1 else "."
        return strong, medium, weak

    def _render_bar_row(
        self,
        d: DivergingBarData,
        max_label_len: int,
        half_bar: int,
        max_abs_pct: float,
        annotation_width: int,
        fill_chars: tuple[str, str, str],
        colors: object,
        box: dict[str, str],
    ) -> str:
        """Render a single bar row for an improvement or regression."""
        label = self._truncate_label(d.label, max_label_len).ljust(max_label_len)
        abs_pct = abs(d.pct_change)
        strong, medium, weak = fill_chars

        fill = strong if abs_pct > 50 else (medium if abs_pct > 15 else weak)

        clipped = abs_pct > self.clip_pct
        display_pct = min(abs_pct, self.clip_pct)
        bar_len = max(0, int((display_pct / max_abs_pct) * half_bar))

        left_side, right_side = self._build_bar_sides(d.pct_change, fill, bar_len, half_bar, clipped)
        colored_left, colored_right = self._colorize_sides(d.pct_change, left_side, right_side, colors)

        annotation = f"+{d.pct_change:.1f}%" if d.pct_change > 0 else f"{d.pct_change:.1f}%"
        annotation = annotation.rjust(annotation_width)

        return f"{label} {colored_left}{box['v']}{colored_right} {annotation}"

    def _build_bar_sides(
        self, pct_change: float, fill: str, bar_len: int, half_bar: int, clipped: bool
    ) -> tuple[str, str]:
        """Build left and right bar strings based on change direction."""
        bar_str = fill * bar_len
        if pct_change < 0:
            if clipped:
                bar_str = self._apply_overflow_arrow(bar_str, bar_len, self._overflow_arrow_left(), left=True)
            return bar_str.rjust(half_bar), " " * half_bar

        if clipped:
            bar_str = self._apply_overflow_arrow(bar_str, bar_len, self._overflow_arrow_right(), left=False)
        return " " * half_bar, bar_str.ljust(half_bar)

    def _apply_overflow_arrow(self, bar_str: str, bar_len: int, arrow: str, *, left: bool) -> str:
        """Apply an overflow arrow to a bar string."""
        if len(arrow) < bar_len:
            fill = bar_str[0] if bar_str else " "
            if left:
                return arrow + fill * (bar_len - len(arrow))
            return fill * (bar_len - len(arrow)) + arrow
        return arrow[:bar_len] if bar_len > 0 else ""

    def _colorize_sides(self, pct_change: float, left_side: str, right_side: str, colors: object) -> tuple[str, str]:
        """Colorize left and right bar sides based on change direction."""
        if pct_change < 0:
            return colors.colorize(left_side, fg_color="#66a61e"), right_side
        if pct_change > 0:
            return left_side, colors.colorize(right_side, fg_color="#d95f02")
        return left_side, right_side

    def _render_summary_counts(self, colors: object) -> str:
        """Render the summary counts line (improved/stable/regressed)."""
        n_improved = sum(1 for d in self.data if d.pct_change < -self._stable_threshold())
        n_regressed = sum(1 for d in self.data if d.pct_change > self._stable_threshold())
        n_stable = len(self.data) - n_improved - n_regressed
        no_color = not self.options.use_color
        if no_color:
            down = "\u2193" if self.options.use_unicode else "v"
            up = "\u2191" if self.options.use_unicode else "^"
            improved_text = f"{down}{n_improved} improved"
            regressed_text = f"{up}{n_regressed} regressed"
        else:
            improved_text = colors.colorize(f"{n_improved} improved", fg_color="#66a61e")
            regressed_text = colors.colorize(f"{n_regressed} regressed", fg_color="#d95f02")
        return f"  {improved_text}  {n_stable} stable  {regressed_text}"

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
