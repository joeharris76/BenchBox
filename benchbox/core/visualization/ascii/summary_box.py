"""ASCII summary box renderer for aggregate benchmark statistics."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from benchbox.core.visualization.ascii.base import ASCIIChartBase, ASCIIChartOptions, TerminalColors


@dataclass
class SummaryStats:
    """Aggregate statistics for a summary box.

    For single-run summaries, only baseline fields are used.
    For comparison summaries, both baseline and comparison fields are used.
    """

    title: str = "Benchmark Summary"

    # Aggregate metrics
    geo_mean_ms: float | None = None
    median_ms: float | None = None
    total_time_ms: float | None = None

    # Comparison metrics (only for two-run comparisons)
    geo_mean_baseline_ms: float | None = None
    geo_mean_comparison_ms: float | None = None
    total_time_baseline_ms: float | None = None
    total_time_comparison_ms: float | None = None
    baseline_name: str = "Baseline"
    comparison_name: str = "Comparison"

    # Counts
    num_queries: int = 0
    num_improved: int = 0
    num_stable: int = 0
    num_regressed: int = 0

    # Best/worst queries
    best_queries: list[tuple[str, float]] = field(default_factory=list)
    worst_queries: list[tuple[str, float]] = field(default_factory=list)

    # System environment info (displayed in right column)
    # Expected keys: "OS", "Python", "CPUs", "Memory"
    environment: dict[str, str] | None = None

    @property
    def is_comparison(self) -> bool:
        """Whether this is a comparison summary (two runs)."""
        return self.geo_mean_baseline_ms is not None or self.total_time_baseline_ms is not None


class ASCIISummaryBox(ASCIIChartBase):
    """Bordered summary panel with aggregate benchmark statistics.

    Displays key metrics in a box-drawing bordered panel. Supports
    both single-run and comparison summaries.

    Example output:
    ```
    ┌──────────────────────────────────────┐
    │       Benchmark Summary              │
    ├──────────────────────────────────────┤
    │  Geo Mean:  142.3ms → 98.7ms  -30.6%│
    │  Total:     3.2s → 2.1s       -34.4%│
    ├──────────────────────────────────────┤
    │  5 improved  12 stable  5 regressed  │
    ├──────────────────────────────────────┤
    │  Best:  Q6 (-57.2%), Q14 (-38.1%)   │
    │  Worst: Q21 (+726%), Q17 (+23.4%)   │
    └──────────────────────────────────────┘
    ```
    """

    def __init__(
        self,
        stats: SummaryStats,
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.stats = stats

    # Minimum inner width required for two-column layout
    _MIN_TWO_COL_INNER = 56

    def render(self) -> str:
        """Render the summary box as a string."""
        self._detect_capabilities()

        colors = self.options.get_colors()
        box = self.options.get_box_chars()
        width = self.options.get_effective_width()

        # Inner width (minus left and right borders + padding)
        inner = width - 4  # "│ " + content + " │"
        if inner < 20:
            inner = 20

        two_col = self.stats.environment and inner >= self._MIN_TWO_COL_INNER

        lines: list[str] = []

        # Top border
        lines.append(f"{box['tl']}{box['h'] * (inner + 2)}{box['tr']}")

        # Title
        title_text = self.stats.title
        title_centered = title_text.center(inner)
        bold_title = colors.bold() + title_centered + colors.reset()
        lines.append(f"{box['v']} {bold_title} {box['v']}")

        # Aggregate metrics (with optional environment column)
        if two_col:
            assert self.stats.environment is not None
            left_width = inner * 5 // 9  # ~55% for metrics
            right_width = inner - left_width - 1  # -1 for divider

            # Separator with column divider
            lines.append(
                f"{box['lm']}{box['h'] * (left_width + 1)}{box['tm']}{box['h'] * (right_width + 1)}{box['rm']}"
            )

            if self.stats.is_comparison:
                metric_lines = self._build_metric_texts_comparison(colors)
            else:
                metric_lines = self._build_metric_texts_single()
            env_lines = self._build_env_lines(colors)
            env_lines_visible = self._build_env_lines_visible()

            # Pad to equal length
            row_count = max(len(metric_lines), len(env_lines))
            while len(metric_lines) < row_count:
                metric_lines.append("")
            while len(env_lines) < row_count:
                env_lines.append("")
                env_lines_visible.append("")

            for left_text, right_text, right_visible_text in zip(metric_lines, env_lines, env_lines_visible):
                left_visible = len(left_text)
                left_pad = max(0, left_width - left_visible)
                right_visible = len(right_visible_text)
                right_pad = max(0, right_width - right_visible)
                lines.append(
                    f"{box['v']} {left_text}{' ' * left_pad}{box['v']} {right_text}{' ' * right_pad}{box['v']}"
                )

            # Separator closing the two-column section
            lines.append(
                f"{box['lm']}{box['h'] * (left_width + 1)}{box['bm']}{box['h'] * (right_width + 1)}{box['rm']}"
            )
        else:
            # Separator
            lines.append(f"{box['lm']}{box['h'] * (inner + 2)}{box['rm']}")

            if self.stats.is_comparison:
                lines.extend(self._render_comparison_metrics(box, colors, inner))
            else:
                lines.extend(self._render_single_metrics(box, colors, inner))

        # Separator before counts (comparison mode only)
        if self.stats.is_comparison and self.stats.num_queries > 0:
            if not two_col:
                lines.append(f"{box['lm']}{box['h'] * (inner + 2)}{box['rm']}")
            # Counts row
            improved = colors.colorize(f"{self.stats.num_improved} improved", fg_color="#66a61e")
            regressed = colors.colorize(f"{self.stats.num_regressed} regressed", fg_color="#d95f02")
            counts_text = f"{improved}  {self.stats.num_stable} stable  {regressed}"
            visible_len = (
                len(f"{self.stats.num_improved} improved")
                + 2
                + len(f"{self.stats.num_stable} stable")
                + 2
                + len(f"{self.stats.num_regressed} regressed")
            )
            padding = max(0, inner - visible_len)
            lines.append(f"{box['v']} {counts_text}{' ' * padding} {box['v']}")

        # Best/worst queries
        if self.stats.best_queries or self.stats.worst_queries:
            if not (two_col and not self.stats.is_comparison):
                lines.append(f"{box['lm']}{box['h'] * (inner + 2)}{box['rm']}")

            if self.stats.best_queries:
                best_items = ", ".join(
                    f"{q} ({v:+.1f}%)" if self.stats.is_comparison else f"{q} ({self._format_time(v)})"
                    for q, v in self.stats.best_queries[:3]
                )
                best_label = "Best: "
                best_line = f"{best_label}{best_items}"
                best_colored = colors.colorize(best_label, fg_color="#66a61e") + best_items
                visible_len = len(best_line)
                padding = max(0, inner - visible_len)
                lines.append(f"{box['v']} {best_colored}{' ' * padding} {box['v']}")

            if self.stats.worst_queries:
                worst_items = ", ".join(
                    f"{q} ({v:+.1f}%)" if self.stats.is_comparison else f"{q} ({self._format_time(v)})"
                    for q, v in self.stats.worst_queries[:3]
                )
                worst_label = "Worst: "
                worst_line = f"{worst_label}{worst_items}"
                worst_colored = colors.colorize(worst_label, fg_color="#d95f02") + worst_items
                visible_len = len(worst_line)
                padding = max(0, inner - visible_len)
                lines.append(f"{box['v']} {worst_colored}{' ' * padding} {box['v']}")

        # Bottom border
        lines.append(f"{box['bl']}{box['h'] * (inner + 2)}{box['br']}")

        return "\n".join(lines)

    def _render_comparison_metrics(
        self,
        box: dict[str, str],
        colors: TerminalColors,
        inner: int,
    ) -> list[str]:
        """Render comparison metrics (two runs)."""
        lines: list[str] = []

        if self.stats.geo_mean_baseline_ms is not None and self.stats.geo_mean_comparison_ms is not None:
            b_val = self.stats.geo_mean_baseline_ms
            c_val = self.stats.geo_mean_comparison_ms
            pct = ((c_val - b_val) / b_val * 100) if b_val != 0 else 0.0
            arrow = "\u2192" if self.options.use_unicode else "->"
            metric_text = f"Geo Mean:  {self._format_value(b_val)}ms {arrow} {self._format_value(c_val)}ms"
            pct_text = self._format_pct_colored(pct, colors)
            visible_len = len(metric_text) + len(f"{pct:+.1f}%")
            padding = max(0, inner - visible_len)
            leader = self._dot_leader(padding, colors)
            lines.append(f"{box['v']} {metric_text}{leader}{pct_text} {box['v']}")

        if self.stats.total_time_baseline_ms is not None and self.stats.total_time_comparison_ms is not None:
            b_val = self.stats.total_time_baseline_ms
            c_val = self.stats.total_time_comparison_ms
            pct = ((c_val - b_val) / b_val * 100) if b_val != 0 else 0.0
            arrow = "\u2192" if self.options.use_unicode else "->"
            b_str = self._format_time(b_val)
            c_str = self._format_time(c_val)
            metric_text = f"Total:     {b_str} {arrow} {c_str}"
            pct_text = self._format_pct_colored(pct, colors)
            visible_len = len(metric_text) + len(f"{pct:+.1f}%")
            padding = max(0, inner - visible_len)
            leader = self._dot_leader(padding, colors)
            lines.append(f"{box['v']} {metric_text}{leader}{pct_text} {box['v']}")

        if self.stats.num_queries > 0:
            queries_text = f"Queries:   {self.stats.num_queries}"
            padding = max(0, inner - len(queries_text))
            lines.append(f"{box['v']} {queries_text}{' ' * padding} {box['v']}")

        return lines

    def _render_single_metrics(
        self,
        box: dict[str, str],
        colors: TerminalColors,
        inner: int,
    ) -> list[str]:
        """Render single-run metrics."""
        lines: list[str] = []

        if self.stats.geo_mean_ms is not None:
            text = f"Geo Mean:  {self._format_value(self.stats.geo_mean_ms)}ms"
            padding = max(0, inner - len(text))
            lines.append(f"{box['v']} {text}{' ' * padding} {box['v']}")

        if self.stats.median_ms is not None:
            text = f"Median:    {self._format_value(self.stats.median_ms)}ms"
            padding = max(0, inner - len(text))
            lines.append(f"{box['v']} {text}{' ' * padding} {box['v']}")

        if self.stats.total_time_ms is not None:
            text = f"Total:     {self._format_time(self.stats.total_time_ms)}"
            padding = max(0, inner - len(text))
            lines.append(f"{box['v']} {text}{' ' * padding} {box['v']}")

        if self.stats.num_queries > 0:
            text = f"Queries:   {self.stats.num_queries}"
            padding = max(0, inner - len(text))
            lines.append(f"{box['v']} {text}{' ' * padding} {box['v']}")

        return lines

    def _build_metric_texts_single(self) -> list[str]:
        """Build plain metric text lines for single-run (no borders)."""
        texts: list[str] = []
        if self.stats.geo_mean_ms is not None:
            texts.append(f"Geo Mean:  {self._format_value(self.stats.geo_mean_ms)}ms")
        if self.stats.median_ms is not None:
            texts.append(f"Median:    {self._format_value(self.stats.median_ms)}ms")
        if self.stats.total_time_ms is not None:
            texts.append(f"Total:     {self._format_time(self.stats.total_time_ms)}")
        if self.stats.num_queries > 0:
            texts.append(f"Queries:   {self.stats.num_queries}")
        return texts

    def _build_metric_texts_comparison(self, colors: TerminalColors) -> list[str]:
        """Build plain metric text lines for comparison (no borders)."""
        texts: list[str] = []
        arrow = "\u2192" if self.options.use_unicode else "->"
        if self.stats.geo_mean_baseline_ms is not None and self.stats.geo_mean_comparison_ms is not None:
            b, c = self.stats.geo_mean_baseline_ms, self.stats.geo_mean_comparison_ms
            texts.append(f"Geo Mean:  {self._format_value(b)}ms {arrow} {self._format_value(c)}ms")
        if self.stats.total_time_baseline_ms is not None and self.stats.total_time_comparison_ms is not None:
            b, c = self.stats.total_time_baseline_ms, self.stats.total_time_comparison_ms
            b_str, c_str = self._format_time(b), self._format_time(c)
            texts.append(f"Total:     {b_str} {arrow} {c_str}")
        if self.stats.num_queries > 0:
            texts.append(f"Queries:   {self.stats.num_queries}")
        return texts

    def _build_env_lines(self, colors: TerminalColors) -> list[str]:
        """Build environment info text lines for the right column."""
        if not self.stats.environment:
            return []
        lines: list[str] = []
        dim = colors.colorize
        for key, value in self.stats.environment.items():
            lines.append(f"{dim(f'{key}:', fg_color='#666666')} {value}")
        return lines

    def _build_env_lines_visible(self) -> list[str]:
        """Build environment lines with visible lengths (no ANSI) for width calculations."""
        if not self.stats.environment:
            return []
        return [f"{key}: {value}" for key, value in self.stats.environment.items()]

    def _dot_leader(self, width: int, colors: TerminalColors) -> str:
        """Create a dot-leader string for visual tracing between values.

        Uses middle dot (·) in Unicode mode, period (.) in ASCII mode,
        with 1-char space gap on each side for readability.
        """
        if width <= 2:
            return " " * width
        dot_char = "·" if self.options.use_unicode else "."
        dots = dot_char * (width - 2)
        leader = f" {dots} "
        return colors.colorize(leader, fg_color="#666666")

    def _format_pct_colored(self, pct: float, colors: TerminalColors) -> str:
        """Format a percentage with color (green for improvement, red for regression)."""
        text = f"{pct:+.1f}%"
        if pct < -2:
            return colors.colorize(text, fg_color="#66a61e")
        elif pct > 2:
            return colors.colorize(text, fg_color="#d95f02")
        return text

    def _format_time(self, ms: float) -> str:
        """Format milliseconds into a human-readable time string."""
        if not math.isfinite(ms):
            return "N/A"
        if ms >= 60_000:
            return f"{ms / 60_000:.1f}min"
        if ms >= 1_000:
            return f"{ms / 1_000:.1f}s"
        return f"{ms:.1f}ms"
