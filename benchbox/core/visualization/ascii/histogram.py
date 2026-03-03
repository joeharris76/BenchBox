"""ASCII vertical bar histogram for query latency visualization."""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

from benchbox.core.visualization.ascii.base import (
    DEFAULT_PALETTE,
    TRUNCATION_MARKER,
    ASCIIChartBase,
    ASCIIChartOptions,
    outlier_severity_markers,
    robust_p95,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class HistogramBar:
    """Data for a single histogram bar."""

    query_id: str
    latency_ms: float
    platform: str | None = None
    error: float | None = None
    is_best: bool = False
    is_worst: bool = False


class ASCIIQueryHistogram(ASCIIChartBase):
    """Vertical bar histogram showing per-query latency in ASCII.

    Renders vertical bars with query IDs below, suitable for terminal display.
    Automatically splits into multiple charts when queries exceed max_per_chart.

    Example output:
    ```
    Query Latency Histogram (ms)
    ────────────────────────────────────────
         ▄▄                    ▄▄
         ██ ▄▄    ▄▄    ▄▄    ██
         ██ ██ ▄▄ ██    ██ ▄▄ ██
    ▄▄   ██ ██ ██ ██ ▄▄ ██ ██ ██
    ██   ██ ██ ██ ██ ██ ██ ██ ██
    ──────────────────────────────
    Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10
                                 ·····Mean
    ```
    """

    DEFAULT_MAX_QUERIES = 33

    def __init__(
        self,
        data: Sequence[HistogramBar],
        title: str | None = None,
        y_label: str = "Execution Time (ms)",
        sort_by: str = "query_id",
        max_per_chart: int = DEFAULT_MAX_QUERIES,
        show_mean_line: bool = True,
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        """Initialize the histogram.

        Args:
            data: Query latency data points.
            title: Chart title.
            y_label: Y-axis label.
            sort_by: Sort order - "query_id" (natural) or "latency" (ranked).
            max_per_chart: Maximum queries per chart before splitting.
            show_mean_line: Whether to show a horizontal mean line.
            options: Chart rendering options.
            metadata: Optional chart metadata (scale_factor, platform_version, tuning).
        """
        super().__init__(options, metadata=metadata)
        self.data = list(data)
        self.title = title or "Query Latency Histogram"
        self.y_label = y_label
        self.sort_by = sort_by
        self.max_per_chart = max(5, max_per_chart)
        self.show_mean_line = show_mean_line
        self._scale_max = 0.0
        self._global_max = 0.0
        self._capping_active = False

    def render(self) -> str:
        """Render the histogram as a string (may contain multiple charts)."""
        self._detect_capabilities()

        if not self.data:
            return "No data to display"

        # Detect multi-platform mode
        platforms = sorted({d.platform for d in self.data if d.platform})
        self._use_grouped = len(platforms) > 1
        self._platforms = platforms

        sorted_data = self._sort_data()

        # For grouped charts, cap queries per chunk so bars stay within chart width.
        # Minimum group width is 1 char per platform + 1 gap; compute how many queries fit.
        if self._use_grouped and platforms:
            _bar_area = self.options.get_effective_width() - 8 - 2
            _max_q = max(1, _bar_area // (len(platforms) + 1))
            effective_max = min(self.max_per_chart, _max_q)
        else:
            effective_max = self.max_per_chart

        chunks = self._chunk_data(sorted_data, effective_max)

        # Calculate global statistics for consistent scaling
        all_latencies = [d.latency_ms for d in sorted_data]
        global_max = max(all_latencies) if all_latencies else 1
        global_mean = sum(all_latencies) / len(all_latencies) if all_latencies else 0

        # Detect outliers via IQR and cap scale to avoid compression
        self._outlier_ids: set[str] = set()
        self._outlier_bar_keys: set[tuple[str | None, str]] = set()
        scale_max = global_max
        if len(all_latencies) >= 4:
            s = sorted(all_latencies)
            n = len(s)
            q1 = s[n // 4]
            q3 = s[3 * n // 4]
            iqr = q3 - q1
            upper_fence = q3 + 1.5 * iqr
            # Zero-heavy fallback: when IQR collapses to 0 (Q1=Q3=0),
            # upper_fence is 0 and no outliers are detected. Use robust_p95 instead.
            if upper_fence <= 0:
                p95 = robust_p95(all_latencies)
                if p95 > 0:
                    upper_fence = p95 * 1.5
            for d in sorted_data:
                if d.latency_ms > upper_fence:
                    self._outlier_ids.add(d.query_id)
                    self._outlier_bar_keys.add((d.platform, d.query_id))
            # Cap scale at fence so outliers don't compress the rest
            if upper_fence > 0 and global_max > upper_fence * 1.5:
                scale_max = upper_fence * 1.5

        self._scale_max = scale_max
        self._global_max = global_max
        self._capping_active = self._scale_max < self._global_max

        rendered_charts: list[str] = []
        for chunk_idx, chunk in enumerate(chunks):
            if len(chunks) > 1:
                # Use actual query IDs for range labels
                first_id = chunk[0].query_id
                last_id = chunk[-1].query_id
                chunk_title = f"{self.title} ({first_id}-{last_id})"
            else:
                chunk_title = self.title

            chart_str = self._render_chunk(
                chunk,
                chunk_title,
                scale_max,
                global_mean,
            )
            rendered_charts.append(chart_str)

        return "\n\n".join(rendered_charts)

    def _sort_data(self) -> list[HistogramBar]:
        """Sort data according to sort_by setting."""
        if self.sort_by == "latency":
            return sorted(self.data, key=lambda d: d.latency_ms, reverse=True)
        return sorted(self.data, key=lambda d: self._natural_sort_key(d.query_id))

    @staticmethod
    def _natural_sort_key(query_id: str) -> tuple[str, int]:
        """Natural sort key for query IDs (Q1, Q2, Q10 not Q1, Q10, Q2)."""
        match = re.match(r"([A-Za-z]*)(\d+)", query_id)
        if match:
            prefix, num = match.groups()
            return (prefix.upper(), int(num))
        return (query_id.upper(), 0)

    @staticmethod
    def _compact_query_label(query_id: str, width: int) -> str:
        """Compact a query label to fit width without collapsing numeric IDs."""
        if width <= 0:
            return ""

        label = str(query_id)
        if len(label) <= width:
            return label

        match = re.fullmatch(r"([A-Za-z]+)(\d+)", label)
        if match:
            _, number = match.groups()
            if len(number) >= width:
                return number[-width:]
            return (label[0] + number)[-width:]

        return label[:width]

    def _chunk_data(self, sorted_data: list[HistogramBar], max_queries: int | None = None) -> list[list[HistogramBar]]:
        """Split data into chunks for multi-chart rendering.

        For multi-platform data, chunks are split by unique query count (not bar count),
        so all platforms for a query stay in the same chunk.

        Args:
            sorted_data: Sorted histogram bars.
            max_queries: Override for max queries per chunk (default: self.max_per_chart).
        """
        limit = max_queries if max_queries is not None else self.max_per_chart
        if not self._use_grouped:
            if len(sorted_data) <= limit:
                return [sorted_data]
            chunks: list[list[HistogramBar]] = []
            for i in range(0, len(sorted_data), limit):
                chunks.append(sorted_data[i : i + limit])
            return chunks

        # Multi-platform: chunk by unique query IDs
        unique_queries = list(dict.fromkeys(d.query_id for d in sorted_data))
        if len(unique_queries) <= limit:
            return [sorted_data]

        chunks = []
        for i in range(0, len(unique_queries), limit):
            chunk_qids = set(unique_queries[i : i + limit])
            chunk_bars = [d for d in sorted_data if d.query_id in chunk_qids]
            chunks.append(chunk_bars)
        return chunks

    def _is_outlier(self, datum: HistogramBar) -> bool:
        """Check whether a specific bar is an outlier."""
        return (datum.platform, datum.query_id) in self._outlier_bar_keys

    def _get_outlier_char(self, no_color: bool, used_fills: set[str] | None = None) -> str:
        """Select an outlier glyph that stays distinct from normal series fills."""
        if not no_color:
            return "▓" if self.options.use_unicode else "#"

        used = used_fills or set()
        if self.options.use_unicode:
            # Prefer shaded/symbol glyphs not used in the default fill palette.
            candidates = ("▩", "▨", "▥", "▤", "◈", "◆")
            for ch in candidates:
                if ch not in used:
                    return ch
            return "◆"

        candidates = ("!", "%", "@", "~")
        for ch in candidates:
            if ch not in used:
                return ch
        return "!"

    def _render_chunk(
        self,
        chunk: list[HistogramBar],
        title: str,
        global_max: float,
        global_mean: float,
    ) -> str:
        """Render a single chunk of data as a vertical bar chart."""
        if self._use_grouped:
            return self._render_grouped_chunk(chunk, title, global_max, global_mean)
        return self._render_simple_chunk(chunk, title, global_max, global_mean)

    def _render_simple_chunk(
        self,
        chunk: list[HistogramBar],
        title: str,
        global_max: float,
        global_mean: float,
    ) -> str:
        """Render a single-platform chunk as vertical bars."""
        colors = self.options.get_colors()
        blocks = self.options.get_vertical_block_chars()
        width = self.options.get_effective_width()

        lines: list[str] = []

        title_text = f"{title} ({self.y_label})"
        lines.append(self._render_title(title_text, width))
        subtitle = self._render_subtitle(width)
        if subtitle:
            lines.append(subtitle)
        lines.append(self._render_horizontal_line(width))

        num_bars = len(chunk)
        y_axis_width = 8
        bar_area_width = width - y_axis_width - 2

        bar_spacing = max(1, bar_area_width // num_bars)
        bar_width = max(1, bar_spacing - 1)

        chart_height = 12
        normalized = self._normalize_bars(chunk, global_max, chart_height)
        mean_row = round((global_mean / global_max) * chart_height) if global_max > 0 else 0

        palette = list(DEFAULT_PALETTE)
        no_color = not self.options.use_color

        chart_rows = self._build_simple_chart_rows(
            chunk,
            normalized,
            chart_height,
            y_axis_width,
            bar_width,
            mean_row,
            palette,
            no_color,
            colors,
            blocks,
            global_max,
        )
        lines.extend(chart_rows)

        lines.append(" " * y_axis_width + self._render_horizontal_line(bar_area_width))

        label_row: list[str] = [" " * y_axis_width]
        for datum in chunk:
            label = self._compact_query_label(datum.query_id, bar_width)
            label_row.append(label.center(bar_width))
            label_row.append(" ")
        lines.append("".join(label_row).rstrip())

        lines.append(self._render_axis_label("Query ID", width, axis="x"))

        footer = self._build_simple_footer(chunk, global_mean, no_color, colors, y_axis_width)
        if footer:
            lines.append("")
            lines.append(footer)

        return "\n".join(lines)

    def _normalize_bars(self, chunk: list[HistogramBar], global_max: float, chart_height: int) -> list[float]:
        """Normalize bar heights to chart row units."""
        if global_max > 0:
            normalized = [(d.latency_ms / global_max) * chart_height for d in chunk]
            return [max(0.125, n) if n > 0 else 0 for n in normalized]
        return [0] * len(chunk)

    def _build_simple_chart_rows(
        self,
        chunk: list[HistogramBar],
        normalized: list[float],
        chart_height: int,
        y_axis_width: int,
        bar_width: int,
        mean_row: int,
        palette: list[str],
        no_color: bool,
        colors: object,
        blocks: list[str],
        global_max: float = 0,
    ) -> list[str]:
        """Build the vertical bar chart rows for single-platform mode."""
        chart_rows: list[str] = []
        for row in range(chart_height, 0, -1):
            row_chars: list[str] = [self._format_y_label(row, chart_height, y_axis_width, global_max)]

            for i, datum in enumerate(chunk):
                colored_block = self._render_simple_bar_cell(
                    row,
                    normalized[i],
                    datum,
                    bar_width,
                    mean_row,
                    palette,
                    no_color,
                    colors,
                    blocks,
                    chart_height,
                )
                row_chars.append(colored_block)
                row_chars.append(" ")

            chart_rows.append("".join(row_chars).rstrip())
        return chart_rows

    def _format_y_label(self, row: int, chart_height: int, y_axis_width: int, global_max: float) -> str:
        """Format the Y-axis label for a given chart row."""
        if row == chart_height or row == chart_height // 2 or row == 1:
            y_value = (row / chart_height) * global_max
            return self._format_value(y_value).rjust(y_axis_width - 1) + " "
        return " " * y_axis_width

    def _render_simple_bar_cell(
        self,
        row: int,
        bar_height: float,
        datum: HistogramBar,
        bar_width: int,
        mean_row: int,
        palette: list[str],
        no_color: bool,
        colors: object,
        blocks: list[str],
        chart_height: int = 12,
    ) -> str:
        """Render a single bar cell in a simple histogram row."""
        is_outlier = self._is_outlier(datum)
        is_truncated = self._capping_active and datum.latency_ms > self._scale_max
        bar_color = self._simple_bar_color(datum, palette)
        outlier_char = self._get_outlier_char(no_color=no_color)
        fill_char = outlier_char if is_outlier else blocks[-1]

        if row <= math.ceil(bar_height):
            block = self._compute_bar_block(row, bar_height, fill_char, is_outlier, bar_width, blocks)
            # Top row of a truncated bar: append severity markers
            if is_truncated and row == chart_height:
                markers = outlier_severity_markers(datum.latency_ms, self._scale_max)
                if markers and len(markers) < bar_width:
                    block = block[: bar_width - len(markers)] + markers
                elif markers and bar_width >= 2:
                    # Very narrow bars: use as many markers as fit, keep 1 fill char
                    trimmed = markers[: bar_width - 1]
                    block = block[:1] + trimmed
            return colors.colorize(block, fg_color=bar_color)

        if self.show_mean_line and row == mean_row:
            return colors.colorize("·" * bar_width, fg_color="#6b7075")
        return " " * bar_width

    def _simple_bar_color(self, datum: HistogramBar, palette: list[str]) -> str:
        """Determine bar color for single-platform mode."""
        if datum.is_best:
            return "#1b9e77"
        if datum.is_worst:
            return "#d95f02"
        return palette[0]

    def _compute_bar_block(
        self, row: int, bar_height: float, fill_char: str, is_outlier: bool, bar_width: int, blocks: list[str]
    ) -> str:
        """Compute the block string for a bar cell at a given row."""
        if row <= int(bar_height):
            return fill_char * bar_width
        if is_outlier:
            return fill_char * bar_width
        partial = int((bar_height - int(bar_height)) * 8)
        if partial > 0 and partial < len(blocks):
            return blocks[partial] * bar_width
        return fill_char * bar_width

    def _build_simple_footer(
        self,
        chunk: list[HistogramBar],
        global_mean: float,
        no_color: bool,
        colors: object,
        y_axis_width: int,
    ) -> str:
        """Build the footer line with mean and legend markers."""
        footer_parts: list[str] = []
        if self.show_mean_line and global_mean > 0:
            mean_text = f"····· Mean: {self._format_value(global_mean)} ms"
            footer_parts.append(colors.colorize(mean_text, fg_color="#6b7075"))

        has_best = any(d.is_best for d in chunk)
        has_worst = any(d.is_worst for d in chunk)
        has_outlier = any(self._is_outlier(d) for d in chunk)
        has_truncated = self._capping_active and any(d.latency_ms > self._scale_max for d in chunk)

        if has_best:
            best_marker = self.options.get_series_marker(0)
            footer_parts.append(f"{colors.colorize(best_marker, fg_color='#1b9e77')} Best")
        if has_worst:
            worst_marker = self.options.get_series_marker(1)
            footer_parts.append(f"{colors.colorize(worst_marker, fg_color='#d95f02')} Worst")
        if has_outlier:
            outlier_char = self._get_outlier_char(no_color=no_color)
            footer_parts.append(f"{colors.colorize(outlier_char, fg_color='#666666')} Outlier")
        if has_truncated:
            footer_parts.append(f"{TRUNCATION_MARKER} Truncated")

        if footer_parts:
            return " " * y_axis_width + "  ".join(footer_parts)
        return ""

    def _render_grouped_chunk(
        self,
        chunk: list[HistogramBar],
        title: str,
        global_max: float,
        global_mean: float,
    ) -> str:
        """Render a multi-platform chunk with grouped bars per query."""
        colors = self.options.get_colors()
        blocks = self.options.get_vertical_block_chars()
        width = self.options.get_effective_width()
        palette = list(DEFAULT_PALETTE)

        platform_colors, platform_fills = self._build_platform_maps(palette)
        no_color = not self.options.use_color

        lines: list[str] = []

        title_text = f"{title} ({self.y_label})"
        lines.append(self._render_title(title_text, width))
        subtitle = self._render_subtitle(width)
        if subtitle:
            lines.append(subtitle)
        lines.append(self._render_horizontal_line(width))

        unique_queries = list(dict.fromkeys(d.query_id for d in chunk))
        num_platforms = len(self._platforms)
        num_queries = len(unique_queries)

        y_axis_width = 8
        bar_area_width = width - y_axis_width - 2

        group_width = max(num_platforms + 1, bar_area_width // num_queries)
        sub_bar_width = max(1, (group_width - 1) // num_platforms)

        chart_height = 12

        lookup: dict[tuple[str, str], HistogramBar] = {}
        for d in chunk:
            if d.platform:
                lookup[(d.platform, d.query_id)] = d

        mean_row = round((global_mean / global_max) * chart_height) if global_max > 0 else 0

        chart_rows = self._build_grouped_chart_rows(
            unique_queries,
            lookup,
            chart_height,
            y_axis_width,
            sub_bar_width,
            global_max,
            mean_row,
            platform_colors,
            platform_fills,
            no_color,
            colors,
            blocks,
            palette,
        )
        lines.extend(chart_rows)

        lines.append(" " * y_axis_width + self._render_horizontal_line(bar_area_width))

        label_row: list[str] = [" " * y_axis_width]
        total_group_width = num_platforms * sub_bar_width
        for qid in unique_queries:
            label = self._compact_query_label(qid, total_group_width)
            label_row.append(label.center(total_group_width))
            label_row.append(" ")
        lines.append("".join(label_row).rstrip())

        lines.append(self._render_axis_label("Query ID", width, axis="x"))

        footer = self._build_grouped_footer(
            chunk,
            global_mean,
            platform_colors,
            platform_fills,
            no_color,
            colors,
            y_axis_width,
            width,
        )
        if footer:
            lines.append("")
            lines.append(footer)

        return "\n".join(lines)

    def _build_platform_maps(self, palette: list[str]) -> tuple[dict[str, str], dict[str, str]]:
        """Build platform-to-color and platform-to-fill mappings."""
        platform_colors: dict[str, str] = {}
        platform_fills: dict[str, str] = {}
        for i, platform in enumerate(self._platforms):
            platform_colors[platform] = palette[i % len(palette)]
            platform_fills[platform] = self.options.get_series_fill(i)
        return platform_colors, platform_fills

    def _build_grouped_chart_rows(
        self,
        unique_queries: list[str],
        lookup: dict[tuple[str, str], HistogramBar],
        chart_height: int,
        y_axis_width: int,
        sub_bar_width: int,
        global_max: float,
        mean_row: int,
        platform_colors: dict[str, str],
        platform_fills: dict[str, str],
        no_color: bool,
        colors: object,
        blocks: list[str],
        palette: list[str],
    ) -> list[str]:
        """Build the vertical bar chart rows for grouped (multi-platform) mode."""
        chart_rows: list[str] = []
        for row in range(chart_height, 0, -1):
            row_chars: list[str] = [self._format_y_label(row, chart_height, y_axis_width, global_max)]

            for qid in unique_queries:
                for platform in self._platforms:
                    datum = lookup.get((platform, qid))
                    if not datum:
                        row_chars.append(" " * sub_bar_width)
                        continue

                    cell = self._render_grouped_bar_cell(
                        row,
                        datum,
                        sub_bar_width,
                        global_max,
                        chart_height,
                        mean_row,
                        platform_colors,
                        platform_fills,
                        no_color,
                        colors,
                        blocks,
                        palette,
                    )
                    row_chars.append(cell)

                row_chars.append(" ")  # Gap between query groups

            chart_rows.append("".join(row_chars).rstrip())
        return chart_rows

    def _render_grouped_bar_cell(
        self,
        row: int,
        datum: HistogramBar,
        sub_bar_width: int,
        global_max: float,
        chart_height: int,
        mean_row: int,
        platform_colors: dict[str, str],
        platform_fills: dict[str, str],
        no_color: bool,
        colors: object,
        blocks: list[str],
        palette: list[str],
    ) -> str:
        """Render a single bar cell in a grouped histogram row."""
        raw_height = (datum.latency_ms / global_max) * chart_height if global_max > 0 else 0
        bar_height = max(0.125, raw_height) if raw_height > 0 else 0
        is_outlier = self._is_outlier(datum)
        is_truncated = self._capping_active and datum.latency_ms > self._scale_max

        bar_color = platform_colors.get(datum.platform or "", palette[0])
        outlier_char = self._get_outlier_char(no_color=no_color, used_fills=set(platform_fills.values()))
        platform_fill = platform_fills.get(datum.platform or "", blocks[-1])
        fill_char = outlier_char if is_outlier else (platform_fill if no_color else blocks[-1])

        if row <= math.ceil(bar_height):
            block = self._compute_bar_block(row, bar_height, fill_char, is_outlier, sub_bar_width, blocks)
            # Top row of a truncated bar: append severity markers
            if is_truncated and row == chart_height:
                markers = outlier_severity_markers(datum.latency_ms, self._scale_max)
                if markers and len(markers) < sub_bar_width:
                    block = block[: sub_bar_width - len(markers)] + markers
                elif markers and sub_bar_width >= 2:
                    trimmed = markers[: sub_bar_width - 1]
                    block = block[:1] + trimmed
            return colors.colorize(block, fg_color=bar_color)

        if self.show_mean_line and row == mean_row:
            return colors.colorize("·" * sub_bar_width, fg_color="#6b7075")
        return " " * sub_bar_width

    def _build_grouped_footer(
        self,
        chunk: list[HistogramBar],
        global_mean: float,
        platform_colors: dict[str, str],
        platform_fills: dict[str, str],
        no_color: bool,
        colors: object,
        y_axis_width: int,
        width: int,
    ) -> str:
        """Build the footer line(s) with mean and platform legend for grouped mode.

        Long legends are wrapped to multiple lines so they stay within chart width.
        """
        footer_parts: list[str] = []
        if self.show_mean_line and global_mean > 0:
            mean_text = f"····· Mean: {self._format_value(global_mean)} ms"
            footer_parts.append(colors.colorize(mean_text, fg_color="#6b7075"))

        for i, platform in enumerate(self._platforms):
            color = platform_colors[platform]
            series_glyph = platform_fills[platform] if no_color else self.options.get_series_marker(i)
            footer_parts.append(f"{colors.colorize(series_glyph, fg_color=color)} {platform}")

        has_outlier = any(self._is_outlier(d) for d in chunk)
        if has_outlier:
            outlier_char = self._get_outlier_char(no_color=no_color, used_fills=set(platform_fills.values()))
            footer_parts.append(f"{colors.colorize(outlier_char, fg_color='#666666')} Outlier")

        if not footer_parts:
            return ""

        # Wrap footer parts into lines that stay within chart width
        ansi_re = re.compile(r"\x1b\[[0-9;]*m")
        indent = " " * y_axis_width
        lines: list[str] = []
        current = indent
        current_visible = y_axis_width
        for part in footer_parts:
            part_visible = len(ansi_re.sub("", part))
            if current_visible > y_axis_width:
                if current_visible + 2 + part_visible > width:
                    lines.append(current)
                    current = indent + part
                    current_visible = y_axis_width + part_visible
                else:
                    current += "  " + part
                    current_visible += 2 + part_visible
            else:
                current += part
                current_visible += part_visible
        if current.strip():
            lines.append(current)
        return "\n".join(lines)


def from_query_latency_data(
    data: Sequence,
    title: str | None = None,
    y_label: str = "Execution Time (ms)",
    sort_by: str = "query_id",
    max_per_chart: int = ASCIIQueryHistogram.DEFAULT_MAX_QUERIES,
    show_mean_line: bool = True,
    options: ASCIIChartOptions | None = None,
) -> ASCIIQueryHistogram:
    """Create ASCIIQueryHistogram from objects with query_id/latency_ms attributes."""
    converted: list[HistogramBar] = []
    for item in data:
        if isinstance(item, HistogramBar):
            converted.append(item)
        else:
            converted.append(
                HistogramBar(
                    query_id=getattr(item, "query_id", str(item)),
                    latency_ms=getattr(item, "latency_ms", 0),
                    platform=getattr(item, "platform", None),
                    error=getattr(item, "error", None),
                    is_best=getattr(item, "is_best", False),
                    is_worst=getattr(item, "is_worst", False),
                )
            )

    return ASCIIQueryHistogram(
        data=converted,
        title=title,
        y_label=y_label,
        sort_by=sort_by,
        max_per_chart=max_per_chart,
        show_mean_line=show_mean_line,
        options=options,
    )
