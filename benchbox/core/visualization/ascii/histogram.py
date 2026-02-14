"""ASCII vertical bar histogram for query latency visualization."""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

from benchbox.core.visualization.ascii.base import DEFAULT_PALETTE, ASCIIChartBase, ASCIIChartOptions

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
        chunks = self._chunk_data(sorted_data)

        # Calculate global statistics for consistent scaling
        all_latencies = [d.latency_ms for d in sorted_data]
        global_max = max(all_latencies) if all_latencies else 1
        global_mean = sum(all_latencies) / len(all_latencies) if all_latencies else 0

        # Detect outliers via IQR
        self._outlier_ids: set[str] = set()
        if len(all_latencies) >= 4:
            s = sorted(all_latencies)
            n = len(s)
            q1 = s[n // 4]
            q3 = s[3 * n // 4]
            iqr = q3 - q1
            upper_fence = q3 + 1.5 * iqr
            for d in sorted_data:
                if d.latency_ms > upper_fence:
                    self._outlier_ids.add(d.query_id)

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
                global_max,
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

    def _chunk_data(self, sorted_data: list[HistogramBar]) -> list[list[HistogramBar]]:
        """Split data into chunks for multi-chart rendering.

        For multi-platform data, chunks are split by unique query count (not bar count),
        so all platforms for a query stay in the same chunk.
        """
        if not self._use_grouped:
            if len(sorted_data) <= self.max_per_chart:
                return [sorted_data]
            chunks: list[list[HistogramBar]] = []
            for i in range(0, len(sorted_data), self.max_per_chart):
                chunks.append(sorted_data[i : i + self.max_per_chart])
            return chunks

        # Multi-platform: chunk by unique query IDs
        unique_queries = list(dict.fromkeys(d.query_id for d in sorted_data))
        if len(unique_queries) <= self.max_per_chart:
            return [sorted_data]

        chunks = []
        for i in range(0, len(unique_queries), self.max_per_chart):
            chunk_qids = set(unique_queries[i : i + self.max_per_chart])
            chunk_bars = [d for d in sorted_data if d.query_id in chunk_qids]
            chunks.append(chunk_bars)
        return chunks

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

        if global_max > 0:
            normalized = [(d.latency_ms / global_max) * chart_height for d in chunk]
            # Ensure minimum visible bar (at least 1/8th of a row) for any query that actually ran
            normalized = [max(0.125, n) if n > 0 else 0 for n in normalized]
        else:
            normalized = [0] * len(chunk)

        mean_row = round((global_mean / global_max) * chart_height) if global_max > 0 else 0

        palette = list(DEFAULT_PALETTE)
        chart_rows: list[str] = []

        for row in range(chart_height, 0, -1):
            row_chars: list[str] = []

            if row == chart_height or row == chart_height // 2 or row == 1:
                y_value = (row / chart_height) * global_max
                y_label = self._format_value(y_value).rjust(y_axis_width - 1) + " "
            else:
                y_label = " " * y_axis_width

            row_chars.append(y_label)

            for i, datum in enumerate(chunk):
                bar_height = normalized[i]
                is_outlier = datum.query_id in self._outlier_ids

                if datum.is_best:
                    bar_color = "#1b9e77"
                elif datum.is_worst:
                    bar_color = "#d95f02"
                else:
                    bar_color = palette[0]

                # Use ▓ for outlier bars, █ for normal bars
                outlier_char = "▓" if self.options.use_unicode else "#"
                fill_char = outlier_char if is_outlier else blocks[-1]

                if row <= math.ceil(bar_height):
                    if row <= int(bar_height):
                        block = fill_char * bar_width
                    else:
                        partial = int((bar_height - int(bar_height)) * 8)
                        if is_outlier:
                            block = fill_char * bar_width
                        elif partial > 0 and partial < len(blocks):
                            block = blocks[partial] * bar_width
                        else:
                            block = fill_char * bar_width
                    colored_block = colors.colorize(block, fg_color=bar_color)
                else:
                    if self.show_mean_line and row == mean_row:
                        colored_block = colors.colorize("·" * bar_width, fg_color="#6b7075")
                    else:
                        colored_block = " " * bar_width

                row_chars.append(colored_block)
                row_chars.append(" ")

            chart_rows.append("".join(row_chars).rstrip())

        lines.extend(chart_rows)

        lines.append(" " * y_axis_width + self._render_horizontal_line(bar_area_width))

        label_row: list[str] = [" " * y_axis_width]
        for datum in chunk:
            label = self._compact_query_label(datum.query_id, bar_width)
            label_row.append(label.center(bar_width))
            label_row.append(" ")
        lines.append("".join(label_row).rstrip())

        # Axis label
        lines.append(self._render_axis_label("Query ID", width, axis="x"))

        # Consolidate mean and legend into a single line
        footer_parts: list[str] = []
        if self.show_mean_line and global_mean > 0:
            mean_text = f"····· Mean: {self._format_value(global_mean)} ms"
            footer_parts.append(colors.colorize(mean_text, fg_color="#6b7075"))

        has_best = any(d.is_best for d in chunk)
        has_worst = any(d.is_worst for d in chunk)
        has_outlier = any(d.query_id in self._outlier_ids for d in chunk)

        if has_best or has_worst or has_outlier:
            if has_best:
                footer_parts.append(f"{colors.colorize('■', fg_color='#1b9e77')} Best")
            if has_worst:
                footer_parts.append(f"{colors.colorize('■', fg_color='#d95f02')} Worst")
            if has_outlier:
                outlier_char = "▓" if self.options.use_unicode else "#"
                footer_parts.append(f"{colors.colorize(outlier_char, fg_color='#666666')} Outlier")

        if footer_parts:
            lines.append("")
            lines.append(" " * y_axis_width + "  ".join(footer_parts))

        return "\n".join(lines)

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

        platform_colors: dict[str, str] = {}
        for i, platform in enumerate(self._platforms):
            platform_colors[platform] = palette[i % len(palette)]

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

        chart_rows: list[str] = []

        for row in range(chart_height, 0, -1):
            row_chars: list[str] = []

            if row == chart_height or row == chart_height // 2 or row == 1:
                y_value = (row / chart_height) * global_max
                y_label = self._format_value(y_value).rjust(y_axis_width - 1) + " "
            else:
                y_label = " " * y_axis_width

            row_chars.append(y_label)

            for qid in unique_queries:
                for platform in self._platforms:
                    datum = lookup.get((platform, qid))
                    if not datum:
                        row_chars.append(" " * sub_bar_width)
                        continue

                    raw_height = (datum.latency_ms / global_max) * chart_height if global_max > 0 else 0
                    # Ensure minimum visible bar (at least 1/8th of a row) for any query that actually ran
                    bar_height = max(0.125, raw_height) if raw_height > 0 else 0
                    is_outlier = datum.query_id in self._outlier_ids

                    # In multi-platform mode, always use platform color (identity is primary)
                    bar_color = platform_colors.get(datum.platform or "", palette[0])

                    outlier_char = "▓" if self.options.use_unicode else "#"
                    fill_char = outlier_char if is_outlier else blocks[-1]

                    if row <= math.ceil(bar_height):
                        if row <= int(bar_height):
                            block = fill_char * sub_bar_width
                        else:
                            if is_outlier:
                                block = fill_char * sub_bar_width
                            else:
                                partial = int((bar_height - int(bar_height)) * 8)
                                if partial > 0 and partial < len(blocks):
                                    block = blocks[partial] * sub_bar_width
                                else:
                                    block = fill_char * sub_bar_width
                        row_chars.append(colors.colorize(block, fg_color=bar_color))
                    else:
                        if self.show_mean_line and row == mean_row:
                            row_chars.append(colors.colorize("·" * sub_bar_width, fg_color="#6b7075"))
                        else:
                            row_chars.append(" " * sub_bar_width)

                row_chars.append(" ")  # Gap between query groups

            chart_rows.append("".join(row_chars).rstrip())

        lines.extend(chart_rows)

        lines.append(" " * y_axis_width + self._render_horizontal_line(bar_area_width))

        label_row: list[str] = [" " * y_axis_width]
        total_group_width = num_platforms * sub_bar_width
        for qid in unique_queries:
            label = self._compact_query_label(qid, total_group_width)
            label_row.append(label.center(total_group_width))
            label_row.append(" ")
        lines.append("".join(label_row).rstrip())

        # Axis label
        lines.append(self._render_axis_label("Query ID", width, axis="x"))

        # Consolidate mean and legend into a single line
        footer_parts: list[str] = []
        if self.show_mean_line and global_mean > 0:
            mean_text = f"····· Mean: {self._format_value(global_mean)} ms"
            footer_parts.append(colors.colorize(mean_text, fg_color="#6b7075"))

        for platform in self._platforms:
            color = platform_colors[platform]
            footer_parts.append(f"{colors.colorize('■', fg_color=color)} {platform}")

        has_outlier = any(d.query_id in self._outlier_ids for d in chunk)
        if has_outlier:
            outlier_char = "▓" if self.options.use_unicode else "#"
            footer_parts.append(f"{colors.colorize(outlier_char, fg_color='#666666')} Outlier")

        if footer_parts:
            lines.append("")
            lines.append(" " * y_axis_width + "  ".join(footer_parts))

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
