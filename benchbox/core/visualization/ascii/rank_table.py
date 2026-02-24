"""ASCII rank table chart for competitive per-query platform ranking."""

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
class RankTableData:
    """Data for a rank table chart."""

    queries: list[str]
    platforms: list[str]
    times: dict[tuple[str, str], float]  # (platform, query) -> execution time


class ASCIIRankTable(ASCIIChartBase):
    """Rank table showing per-query competitive rankings across platforms.

    Each cell shows the ordinal rank (1st, 2nd, 3rd...) of each platform
    for each query, with aggregate win/loss tallies and geometric mean rank.

    Example output:
    ```
    Query Rankings (1st = fastest)
    ──────────────────────────────────────
    Query    DuckDB  Polars  SQLite  Pandas
    Q1        1st     2nd     3rd     4th
    Q2        2nd     1st     3rd     4th
    ──────────────────────────────────────
    1st wins    15      5       2       0
    GeoRank   1.2    1.8     2.8     3.9
    ```
    """

    def __init__(
        self,
        data: RankTableData,
        title: str | None = None,
        options: ASCIIChartOptions | None = None,
        metadata: dict | None = None,
    ):
        super().__init__(options, metadata=metadata)
        self.data = data
        self.title = title or "Query Rankings (1st = fastest)"

    def render(self) -> str:
        """Render the rank table as a string."""
        self._detect_capabilities()

        if not self.data.queries or not self.data.platforms:
            return "No data to display"

        colors = self.options.get_colors()
        width = self.options.get_effective_width()

        # Sort queries naturally
        queries = sorted(self.data.queries, key=self._natural_sort_key)
        platforms = self.data.platforms

        # Compute rankings for each query
        rankings: dict[str, dict[str, int]] = {}  # query -> {platform -> rank}
        for query in queries:
            # Get times for this query
            qtimes: list[tuple[str, float]] = []
            for platform in platforms:
                time = self.data.times.get((platform, query))
                if time is not None:
                    qtimes.append((platform, time))

            # Sort by time ascending (faster = better rank)
            qtimes.sort(key=lambda x: x[1])

            # Assign ranks with tie handling
            query_ranks: dict[str, int] = {}
            i = 0
            while i < len(qtimes):
                # Find all tied platforms
                j = i
                while j < len(qtimes) and qtimes[j][1] == qtimes[i][1]:
                    j += 1
                rank = i + 1
                for k in range(i, j):
                    query_ranks[qtimes[k][0]] = rank
                i = j

            rankings[query] = query_ranks

        # Calculate column widths
        query_col_width = max(5, max(len(q) for q in queries) + 1)
        plat_col_width = max(6, max(len(p) for p in platforms) + 1)

        # Check if table fits; if not, truncate platforms
        total_width = query_col_width + len(platforms) * plat_col_width
        if total_width > width:
            plat_col_width = max(5, (width - query_col_width) // len(platforms))

        n_platforms = len(platforms)

        lines: list[str] = []

        # Title and subtitle
        lines.append(self._render_title(self.title, width))
        subtitle = self._render_subtitle(width)
        if subtitle:
            lines.append(subtitle)
        lines.append(self._render_horizontal_line(width))

        # Header row
        header = "Query".ljust(query_col_width)
        for platform in platforms:
            header += self._truncate_label(platform, plat_col_width - 1).center(plat_col_width)
        lines.append(header)

        # Data rows
        for query in queries:
            row = query.ljust(query_col_width)
            query_ranks = rankings.get(query, {})
            max_rank = max(query_ranks.values()) if query_ranks else n_platforms
            for platform in platforms:
                rank = query_ranks.get(platform)
                if rank is None:
                    cell = "-"
                else:
                    cell = self._ordinal(rank)

                # Pad BEFORE colorizing to avoid ANSI codes breaking alignment
                padded_cell = cell.center(plat_col_width)
                # Color: 1st green, worst rank red
                if rank == 1:
                    padded_cell = colors.colorize(padded_cell, fg_color="#1b9e77")
                elif rank is not None and rank == max_rank and max_rank > 1:
                    padded_cell = colors.colorize(padded_cell, fg_color="#d95f02")

                row += padded_cell
            lines.append(row)

        lines.append(self._render_horizontal_line(width))

        # Footer: win tallies
        wins_row = "Wins".ljust(query_col_width)
        all_ranks: dict[str, list[int]] = {p: [] for p in platforms}

        for query in queries:
            for platform in platforms:
                rank = rankings.get(query, {}).get(platform)
                if rank is not None:
                    all_ranks[platform].append(rank)

        win_counts = {p: sum(1 for r in ranks if r == 1) for p, ranks in all_ranks.items()}
        for platform in platforms:
            count_str = str(win_counts.get(platform, 0))
            wins_row += count_str.center(plat_col_width)
        lines.append(wins_row)

        # Footer: geometric mean rank
        georank_row = "GeoRank".ljust(query_col_width)
        for platform in platforms:
            ranks = all_ranks.get(platform, [])
            if ranks:
                geo_mean = math.exp(sum(math.log(r) for r in ranks) / len(ranks))
                georank_row += f"{geo_mean:.1f}".center(plat_col_width)
            else:
                georank_row += "-".center(plat_col_width)
        lines.append(georank_row)

        return "\n".join(lines)

    @staticmethod
    def _ordinal(n: int) -> str:
        """Convert integer to ordinal string (1st, 2nd, 3rd, 4th...)."""
        if 11 <= (n % 100) <= 13:
            return f"{n}th"
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suffix}"

    @staticmethod
    def _natural_sort_key(s: str):
        """Natural sort key for query IDs (Q1, Q2, Q10 not Q1, Q10, Q2)."""
        import re

        return [int(c) if c.isdigit() else c.lower() for c in re.split(r"(\d+)", s)]


def from_heatmap_data(
    matrix: Sequence[Sequence[float]],
    queries: Sequence[str],
    platforms: Sequence[str],
    title: str | None = None,
    options: ASCIIChartOptions | None = None,
) -> ASCIIRankTable:
    """Create ASCIIRankTable from the same matrix format as ASCIIHeatmap.

    Args:
        matrix: 2D matrix of execution times [query_idx][platform_idx].
        queries: Query labels (row labels).
        platforms: Platform labels (column labels).
        title: Optional chart title.
        options: Chart rendering options.

    Returns:
        Configured ASCIIRankTable instance.
    """
    times: dict[tuple[str, str], float] = {}
    for qi, query in enumerate(queries):
        for pi, platform in enumerate(platforms):
            if qi < len(matrix) and pi < len(matrix[qi]):
                times[(platform, query)] = matrix[qi][pi]

    data = RankTableData(
        queries=list(queries),
        platforms=list(platforms),
        times=times,
    )
    return ASCIIRankTable(data=data, title=title, options=options)
