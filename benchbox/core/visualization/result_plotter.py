"""High-level chart orchestration for benchmark results."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from benchbox.core.results.loader import find_latest_result
from benchbox.core.results.models import BenchmarkResults
from benchbox.core.results.normalizer import normalize_result_dict
from benchbox.core.visualization.exceptions import VisualizationError
from benchbox.utils.scale_factor import format_scale_factor

logger = logging.getLogger(__name__)


@dataclass
class NormalizedQuery:
    query_id: str
    execution_time_ms: float | None
    status: str = "UNKNOWN"


@dataclass
class NormalizedResult:
    benchmark: str
    platform: str
    scale_factor: str | float | int
    execution_id: str | None
    timestamp: datetime | None
    total_time_ms: float | None
    avg_time_ms: float | None
    success_rate: float | None
    cost_total: float | None
    queries: list[NormalizedQuery] = field(default_factory=list)
    source_path: Path | None = None
    raw: dict[str, Any] = field(default_factory=dict)
    execution_mode: str | None = None


class ResultPlotter:
    """Load and normalize benchmark results for ASCII chart generation."""

    # Abbreviated execution mode labels for display
    MODE_ABBREVIATIONS: dict[str, str] = {
        "dataframe": "df",
        "sql": "sql",
        "datagen": "datagen",
        "data_only": "data_only",
    }

    def __init__(self, results: Sequence[NormalizedResult], theme: str = "light"):
        if not results:
            raise VisualizationError("No results provided for visualization.")
        self.results = list(results)
        self._disambiguate_platforms()
        self._disambiguate_modes()
        self._disambiguate_versions()
        self._sort_results_by_version()
        self.theme = theme

    def _disambiguate_platforms(self) -> None:
        """Append scale factor to platform labels when needed to avoid duplicates."""
        from collections import Counter

        platform_counts = Counter(r.platform for r in self.results)
        duplicated_platforms = {p for p, count in platform_counts.items() if count > 1}
        if not duplicated_platforms:
            return

        for platform in duplicated_platforms:
            matching = [r for r in self.results if r.platform == platform]
            scale_factors = {r.scale_factor for r in matching}
            if len(scale_factors) > 1:
                for result in matching:
                    sf_label = format_scale_factor(result.scale_factor)
                    object.__setattr__(result, "platform", f"{result.platform} SF={sf_label}")

    def _disambiguate_modes(self) -> None:
        """Ensure symmetric mode suffixes when same platform appears with different modes.

        When two results share the same base platform name but differ by execution mode,
        both get their mode suffix appended. Uses abbreviated labels: "df" not "dataframe".
        Single-result or single-mode cases keep the bare platform name.
        """
        from collections import Counter

        platform_counts = Counter(r.platform for r in self.results)
        duplicated_platforms = {p for p, count in platform_counts.items() if count > 1}
        if not duplicated_platforms:
            return

        for platform in duplicated_platforms:
            matching = [r for r in self.results if r.platform == platform]
            # Check execution_mode differences
            modes: dict[int, str] = {}
            for r in matching:
                mode = r.execution_mode or "sql"
                modes[id(r)] = mode

            unique_modes = set(modes.values())
            if len(unique_modes) <= 1:
                continue

            # Multiple modes for same platform — append abbreviated mode to each
            for result in matching:
                mode = modes[id(result)]
                abbrev = self.MODE_ABBREVIATIONS.get(mode, mode)
                object.__setattr__(result, "platform", f"{result.platform} ({abbrev})")

    def _disambiguate_versions(self) -> None:
        """Append driver version to platform labels when platforms are still duplicated.

        Runs after scale-factor and mode disambiguation. When two results share the
        same platform label but have different driver versions, both labels get the
        version appended so charts can distinguish them (e.g. "DuckDB 1.0.0" vs
        "DuckDB 1.4.3").
        """
        from collections import Counter

        platform_counts = Counter(r.platform for r in self.results)
        duplicated_platforms = {p for p, count in platform_counts.items() if count > 1}
        if not duplicated_platforms:
            return

        for platform in duplicated_platforms:
            matching = [r for r in self.results if r.platform == platform]
            versions: dict[int, str | None] = {}
            for r in matching:
                raw = r.raw or {}
                platform_block = raw.get("platform") or raw.get("platform_info") or {}
                version = platform_block.get("version") if isinstance(platform_block, dict) else None
                versions[id(r)] = version

            unique_versions = {v for v in versions.values() if v is not None}
            if len(unique_versions) <= 1:
                continue

            for result in matching:
                v = versions[id(result)]
                if v:
                    object.__setattr__(result, "platform", f"{result.platform} {v}")

    def _sort_results_by_version(self) -> None:
        """Sort results by semantic version extracted from the platform label.

        Provides a natural progression ordering (1.0.0 → 1.1.3 → 1.2.2 ...) for
        multi-version comparisons, regardless of run timestamp or file order.
        Non-versioned labels sort last, preserving their relative order.
        """
        import re

        def _version_key(result: NormalizedResult) -> tuple:
            m = re.search(r"(\d+)\.(\d+)(?:\.(\d+))?(?:-(\w+))?", result.platform)
            if not m:
                return (float("inf"), 0, 0, 0, result.platform)
            major = int(m.group(1))
            minor = int(m.group(2))
            patch = int(m.group(3)) if m.group(3) else 0
            # Pre-release suffixes (dev, alpha, beta) sort after the base release
            # since they represent development snapshots beyond the tagged version.
            is_pre = 1 if m.group(4) else 0
            return (major, minor, patch, is_pre, result.platform)

        self.results.sort(key=_version_key)

    # ------------------------------------------------------------------ Loading
    @classmethod
    def from_sources(
        cls,
        sources: Sequence[str | Path] | None = None,
        theme: str = "light",
    ) -> ResultPlotter:
        """Create a plotter from JSON result files or directories."""
        normalized: list[NormalizedResult] = []
        resolved_sources = cls._expand_sources(sources)
        for path in resolved_sources:
            try:
                with open(path, encoding="utf-8") as handle:
                    data = json.load(handle)
            except FileNotFoundError:
                logger.warning("Result file not found: %s", path)
                continue
            except json.JSONDecodeError as exc:
                logger.warning("Skipping invalid JSON file %s: %s", path, exc)
                continue

            normalized.append(cls._normalize_dict(data, source_path=path))

        if not normalized:
            raise VisualizationError("No valid result files found for visualization.")
        return cls(normalized, theme=theme)

    @classmethod
    def from_benchmark_results(
        cls,
        results: Sequence[BenchmarkResults],
        theme: str = "light",
    ) -> ResultPlotter:
        normalized = [cls._normalize_benchmark_result(res) for res in results]
        return cls(normalized, theme=theme)

    @staticmethod
    def _expand_sources(sources: Sequence[str | Path] | None) -> list[Path]:
        if sources:
            candidates = []
            for source in sources:
                path = Path(source)
                if path.is_dir():
                    candidates.extend(path.rglob("*.json"))
                elif path.is_file():
                    candidates.append(path)
            return sorted({p.resolve() for p in candidates})

        latest = find_latest_result(Path("benchmark_runs/results"))
        if not latest:
            raise VisualizationError("No result sources provided and none found in benchmark_runs/results.")
        return [latest]

    @staticmethod
    def _normalize_dict(data: dict[str, Any], source_path: Path | None) -> NormalizedResult:
        """Convert raw JSON dict to NormalizedResult."""
        normalized = normalize_result_dict(data)

        platform = normalized.platform

        queries = [
            NormalizedQuery(
                query_id=q.query_id,
                execution_time_ms=q.execution_time_ms,
                status=q.status,
            )
            for q in normalized.queries
        ]

        return NormalizedResult(
            benchmark=normalized.benchmark,
            platform=platform,
            scale_factor=normalized.scale_factor,
            execution_id=normalized.execution_id,
            timestamp=normalized.timestamp,
            total_time_ms=normalized.total_time_ms,
            avg_time_ms=normalized.avg_time_ms,
            success_rate=normalized.success_rate,
            cost_total=normalized.cost_total,
            queries=queries,
            source_path=source_path,
            raw=data,
            execution_mode=normalized.execution_mode,
        )

    @staticmethod
    def _normalize_benchmark_result(result: BenchmarkResults) -> NormalizedResult:
        timestamp = getattr(result, "timestamp", None)
        if timestamp and not isinstance(timestamp, datetime):
            try:
                timestamp = datetime.fromisoformat(str(timestamp))
            except Exception:
                timestamp = None

        queries: list[NormalizedQuery] = []
        for query in getattr(result, "query_results", []) or []:
            execution_time_ms = query.get("execution_time_ms")
            if execution_time_ms is None and "execution_time_seconds" in query:
                execution_time_ms = float(query["execution_time_seconds"]) * 1000.0
            if execution_time_ms is None and "execution_time" in query:
                execution_time_ms = float(query["execution_time"]) * 1000.0
            queries.append(
                NormalizedQuery(
                    query_id=query.get("query_id") or query.get("id") or "unknown",
                    execution_time_ms=execution_time_ms,
                    status=query.get("status", "UNKNOWN"),
                )
            )

        total_ms = getattr(result, "total_execution_time", None)
        if total_ms is not None:
            total_ms = float(total_ms) * 1000.0

        avg_ms = getattr(result, "average_query_time", None)
        if avg_ms is not None:
            avg_ms = float(avg_ms) * 1000.0

        success_rate = None
        if getattr(result, "total_queries", None):
            success_rate = (result.successful_queries or 0) / float(result.total_queries)

        cost_summary = getattr(result, "cost_summary", None) or {}

        platform = str(getattr(result, "platform", "unknown"))
        platform_info = getattr(result, "platform_info", None) or {}
        execution_mode = platform_info.get("execution_mode")

        return NormalizedResult(
            benchmark=str(getattr(result, "benchmark_name", "unknown")),
            platform=platform,
            scale_factor=getattr(result, "scale_factor", "unknown"),
            execution_id=getattr(result, "execution_id", None),
            timestamp=timestamp,
            total_time_ms=total_ms,
            avg_time_ms=avg_ms,
            success_rate=success_rate,
            cost_total=cost_summary.get("total_cost") if isinstance(cost_summary, dict) else None,
            queries=queries,
            source_path=None,
            raw={},
            execution_mode=execution_mode,
        )

    # ---------------------------------------------------------------- Utilities
    def _suggest_chart_types(self) -> list[str]:
        types = ["performance_bar"]
        if any(result.cost_total is not None for result in self.results):
            types.append("cost_scatter")
        if any(result.queries for result in self.results):
            types.append("distribution_box")
            types.append("query_histogram")
        if len(self.results) > 1 and any(result.queries for result in self.results):
            types.append("query_heatmap")
        if len(self.results) > 2:
            types.append("time_series")
        return types

    def _performance_score(self, result: NormalizedResult) -> float:
        if result.avg_time_ms and result.avg_time_ms > 0:
            return 1000.0 / result.avg_time_ms
        if result.total_time_ms and result.total_time_ms > 0 and result.queries:
            return (len(result.queries) * 1000.0) / result.total_time_ms
        return 0.0

    def _benchmark_label(self) -> str:
        benchmarks = {r.benchmark for r in self.results}
        if len(benchmarks) == 1:
            return next(iter(benchmarks))
        return ", ".join(sorted(benchmarks))

    @staticmethod
    def _natural_sort_key(s: str) -> tuple[float, str]:
        """Sort key for natural ordering of query IDs."""
        import re

        match = re.match(r"^(\D*)(\d+)(.*)$", s)
        if match:
            prefix, num, suffix = match.groups()
            return (float(num), prefix + suffix)
        return (float("inf"), s)

    def group_by(self, field: str) -> dict[str, ResultPlotter]:
        """Split results by a field (platform or benchmark) for batch rendering."""
        if field not in {"platform", "benchmark"}:
            raise VisualizationError("group_by must be 'platform' or 'benchmark'.")

        groups: dict[str, list[NormalizedResult]] = {}
        for result in self.results:
            key = getattr(result, field)
            groups.setdefault(key, []).append(result)

        return {key: ResultPlotter(values, theme=self.theme) for key, values in groups.items()}
