"""Unified benchmark suite for cross-platform comparisons.

Provides a single interface for running benchmarks across both SQL and
DataFrame platforms with unified result collection and reporting.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import json
import logging
import math
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from benchbox.core.comparison.types import (
    PlatformType,
    UnifiedBenchmarkConfig,
    UnifiedComparisonSummary,
    UnifiedPlatformResult,
    UnifiedQueryResult,
    detect_platform_types,
)

logger = logging.getLogger(__name__)


class UnifiedBenchmarkSuite:
    """Unified benchmark suite for SQL and DataFrame platform comparisons.

    Provides a single interface for:
    - Running benchmarks across multiple platforms
    - Collecting and normalizing results
    - Statistical analysis
    - Report generation

    Example:
        suite = UnifiedBenchmarkSuite(
            config=UnifiedBenchmarkConfig(
                platform_type=PlatformType.AUTO,
                scale_factor=0.01,
                benchmark="tpch",
            )
        )

        # Run comparison
        results = suite.run_comparison(
            platforms=["duckdb", "sqlite"],  # SQL platforms
            data_dir="benchmark_runs/tpch/sf001/data",
        )

        # Get summary
        summary = suite.get_summary(results)
        print(f"Fastest: {summary.fastest_platform}")
    """

    def __init__(self, config: UnifiedBenchmarkConfig | None = None):
        """Initialize the unified benchmark suite.

        Args:
            config: Benchmark configuration. Defaults to standard config.
        """
        self.config = config or UnifiedBenchmarkConfig()

    def get_available_platforms(self, platform_type: PlatformType | None = None) -> list[str]:
        """Get available platforms of the specified type.

        Args:
            platform_type: Filter by platform type. None returns all.

        Returns:
            List of available platform names.
        """
        platforms = []

        # Get SQL platforms
        if platform_type in (None, PlatformType.SQL, PlatformType.AUTO):
            from benchbox.platforms import list_available_platforms

            sql_available = list_available_platforms()
            platforms.extend(sql_available)

        # Get DataFrame platforms
        if platform_type in (None, PlatformType.DATAFRAME, PlatformType.AUTO):
            from benchbox.platforms import list_available_dataframe_platforms

            df_available = list_available_dataframe_platforms()
            platforms.extend([name for name, available in df_available.items() if available])

        return sorted(set(platforms))

    def run_comparison(
        self,
        platforms: list[str],
        data_dir: str | Path | None = None,
    ) -> list[UnifiedPlatformResult]:
        """Run benchmark comparison across multiple platforms.

        Args:
            platforms: List of platform names to benchmark
            data_dir: Directory containing benchmark data (for DataFrame platforms)

        Returns:
            List of UnifiedPlatformResult for each platform
        """
        if not platforms:
            raise ValueError("At least one platform is required")

        # Detect platform type if auto
        if self.config.platform_type == PlatformType.AUTO:
            detected_type, inconsistent = detect_platform_types(platforms)
            if inconsistent:
                raise ValueError(
                    f"Mixed platform types detected. "
                    f"Cannot compare {detected_type.value} platforms with: {inconsistent}. "
                    f"Use --type to explicitly specify platform type."
                )
            platform_type = detected_type
        else:
            platform_type = self.config.platform_type

        logger.info(f"Running {platform_type.value} comparison across {len(platforms)} platforms")

        # Route to appropriate benchmark runner
        if platform_type == PlatformType.DATAFRAME:
            return self._run_dataframe_comparison(platforms, data_dir)
        else:
            return self._run_sql_comparison(platforms, data_dir)

    def _run_sql_comparison(
        self,
        platforms: list[str],
        data_dir: str | Path | None = None,
    ) -> list[UnifiedPlatformResult]:
        """Run SQL platform comparison.

        Args:
            platforms: SQL platform names
            data_dir: Data directory (for embedded platforms)

        Returns:
            List of results for each platform
        """
        results = []

        for platform in platforms:
            logger.info(f"Benchmarking SQL platform: {platform}")
            try:
                result = self._benchmark_sql_platform(platform, data_dir)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to benchmark {platform}: {e}")
                results.append(
                    UnifiedPlatformResult(
                        platform=platform,
                        platform_type=PlatformType.SQL,
                        query_results=[
                            UnifiedQueryResult(
                                query_id="ALL",
                                platform=platform,
                                platform_type=PlatformType.SQL,
                                status="ERROR",
                                error_message=str(e),
                            )
                        ],
                    )
                )

        return results

    def _benchmark_sql_platform(
        self,
        platform: str,
        data_dir: str | Path | None = None,
    ) -> UnifiedPlatformResult:
        """Benchmark a single SQL platform.

        Args:
            platform: Platform name
            data_dir: Data directory for embedded platforms

        Returns:
            UnifiedPlatformResult with query results
        """
        from benchbox.platforms import get_adapter

        # Determine query IDs (default to TPC-H Q1-Q22)
        query_ids = self.config.query_ids or [f"Q{i}" for i in range(1, 23)]

        query_results = []

        # Get adapter for the platform
        adapter = get_adapter(platform)

        # Handle embedded platforms that need data loading
        if platform in ("duckdb", "sqlite", "datafusion"):
            if data_dir is None:
                sf_str = f"sf{self.config.scale_factor}".replace(".", "")
                data_dir = Path(f"benchmark_runs/tpch/{sf_str}/data")

            data_path = Path(data_dir)
            if not data_path.exists():
                raise ValueError(f"Data directory not found: {data_dir}")

            conn = self._setup_embedded_sql(platform, data_path)
        else:
            # For remote platforms, assume data is already loaded
            conn = adapter.connect()

        try:
            for query_id in query_ids:
                logger.info(f"  Running {query_id}...")
                result = self._run_sql_query(query_id, platform, conn)
                query_results.append(result)
        finally:
            if hasattr(conn, "close"):
                conn.close()

        # Calculate aggregates
        return self._build_platform_result(platform, PlatformType.SQL, query_results)

    def _setup_embedded_sql(self, platform: str, data_path: Path):
        """Set up embedded SQL platform with data.

        Args:
            platform: Platform name
            data_path: Path to data directory

        Returns:
            Database connection
        """
        parquet_dir = data_path / "parquet"
        if not parquet_dir.exists():
            parquet_dir = data_path

        tables = ["lineitem", "orders", "customer", "supplier", "part", "partsupp", "nation", "region"]

        if platform == "duckdb":
            import duckdb

            conn = duckdb.connect()
            for table in tables:
                table_path = parquet_dir / f"{table}.parquet"
                if table_path.exists():
                    conn.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{table_path}')")
            return conn

        elif platform == "sqlite":
            import sqlite3

            import pandas as pd

            conn = sqlite3.connect(":memory:")
            for table in tables:
                table_path = parquet_dir / f"{table}.parquet"
                if table_path.exists():
                    df = pd.read_parquet(str(table_path))
                    df.to_sql(table, conn, index=False)
            return conn

        else:
            raise ValueError(f"Unsupported embedded SQL platform: {platform}")

    def _run_sql_query(
        self,
        query_id: str,
        platform: str,
        conn,
    ) -> UnifiedQueryResult:
        """Run a single SQL query benchmark.

        Args:
            query_id: Query identifier
            platform: Platform name
            conn: Database connection

        Returns:
            UnifiedQueryResult with timing data
        """
        from benchbox.core.tpch.queries import TPCHQuery

        try:
            # Get query SQL
            query_num = int(query_id.replace("Q", ""))
            tpch_query = TPCHQuery.get_query(query_num, dialect=platform)

            execution_times = []
            rows_returned = 0

            # Warmup
            for _ in range(self.config.warmup_iterations):
                conn.execute(tpch_query.sql).fetchall()

            # Benchmark iterations
            for i in range(self.config.benchmark_iterations):
                start = time.perf_counter()
                result = conn.execute(tpch_query.sql).fetchall()
                elapsed_ms = (time.perf_counter() - start) * 1000
                execution_times.append(elapsed_ms)

                if i == 0:
                    rows_returned = len(result)

            return UnifiedQueryResult(
                query_id=query_id,
                platform=platform,
                platform_type=PlatformType.SQL,
                iterations=len(execution_times),
                execution_times_ms=execution_times,
                mean_time_ms=statistics.mean(execution_times) if execution_times else 0,
                std_time_ms=statistics.stdev(execution_times) if len(execution_times) > 1 else 0,
                min_time_ms=min(execution_times) if execution_times else 0,
                max_time_ms=max(execution_times) if execution_times else 0,
                rows_returned=rows_returned,
                status="SUCCESS",
            )

        except Exception as e:
            logger.error(f"Error running {query_id} on {platform}: {e}")
            return UnifiedQueryResult(
                query_id=query_id,
                platform=platform,
                platform_type=PlatformType.SQL,
                status="ERROR",
                error_message=str(e),
            )

    def _run_dataframe_comparison(
        self,
        platforms: list[str],
        data_dir: str | Path | None = None,
    ) -> list[UnifiedPlatformResult]:
        """Run DataFrame platform comparison.

        Delegates to the existing DataFrameBenchmarkSuite.

        Args:
            platforms: DataFrame platform names
            data_dir: Data directory

        Returns:
            List of results for each platform
        """
        from benchbox.core.dataframe.benchmark_suite import (
            BenchmarkConfig,
            DataFrameBenchmarkSuite,
        )

        # Create DataFrame benchmark config
        df_config = BenchmarkConfig(
            scale_factor=self.config.scale_factor,
            query_ids=self.config.query_ids,
            warmup_iterations=self.config.warmup_iterations,
            benchmark_iterations=self.config.benchmark_iterations,
            track_memory=self.config.track_memory,
            timeout_seconds=self.config.timeout_seconds,
        )

        suite = DataFrameBenchmarkSuite(config=df_config)

        if data_dir is None:
            sf_str = f"sf{self.config.scale_factor}".replace(".", "")
            data_dir = Path(f"benchmark_runs/tpch/{sf_str}/data")

        # Run DataFrame comparison
        df_results = suite.run_comparison(platforms=platforms, data_dir=data_dir)

        # Convert to unified format
        unified_results = []
        for df_result in df_results:
            query_results = []
            for qr in df_result.query_results:
                query_results.append(
                    UnifiedQueryResult(
                        query_id=qr.query_id,
                        platform=qr.platform,
                        platform_type=PlatformType.DATAFRAME,
                        iterations=qr.iterations,
                        execution_times_ms=qr.execution_times_ms,
                        mean_time_ms=qr.mean_time_ms,
                        std_time_ms=qr.std_time_ms,
                        min_time_ms=qr.min_time_ms,
                        max_time_ms=qr.max_time_ms,
                        memory_peak_mb=qr.memory_peak_mb,
                        rows_returned=qr.rows_returned,
                        status=qr.status,
                        error_message=qr.error_message,
                    )
                )

            unified_results.append(
                UnifiedPlatformResult(
                    platform=df_result.platform,
                    platform_type=PlatformType.DATAFRAME,
                    query_results=query_results,
                    total_time_ms=df_result.total_time_ms,
                    geometric_mean_ms=df_result.geometric_mean_ms,
                    success_rate=df_result.success_rate,
                )
            )

        return unified_results

    def _build_platform_result(
        self,
        platform: str,
        platform_type: PlatformType,
        query_results: list[UnifiedQueryResult],
    ) -> UnifiedPlatformResult:
        """Build platform result with calculated aggregates.

        Args:
            platform: Platform name
            platform_type: Platform type
            query_results: Query results

        Returns:
            UnifiedPlatformResult with aggregates
        """
        successful = [r for r in query_results if r.status == "SUCCESS"]

        # Calculate total time and geometric mean
        total_time = sum(r.mean_time_ms for r in successful)
        geometric_mean = 0.0

        if successful:
            mean_times = [r.mean_time_ms for r in successful if r.mean_time_ms > 0]
            if mean_times:
                log_sum = sum(math.log(t) for t in mean_times)
                geometric_mean = math.exp(log_sum / len(mean_times))

        # Calculate success rate
        success_rate = (len(successful) / len(query_results) * 100) if query_results else 0

        return UnifiedPlatformResult(
            platform=platform,
            platform_type=platform_type,
            query_results=query_results,
            total_time_ms=total_time,
            geometric_mean_ms=geometric_mean,
            success_rate=success_rate,
        )

    def get_summary(self, results: list[UnifiedPlatformResult]) -> UnifiedComparisonSummary:
        """Generate comparison summary from results.

        Args:
            results: List of platform results

        Returns:
            UnifiedComparisonSummary with comparison metrics
        """
        if not results:
            raise ValueError("No results to summarize")

        platforms = [r.platform for r in results]
        platform_type = results[0].platform_type

        # Get geometric means for ranking
        geomeans = {r.platform: r.geometric_mean_ms for r in results if r.geometric_mean_ms > 0}

        if not geomeans:
            return UnifiedComparisonSummary(
                platforms=platforms,
                platform_type=platform_type,
                fastest_platform=platforms[0],
                slowest_platform=platforms[0],
                speedup_ratio=1.0,
                query_winners={},
                total_queries=0,
            )

        fastest = min(geomeans, key=geomeans.get)
        slowest = max(geomeans, key=geomeans.get)
        speedup_ratio = geomeans[slowest] / geomeans[fastest] if geomeans[fastest] > 0 else 1.0

        # Find query winners
        query_winners: dict[str, str] = {}
        all_query_ids = set()
        for result in results:
            for qr in result.query_results:
                all_query_ids.add(qr.query_id)

        for query_id in all_query_ids:
            best_time = float("inf")
            best_platform = ""
            for result in results:
                for qr in result.query_results:
                    if qr.query_id == query_id and qr.status == "SUCCESS" and qr.mean_time_ms < best_time:
                        best_time = qr.mean_time_ms
                        best_platform = result.platform
            if best_platform:
                query_winners[query_id] = best_platform

        return UnifiedComparisonSummary(
            platforms=platforms,
            platform_type=platform_type,
            fastest_platform=fastest,
            slowest_platform=slowest,
            speedup_ratio=speedup_ratio,
            query_winners=query_winners,
            total_queries=len(all_query_ids),
        )

    def export_results(
        self,
        results: list[UnifiedPlatformResult],
        output_path: str | Path,
        format: str = "json",
    ) -> Path:
        """Export benchmark results to file.

        Args:
            results: List of platform results
            output_path: Output file path
            format: Output format (json, markdown, text)

        Returns:
            Path to created file
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format == "json":
            data = {
                "benchmark_suite": "unified_comparison",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "config": {
                    "platform_type": self.config.platform_type.value,
                    "scale_factor": self.config.scale_factor,
                    "benchmark": self.config.benchmark,
                    "query_ids": self.config.query_ids,
                    "warmup_iterations": self.config.warmup_iterations,
                    "benchmark_iterations": self.config.benchmark_iterations,
                },
                "results": [r.to_dict() for r in results],
                "summary": self.get_summary(results).to_dict() if results else None,
            }
            output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

        elif format == "markdown":
            content = self._generate_markdown_report(results)
            output_path.write_text(content, encoding="utf-8")

        elif format == "text":
            content = self._generate_text_report(results)
            output_path.write_text(content, encoding="utf-8")

        else:
            raise ValueError(f"Unsupported export format: {format}")

        return output_path

    def _generate_markdown_report(self, results: list[UnifiedPlatformResult]) -> str:
        """Generate markdown report from results."""
        lines = []
        summary = self.get_summary(results)

        lines.append("# Platform Comparison Report")
        lines.append("")
        lines.append(f"**Platform Type:** {summary.platform_type.value}")
        lines.append(f"**Scale Factor:** {self.config.scale_factor}")
        lines.append(f"**Benchmark:** {self.config.benchmark}")
        lines.append(f"**Iterations:** {self.config.benchmark_iterations}")
        lines.append(f"**Generated:** {datetime.now(timezone.utc).isoformat()}")
        lines.append("")

        lines.append("## Summary")
        lines.append("")
        lines.append(f"**Fastest Platform:** {summary.fastest_platform}")
        lines.append(f"**Slowest Platform:** {summary.slowest_platform}")
        lines.append(f"**Speedup Ratio:** {summary.speedup_ratio:.2f}x")
        lines.append("")

        lines.append("## Platform Results")
        lines.append("")
        lines.append("| Platform | Geomean (ms) | Total (ms) | Success Rate |")
        lines.append("|----------|--------------|------------|--------------|")

        for result in sorted(results, key=lambda r: r.geometric_mean_ms or float("inf")):
            geomean = f"{result.geometric_mean_ms:.2f}" if result.geometric_mean_ms else "N/A"
            total = f"{result.total_time_ms:.2f}" if result.total_time_ms else "N/A"
            success = f"{result.success_rate:.1f}%"
            lines.append(f"| {result.platform} | {geomean} | {total} | {success} |")

        lines.append("")

        if summary.query_winners:
            lines.append("## Query Winners")
            lines.append("")
            for query_id, winner in sorted(summary.query_winners.items()):
                lines.append(f"- **{query_id}**: {winner}")
            lines.append("")

        return "\n".join(lines)

    def _generate_text_report(self, results: list[UnifiedPlatformResult]) -> str:
        """Generate text report from results."""
        lines = []
        summary = self.get_summary(results)

        lines.append("=" * 60)
        lines.append("PLATFORM COMPARISON RESULTS")
        lines.append("=" * 60)
        lines.append("")

        lines.append(f"Platform Type: {summary.platform_type.value}")
        lines.append(f"Platforms: {', '.join(summary.platforms)}")
        lines.append(f"Total Queries: {summary.total_queries}")
        lines.append("")

        lines.append(f"Fastest: {summary.fastest_platform}")
        lines.append(f"Slowest: {summary.slowest_platform}")
        lines.append(f"Speedup: {summary.speedup_ratio:.2f}x")
        lines.append("")

        lines.append(f"{'Platform':15s} {'Geomean (ms)':>15s} {'Total (ms)':>15s} {'Success':>10s}")
        lines.append("-" * 60)

        for result in sorted(results, key=lambda r: r.geometric_mean_ms or float("inf")):
            geomean = f"{result.geometric_mean_ms:.2f}" if result.geometric_mean_ms else "N/A"
            total = f"{result.total_time_ms:.2f}" if result.total_time_ms else "N/A"
            success = f"{result.success_rate:.0f}%"
            lines.append(f"{result.platform:15s} {geomean:>15s} {total:>15s} {success:>10s}")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)


def run_unified_comparison(
    platforms: list[str],
    platform_type: PlatformType = PlatformType.AUTO,
    scale_factor: float = 0.01,
    benchmark: str = "tpch",
    query_ids: list[str] | None = None,
    data_dir: str | Path | None = None,
) -> list[UnifiedPlatformResult]:
    """Run a unified cross-platform comparison.

    Convenience function for quick comparisons.

    Args:
        platforms: Platforms to compare
        platform_type: SQL, DATAFRAME, or AUTO
        scale_factor: Benchmark scale factor
        benchmark: Benchmark name
        query_ids: Optional query subset
        data_dir: Data directory

    Returns:
        List of UnifiedPlatformResult
    """
    suite = UnifiedBenchmarkSuite(
        config=UnifiedBenchmarkConfig(
            platform_type=platform_type,
            scale_factor=scale_factor,
            benchmark=benchmark,
            query_ids=query_ids,
        )
    )
    return suite.run_comparison(platforms=platforms, data_dir=data_dir)


__all__ = [
    "UnifiedBenchmarkSuite",
    "run_unified_comparison",
]
