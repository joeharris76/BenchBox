"""PostgreSQL Extension Comparison Report Generator.

Generates comparison reports between PostgreSQL baseline and extension
benchmark results. Supports comparing any PostgreSQL extension (pg_duckdb,
pg_mooncake, TimescaleDB) against vanilla PostgreSQL or against each other.

Report types:
- Query-by-query speedup ratios
- Load time comparison
- Total execution time comparison

Example:
    >>> from benchbox.core.pg_extension_comparison import generate_comparison_report
    >>> report = generate_comparison_report(baseline_results, extension_results)
    >>> emit(report)

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from typing import Any

from benchbox.utils import format_duration


def generate_comparison_report(
    baseline: dict[str, Any],
    extension: dict[str, Any],
) -> str:
    """Generate a comparison report between two benchmark results.

    Compares a baseline run (typically vanilla PostgreSQL) against an
    extension run (pg_duckdb, pg_mooncake, etc.) and produces a formatted
    report with speedup ratios and timing differences.

    Args:
        baseline: Result data from the baseline platform run.
        extension: Result data from the extension platform run.

    Returns:
        Formatted multi-line comparison report string.
    """
    lines = []

    baseline_platform = baseline.get("platform", "baseline")
    extension_platform = extension.get("platform", "extension")
    benchmark = baseline.get("benchmark", extension.get("benchmark", "unknown")).upper()
    scale = baseline.get("scale_factor", extension.get("scale_factor", "unknown"))

    lines.append("PostgreSQL Extension Comparison Report")
    lines.append("=" * 60)
    lines.append(f"Benchmark: {benchmark} (SF {scale})")
    lines.append(f"Baseline:  {baseline_platform}")
    lines.append(f"Extension: {extension_platform}")
    lines.append("")

    # Overall execution time comparison
    baseline_total = baseline.get("total_execution_time")
    extension_total = extension.get("total_execution_time")

    if baseline_total is not None and extension_total is not None and baseline_total > 0:
        speedup = baseline_total / extension_total if extension_total > 0 else float("inf")
        lines.append("Overall Execution")
        lines.append("-" * 40)
        lines.append(f"  {baseline_platform}: {format_duration(baseline_total)}")
        lines.append(f"  {extension_platform}: {format_duration(extension_total)}")
        lines.append(f"  Speedup: {speedup:.2f}x")
        lines.append("")

    # Data loading comparison
    baseline_load = baseline.get("data_loading_time")
    extension_load = extension.get("data_loading_time")

    if baseline_load is not None and extension_load is not None and baseline_load > 0:
        load_ratio = baseline_load / extension_load if extension_load > 0 else float("inf")
        lines.append("Data Loading")
        lines.append("-" * 40)
        lines.append(f"  {baseline_platform}: {format_duration(baseline_load)}")
        lines.append(f"  {extension_platform}: {format_duration(extension_load)}")
        lines.append(f"  Ratio: {load_ratio:.2f}x")
        lines.append("")

    # Query-by-query comparison
    baseline_queries = baseline.get("query_results", {})
    extension_queries = extension.get("query_results", {})

    if baseline_queries and extension_queries:
        common_queries = sorted(set(baseline_queries.keys()) & set(extension_queries.keys()))
        if common_queries:
            lines.append("Query-by-Query Comparison")
            lines.append("-" * 60)
            lines.append(f"  {'Query':<10} {'Baseline':>12} {'Extension':>12} {'Speedup':>10}")
            lines.append(f"  {'-' * 10} {'-' * 12} {'-' * 12} {'-' * 10}")

            speedup_entries = []  # list of (query_id, speedup) tuples
            for qid in common_queries:
                b_time = baseline_queries[qid].get("execution_time", 0)
                e_time = extension_queries[qid].get("execution_time", 0)

                if b_time > 0 and e_time > 0:
                    speedup = b_time / e_time
                    speedup_entries.append((qid, speedup))
                    lines.append(
                        f"  {qid:<10} {format_duration(b_time):>12} {format_duration(e_time):>12} {speedup:>9.2f}x"
                    )
                elif b_time > 0:
                    lines.append(f"  {qid:<10} {format_duration(b_time):>12} {'FAILED':>12} {'N/A':>10}")
                elif e_time > 0:
                    lines.append(f"  {qid:<10} {'FAILED':>12} {format_duration(e_time):>12} {'N/A':>10}")

            if speedup_entries:
                lines.append("")
                speedups = [s for _, s in speedup_entries]
                geo_mean = _geometric_mean(speedups)
                lines.append(f"  Geometric mean speedup: {geo_mean:.2f}x")
                max_qid, max_speedup = max(speedup_entries, key=lambda x: x[1])
                lines.append(f"  Max speedup: {max_speedup:.2f}x ({max_qid})")
                min_qid, min_speedup = min(speedup_entries, key=lambda x: x[1])
                lines.append(f"  Min speedup: {min_speedup:.2f}x ({min_qid})")
            lines.append("")

    # Summary
    lines.append("Summary")
    lines.append("-" * 40)
    baseline_success = baseline.get("success", False)
    extension_success = extension.get("success", False)
    lines.append(f"  {baseline_platform}: {'PASSED' if baseline_success else 'FAILED'}")
    lines.append(f"  {extension_platform}: {'PASSED' if extension_success else 'FAILED'}")

    if baseline_total and extension_total and baseline_total > 0:
        speedup = baseline_total / extension_total if extension_total > 0 else float("inf")
        if speedup > 1:
            lines.append(f"  {extension_platform} is {speedup:.1f}x faster than {baseline_platform}")
        elif speedup < 1:
            lines.append(f"  {baseline_platform} is {1 / speedup:.1f}x faster than {extension_platform}")
        else:
            lines.append("  Performance is equivalent")

    return "\n".join(lines)


def _geometric_mean(values: list[float]) -> float:
    """Calculate geometric mean of positive values.

    Args:
        values: List of positive float values.

    Returns:
        Geometric mean of the values.
    """
    if not values:
        return 0.0
    import math

    positive = [v for v in values if v > 0]
    if not positive:
        return 0.0
    log_sum = sum(math.log(v) for v in positive)
    return math.exp(log_sum / len(positive))
